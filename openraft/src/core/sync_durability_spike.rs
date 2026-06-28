//! Phase 3b.2 SPIKE — proves the durability-consumer mechanism in isolation.
//!
//! Three unknowns this de-risks before the real `log_store` migration:
//!  1. A **reactor-free `block_on`** drives an `async` future to completion with
//!     no tokio runtime (a never-park poll loop). Real storage impls complete via
//!     sync ops, so this works on a dedicated consumer thread.
//!  2. A **disruptor ring carries a move-only payload** (here: a value + a
//!     completion `Sender`). The `EventPoller` yields `&mut event`, so the
//!     consumer `take()`s the owned payload out of the slot — unlike
//!     `handle_events_with`, which only lends `&event`.
//!  3. The consumer executes the op (reactor-free) and **signals completion back**
//!     to the producer side.
//!
//! This module is feature-gated scaffolding; it is deleted once Phase 3b.2's real
//! durability consumer lands. The `block_on` helper graduates into the runtime.

use std::future::Future;
use std::task::Context;
use std::task::Poll;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Waker;

/// Reactor-free `block_on`: drive `fut` to completion by polling with a no-op
/// waker, never parking. For futures that complete via synchronous operations
/// (as reactor-free storage/network impls do) the first poll returns `Ready`;
/// a `Pending` simply re-polls (busy-spin), so no runtime/reactor is needed.
pub(crate) fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = Box::pin(fut);
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => std::hint::spin_loop(),
        }
    }
}

const NOOP_VTABLE: RawWakerVTable = RawWakerVTable::new(noop_clone, noop, noop, noop);
fn noop_clone(_: *const ()) -> RawWaker {
    RawWaker::new(std::ptr::null(), &NOOP_VTABLE)
}
fn noop(_: *const ()) {}
fn noop_waker() -> Waker {
    // SAFETY: all vtable fns are no-ops over a null data pointer.
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &NOOP_VTABLE)) }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use disruptor::BusySpin;
    use disruptor::Polling;
    use disruptor::Producer;
    use disruptor::build_single_producer;

    use super::block_on;

    /// One durability op: a value to "persist" + a channel to report completion.
    /// Stands in for `(entries, IOFlushed callback)` in the real consumer.
    type Op = (u64, mpsc::Sender<u64>);

    /// The disruptor slot. disruptor requires `E: Send + Sync` and only lends
    /// `&event` (never `&mut`), so a move-only payload must live behind a `Sync`
    /// interior-mutability cell. `Mutex<Option<Op>>` fits — and the disruptor's
    /// sequence barrier guarantees the producer never touches a slot the consumer
    /// holds, so the lock is always uncontended. The consumer `take()`s under it.
    struct DurabilityEvent {
        op: std::sync::Mutex<Option<Op>>,
    }

    /// Proves: producer → disruptor ring → reactor-free consumer (block_on an
    /// async "storage" op) → completion fed back, with NO tokio runtime anywhere.
    #[test]
    fn durability_ring_mechanism() {
        let factory = || DurabilityEvent { op: std::sync::Mutex::new(None) };
        let (mut poller, builder) = build_single_producer(64, factory, BusySpin).new_event_poller();
        let mut producer = builder.build();

        // Durability consumer thread: busy-spin the poller, take each op, drive a
        // reactor-free async "append", report the result.
        let consumer = std::thread::spawn(move || {
            loop {
                match poller.poll() {
                    Ok(mut events) => {
                        for event in &mut events {
                            // `event` is `&DurabilityEvent`; take the op out under the
                            // (uncontended) slot lock.
                            let taken = event.op.lock().unwrap().take();
                            if let Some((value, done)) = taken {
                                // Reactor-free: no runtime; the async block completes synchronously.
                                let persisted = block_on(async move { value.wrapping_mul(2) });
                                done.send(persisted).ok();
                            }
                        }
                    }
                    Err(Polling::NoEvents) => std::hint::spin_loop(),
                    Err(Polling::Shutdown) => break,
                }
            }
        });

        // Producer side: publish 3 ops, each carrying its own completion sender.
        let (tx, rx) = mpsc::channel();
        for v in [10u64, 20, 30] {
            let tx = tx.clone();
            producer.publish(move |e| {
                // `e` is `&mut DurabilityEvent`; `get_mut` skips the lock on the
                // exclusively-held slot.
                *e.op.get_mut().unwrap() = Some((v, tx));
            });
        }

        // Collect the 3 completions (order across the ring is FIFO, but sort to be robust).
        let mut got = vec![rx.recv().unwrap(), rx.recv().unwrap(), rx.recv().unwrap()];
        got.sort_unstable();
        assert_eq!(got, vec![20, 40, 60], "each op driven reactor-free and reported back");

        // Dropping the producer shuts the ring down → poller returns Shutdown.
        drop(producer);
        consumer.join().unwrap();
    }
}
