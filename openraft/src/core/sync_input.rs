//! Phase 3c.2 — the **input ring**: a single disruptor MPSC ring carrying
//! [`Notification<C>`](crate::core::notification::Notification) slots into the synchronous
//! consensus loop ([`SyncCore`](crate::core::SyncCore)).
//!
//! Today every `Notification` (network acks, io-done, tick, sm-apply, vote responses) reaches
//! the consensus loop via the tokio-mpsc `rx_notification`. This module introduces a
//! reactor-free, busy-spin-friendly path: a `build_multi_producer` ring whose [`EventPoller`]
//! is drained by the consensus loop each iteration, and whose [`MultiProducer`] is cloned to
//! the off-loop producers that need to feed the engine.
//!
//! **Task 1 (3c.2)** routes only the **durability io-done** (`LocalIO` / `StorageError`)
//! through this ring: the durability consumer's `IOFlushed` callback / append-failure path
//! publishes directly to the ring, replacing the old `io_completion_forwarder` tokio task +
//! `tx_io_completed` watch channel. Network acks still flow via `tx_notification` until Task 2
//! moves them; so after Task 1 the loop drains BOTH the input ring (io-done) AND
//! `rx_notification` (everything else).
//!
//! ## Why a multi-producer ring
//!
//! The `IOFlushed` callback may fire from a storage flush thread, not the durability-consumer
//! thread. [`MultiProducer`] is `Clone` (Arc-shared, per-clone publish cursor) and built for
//! concurrent producers, so each producer site (the consumer thread + the callback) gets its
//! own clone. Publishes block (busy-spin) when the ring is full; producers are busy-spin
//! threads so that is acceptable. The ring is generously sized (4096).
//!
//! The disruptor only lends `&event`, so the move-only `Notification` lives behind a
//! `Mutex<Option<_>>` (the spike-proven pattern from `sync_durability`); the sequence barrier
//! keeps the lock uncontended.

use std::sync::Mutex;

use disruptor::BusySpin;
use disruptor::EventPoller;
use disruptor::MultiProducer;
use disruptor::MultiProducerBarrier;
use disruptor::Producer;
use disruptor::SingleConsumerBarrier;
use disruptor::build_multi_producer;

use crate::RaftTypeConfig;
use crate::core::notification::Notification;

/// The disruptor ring slot. disruptor requires `E: Send + Sync` and only lends `&event`, so
/// the move-only [`Notification`] lives behind a `Mutex<Option<_>>`; the sequence barrier keeps
/// the lock uncontended.
pub(crate) struct InputEvent<C: RaftTypeConfig> {
    pub(crate) notify: Mutex<Option<Notification<C>>>,
}

/// A producer clone for the input ring. `Clone` (Arc-shared); hand one to each producer site.
///
/// The second type parameter is the *consumer* barrier: after one `new_event_poller()` the
/// `build()` call yields a `SingleConsumerBarrier` (one consumer — the consensus loop).
pub(crate) type InputProducer<C> = MultiProducer<InputEvent<C>, SingleConsumerBarrier>;

/// The single consumer-side poller for the input ring, drained by the consensus loop. Its
/// barrier is the producers' [`MultiProducerBarrier`].
pub(crate) type InputPoller<C> = EventPoller<InputEvent<C>, MultiProducerBarrier>;

/// Build the input ring: a 4096-slot `build_multi_producer` disruptor (min ring size is 64).
/// Returns the consumer-side [`InputPoller`] and one [`InputProducer`]; clone the producer for
/// each additional producer site.
pub(crate) fn build_input_ring<C: RaftTypeConfig>() -> (InputPoller<C>, InputProducer<C>) {
    let factory = || InputEvent::<C> { notify: Mutex::new(None) };
    let (poller, builder) = build_multi_producer(4096, factory, BusySpin).new_event_poller();
    let producer = builder.build();
    (poller, producer)
}

/// Publish one [`Notification`] to the input ring. Helper so call sites do not repeat the
/// move-into-closure pattern; `publish` blocks (busy-spin) while the ring is full.
pub(crate) fn publish_notification<C: RaftTypeConfig>(producer: &mut InputProducer<C>, n: Notification<C>) {
    producer.publish(move |slot| {
        *slot.notify.lock().unwrap() = Some(n);
    });
}

#[cfg(test)]
mod tests {
    use disruptor::Polling;

    use super::build_input_ring;
    use super::publish_notification;
    use crate::core::notification::Notification;
    use crate::engine::testing::UTConfig;

    /// Round-trip the multi-producer input ring: two producer clones publish three `Tick`
    /// notifications between them; the poller (driven on this thread) must drain all three.
    /// MPSC does not guarantee cross-producer order, only per-producer order, so this asserts
    /// set-equality `{1,2,3}`.
    #[test]
    fn input_ring_multi_producer_round_trip() {
        type TC = UTConfig<()>;

        let (mut poller, mut producer_a) = build_input_ring::<TC>();
        let mut producer_b = producer_a.clone();

        // Producer A publishes Tick{1}, Tick{2}; clone B publishes Tick{3}.
        publish_notification(&mut producer_a, Notification::Tick { i: 1 });
        publish_notification(&mut producer_a, Notification::Tick { i: 2 });
        publish_notification(&mut producer_b, Notification::Tick { i: 3 });

        // Drain the poller until all three arrive.
        let mut got: Vec<u64> = Vec::new();
        while got.len() < 3 {
            match poller.poll() {
                Ok(mut events) => {
                    for e in &mut events {
                        if let Some(Notification::Tick { i }) = e.notify.lock().unwrap().take() {
                            got.push(i);
                        }
                    }
                }
                Err(Polling::NoEvents) => std::thread::yield_now(),
                Err(Polling::Shutdown) => break,
            }
        }

        got.sort_unstable();
        assert_eq!(got, vec![1, 2, 3], "all three notifications drained across two producer clones");
    }
}
