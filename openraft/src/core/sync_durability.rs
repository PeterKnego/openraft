//! Phase 3b.2 — the **log-store consumer**: moves `log_store` write I/O off the
//! (tokio-driven) consensus loop onto a dedicated, **reactor-free** thread.
//!
//! The consumer is the *sole owner* of `log_store` and services **two inputs** each
//! loop iteration:
//!  1. the **disruptor write ring** — the 5 storage write Commands
//!     (`AppendEntries`/`SaveVote`/`PurgeLog`/`TruncateLog`/`SaveCommittedAndApply`),
//!     each driven to completion with the reactor-free [`block_on`] (graduated from the
//!     3b.2 spike); and
//!  2. a **reader-request channel** — because the still-delegated replication path
//!     (`RaftCore::spawn_replication_stream` → `log_store.get_log_reader()`) needs read
//!     handles, and `log_store` now lives here. The consumer vends a fresh reader on
//!     each request via a oneshot.
//!
//! Write completion follows the IO-completion model:
//!  - `Append` is **fire-and-forget** — its `IOFlushed` callback (fired by
//!    `log_store.append`) drives the existing `tx_io_completed` → forwarder →
//!    `Notification::LocalIO` path. No oneshot.
//!  - the other four ops carry a `done` oneshot; the consumer signals it after the
//!    storage call so the consensus loop can run the op's consensus-state after-work.
//!
//! This module is feature-gated (`sync-core`). The reactor-free `block_on` graduates
//! here from `sync_durability_spike.rs` (which is deleted once it is no longer used).

use std::fmt::Debug;
use std::future::Future;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::mpsc;
use std::task::Context;
use std::task::Poll;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Waker;
use std::thread::JoinHandle;

use disruptor::BusySpin;
use disruptor::EventPoller;
use disruptor::Polling;
use disruptor::Producer;
use disruptor::SingleConsumerBarrier;
use disruptor::SingleProducer;
use disruptor::SingleProducerBarrier;
use disruptor::build_single_producer;

use crate::RaftTypeConfig;
use crate::StorageError;
use crate::async_runtime::WatchSender;
use crate::async_runtime::watch::WatchReceiver;
use crate::base::OptionalSend;
use crate::errors::StorageIOResult;
use crate::raft_state::io_state::io_id::IOId;
use crate::storage::IOFlushed;
use crate::storage::RaftLogReader;
use crate::storage::RaftLogStorage;
use crate::type_config::TypeConfigExt;
use crate::type_config::alias::BatchOf;
use crate::type_config::alias::EntryOf;
use crate::type_config::alias::LogIdOf;
use crate::type_config::alias::OneshotSenderOf;
use crate::type_config::alias::VoteOf;
use crate::type_config::alias::WatchReceiverOf;
use crate::type_config::alias::WatchSenderOf;
use crate::type_config::async_runtime::oneshot::OneshotSender;

/// Reactor-free `block_on`: drive `fut` to completion by polling with a no-op waker,
/// never parking. Reactor-free storage impls complete via synchronous operations, so the
/// first poll usually returns `Ready`; a `Pending` simply re-polls (busy-spin), so no
/// runtime/reactor is needed. Graduated from the 3b.2 spike.
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

/// Non-blocking single poll of a future with the reactor-free no-op waker. Returns
/// `Poll::Ready(_)` if the future has already completed, `Poll::Pending` otherwise — it
/// never spins. The synchronous consensus loop uses this to check the shutdown oneshot
/// each iteration without blocking on it.
pub(crate) fn poll_once<F: Future>(mut fut: Pin<&mut F>) -> Poll<F::Output> {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    fut.as_mut().poll(&mut cx)
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

/// The completion channel payload for the four await-completion write ops.
type Done<C> = OneshotSenderOf<C, Result<(), StorageError<C>>>;

/// A reader request: the consumer fulfils it by sending a fresh **gated** reader back.
pub(crate) type ReaderRequest<C, LS> = OneshotSenderOf<C, VendedReader<C, LS>>;

/// The reader type vended to the (delegated) replication path: the storage's own reader
/// wrapped in the readability gate.
pub(crate) type VendedReader<C, LS> = GatedLogReader<C, <LS as RaftLogStorage<C>>::LogReader>;

/// Wraps a `RaftLogStorage::LogReader` with a readability gate. Because append is
/// fire-and-forget onto the durability consumer, an entry may be "submitted" on the
/// consensus loop before the consumer has actually `append`ed it. The
/// `RaftLogStorage::append` contract requires appended entries to be readable the moment
/// `append` returns, so a reader on another thread must not short-read an entry that is
/// in-flight. This wrapper blocks each read until the consumer's `readable` watermark
/// (highest log index currently in the store) covers the requested range, then delegates.
pub(crate) struct GatedLogReader<C, R>
where
    C: RaftTypeConfig,
    R: RaftLogReader<C>,
{
    inner: R,
    /// Highest readable log index currently in the store (`None` = nothing readable). Updated
    /// by the consumer in FIFO op order, so it tracks truncation (it can decrease).
    readable: WatchReceiverOf<C, Option<u64>>,
}

impl<C, R> GatedLogReader<C, R>
where
    C: RaftTypeConfig,
    R: RaftLogReader<C>,
{
    pub(crate) fn new(inner: R, readable: WatchReceiverOf<C, Option<u64>>) -> Self {
        Self { inner, readable }
    }

    /// Block until the watermark covers `hi` (the highest index the read needs). Best-effort:
    /// if the watch sender is gone (consumer shut down) we proceed and let the inner read
    /// reflect final state. Only the END of the range is gated — absence at the start
    /// (purged entries) is a tolerated short read per the reader contract.
    async fn await_readable(&mut self, hi: Option<u64>) {
        if let Some(hi) = hi {
            let _ = self.readable.wait_until_ge(&Some(hi)).await;
        }
    }
}

/// Compute the highest index a range needs, or `None` for an empty/unbounded-end range.
fn range_hi<RB: RangeBounds<u64>>(range: &RB) -> Option<u64> {
    match range.end_bound() {
        Bound::Included(&e) => Some(e),
        Bound::Excluded(&e) => e.checked_sub(1),
        Bound::Unbounded => None,
    }
}

impl<C, R> RaftLogReader<C> for GatedLogReader<C, R>
where
    C: RaftTypeConfig,
    R: RaftLogReader<C>,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<C>>, std::io::Error> {
        self.await_readable(range_hi(&range)).await;
        self.inner.try_get_log_entries(range).await
    }

    async fn limited_get_log_entries(&mut self, start: u64, end: u64) -> Result<Vec<EntryOf<C>>, std::io::Error> {
        self.await_readable(end.checked_sub(1)).await;
        self.inner.limited_get_log_entries(start, end).await
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<C>>, std::io::Error> {
        self.inner.read_vote().await
    }
}

/// One storage write op routed to the consumer.
///
/// All five carry a `done` oneshot the consumer signals once the storage call returns. For
/// `Append`, that return point is *submitted/readable* (per the `RaftLogStorage::append`
/// contract) — distinct from *flushed*, which still flows separately through the `IOFlushed`
/// callback (built from `io_id` + the `tx_io_completed` watch). Awaiting `done` restores the
/// "submitted ⇒ readable before the consensus arm returns" invariant across the
/// consensus→consumer thread boundary (see the `AppendEntries` arm in `sync_core`).
pub(crate) enum DurabilityOp<C>
where C: RaftTypeConfig
{
    Append {
        entries: BatchOf<C, C::Entry>,
        io_id: IOId<C>,
        tx_io_completed: WatchSenderOf<C, Result<IOId<C>, StorageError<C>>>,
        done: Done<C>,
    },
    SaveVote {
        vote: VoteOf<C>,
        done: Done<C>,
    },
    Purge {
        upto: LogIdOf<C>,
        done: Done<C>,
    },
    Truncate {
        after: Option<LogIdOf<C>>,
        done: Done<C>,
    },
    SaveCommitted {
        committed: Option<LogIdOf<C>>,
        done: Done<C>,
    },
}

/// The disruptor ring slot. disruptor requires `E: Send + Sync` and only lends `&event`,
/// so the move-only op lives behind a `Mutex<Option<_>>` (the spike-proven pattern); the
/// sequence barrier keeps the lock uncontended.
pub(crate) struct DurabilityEvent<C>
where C: RaftTypeConfig
{
    op: Mutex<Option<DurabilityOp<C>>>,
}

/// Producer + reader-requester handle, held by `SyncCore`. Dropping it shuts the consumer
/// down (the producer drop signals ring shutdown) and joins the thread.
pub(crate) struct LogStoreHandle<C>
where C: RaftTypeConfig
{
    /// Write-ring producer. `Option` so `Drop` can drop it *before* joining (dropping the
    /// producer is what signals `Polling::Shutdown` to the consumer).
    producer: Option<SingleProducer<DurabilityEvent<C>, SingleConsumerBarrier>>,
    join: Option<JoinHandle<()>>,
}

impl<C> LogStoreHandle<C>
where C: RaftTypeConfig
{
    /// Publish a write op to the consumer (FIFO; preserves storage-op ordering).
    pub(crate) fn publish(&mut self, op: DurabilityOp<C>) {
        let producer = self.producer.as_mut().expect("producer present until shutdown");
        producer.publish(move |slot| {
            // `slot` is `&mut DurabilityEvent`; `get_mut` skips the (uncontended) lock.
            *slot.op.get_mut().unwrap() = Some(op);
        });
    }
}

impl<C> Drop for LogStoreHandle<C>
where C: RaftTypeConfig
{
    fn drop(&mut self) {
        // Drop the producer first → consumer observes `Polling::Shutdown` and exits.
        self.producer.take();
        // Then join, so `log_store` is dropped on the consumer thread before we return.
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

/// Spawn the log-store consumer thread, transferring ownership of `log_store` to it.
///
/// Returns the [`LogStoreHandle`] (write-ring producer + join handle). The caller wires the
/// matching reader-request `Sender` into `RaftCore` (see `raft/mod.rs`).
pub(crate) fn spawn<C, LS>(
    log_store: LS,
    reader_rx: mpsc::Receiver<ReaderRequest<C, LS>>,
) -> LogStoreHandle<C>
where
    C: RaftTypeConfig,
    LS: RaftLogStorage<C>,
{
    let factory = || DurabilityEvent::<C> { op: Mutex::new(None) };
    // Power-of-two ring; generously sized — the consensus loop awaits the 4 non-append ops
    // (so at most one in flight) and the consumer drains appends promptly.
    let (poller, builder) = build_single_producer(1024, factory, BusySpin).new_event_poller();
    let producer = builder.build();

    // Readable watermark: highest log index currently in the store. Vended readers gate on it.
    // The consumer keeps `readable_rx0` alive so that `send()` never silently drops updates
    // before the first `subscribe()` is called: tokio watch's `send()` discards the value if
    // all receivers have been dropped.
    let (readable_tx, readable_rx0) = C::watch_channel::<Option<u64>>(None);

    let join = std::thread::spawn(move || consumer_loop::<C, LS>(log_store, poller, reader_rx, readable_tx, readable_rx0));

    LogStoreHandle {
        producer: Some(producer),
        join: Some(join),
    }
}

/// The reactor-free consumer loop. Owns `log_store`; services reader requests then the
/// write ring each iteration; exits on `Polling::Shutdown` (producer dropped).
///
/// `readable_rx0` is the initial receiver kept alive so that `send()` never silently
/// discards watermark updates before the first `subscribe()` call.
fn consumer_loop<C, LS>(
    mut log_store: LS,
    mut poller: EventPoller<DurabilityEvent<C>, SingleProducerBarrier>,
    reader_rx: mpsc::Receiver<ReaderRequest<C, LS>>,
    readable_tx: WatchSenderOf<C, Option<u64>>,
    _readable_rx0: WatchReceiverOf<C, Option<u64>>,
) where
    C: RaftTypeConfig,
    LS: RaftLogStorage<C>,
{
    // Initialise the watermark from any entries already on disk (e.g. a node restarting
    // with an existing log). Without this, the gate would block replication streams trying
    // to read entries that are already readable in storage, even though no Append op has
    // gone through the consumer yet.
    let init = block_on(log_store.get_log_state())
        .ok()
        .and_then(|s| s.last_log_id)
        .map(|l| l.index());
    readable_tx.send(init).ok();

    loop {
        // (a) Vend gated readers (rare — per replication-stream rebuild). Drain all pending.
        loop {
            match reader_rx.try_recv() {
                Ok(done) => {
                    let reader = block_on(log_store.get_log_reader());
                    done.send(GatedLogReader::new(reader, readable_tx.subscribe())).ok();
                }
                Err(mpsc::TryRecvError::Empty) => break,
                // Senders gone: stop vending; ring shutdown drives the actual exit.
                Err(mpsc::TryRecvError::Disconnected) => break,
            }
        }

        // (b) Drain the write ring.
        let mut did_work = false;
        match poller.poll() {
            Ok(mut events) => {
                for event in &mut events {
                    let taken = event.op.lock().unwrap().take();
                    if let Some(op) = taken {
                        run_op(&mut log_store, op, &readable_tx);
                        did_work = true;
                    }
                }
            }
            Err(Polling::NoEvents) => {}
            Err(Polling::Shutdown) => return,
        }

        if !did_work {
            // Reactor-free idle: yield rather than peg a core, so the many consumer
            // threads in the test suite don't starve the tokio runtime. Latency-irrelevant
            // for correctness; perf tuning is a later phase.
            std::thread::yield_now();
        }
    }
}

/// Execute one write op against the (consumer-owned) `log_store`, reactor-free.
/// Updates `readable_tx` after `Append` and `Truncate` so gated readers know the new watermark.
fn run_op<C, LS>(log_store: &mut LS, op: DurabilityOp<C>, readable_tx: &WatchSenderOf<C, Option<u64>>)
where
    C: RaftTypeConfig,
    LS: RaftLogStorage<C>,
{
    match op {
        DurabilityOp::Append {
            entries,
            io_id,
            tx_io_completed,
            done,
        } => {
            // Extract the last index BEFORE moving io_id into the callback.
            let last_idx = io_id.last_log_id().map(|l| l.index());
            let callback = IOFlushed::new(io_id, tx_io_completed);
            // `append` returns at *submitted/readable* (per the `RaftLogStorage::append`
            // contract); the `callback` drives the later *flush* completion, unchanged.
            // Signal `done` with the submission result so the consensus arm's await unblocks
            // only after the entries are readable. On a submission error the consensus arm's
            // `?` goes Fatal (matching the original `append(..).await.sto_write_logs()?`),
            // and the un-fired callback is simply dropped, as before.
            let res = block_on(log_store.append(entries, callback)).sto_write_logs();
            // FIFO consumer order ⇒ this is the current store high-water for appends.
            if res.is_ok() && let Some(idx) = last_idx {
                readable_tx.send(Some(idx)).ok();
            }
            done.send(res).ok();
        }
        DurabilityOp::SaveVote { vote, done } => {
            let res = block_on(log_store.save_vote(&vote)).sto_write_vote();
            done.send(res).ok();
        }
        DurabilityOp::Purge { upto, done } => {
            let res = block_on(log_store.purge(upto)).sto_write_logs();
            done.send(res).ok();
        }
        DurabilityOp::Truncate { after, done } => {
            let res = block_on(log_store.truncate_after(after.clone())).sto_write_logs();
            if res.is_ok() {
                // Suffix removed ⇒ the high-water drops to the truncation point.
                readable_tx.send(after.map(|l| l.index())).ok();
            }
            done.send(res).ok();
        }
        DurabilityOp::SaveCommitted { committed, done } => {
            let res = block_on(log_store.save_committed(committed)).sto_write();
            done.send(res).ok();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use disruptor::BusySpin;
    use disruptor::Polling;
    use disruptor::Producer;
    use disruptor::build_single_producer;

    use super::GatedLogReader;
    use super::block_on;

    /// Drives a `GatedLogReader` with the no-op-waker `block_on` and a side thread that
    /// advances the watermark. Asserts the inner read does NOT happen until the watermark
    /// reaches the requested range's high index.
    #[test]
    fn gated_reader_blocks_until_watermark_covers_range() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicU64;
        use std::sync::atomic::Ordering;

        use crate::RaftLogReader;
        use crate::async_runtime::WatchSender as _;
        use crate::engine::testing::UTConfig;
        use crate::type_config::TypeConfigExt as _;

        // UTConfig is generic (UTConfig<N = ()>); be explicit to avoid type-inference ambiguity.
        type TC = UTConfig<()>;
        let (tx, rx) = TC::watch_channel::<Option<u64>>(None);

        // Stub inner reader: records the highest index requested; returns empty.
        #[derive(Clone)]
        struct Stub(Arc<AtomicU64>);
        impl RaftLogReader<TC> for Stub {
            async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + crate::base::OptionalSend>(
                &mut self,
                range: RB,
            ) -> Result<Vec<crate::type_config::alias::EntryOf<TC>>, std::io::Error> {
                let end = match range.end_bound() {
                    std::ops::Bound::Included(&e) => e,
                    std::ops::Bound::Excluded(&e) => e.saturating_sub(1),
                    std::ops::Bound::Unbounded => u64::MAX,
                };
                self.0.store(end, Ordering::SeqCst);
                Ok(vec![])
            }

            async fn read_vote(
                &mut self,
            ) -> Result<Option<crate::type_config::alias::VoteOf<TC>>, std::io::Error> {
                Ok(None)
            }
        }

        let observed = Arc::new(AtomicU64::new(0));
        let mut reader = GatedLogReader::new(Stub(observed.clone()), rx);

        // Advance the watermark from another thread after a short spin, then the read should unblock.
        let advancer = std::thread::spawn(move || {
            for _ in 0..1000 {
                std::hint::spin_loop();
            }
            tx.send(Some(5)).ok();
            // Keep tx alive until the reader has observed the value.
            std::thread::sleep(std::time::Duration::from_millis(50));
            drop(tx);
        });

        // block_on drives the gated read; it must wait for watermark>=5 (range 0..6 -> hi=5).
        let res = block_on(reader.try_get_log_entries(0u64..6u64));
        assert!(res.is_ok());
        assert_eq!(observed.load(Ordering::SeqCst), 5, "inner read happened only after the gate opened");
        advancer.join().unwrap();
    }

    /// Mirrors the real consumer's two-input loop shape (a fake write op + a reader
    /// round-trip with a stub store), using the *real* graduated `block_on`. The generic
    /// `consumer_loop`/`run_op` over `RaftLogStorage` is integration-validated by the
    /// 180-test suite; this test pins the ring + reader-channel + reactor-free wiring.
    type WriteOp = (u64, mpsc::Sender<u64>);

    struct Slot {
        op: std::sync::Mutex<Option<WriteOp>>,
    }

    /// Stub "log store": a value the reader-vend doubles; stands in for `get_log_reader`.
    struct StubStore;
    impl StubStore {
        async fn get_reader(&self, seed: u64) -> u64 {
            seed.wrapping_add(1000)
        }
    }

    #[test]
    fn consumer_services_writes_and_reader_requests() {
        let factory = || Slot {
            op: std::sync::Mutex::new(None),
        };
        let (mut poller, builder) = build_single_producer(64, factory, BusySpin).new_event_poller();
        let mut producer = builder.build();

        // Reader-request channel: carries (seed, reply-sender); mirrors the oneshot vend.
        let (reader_tx, reader_rx) = mpsc::channel::<(u64, mpsc::Sender<u64>)>();

        let consumer = std::thread::spawn(move || {
            let store = StubStore;
            loop {
                // (a) reader requests
                loop {
                    match reader_rx.try_recv() {
                        Ok((seed, reply)) => {
                            let r = block_on(store.get_reader(seed));
                            reply.send(r).ok();
                        }
                        Err(mpsc::TryRecvError::Empty) => break,
                        Err(mpsc::TryRecvError::Disconnected) => break,
                    }
                }
                // (b) write ring
                match poller.poll() {
                    Ok(mut events) => {
                        for event in &mut events {
                            let taken = event.op.lock().unwrap().take();
                            if let Some((value, reply)) = taken {
                                let persisted = block_on(async move { value.wrapping_mul(2) });
                                reply.send(persisted).ok();
                            }
                        }
                    }
                    Err(Polling::NoEvents) => std::thread::yield_now(),
                    Err(Polling::Shutdown) => return,
                }
            }
        });

        // Reader round-trip.
        let (rtx, rrx) = mpsc::channel();
        reader_tx.send((7, rtx)).unwrap();
        assert_eq!(rrx.recv().unwrap(), 1007, "reader request vended through the consumer");

        // Write ops round-trip.
        let (wtx, wrx) = mpsc::channel();
        for v in [10u64, 20, 30] {
            let wtx = wtx.clone();
            producer.publish(move |e: &mut Slot| {
                *e.op.get_mut().unwrap() = Some((v, wtx));
            });
        }
        let mut got = vec![wrx.recv().unwrap(), wrx.recv().unwrap(), wrx.recv().unwrap()];
        got.sort_unstable();
        assert_eq!(got, vec![20, 40, 60], "each write op driven reactor-free and reported back");

        // Drop producer + reader sender → consumer observes Shutdown and exits.
        drop(producer);
        drop(reader_tx);
        consumer.join().unwrap();
    }
}
