//! Phase 3c.1 — the **per-peer network consumer**: a busy-spin, reactor-free port of
//! [`ReplicationCore`](crate::replication::ReplicationCore)'s append + heartbeat send path.
//!
//! This module is the structural mirror of [`sync_durability`](super::sync_durability):
//!  - One **`NetEvent<C>` slot** per ring cell (`Mutex<Option<NetOp<C>>>` — the same
//!    move-only pattern; disruptor only lends `&event`).
//!  - One **`PeerConsumerHandle<C>`** per peer: holds the `SingleProducer` and a
//!    `JoinHandle`; `Drop` drops the producer first (signalling `Polling::Shutdown`) then
//!    joins the thread.
//!  - **`spawn_peer`** creates the ring, enters the tokio runtime context on the thread (the
//!    "hybrid reactor" — so quinn I/O driven via [`block_on`] finds a driver) and runs the
//!    busy-spin [`consumer_loop`] over a [`PeerExecutor`].
//!
//! ## What the executor reproduces (Option 1: bounded per-iteration re-stream)
//!
//! `ReplicationCore::main` is a long-lived async loop driven by watch channels. Here the
//! per-peer thread is instead a **ring-responsive busy-spin consumer**: each loop iteration
//! drains the send ring (`NetOp::Replicate` / `NetOp::Heartbeat`) and then drives **bounded**
//! work reactor-free:
//!  - A `NetOp::Replicate` sets the pending payload + `inflight_id` and drives one
//!    [`PeerExecutor::drive_replicate`] cycle.
//!  - For `LogsSince` (streaming) payloads, the loop re-reads the `io_submitted` / `committed`
//!    watermarks each iteration (via `borrow_watched()`, never blocking) and re-drives the
//!    tail only when there is real progress to make — the **anti-spam gate**
//!    ([`PeerExecutor::should_drive_replicate`]). Heartbeat cadence comes from
//!    `NetOp::Heartbeat` ops (tick → `BroadcastHeartbeat`), not from spinning.
//!  - Entry reads go through the durability consumer's [`GatedLogReader`](super::GatedLogReader)
//!    (blocks until the readability watermark covers the range — the "submitted ⇒ readable"
//!    gate; no extra gate needed). RPCs are driven via the reactor-free [`block_on`].
//!
//! Per-peer FIFO of `ReplicationProgress` is free: a single consumer thread handles each
//! peer's responses strictly in network-response order. In 3c.1 acks are emitted via the
//! existing `tx_notification` channel (drained by `SyncCore`'s `process_notification`); the
//! disruptor input ring is 3c.2.
//!
//! The ack contract is ported method-for-method from `ReplicationCore`
//! (`handle_response_stream` / `notify_progress` / `send_progress_error` /
//! `notify_heartbeat_progress`) and the heartbeat worker
//! ([`HeartbeatWorker`](crate::core::heartbeat::worker)). See those references for the exact
//! conditions; the [`AckEmitter`] below is a faithful transcription.


use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::thread::JoinHandle;
use std::time::Duration;

use disruptor::BusySpin;
use disruptor::EventPoller;
use disruptor::Polling;
use disruptor::Producer;
use disruptor::SingleConsumerBarrier;
use disruptor::SingleProducer;
use disruptor::SingleProducerBarrier;
use disruptor::build_single_producer;
use futures_util::FutureExt;
use futures_util::StreamExt;

use crate::Config;
use crate::RaftTypeConfig;
use crate::StorageError;
use crate::async_runtime::Mutex as _;
use crate::async_runtime::MpscSender as _;
use crate::async_runtime::watch::WatchReceiver as _;
use crate::base::BoxStream;
use crate::base::OptionalSend;
use crate::core::SharedReplicateBatch;
use crate::core::VendedReader;
use crate::core::sync_input;
use crate::core::sync_input::InputProducer;
use crate::core::heartbeat::event::HeartbeatEvent;
use crate::core::notification::Notification;
use crate::core::sm::handle::SnapshotReader;
use crate::core::sync_durability::block_on_yielding;
use crate::errors::HigherVote;
use crate::errors::RPCError;
use crate::errors::ReplicationClosed;
use crate::errors::ReplicationError;
use crate::log_id_range::LogIdRange;
use crate::network::Backoff;
use crate::network::NetBackoff;
use crate::network::NetSnapshot;
use crate::network::NetStreamAppend;
use crate::network::NetTransferLeader;
use crate::network::NetVote;
use crate::network::RPCOption;
use crate::progress::inflight_id::InflightId;
use crate::progress::stream_id::StreamId;
use crate::raft::AppendEntriesRequest;
use crate::raft::StreamAppendError;
use crate::raft::StreamAppendResult;
use crate::raft::message::TransferLeaderRequest;
use crate::raft::message::VoteRequest;
use crate::replication::backoff_state::BackoffState;
use crate::replication::event_watcher::EventWatcher;
use crate::replication::inflight_append_queue::InflightAppendQueue;
use crate::replication::payload::Payload;
use crate::replication::replicate::Replicate;
use crate::replication::replication_context::ReplicationContext;
use crate::replication::response::Progress;
use crate::replication::response::ReplicationResult;
use crate::replication::stream_context::StreamContext;
use crate::replication::stream_state::StreamState;
use crate::storage::RaftLogStorage;
use crate::type_config::TypeConfigExt;
use crate::type_config::alias::CommittedVoteOf;
use crate::type_config::alias::InstantOf;
use crate::type_config::alias::LogIdOf;
use crate::type_config::alias::MpscSenderOf;
use crate::type_config::alias::MutexOf;
use crate::type_config::alias::SnapshotOf;
use crate::type_config::alias::VoteOf;
use crate::type_config::alias::WatchReceiverOf;
use crate::type_config::alias::WatchSenderOf;
use crate::vote::RaftVote;
use crate::vote::raft_vote::RaftVoteExt;

/// One network op routed to a per-peer consumer.
///
/// `Replicate`, `Heartbeat` and `Snapshot` are published to the ring (the Replicate /
/// BroadcastHeartbeat / ReplicateSnapshot arms). Vote / transfer-leader are NOT routed here:
/// they fan out to *all voters* during an election (when there are typically no replication-peer
/// consumers yet, since you are a candidate, not a leader), so SyncCore drives them via a thin
/// off-loop fan-out (`spawn_parallel_vote_requests` / `broadcast_transfer_leader`) using the
/// per-voter [`send_vote_request`] / [`send_transfer_leader_request`] helpers below.
pub(crate) enum NetOp<C>
where C: RaftTypeConfig
{
    /// Replicate log entries to this peer.
    Replicate { req: Replicate<C> },

    /// Send one heartbeat (zero-length AppendEntries) to this peer.
    Heartbeat { event: HeartbeatEvent<C> },

    /// Transmit a snapshot to this peer.
    Snapshot { inflight_id: InflightId },
}

/// The disruptor ring slot for the network consumer.
///
/// `disruptor` requires `E: Send + Sync` and only lends `&event`, so the move-only
/// `NetOp` lives behind a `Mutex<Option<_>>` (same spike-proven pattern as
/// `DurabilityEvent`); the sequence barrier keeps the lock uncontended.
pub(crate) struct NetEvent<C>
where C: RaftTypeConfig
{
    op: Mutex<Option<NetOp<C>>>,
}

/// Cancel/replace signal shared between a peer's producer-side [`PeerConsumerHandle`] and the
/// consumer-side [`PeerExecutor`]'s in-flight snapshot.
///
/// A snapshot send is the only op that can monopolise the consumer thread for a long time
/// (the `full_snapshot` stream runs to completion under `block_on_yielding`), so while it runs
/// the ring is not drained. The reference [`SnapshotTransmitter`](crate::replication::SnapshotTransmitter)
/// runs in its own task and is cancelled via a `cancel_rx` watch that `RaftCore` signals on
/// close/rebuild/replace. Here the equivalent is this shared cell:
///  - `epoch` is bumped by [`PeerConsumerHandle::publish`] for every **replication-intent** op
///    (`Replicate` / `Snapshot`) — a newer such op must preempt an in-flight snapshot (we
///    cannot stream a snapshot and replicate at once). Heartbeats deliberately do NOT bump it;
///    otherwise a heartbeat tick arriving mid-snapshot would perpetually cancel a slow snapshot.
///  - `shutdown` is set by [`PeerConsumerHandle`]'s `Drop` so a leader stepping down / a
///    membership rebuild aborts an in-flight snapshot promptly instead of blocking for its full
///    duration.
///
/// The in-flight snapshot's `cancel` future ([`snapshot_cancel_future`]) — polled by the
/// transport between chunks, exactly as `cancel_rx` is in the reference — fires when either the
/// epoch advances past the value captured at snapshot start, or shutdown is set.
#[derive(Default)]
pub(crate) struct SnapshotCancel {
    epoch: AtomicU64,
    shutdown: AtomicBool,
}

/// Build the `cancel` future for an in-flight snapshot: it becomes `Ready` (cancel) once the
/// shared `epoch` advances past `start_epoch` (a newer Replicate/Snapshot op was published) or
/// `shutdown` is set (the peer handle was dropped). Reactor-free: it is polled by the transport's
/// chunk loop, which `block_on_yielding` re-polls; no waker wiring needed.
fn snapshot_cancel_future(
    cancel: Arc<SnapshotCancel>,
    start_epoch: u64,
) -> impl Future<Output = ReplicationClosed> + OptionalSend + 'static {
    futures_util::future::poll_fn(move |_cx| {
        if cancel.shutdown.load(Ordering::Acquire) || cancel.epoch.load(Ordering::Acquire) != start_epoch {
            Poll::Ready(ReplicationClosed::new("snapshot preempted by a newer op or shutdown"))
        } else {
            Poll::Pending
        }
    })
}

/// Per-peer producer handle, held by `SyncCore`'s `PeerTable`. Dropping it shuts the
/// consumer thread down (producer drop → `Polling::Shutdown`) and joins it.
pub(crate) struct PeerConsumerHandle<C>
where C: RaftTypeConfig
{
    /// Write-ring producer. `Option` so `Drop` can drop it *before* joining — dropping
    /// the producer is what signals `Polling::Shutdown` to the consumer thread.
    producer: Option<SingleProducer<NetEvent<C>, SingleConsumerBarrier>>,
    join: Option<JoinHandle<()>>,

    /// Shared cancel/replace signal for the consumer's in-flight snapshot (see [`SnapshotCancel`]).
    snapshot_cancel: Arc<SnapshotCancel>,
}

impl<C> PeerConsumerHandle<C>
where C: RaftTypeConfig
{
    /// Publish a network op to the peer consumer (FIFO; preserves send ordering).
    pub(crate) fn publish(&mut self, op: NetOp<C>) {
        // A newer replication-intent op preempts an in-flight snapshot on the consumer (it
        // cannot stream a snapshot and process the ring at once). Heartbeats do NOT preempt —
        // see [`SnapshotCancel`]. The op that *starts* a snapshot (`Snapshot`) also bumps here,
        // but `drive_snapshot` captures the epoch AFTER taking it from the ring, so it never
        // self-cancels; only a strictly-later op trips the cancel future.
        if matches!(op, NetOp::Replicate { .. } | NetOp::Snapshot { .. }) {
            self.snapshot_cancel.epoch.fetch_add(1, Ordering::Release);
        }
        let producer = self.producer.as_mut().expect("producer present until shutdown");
        producer.publish(move |slot| {
            // `slot` is `&mut NetEvent`; `get_mut` skips the (uncontended) lock.
            *slot.op.get_mut().unwrap() = Some(op);
        });
    }
}

impl<C> Drop for PeerConsumerHandle<C>
where C: RaftTypeConfig
{
    fn drop(&mut self) {
        // Signal any in-flight snapshot to abort promptly (its `cancel` future observes this),
        // so a stepped-down leader / membership rebuild does not block on a long snapshot. The
        // append/heartbeat drives are already bounded (heartbeat via `C::timeout`); only the
        // snapshot stream needs this explicit nudge.
        self.snapshot_cancel.shutdown.store(true, Ordering::Release);
        // Drop the producer → the consumer observes `Polling::Shutdown` and exits on its next
        // loop iteration.
        self.producer.take();
        // **Detach, do not join.** A drop happens on the consensus thread (e.g. a leader
        // stepping down runs `CloseReplicationStreams` → clears the peer table → drops handles).
        // The consumer only checks the ring for shutdown *between* drives, so it may be mid-wait
        // — a backoff `C::sleep` to an unreachable follower can be hundreds of ms, and a slow RPC
        // longer. Joining here would block the consensus thread for that whole duration, delaying
        // critical work such as a vote flush past the vote-RPC timeout and breaking elections.
        // The original `ReplicationCore` interrupts its waits via a `cancel_rx` select; this
        // bounded port cannot, so it lets the thread finish its current drive and exit on its own
        // (it gets no further ops; the producer is gone). Dropping the `JoinHandle` detaches it.
        drop(self.join.take());
    }
}

/// Map from peer node-id to its consumer handle. Held by `SyncCore`.
pub(crate) type PeerTable<C> = BTreeMap<<C as RaftTypeConfig>::NodeId, PeerConsumerHandle<C>>;

/// Outcome of [`AckEmitter::on_snapshot_error`] — the port of `SnapshotTransmitter::stream_snapshot`'s
/// terminal error match. `Stop` means the snapshot loop ends (already emitted the right
/// notification, if any); `Rpc` carries an RPC error back to `drive_snapshot` for the
/// backoff/retry handling (which needs the network + config, so stays on the executor).
enum SnapshotErrAction<C>
where C: RaftTypeConfig
{
    Stop,
    Rpc(RPCError<C>),
}

/// The ack-emitting half of the per-peer executor.
///
/// Holds only what the ack contract needs (target / leader_vote / stream_id / the
/// notification sender, plus the per-peer matched log id and current inflight id), so the
/// contract can be unit-tested without a log store or network. Every method here is a
/// faithful transcription of `ReplicationCore` / `HeartbeatWorker`; see those references.
struct AckEmitter<C>
where C: RaftTypeConfig
{
    target: C::NodeId,
    leader_vote: CommittedVoteOf<C>,
    stream_id: StreamId,
    /// Disruptor input-ring producer (3c.2). Each per-peer ack is published here instead of
    /// the tokio mpsc `tx_notification`; the consensus loop drains the ring each iteration.
    producer: InputProducer<C>,

    /// The last log id known to match on the follower (`ReplicationProgress::remote_matched`).
    remote_matched: Option<LogIdOf<C>>,

    /// Identifies the current in-flight replication batch for progress tracking.
    inflight_id: Option<InflightId>,
}

impl<C> AckEmitter<C>
where C: RaftTypeConfig
{
    /// Port of `ReplicationCore::handle_response_stream`: consume the response stream,
    /// emitting the exact ack contract per response, strictly in network-response order.
    async fn handle_response_stream<'s>(
        &mut self,
        resp_strm: BoxStream<'s, Result<StreamAppendResult<C>, RPCError<C>>>,
        inflight_queue: InflightAppendQueue<C>,
        backoff_state: &mut BackoffState,
    ) -> Result<(), &'static str> {
        let mut resp_strm = std::pin::pin!(resp_strm);

        while let Some(rpc_res) = resp_strm.next().await {
            tracing::debug!("AppendEntries RPC response: {:?}", rpc_res);

            backoff_state.observe(&rpc_res);
            let append_res = match rpc_res {
                Ok(stream_append_res) => stream_append_res,
                Err(rpc_err) => {
                    self.send_progress_error(rpc_err, "stream-replication");
                    return Err("RPCError");
                }
            };

            match append_res {
                Ok(matching) => {
                    let last_acked_sending_time = inflight_queue.drain_acked(&matching);

                    if let Some(last) = last_acked_sending_time {
                        self.notify_heartbeat_progress(last);
                    }

                    self.remote_matched = matching.clone();

                    self.notify_progress(ReplicationResult(Ok(matching)));
                }
                Err(append_err) => {
                    match append_err {
                        StreamAppendError::Conflict(conflict_log_id) => {
                            self.notify_progress(ReplicationResult(Err(conflict_log_id)));
                        }
                        StreamAppendError::HigherVote(higher) => {
                            sync_input::publish_notification(
                                &mut self.producer,
                                Notification::HigherVote {
                                    target: self.target.clone(),
                                    higher,
                                    leader_vote: self.leader_vote.clone(),
                                },
                            );
                        }
                    }

                    return Err("AppendError");
                }
            }
        }
        Ok(())
    }

    /// Port of `ReplicationCore::send_progress_error`: report an RPC error to the engine,
    /// but only when a payload is in flight (`inflight_id.is_some()`).
    fn send_progress_error(&mut self, err: RPCError<C>, when: impl fmt::Display) {
        tracing::warn!("peer executor recv RPCError: {}, when:({})", err, when);

        // No inflight id ⇒ no payload was sent and nobody is waiting, no need to report.
        if self.inflight_id.is_none() {
            return;
        }
        sync_input::publish_notification(
            &mut self.producer,
            Notification::ReplicationProgress {
                progress: Progress {
                    target: self.target.clone(),
                    result: Err(err.to_string()),
                },
                inflight_id: self.inflight_id,
            },
        );
    }

    /// Port of `ReplicationCore::notify_heartbeat_progress`: a successful replication round-trip
    /// implies a successful heartbeat.
    fn notify_heartbeat_progress(&mut self, sending_time: InstantOf<C>) {
        sync_input::publish_notification(
            &mut self.producer,
            Notification::HeartbeatProgress {
                stream_id: self.stream_id,
                target: self.target.clone(),
                sending_time,
            },
        );
    }

    /// Port of `ReplicationCore::notify_progress`: emit a `ReplicationProgress` for a match or
    /// conflict. Crucially, a successful match with `matching.is_none()` emits **nothing**; a
    /// conflict always emits (even with `inflight_id == None`).
    fn notify_progress(&mut self, replication_result: ReplicationResult<C>) {
        match &replication_result.0 {
            Ok(matching) => {
                self.remote_matched = matching.clone();

                // No need to notify.
                if matching.is_none() {
                    return;
                }
            }
            Err(_conflict) => {
                // Conflict is not allowed to be less than the current matching.
            }
        }

        // Always send Conflict error back, even when the inflight id is None, so heartbeat can
        // detect log reversion.
        sync_input::publish_notification(
            &mut self.producer,
            Notification::ReplicationProgress {
                progress: Progress {
                    target: self.target.clone(),
                    result: Ok(replication_result.clone()),
                },
                // If None, it is not a response to a request with payload.
                inflight_id: self.inflight_id,
            },
        );
    }

    /// Port of `HeartbeatWorker::handle_stream_result`: turn one heartbeat round-trip result
    /// into the heartbeat ack contract.
    fn handle_heartbeat_result(&mut self, result: StreamAppendResult<C>, heartbeat: &HeartbeatEvent<C>) {
        match result {
            Ok(_) => {
                self.send_heartbeat_progress(heartbeat);
            }
            Err(StreamAppendError::HigherVote(vote)) => {
                tracing::debug!("seen a higher vote from {}; when:(sending heartbeat)", self.target);
                sync_input::publish_notification(
                    &mut self.producer,
                    Notification::HigherVote {
                        target: self.target.clone(),
                        higher: vote,
                        leader_vote: self.leader_vote.clone(),
                    },
                );
                // Higher vote means leadership is not granted; do not send HeartbeatProgress.
            }
            Err(StreamAppendError::Conflict(_conflict_log_id)) => {
                // The follower does not have `matching`. Use `matching` as the conflict point —
                // safe unwrap(): a None never conflicts.
                let conflict_log_id = heartbeat.matching.clone().unwrap();

                sync_input::publish_notification(
                    &mut self.producer,
                    Notification::ReplicationProgress {
                        progress: Progress {
                            target: self.target.clone(),
                            result: Ok(ReplicationResult(Err(conflict_log_id))),
                        },
                        inflight_id: None,
                    },
                );
                self.send_heartbeat_progress(heartbeat);
            }
        }
    }

    fn send_heartbeat_progress(&mut self, heartbeat: &HeartbeatEvent<C>) {
        sync_input::publish_notification(
            &mut self.producer,
            Notification::HeartbeatProgress {
                stream_id: self.stream_id,
                sending_time: heartbeat.time,
                target: self.target.clone(),
            },
        );
    }

    // ---- Snapshot emits (port of `SnapshotTransmitter`) -----------------------------------

    /// Port of `SnapshotTransmitter::send_snapshot`: transmit one full snapshot and, on
    /// success, emit the snapshot ack contract — `HeartbeatProgress` (a successful round-trip
    /// is also a heartbeat) then `ReplicationProgress{Ok(Ok(meta.last_log_id)), inflight_id =
    /// Some(id)}`. A higher vote in the response returns `Err(HigherVote)` (no emits here — the
    /// caller routes it through [`on_snapshot_error`](Self::on_snapshot_error)).
    ///
    /// `network` is taken as `&mut N` (the executor lends its `network` local) so this method
    /// stays free of the executor's `LS`/`SM` generics and is unit-testable with a stub
    /// `NetSnapshot`.
    async fn send_snapshot<N>(
        &mut self,
        network: &mut N,
        snapshot: SnapshotOf<C>,
        inflight_id: InflightId,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<(), ReplicationError<C>>
    where
        N: NetSnapshot<C>,
    {
        let meta = snapshot.meta.clone();
        let sender_vote: VoteOf<C> = self.leader_vote.clone().into_vote();

        let start_time = C::now();

        let resp = network.full_snapshot(sender_vote.clone(), snapshot, cancel, option).await?;

        tracing::info!("finished sending full_snapshot, resp: {}", resp);

        // Handle response conditions.
        if resp.vote.as_ref_vote() > sender_vote.as_ref_vote() {
            return Err(ReplicationError::HigherVote(HigherVote {
                higher: resp.vote,
                sender_vote,
            }));
        }

        self.notify_heartbeat_progress(start_time);
        self.notify_snapshot_progress(meta.last_log_id, inflight_id);
        Ok(())
    }

    /// Port of `SnapshotTransmitter::notify_progress`: emit the post-snapshot
    /// `ReplicationProgress`. Unlike [`notify_progress`](Self::notify_progress) (the replication
    /// path), this has **no** `matching.is_none()` guard and always carries `inflight_id =
    /// Some(id)` — a snapshot send always reports its matching to the engine.
    fn notify_snapshot_progress(&mut self, matching: Option<LogIdOf<C>>, inflight_id: InflightId) {
        sync_input::publish_notification(
            &mut self.producer,
            Notification::ReplicationProgress {
                progress: Progress {
                    target: self.target.clone(),
                    result: Ok(ReplicationResult(Ok(matching))),
                },
                inflight_id: Some(inflight_id),
            },
        );
    }

    /// Port of `SnapshotTransmitter::stream_snapshot`'s terminal error match. `Closed` →
    /// silently stop; `HigherVote` → emit `HigherVote`, stop; `StorageError` → emit
    /// `StorageError` (fatal), stop; `RPCError` → hand back to the executor for backoff/retry.
    fn on_snapshot_error(&mut self, error: ReplicationError<C>) -> SnapshotErrAction<C> {
        match error {
            ReplicationError::Closed(closed) => {
                tracing::info!("snapshot transmission canceled: {}", closed);
                SnapshotErrAction::Stop
            }
            ReplicationError::HigherVote(h) => {
                tracing::info!("snapshot transmission aborted, higher vote seen: {}", h);
                sync_input::publish_notification(
                    &mut self.producer,
                    Notification::HigherVote {
                        target: self.target.clone(),
                        higher: h.higher,
                        leader_vote: self.leader_vote.clone(),
                    },
                );
                SnapshotErrAction::Stop
            }
            ReplicationError::StorageError(error) => {
                tracing::error!("error replication to target: {}, error: {}", self.target, error);
                sync_input::publish_notification(&mut self.producer, Notification::StorageError { error });
                SnapshotErrAction::Stop
            }
            ReplicationError::RPCError(err) => SnapshotErrAction::Rpc(err),
        }
    }
}

/// The per-peer executor: a reactor-free port of `ReplicationCore`'s append + heartbeat path
/// plus `SnapshotTransmitter`'s snapshot path.
pub(crate) struct PeerExecutor<C, N, LS, SM>
where
    C: RaftTypeConfig,
    N: NetStreamAppend<C> + NetBackoff<C> + NetSnapshot<C>,
    LS: RaftLogStorage<C>,
{
    /// Ack-emitting state + the ported ack-contract methods.
    ack: AckEmitter<C>,

    /// Shared state for generating the AppendEntries request stream (owns the gated reader).
    /// Mirrors `ReplicationCore::stream_state`.
    stream_state: Arc<MutexOf<C, StreamState<C, LS>>>,

    /// Watch receivers for current leader state (read non-blocking via `borrow_watched()`).
    event_watcher: EventWatcher<C>,

    /// The network client to this peer. `Option` so a drive can `take()` it into a local —
    /// keeping the `stream_append` borrow off `self` so the surrounding self-mutations
    /// (`clear_pending`, ack emits) don't conflict (mirrors `ReplicationCore`'s
    /// `let mut network = self.network.take()`). Restored before the drive returns.
    network: Option<N>,

    /// The pending replication payload (the `next_action` analog). `LogsSince` persists across
    /// loop iterations (streaming); `LogIdRange` persists only while partially sent.
    payload: Option<Payload<C>>,

    /// The last committed log id this executor has propagated (gates `LogsSince` re-drive).
    leader_committed: Option<LogIdOf<C>>,

    /// Backoff state for rate-limiting retries on persistent RPC errors.
    backoff_state: BackoffState,

    config: Arc<Config>,

    /// Snapshot reader (handle to the state machine worker), used by the snapshot path to fetch
    /// the current snapshot — the port of `SnapshotTransmitter::snapshot_reader`. Obtained from
    /// `sm_handle.new_snapshot_reader()` when the peer executor is spawned.
    snapshot_reader: SnapshotReader<C, SM>,

    /// Shared cancel/replace signal for the in-flight snapshot (see [`SnapshotCancel`]). A clone
    /// of the same `Arc` lives in this peer's [`PeerConsumerHandle`].
    snapshot_cancel: Arc<SnapshotCancel>,

    /// Keep the cancel/replicate watch senders alive so the receivers held inside
    /// `stream_state` / `event_watcher` stay open (the LogsSince/backoff `select!` paths in
    /// `StreamState` borrow them, though the bounded port never blocks on them).
    _cancel_tx: WatchSenderOf<C, ()>,
    _replicate_tx: WatchSenderOf<C, Replicate<C>>,
}

impl<C, N, LS, SM> PeerExecutor<C, N, LS, SM>
where
    C: RaftTypeConfig,
    N: NetStreamAppend<C> + NetBackoff<C> + NetSnapshot<C>,
    LS: RaftLogStorage<C>,
    SM: 'static,
{
    /// Build a peer executor. Called on the consensus thread (in `SyncCore::run_command`),
    /// then moved onto the per-peer consumer thread by [`spawn_peer`].
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id: C::NodeId,
        target: C::NodeId,
        leader_vote: CommittedVoteOf<C>,
        stream_id: StreamId,
        config: Arc<Config>,
        tx_notify: MpscSenderOf<C, Notification<C>>,
        producer: InputProducer<C>,
        network: N,
        log_reader: VendedReader<C, LS>,
        committed_rx: WatchReceiverOf<C, Option<LogIdOf<C>>>,
        io_accepted_rx: WatchReceiverOf<C, crate::raft_state::IOId<C>>,
        io_submitted_rx: WatchReceiverOf<C, crate::raft_state::IOId<C>>,
        replicate_batch: SharedReplicateBatch,
        remote_matched: Option<LogIdOf<C>>,
        snapshot_reader: SnapshotReader<C, SM>,
    ) -> Self {
        let (cancel_tx, cancel_rx) = C::watch_channel(());
        let (replicate_tx, replicate_rx) = C::watch_channel(Replicate::default());

        let replication_context = ReplicationContext {
            id,
            target: target.clone(),
            leader_vote: leader_vote.clone(),
            stream_id,
            config: config.clone(),
            tx_notify: tx_notify.clone(),
            cancel_rx,
            replicate_batch,
        };

        let event_watcher = EventWatcher {
            replicate_rx,
            committed_rx,
            io_accepted_rx,
            io_submitted_rx,
        };

        let backoff_state = BackoffState::new();

        let stream_state = Arc::new(MutexOf::<C, _>::new(StreamState {
            replication_context,
            event_watcher: event_watcher.clone(),
            log_reader,
            payload: None,
            inflight_id: None,
            leader_committed: None,
            backoff_consumer: backoff_state.consumer(),
            empty_read_count: 0,
            escalated_inflight: None,
            end_session_unservable: false,
        }));

        let ack = AckEmitter {
            target,
            leader_vote,
            stream_id,
            producer,
            remote_matched,
            inflight_id: None,
        };

        Self {
            ack,
            stream_state,
            event_watcher,
            network: Some(network),
            payload: None,
            leader_committed: None,
            backoff_state,
            config,
            snapshot_reader,
            snapshot_cancel: Arc::new(SnapshotCancel::default()),
            _cancel_tx: cancel_tx,
            _replicate_tx: replicate_tx,
        }
    }

    /// A clone of this executor's snapshot cancel/replace signal, for the producer-side
    /// [`PeerConsumerHandle`] to share (see [`SnapshotCancel`]).
    fn snapshot_cancel(&self) -> Arc<SnapshotCancel> {
        self.snapshot_cancel.clone()
    }

    /// Dispatch one ring op into the executor (mirrors `consumer_loop`'s `run_op`).
    async fn run_op(&mut self, op: NetOp<C>) {
        match op {
            NetOp::Replicate { req } => {
                // Mirrors `drain_events`' entries branch: set inflight + payload, then drive.
                self.ack.inflight_id = Some(req.inflight_id);
                self.payload = Some(req.payload);
                let _ = self.drive_replicate().await;
            }
            NetOp::Heartbeat { event } => {
                self.drive_heartbeat(event).await;
            }
            NetOp::Snapshot { inflight_id } => {
                self.drive_snapshot(inflight_id).await;
            }
        }
    }

    /// The anti-spam gate for the streaming (`LogsSince`) / partial-`LogIdRange` re-drive.
    ///
    /// Returns true only when there is real progress to make without a fresh op:
    ///  - a partially-sent `LogIdRange` still has entries left, or
    ///  - a `LogsSince` tail advanced (`io_submitted` beyond `remote_matched`) or `committed`
    ///    advanced beyond what we last propagated.
    ///
    /// Otherwise the loop must yield rather than fire an empty RPC every iteration.
    fn should_drive_replicate(&self) -> bool {
        let Some(payload) = &self.payload else {
            return false;
        };
        // A stale executor (old leader) makes no progress; let it idle until its handle is
        // dropped (Close/Rebuild), rather than hot-looping `drive_replicate`'s no-op guard.
        if self.event_watcher.io_accepted_rx.borrow_watched().leader_id() != self.ack.leader_vote.leader_id() {
            return false;
        }
        match payload {
            // A leftover (partially-sent) fixed range: keep sending the remainder.
            Payload::LogIdRange { .. } => true,
            Payload::LogsSince { .. } => {
                let io_last = self.event_watcher.io_submitted_rx.borrow_watched().last_log_id().cloned();
                let committed = self.event_watcher.committed_rx.borrow_watched().clone();
                io_last > self.ack.remote_matched || committed > self.leader_committed
            }
        }
    }

    /// One bounded replication drive cycle (port of `ReplicationCore::main`'s loop body with a
    /// bounded, non-blocking payload). Sends the currently-available range as a `stream_append`,
    /// handles all responses (emitting the ack contract), and advances local state.
    async fn drive_replicate(&mut self) -> Result<(), ReplicationClosed> {
        // Leader-change guard (mirrors `main`/`next_request`): a stale executor for an old
        // leader does nothing; lifecycle (Close/Rebuild) drops its handle.
        {
            let accepted_io = self.event_watcher.io_accepted_rx.borrow_watched().clone();
            if accepted_io.leader_id() != self.ack.leader_vote.leader_id() {
                return Ok(());
            }
        }

        let Some(orig_payload) = self.payload.clone() else {
            return Ok(());
        };

        let committed = self.event_watcher.committed_rx.borrow_watched().clone();

        // Bound the payload for this drive: convert `LogsSince` to a fixed range up to the
        // current `io_submitted` watermark so the request stream ends instead of blocking.
        let effective = match &orig_payload {
            Payload::LogIdRange { log_id_range } => log_id_range.clone(),
            Payload::LogsSince { prev } => {
                let io_last = self.event_watcher.io_submitted_rx.borrow_watched().last_log_id().cloned();
                // `prev` arrives via the op while `io_last` is observed via the watch, which may
                // lag — never produce a reversed range.
                let last = std::cmp::max(io_last, prev.clone());
                LogIdRange::new(prev.clone(), last)
            }
        };

        // Take the network into a local so its `stream_append` borrow does not tie up `self`
        // (mirrors `ReplicationCore`'s `let mut network = self.network.take()`); restored before
        // returning. All `self`-mutations below are then free of the network borrow.
        let mut network = self.network.take().expect("network present until executor drop");

        // Reconcile backoff before the stream session (mirrors `main`).
        let config = self.config.clone();
        self.backoff_state.reconcile(|| network.backoff().unwrap_or_else(|| config.build_backoff()));

        let mut payload_local = Payload::LogIdRange {
            log_id_range: effective,
        };

        {
            let mut stream_state = self.stream_state.lock().await;
            stream_state.payload = Some(payload_local.clone());
            stream_state.inflight_id = self.ack.inflight_id;
            stream_state.leader_committed = committed.clone();
            // Fresh payload = fresh read attempt (see ReplicationCore::main).
            stream_state.empty_read_count = 0;
        }

        let inflight_queue = InflightAppendQueue::new();
        let fatal_error = Arc::new(MutexOf::<C, _>::new(None));

        let stream_context = StreamContext {
            stream_state: self.stream_state.clone(),
            inflight_append_queue: inflight_queue.clone(),
            fatal_error: fatal_error.clone(),
        };

        let req_strm = Self::new_request_stream(stream_context);

        let rpc_timeout = Duration::from_millis(self.config.heartbeat_interval);
        let option = RPCOption::new(rpc_timeout);

        // Scope the response stream (its Ok variant borrows `network`) inside this block so the
        // borrow is released before the tail restore (`self.network = Some(network)`).
        let outcome: Result<(), ReplicationClosed> = 'drive: {
            let resp_strm_res = network.stream_append(req_strm, option).await;

            // A custom transport may poll the request stream while establishing `stream_append()`
            // and then still return an RPC error — check the fatal marker here too.
            if let Some(err) = take_stream_fatal_error::<C>(&fatal_error).await {
                self.clear_pending();
                break 'drive Err(err);
            }

            let resp_strm = match resp_strm_res {
                Ok(resp_strm) => resp_strm,
                Err(rpc_err) => {
                    self.backoff_state.on_error(rpc_err.backoff_rank());
                    self.ack.send_progress_error(rpc_err, "initiate-stream-replication");
                    // Engine resets inflight to None on the error notification and re-drives.
                    self.clear_pending();
                    break 'drive Ok(());
                }
            };

            let res = self.ack.handle_response_stream(resp_strm, inflight_queue, &mut self.backoff_state).await;

            if let Some(err) = take_stream_fatal_error::<C>(&fatal_error).await {
                self.clear_pending();
                break 'drive Err(err);
            }

            if res.is_ok() {
                // The stream was exhausted successfully. Advance the payload toward the new matching.
                payload_local.update_matching(self.ack.remote_matched.clone());
                match &orig_payload {
                    Payload::LogIdRange { .. } => {
                        if payload_local.len() == Some(0) {
                            // Fully sent.
                            self.clear_pending();
                        } else {
                            // Partial success: keep the remaining range and re-drive next iteration.
                            self.payload = Some(payload_local);
                        }
                    }
                    Payload::LogsSince { .. } => {
                        // Streaming: advance `prev` and keep the same inflight id; record committed.
                        self.payload = Some(Payload::LogsSince {
                            prev: self.ack.remote_matched.clone(),
                        });
                        self.leader_committed = committed;
                    }
                }
            } else {
                // Conflict / HigherVote / RPCError: the engine resets inflight and re-drives.
                self.clear_pending();
            }

            Ok(())
        };

        // Restore the network for the next drive.
        self.network = Some(network);
        outcome
    }

    /// Clear the pending payload + inflight id so the gate stops re-driving until the engine
    /// emits the next op.
    fn clear_pending(&mut self) {
        self.payload = None;
        self.ack.inflight_id = None;
    }

    /// One heartbeat drive (port of `HeartbeatWorker::do_run`'s body): a single zero-length
    /// AppendEntries with `prev_log_id == matching`, then the heartbeat ack contract.
    async fn drive_heartbeat(&mut self, heartbeat: HeartbeatEvent<C>) {
        let timeout = Duration::from_millis(self.config.heartbeat_interval);
        let option = RPCOption::new(timeout);

        let payload = AppendEntriesRequest {
            vote: self.ack.leader_vote.clone().into_vote(),
            // Use last known matching log id as prev_log_id to detect follower reversion.
            prev_log_id: heartbeat.matching.clone(),
            leader_commit: heartbeat.cluster_committed.clone(),
            entries: vec![],
        };

        let input_stream = Box::pin(futures_util::stream::once(async { payload }));

        // Take the network local (see `drive_replicate`) so the RPC borrow doesn't tie up `self`.
        let mut network = self.network.take().expect("network present until executor drop");
        // Mirror HeartbeatWorker::do_run: wrap in C::timeout so an unreachable peer cannot
        // block this consumer indefinitely (a hung stream_append would park the thread in
        // block_on_yielding, preventing all ring ops and shutdown from being processed).
        let res = C::timeout(timeout, async {
            let mut output = network.stream_append(input_stream, option).await?;
            output.next().await.transpose()
        })
        .await;
        self.network = Some(network);

        tracing::debug!("peer {} sent heartbeat: result: {:?}", self.ack.target, res);

        match res {
            Ok(Ok(Some(stream_result))) => {
                self.ack.handle_heartbeat_result(stream_result, &heartbeat);
            }
            Ok(Ok(None)) => {
                tracing::warn!("heartbeat stream to {} returned no response", self.ack.target);
            }
            other => {
                tracing::warn!("failed to send heartbeat to {} (timeout or error): {:?}", self.ack.target, other);
            }
        }
    }

    /// Transmit a snapshot to this peer — the port of `SnapshotTransmitter::stream_snapshot`'s
    /// retry loop. Runs on the peer consumer thread (so it serialises with this peer's
    /// append/heartbeat drives, unlike the reference's separate task). Responsiveness to a newer
    /// op / shutdown comes from the [`SnapshotCancel`] signal woven into the `cancel` future the
    /// transport polls between chunks (and into the backoff `select!`), so a long `full_snapshot`
    /// does not stall the ring drain indefinitely.
    async fn drive_snapshot(&mut self, inflight_id: InflightId) {
        // Capture the epoch AFTER this `Snapshot` op was taken from the ring: a strictly-later
        // Replicate/Snapshot op (or shutdown) trips the cancel future; this op itself does not.
        let start_epoch = self.snapshot_cancel.epoch.load(Ordering::Acquire);

        // Backoff policy on `Unreachable`; reset on a non-unreachable RPC error (port of the
        // reference's `self.backoff`).
        let mut backoff: Option<Backoff> = None;

        let mut ith: i32 = -1;
        loop {
            ith += 1;

            let res = self.read_and_send_snapshot(inflight_id, ith, start_epoch).await;

            let error = match res {
                Ok(()) => return,
                Err(error) => error,
            };

            tracing::error!("ReplicationError while sending snapshot: {}", error);

            let err = match self.ack.on_snapshot_error(error) {
                SnapshotErrAction::Stop => return,
                SnapshotErrAction::Rpc(err) => err,
            };

            // Port of `stream_snapshot`'s RPCError branch: set/reset backoff, then wait (racing
            // the cancel signal) before retrying.
            match &err {
                RPCError::Unreachable(_unreachable) => {
                    if backoff.is_none() {
                        let config = self.config.clone();
                        let net = self.network.as_ref().expect("network present until executor drop");
                        backoff = Some(net.backoff().unwrap_or_else(|| config.build_backoff()));
                    }
                }
                RPCError::Timeout(_) | RPCError::Network(_) | RPCError::RemoteError(_) => {
                    backoff = None;
                }
            }

            if let Some(b) = &mut backoff {
                let duration = b.next().unwrap_or_else(|| {
                    tracing::warn!("backoff exhausted, using default");
                    Duration::from_millis(500)
                });

                let sleep = C::sleep(duration);
                let cancel = snapshot_cancel_future(self.snapshot_cancel.clone(), start_epoch);

                futures_util::select! {
                    _ = sleep.fuse() => {
                        tracing::debug!("snapshot backoff timeout");
                    }
                    _ = cancel.fuse() => {
                        tracing::info!("snapshot transmission canceled during backoff");
                        return;
                    }
                }
            }
        }
    }

    /// Port of `SnapshotTransmitter::read_and_send_snapshot`: fetch the current snapshot from the
    /// state machine, then send it. A missing snapshot is a fatal storage error (mirrors the
    /// reference). The `cancel` future is built fresh per attempt so each `full_snapshot` polls a
    /// live signal.
    async fn read_and_send_snapshot(
        &mut self,
        inflight_id: InflightId,
        ith: i32,
        start_epoch: u64,
    ) -> Result<(), ReplicationError<C>> {
        let snapshot = self.snapshot_reader.get_snapshot().await.map_err(|reason| {
            tracing::warn!("failed to get snapshot from state machine: {}", reason);
            ReplicationClosed::new(reason)
        })?;

        tracing::info!("{}-th snapshot sending", ith);

        let snapshot = match snapshot {
            None => {
                let sto_err = StorageError::read_snapshot(None, C::err_from_string("snapshot not found"));
                return Err(sto_err.into());
            }
            Some(x) => x,
        };

        let mut option = RPCOption::new(self.config.install_snapshot_timeout());
        option.snapshot_chunk_size = Some(self.config.snapshot_max_chunk_size as usize);

        let cancel = snapshot_cancel_future(self.snapshot_cancel.clone(), start_epoch);

        // Take the network into a local (see `drive_replicate`) so the `full_snapshot` borrow
        // does not tie up `self` for the ack emits; restored before returning.
        let mut network = self.network.take().expect("network present until executor drop");
        let result = self.ack.send_snapshot(&mut network, snapshot, inflight_id, cancel, option).await;
        self.network = Some(network);
        result
    }

    /// Port of `ReplicationCore::new_request_stream`.
    fn new_request_stream(stream_context: StreamContext<C, LS>) -> BoxStream<'static, AppendEntriesRequest<C>> {
        let strm = futures_util::stream::unfold(stream_context, Self::next_append_request);
        Box::pin(strm)
    }

    /// Port of `ReplicationCore::next_append_request`.
    async fn next_append_request(
        stream_context: StreamContext<C, LS>,
    ) -> Option<(AppendEntriesRequest<C>, StreamContext<C, LS>)> {
        let res = {
            let mut state = stream_context.stream_state.as_ref().lock().await;
            state.next_request().await
        };

        let req = match res {
            Ok(Some(req)) => req,
            Ok(None) => return None,
            Err(err) => {
                let mut fatal_error = stream_context.fatal_error.lock().await;
                *fatal_error = Some(err);
                return None;
            }
        };

        stream_context.inflight_append_queue.push(req.last_log_id());

        Some((req, stream_context))
    }
}

/// Port of `ReplicationCore::take_stream_fatal_error`.
async fn take_stream_fatal_error<C>(
    fatal_error: &Arc<MutexOf<C, Option<ReplicationClosed>>>,
) -> Option<ReplicationClosed>
where C: RaftTypeConfig {
    let mut fatal_error = fatal_error.lock().await;
    fatal_error.take()
}

/// Selects whether [`send_vote_request`] sends a real Vote or a Pre-Vote RPC (port of
/// `RaftCore`'s private `VoteRequestKind`).
#[derive(Clone, Copy)]
pub(crate) enum VoteRequestKind {
    Vote,
    PreVote,
}

impl VoteRequestKind {
    /// Lowercase label used in log messages: `"vote"` or `"pre-vote"`.
    fn as_str(self) -> &'static str {
        match self {
            VoteRequestKind::Vote => "vote",
            VoteRequestKind::PreVote => "pre-vote",
        }
    }
}

/// Send one (pre-)vote RPC to `target` and emit the response notification — the per-voter body of
/// `RaftCore::spawn_parallel_vote_requests` (`raft_core.rs`), ported as a standalone, off-loop
/// helper that `SyncCore::spawn_parallel_vote_requests` fans out via `C::spawn` (one task per
/// voter). It owns nothing of the executor's generics, so it is unit-testable with a stub
/// [`NetVote`].
///
/// Contract (faithful to the reference):
///  - On `Ok(resp)`: emit `Notification::VoteResponse{target,resp,candidate_vote}` (or
///    `PreVoteResponse` for [`VoteRequestKind::PreVote`]), where `candidate_vote` is the request's
///    vote downgraded to non-committed.
///  - On timeout or transport `Err`: emit **nothing**. A transport failure — including
///    `Unreachable` from a genuinely partitioned peer — is **not** a grant; otherwise an isolated
///    node could synthesize a quorum and inflate its term.
pub(crate) async fn send_vote_request<C, N>(
    mut client: N,
    target: C::NodeId,
    req: VoteRequest<C>,
    kind: VoteRequestKind,
    ttl: Duration,
    tx_notify: MpscSenderOf<C, Notification<C>>,
) where
    C: RaftTypeConfig,
    N: NetVote<C>,
{
    let vote = req.vote.clone();
    let option = RPCOption::new(ttl);

    let tm_res = match kind {
        VoteRequestKind::Vote => C::timeout(ttl, client.vote(req, option)).await,
        VoteRequestKind::PreVote => C::timeout(ttl, client.pre_vote(req, option)).await,
    };

    let res = match tm_res {
        Ok(res) => res,
        Err(_timeout) => {
            tracing::error!("timeout while requesting {} from target {}", kind.as_str(), target);
            return;
        }
    };

    match res {
        Ok(resp) => {
            let candidate_vote = vote.into_non_committed();
            let notification = match kind {
                VoteRequestKind::Vote => Notification::VoteResponse {
                    target,
                    resp,
                    candidate_vote,
                },
                VoteRequestKind::PreVote => Notification::PreVoteResponse {
                    target,
                    resp,
                    candidate_vote,
                },
            };
            tx_notify.send(notification).await.ok();
        }
        // A transport failure is not a grant: a partitioned peer must not count toward the
        // (Pre-)Vote quorum (see the reference's note). A network without `pre_vote` returns
        // `Ok(granted)` from the default impl, so Pre-Vote degrades to a no-op rather than relying
        // on this branch.
        Err(err) => {
            tracing::error!("while requesting {}, error: {}, target: {}", kind.as_str(), err, target);
        }
    }
}

/// Send one leadership-transfer RPC to `target` — the per-voter body of
/// `RaftCore::broadcast_transfer_leader`, ported as a standalone off-loop helper that
/// `SyncCore::broadcast_transfer_leader` fans out via `C::spawn`. Emits no notification (the
/// response is purely advisory; a transfer takes effect via the timeout-driven election the target
/// triggers); failures are logged, like the reference.
pub(crate) async fn send_transfer_leader_request<C, N>(
    mut client: N,
    target: C::NodeId,
    req: TransferLeaderRequest<C>,
    ttl: Duration,
) where
    C: RaftTypeConfig,
    N: NetTransferLeader<C>,
{
    let option = RPCOption::new(ttl);

    let tm_res = C::timeout(ttl, client.transfer_leader(req, option)).await;
    let res = match tm_res {
        Ok(res) => res,
        Err(timeout) => {
            tracing::error!("timeout sending transfer_leader: {}, target: {}", timeout, target);
            return;
        }
    };

    match res {
        Err(e) => {
            tracing::error!("error sending transfer_leader: {}, target: {}", e, target);
        }
        Ok(resp) => {
            tracing::info!("Done transfer_leader sent to {}, resp: {:?}", target, resp);
        }
    }
}

/// Spawn the per-peer busy-spin network consumer, transferring ownership of `executor` to it.
///
/// The thread enters the tokio runtime context (`rt_handle.enter()`) so the quinn I/O the
/// executor drives via [`block_on`] finds a driver — the same "hybrid reactor" approach as the
/// consensus thread. `target` is included in the thread name so peers are distinguishable in
/// dumps.
pub(crate) fn spawn_peer<C, N, LS, SM>(
    rt_handle: tokio::runtime::Handle,
    executor: PeerExecutor<C, N, LS, SM>,
    target: &C::NodeId,
) -> PeerConsumerHandle<C>
where
    C: RaftTypeConfig,
    N: NetStreamAppend<C> + NetBackoff<C> + NetSnapshot<C>,
    LS: RaftLogStorage<C>,
    SM: 'static,
{
    let factory = || NetEvent::<C> { op: Mutex::new(None) };
    // Power-of-two ring; 64 slots is generous for burst replication ops per peer.
    let (poller, builder) = build_single_producer(64, factory, BusySpin).new_event_poller();
    let producer = builder.build();

    // Share the snapshot cancel/replace signal between the producer-side handle and the
    // consumer (clone the Arc out before `executor` is moved onto the thread).
    let snapshot_cancel = executor.snapshot_cancel();

    let join = std::thread::Builder::new()
        .name(format!("openraft-sync-net-peer-{}", target))
        .spawn(move || {
            // Enter the tokio runtime context so the executor's quinn I/O finds a driver.
            let _enter = rt_handle.enter();
            consumer_loop::<C, N, LS, SM>(executor, poller);
        })
        .expect("failed to spawn peer network consumer thread");

    PeerConsumerHandle {
        producer: Some(producer),
        join: Some(join),
        snapshot_cancel,
    }
}

/// The reactor-free per-peer consumer loop. Drains the send ring, drives ops, then re-drives
/// the streaming tail only when the anti-spam gate allows. Exits on `Polling::Shutdown`.
fn consumer_loop<C, N, LS, SM>(
    mut executor: PeerExecutor<C, N, LS, SM>,
    mut poller: EventPoller<NetEvent<C>, SingleProducerBarrier>,
) where
    C: RaftTypeConfig,
    N: NetStreamAppend<C> + NetBackoff<C> + NetSnapshot<C>,
    LS: RaftLogStorage<C>,
    SM: 'static,
{
    loop {
        let mut did_work = false;

        match poller.poll() {
            Ok(mut events) => {
                for event in &mut events {
                    let taken = event.op.lock().unwrap().take();
                    if let Some(op) = taken {
                        block_on_yielding(executor.run_op(op));
                        did_work = true;
                    }
                }
            }
            Err(Polling::NoEvents) => {}
            Err(Polling::Shutdown) => return,
        }

        // Gate-driven re-drive: stream the LogsSince tail / partial range when there is real
        // progress to make (never an empty RPC per iteration).
        if executor.should_drive_replicate() {
            let _ = block_on_yielding(executor.drive_replicate());
            did_work = true;
        }

        if !did_work {
            // Idle backoff. Unlike the single-per-node durability consumer, there are several
            // network consumers per leader; a `yield_now` busy-spin across all of them
            // oversubscribes CPU and starves the tokio runtime workers (quinn I/O, timers) when
            // many clusters run in one process (the test suite). A short *off-CPU* sleep frees
            // the core while idle; it only adds latency when there is genuinely no work to do (a
            // published op or an advanced watermark wakes the next iteration). Steady-state
            // replication never sleeps (did_work stays true). Latency tuning — e.g. a disruptor
            // blocking wait strategy — is a later phase.
            std::thread::sleep(IDLE_BACKOFF);
        }
    }
}

/// Off-CPU idle backoff for an otherwise-idle peer consumer. Small enough to be latency-
/// negligible for correctness (suite timeouts are seconds), large enough to keep many idle
/// consumers from oversubscribing CPU.
const IDLE_BACKOFF: Duration = Duration::from_micros(100);

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::mpsc;

    use disruptor::BusySpin;
    use disruptor::Polling;
    use disruptor::Producer;
    use disruptor::build_single_producer;

    use super::NetEvent;
    use super::NetOp;
    use crate::core::sync_durability::block_on;
    use crate::core::sync_input;
    use crate::core::sync_input::InputPoller;
    use crate::engine::testing::UTConfig;
    use crate::progress::inflight_id::InflightId;
    use crate::replication::replicate::Replicate;

    type TC = UTConfig<()>;

    /// Pins the per-peer ring + FIFO wiring (Task-1 scaffold test, retained).
    #[test]
    fn ring_delivers_replicate_ops_in_fifo_order() {
        let factory = || NetEvent::<TC> { op: Mutex::new(None) };
        let (mut poller, builder) = build_single_producer(64, factory, BusySpin).new_event_poller();
        let mut producer = builder.build();

        let (reply_tx, reply_rx) = mpsc::channel::<u64>();

        let consumer = std::thread::spawn(move || loop {
            match poller.poll() {
                Ok(mut events) => {
                    for event in &mut events {
                        let op = event.op.lock().unwrap().take();
                        if let Some(NetOp::Replicate { req }) = op {
                            let id = block_on(async move { *req.inflight_id });
                            reply_tx.send(id).ok();
                        }
                    }
                }
                Err(Polling::NoEvents) => std::thread::yield_now(),
                Err(Polling::Shutdown) => return,
            }
        });

        for id in [1u64, 2, 3] {
            let req = Replicate::<TC> {
                inflight_id: InflightId::new(id),
                ..Replicate::default()
            };
            let op = NetOp::Replicate { req };
            producer.publish(move |slot: &mut NetEvent<TC>| {
                *slot.op.get_mut().unwrap() = Some(op);
            });
        }

        let received: Vec<u64> = (0..3).map(|_| reply_rx.recv().unwrap()).collect();
        assert_eq!(received, [1, 2, 3], "ring delivers Replicate ops in FIFO order");

        drop(producer);
        consumer.join().unwrap();
    }

    // ---- Ack-contract tests (the make-or-break part) -------------------------------------

    use crate::base::BoxStream;
    use crate::core::heartbeat::event::HeartbeatEvent;
    use crate::core::notification::Notification;
    use crate::engine::testing::log_id;
    use crate::errors::RPCError;
    use crate::errors::Unreachable;
    use crate::progress::stream_id::StreamId;
    use crate::raft::StreamAppendError;
    use crate::raft::StreamAppendResult;
    use crate::replication::backoff_state::BackoffState;
    use crate::replication::inflight_append_queue::InflightAppendQueue;
    use crate::type_config::TypeConfigExt;
    use crate::type_config::alias::LogIdOf;
    use crate::vote::Vote;
    use crate::vote::raft_vote::RaftVoteExt;

    use super::AckEmitter;

    type Noti = Notification<TC>;

    /// `Notification` is `Display` but not `Debug`; render for assertion messages.
    fn names(notis: &[Noti]) -> Vec<String> {
        notis.iter().map(|n| format!("{}", n)).collect()
    }

    fn leader_vote() -> crate::type_config::alias::CommittedVoteOf<TC> {
        Vote::new(1, 2).into_committed()
    }

    fn new_emitter() -> (AckEmitter<TC>, InputPoller<TC>) {
        let (poller, producer) = sync_input::build_input_ring::<TC>();
        let ack = AckEmitter::<TC> {
            target: 3,
            leader_vote: leader_vote(),
            stream_id: StreamId::new(7),
            producer,
            remote_matched: None,
            inflight_id: None,
        };
        (ack, poller)
    }

    /// Drain all notifications currently visible on the input-ring poller. Polls until
    /// `Polling::NoEvents` (no more published entries). Single-threaded: the producer has
    /// already published everything by the time we drain, so all slots are immediately visible.
    /// Used by all `AckEmitter` ack-contract tests (ring path).
    fn drain(poller: &mut InputPoller<TC>) -> Vec<Noti> {
        let mut out = vec![];
        loop {
            match poller.poll() {
                Ok(mut events) => {
                    for e in &mut events {
                        if let Some(n) = e.notify.lock().unwrap().take() {
                            out.push(n);
                        }
                    }
                }
                Err(Polling::NoEvents) => break,
                Err(Polling::Shutdown) => break,
            }
        }
        out
    }

    fn resp_stream(
        items: Vec<Result<StreamAppendResult<TC>, RPCError<TC>>>,
    ) -> BoxStream<'static, Result<StreamAppendResult<TC>, RPCError<TC>>> {
        Box::pin(futures_util::stream::iter(items))
    }

    /// (a) Successful match → `HeartbeatProgress` (drain_acked) + `ReplicationProgress` with
    /// `Ok(Ok(Some(matching)))` and `inflight_id = Some`.
    #[test]
    fn ack_contract_match() {
        let (mut ack, mut poller) = new_emitter();
        ack.inflight_id = Some(InflightId::new(1));

        let matching: Option<LogIdOf<TC>> = Some(log_id(1, 2, 5));

        let q = InflightAppendQueue::<TC>::new();
        q.push(matching.clone());

        let mut backoff = BackoffState::new();
        let res = block_on(ack.handle_response_stream(resp_stream(vec![Ok(Ok(matching.clone()))]), q, &mut backoff));
        assert!(res.is_ok(), "stream exhausted normally");

        let notis = drain(&mut poller);
        // HeartbeatProgress first, then ReplicationProgress (network-response order).
        assert_eq!(notis.len(), 2, "expected HeartbeatProgress + ReplicationProgress, got {:?}", names(&notis));
        assert!(matches!(notis[0], Notification::HeartbeatProgress { .. }), "first: {}", notis[0]);
        match &notis[1] {
            Notification::ReplicationProgress { progress, inflight_id } => {
                assert_eq!(*inflight_id, Some(InflightId::new(1)), "inflight_id set when entries sent");
                let result = progress.result.as_ref().expect("Ok progress");
                assert!(matches!(result.0, Ok(Some(_))), "match result: {:?}", result);
            }
            other => panic!("expected ReplicationProgress, got {}", other),
        }
    }

    /// (a') A successful match with `matching.is_none()` emits **no** `ReplicationProgress`
    /// (the `matching.is_none()` guard), but still emits `HeartbeatProgress`.
    #[test]
    fn ack_contract_match_none_no_replication_progress() {
        let (mut ack, mut poller) = new_emitter();
        ack.inflight_id = Some(InflightId::new(1));

        let q = InflightAppendQueue::<TC>::new();
        q.push(None); // last_log_id None; drain_acked(&None) -> Some

        let mut backoff = BackoffState::new();
        let res = block_on(ack.handle_response_stream(resp_stream(vec![Ok(Ok(None))]), q, &mut backoff));
        assert!(res.is_ok());

        let notis = drain(&mut poller);
        assert_eq!(notis.len(), 1, "only HeartbeatProgress, got {:?}", names(&notis));
        assert!(matches!(notis[0], Notification::HeartbeatProgress { .. }));
    }

    /// (b) Conflict → `ReplicationProgress` with `Ok(Err(conflict))`, no `HeartbeatProgress`,
    /// and the stream handler returns `Err("AppendError")`.
    #[test]
    fn ack_contract_conflict() {
        let (mut ack, mut poller) = new_emitter();
        ack.inflight_id = Some(InflightId::new(1));

        let conflict = log_id(1, 2, 9);
        let q = InflightAppendQueue::<TC>::new();

        let mut backoff = BackoffState::new();
        let res = block_on(ack.handle_response_stream(
            resp_stream(vec![Ok(Err(StreamAppendError::Conflict(conflict)))]),
            q,
            &mut backoff,
        ));
        assert_eq!(res, Err("AppendError"));

        let notis = drain(&mut poller);
        assert_eq!(notis.len(), 1, "only ReplicationProgress(conflict), got {:?}", names(&notis));
        match &notis[0] {
            Notification::ReplicationProgress { progress, .. } => {
                let result = progress.result.as_ref().expect("Ok progress (accepted, conflicting)");
                assert!(matches!(result.0, Err(_)), "conflict result: {:?}", result);
            }
            other => panic!("expected ReplicationProgress, got {}", other),
        }
    }

    /// (c) RPC error with inflight → `ReplicationProgress` with `Err(_)` and `inflight_id = Some`;
    /// the stream handler returns `Err("RPCError")`.
    #[test]
    fn ack_contract_rpc_error_with_inflight() {
        let (mut ack, mut poller) = new_emitter();
        ack.inflight_id = Some(InflightId::new(2));

        let q = InflightAppendQueue::<TC>::new();
        let mut backoff = BackoffState::new();
        let err = RPCError::Unreachable(Unreachable::<TC>::from_string("boom"));
        let res = block_on(ack.handle_response_stream(resp_stream(vec![Err(err)]), q, &mut backoff));
        assert_eq!(res, Err("RPCError"));

        let notis = drain(&mut poller);
        assert_eq!(notis.len(), 1, "ReplicationProgress(err), got {:?}", names(&notis));
        match &notis[0] {
            Notification::ReplicationProgress { progress, inflight_id } => {
                assert_eq!(*inflight_id, Some(InflightId::new(2)), "inflight_id preserved on error");
                assert!(progress.result.is_err(), "error result: {:?}", progress.result);
            }
            other => panic!("expected ReplicationProgress, got {}", other),
        }
    }

    /// (c') An RPC error with **no** inflight emits nothing (`send_progress_error` guard).
    #[test]
    fn ack_contract_rpc_error_no_inflight_emits_nothing() {
        let (mut ack, mut poller) = new_emitter();
        ack.inflight_id = None;

        let q = InflightAppendQueue::<TC>::new();
        let mut backoff = BackoffState::new();
        let err = RPCError::Unreachable(Unreachable::<TC>::from_string("boom"));
        let res = block_on(ack.handle_response_stream(resp_stream(vec![Err(err)]), q, &mut backoff));
        assert_eq!(res, Err("RPCError"));

        assert!(drain(&mut poller).is_empty(), "no inflight ⇒ no progress notification");
    }

    /// (d) A successful heartbeat emits only `HeartbeatProgress` (no `ReplicationProgress`).
    #[test]
    fn ack_contract_heartbeat_success() {
        let (mut ack, mut poller) = new_emitter();
        let ev = HeartbeatEvent::<TC> {
            time: TC::now(),
            matching: Some(log_id(1, 2, 5)),
            cluster_committed: Some(log_id(1, 2, 5)),
        };
        let result: StreamAppendResult<TC> = Ok(Some(log_id(1, 2, 5)));
        ack.handle_heartbeat_result(result, &ev);

        let notis = drain(&mut poller);
        assert_eq!(notis.len(), 1, "only HeartbeatProgress, got {:?}", names(&notis));
        assert!(matches!(notis[0], Notification::HeartbeatProgress { .. }));
    }

    /// (d') A heartbeat conflict emits `ReplicationProgress(conflict, inflight=None)` then
    /// `HeartbeatProgress`.
    #[test]
    fn ack_contract_heartbeat_conflict() {
        let (mut ack, mut poller) = new_emitter();
        let matching = log_id(1, 2, 5);
        let ev = HeartbeatEvent::<TC> {
            time: TC::now(),
            matching: Some(matching),
            cluster_committed: Some(matching),
        };
        let result: StreamAppendResult<TC> = Err(StreamAppendError::Conflict(log_id(1, 2, 9)));
        ack.handle_heartbeat_result(result, &ev);

        let notis = drain(&mut poller);
        assert_eq!(notis.len(), 2, "ReplicationProgress(conflict) + HeartbeatProgress, got {:?}", names(&notis));
        match &notis[0] {
            Notification::ReplicationProgress { progress, inflight_id } => {
                assert_eq!(*inflight_id, None, "heartbeat conflict carries inflight_id None");
                assert!(progress.result.as_ref().map(|r| r.0.is_err()).unwrap_or(false));
            }
            other => panic!("expected ReplicationProgress, got {}", other),
        }
        assert!(matches!(notis[1], Notification::HeartbeatProgress { .. }));
    }

    /// (e) A heartbeat seeing a higher vote emits `HigherVote` and **no** `HeartbeatProgress`.
    #[test]
    fn ack_contract_heartbeat_higher_vote() {
        let (mut ack, mut poller) = new_emitter();
        let ev = HeartbeatEvent::<TC> {
            time: TC::now(),
            matching: Some(log_id(1, 2, 5)),
            cluster_committed: None,
        };
        let higher = Vote::new(5, 9);
        let result: StreamAppendResult<TC> = Err(StreamAppendError::HigherVote(higher));
        ack.handle_heartbeat_result(result, &ev);

        let notis = drain(&mut poller);
        assert_eq!(notis.len(), 1, "only HigherVote, got {:?}", names(&notis));
        assert!(matches!(notis[0], Notification::HigherVote { .. }));
    }

    // ---- Snapshot ack-contract tests (Task 3) --------------------------------------------
    //
    // These pin the snapshot-send emit contract ported from `SnapshotTransmitter`
    // (`send_snapshot` + `stream_snapshot`'s terminal error match):
    //  - success → `HeartbeatProgress` then `ReplicationProgress{Ok(Ok(meta.last_log_id)),
    //    inflight_id = Some(id)}`;
    //  - a higher vote in the response → `HigherVote`, no progress;
    //  - a storage error → `StorageError` (fatal), no progress.
    // They exercise the same `AckEmitter::send_snapshot` / `on_snapshot_error` methods the
    // peer executor's `drive_snapshot` drives, with a stubbed `NetSnapshot::full_snapshot`.

    use std::future::Future;
    use std::time::Duration;

    use super::SnapshotErrAction;
    use crate::StorageError;
    use crate::base::OptionalSend;
    use crate::errors::ReplicationClosed;
    use crate::errors::StreamingError;
    use crate::network::NetSnapshot;
    use crate::network::RPCOption;
    use crate::raft::SnapshotResponse;
    use crate::storage::Snapshot;
    use crate::storage::SnapshotMeta;
    use crate::type_config::alias::SnapshotOf;
    use crate::type_config::alias::VoteOf;

    /// A stub `NetSnapshot` whose `full_snapshot` returns one canned response.
    struct StubSnapNet {
        resp: Option<Result<SnapshotResponse<TC>, StreamingError<TC>>>,
    }

    impl NetSnapshot<TC> for StubSnapNet {
        async fn full_snapshot(
            &mut self,
            _vote: VoteOf<TC>,
            _snapshot: SnapshotOf<TC>,
            _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
            _option: RPCOption,
        ) -> Result<SnapshotResponse<TC>, StreamingError<TC>> {
            self.resp.take().expect("full_snapshot called once")
        }
    }

    /// Build a snapshot whose meta carries `last_log_id`.
    fn snapshot_with(last: Option<LogIdOf<TC>>) -> SnapshotOf<TC> {
        let meta = SnapshotMeta {
            last_log_id: last,
            last_membership: Default::default(),
            snapshot_id: "snap-1".to_string(),
        };
        Snapshot {
            meta,
            snapshot: std::io::Cursor::new(vec![]),
        }
    }

    /// A cancel future that never fires (the stub returns synchronously).
    fn never_cancel() -> impl Future<Output = ReplicationClosed> + OptionalSend + 'static {
        futures_util::future::pending()
    }

    /// (snapshot-a) Success → `HeartbeatProgress` then `ReplicationProgress` with
    /// `Ok(Ok(meta.last_log_id))` and `inflight_id = Some(id)`.
    #[test]
    fn ack_contract_snapshot_success() {
        let (mut ack, mut poller) = new_emitter();
        let id = InflightId::new(7);
        let last = Some(log_id(1, 2, 5));

        // Response vote equals the sender vote (no higher vote) → success.
        let mut net = StubSnapNet {
            resp: Some(Ok(SnapshotResponse::new(Vote::new(1, 2)))),
        };
        let opt = RPCOption::new(Duration::from_millis(1000));
        let res = block_on(ack.send_snapshot(&mut net, snapshot_with(last.clone()), id, never_cancel(), opt));
        assert!(res.is_ok(), "snapshot sent successfully");

        let notis = drain(&mut poller);
        assert_eq!(notis.len(), 2, "expected HeartbeatProgress + ReplicationProgress, got {:?}", names(&notis));
        assert!(matches!(notis[0], Notification::HeartbeatProgress { .. }), "first: {}", notis[0]);
        match &notis[1] {
            Notification::ReplicationProgress { progress, inflight_id } => {
                assert_eq!(*inflight_id, Some(id), "snapshot progress carries the inflight id");
                let result = progress.result.as_ref().expect("Ok progress");
                assert_eq!(result.0, Ok(last), "snapshot match == meta.last_log_id");
            }
            other => panic!("expected ReplicationProgress, got {}", other),
        }
    }

    /// (snapshot-b) A higher vote in the response → `send_snapshot` returns
    /// `Err(HigherVote)`, and `on_snapshot_error` emits `HigherVote` (no progress).
    #[test]
    fn ack_contract_snapshot_higher_vote() {
        let (mut ack, mut poller) = new_emitter();
        let id = InflightId::new(7);

        let mut net = StubSnapNet {
            resp: Some(Ok(SnapshotResponse::new(Vote::new(5, 9)))),
        };
        let opt = RPCOption::new(Duration::from_millis(1000));
        let res = block_on(ack.send_snapshot(&mut net, snapshot_with(Some(log_id(1, 2, 5))), id, never_cancel(), opt));
        let err = res.expect_err("higher vote → error");

        let action = ack.on_snapshot_error(err);
        assert!(matches!(action, SnapshotErrAction::Stop), "higher vote stops the snapshot loop");

        let notis = drain(&mut poller);
        assert_eq!(notis.len(), 1, "only HigherVote, got {:?}", names(&notis));
        assert!(matches!(notis[0], Notification::HigherVote { .. }), "got {}", notis[0]);
    }

    /// (snapshot-c) A storage error from `full_snapshot` → `send_snapshot` propagates it and
    /// `on_snapshot_error` emits `StorageError` (fatal, no progress).
    #[test]
    fn ack_contract_snapshot_storage_error() {
        let (mut ack, mut poller) = new_emitter();
        let id = InflightId::new(7);

        let sto_err = StorageError::<TC>::read_snapshot(None, TC::err_from_string("snap fail"));
        let mut net = StubSnapNet {
            resp: Some(Err(StreamingError::StorageError(sto_err))),
        };
        let opt = RPCOption::new(Duration::from_millis(1000));
        let res = block_on(ack.send_snapshot(&mut net, snapshot_with(Some(log_id(1, 2, 5))), id, never_cancel(), opt));
        let err = res.expect_err("storage error → error");

        let action = ack.on_snapshot_error(err);
        assert!(matches!(action, SnapshotErrAction::Stop), "storage error stops the snapshot loop");

        let notis = drain(&mut poller);
        assert_eq!(notis.len(), 1, "only StorageError, got {:?}", names(&notis));
        assert!(matches!(notis[0], Notification::StorageError { .. }), "got {}", notis[0]);
    }

    // ---- AckEmitter→ring test (Task 2) --------------------------------------------------
    //
    // Pins that `AckEmitter` publishes to the disruptor input ring (not the tokio mpsc).
    // Mirrors the ack-contract tests but asserts via the `EventPoller` instead of an mpsc
    // receiver — RED before the field swap, GREEN after.

    /// (ring-a) `AckEmitter` publishes `HeartbeatProgress` to the input ring; the `EventPoller`
    /// drained on the test thread sees the notification.
    #[test]
    fn ack_emitter_publishes_to_ring() {
        let (mut ack, mut poller) = new_emitter();

        // Drive one ack-emit: a successful heartbeat emits `HeartbeatProgress`.
        let ev = HeartbeatEvent::<TC> {
            time: TC::now(),
            matching: Some(log_id(1, 2, 5)),
            cluster_committed: Some(log_id(1, 2, 5)),
        };
        let result: StreamAppendResult<TC> = Ok(Some(log_id(1, 2, 5)));
        ack.handle_heartbeat_result(result, &ev);

        // Drain the ring poller — the notification must arrive here, not on a tokio mpsc.
        let notis = drain(&mut poller);
        assert_eq!(notis.len(), 1, "AckEmitter→ring: expected exactly one HeartbeatProgress, got {:?}", names(&notis));
        assert!(
            matches!(notis[0], Notification::HeartbeatProgress { stream_id, target, .. }
                if stream_id == StreamId::new(7) && target == 3),
            "HeartbeatProgress carries the emitter's stream_id + target: {}",
            notis[0]
        );
    }

    // ---- Vote send-contract tests (Task 4) -----------------------------------------------
    //
    // These pin the per-voter vote-send + emit contract ported from
    // `RaftCore::spawn_parallel_vote_requests`:
    //  - `Ok(resp)` → `Notification::VoteResponse{target,resp,candidate_vote}` (or
    //    `PreVoteResponse` for the pre-vote kind);
    //  - a transport `Err` → **no** notification (a partitioned peer must not count as a grant).
    // They exercise `send_vote_request` with a stubbed `NetVote::vote` / `pre_vote`.

    use super::VoteRequestKind;
    use super::send_vote_request;
    use crate::network::NetVote;
    use crate::raft::VoteRequest;
    use crate::raft::VoteResponse;

    /// A stub `NetVote` whose `vote` / `pre_vote` return one canned response.
    struct StubVoteNet {
        resp: Option<Result<VoteResponse<TC>, RPCError<TC>>>,
    }

    impl NetVote<TC> for StubVoteNet {
        async fn vote(
            &mut self,
            _rpc: VoteRequest<TC>,
            _option: RPCOption,
        ) -> Result<VoteResponse<TC>, RPCError<TC>> {
            self.resp.take().expect("vote called once")
        }

        async fn pre_vote(
            &mut self,
            _rpc: VoteRequest<TC>,
            _option: RPCOption,
        ) -> Result<VoteResponse<TC>, RPCError<TC>> {
            self.resp.take().expect("pre_vote called once")
        }
    }

    /// Drain all notifications from a tokio mpsc receiver (non-blocking). Used by the vote
    /// send-contract tests: `send_vote_request` stays on `tx_notification` (async C::spawn path).
    fn drain_mpsc(rx: &mut crate::type_config::alias::MpscReceiverOf<TC, Noti>) -> Vec<Noti> {
        use crate::async_runtime::MpscReceiver as _;
        let mut out = vec![];
        while let Ok(n) = rx.try_recv() {
            out.push(n);
        }
        out
    }

    fn a_vote_req() -> VoteRequest<TC> {
        VoteRequest::new(Vote::new(2, 1), Some(log_id(1, 1, 3)))
    }

    /// Drive a future under the reactor-free [`block_on`], but within a tokio runtime *context* so
    /// the `C::timeout` the helper uses finds a timer driver (the stub resolves immediately, so the
    /// timer never actually fires — this just mirrors the real consumer thread, which enters the
    /// runtime context via `spawn_peer`).
    fn block_on_in_rt<F: std::future::Future>(fut: F) -> F::Output {
        let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
        let _enter = rt.enter();
        block_on(fut)
    }

    /// (vote-a) A granted vote response → exactly one `Notification::VoteResponse` carrying the
    /// target, the response, and the request's vote as the (non-committed) candidate vote.
    #[test]
    fn vote_contract_response_emitted() {
        let (tx, mut rx) = TC::mpsc::<Noti>(64);

        let resp = VoteResponse::new(Vote::new(2, 1), Some(log_id(1, 1, 3)), true);
        let net = StubVoteNet { resp: Some(Ok(resp)) };

        block_on_in_rt(send_vote_request::<TC, _>(
            net,
            3, // target
            a_vote_req(),
            VoteRequestKind::Vote,
            Duration::from_millis(1000),
            tx,
        ));

        let notis = drain_mpsc(&mut rx);
        assert_eq!(notis.len(), 1, "exactly one VoteResponse, got {:?}", names(&notis));
        match &notis[0] {
            Notification::VoteResponse { target, candidate_vote, .. } => {
                assert_eq!(*target, 3, "VoteResponse carries the target");
                assert_eq!(
                    *candidate_vote,
                    Vote::new(2, 1).into_non_committed(),
                    "candidate_vote == the request's vote (non-committed)"
                );
            }
            other => panic!("expected VoteResponse, got {}", other),
        }
    }

    /// (vote-a') A pre-vote granted response → `Notification::PreVoteResponse`.
    #[test]
    fn pre_vote_contract_response_emitted() {
        let (tx, mut rx) = TC::mpsc::<Noti>(64);

        let resp = VoteResponse::new(Vote::new(2, 1), Some(log_id(1, 1, 3)), true);
        let net = StubVoteNet { resp: Some(Ok(resp)) };

        block_on_in_rt(send_vote_request::<TC, _>(
            net,
            3,
            a_vote_req(),
            VoteRequestKind::PreVote,
            Duration::from_millis(1000),
            tx,
        ));

        let notis = drain_mpsc(&mut rx);
        assert_eq!(notis.len(), 1, "exactly one PreVoteResponse, got {:?}", names(&notis));
        assert!(matches!(notis[0], Notification::PreVoteResponse { .. }), "got {}", notis[0]);
    }

    /// (vote-b) A transport error → **no** notification (a partitioned peer is not a grant).
    #[test]
    fn vote_contract_transport_error_emits_nothing() {
        let (tx, mut rx) = TC::mpsc::<Noti>(64);

        let err = RPCError::Unreachable(Unreachable::<TC>::from_string("partitioned"));
        let net = StubVoteNet { resp: Some(Err(err)) };

        block_on_in_rt(send_vote_request::<TC, _>(
            net,
            3,
            a_vote_req(),
            VoteRequestKind::Vote,
            Duration::from_millis(1000),
            tx,
        ));

        assert!(drain_mpsc(&mut rx).is_empty(), "transport failure ⇒ no vote notification");
    }
}
