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

// A few NetOp variants (Snapshot / Vote / TransferLeader) stay delegated until Tasks 3-4, so
// remain unconstructed here; keep the module-level allow rather than scattering per-item ones.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
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
use futures_util::StreamExt;

use crate::Config;
use crate::RaftTypeConfig;
use crate::async_runtime::Mutex as _;
use crate::async_runtime::MpscSender as _;
use crate::async_runtime::watch::WatchReceiver as _;
use crate::base::BoxStream;
use crate::core::SharedReplicateBatch;
use crate::core::VendedReader;
use crate::core::heartbeat::event::HeartbeatEvent;
use crate::core::notification::Notification;
use crate::core::sync_durability::block_on_yielding;
use crate::errors::RPCError;
use crate::errors::ReplicationClosed;
use crate::log_id_range::LogIdRange;
use crate::network::NetBackoff;
use crate::network::NetStreamAppend;
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
use crate::type_config::alias::WatchReceiverOf;
use crate::type_config::alias::WatchSenderOf;
use crate::vote::RaftVote;

/// One network op routed to a per-peer consumer.
///
/// In 3c.1 only `Replicate` and `Heartbeat` are published to the ring (the Replicate /
/// BroadcastHeartbeat arms). The remaining variants exist so the enum is forward-compatible
/// with Tasks 3-4, which relocate the vote / snapshot / transfer-leader arms.
pub(crate) enum NetOp<C>
where C: RaftTypeConfig
{
    /// Replicate log entries to this peer.
    Replicate { req: Replicate<C> },

    /// Send one heartbeat (zero-length AppendEntries) to this peer.
    Heartbeat { event: HeartbeatEvent<C> },

    /// Transmit a snapshot to this peer. (Delegated until Tasks 3-4.)
    Snapshot { inflight_id: InflightId },

    /// Send a (pre-)vote request to this peer. (Delegated until Tasks 3-4.)
    Vote { req: VoteRequest<C> },

    /// Forward a leadership-transfer request to this peer. (Delegated until Tasks 3-4.)
    TransferLeader { req: TransferLeaderRequest<C> },
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

/// Per-peer producer handle, held by `SyncCore`'s `PeerTable`. Dropping it shuts the
/// consumer thread down (producer drop → `Polling::Shutdown`) and joins it.
pub(crate) struct PeerConsumerHandle<C>
where C: RaftTypeConfig
{
    /// Write-ring producer. `Option` so `Drop` can drop it *before* joining — dropping
    /// the producer is what signals `Polling::Shutdown` to the consumer thread.
    producer: Option<SingleProducer<NetEvent<C>, SingleConsumerBarrier>>,
    join: Option<JoinHandle<()>>,
}

impl<C> PeerConsumerHandle<C>
where C: RaftTypeConfig
{
    /// Publish a network op to the peer consumer (FIFO; preserves send ordering).
    pub(crate) fn publish(&mut self, op: NetOp<C>) {
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
    tx_notify: MpscSenderOf<C, Notification<C>>,

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
                    self.send_progress_error(rpc_err, "stream-replication").await;
                    return Err("RPCError");
                }
            };

            match append_res {
                Ok(matching) => {
                    let last_acked_sending_time = inflight_queue.drain_acked(&matching);

                    if let Some(last) = last_acked_sending_time {
                        self.notify_heartbeat_progress(last).await;
                    }

                    self.remote_matched = matching.clone();

                    self.notify_progress(ReplicationResult(Ok(matching))).await;
                }
                Err(append_err) => {
                    match append_err {
                        StreamAppendError::Conflict(conflict_log_id) => {
                            self.notify_progress(ReplicationResult(Err(conflict_log_id))).await;
                        }
                        StreamAppendError::HigherVote(higher) => {
                            self.tx_notify
                                .send(Notification::HigherVote {
                                    target: self.target.clone(),
                                    higher,
                                    leader_vote: self.leader_vote.clone(),
                                })
                                .await
                                .ok();
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
    async fn send_progress_error(&mut self, err: RPCError<C>, when: impl fmt::Display) {
        tracing::warn!("peer executor recv RPCError: {}, when:({})", err, when);

        // No inflight id ⇒ no payload was sent and nobody is waiting, no need to report.
        if self.inflight_id.is_none() {
            return;
        }
        self.tx_notify
            .send(Notification::ReplicationProgress {
                progress: Progress {
                    target: self.target.clone(),
                    result: Err(err.to_string()),
                },
                inflight_id: self.inflight_id,
            })
            .await
            .ok();
    }

    /// Port of `ReplicationCore::notify_heartbeat_progress`: a successful replication round-trip
    /// implies a successful heartbeat.
    async fn notify_heartbeat_progress(&mut self, sending_time: InstantOf<C>) {
        self.tx_notify
            .send(Notification::HeartbeatProgress {
                stream_id: self.stream_id,
                target: self.target.clone(),
                sending_time,
            })
            .await
            .ok();
    }

    /// Port of `ReplicationCore::notify_progress`: emit a `ReplicationProgress` for a match or
    /// conflict. Crucially, a successful match with `matching.is_none()` emits **nothing**; a
    /// conflict always emits (even with `inflight_id == None`).
    async fn notify_progress(&mut self, replication_result: ReplicationResult<C>) {
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
        self.tx_notify
            .send(Notification::ReplicationProgress {
                progress: Progress {
                    target: self.target.clone(),
                    result: Ok(replication_result.clone()),
                },
                // If None, it is not a response to a request with payload.
                inflight_id: self.inflight_id,
            })
            .await
            .ok();
    }

    /// Port of `HeartbeatWorker::handle_stream_result`: turn one heartbeat round-trip result
    /// into the heartbeat ack contract.
    async fn handle_heartbeat_result(&mut self, result: StreamAppendResult<C>, heartbeat: &HeartbeatEvent<C>) {
        match result {
            Ok(_) => {
                self.send_heartbeat_progress(heartbeat).await;
            }
            Err(StreamAppendError::HigherVote(vote)) => {
                tracing::debug!("seen a higher vote from {}; when:(sending heartbeat)", self.target);
                self.tx_notify
                    .send(Notification::HigherVote {
                        target: self.target.clone(),
                        higher: vote,
                        leader_vote: self.leader_vote.clone(),
                    })
                    .await
                    .ok();
                // Higher vote means leadership is not granted; do not send HeartbeatProgress.
            }
            Err(StreamAppendError::Conflict(_conflict_log_id)) => {
                // The follower does not have `matching`. Use `matching` as the conflict point —
                // safe unwrap(): a None never conflicts.
                let conflict_log_id = heartbeat.matching.clone().unwrap();

                self.tx_notify
                    .send(Notification::ReplicationProgress {
                        progress: Progress {
                            target: self.target.clone(),
                            result: Ok(ReplicationResult(Err(conflict_log_id))),
                        },
                        inflight_id: None,
                    })
                    .await
                    .ok();
                self.send_heartbeat_progress(heartbeat).await;
            }
        }
    }

    async fn send_heartbeat_progress(&mut self, heartbeat: &HeartbeatEvent<C>) {
        self.tx_notify
            .send(Notification::HeartbeatProgress {
                stream_id: self.stream_id,
                sending_time: heartbeat.time,
                target: self.target.clone(),
            })
            .await
            .ok();
    }
}

/// The per-peer executor: a reactor-free port of `ReplicationCore`'s append + heartbeat path.
pub(crate) struct PeerExecutor<C, N, LS>
where
    C: RaftTypeConfig,
    N: NetStreamAppend<C> + NetBackoff<C>,
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

    /// Keep the cancel/replicate watch senders alive so the receivers held inside
    /// `stream_state` / `event_watcher` stay open (the LogsSince/backoff `select!` paths in
    /// `StreamState` borrow them, though the bounded port never blocks on them).
    _cancel_tx: WatchSenderOf<C, ()>,
    _replicate_tx: WatchSenderOf<C, Replicate<C>>,
}

impl<C, N, LS> PeerExecutor<C, N, LS>
where
    C: RaftTypeConfig,
    N: NetStreamAppend<C> + NetBackoff<C>,
    LS: RaftLogStorage<C>,
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
        network: N,
        log_reader: VendedReader<C, LS>,
        committed_rx: WatchReceiverOf<C, Option<LogIdOf<C>>>,
        io_accepted_rx: WatchReceiverOf<C, crate::raft_state::IOId<C>>,
        io_submitted_rx: WatchReceiverOf<C, crate::raft_state::IOId<C>>,
        replicate_batch: SharedReplicateBatch,
        remote_matched: Option<LogIdOf<C>>,
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
        }));

        let ack = AckEmitter {
            target,
            leader_vote,
            stream_id,
            tx_notify,
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
            _cancel_tx: cancel_tx,
            _replicate_tx: replicate_tx,
        }
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
            NetOp::Snapshot { .. } | NetOp::Vote { .. } | NetOp::TransferLeader { .. } => {
                // These arms stay delegated to RaftCore until Tasks 3-4 — never published here.
                tracing::warn!("sync-net peer consumer received an unhandled NetOp variant (ignored)");
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
                    self.ack.send_progress_error(rpc_err, "initiate-stream-replication").await;
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
        let res: Result<Option<StreamAppendResult<C>>, RPCError<C>> = async {
            let mut output = network.stream_append(input_stream, option).await?;
            output.next().await.transpose()
        }
        .await;
        self.network = Some(network);

        match res {
            Ok(Some(stream_result)) => {
                self.ack.handle_heartbeat_result(stream_result, &heartbeat).await;
            }
            Ok(None) => {
                tracing::warn!("heartbeat stream to {} returned no response", self.ack.target);
            }
            Err(e) => {
                tracing::warn!("failed to send heartbeat to {}: {}", self.ack.target, e);
            }
        }
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

/// Spawn the per-peer busy-spin network consumer, transferring ownership of `executor` to it.
///
/// The thread enters the tokio runtime context (`rt_handle.enter()`) so the quinn I/O the
/// executor drives via [`block_on`] finds a driver — the same "hybrid reactor" approach as the
/// consensus thread. `target` is included in the thread name so peers are distinguishable in
/// dumps.
pub(crate) fn spawn_peer<C, N, LS>(
    rt_handle: tokio::runtime::Handle,
    executor: PeerExecutor<C, N, LS>,
    target: &C::NodeId,
) -> PeerConsumerHandle<C>
where
    C: RaftTypeConfig,
    N: NetStreamAppend<C> + NetBackoff<C>,
    LS: RaftLogStorage<C>,
{
    let factory = || NetEvent::<C> { op: Mutex::new(None) };
    // Power-of-two ring; 64 slots is generous for burst replication ops per peer.
    let (poller, builder) = build_single_producer(64, factory, BusySpin).new_event_poller();
    let producer = builder.build();

    let join = std::thread::Builder::new()
        .name(format!("openraft-sync-net-peer-{}", target))
        .spawn(move || {
            // Enter the tokio runtime context so the executor's quinn I/O finds a driver.
            let _enter = rt_handle.enter();
            consumer_loop::<C, N, LS>(executor, poller);
        })
        .expect("failed to spawn peer network consumer thread");

    PeerConsumerHandle {
        producer: Some(producer),
        join: Some(join),
    }
}

/// The reactor-free per-peer consumer loop. Drains the send ring, drives ops, then re-drives
/// the streaming tail only when the anti-spam gate allows. Exits on `Polling::Shutdown`.
fn consumer_loop<C, N, LS>(
    mut executor: PeerExecutor<C, N, LS>,
    mut poller: EventPoller<NetEvent<C>, SingleProducerBarrier>,
) where
    C: RaftTypeConfig,
    N: NetStreamAppend<C> + NetBackoff<C>,
    LS: RaftLogStorage<C>,
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

    fn new_emitter() -> (AckEmitter<TC>, crate::type_config::alias::MpscReceiverOf<TC, Noti>) {
        let (tx, rx) = TC::mpsc::<Noti>(64);
        let ack = AckEmitter::<TC> {
            target: 3,
            leader_vote: leader_vote(),
            stream_id: StreamId::new(7),
            tx_notify: tx,
            remote_matched: None,
            inflight_id: None,
        };
        (ack, rx)
    }

    /// Drain all currently-queued notifications.
    fn drain(rx: &mut crate::type_config::alias::MpscReceiverOf<TC, Noti>) -> Vec<Noti> {
        use crate::async_runtime::MpscReceiver as _;
        let mut out = vec![];
        while let Ok(n) = rx.try_recv() {
            out.push(n);
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
        let (mut ack, mut rx) = new_emitter();
        ack.inflight_id = Some(InflightId::new(1));

        let matching: Option<LogIdOf<TC>> = Some(log_id(1, 2, 5));

        let q = InflightAppendQueue::<TC>::new();
        q.push(matching.clone());

        let mut backoff = BackoffState::new();
        let res = block_on(ack.handle_response_stream(resp_stream(vec![Ok(Ok(matching.clone()))]), q, &mut backoff));
        assert!(res.is_ok(), "stream exhausted normally");

        let notis = drain(&mut rx);
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
        let (mut ack, mut rx) = new_emitter();
        ack.inflight_id = Some(InflightId::new(1));

        let q = InflightAppendQueue::<TC>::new();
        q.push(None); // last_log_id None; drain_acked(&None) -> Some

        let mut backoff = BackoffState::new();
        let res = block_on(ack.handle_response_stream(resp_stream(vec![Ok(Ok(None))]), q, &mut backoff));
        assert!(res.is_ok());

        let notis = drain(&mut rx);
        assert_eq!(notis.len(), 1, "only HeartbeatProgress, got {:?}", names(&notis));
        assert!(matches!(notis[0], Notification::HeartbeatProgress { .. }));
    }

    /// (b) Conflict → `ReplicationProgress` with `Ok(Err(conflict))`, no `HeartbeatProgress`,
    /// and the stream handler returns `Err("AppendError")`.
    #[test]
    fn ack_contract_conflict() {
        let (mut ack, mut rx) = new_emitter();
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

        let notis = drain(&mut rx);
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
        let (mut ack, mut rx) = new_emitter();
        ack.inflight_id = Some(InflightId::new(2));

        let q = InflightAppendQueue::<TC>::new();
        let mut backoff = BackoffState::new();
        let err = RPCError::Unreachable(Unreachable::<TC>::from_string("boom"));
        let res = block_on(ack.handle_response_stream(resp_stream(vec![Err(err)]), q, &mut backoff));
        assert_eq!(res, Err("RPCError"));

        let notis = drain(&mut rx);
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
        let (mut ack, mut rx) = new_emitter();
        ack.inflight_id = None;

        let q = InflightAppendQueue::<TC>::new();
        let mut backoff = BackoffState::new();
        let err = RPCError::Unreachable(Unreachable::<TC>::from_string("boom"));
        let res = block_on(ack.handle_response_stream(resp_stream(vec![Err(err)]), q, &mut backoff));
        assert_eq!(res, Err("RPCError"));

        assert!(drain(&mut rx).is_empty(), "no inflight ⇒ no progress notification");
    }

    /// (d) A successful heartbeat emits only `HeartbeatProgress` (no `ReplicationProgress`).
    #[test]
    fn ack_contract_heartbeat_success() {
        let (mut ack, mut rx) = new_emitter();
        let ev = HeartbeatEvent::<TC> {
            time: TC::now(),
            matching: Some(log_id(1, 2, 5)),
            cluster_committed: Some(log_id(1, 2, 5)),
        };
        let result: StreamAppendResult<TC> = Ok(Some(log_id(1, 2, 5)));
        block_on(ack.handle_heartbeat_result(result, &ev));

        let notis = drain(&mut rx);
        assert_eq!(notis.len(), 1, "only HeartbeatProgress, got {:?}", names(&notis));
        assert!(matches!(notis[0], Notification::HeartbeatProgress { .. }));
    }

    /// (d') A heartbeat conflict emits `ReplicationProgress(conflict, inflight=None)` then
    /// `HeartbeatProgress`.
    #[test]
    fn ack_contract_heartbeat_conflict() {
        let (mut ack, mut rx) = new_emitter();
        let matching = log_id(1, 2, 5);
        let ev = HeartbeatEvent::<TC> {
            time: TC::now(),
            matching: Some(matching),
            cluster_committed: Some(matching),
        };
        let result: StreamAppendResult<TC> = Err(StreamAppendError::Conflict(log_id(1, 2, 9)));
        block_on(ack.handle_heartbeat_result(result, &ev));

        let notis = drain(&mut rx);
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
        let (mut ack, mut rx) = new_emitter();
        let ev = HeartbeatEvent::<TC> {
            time: TC::now(),
            matching: Some(log_id(1, 2, 5)),
            cluster_committed: None,
        };
        let higher = Vote::new(5, 9);
        let result: StreamAppendResult<TC> = Err(StreamAppendError::HigherVote(higher));
        block_on(ack.handle_heartbeat_result(result, &ev));

        let notis = drain(&mut rx);
        assert_eq!(notis.len(), 1, "only HigherVote, got {:?}", names(&notis));
        assert!(matches!(notis[0], Notification::HigherVote { .. }));
    }
}
