//! `SyncCore` — experimental synchronous execution harness around the Raft
//! [`Engine`](crate::engine::Engine), an alternative to the async [`RaftCore`].
//! Gated by the `sync-core` feature; [`Raft::new`](crate::Raft::new) builds it
//! instead of `RaftCore` when the feature is on.
//!
//! ## Why
//!
//! `RaftCore` runs the Raft algorithm as a set of async tasks coordinated by
//! channels — every internal hop is a scheduler reschedule. The `Engine` it
//! drives is, however, a *pure synchronous* state machine: feed it events, drain
//! [`Command`](crate::engine::Command)s, execute them. `SyncCore` aims to drive
//! that same Engine from a synchronous loop (ultimately a busy-spin ring
//! pipeline with isolated I/O consumers), keeping openraft's proven algorithm.
//!
//! ## Status: v5 — synchronous consensus loop on a dedicated thread (minimal "3d")
//!
//! `SyncCore` now runs its event loop **synchronously on a dedicated `std::thread`**,
//! off the tokio scheduler. `Raft::new` spawns that thread (entering the tokio runtime
//! *context* — the "hybrid reactor" — so the still-delegated replication commands'
//! `C::spawn` and the network/IO it drives keep a runtime to run on), and the loop:
//!  - drains inputs with `try_recv` (`process_raft_msg`/`process_notification`) and polls
//!    the shutdown oneshot non-blockingly — no async `select!`;
//!  - drives the async storage/apply/network trait seam to completion with the
//!    reactor-free, never-park [`block_on`](crate::core::sync_durability::block_on).
//!
//! This is the minimal foundation: the loop is off the scheduler, but I/O *completions*
//! still busy-spin the consensus thread (every `block_on`), partly re-serializing what
//! 3b.2 moved off-thread. The follow-up completion-as-notification redesign feeds I/O
//! completions back as later loop inputs to remove that.
//!
//! `SyncCore` **owns the full orchestration and command execution**.
//! Methods owned: `run`, `do_main`, `runtime_loop`, `process_raft_msg`,
//! `process_notification`, `run_engine_commands`, `run_progress_driven_command`,
//! and `run_command`.
//!
//! Within `run_command`, all storage/apply/pure-sync commands execute inline:
//! `AppendEntries`, `SaveVote`, `PurgeLog`, `TruncateLog`, `UpdateIOProgress`,
//! `ReplicateCommitted`, `Respond`, `SaveCommittedAndApply`, `StateMachine`.
//!
//! Replication / heartbeat / snapshot run on the per-peer network consumers
//! (`Replicate`, `BroadcastHeartbeat`, `ReplicateSnapshot`, `RebuildReplicationStreams`,
//! `CloseReplicationStreams`). Still delegated to `RaftCore`: the vote / transfer-leader
//! commands (`SendVote`, `SendPreVote`, `BroadcastTransferLeader`) and the two engine-driving
//! per-message handlers (`handle_api_msg`, `handle_notification`). Task 4 relocates these.
//!
//! Validated by running openraft's own integration suite with
//! `--features sync-core`.


use std::time::Duration;

use crate::async_runtime::MpscReceiver;
use crate::async_runtime::TryRecvError;
use crate::async_runtime::watch::WatchSender;
use crate::batch::Batch;
use crate::core::RaftCore;
use crate::core::ServerState;
use crate::core::balancer::Balancer;
use crate::core::sync_durability;
use crate::core::sync_durability::block_on;
use crate::core::sync_durability::DurabilityOp;
use crate::core::sync_durability::LogStoreHandle;
use crate::core::sync_durability::ReaderRequest;
use crate::core::sync_network;
use crate::core::sync_network::NetOp;
use crate::core::sync_network::PeerExecutor;
use crate::core::sync_network::PeerTable;
use crate::core::heartbeat::event::HeartbeatEvent;
use crate::core::notification::Notification;
use crate::core::stage::Stage;
use crate::engine::Command;
use crate::engine::replication_progress::TargetProgress;
use crate::replication::ReplicationSessionId;
use crate::type_config::alias::CommittedVoteOf;
use crate::entry::RaftEntry;
use crate::errors::ClientWriteError;
use crate::errors::Fatal;
use crate::errors::ForwardToLeader;
use crate::errors::Infallible;
use crate::log_id::option_raft_log_id_ext::OptionRaftLogIdExt;
use crate::network::RaftNetworkFactory;
use crate::progress::Progress;
use crate::raft::responder::Responder;
use crate::raft::VoteResponse;
use crate::raft_state::io_state::io_id::IOId;
use crate::raft_state::LogStateReader;
use crate::rt::MpscSender;
use crate::storage::RaftLogStorage;
use crate::type_config::TypeConfigExt;
use crate::type_config::alias::OneshotReceiverOf;
use crate::type_config::alias::WatchSenderOf;
use crate::vote::raft_vote::RaftVoteExt;
use crate::vote::vote_status::VoteStatus;
use crate::RaftTypeConfig;
use crate::StorageError;

/// Synchronous alternative to [`RaftCore`]. See module docs.
pub(crate) struct SyncCore<C, NF, LS, SM>
where
    C: RaftTypeConfig,
    NF: RaftNetworkFactory<C>,
    LS: RaftLogStorage<C>,
{
    core: RaftCore<C, NF, LS, SM>,

    /// Handle to the durability (log-store) consumer thread, which owns `log_store` and
    /// executes all storage write I/O reactor-free. Dropping it shuts the consumer down
    /// (the producer drop signals ring shutdown) and joins the thread.
    durability: LogStoreHandle<C>,

    /// Per-peer network consumers (3c.1). The `Replicate` / `BroadcastHeartbeat` /
    /// `Rebuild`/`CloseReplicationStreams` commands manage and feed this table instead of
    /// delegating to `RaftCore`'s tokio replication tasks. Dropping a handle joins its
    /// consumer thread.
    peers: PeerTable<C>,
}

impl<C, NF, LS, SM> SyncCore<C, NF, LS, SM>
where
    C: RaftTypeConfig,
    NF: RaftNetworkFactory<C>,
    LS: RaftLogStorage<C>,
    SM: 'static,
{
    pub(crate) fn new(
        mut core: RaftCore<C, NF, LS, SM>,
        reader_rx: std::sync::mpsc::Receiver<ReaderRequest<C, LS>>,
        readable_tx: WatchSenderOf<C, Option<u64>>,
    ) -> Self {
        // The durability consumer becomes the sole owner of `log_store`.
        let log_store = core.log_store.take().expect("log_store present at SyncCore construction");
        let durability = sync_durability::spawn(log_store, reader_rx, readable_tx);
        Self {
            core,
            durability,
            peers: PeerTable::new(),
        }
    }

    /// Await a storage-write completion signalled by the durability consumer, mapping a
    /// dropped consumer (oneshot cancelled) to a storage error.
    async fn await_completion(
        rx: OneshotReceiverOf<C, Result<(), StorageError<C>>>,
    ) -> Result<(), StorageError<C>> {
        match rx.await {
            Ok(res) => res,
            Err(_) => Err(StorageError::write(C::err_from_string(
                "durability consumer dropped before completing IO",
            ))),
        }
    }

    /// Synchronous entry point, run on the dedicated consensus `std::thread` (see
    /// `Raft::new`). Mirrors `RaftCore::main`: run `do_main`, then flush metrics and
    /// publish the shutdown state.
    pub(crate) fn run(mut self, rx_shutdown: OneshotReceiverOf<C, ()>) -> Result<Infallible, Fatal<C>> {
        let res = self.do_main(rx_shutdown);

        // Flush buffered metrics.
        self.core.flush_metrics();

        // Safe unwrap: res is Result<Infallible, _>.
        let err = res.unwrap_err();
        match err {
            Fatal::Stopped => { /* Normal quit */ }
            _ => tracing::error!("SyncCore::run error: {}", err),
        }

        {
            let mut curr = self.core.tx_metrics.borrow_watched().clone();
            curr.state = ServerState::Shutdown;
            curr.running_state = Err(err.clone());
            self.core.tx_metrics.send(curr).ok();
        }

        tracing::info!("SyncCore shutdown complete");
        Err(err)
    }

    /// Mirrors `RaftCore::do_main`: startup the Engine, drain its startup
    /// commands, then enter the synchronous runtime loop. Startup commands are driven
    /// reactor-free via [`block_on`].
    fn do_main(&mut self, rx_shutdown: OneshotReceiverOf<C, ()>) -> Result<Infallible, Fatal<C>> {
        tracing::debug!("SyncCore is initializing");

        self.core.engine.startup();
        block_on(self.run_engine_commands())?;
        self.core.flush_metrics();

        self.runtime_loop(rx_shutdown)
    }

    /// The synchronous event loop — the orchestration `SyncCore` owns. Replaces the async
    /// `select!` with `try_recv` input draining + a non-blocking shutdown poll, and drives
    /// the async helpers (which still touch the async storage/network trait seam) to
    /// completion with the reactor-free, never-park [`block_on`]. Busy-spins; yields when a
    /// whole iteration processed nothing so the suite's many nodes don't peg every core.
    ///
    /// This is the minimal "3d": off the scheduler, but I/O completions still busy-spin the
    /// thread inside `block_on`. The completion-as-notification redesign removes that by
    /// feeding completions back as later loop inputs.
    fn runtime_loop(&mut self, rx_shutdown: OneshotReceiverOf<C, ()>) -> Result<Infallible, Fatal<C>> {
        let mut balancer = Balancer::new(10_000);
        let mut rx_shutdown = std::pin::pin!(rx_shutdown);

        loop {
            self.core.flush_metrics();

            // Shutdown check: non-blocking poll of the oneshot. Ready means either the
            // signal arrived or the sender was dropped — both mean stop.
            if sync_durability::poll_once(rx_shutdown.as_mut()).is_ready() {
                tracing::info!("recv from rx_shutdown");
                return Err(Fatal::Stopped);
            }

            block_on(self.run_engine_commands())?;

            // Drain channels one by one, bounded by the balancer's budgets. Both helpers
            // are non-blocking (`try_recv` internally) and return how many they processed.
            let raft_msg_processed = block_on(self.process_raft_msg(balancer.raft_msg()))?;
            let notify_processed = block_on(self.process_notification(balancer.notification()))?;

            #[allow(clippy::collapsible_else_if)]
            if notify_processed == balancer.notification() {
                balancer.increase_notification();
            } else {
                if raft_msg_processed == balancer.raft_msg() {
                    balancer.increase_raft_msg();
                }
            }

            self.core.trigger_routine_actions();
            block_on(self.run_engine_commands())?;

            // Reactor-free idle backoff: a fully idle iteration yields rather than pegging
            // the core (the durability consumer does the same). Latency tuning is later.
            if raft_msg_processed == 0 && notify_processed == 0 {
                std::thread::yield_now();
            }
        }
    }

    /// Mirrors `RaftCore::process_raft_msg`: drain the API channel up to
    /// `at_most` messages, calling `RaftCore::handle_api_msg` per message and
    /// `SyncCore::run_engine_commands` for the command drain after each batch.
    async fn process_raft_msg(&mut self, at_most: u64) -> Result<u64, Fatal<C>> {
        self.core.runtime_stats.raft_msg_budget.record(at_most);

        let mut processed = 0u64;
        let mut total = 0u64;
        // Being 0 disabled batch msg processing.
        // TODO: make it configurable
        let run_command_threshold = 0;
        let mut last_log_index = 0;

        for _i in 0..at_most {
            let res = self.core.rx_api.try_recv().await?;
            let Some(msg) = res else {
                break;
            };

            self.core.handle_api_msg(msg).await;
            processed += 1;
            total += 1;

            let index = self.core.engine.state.last_log_id().next_index();

            if index.saturating_sub(last_log_index) >= run_command_threshold {
                // After handling all the inputs, batch run all the commands for better performance
                self.core.runtime_stats.raft_msg_per_run.record(processed);
                self.core.runtime_stats.raft_msg_usage_permille.record(processed * 1000 / at_most);
                self.run_engine_commands().await?;

                last_log_index = index;
                processed = 0;
            }
        }

        // After handling all the inputs, batch run all the commands for better performance
        self.core.runtime_stats.raft_msg_per_run.record(processed);
        self.core.runtime_stats.raft_msg_usage_permille.record(processed * 1000 / at_most);
        self.run_engine_commands().await?;

        if total == at_most {
            tracing::debug!("at_most({}) reached, there are more queued RaftMsg to process", at_most);
        }

        Ok(total)
    }

    /// Mirrors `RaftCore::process_notification`: drain the notification channel
    /// up to `at_most` messages, calling `RaftCore::handle_notification` per
    /// message and `SyncCore::run_engine_commands` for the command drain after each.
    async fn process_notification(&mut self, at_most: u64) -> Result<u64, Fatal<C>> {
        self.core.runtime_stats.notification_budget.record(at_most);

        let mut processed = 0u64;

        for _i in 0..at_most {
            let res = self.core.rx_notification.try_recv();
            let notify = match res {
                Ok(msg) => msg,
                Err(e) => match e {
                    TryRecvError::Empty => {
                        tracing::debug!("all Notification are processed, wait for more");
                        break;
                    }
                    TryRecvError::Disconnected => {
                        tracing::error!("rx_notify is disconnected, quit");
                        return Err(Fatal::Stopped);
                    }
                },
            };

            self.core.handle_notification(notify)?;
            processed += 1;

            // TODO: does run_engine_commands() run too frequently?
            //       to run many commands in one shot, it is possible to batch more commands to gain
            //       better performance.

            self.run_engine_commands().await?;
        }

        self.core.runtime_stats.notification_usage_permille.record(processed * 1000 / at_most);

        if processed == at_most {
            tracing::debug!(
                "at_most({}) reached, there are more queued Notification to process",
                at_most
            );
        }

        Ok(processed)
    }

    /// Per-command execution. As of Phase 3c.1 (Task 4), SyncCore owns **every** Engine command:
    /// storage / apply / pure-sync commands execute inline; replication / heartbeat / snapshot are
    /// routed to the per-peer network consumers; vote / transfer-leader fan out off the consensus
    /// loop. There is no `RaftCore::run_command` delegation left.
    async fn run_command(&mut self, cmd: Command<C, SM>) -> Result<Option<Command<C, SM>>, StorageError<C>> {
        // SyncCore now owns every command — there is no `self.core.run_command` delegation left.
        // Condition gate + stats, then execute inline.
        let condition = cmd.condition();
        if let Some(condition) = condition
            && !condition.is_met(&self.core.engine.state.io_state)
        {
            tracing::debug!("{} not yet met, postpone cmd: {}", condition, cmd);
            return Ok(Some(cmd));
        }
        self.core.runtime_stats.record_command(cmd.name());

        match cmd {
            Command::UpdateIOProgress { io_id, .. } => {
                self.core.io_accepted_tx.send_if_greater(io_id.clone());
                self.core.engine.state.log_progress_mut().submit(io_id.clone());
                let notify = Notification::LocalIO { io_id: io_id.clone() };
                self.core.tx_notification.send(notify).await.ok();
            }
            Command::ReplicateCommitted { committed } => {
                self.core.committed_tx.send_if_greater(committed);
            }
            Command::Respond { resp: send, .. } => {
                send.send();
            }
            Command::AppendEntries { committed_vote: vote, entries } => {
                let last_log_id = entries.last().unwrap().log_id();
                let last_log_index = last_log_id.index();
                let entry_count = entries.len() as u64;
                self.core.runtime_stats.append_batch.record(entry_count);
                if let Some(r) = &self.core.metrics_recorder {
                    r.record_append_batch(entry_count);
                }
                let io_id = IOId::new_log_io(vote, Some(last_log_id));
                // Before-work stays on the consensus loop, ahead of publish.
                self.core.io_accepted_tx.send_if_greater(io_id.clone());
                self.core.engine.state.log_progress_mut().submit(io_id.clone());
                self.core.runtime_stats.record_log_stage_now(Stage::Submitted, last_log_index + 1);
                // Fire-and-forget: publish and return. Flush completion flows via the `IOFlushed`
                // callback → `tx_io_completed` → forwarder → `Notification::LocalIO` → engine, as
                // in RaftCore. Readability for a later (delegated) `Replicate` is preserved by the
                // durability consumer's readable-watermark + `GatedLogReader` (no consensus-loop
                // wait). Submission errors surface via `tx_io_completed` (see `run_op`).
                self.durability.publish(DurabilityOp::Append {
                    entries,
                    io_id,
                    tx_io_completed: self.core.tx_io_completed.clone(),
                });
            }
            Command::SaveVote { vote } => {
                let io_id = IOId::new(&vote);
                self.core.io_accepted_tx.send_if_greater(io_id.clone());
                self.core.engine.state.log_progress_mut().submit(io_id.clone());
                // Storage call on the consumer; await completion before the after-work.
                let (tx, rx) = C::oneshot();
                self.durability.publish(DurabilityOp::SaveVote { vote: vote.clone(), done: tx });
                Self::await_completion(rx).await?;
                self.core.tx_notification
                    .send(Notification::LocalIO { io_id: IOId::new(&vote) })
                    .await
                    .ok();
                if let VoteStatus::Pending(non_committed) = vote.clone().into_vote_status() {
                    self.core.tx_notification
                        .send(Notification::VoteResponse {
                            target: self.core.id.clone(),
                            resp: VoteResponse::new(vote, None, true),
                            candidate_vote: non_committed,
                        })
                        .await
                        .ok();
                }
            }
            Command::PurgeLog { upto } => {
                let (tx, rx) = C::oneshot();
                self.durability.publish(DurabilityOp::Purge { upto: upto.clone(), done: tx });
                Self::await_completion(rx).await?;
                let leader_id = self.core.current_leader();
                let leader_node = self.core.get_leader_node(leader_id.clone());
                for (log_index, tx) in self.core.client_responders.drain_upto(upto.index()) {
                    tx.on_complete(Err(ClientWriteError::ForwardToLeader(ForwardToLeader {
                        leader_id: leader_id.clone(),
                        leader_node: leader_node.clone(),
                    })));
                    tracing::debug!("sent ForwardToLeader for purged log_index: {}", log_index);
                }
                self.core.engine.state.io_state_mut().update_purged(Some(upto));
            }
            Command::TruncateLog { after } => {
                let (tx, rx) = C::oneshot();
                self.durability.publish(DurabilityOp::Truncate { after: after.clone(), done: tx });
                Self::await_completion(rx).await?;
                let leader_id = self.core.current_leader();
                let leader_node = self.core.get_leader_node(leader_id.clone());
                for (log_index, tx) in self.core.client_responders.drain_from(after.next_index()) {
                    tx.on_complete(Err(ClientWriteError::ForwardToLeader(ForwardToLeader {
                        leader_id: leader_id.clone(),
                        leader_node: leader_node.clone(),
                    })));
                    tracing::debug!("sent ForwardToLeader for log_index: {}", log_index);
                }
            }
            Command::SaveCommittedAndApply { already_applied: already_committed, upto } => {
                self.core.runtime_stats.record_log_stage_now(Stage::Committed, upto.index() + 1);
                self.core.engine.state.apply_progress_mut().submit(upto.clone());
                // Fire-and-forget: `save_committed` is optional/advisory (recovery optimization)
                // and apply does not depend on it being durable; FIFO consumer order keeps the
                // persisted committed marker monotonic.
                self.durability.publish(DurabilityOp::SaveCommitted { committed: Some(upto.clone()) });
                // No on-loop wait here: the sm worker's log reader is a `GatedLogReader` whose
                // readability watermark is updated by the durability consumer after each `Append`.
                // The gate blocks the sm worker's `entries_stream` reads until the consumer has
                // completed all preceding Append ops, keeping the wait off the consensus loop.
                let first = self.core.engine.state.get_log_id(already_committed.next_index()).unwrap();
                self.core.apply_to_state_machine(first, upto).await?;
            }
            Command::StateMachine { command } => {
                let io_id = command.get_log_progress();
                if let Some(io_id) = io_id {
                    self.core.engine.state.log_progress_mut().submit(io_id);
                }
                if let Some(log_id) = command.get_apply_progress() {
                    self.core.engine.state.apply_progress_mut().submit(log_id);
                }
                if let Some(log_id) = command.get_snapshot_progress() {
                    self.core.engine.state.snapshot_progress_mut().submit(log_id);
                }
                self.core.sm_handle
                    .send(command)
                    .await
                    .map_err(|_e| StorageError::write_state_machine(C::err_from_string("cannot send to sm::Worker")))?;
            }

            // ---- Phase 3c.1: replication driven by per-peer network consumers --------------
            Command::Replicate { req, target } => {
                if let Some(peer) = self.peers.get_mut(&target) {
                    peer.publish(NetOp::Replicate { req });
                } else {
                    // The engine emits Replicate only after RebuildReplicationStreams created the
                    // peer; a missing peer means a teardown race — drop the op (the engine
                    // re-drives after the next Rebuild).
                    tracing::warn!("Replicate to target {} with no peer consumer; dropping op", target);
                }
            }
            Command::ReplicateSnapshot { target, inflight_id, .. } => {
                // Port of `RaftCore::ReplicateSnapshot`: hand the snapshot send to the target's
                // peer consumer (which owns the snapshot reader + cancel/replace). The executor
                // uses its own `leader_vote` (set at spawn), which equals the command's.
                if let Some(peer) = self.peers.get_mut(&target) {
                    peer.publish(NetOp::Snapshot { inflight_id });
                } else {
                    // Like `Replicate`: a missing peer means a teardown race — drop the op (the
                    // engine re-drives after the next Rebuild).
                    tracing::warn!("ReplicateSnapshot to target {} with no peer consumer; dropping op", target);
                }
            }
            Command::BroadcastHeartbeat { session_id } => {
                // Port of `RaftCore::broadcast_heartbeat`: validate the session, compute each
                // peer's `HeartbeatEvent`, then publish one `NetOp::Heartbeat` per peer consumer.
                let events: Vec<(C::NodeId, HeartbeatEvent<C>)> = {
                    let Ok(lh) = self.core.engine.try_leader_handler() else {
                        // No longer a leader — nothing to broadcast.
                        return Ok(None);
                    };

                    let committed_vote = lh.leader.committed_vote.clone();
                    let membership_log_id = lh.state.membership_state.effective().log_id();
                    let current_session_id = ReplicationSessionId::new(committed_vote, membership_log_id.clone());

                    if current_session_id != session_id {
                        // Session changed (leader/membership) — skip heartbeat.
                        return Ok(None);
                    }

                    let cluster_committed = lh.state.cluster_committed().cloned();
                    let now = C::now();
                    lh.leader
                        .progress
                        .iter()
                        .filter(|progress_entry| progress_entry.id != self.core.id)
                        .map(|progress_entry| {
                            (progress_entry.id.clone(), HeartbeatEvent {
                                time: now,
                                matching: progress_entry.val.matching.clone(),
                                cluster_committed: cluster_committed.clone(),
                            })
                        })
                        .collect()
                };

                for (target, event) in events {
                    if let Some(peer) = self.peers.get_mut(&target) {
                        peer.publish(NetOp::Heartbeat { event });
                    }
                }
            }
            Command::CloseReplicationStreams => {
                // Drop every peer consumer handle → producers drop → consumer threads exit.
                // (Heartbeat AND snapshot are folded into the same consumers; dropping a handle
                // sets its `SnapshotCancel::shutdown`, aborting any in-flight snapshot.)
                self.peers.clear();
            }
            Command::RebuildReplicationStreams {
                leader_vote,
                targets,
                close_old_streams,
            } => {
                // Build the new peer set, reusing existing consumers when membership-only change
                // (close_old_streams == false). Mirrors `RaftCore`'s replication-table rebuild.
                // Each consumer owns its own snapshot path (reader + cancel/replace), so there is
                // no longer a parallel `RaftCore.replications` map to maintain.
                let mut new_peers: PeerTable<C> = PeerTable::new();

                for prog in targets.iter() {
                    let reused = match self.peers.remove(&prog.target) {
                        Some(existing) if !close_old_streams => Some(existing),
                        Some(existing) => {
                            // Vote changed: close the old consumer (drop detaches its thread and
                            // aborts any in-flight snapshot via `SnapshotCancel::shutdown`).
                            drop(existing);
                            None
                        }
                        None => None,
                    };

                    let handle = match reused {
                        Some(handle) => handle,
                        None => self.spawn_peer_executor(leader_vote.clone(), prog).await,
                    };

                    new_peers.insert(prog.target.clone(), handle);
                }

                // Targets no longer present are dropped (their peer consumers detach and abort any
                // in-flight snapshot) when the old peer table is replaced.
                self.peers = new_peers;
            }

            // ---- Phase 3c.1 (Task 4): vote / transfer-leader off-loop fan-out --------------
            // Unlike replication, these broadcast to *all voters* during an election (when there
            // are typically no peer consumers — you are a candidate, not a leader), so they fan out
            // via a thin SyncCore-owned helper that `C::spawn`s one task per voter (off the
            // consensus loop), faithfully mirroring `RaftCore::spawn_parallel_vote_requests` /
            // `broadcast_transfer_leader`.
            Command::SendVote { vote_req } => {
                self.spawn_parallel_vote_requests(&vote_req, sync_network::VoteRequestKind::Vote).await;
            }
            Command::SendPreVote { vote_req } => {
                self.spawn_parallel_vote_requests(&vote_req, sync_network::VoteRequestKind::PreVote).await;
            }
            Command::BroadcastTransferLeader { req } => {
                self.broadcast_transfer_leader(req).await;
            }
        }
        Ok(None)
    }

    /// Build and spawn a per-peer network consumer for `prog` (the 3c.1 analog of
    /// `RaftCore::spawn_replication_stream`): create the network client, vend a readability-gated
    /// log reader from the durability consumer, subscribe the leader-global watch channels, and
    /// hand the assembled [`PeerExecutor`] to a busy-spin consumer thread.
    async fn spawn_peer_executor(
        &mut self,
        leader_vote: CommittedVoteOf<C>,
        prog: &TargetProgress<C>,
    ) -> sync_network::PeerConsumerHandle<C> {
        let network = self.core.network_factory.new_client(prog.target.clone(), &prog.target_node).await;

        // Under sync-core the log store lives on the durability consumer; request a fresh gated
        // reader from it (the same reader-vend side-channel `spawn_replication_stream` uses).
        let log_reader = {
            let (tx, rx) = C::oneshot();
            self.core
                .log_reader_request_tx
                .send(tx)
                .expect("log-store consumer alive while spawning peer");
            rx.await.expect("log-store consumer vends a reader")
        };

        // Snapshot reader (handle to the sm worker) for this peer's snapshot path — the
        // equivalent of `RaftCore::ReplicateSnapshot`'s `self.sm_handle.new_snapshot_reader()`,
        // created once per peer consumer and reused for each `NetOp::Snapshot`.
        let snapshot_reader = self.core.sm_handle.new_snapshot_reader();

        // `LS` cannot be inferred through the `VendedReader<C, LS>` associated-type projection,
        // so name the executor type explicitly.
        let executor = PeerExecutor::<C, NF::Network, LS, SM>::new(
            self.core.id.clone(),
            prog.target.clone(),
            leader_vote,
            prog.progress.stream_id,
            self.core.config.clone(),
            self.core.tx_notification.clone(),
            network,
            log_reader,
            self.core.committed_tx.subscribe(),
            self.core.io_accepted_tx.subscribe(),
            self.core.io_submitted_tx.subscribe(),
            self.core.shared_replicate_batch.clone(),
            prog.progress.matching.clone(),
            snapshot_reader,
        );

        let rt_handle = tokio::runtime::Handle::current();
        sync_network::spawn_peer(rt_handle, executor, &prog.target)
    }

    /// Fan out (pre-)vote requests to every other voter — the SyncCore-owned port of
    /// `RaftCore::spawn_parallel_vote_requests`. Each voter's RPC + emit runs **off the consensus
    /// loop** on its own `C::spawn`ed task (the consensus thread has entered the tokio runtime
    /// context, so `C::spawn` has a runtime), so the election round does not serialize on this
    /// loop. The per-voter body is [`sync_network::send_vote_request`], which emits the
    /// `VoteResponse`/`PreVoteResponse` ack contract (nothing on a transport failure).
    ///
    /// Votes broadcast to all voters during an election, when there are typically no replication
    /// peer consumers (you are a candidate, not a leader), so this does not reuse the peer table.
    async fn spawn_parallel_vote_requests(
        &mut self,
        vote_req: &crate::raft::VoteRequest<C>,
        kind: sync_network::VoteRequestKind,
    ) {
        let members = self.core.engine.state.membership_state.effective().voter_ids();
        let ttl = Duration::from_millis(self.core.config.election_timeout_min);

        for target in members {
            if target == self.core.id {
                continue;
            }

            let req = vote_req.clone();
            // Safe unwrap(): target is a voter in the effective membership.
            let target_node =
                self.core.engine.state.membership_state.effective().get_node(&target).unwrap().clone();
            let client = self.core.network_factory.new_client(target.clone(), &target_node).await;
            let tx = self.core.tx_notification.clone();

            // False positive lint (`non-binding let on a future`): rust-clippy#9932.
            #[allow(clippy::let_underscore_future)]
            let _ = C::spawn(sync_network::send_vote_request::<C, NF::Network>(
                client,
                target.clone(),
                req,
                kind,
                ttl,
                tx,
            ));
        }
    }

    /// Fan out a leadership-transfer request to every other voter — the SyncCore-owned port of
    /// `RaftCore::broadcast_transfer_leader`. Like the vote fan-out, each RPC runs off the
    /// consensus loop on a `C::spawn`ed task; the per-voter body is
    /// [`sync_network::send_transfer_leader_request`] (no notification; failures logged).
    async fn broadcast_transfer_leader(&mut self, req: crate::raft::message::TransferLeaderRequest<C>) {
        let voter_ids = self.core.engine.state.membership_state.effective().voter_ids();
        let ttl = Duration::from_millis(self.core.config.election_timeout_min);

        for target in voter_ids {
            if target == self.core.id {
                continue;
            }

            let r = req.clone();
            // Safe unwrap(): target is a voter in the effective membership.
            let target_node =
                self.core.engine.state.membership_state.effective().get_node(&target).unwrap().clone();
            let client = self.core.network_factory.new_client(target.clone(), &target_node).await;

            // False positive lint (`non-binding let on a future`): rust-clippy#9932.
            #[allow(clippy::let_underscore_future)]
            let _ = C::spawn(sync_network::send_transfer_leader_request::<C, NF::Network>(
                client,
                target.clone(),
                r,
                ttl,
            ));
        }
    }

    /// Mirrors `RaftCore::run_engine_commands`: drain and execute the Engine's
    /// emitted commands, delegating per-command execution to `SyncCore::run_command`.
    async fn run_engine_commands(&mut self) -> Result<(), StorageError<C>> {
        self.core.send_satisfied_responds();

        loop {
            self.core.engine.output.sched_commands(&self.core.config);

            let Some(cmd) = self.core.engine.output.pop_command() else {
                break;
            };

            let res = self.run_command(cmd).await?;

            let Some(cmd) = res else {
                continue;
            };

            // Command can't run yet; postpone it.
            if self.core.engine.output.postpone_command(cmd).is_ok() {
                continue;
            }
            break;
        }

        self.run_progress_driven_command().await?;
        Ok(())
    }

    /// Mirrors `RaftCore::run_progress_driven_command`.
    async fn run_progress_driven_command(&mut self) -> Result<(), StorageError<C>> {
        while let Some(cmd) = self.core.engine.next_progress_driven_command() {
            let res = self.run_command(cmd).await?;
            debug_assert!(res.is_none(), "progress driven command should always be executed");
        }
        Ok(())
    }
}
