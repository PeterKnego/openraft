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
//! Still delegated to `RaftCore`: the 8 task-spawning / network commands
//! (`SendVote`, `SendPreVote`, `BroadcastHeartbeat`, `Replicate`,
//! `ReplicateSnapshot`, `BroadcastTransferLeader`, `CloseReplicationStreams`,
//! `RebuildReplicationStreams`) and the two engine-driving per-message handlers
//! (`handle_api_msg`, `handle_notification`). Phase 3c relocates these.
//!
//! Validated by running openraft's own integration suite with
//! `--features sync-core`.

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
use crate::core::notification::Notification;
use crate::core::stage::Stage;
use crate::engine::Command;
use crate::entry::RaftEntry;
use crate::errors::ClientWriteError;
use crate::errors::Fatal;
use crate::errors::ForwardToLeader;
use crate::errors::Infallible;
use crate::log_id::option_raft_log_id_ext::OptionRaftLogIdExt;
use crate::network::RaftNetworkFactory;
use crate::raft::responder::Responder;
use crate::raft::VoteResponse;
use crate::raft_state::io_state::io_id::IOId;
use crate::raft_state::LogStateReader;
use crate::rt::MpscSender;
use crate::runtime::RaftRuntime;
use crate::storage::RaftLogStorage;
use crate::type_config::TypeConfigExt;
use crate::type_config::alias::OneshotReceiverOf;
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
    ) -> Self {
        // The durability consumer becomes the sole owner of `log_store`.
        let log_store = core.log_store.take().expect("log_store present at SyncCore construction");
        let durability = sync_durability::spawn(log_store, reader_rx);
        Self { core, durability }
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

    /// Per-command execution. Phase 3b.1: the task-spawning/network commands are
    /// delegated to `RaftCore::run_command` (Phase 3c relocates them); the
    /// storage/apply/pure-sync commands are moved inline in later tasks. For now
    /// everything delegates — this task just establishes SyncCore as the dispatch
    /// point.
    async fn run_command(&mut self, cmd: Command<C, SM>) -> Result<Option<Command<C, SM>>, StorageError<C>> {
        // Task-spawning / network commands stay RaftCore's for now (Phase 3c).
        // Use `matches!` (a transient borrow) rather than `match &cmd { .. => return
        // self.core.run_command(cmd) }` — the latter moves `cmd` while the `&cmd`
        // scrutinee borrow is still live, which does not compile.
        let delegate = matches!(
            &cmd,
            Command::SendVote { .. }
                | Command::SendPreVote { .. }
                | Command::BroadcastHeartbeat { .. }
                | Command::Replicate { .. }
                | Command::ReplicateSnapshot { .. }
                | Command::BroadcastTransferLeader { .. }
                | Command::CloseReplicationStreams
                | Command::RebuildReplicationStreams { .. }
        );
        if delegate {
            return self.core.run_command(cmd).await;
        }

        // Owned commands: condition gate + stats, then execute inline.
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
                // Uniform-await (Task 1): publish + await *submission*. The consumer builds the
                // `IOFlushed` callback from `io_id` + `tx_io_completed` and signals `done` once
                // `append` returns, which the contract guarantees is the *readable* point. We
                // await that before returning, restoring the original invariant "submitted ⇒
                // entries readable before this arm returns" — so a later `Replicate` (gated on
                // the consensus *submitted* marker) cannot read not-yet-readable entries across
                // the consensus→consumer thread boundary. Flush completion still flows via the
                // callback → forwarder → `Notification::LocalIO` path, unchanged.
                //
                // TASK 2 (AppendEntries pipelined) reintroduces fire-and-forget here, but MUST
                // first add a real readability gate (e.g. gate `Replicate` on the consumer's
                // append-submit signal, not just the consensus `submit` marker) before dropping
                // this await — that is the invariant this await currently upholds.
                let (tx, rx) = C::oneshot();
                self.durability.publish(DurabilityOp::Append {
                    entries,
                    io_id,
                    tx_io_completed: self.core.tx_io_completed.clone(),
                    done: tx,
                });
                Self::await_completion(rx).await?;
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
                let (tx, rx) = C::oneshot();
                self.durability.publish(DurabilityOp::SaveCommitted { committed: Some(upto.clone()), done: tx });
                Self::await_completion(rx).await?;
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
            // All owned commands are explicit above; task-spawning commands returned
            // via the by-ref delegate block at the top of run_command.
            _ => unreachable!("task-spawning commands are delegated before this match"),
        }
        Ok(None)
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
