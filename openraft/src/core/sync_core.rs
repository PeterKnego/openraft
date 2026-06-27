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
//! ## Status: v2 — owns the event loop
//!
//! `SyncCore` now **owns the `do_main` + `runtime_loop` orchestration** (the
//! select over shutdown / notifications / API messages, the command-run
//! ordering, the budget balancer, startup and shutdown-metrics handling). It
//! still delegates the *per-message handlers* and *command execution* to the
//! proven `RaftCore` helpers (`handle_notification`, `process_raft_msg`,
//! `process_notification`, `run_engine_commands`). Those are what the next step
//! replaces with synchronous, ring-dispatched execution — but the loop is ours.
//!
//! Validated by running openraft's own integration suite with
//! `--features sync-core`.

use futures_util::FutureExt;

use crate::async_runtime::MpscReceiver;
use crate::async_runtime::watch::WatchSender;
use crate::core::RaftCore;
use crate::core::ServerState;
use crate::core::balancer::Balancer;
use crate::errors::Fatal;
use crate::errors::Infallible;
use crate::StorageError;
use crate::runtime::RaftRuntime;
use crate::network::RaftNetworkFactory;
use crate::storage::RaftLogStorage;
use crate::type_config::alias::OneshotReceiverOf;
use crate::RaftTypeConfig;

/// Synchronous alternative to [`RaftCore`]. See module docs.
pub(crate) struct SyncCore<C, NF, LS, SM>
where
    C: RaftTypeConfig,
    NF: RaftNetworkFactory<C>,
    LS: RaftLogStorage<C>,
{
    core: RaftCore<C, NF, LS, SM>,
}

impl<C, NF, LS, SM> SyncCore<C, NF, LS, SM>
where
    C: RaftTypeConfig,
    NF: RaftNetworkFactory<C>,
    LS: RaftLogStorage<C>,
    SM: 'static,
{
    pub(crate) fn new(core: RaftCore<C, NF, LS, SM>) -> Self {
        Self { core }
    }

    /// Entry point. Mirrors `RaftCore::main`: run `do_main`, then flush metrics
    /// and publish the shutdown state.
    pub(crate) async fn main(mut self, rx_shutdown: OneshotReceiverOf<C, ()>) -> Result<Infallible, Fatal<C>> {
        let res = self.do_main(rx_shutdown).await;

        // Flush buffered metrics.
        self.core.flush_metrics();

        // Safe unwrap: res is Result<Infallible, _>.
        let err = res.unwrap_err();
        match err {
            Fatal::Stopped => { /* Normal quit */ }
            _ => tracing::error!("SyncCore::main error: {}", err),
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
    /// commands, then enter the runtime loop.
    async fn do_main(&mut self, rx_shutdown: OneshotReceiverOf<C, ()>) -> Result<Infallible, Fatal<C>> {
        tracing::debug!("SyncCore is initializing");

        self.core.engine.startup();
        self.run_engine_commands().await?;
        self.core.flush_metrics();

        self.runtime_loop(rx_shutdown).await
    }

    /// The event loop. This is the orchestration `SyncCore` owns — the next step
    /// replaces the async `select`/command execution with a synchronous ring
    /// pipeline. Today it mirrors `RaftCore::runtime_loop`, delegating the
    /// per-message handlers and command execution to the proven `RaftCore`
    /// helpers.
    async fn runtime_loop(&mut self, mut rx_shutdown: OneshotReceiverOf<C, ()>) -> Result<Infallible, Fatal<C>> {
        let mut balancer = Balancer::new(10_000);

        loop {
            self.core.flush_metrics();

            // Block until any channel has a message; check shutdown first.
            futures_util::select_biased! {
                _ = (&mut rx_shutdown).fuse() => {
                    tracing::info!("recv from rx_shutdown");
                    return Err(Fatal::Stopped);
                }

                notify_res = self.core.rx_notification.recv().fuse() => {
                    match notify_res {
                        Some(notify) => self.core.handle_notification(notify)?,
                        None => {
                            tracing::error!("all rx_notify senders are dropped");
                            return Err(Fatal::Stopped);
                        }
                    };
                }

                msg_res = self.core.rx_api.ensure_buffered().fuse() => {
                    msg_res?;
                }
            };

            self.run_engine_commands().await?;

            // Drain channels one by one, bounded by the balancer's budgets.
            let raft_msg_processed = self.core.process_raft_msg(balancer.raft_msg()).await?;
            let notify_processed = self.core.process_notification(balancer.notification()).await?;

            #[allow(clippy::collapsible_else_if)]
            if notify_processed == balancer.notification() {
                balancer.increase_notification();
            } else {
                if raft_msg_processed == balancer.raft_msg() {
                    balancer.increase_raft_msg();
                }
            }

            self.core.trigger_routine_actions();
            self.run_engine_commands().await?;
        }
    }

    /// Mirrors `RaftCore::run_engine_commands`: drain and execute the Engine's
    /// emitted commands, delegating per-command execution to `RaftCore::run_command`.
    async fn run_engine_commands(&mut self) -> Result<(), StorageError<C>> {
        self.core.send_satisfied_responds();

        loop {
            self.core.engine.output.sched_commands(&self.core.config);

            let Some(cmd) = self.core.engine.output.pop_command() else {
                break;
            };

            let res = self.core.run_command(cmd).await?;

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
            let res = self.core.run_command(cmd).await?;
            debug_assert!(res.is_none(), "progress driven command should always be executed");
        }
        Ok(())
    }
}
