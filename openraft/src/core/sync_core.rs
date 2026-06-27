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
//! ## Status: v1 scaffold
//!
//! This version **wraps `RaftCore` and delegates** to its loop. Its only job is
//! to establish the seam in `Raft::new` and confirm openraft's own test suite
//! runs unchanged through `--features sync-core`. Subsequent steps move the
//! event loop and command execution into this module so the Engine is driven
//! synchronously.

use crate::core::RaftCore;
use crate::errors::Fatal;
use crate::errors::Infallible;
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

    /// Run the core to completion. v1 delegates to `RaftCore::main`; later
    /// versions own the event loop and command execution here.
    pub(crate) async fn main(self, rx_shutdown: OneshotReceiverOf<C, ()>) -> Result<Infallible, Fatal<C>> {
        self.core.main(rx_shutdown).await
    }
}
