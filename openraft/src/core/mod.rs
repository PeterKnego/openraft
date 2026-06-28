//! Core Raft runtime and orchestration.
//!
//! This module contains [`RaftCore`], the runtime that supports the Raft algorithm implementation
//! ([`Engine`](crate::engine::Engine)).
//!
//! ## Architecture
//!
//! [`RaftCore`] acts as the bridge between the application and the Raft consensus algorithm:
//!
//! - **Event Processing**: Receives events from application, timer, or network and passes them to
//!   [`Engine`](crate::engine::Engine)
//! - **Command Execution**: Executes [`Command`](crate::engine::Command)s emitted by
//!   [`Engine`](crate::engine::Engine) to:
//!   - Apply Raft state to underlying storage
//!   - Forward messages to other Raft nodes
//!
//! ## Key Types
//!
//! - [`RaftCore`] - Main runtime orchestrator
//! - [`ServerState`] - Server state (Leader, Follower, Candidate, Learner)
//! - [`ApplyResult`] - Result of applying log entries to state machine
//!
//! See the [Engine/Runtime architecture guide](crate::docs::components::engine_runtime) for
//! details.

pub(crate) mod balancer;
pub(crate) mod core_state;
pub(crate) mod heartbeat;
pub(crate) mod io_flush_tracking;
pub(crate) mod merged_raft_msg_receiver;
pub(crate) mod notification;
pub(crate) mod raft_msg;
pub(crate) mod runtime_stats;
pub(crate) mod sm;
pub(crate) mod stage;

mod client_responder_queue;
mod notification_name;
mod raft_core;
mod replication_state;
mod server_state;
mod shared_replicate_batch;
mod step_down_watcher;
#[cfg(feature = "sync-core")]
mod sync_core;
#[cfg(feature = "sync-core")]
mod sync_durability;
mod tick;

pub(crate) use client_responder_queue::ClientResponderQueue;
#[cfg(feature = "sync-core")]
pub(crate) use sync_core::SyncCore;
#[cfg(feature = "sync-core")]
pub(crate) use sync_durability::GatedLogReader;
#[cfg(feature = "sync-core")]
pub(crate) use sync_durability::VendedReader;
pub use notification_name::NotificationName;
pub(crate) use raft_core::ApplyResult;
pub use raft_core::RaftCore;
pub(crate) use replication_state::replication_lag;
pub use server_state::ServerState;
pub(crate) use shared_replicate_batch::SharedReplicateBatch;
pub(crate) use step_down_watcher::StepDownWatcher;
pub(crate) use tick::Tick;
pub(crate) use tick::TickHandle;
