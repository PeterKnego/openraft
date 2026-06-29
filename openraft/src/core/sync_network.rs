//! Phase 3c.1 — the **network-consumer scaffold**: per-peer disruptor ring +
//! busy-spin consumer thread + lifecycle handle.
//!
//! This module is the structural mirror of [`sync_durability`](super::sync_durability):
//!  - One **`NetEvent<C>` slot** per ring cell (`Mutex<Option<NetOp<C>>>` — same
//!    move-only pattern, disruptor only lends `&event`).
//!  - One **`PeerConsumerHandle<C>`** per peer: holds the `SingleProducer` and
//!    a `JoinHandle`; `Drop` drops the producer first (signalling `Polling::Shutdown`)
//!    then joins the thread.
//!  - **`spawn_peer`** creates the ring, enters the tokio runtime context on the thread
//!    (the "hybrid reactor" — so later tasks' quinn I/O finds a driver), and starts
//!    the busy-spin `consumer_loop`.
//!
//! **Task 1: inert scaffold.** The `consumer_loop` is a **no-op executor**: it drains
//! the ring and drops every `NetOp`.  Nothing in `SyncCore::run_command` routes to it
//! yet — the 8 replication commands still fully delegate to `RaftCore`.  Subsequent
//! tasks port the actual `ReplicationCore` logic here.
//!
//! All public items are dead code until Tasks 2-4 wire them up; `#[allow(dead_code)]`
//! suppresses the scaffold warnings without restructuring anything outside this task.

// Scaffold: every item in this module is dead until Tasks 2-4 wire it up.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Mutex;
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
use crate::progress::inflight_id::InflightId;
use crate::raft::message::VoteRequest;
use crate::raft::message::TransferLeaderRequest;
use crate::replication::replicate::Replicate;
use crate::replication::ReplicationSessionId;
use crate::type_config::alias::LogIdOf;

/// One network op routed to a per-peer consumer.
///
/// Task 1 only needs `Replicate` to be active; the remaining variants are declared so the
/// enum compiles and later tasks can flesh out their payloads without breaking changes.
#[allow(dead_code)]
pub(crate) enum NetOp<C>
where C: RaftTypeConfig
{
    /// Replicate log entries (or a snapshot) to this peer.
    Replicate { req: Replicate<C> },

    /// Broadcast a heartbeat with the latest committed log id.
    // Tasks 2-4 flesh out the payload; declared here so the variant exists.
    Heartbeat {
        session_id: ReplicationSessionId<C>,
        committed: Option<LogIdOf<C>>,
    },

    /// Transmit a snapshot to this peer.
    Snapshot { inflight_id: InflightId },

    /// Send a (pre-)vote request to this peer.
    Vote { req: VoteRequest<C> },

    /// Forward a leadership-transfer request to this peer.
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
        // Drop the producer first → consumer observes `Polling::Shutdown` and exits.
        self.producer.take();
        // Then join so the consumer thread is clean before we return.
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

/// Map from peer node-id to its consumer handle. Held by `SyncCore` (unused until Task 2).
pub(crate) type PeerTable<C> = BTreeMap<<C as RaftTypeConfig>::NodeId, PeerConsumerHandle<C>>;

/// Spawn the per-peer busy-spin network consumer.
///
/// The thread enters the tokio runtime context (`rt_handle.enter()`) so that later
/// tasks' quinn I/O (which requires a runtime driver) works correctly — the same
/// "hybrid reactor" approach as the consensus thread in `raft/mod.rs`.
///
/// Task 1: the consumer is a no-op; no network client or config is passed yet.
/// Tasks 2-4 will extend this signature with the actual network client and session
/// parameters.
pub(crate) fn spawn_peer<C>(rt_handle: tokio::runtime::Handle) -> PeerConsumerHandle<C>
where C: RaftTypeConfig
{
    let factory = || NetEvent::<C> { op: Mutex::new(None) };
    // Power-of-two ring; 64 slots is generous for burst replication ops per peer.
    let (poller, builder) = build_single_producer(64, factory, BusySpin).new_event_poller();
    let producer = builder.build();

    let join = std::thread::Builder::new()
        .name("openraft-sync-net-peer".to_string())
        .spawn(move || {
            // Enter the tokio runtime context so downstream quinn I/O finds a driver.
            let _enter = rt_handle.enter();
            consumer_loop::<C>(poller);
        })
        .expect("failed to spawn peer network consumer thread");

    PeerConsumerHandle {
        producer: Some(producer),
        join: Some(join),
    }
}

/// No-op consumer loop (Task 1). Drains the ring and drops every `NetOp`; yields
/// when idle to avoid starving the tokio runtime threads in the test suite.
/// Exits on `Polling::Shutdown` (producer dropped).
fn consumer_loop<C>(mut poller: EventPoller<NetEvent<C>, SingleProducerBarrier>)
where C: RaftTypeConfig
{
    loop {
        match poller.poll() {
            Ok(mut events) => {
                for event in &mut events {
                    // Take the op out of the slot — then drop it (no-op executor).
                    let _op = event.op.lock().unwrap().take();
                    // Task 2+ will call run_op(op) here.
                }
            }
            Err(Polling::NoEvents) => {
                // Reactor-free idle: yield rather than peg the core.
                std::thread::yield_now();
            }
            Err(Polling::Shutdown) => return,
        }
    }
}

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

    /// Pins the per-peer ring + FIFO wiring before any executor logic is added.
    ///
    /// Uses the real disruptor API and the real reactor-free `block_on` from
    /// `sync_durability`.  A stub consumer receives 3 `NetOp::Replicate`-tagged events,
    /// drives each through `block_on` (trivially, mirroring the future async-op path),
    /// and reports the inflight ids back over an `mpsc`.  Asserts FIFO delivery order.
    #[test]
    fn ring_delivers_replicate_ops_in_fifo_order() {
        // Build the ring using the real NetEvent<TC> slot type.
        let factory = || NetEvent::<TC> { op: Mutex::new(None) };
        let (mut poller, builder) = build_single_producer(64, factory, BusySpin).new_event_poller();
        let mut producer = builder.build();

        let (reply_tx, reply_rx) = mpsc::channel::<u64>();

        // Stub consumer: receives NetOp::Replicate, drives the id extraction through
        // block_on (mirrors the future async run_op path), reports id back over mpsc.
        let consumer = std::thread::spawn(move || loop {
            match poller.poll() {
                Ok(mut events) => {
                    for event in &mut events {
                        let op = event.op.lock().unwrap().take();
                        if let Some(NetOp::Replicate { req }) = op {
                            // Use block_on to mirror the reactor-free async-op path.
                            let id = block_on(async move { *req.inflight_id });
                            reply_tx.send(id).ok();
                        }
                    }
                }
                Err(Polling::NoEvents) => std::thread::yield_now(),
                Err(Polling::Shutdown) => return,
            }
        });

        // Publish 3 Replicate ops with distinct inflight ids 1, 2, 3.
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

        // Assert FIFO order: 1, 2, 3.
        let received: Vec<u64> = (0..3).map(|_| reply_rx.recv().unwrap()).collect();
        assert_eq!(received, [1, 2, 3], "ring delivers Replicate ops in FIFO order");

        // Drop producer → consumer sees Shutdown and exits.
        drop(producer);
        consumer.join().unwrap();
    }
}
