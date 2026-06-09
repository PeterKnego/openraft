//! End-to-end reproduction of the apply-worker drain debug-assert
//! (`assert_eq!(end - 1, got_last_index)` at `openraft/src/core/sm/worker.rs:214`)
//! inside a *running* single-node Raft cluster.
//!
//! Unlike the focused unit test in `openraft/src/core/sm/worker.rs`, this drives a real
//! [`Raft`] instance through the full `RaftCore -> state-machine worker -> apply` pipeline.
//! It wraps the in-memory log store with [`ShortReadStore`], whose log reader performs a
//! *contract-compliant* short read on the apply path: it omits the entry at the end of the
//! requested range. Per the documented `RaftLogReader::try_get_log_entries` contract
//! ("the absence of an entry is tolerated only at the beginning or end of the range"), this is
//! legal. Yet it trips the worker's stricter debug-assert, which is the openraft-side bug.
//!
//! Detection: the assert panics inside the spawned state-machine-worker task (it does not
//! propagate to the test thread), so we install a panic hook that records any panic originating
//! in `worker.rs`. The test passes iff that assert fired during a real client write.
//!
//! The assert is `#[cfg(debug_assertions)]`-only, so the whole test is gated the same way.

#![cfg(debug_assertions)]

use std::fmt::Debug;
use std::future::Future;
use std::io;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures::Stream;
use futures::stream;
use maplit::btreeset;
use openraft::AsyncRuntime;
use openraft::Config;
use openraft::OptionalSend;
use openraft::Raft;
use openraft::RaftTypeConfig;
use openraft::ServerState;
use openraft::Vote;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::SnapshotOf;
use openraft::errors::RPCError;
use openraft::errors::ReplicationClosed;
use openraft::errors::StreamingError;
use openraft::network::RPCOption;
use openraft::network::RaftNetworkFactory;
use openraft::network::v2::RaftNetworkV2;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::storage::IOFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use openraft::type_config::TypeConfigExt;
use openraft_memstore::ClientRequest;
use openraft_memstore::MemLogStore;
use openraft_memstore::TypeConfig;

type C = TypeConfig;

/// A log reader that delegates to the in-memory store, but performs a single legal short read:
/// once `arm` is set, the next `entries_stream` drops the entry at the end of the range.
#[derive(Clone)]
struct ShortReadReader {
    inner: Arc<MemLogStore>,
    arm: Arc<AtomicBool>,
}

impl RaftLogReader<C> for ShortReadReader {
    async fn try_get_log_entries<RB>(&mut self, range: RB) -> Result<Vec<EntryOf<C>>, io::Error>
    where RB: RangeBounds<u64> + Clone + Debug + OptionalSend {
        self.inner.try_get_log_entries(range).await
    }

    async fn read_vote(&mut self) -> Result<Option<openraft::alias::VoteOf<C>>, io::Error> {
        self.inner.read_vote().await
    }

    /// Apply reads its batch through this method. We mimic the default implementation but, when
    /// armed, drop the last entry — a short read that the `try_get_log_entries` contract permits
    /// "at the end of the range".
    async fn entries_stream<RB>(
        &mut self,
        range: RB,
    ) -> impl Stream<Item = Result<EntryOf<C>, io::Error>> + OptionalSend
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        let res = self.inner.try_get_log_entries(range).await;

        let boxed: stream::BoxStream<'static, Result<EntryOf<C>, io::Error>> = match res {
            Ok(mut entries) => {
                if self.arm.swap(false, Ordering::SeqCst) {
                    // Legal short read: omit the entry at the end of the range.
                    entries.pop();
                }
                Box::pin(stream::iter(entries.into_iter().map(Ok)))
            }
            Err(e) => Box::pin(stream::iter([Err(e)])),
        };
        boxed
    }
}

/// A log storage that wraps the in-memory store and hands out [`ShortReadReader`]s.
///
/// Every `RaftLogStorage` method just forwards to the inner store; only `get_log_reader` is
/// interesting — it injects the short-read behaviour into the apply path.
#[derive(Clone)]
struct ShortReadStore {
    inner: Arc<MemLogStore>,
    arm: Arc<AtomicBool>,
}

impl RaftLogStorage<C> for ShortReadStore {
    type LogReader = ShortReadReader;

    async fn get_log_state(&mut self) -> Result<LogState<C>, io::Error> {
        self.inner.get_log_state().await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        ShortReadReader {
            inner: self.inner.clone(),
            arm: self.arm.clone(),
        }
    }

    async fn save_vote(&mut self, vote: &openraft::alias::VoteOf<C>) -> Result<(), io::Error> {
        self.inner.save_vote(vote).await
    }

    async fn save_committed(&mut self, committed: Option<LogIdOf<C>>) -> Result<(), io::Error> {
        self.inner.save_committed(committed).await
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<C>>, io::Error> {
        self.inner.read_committed().await
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<C>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        self.inner.append(entries, callback).await
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogIdOf<C>>) -> Result<(), io::Error> {
        self.inner.truncate_after(last_log_id).await
    }

    async fn purge(&mut self, log_id: LogIdOf<C>) -> Result<(), io::Error> {
        self.inner.purge(log_id).await
    }
}

/// A network factory that is never exercised: a single-node cluster sends no RPCs.
#[derive(Clone)]
struct NoNetwork;

impl RaftNetworkFactory<C> for NoNetwork {
    type Network = NoNetwork;

    async fn new_client(&mut self, _target: u64, _node: &()) -> Self::Network {
        NoNetwork
    }
}

impl RaftNetworkV2<C> for NoNetwork {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<C>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<C>, RPCError<C>> {
        unreachable!("single-node cluster sends no AppendEntries RPCs")
    }

    async fn vote(&mut self, _rpc: VoteRequest<C>, _option: RPCOption) -> Result<VoteResponse<C>, RPCError<C>> {
        unreachable!("single-node cluster sends no Vote RPCs")
    }

    async fn full_snapshot(
        &mut self,
        _vote: Vote<<C as RaftTypeConfig>::LeaderId>,
        _snapshot: SnapshotOf<C>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<openraft::raft::SnapshotResponse<C>, StreamingError<C>> {
        unreachable!("single-node cluster sends no snapshot RPCs")
    }
}

#[test]
fn apply_drain_assert_fires_in_running_cluster() {
    // Capture panics originating in the state-machine worker (the assert panics in a spawned
    // task and does not propagate to this thread).
    let captured = Arc::new(Mutex::new(Vec::<String>::new()));
    let sink = captured.clone();
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let loc = info.location().map(|l| l.file().to_string()).unwrap_or_default();
        let msg = info.to_string();
        if loc.contains("worker.rs") {
            // The expected apply-worker assert: record it, and stay quiet to keep output clean.
            sink.lock().unwrap().push(format!("{loc} :: {msg}"));
        } else {
            // Anything else is unexpected (e.g. a setup failure) — surface it for debugging.
            eprintln!("[unexpected panic] {loc} :: {msg}");
        }
    }));

    let (log_inner, sm) = openraft_memstore::new_mem_store();
    let arm = Arc::new(AtomicBool::new(false));
    let store = ShortReadStore {
        inner: log_inner,
        arm: arm.clone(),
    };

    let poll_sink = captured.clone();
    let assert_fired = <C as RaftTypeConfig>::AsyncRuntime::run(async move {
        let config = Arc::new(
            Config {
                heartbeat_interval: 100,
                election_timeout_min: 200,
                election_timeout_max: 400,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );

        let raft = Raft::new(1u64, config, NoNetwork, store, sm).await.unwrap();

        // Bootstrap a single-node cluster: the membership entry is committed and applied at index 1
        // (this openraft build does not append a leader blank no-op entry).
        raft.initialize(btreeset! {1u64}).await.unwrap();
        raft.wait(Some(Duration::from_secs(3)))
            .state(ServerState::Leader, "become single-node leader")
            .await
            .unwrap();
        raft.wait(Some(Duration::from_secs(3)))
            .applied_index_at_least(Some(1), "bootstrap membership applied")
            .await
            .unwrap();

        // Arm the short read so the *next* apply (the client write below) gets a stream that is
        // missing its final entry.
        arm.store(true, Ordering::SeqCst);

        // Issuing this write commits a Normal entry at index 2 and applies range [2, 3). The armed
        // reader drops the tail, so `apply` drains a short (empty) stream, `got_last_index` never
        // reaches `end - 1`, and the worker's debug-assert panics — killing the apply worker.
        //
        // We fire it on a background task and do NOT await it: once the apply worker dies, the write
        // never completes (RaftCore does not promptly fail the in-flight request), so awaiting it
        // would hang. The decisive signal is the captured worker panic, which we poll for instead.
        let writer = raft.clone();
        C::spawn(async move {
            let _ = writer
                .client_write(ClientRequest {
                    client: "c1".to_string(),
                    serial: 1,
                    status: "hello".to_string(),
                })
                .await;
        });

        // Poll up to ~4s for the apply-worker assert to fire.
        let mut fired = false;
        for _ in 0..200 {
            if poll_sink.lock().unwrap().iter().any(|s| s.contains("worker.rs") && s.contains("left == right")) {
                fired = true;
                break;
            }
            C::sleep(Duration::from_millis(20)).await;
        }
        fired
    });

    std::panic::set_hook(prev_hook);

    let captured_panics = captured.lock().unwrap().clone();

    // The decisive check: the apply-worker drain debug-assert fired in the real running cluster,
    // triggered by a contract-compliant short read at the end of the apply range.
    assert!(
        assert_fired,
        "expected the apply-drain debug-assert (worker.rs `assert_eq!(end-1, got_last_index)`) to fire \
         during a real client write; captured worker panics: {captured_panics:?}"
    );
}
