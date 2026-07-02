use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
use openraft::Config;
use openraft::SnapshotPolicy;

use crate::fixtures::RaftRouter;
use crate::fixtures::log_id;
use crate::fixtures::ut_harness;

/// A replication stream whose reads persistently return empty (its range fell below the purge
/// horizon, e.g. the prefix was purged under a live `LogsSince` stream) must escalate to
/// RaftCore instead of retrying forever.
///
/// Without escalation the failure is silent and total (observed on a real cluster,
/// ultima_cluster 2026-07-02):
/// - the stream loops on `limited_get_log_entries -> empty -> heartbeat -> retry` (~100/s),
///   while its healthy heartbeat ACKs keep the leader content, so nothing re-decides;
/// - the target never advances (needs a snapshot but never gets one);
/// - `try_purge_log` sees the stuck inflight as "in use" and postpones purging forever, so the
///   log grows without bound.
///
/// With escalation, RaftCore resets the target's inflight, purge proceeds, and `next_send`
/// correctly re-decides snapshot replication (`searching_end < purge_upto`), after which log
/// replication resumes above the horizon.
///
/// Cluster shape: single voter (node 0) + learner (node 1), so the leader self-commits and the
/// wedged stream cannot mask itself behind a healthy quorum.
#[tracing::instrument]
#[test_harness::test(harness = ut_harness)]
async fn empty_reads_escalate_to_snapshot() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            // Snapshot only when triggered explicitly.
            snapshot_policy: SnapshotPolicy::Never,
            // Purge everything covered by the snapshot.
            max_in_snapshot_log_to_keep: 0,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- initializing single-voter cluster with one learner");
    let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {1}).await?;

    tracing::info!(log_index, "--- write logs; learner replicates normally");
    log_index += router.client_request_many(0, "foo", 10).await?;
    router.wait(&1, timeout()).applied_index(Some(log_index), "learner caught up").await?;

    // Everything written from here on will be invisible to the learner's replication stream:
    // reads starting at or below `horizon` return empty, exactly like a purged prefix.
    let horizon = log_index + 10;
    tracing::info!(
        log_index,
        horizon,
        "--- simulate purge horizon: leader reads below {} return empty",
        horizon + 1
    );
    router.set_empty_limited_get_below(&0, horizon + 1)?;

    tracing::info!(log_index, "--- write more; leader self-commits, learner stream reads empty");
    log_index += router.client_request_many(0, "foo", 10).await?;
    assert_eq!(horizon, log_index);
    router.wait(&0, timeout()).applied_index(Some(log_index), "leader self-committed").await?;

    tracing::info!(log_index, "--- trigger snapshot; purge_upto advances to the horizon");
    let n0 = router.get_raft_handle(&0)?;
    n0.trigger().snapshot().await?;
    router.wait(&0, timeout()).snapshot(log_id(1, 0, log_index), "leader snapshot").await?;

    // The learner can only reach `log_index` via snapshot replication: the log entries it
    // needs are unreadable (below the horizon). Without stream escalation this waits forever.
    tracing::info!(log_index, "--- learner must recover via snapshot");
    router
        .wait(&1, Some(Duration::from_millis(10_000)))
        .applied_index(Some(log_index), "learner recovered via snapshot")
        .await?;

    // Purge must also have been unblocked (the wedged inflight was postponing it).
    router
        .wait(&0, timeout())
        .purged(Some(log_id(1, 0, log_index)), "leader purged up to the snapshot")
        .await?;

    tracing::info!(log_index, "--- subsequent logs replicate normally (reads above the horizon)");
    log_index += router.client_request_many(0, "foo", 5).await?;
    router.wait(&1, timeout()).applied_index(Some(log_index), "learner replicates tail").await?;

    Ok(())
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(2_000))
}
