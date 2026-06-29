//! Commit→apply latency microbench: SyncCore (synchronous consensus loop) vs RaftCore
//! (async). Isolates the *software choreography* cost of the commit→apply path.
//!
//! It drives a single client sequentially (inflight = 1) against an in-memory cluster
//! (no fsync, no real network), so the measured per-op latency is essentially the Raft
//! consensus orchestration cost — the bucket the SyncCore work targets. Build twice to
//! A/B (the cluster is built through SyncCore iff the `sync-core` feature is on):
//!
//!   cargo run --release -p bench-minimal --bin commit_latency                     # RaftCore (async)
//!   cargo run --release -p bench-minimal --bin commit_latency --features sync-core # SyncCore (sync)
//!
//! `--members 1` (default) isolates the consensus BASE bucket (no replication).
//! `--members 3` adds replication — but in the current spike replication is still the
//! async RaftCore path (only the consensus *loop* is synchronous), so expect a smaller
//! relative delta there until 3c.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use bench_minimal::network::BenchRaft;
use bench_minimal::network::Router;
use bench_minimal::store::ClientRequest;
use clap::Parser;
use openraft::Config;
use tokio::runtime::Builder;
use tokio::runtime::Runtime;

/// Which core this binary was built against — selected at compile time by the feature.
const CORE_LABEL: &str = if cfg!(feature = "sync-core") {
    "SyncCore(sync)"
} else {
    "RaftCore(async)"
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Raft cluster members. 1 isolates the consensus base; 3/5 add replication.
    #[arg(short = 'm', long, default_value_t = 1)]
    members: u64,

    /// Measured operations (recorded after warmup).
    #[arg(short = 'n', long, default_value_t = 50_000)]
    operations: u64,

    /// Warmup operations (discarded — lets the cluster settle + caches warm).
    #[arg(short = 'w', long, default_value_t = 5_000)]
    warmup: u64,

    /// Server runtime worker threads (the Raft nodes run here).
    #[arg(long, default_value_t = 8)]
    server_workers: usize,

    /// Concurrent sequential clients. 1 = pure inflight-1 latency; >1 also reports
    /// aggregate throughput.
    #[arg(short = 'c', long, default_value_t = 1)]
    concurrency: u64,

    /// Injected per-commit committed-marker fsync latency (µs). Models the one hot-path
    /// durability op the 3d redesign differentiates on: RaftCore awaits it inline (on the
    /// commit critical path); SyncCore overlaps it off the consensus loop. Sweep this to find
    /// where SyncCore crosses over RaftCore. 0 = off (the zero-latency baseline).
    #[arg(long, default_value_t = 0)]
    fsync_us: u64,

    /// Injected per-RPC network RTT (µs) on the in-process router (multi-node only; common-mode
    /// between cores in 3d). 0 = off.
    #[arg(long, default_value_t = 0)]
    rtt_us: u64,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Publish the injected-latency knobs to the in-memory mocks (read at store/router
    // construction in create_cluster, below). SAFETY: set before any runtime or thread is
    // spawned — this is single-threaded process startup.
    unsafe {
        std::env::set_var("BENCH_FSYNC_US", args.fsync_us.to_string());
        std::env::set_var("BENCH_RTT_US", args.rtt_us.to_string());
    }

    let members: BTreeSet<u64> = (0..args.members).collect();

    // Server runtime owns the Raft nodes. With sync-core, each node's consensus loop
    // runs on its own std::thread that enters *this* runtime's context (hybrid reactor).
    let server_rt = Builder::new_multi_thread()
        .worker_threads(args.server_workers)
        .enable_all()
        .thread_name("bench-server")
        .build()?;

    let (router, leader) = create_cluster(&server_rt, members)?;

    let client_rt = Builder::new_multi_thread()
        .worker_threads(args.concurrency.max(1) as usize)
        .enable_all()
        .thread_name("bench-client")
        .build()?;

    let res = client_rt.block_on(run(&args, leader));

    // Hold the cluster alive until measurement is done, then tear down explicitly.
    drop(router);
    drop(server_rt);
    res
}

fn create_cluster(server_rt: &Runtime, members: BTreeSet<u64>) -> anyhow::Result<(Router, BenchRaft)> {
    server_rt.block_on(async move {
        let config = Arc::new(
            Config {
                election_timeout_min: 200,
                election_timeout_max: 2000,
                purge_batch_size: 1024,
                max_payload_entries: 1024,
                ..Default::default()
            }
            .validate()?,
        );

        let mut router = Router::new();
        router.new_cluster(config, members).await?;
        let leader = router.get_raft(0);
        Ok((router, leader))
    })
}

async fn run(args: &Args, leader: BenchRaft) -> anyhow::Result<()> {
    // Warmup (discarded).
    for _ in 0..args.warmup {
        write_one(&leader).await?;
    }

    let wall_start = Instant::now();
    let mut latencies: Vec<u64> = if args.concurrency <= 1 {
        let mut lat = Vec::with_capacity(args.operations as usize);
        for _ in 0..args.operations {
            let t0 = Instant::now();
            write_one(&leader).await?;
            lat.push(t0.elapsed().as_nanos() as u64);
        }
        lat
    } else {
        let per_client = args.operations / args.concurrency;
        let mut handles = Vec::new();
        for _ in 0..args.concurrency {
            let l = leader.clone();
            handles.push(tokio::spawn(async move {
                let mut lat = Vec::with_capacity(per_client as usize);
                for _ in 0..per_client {
                    let t0 = Instant::now();
                    write_one(&l).await?;
                    lat.push(t0.elapsed().as_nanos() as u64);
                }
                Ok::<_, anyhow::Error>(lat)
            }));
        }
        let mut all = Vec::new();
        for h in handles {
            all.extend(h.await??);
        }
        all
    };
    let wall = wall_start.elapsed();

    report(args, &mut latencies, wall);
    Ok(())
}

async fn write_one(leader: &BenchRaft) -> anyhow::Result<()> {
    leader.client_write(ClientRequest {}).await.map_err(|e| anyhow::anyhow!("client_write: {e:?}"))?;
    Ok(())
}

fn report(args: &Args, latencies: &mut [u64], wall: Duration) {
    latencies.sort_unstable();
    let n = latencies.len();
    assert!(n > 0, "no latency samples recorded");

    let pct = |p: f64| -> u64 {
        let idx = ((n as f64) * p) as usize;
        latencies[idx.min(n - 1)]
    };
    let mean = latencies.iter().sum::<u64>() / n as u64;

    println!(
        "{CORE_LABEL} members={} conc={} fsync_us={} rtt_us={} n={}: \
         min={} p50={} p90={} p99={} p99.9={} max={} mean={} (ns/op)",
        args.members,
        args.concurrency,
        args.fsync_us,
        args.rtt_us,
        n,
        latencies[0],
        pct(0.50),
        pct(0.90),
        pct(0.99),
        pct(0.999),
        latencies[n - 1],
        mean,
    );

    if args.concurrency > 1 {
        let secs = wall.as_secs_f64().max(1e-9);
        println!(
            "{CORE_LABEL} members={} conc={}: throughput {:.0} op/s over {:.3}s",
            args.members,
            args.concurrency,
            (n as f64) / secs,
            secs,
        );
    }
}
