//! The whole thing, running inside the simulator.
//!
//! Raft, the WAL, the state machine, the client protocol, and the virtual
//! network/disk/clocks, all at once. Where `tests/raft.rs` pins the scenarios
//! we already know are hard, this file asks a different question: does the
//! assembled system work when the environment is actively hostile?

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use chrono_sim::fault::FaultPolicy;
use chrono_sim::prelude::*;
use chronolog::client::{CallResult, Client, Outcome, ReadMode};
use chronolog::node::{self, NodeOptions};
use chronolog::raft::RaftOptions;
use chronolog::types::Config;
use chronolog::wal::WalOptions;

const SERVERS: u32 = 3;
const CLIENT_NODE: NodeId = 100;

/// Key/value pairs the cluster acknowledged, which must survive whatever
/// happens next.
type Acked = Arc<Mutex<Vec<(Vec<u8>, Vec<u8>)>>>;

fn options() -> NodeOptions {
    NodeOptions {
        raft: RaftOptions {
            election_ticks: 8,
            heartbeat_ticks: 2,
            snapshot_interval: 400,
            ..RaftOptions::default()
        },
        wal: WalOptions {
            segment_bytes: 32 * 1024,
            compact_slack_bytes: 8 * 1024,
        },
        tick_interval: Nanos::from_millis(20),
        bootstrap: Config::simple(0..SERVERS),
        inspect: false,
    }
}

/// Build a cluster. `workload` runs on a dedicated client node that chaos never
/// touches.
fn cluster<F, Fut>(seed: u64, policy: FaultPolicy, workload: F) -> Sim
where
    F: FnOnce(Host, Client) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let sim = Sim::new(seed, policy, TraceMode::HashOnly);
    sim.set_boot(move |host: Host| {
        if host.node < SERVERS {
            node::start(host, options());
        }
    });
    for id in 0..SERVERS {
        sim.add_node(id, Role::Server);
    }
    let client_host = sim.add_node(CLIENT_NODE, Role::Client);
    sim.boot_all();

    let client = Client::new(client_host.clone(), 1, (0..SERVERS).collect())
        .with_timeout(Nanos::from_millis(400))
        .with_max_attempts(30);
    client_host.spawn_with("workload", move |h| workload(h, client));
    sim
}

// ---------------------------------------------------------------------------
// The basics, end to end
// ---------------------------------------------------------------------------

#[test]
fn a_cluster_elects_a_leader_and_serves_a_write_then_a_read() {
    let done = Arc::new(AtomicU64::new(0));
    let got: Arc<Mutex<Option<Outcome>>> = Arc::new(Mutex::new(None));
    let (d, g) = (Arc::clone(&done), Arc::clone(&got));

    let sim = cluster(1, FaultPolicy::benign(), move |_h, mut client| async move {
        let put = client.put(b"greeting", b"hello").await;
        assert!(
            matches!(put, CallResult::Ok(Outcome::Applied { .. })),
            "put failed: {put:?}"
        );
        let read = client.get(b"greeting", ReadMode::Linearizable).await;
        if let CallResult::Ok(outcome) = read {
            *g.lock().unwrap() = Some(outcome);
        }
        d.store(1, Ordering::SeqCst);
    });

    sim.run_until(Nanos::from_secs(30));
    assert_eq!(
        done.load(Ordering::SeqCst),
        1,
        "the workload never finished"
    );
    assert_eq!(
        got.lock().unwrap().clone(),
        Some(Outcome::Value(Some(b"hello".to_vec()))),
        "a linearizable read must observe the write that preceded it"
    );
}

#[test]
fn writes_survive_a_full_cluster_restart() {
    // Durability, end to end: acknowledged writes must come back after every
    // node loses power simultaneously.
    let acked: Acked = Arc::new(Mutex::new(Vec::new()));
    let a = Arc::clone(&acked);
    let sim = cluster(2, FaultPolicy::benign(), move |_h, mut client| async move {
        for i in 0..40u32 {
            let (k, v) = (format!("k{i}").into_bytes(), format!("v{i}").into_bytes());
            if let CallResult::Ok(Outcome::Applied { .. }) = client.put(&k, &v).await {
                a.lock().unwrap().push((k, v));
            }
        }
    });
    sim.run_until(Nanos::from_secs(60));

    let written = acked.lock().unwrap().clone();
    assert!(
        written.len() > 30,
        "expected most writes to be acknowledged, got {}",
        written.len()
    );

    // Power-cycle everything.
    for id in 0..SERVERS {
        sim.crash(id);
    }
    sim.run_until(sim.now() + Nanos::from_secs(2));
    for id in 0..SERVERS {
        sim.restart(id);
    }

    // Read it all back with a fresh client session.
    let found = Arc::new(Mutex::new(Vec::new()));
    let f = Arc::clone(&found);
    let host = sim.host(CLIENT_NODE);
    let mut client = Client::new(host.clone(), 2, (0..SERVERS).collect())
        .with_timeout(Nanos::from_millis(500))
        .with_max_attempts(40);
    host.spawn_with("verify", move |_h| async move {
        for (k, v) in written {
            if let CallResult::Ok(Outcome::Value(got)) =
                client.get(&k, ReadMode::Linearizable).await
            {
                f.lock().unwrap().push((k, v, got));
            }
        }
    });
    sim.run_until(sim.now() + Nanos::from_secs(120));

    let results = found.lock().unwrap();
    assert!(
        !results.is_empty(),
        "the cluster never recovered enough to answer a read"
    );
    for (k, want, got) in results.iter() {
        assert_eq!(
            got.as_deref(),
            Some(&want[..]),
            "key {:?} was acknowledged but did not survive the restart",
            String::from_utf8_lossy(k)
        );
    }
}

#[test]
fn a_retried_write_is_applied_exactly_once() {
    // The end-to-end version of the session-table test: under a lossy network
    // the client *will* retry, and the value must not be applied twice.
    let policy = FaultPolicy {
        link: chrono_sim::fault::LinkPolicy {
            loss_ppm: 250_000,
            duplicate_ppm: 150_000,
            ..Default::default()
        },
        ..FaultPolicy::benign()
    };
    let versions: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let v = Arc::clone(&versions);
    let sim = cluster(3, policy, move |_h, mut client| async move {
        for i in 0..30u32 {
            let key = format!("k{i}").into_bytes();
            if let CallResult::Ok(Outcome::Applied { version }) = client.put(&key, b"once").await {
                v.lock().unwrap().push(version);
            }
        }
    });
    sim.run_until(Nanos::from_secs(90));

    let seen = versions.lock().unwrap().clone();
    assert!(
        seen.len() > 15,
        "expected progress despite loss, got {}",
        seen.len()
    );
    let unique: std::collections::BTreeSet<u64> = seen.iter().copied().collect();
    assert_eq!(
        unique.len(),
        seen.len(),
        "two distinct writes reported the same version — a retry was applied twice"
    );
}

// ---------------------------------------------------------------------------
// Under duress
// ---------------------------------------------------------------------------

#[test]
fn the_cluster_makes_progress_under_the_nemesis_policy() {
    // Partitions, crashes, pauses, clock skew, torn writes — all live. The
    // cluster does not have to be fast, but it must not stop forever.
    let ok = Arc::new(AtomicU64::new(0));
    let unknown = Arc::new(AtomicU64::new(0));
    let (o, u) = (Arc::clone(&ok), Arc::clone(&unknown));

    let sim = cluster(
        0x8f3a_2b1c,
        FaultPolicy::nemesis(),
        move |_h, mut client| async move {
            for i in 0..200u32 {
                let key = format!("k{}", i % 20).into_bytes();
                match client.put(&key, format!("v{i}").as_bytes()).await {
                    CallResult::Ok(Outcome::Applied { .. }) => {
                        o.fetch_add(1, Ordering::SeqCst);
                    }
                    _ => {
                        u.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        },
    );
    sim.run_until(Nanos::from_secs(30 * 60));

    let (succeeded, failed) = (ok.load(Ordering::SeqCst), unknown.load(Ordering::SeqCst));
    assert!(
        succeeded > 100,
        "only {succeeded} of 200 writes succeeded under nemesis ({failed} unknown); \
         the cluster is not making adequate progress"
    );
}

#[test]
fn a_rolling_restart_never_loses_an_acknowledged_write() {
    // One node down at a time, forever. A quorum always exists, so every write
    // that is acknowledged must be readable afterwards.
    let acked: Acked = Arc::new(Mutex::new(Vec::new()));
    let a = Arc::clone(&acked);
    let sim = cluster(5, FaultPolicy::benign(), move |_h, mut client| async move {
        for i in 0..60u32 {
            let (k, v) = (format!("rk{i}").into_bytes(), format!("rv{i}").into_bytes());
            if let CallResult::Ok(Outcome::Applied { .. }) = client.put(&k, &v).await {
                a.lock().unwrap().push((k, v));
            }
        }
    });

    // Cycle nodes while the workload runs.
    for round in 0..9 {
        sim.run_until(sim.now() + Nanos::from_secs(3));
        let victim = round % SERVERS;
        sim.crash(victim);
        sim.run_until(sim.now() + Nanos::from_secs(2));
        sim.restart(victim);
    }
    sim.run_until(sim.now() + Nanos::from_secs(120));

    let written = acked.lock().unwrap().clone();
    assert!(
        written.len() > 40,
        "expected steady progress, got {}",
        written.len()
    );

    let found = Arc::new(Mutex::new(Vec::new()));
    let f = Arc::clone(&found);
    let host = sim.host(CLIENT_NODE);
    let mut verifier = Client::new(host.clone(), 9, (0..SERVERS).collect())
        .with_timeout(Nanos::from_millis(500))
        .with_max_attempts(40);
    host.spawn_with("verify", move |_h| async move {
        for (k, v) in written {
            if let CallResult::Ok(Outcome::Value(got)) =
                verifier.get(&k, ReadMode::Linearizable).await
            {
                f.lock().unwrap().push((k, v, got));
            }
        }
    });
    sim.run_until(sim.now() + Nanos::from_secs(180));

    let results = found.lock().unwrap();
    assert!(!results.is_empty());
    for (k, want, got) in results.iter() {
        assert_eq!(
            got.as_deref(),
            Some(&want[..]),
            "acknowledged key {:?} vanished across a rolling restart",
            String::from_utf8_lossy(k)
        );
    }
}

#[test]
fn a_minority_partition_does_not_stop_the_cluster() {
    let ok = Arc::new(AtomicU64::new(0));
    let o = Arc::clone(&ok);
    let sim = cluster(6, FaultPolicy::benign(), move |_h, mut client| async move {
        for i in 0..80u32 {
            if let CallResult::Ok(Outcome::Applied { .. }) =
                client.put(format!("p{i}").as_bytes(), b"v").await
            {
                o.fetch_add(1, Ordering::SeqCst);
            }
        }
    });
    sim.run_until(Nanos::from_secs(5));
    // Cut one of three off entirely — the remaining two are still a quorum.
    sim.partition(&[0], &[1, 2], false);
    sim.run_until(Nanos::from_secs(120));
    assert!(
        ok.load(Ordering::SeqCst) > 60,
        "a 2-of-3 quorum must keep serving, got {}",
        ok.load(Ordering::SeqCst)
    );
}

// ---------------------------------------------------------------------------
// Snapshots and compaction, end to end
// ---------------------------------------------------------------------------

#[test]
fn a_node_that_misses_a_compaction_window_is_caught_up_by_snapshot() {
    let sim = cluster(7, FaultPolicy::benign(), move |_h, mut client| async move {
        for i in 0..900u32 {
            let _ = client
                .put(
                    format!("s{}", i % 50).as_bytes(),
                    format!("v{i}").as_bytes(),
                )
                .await;
        }
    });
    // Take a node down long enough for the leader to compact past it.
    sim.run_until(Nanos::from_secs(3));
    sim.crash(2);
    sim.run_until(Nanos::from_secs(200));
    sim.restart(2);
    sim.run_until(sim.now() + Nanos::from_secs(200));

    // It must converge on the same applied state as the others.
    let read = Arc::new(Mutex::new(Vec::new()));
    let r = Arc::clone(&read);
    let host = sim.host(CLIENT_NODE);
    let mut client = Client::new(host.clone(), 3, vec![2])
        .with_timeout(Nanos::from_millis(500))
        .with_max_attempts(20);
    host.spawn_with("stale-read-n2", move |_h| async move {
        for i in 0..50u32 {
            if let CallResult::Ok(Outcome::Value(v)) = client
                .get(format!("s{i}").as_bytes(), ReadMode::Stale)
                .await
            {
                r.lock().unwrap().push((i, v));
            }
        }
    });
    sim.run_until(sim.now() + Nanos::from_secs(60));

    let results = read.lock().unwrap();
    let present = results.iter().filter(|(_, v)| v.is_some()).count();
    assert!(
        present > 40,
        "the restarted node should hold nearly all 50 keys after catching up, has {present}"
    );
}

// ---------------------------------------------------------------------------
// Determinism of the whole system
// ---------------------------------------------------------------------------

fn run_for_hash(seed: u64) -> (u64, Stats, Nanos) {
    let sim = cluster(seed, FaultPolicy::nemesis(), |_h, mut client| async move {
        for i in 0..150u32 {
            let key = format!("k{}", i % 10).into_bytes();
            if i % 3 == 0 {
                let _ = client.get(&key, ReadMode::Linearizable).await;
            } else {
                let _ = client.put(&key, format!("v{i}").as_bytes()).await;
            }
        }
    });
    sim.run_until(Nanos::from_secs(20 * 60));
    (sim.trace_hash(), sim.stats(), sim.now())
}

#[test]
fn the_full_system_is_bit_identical_across_runs_of_a_seed() {
    // The property the entire project rests on, applied to the real system
    // rather than a toy workload: Raft, the WAL, the client, and every fault.
    let a = run_for_hash(0x8f3a_2b1c);
    let b = run_for_hash(0x8f3a_2b1c);
    assert_eq!(
        a.0, b.0,
        "trace hashes diverged: chronolog is reading entropy the kernel did not give it"
    );
    assert_eq!(a.1, b.1, "statistics diverged");
    assert_eq!(a.2, b.2, "end times diverged");
    assert!(
        a.1.events > 100_000,
        "the workload should be substantial: {:?}",
        a.1
    );
    // And the run must have been genuinely eventful.
    assert!(a.1.crashes > 0, "nemesis should have crashed something");
    assert!(
        a.1.partitions > 0,
        "nemesis should have partitioned something"
    );
}

#[test]
fn different_seeds_explore_different_executions() {
    let a = run_for_hash(1);
    let b = run_for_hash(2);
    assert_ne!(a.0, b.0);
}
