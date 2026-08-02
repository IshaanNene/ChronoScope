//! Does the simulator actually simulate?
//!
//! These tests are the foundation everything else stands on. If virtual time
//! does not jump, the project is slow. If runs are not reproducible, the
//! project is pointless. If the disk does not lose un-fsynced data, the
//! crash-consistency bugs stay hidden and `BUGS.md` stays empty.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use chrono_sim::fault::{ChaosPolicy, ClockPolicy, DiskPolicy, FaultPolicy, LinkPolicy};
use chrono_sim::prelude::*;
use chrono_sim::rng::LatencyDist;

fn quiet() -> FaultPolicy {
    FaultPolicy::benign()
}

// ---------------------------------------------------------------------------
// Virtual time
// ---------------------------------------------------------------------------

#[test]
fn a_simulated_day_costs_no_wall_clock() {
    let sim = Sim::new(1, quiet(), TraceMode::HashOnly);
    let woke = Arc::new(AtomicU64::new(0));
    let w = Arc::clone(&woke);
    sim.set_boot(move |host: Host| {
        let w = Arc::clone(&w);
        host.spawn_with("sleeper", |h| async move {
            // Twenty-four hours, one hour at a time.
            for _ in 0..24 {
                h.sleep(Nanos::from_secs(3600)).await;
                w.fetch_add(1, Ordering::SeqCst);
            }
        });
    });
    sim.add_node(0, Role::Server);
    sim.boot_all();

    let started = std::time::Instant::now();
    let outcome = sim.run_until(Nanos::from_secs(48 * 3600));
    let elapsed = started.elapsed();

    assert_eq!(woke.load(Ordering::SeqCst), 24);
    assert_eq!(sim.now(), Nanos::from_secs(24 * 3600));
    assert_eq!(outcome, Outcome::Quiesced, "nothing left to do once the sleeper finishes");
    assert!(
        elapsed.as_millis() < 500,
        "24 simulated hours took {elapsed:?} of real time; virtual time is not jumping"
    );
}

#[test]
fn time_only_advances_when_nothing_is_runnable() {
    // Two tasks yielding to each other must interleave at a single instant.
    let sim = Sim::new(2, quiet(), TraceMode::HashOnly);
    let observed = Arc::new(Mutex::new(Vec::<Nanos>::new()));
    let o = Arc::clone(&observed);
    sim.set_boot(move |host: Host| {
        for _ in 0..2 {
            let o = Arc::clone(&o);
            let h = host.clone();
            host.spawn("yielder", async move {
                for _ in 0..10 {
                    h.yield_now().await;
                    o.lock().unwrap().push(h.monotonic());
                }
            });
        }
    });
    sim.add_node(0, Role::Server);
    sim.boot_all();
    sim.run_until(Nanos::from_secs(1));

    let seen = observed.lock().unwrap();
    assert_eq!(seen.len(), 20);
    assert!(seen.iter().all(|&t| t == Nanos::ZERO), "yielding must not advance virtual time");
}

// ---------------------------------------------------------------------------
// Determinism — the whole point
// ---------------------------------------------------------------------------

/// A workload busy enough to exercise every subsystem: timers, messages, disk,
/// crashes, partitions, and concurrent tasks racing each other.
fn busy_cluster(sim: &Sim) {
    sim.set_boot(|host: Host| {
        let peers: Vec<NodeId> = (0..5u32).filter(|&p| p != host.node).collect();

        let h = host.clone();
        host.spawn("chatter", async move {
            let mut n = 0u64;
            loop {
                h.sleep(Nanos::from_millis(20 + (h.rng.next_u64() % 30))).await;
                for &p in &peers {
                    h.net.send(p, format!("ping {} {}", h.node, n).into_bytes());
                }
                n += 1;
            }
        });

        let h = host.clone();
        host.spawn("listener", async move {
            while let Some(env) = h.net.recv().await {
                if env.payload.len() > 4096 {
                    break;
                }
            }
        });

        let h = host.clone();
        host.spawn("writer", async move {
            let f = match h.storage.open("wal").await {
                Ok(f) => f,
                Err(_) => return,
            };
            let mut off = f.len();
            loop {
                h.sleep(Nanos::from_millis(15)).await;
                let payload = vec![(off % 251) as u8; 64];
                if f.write_at(off, payload).await.is_err() {
                    return;
                }
                off += 64;
                if off % 512 == 0 && f.fsync().await.is_err() {
                    return;
                }
            }
        });
    });
    for id in 0..5 {
        sim.add_node(id, Role::Server);
    }
}

fn run_busy(seed: u64) -> (u64, Stats, Nanos) {
    let sim = Sim::new(seed, FaultPolicy::nemesis(), TraceMode::HashOnly);
    busy_cluster(&sim);
    sim.boot_all();
    sim.run_until(Nanos::from_secs(120));
    (sim.trace_hash(), sim.stats(), sim.now())
}

#[test]
fn the_same_seed_produces_a_bit_identical_run() {
    let (h1, s1, t1) = run_busy(0x8f3a_2b1c);
    let (h2, s2, t2) = run_busy(0x8f3a_2b1c);
    assert_eq!(h1, h2, "trace hashes diverged for identical seeds");
    assert_eq!(s1, s2, "statistics diverged for identical seeds");
    assert_eq!(t1, t2);
    assert!(s1.events > 10_000, "workload must be substantial to be worth checking: {s1:?}");
}

#[test]
fn different_seeds_produce_different_runs() {
    let (h1, _, _) = run_busy(1);
    let (h2, _, _) = run_busy(2);
    assert_ne!(h1, h2);
}

#[test]
fn the_determinism_guard_passes_on_an_honest_workload() {
    let report = chrono_sim::check_determinism(0xdead_beef, Nanos::from_secs(60), |sim| {
        busy_cluster(sim);
    });
    assert!(report.is_deterministic(), "{report}");
}

#[test]
fn the_determinism_guard_catches_smuggled_entropy() {
    // The canonical failure: a system reads a clock the kernel does not own.
    // If the guard cannot catch this, it cannot catch anything.
    let sim_run = |seed: u64| {
        let sim = Sim::new(seed, quiet(), TraceMode::HashOnly);
        sim.set_boot(|host: Host| {
            let h = host.clone();
            host.spawn("cheater", async move {
                for _ in 0..20 {
                    h.sleep(Nanos::from_millis(1)).await;
                    // Ambient nondeterminism, smuggled in.
                    let stolen = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .subsec_nanos() as u64;
                    h.net.send(0, vec![0u8; (stolen % 32) as usize + 1]);
                }
            });
        });
        sim.add_node(0, Role::Server);
        sim.boot_all();
        sim.run_until(Nanos::from_secs(1));
        sim.trace_hash()
    };
    // Message *lengths* now depend on the host clock, so the traces differ.
    let hashes: Vec<u64> = (0..6).map(|_| sim_run(7)).collect();
    assert!(
        hashes.iter().any(|h| *h != hashes[0]),
        "the guard failed to notice a task reading SystemTime::now()"
    );
}

// ---------------------------------------------------------------------------
// Network
// ---------------------------------------------------------------------------

#[test]
fn messages_arrive_after_link_latency() {
    let sim = Sim::new(3, quiet(), TraceMode::HashOnly);
    let arrival = Arc::new(Mutex::new(None::<Nanos>));
    let a = Arc::clone(&arrival);
    sim.set_boot(move |host: Host| {
        if host.node == 0 {
            let h = host.clone();
            host.spawn("sender", async move {
                h.sleep(Nanos::from_millis(10)).await;
                h.net.send(1, b"hello".to_vec());
            });
        } else {
            let a = Arc::clone(&a);
            let h = host.clone();
            host.spawn("receiver", async move {
                if let Some(env) = h.net.recv().await {
                    assert_eq!(env.payload, b"hello");
                    assert_eq!(env.from, 0);
                    *a.lock().unwrap() = Some(h.monotonic());
                }
            });
        }
    });
    sim.add_node(0, Role::Server);
    sim.add_node(1, Role::Server);
    sim.boot_all();
    sim.run_until(Nanos::from_secs(1));

    let at = arrival.lock().unwrap().expect("message never arrived");
    // Sent at 10ms; `benign` latency is 100-400us.
    assert!(at > Nanos::from_millis(10), "arrived at {at}, before it was sent");
    assert!(at < Nanos::from_millis(11), "arrived at {at}, later than the link allows");
}

#[test]
fn a_partition_blocks_traffic_and_a_heal_restores_it() {
    let sim = Sim::new(4, quiet(), TraceMode::HashOnly);
    let got = Arc::new(AtomicU64::new(0));
    let g = Arc::clone(&got);
    sim.set_boot(move |host: Host| {
        if host.node == 0 {
            let h = host.clone();
            host.spawn("sender", async move {
                for _ in 0..100 {
                    h.sleep(Nanos::from_millis(10)).await;
                    h.net.send(1, b"x".to_vec());
                }
            });
        } else {
            let g = Arc::clone(&g);
            let h = host.clone();
            host.spawn("receiver", async move {
                while h.net.recv().await.is_some() {
                    g.fetch_add(1, Ordering::SeqCst);
                }
            });
        }
    });
    sim.add_node(0, Role::Server);
    sim.add_node(1, Role::Server);
    sim.boot_all();

    sim.run_until(Nanos::from_millis(300));
    let before = got.load(Ordering::SeqCst);
    assert!(before > 20, "expected steady traffic before the partition, got {before}");

    sim.partition(&[0], &[1], false);
    sim.run_until(Nanos::from_millis(700));
    let during = got.load(Ordering::SeqCst) - before;
    // A packet already in flight when the partition began still lands.
    assert!(during <= 1, "partition leaked {during} messages");

    sim.heal_all();
    sim.run_until(Nanos::from_millis(1100));
    let after = got.load(Ordering::SeqCst) - before - during;
    assert!(after > 20, "expected traffic to resume after the heal, got {after}");
}

#[test]
fn packets_are_dropped_duplicated_and_reordered() {
    let policy = FaultPolicy {
        link: LinkPolicy {
            latency: LatencyDist::uniform(1_000_000, 50_000_000),
            loss_ppm: 100_000,
            duplicate_ppm: 100_000,
            corrupt_ppm: 0,
        },
        ..FaultPolicy::benign()
    };
    let sim = Sim::new(5, policy, TraceMode::HashOnly);
    let seen = Arc::new(Mutex::new(Vec::<u64>::new()));
    let s = Arc::clone(&seen);
    sim.set_boot(move |host: Host| {
        if host.node == 0 {
            let h = host.clone();
            host.spawn("sender", async move {
                for i in 0u64..500 {
                    h.sleep(Nanos::from_millis(1)).await;
                    h.net.send(1, i.to_le_bytes().to_vec());
                }
            });
        } else {
            let s = Arc::clone(&s);
            let h = host.clone();
            host.spawn("receiver", async move {
                while let Some(env) = h.net.recv().await {
                    let mut b = [0u8; 8];
                    b.copy_from_slice(&env.payload);
                    s.lock().unwrap().push(u64::from_le_bytes(b));
                }
            });
        }
    });
    sim.add_node(0, Role::Server);
    sim.add_node(1, Role::Server);
    sim.boot_all();
    sim.run_until(Nanos::from_secs(5));

    let got = seen.lock().unwrap().clone();
    let stats = sim.stats();
    assert!(stats.msgs_dropped > 20, "expected drops, got {}", stats.msgs_dropped);
    assert!(stats.msgs_duplicated > 20, "expected duplicates, got {}", stats.msgs_duplicated);

    let mut sorted = got.clone();
    sorted.sort_unstable();
    assert_ne!(got, sorted, "expected reordering; delivery order matched send order exactly");

    let unique: std::collections::BTreeSet<u64> = got.iter().copied().collect();
    assert!(got.len() > unique.len(), "expected at least one duplicate to be delivered");
}

// ---------------------------------------------------------------------------
// Disk and crash consistency
// ---------------------------------------------------------------------------

/// Writes `n` records, fsyncing after each of the first `synced`, then hard
/// crashes. Returns what survived on the platter.
fn crash_after_writes(seed: u64, records: usize, synced: usize, disk: DiskPolicy) -> Vec<u8> {
    let policy = FaultPolicy { disk, ..FaultPolicy::benign() };
    let sim = Sim::new(seed, policy, TraceMode::HashOnly);
    let done = Arc::new(AtomicU64::new(0));
    let d = Arc::clone(&done);
    sim.set_boot(move |host: Host| {
        let d = Arc::clone(&d);
        // Only write on the first boot; after the restart we just read back.
        let first = d.load(Ordering::SeqCst) == 0;
        host.spawn_with("writer", |h| async move {
            let Ok(f) = h.storage.open("data").await else { return };
            if !first {
                return;
            }
            for i in 0..records {
                let rec = vec![(i + 1) as u8; 100];
                if f.write_at((i * 100) as u64, rec).await.is_err() {
                    return;
                }
                if i < synced && f.fsync().await.is_err() {
                    return;
                }
            }
            d.store(1, Ordering::SeqCst);
        });
    });
    sim.add_node(0, Role::Server);
    sim.boot_all();
    sim.run_until(Nanos::from_secs(10));
    assert_eq!(done.load(Ordering::SeqCst), 1, "the writer never finished");

    sim.crash(0);
    sim.restart(0);
    sim.run_until(Nanos::from_secs(20));

    // Read the post-crash view through the simulator's own file API.
    let contents = Arc::new(Mutex::new(Vec::new()));
    let c = Arc::clone(&contents);
    let host = sim.host(0);
    host.spawn_with("reader", |h| async move {
        let Ok(f) = h.storage.open("data").await else { return };
        let len = f.len() as usize;
        if let Ok(b) = f.read_at(0, len).await {
            *c.lock().unwrap() = b;
        }
    });
    sim.run_until(Nanos::from_secs(30));
    let out = contents.lock().unwrap().clone();
    out
}

#[test]
fn fsynced_data_survives_a_crash() {
    let disk = DiskPolicy { torn_write_ppm: 0, lost_write_ppm: 1_000_000, ..DiskPolicy::default() };
    // Every un-fsynced write is lost, every fsynced one must survive.
    let survived = crash_after_writes(11, 10, 6, disk);
    assert!(survived.len() >= 600, "fsynced prefix vanished: {} bytes left", survived.len());
    for i in 0..6 {
        assert_eq!(
            survived[i * 100],
            (i + 1) as u8,
            "record {i} was fsynced and must be on the platter"
        );
    }
}

#[test]
fn un_fsynced_data_can_vanish_on_power_loss() {
    let disk = DiskPolicy { torn_write_ppm: 0, lost_write_ppm: 1_000_000, ..DiskPolicy::default() };
    let survived = crash_after_writes(12, 10, 6, disk);
    assert_eq!(
        survived.len(),
        600,
        "writes 7-10 were never fsynced and must not be on the platter"
    );
}

#[test]
fn writes_tear_at_sector_granularity() {
    // Every un-fsynced write tears; none are lost outright.
    let disk = DiskPolicy {
        torn_write_ppm: 1_000_000,
        lost_write_ppm: 0,
        sector_size: 512,
        ..DiskPolicy::default()
    };
    let mut saw_torn = false;
    for seed in 0..25u64 {
        let sim = Sim::new(seed, FaultPolicy { disk: disk.clone(), ..FaultPolicy::benign() },
            TraceMode::HashOnly);
        let done = Arc::new(AtomicU64::new(0));
        let d = Arc::clone(&done);
        sim.set_boot(move |host: Host| {
            let d = Arc::clone(&d);
            if d.load(Ordering::SeqCst) > 0 {
                return;
            }
            host.spawn_with("writer", |h| async move {
                let Ok(f) = h.storage.open("data").await else { return };
                // One 4 KiB write spanning eight 512-byte sectors, never fsynced.
                let _ = f.write_at(0, vec![0xAB; 4096]).await;
                d.store(1, Ordering::SeqCst);
            });
        });
        sim.add_node(0, Role::Server);
        sim.boot_all();
        sim.run_until(Nanos::from_secs(1));
        sim.crash(0);

        let stats = sim.stats();
        if stats.torn_writes > 0 {
            saw_torn = true;
            let mut torn_len = None;
            sim.with_trace(|_| {});
            // Read back what landed.
            sim.restart(0);
            let got = Arc::new(Mutex::new(Vec::new()));
            let g = Arc::clone(&got);
            let host = sim.host(0);
            host.spawn_with("reader", |h| async move {
                let Ok(f) = h.storage.open("data").await else { return };
                let n = f.len() as usize;
                if let Ok(b) = f.read_at(0, n).await {
                    *g.lock().unwrap() = b;
                }
            });
            sim.run_until(Nanos::from_secs(5));
            let bytes = got.lock().unwrap().clone();
            torn_len = Some(bytes.len());
            let n = torn_len.unwrap();
            assert!(n < 4096, "a torn write must not land whole, got {n}");
            assert_eq!(n % 512, 0, "a torn write must land on a sector boundary, got {n}");
            assert!(bytes.iter().all(|&b| b == 0xAB), "surviving bytes must be the written bytes");
        }
    }
    assert!(saw_torn, "25 seeds at 100% tear rate produced no torn write");
}

#[test]
fn enospc_is_returned_not_panicked() {
    let disk = DiskPolicy { enospc_after_bytes: Some(1024), ..DiskPolicy::default() };
    let sim =
        Sim::new(13, FaultPolicy { disk, ..FaultPolicy::benign() }, TraceMode::HashOnly);
    let written = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let (w, fl) = (Arc::clone(&written), Arc::clone(&failed));
    sim.set_boot(move |host: Host| {
        let (w, fl) = (Arc::clone(&w), Arc::clone(&fl));
        host.spawn_with("writer", |h| async move {
            let Ok(f) = h.storage.open("data").await else { return };
            for i in 0..100u64 {
                match f.write_at(i * 100, vec![1u8; 100]).await {
                    Ok(()) => {
                        w.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(e) => {
                        assert!(chrono_sim::traits::is_enospc(&e), "unexpected error: {e}");
                        fl.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        });
    });
    sim.add_node(0, Role::Server);
    sim.boot_all();
    sim.run_until(Nanos::from_secs(10));
    assert_eq!(written.load(Ordering::SeqCst), 10, "1024-byte quota fits ten 100-byte writes");
    assert_eq!(failed.load(Ordering::SeqCst), 90);
}

// ---------------------------------------------------------------------------
// Process lifecycle
// ---------------------------------------------------------------------------

#[test]
fn a_crash_reaps_tasks_and_a_restart_reruns_boot() {
    let sim = Sim::new(14, quiet(), TraceMode::HashOnly);
    let ticks = Arc::new(AtomicU64::new(0));
    let boots = Arc::new(AtomicU64::new(0));
    let (t, b) = (Arc::clone(&ticks), Arc::clone(&boots));
    sim.set_boot(move |host: Host| {
        b.fetch_add(1, Ordering::SeqCst);
        let t = Arc::clone(&t);
        host.spawn_with("ticker", |h| async move {
            loop {
                h.sleep(Nanos::from_millis(10)).await;
                t.fetch_add(1, Ordering::SeqCst);
            }
        });
    });
    sim.add_node(0, Role::Server);
    sim.boot_all();

    sim.run_until(Nanos::from_millis(105));
    let before = ticks.load(Ordering::SeqCst);
    assert_eq!(boots.load(Ordering::SeqCst), 1);
    assert!(before >= 10, "expected ~10 ticks, got {before}");

    sim.crash(0);
    sim.run_until(Nanos::from_millis(500));
    assert_eq!(ticks.load(Ordering::SeqCst), before, "a crashed node's tasks must not run");

    sim.restart(0);
    sim.run_until(Nanos::from_millis(700));
    assert_eq!(boots.load(Ordering::SeqCst), 2, "restart must re-enter the boot function");
    assert!(ticks.load(Ordering::SeqCst) > before, "the restarted node must make progress");
}

#[test]
fn a_paused_node_freezes_but_its_clock_does_not() {
    let policy = FaultPolicy {
        chaos: ChaosPolicy {
            pause_ppm_per_sec: 1_000_000,
            pause_duration: LatencyDist::fixed(Nanos::from_millis(200).0),
            partition_ppm_per_sec: 0,
            crash_ppm_per_sec: 0,
            ..ChaosPolicy::default()
        },
        ..FaultPolicy::benign()
    };
    let sim = Sim::new(15, policy, TraceMode::HashOnly);
    let observed = Arc::new(Mutex::new(Vec::<(NodeId, Nanos)>::new()));
    let o = Arc::clone(&observed);
    sim.set_boot(move |host: Host| {
        let o = Arc::clone(&o);
        host.spawn_with("ticker", |h| async move {
            loop {
                h.sleep(Nanos::from_millis(10)).await;
                o.lock().unwrap().push((h.node, h.monotonic()));
            }
        });
    });
    for id in 0..3 {
        sim.add_node(id, Role::Server);
    }
    sim.boot_all();
    sim.run_until(Nanos::from_secs(5));

    assert!(sim.stats().pauses > 0, "the pause policy never fired");
    let seen = observed.lock().unwrap();
    // A frozen node cannot observe time passing, so it must show a gap larger
    // than its 10ms tick interval — that gap is the pause.
    let mut worst = Nanos::ZERO;
    for node in 0..3u32 {
        let times: Vec<Nanos> = seen.iter().filter(|(n, _)| *n == node).map(|(_, t)| *t).collect();
        for w in times.windows(2) {
            worst = worst.max(w[1] - w[0]);
        }
    }
    assert!(worst > Nanos::from_millis(100), "no pause-shaped gap in the tick stream: {worst}");
}

// ---------------------------------------------------------------------------
// Clocks
// ---------------------------------------------------------------------------

#[test]
fn nodes_disagree_about_what_time_it_is() {
    let policy = FaultPolicy {
        clock: ClockPolicy {
            max_skew: Nanos::from_millis(50),
            max_drift_ppm: 500,
            step_ppm_per_sec: 0,
            max_step: Nanos::ZERO,
        },
        ..FaultPolicy::benign()
    };
    let sim = Sim::new(16, policy, TraceMode::HashOnly);
    // Virtual time only moves when something is waiting for it, so the nodes
    // need a heartbeat before there is any elapsed time for drift to act on.
    sim.set_boot(|host: Host| {
        host.spawn_with("ticker", |h| async move {
            loop {
                h.sleep(Nanos::from_secs(1)).await;
            }
        });
    });
    let hosts: Vec<Host> = (0..5).map(|id| sim.add_node(id, Role::Server)).collect();
    sim.boot_all();
    sim.run_until(Nanos::from_secs(60));
    assert_eq!(sim.now(), Nanos::from_secs(60));

    let walls: Vec<Nanos> = hosts.iter().map(|h| h.now()).collect();
    let monos: Vec<Nanos> = hosts.iter().map(|h| h.monotonic()).collect();
    let spread = walls.iter().max().unwrap().0 - walls.iter().min().unwrap().0;
    assert!(spread > 1_000_000, "wall clocks agreed to within {spread}ns; skew is not applied");

    // Monotonic clocks drift but never carry the skew offset.
    let mono_spread = monos.iter().max().unwrap().0 - monos.iter().min().unwrap().0;
    assert!(mono_spread > 0, "drift should separate monotonic clocks over 60s");
}

#[test]
fn a_node_whose_clock_runs_fast_wakes_early_in_true_time() {
    let policy = FaultPolicy {
        clock: ClockPolicy {
            max_skew: Nanos::ZERO,
            max_drift_ppm: 100_000, // 10% fast/slow: exaggerated so the effect is unmistakable
            step_ppm_per_sec: 0,
            max_step: Nanos::ZERO,
        },
        ..FaultPolicy::benign()
    };
    let sim = Sim::new(17, policy, TraceMode::HashOnly);
    let wakes = Arc::new(Mutex::new(Vec::<(NodeId, Nanos)>::new()));
    let w = Arc::clone(&wakes);
    sim.set_boot(move |host: Host| {
        let w = Arc::clone(&w);
        host.spawn_with("sleeper", |h| async move {
            h.sleep(Nanos::from_secs(10)).await;
            // `monotonic` is this node's own (drifted) view.
            w.lock().unwrap().push((h.node, h.monotonic()));
        });
    });
    for id in 0..6 {
        sim.add_node(id, Role::Server);
    }
    sim.boot_all();
    sim.run_until(Nanos::from_secs(30));

    let seen = wakes.lock().unwrap();
    assert_eq!(seen.len(), 6);
    // Every node believes it slept ~10s on its own clock, and they are right —
    // that is the illusion. The disagreement lives in true time.
    for &(node, t) in seen.iter() {
        let believed = t.0 as i64 - Nanos::from_secs(10).0 as i64;
        assert!(
            believed.abs() < Nanos::from_millis(50).0 as i64,
            "node {node} thinks it slept {t}, not 10s"
        );
    }
}

// ---------------------------------------------------------------------------
// The scheduler itself
// ---------------------------------------------------------------------------

#[test]
fn task_interleaving_varies_with_the_seed() {
    let order_for = |seed: u64| {
        let sim = Sim::new(seed, quiet(), TraceMode::HashOnly);
        let log = Arc::new(Mutex::new(Vec::<u32>::new()));
        let l = Arc::clone(&log);
        sim.set_boot(move |host: Host| {
            for i in 0..4u32 {
                let l = Arc::clone(&l);
                let h = host.clone();
                host.spawn("racer", async move {
                    for _ in 0..8 {
                        h.yield_now().await;
                        l.lock().unwrap().push(i);
                    }
                });
            }
        });
        sim.add_node(0, Role::Server);
        sim.boot_all();
        sim.run_until(Nanos::from_secs(1));
        let out = log.lock().unwrap().clone();
        out
    };
    let a = order_for(100);
    let b = order_for(200);
    assert_eq!(a.len(), 32);
    assert_eq!(a, order_for(100), "the same seed must produce the same interleaving");
    assert_ne!(a, b, "different seeds must explore different interleavings");
}

#[test]
fn a_panicking_task_is_reported_not_propagated() {
    let sim = Sim::new(18, quiet(), TraceMode::HashOnly);
    sim.set_boot(|host: Host| {
        host.spawn_with("asserter", |h| async move {
            h.sleep(Nanos::from_millis(5)).await;
            panic!("Leader Completeness violated");
        });
    });
    sim.add_node(0, Role::Server);
    sim.boot_all();
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let outcome = sim.run_until(Nanos::from_secs(1));
    std::panic::set_hook(prev);

    match outcome {
        Outcome::Panicked { node, message } => {
            assert_eq!(node, 0);
            assert!(message.contains("Leader Completeness"), "lost the message: {message}");
        }
        other => panic!("expected a reported panic, got {other:?}"),
    }
}

#[test]
fn a_busy_loop_is_reported_as_livelock_not_hung() {
    let sim = Sim::new(19, quiet(), TraceMode::HashOnly);
    sim.set_boot(|host: Host| {
        let h = host.clone();
        host.spawn("spinner", async move {
            loop {
                h.yield_now().await;
            }
        });
    });
    sim.add_node(0, Role::Server);
    sim.boot_all();
    let outcome = sim.run_until(Nanos::from_secs(1));
    assert!(
        matches!(outcome, Outcome::Livelock { .. }),
        "expected livelock detection, got {outcome:?}"
    );
}
