//! Does the write-ahead log survive the disk?
//!
//! Every test here runs inside the simulator, so "crash" means a genuine power
//! cut: un-fsynced writes vanish or tear at sector boundaries, and the process
//! gets no chance to flush anything.
//!
//! The property that matters, stated once:
//!
//! > After any crash, the recovered log is a **prefix** of what was appended,
//! > and that prefix includes **everything a completed fsync acknowledged**.
//!
//! Short is recoverable — Raft's leader will refill a follower's log. A *hole*
//! is not: a log that silently skips an index is undetectably wrong, and every
//! safety property built on Log Matching evaporates.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use chrono_sim::fault::{DiskPolicy, FaultPolicy};
use chrono_sim::prelude::*;
use chronolog::types::{Config, Entry, EntryKind, HardState, Snapshot};
use chronolog::wal::{TailReason, Wal, WalOptions};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// Run an async body on `host` and pump the simulator until it resolves.
fn block_on<F, Fut, T>(sim: &Sim, host: &Host, f: F) -> T
where
    F: FnOnce(Host) -> Fut,
    Fut: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let slot: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let s = Arc::clone(&slot);
    let fut = f(host.clone());
    host.spawn("test-body", async move {
        *s.lock().unwrap() = Some(fut.await);
    });
    let deadline = sim.now() + Nanos::from_secs(600);
    sim.run_until(deadline);
    let out = slot.lock().unwrap().take();
    out.expect("the test body never finished")
}

fn sim_with(seed: u64, disk: DiskPolicy) -> (Sim, Host) {
    let policy = FaultPolicy {
        disk,
        ..FaultPolicy::benign()
    };
    let sim = Sim::new(seed, policy, TraceMode::HashOnly);
    sim.set_boot(|_| {});
    let host = sim.add_node(0, Role::Server);
    sim.boot_all();
    (sim, host)
}

fn quiet_disk() -> DiskPolicy {
    DiskPolicy {
        torn_write_ppm: 0,
        lost_write_ppm: 0,
        slow_ppm: 0,
        ..DiskPolicy::default()
    }
}

fn cmd(term: u64, index: u64) -> Entry {
    Entry {
        term,
        index,
        kind: EntryKind::Normal(format!("set k{index}=v{index}").into_bytes()),
    }
}

/// Assert the recovered log is a prefix of what was written, with no holes.
fn assert_is_prefix(recovered: &[Entry], written: &[Entry]) {
    assert!(
        recovered.len() <= written.len(),
        "recovered {} entries but only {} were ever written",
        recovered.len(),
        written.len()
    );
    for (i, (got, want)) in recovered.iter().zip(written.iter()).enumerate() {
        assert_eq!(
            got, want,
            "entry {i} differs: the log is not a prefix of what was written"
        );
    }
    // Contiguity, restated directly rather than inferred from the above.
    for w in recovered.windows(2) {
        assert_eq!(
            w[1].index,
            w[0].index + 1,
            "hole in the recovered log at {:?}",
            w[0].index
        );
    }
}

// ---------------------------------------------------------------------------
// The happy path
// ---------------------------------------------------------------------------

#[test]
fn a_synced_log_recovers_completely() {
    let (sim, host) = sim_with(1, quiet_disk());
    let written: Vec<Entry> = (1..=200).map(|i| cmd(1, i)).collect();

    let w = written.clone();
    block_on(&sim, &host, move |h| async move {
        let mut wal = Wal::open(h, WalOptions::default()).await.unwrap().wal;
        wal.append(&w).await.unwrap();
        wal.sync().await.unwrap();
    });

    sim.crash(0);
    sim.restart(0);
    let host = sim.host(0);
    let rec = block_on(&sim, &host, |h| async move {
        let r = Wal::open(h, WalOptions::default()).await.unwrap();
        (r.entries, r.tail, r.truncated)
    });

    assert_eq!(
        rec.1,
        TailReason::Clean,
        "a fully synced log must recover cleanly"
    );
    assert_eq!(rec.2, 0);
    assert_eq!(rec.0, written);
}

#[test]
fn hard_state_survives_a_restart() {
    let (sim, host) = sim_with(2, quiet_disk());
    block_on(&sim, &host, |h| async move {
        let mut wal = Wal::open(h, WalOptions::default()).await.unwrap().wal;
        wal.save_hard_state(HardState {
            term: 7,
            vote: Some(3),
            commit: 41,
        })
        .await
        .unwrap();
    });
    sim.crash(0);
    sim.restart(0);
    let host = sim.host(0);
    let hs = block_on(&sim, &host, |h| async move {
        Wal::open(h, WalOptions::default())
            .await
            .unwrap()
            .hard_state
    });
    assert_eq!(
        hs,
        HardState {
            term: 7,
            vote: Some(3),
            commit: 41
        }
    );
}

#[test]
fn segments_roll_over_and_recover_across_the_boundary() {
    // Small segments so 500 entries span many files.
    let opts = WalOptions {
        segment_bytes: 2048,
        compact_slack_bytes: 0,
    };
    let (sim, host) = sim_with(3, quiet_disk());
    let written: Vec<Entry> = (1..=500).map(|i| cmd(2, i)).collect();

    let (w, o) = (written.clone(), opts.clone());
    let segs = block_on(&sim, &host, move |h| async move {
        let mut wal = Wal::open(h, o).await.unwrap().wal;
        wal.append(&w).await.unwrap();
        wal.sync().await.unwrap();
        wal.segment_count()
    });
    assert!(
        segs > 5,
        "500 entries in 2 KiB segments should roll over repeatedly, got {segs}"
    );

    sim.crash(0);
    sim.restart(0);
    let host = sim.host(0);
    let o = opts.clone();
    let rec = block_on(&sim, &host, move |h| async move {
        let r = Wal::open(h, o).await.unwrap();
        (r.entries, r.tail)
    });
    assert_eq!(rec.1, TailReason::Clean);
    assert_eq!(
        rec.0, written,
        "entries must recover across segment boundaries"
    );
}

// ---------------------------------------------------------------------------
// Torn tails
// ---------------------------------------------------------------------------

#[test]
fn an_unsynced_tail_is_lost_but_the_synced_prefix_is_not() {
    // Every un-fsynced write vanishes on power loss.
    let disk = DiskPolicy {
        torn_write_ppm: 0,
        lost_write_ppm: 1_000_000,
        slow_ppm: 0,
        ..DiskPolicy::default()
    };
    let (sim, host) = sim_with(4, disk);
    let written: Vec<Entry> = (1..=100).map(|i| cmd(1, i)).collect();

    let w = written.clone();
    block_on(&sim, &host, move |h| async move {
        let mut wal = Wal::open(h, WalOptions::default()).await.unwrap().wal;
        // Sync the first 60, leave the last 40 in the page cache.
        wal.append(&w[..60]).await.unwrap();
        wal.sync().await.unwrap();
        wal.append(&w[60..]).await.unwrap();
    });

    sim.crash(0);
    sim.restart(0);
    let host = sim.host(0);
    let rec = block_on(&sim, &host, |h| async move {
        Wal::open(h, WalOptions::default()).await.unwrap().entries
    });

    assert_is_prefix(&rec, &written);
    assert_eq!(rec.len(), 60, "exactly the fsynced prefix must survive");
}

#[test]
fn a_torn_record_is_detected_by_its_checksum_and_truncated() {
    // Records must be *larger than a sector* for a tear to land mid-record.
    // With 50-byte entries and 512-byte sectors, a torn write drops whole
    // records and leaves a log that is legitimately clean, just shorter — safe,
    // but it never exercises the checksum. 8 KiB payloads across 512-byte
    // sectors put the tear inside a record, which is the case that must be
    // caught by CRC rather than by running out of file.
    let disk = DiskPolicy {
        torn_write_ppm: 1_000_000,
        lost_write_ppm: 0,
        sector_size: 512,
        slow_ppm: 0,
        ..DiskPolicy::default()
    };
    let big = |term: u64, index: u64| Entry {
        term,
        index,
        kind: EntryKind::Normal(vec![(index % 251) as u8; 8192]),
    };

    let mut saw_a_tear = false;
    let mut saw_a_checksum_failure = false;
    for seed in 0..40u64 {
        let (sim, host) = sim_with(seed, disk.clone());
        let written: Vec<Entry> = (1..=20).map(|i| big(1, i)).collect();

        let w = written.clone();
        block_on(&sim, &host, move |h| async move {
            let mut wal = Wal::open(
                h,
                WalOptions {
                    segment_bytes: 1 << 30,
                    compact_slack_bytes: 0,
                },
            )
            .await
            .unwrap()
            .wal;
            wal.append(&w[..5]).await.unwrap();
            wal.sync().await.unwrap();
            wal.append(&w[5..]).await.unwrap();
        });
        sim.crash(0);
        if sim.stats().torn_writes == 0 {
            continue;
        }
        saw_a_tear = true;

        sim.restart(0);
        let host = sim.host(0);
        let rec = block_on(&sim, &host, |h| async move {
            let r = Wal::open(
                h,
                WalOptions {
                    segment_bytes: 1 << 30,
                    compact_slack_bytes: 0,
                },
            )
            .await
            .unwrap();
            (r.entries, r.tail)
        });

        assert_is_prefix(&rec.0, &written);
        assert!(
            rec.0.len() >= 5,
            "the fsynced prefix must survive a torn tail"
        );
        if matches!(rec.1, TailReason::BadChecksum | TailReason::ShortBody) {
            saw_a_checksum_failure = true;
        }
    }
    assert!(
        saw_a_tear,
        "40 seeds at a 100% tear rate produced no torn write"
    );
    assert!(
        saw_a_checksum_failure,
        "no seed produced a half-written record; the checksum path is untested"
    );
}

#[test]
fn corruption_in_the_middle_truncates_rather_than_skipping_the_bad_record() {
    let (sim, host) = sim_with(6, quiet_disk());
    let written: Vec<Entry> = (1..=50).map(|i| cmd(1, i)).collect();

    let w = written.clone();
    block_on(&sim, &host, move |h| async move {
        let mut wal = Wal::open(h, WalOptions::default()).await.unwrap().wal;
        wal.append(&w).await.unwrap();
        wal.sync().await.unwrap();
    });

    // Flip a bit deep inside the segment, simulating bit rot on the platter.
    block_on(&sim, &host, |h| async move {
        let names = h.storage.list().await.unwrap();
        let seg = names
            .iter()
            .find(|n| n.starts_with("wal-"))
            .unwrap()
            .clone();
        let f = h.storage.open(&seg).await.unwrap();
        let byte = f.read_at(600, 1).await.unwrap();
        f.write_at(600, vec![byte[0] ^ 0x40]).await.unwrap();
        f.fsync().await.unwrap();
    });

    sim.crash(0);
    sim.restart(0);
    let host = sim.host(0);
    let rec = block_on(&sim, &host, |h| async move {
        let r = Wal::open(h, WalOptions::default()).await.unwrap();
        (r.entries, r.tail)
    });

    assert_eq!(
        rec.1,
        TailReason::BadChecksum,
        "the CRC must catch a flipped bit"
    );
    // The crucial part: recovery stops *at* the bad record. It does not hunt
    // forward for the next one that happens to checksum, because that would
    // leave a hole and every safety property downstream assumes contiguity.
    assert_is_prefix(&rec.0, &written);
    assert!(
        rec.0.len() < 50,
        "recovery must stop at the corruption, not read past it"
    );
}

// ---------------------------------------------------------------------------
// Truncation, compaction, snapshots
// ---------------------------------------------------------------------------

#[test]
fn truncate_from_drops_exactly_the_conflicting_suffix() {
    let opts = WalOptions {
        segment_bytes: 1024,
        compact_slack_bytes: 0,
    };
    let (sim, host) = sim_with(7, quiet_disk());
    let written: Vec<Entry> = (1..=200).map(|i| cmd(1, i)).collect();

    // Truncate at 120, then append conflicting entries in a later term — the
    // exact sequence a follower performs when a new leader overwrites its tail.
    let (w, o) = (written.clone(), opts.clone());
    let replacement: Vec<Entry> = (120..=140).map(|i| cmd(9, i)).collect();
    let r2 = replacement.clone();
    block_on(&sim, &host, move |h| async move {
        let mut wal = Wal::open(h, o).await.unwrap().wal;
        wal.append(&w).await.unwrap();
        wal.sync().await.unwrap();
        wal.truncate_from(120).await.unwrap();
        assert_eq!(wal.last_index(), 119);
        wal.append(&r2).await.unwrap();
        wal.sync().await.unwrap();
    });

    sim.crash(0);
    sim.restart(0);
    let host = sim.host(0);
    let o = opts.clone();
    let rec = block_on(&sim, &host, move |h| async move {
        Wal::open(h, o).await.unwrap().entries
    });

    assert_eq!(rec.len(), 140);
    assert_eq!(
        rec[118],
        cmd(1, 119),
        "entries before the cut keep their original term"
    );
    assert_eq!(
        rec[119],
        cmd(9, 120),
        "entries after the cut are the replacements"
    );
    assert_eq!(rec[139], cmd(9, 140));
    for w in rec.windows(2) {
        assert_eq!(w[1].index, w[0].index + 1);
    }
}

#[test]
fn compaction_deletes_superseded_segments_and_recovery_starts_from_the_snapshot() {
    let opts = WalOptions {
        segment_bytes: 1024,
        compact_slack_bytes: 0,
    };
    let (sim, host) = sim_with(8, quiet_disk());
    let written: Vec<Entry> = (1..=400).map(|i| cmd(1, i)).collect();

    let (w, o) = (written.clone(), opts.clone());
    let (before, after) = block_on(&sim, &host, move |h| async move {
        let mut wal = Wal::open(h, o).await.unwrap().wal;
        wal.append(&w).await.unwrap();
        wal.sync().await.unwrap();
        let before = wal.segment_count();
        wal.save_snapshot(&Snapshot {
            last_index: 300,
            last_term: 1,
            config: Config::simple([0, 1, 2]),
            data: b"state-machine-image".to_vec(),
        })
        .await
        .unwrap();
        wal.compact_through(300).await.unwrap();
        (before, wal.segment_count())
    });
    assert!(
        after < before,
        "compaction should free segments: {before} -> {after}"
    );

    sim.crash(0);
    sim.restart(0);
    let host = sim.host(0);
    let o = opts.clone();
    let rec = block_on(&sim, &host, move |h| async move {
        let r = Wal::open(h, o).await.unwrap();
        (r.entries, r.snapshot)
    });

    let snap = rec.1.expect("the snapshot must be recovered");
    assert_eq!(snap.last_index, 300);
    assert_eq!(snap.data, b"state-machine-image");
    // Everything at or below the snapshot point is superseded by it.
    assert!(
        rec.0.first().map(|e| e.index).unwrap_or(301) > 300,
        "entries covered by the snapshot must not be replayed"
    );
    assert_eq!(
        rec.0.last().unwrap().index,
        400,
        "entries past the snapshot must survive"
    );
}

#[test]
fn a_torn_snapshot_falls_back_to_the_previous_slot() {
    // Snapshots alternate between two files precisely so a torn write to the
    // new one cannot destroy the old one.
    let disk = DiskPolicy {
        torn_write_ppm: 1_000_000,
        lost_write_ppm: 0,
        sector_size: 512,
        slow_ppm: 0,
        ..DiskPolicy::default()
    };
    let mut checked = 0;
    for seed in 0..30u64 {
        let (sim, host) = sim_with(seed, disk.clone());
        let good = Snapshot {
            last_index: 100,
            last_term: 1,
            config: Config::simple([0, 1, 2]),
            data: vec![0xAA; 3000],
        };
        let g = good.clone();
        // First snapshot is fully durable; the second is written and then the
        // power is cut before it can be.
        block_on(&sim, &host, move |h| async move {
            let mut wal = Wal::open(h, WalOptions::default()).await.unwrap().wal;
            wal.save_snapshot(&g).await.unwrap();
        });
        let doomed = Snapshot {
            last_index: 200,
            last_term: 2,
            config: Config::simple([0, 1, 2]),
            data: vec![0xBB; 3000],
        };
        let d = doomed.clone();
        let host2 = sim.host(0);
        host2.spawn_with("half-write", |h| async move {
            let mut wal = Wal::open(h, WalOptions::default()).await.unwrap().wal;
            let _ = wal.save_snapshot(&d).await;
        });
        // Cut the power partway through the second write.
        sim.run_until(sim.now() + Nanos::from_micros(30));
        sim.crash(0);
        sim.restart(0);

        let host = sim.host(0);
        let got = block_on(&sim, &host, |h| async move {
            Wal::open(h, WalOptions::default()).await.unwrap().snapshot
        });
        let snap = got.expect("a valid snapshot must always be recoverable");
        // Either the new one landed whole or the old one is still there. What
        // must never happen is a torn image being accepted as valid.
        assert!(
            snap == good || snap == doomed,
            "recovered a snapshot that was never fully written: {snap:?}"
        );
        if snap == good {
            checked += 1;
        }
    }
    assert!(
        checked > 0,
        "30 seeds never exercised the fallback to the previous slot"
    );
}

// ---------------------------------------------------------------------------
// The property, under randomised crashes
// ---------------------------------------------------------------------------

#[test]
fn randomised_crashes_never_lose_acknowledged_data_and_never_leave_a_hole() {
    // The headline property, checked across 60 seeds with a genuinely hostile
    // disk: tears, lost writes, and latency spikes all live.
    let disk = DiskPolicy {
        torn_write_ppm: 400_000,
        lost_write_ppm: 300_000,
        sector_size: 4096,
        slow_ppm: 20_000,
        ..DiskPolicy::default()
    };

    let mut crashed_mid_write = 0;
    for seed in 0..60u64 {
        let (sim, host) = sim_with(seed, disk.clone());
        let written: Vec<Entry> = (1..=300).map(|i| cmd(1, i)).collect();
        // How many entries a *completed* fsync has acknowledged.
        let acked = Arc::new(AtomicU64::new(0));

        let (w, a) = (written.clone(), Arc::clone(&acked));
        host.spawn_with("writer", |h| async move {
            let Ok(r) = Wal::open(
                h,
                WalOptions {
                    segment_bytes: 4096,
                    compact_slack_bytes: 0,
                },
            )
            .await
            else {
                return;
            };
            let mut wal = r.wal;
            // Group commit: append ten, fsync once.
            for chunk in w.chunks(10) {
                if wal.append(chunk).await.is_err() {
                    return;
                }
                if wal.sync().await.is_err() {
                    return;
                }
                // Only now is this batch durable.
                a.store(chunk.last().unwrap().index, Ordering::SeqCst);
            }
        });

        // Cut the power at an arbitrary point in the middle of the workload.
        let when = Nanos((seed + 1) * 137_000 % 4_000_000 + 50_000);
        sim.run_until(when);
        let durable_at_crash = acked.load(Ordering::SeqCst);
        sim.crash(0);
        if durable_at_crash < 300 {
            crashed_mid_write += 1;
        }

        sim.restart(0);
        let host = sim.host(0);
        let rec = block_on(&sim, &host, |h| async move {
            Wal::open(
                h,
                WalOptions {
                    segment_bytes: 4096,
                    compact_slack_bytes: 0,
                },
            )
            .await
            .unwrap()
            .entries
        });

        assert_is_prefix(&rec, &written);
        assert!(
            rec.len() as u64 >= durable_at_crash,
            "seed {seed}: fsync acknowledged through index {durable_at_crash} but only {} \
             entries survived — acknowledged data was lost",
            rec.len()
        );
    }
    assert!(
        crashed_mid_write > 20,
        "only {crashed_mid_write}/60 seeds crashed mid-write; the test is not exercising much"
    );
}
