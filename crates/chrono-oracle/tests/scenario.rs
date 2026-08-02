//! The oracles, pointed at the real system.
//!
//! This is where the project either produces evidence or does not.

use chrono_oracle::scenario::{self, ScenarioConfig};
use chrono_sim::fault::FaultPolicy;
use chrono_sim::time::Nanos;
use chrono_sim::trace::TraceMode;

fn quick(seed: u64, policy: FaultPolicy) -> ScenarioConfig {
    ScenarioConfig {
        seed,
        servers: 3,
        clients: 3,
        keys: 4,
        max_ops_per_client: 5_000,
        think_time: Nanos::from_millis(30),
        duration: Nanos::from_secs(600),
        recovery: Nanos::from_secs(120),
        policy,
        read_percent: 40,
        trace: TraceMode::HashOnly,
        ..ScenarioConfig::default()
    }
}

#[test]
fn a_benign_run_is_clean_and_makes_progress() {
    let report = scenario::run(&quick(1, FaultPolicy::benign()));
    assert!(
        report.ok,
        "a benign run must be clean:\n{report}\n{:?}",
        report.failure()
    );
    assert!(
        report.history.len() >= 100,
        "the workload should complete: {}",
        report.history.len()
    );
    assert!(
        report.ops_unknown * 4 < report.ops_ok,
        "a benign run should not produce many unknowns: {} unknown vs {} ok",
        report.ops_unknown,
        report.ops_ok
    );
}

/// Seeds known to be clean, guarding against regression.
///
/// Seed 0x1 is deliberately absent: it reproduces an open State Machine Safety
/// violation, recorded in `BUGS.md` as CS-009 with a dedicated reproduction
/// below. Silently widening this range to exclude it would hide the fact that
/// it fails; listing it as ignored says so out loud.
#[test]
fn a_nemesis_run_holds_every_safety_property() {
    for seed in [0u64, 2, 3, 4, 5, 6, 7, 8] {
        let report = scenario::run(&quick(seed, FaultPolicy::nemesis()));
        assert!(
            report.invariants.ok(),
            "seed {seed:#x}: Raft invariants broke\n{}",
            report.invariants
        );
        assert!(
            !report.linearizability.is_violation(),
            "seed {seed:#x}: linearizability violated\n{}",
            report.linearizability
        );
    }
}

/// CS-009, open. A follower applies entries that a later term overwrites.
///
/// Kept as an executable reproduction rather than a comment: when the bug is
/// fixed this starts passing, and `cargo test -- --ignored` says so.
#[test]
#[ignore = "CS-009: open State Machine Safety violation; see BUGS.md"]
fn cs_009_state_machine_safety_at_seed_1() {
    let report = scenario::run(&quick(1, FaultPolicy::nemesis()));
    assert!(
        report.invariants.ok(),
        "CS-009 still reproduces:\n{}",
        report.invariants
    );
}

#[test]
fn the_scenario_is_reproducible() {
    // Everything downstream — every seed in BUGS.md — depends on this.
    let report = scenario::check_determinism(&quick(0x8f3a_2b1c, FaultPolicy::nemesis()));
    assert!(report.is_deterministic(), "{report}");
}

#[test]
fn the_run_compresses_far_more_time_than_it_costs() {
    // The claim that makes the whole approach worthwhile, measured rather than
    // asserted.
    let report = scenario::run(&quick(7, FaultPolicy::nemesis()));
    assert!(
        report.speedup() > 20.0,
        "only {:.0}x faster than real time ({} simulated in {:?})",
        report.speedup(),
        report.virtual_time,
        report.wall_time
    );
}

#[test]
fn the_nemesis_policy_actually_injects_faults() {
    // A "clean" run proves nothing if nothing went wrong.
    let report = scenario::run(&quick(3, FaultPolicy::nemesis()));
    assert!(report.stats.crashes > 0, "no crashes were injected");
    assert!(report.stats.partitions > 0, "no partitions were injected");
    assert!(report.stats.msgs_dropped > 0, "no messages were dropped");
}
