//! # chrono-sim
//!
//! A deterministic simulation runtime. Give it a 64-bit seed and it gives you a
//! universe: virtual time, a virtual network that drops and reorders and
//! partitions, a virtual disk that tears writes on power loss, per-node clocks
//! that disagree, and a scheduler that decides thread interleaving with the
//! same PRNG as everything else.
//!
//! Run the same seed twice and you get the same universe, event for event,
//! down to the order two tasks were polled in. That is the entire value
//! proposition: a distributed-systems bug stops being a story about a flaky
//! test and becomes a 64-bit number you can put in a bug report.
//!
//! ## Using it
//!
//! ```no_run
//! use chrono_sim::prelude::*;
//!
//! let sim = Sim::new(0x8f3a_2b1c, FaultPolicy::nemesis(), TraceMode::HashOnly);
//! sim.set_boot(|host: Host| {
//!     host.spawn_with("hello", |h| async move {
//!         h.sleep(Nanos::from_secs(1)).await;
//!         h.note(|| "one simulated second later".into());
//!     });
//! });
//! for id in 0..3 {
//!     sim.add_node(id, Role::Server);
//! }
//! sim.boot_all();
//! let outcome = sim.run_until(Nanos::from_secs(4 * 3600));
//! println!("{outcome:?} trace={:016x}", sim.trace_hash());
//! ```
//!
//! ## The invariant this crate exists to enforce
//!
//! The system under test may only touch [`traits::Host`]. If it calls
//! `std::time::Instant::now()`, iterates a `HashMap`, or spawns an OS thread,
//! determinism breaks — silently, and usually only in CI. That is what
//! [`trace::Recorder`]'s rolling hash is for: run a seed twice, compare, fail
//! the build on any divergence.

#![forbid(unsafe_op_in_unsafe_fn)]
#![warn(missing_debug_implementations)]

pub mod fault;
pub mod kernel;
pub mod rng;
pub mod time;
pub mod trace;
pub mod traits;

#[cfg(feature = "real")]
pub mod real;

pub mod prelude {
    pub use crate::fault::{ChaosPolicy, ClockPolicy, DiskPolicy, FaultPolicy, LinkPolicy};
    pub use crate::kernel::{Outcome, Role, Sim, Stats};
    pub use crate::rng::{LatencyDist, Rng, RngExt};
    pub use crate::time::Nanos;
    pub use crate::trace::{Event, TraceMode};
    pub use crate::traits::{
        BoxFuture, Clock, Envelope, File, Host, Network, NodeId, Spawner, Storage, Tracer,
    };
}

/// Run one seed twice and report whether the two universes were identical.
///
/// This is the determinism guard, and it is the single most valuable thing in
/// the crate. Everything else is a simulator; this is what makes it a
/// *deterministic* simulator, and the difference is falsifiable rather than
/// asserted.
///
/// `build` is called from scratch for each run so that the two runs share no
/// state — if it captures something mutable, that is itself a determinism bug
/// and this will find it.
pub fn check_determinism<F>(seed: u64, horizon: time::Nanos, mut build: F) -> DivergenceReport
where
    F: FnMut(&kernel::Sim),
{
    let mut run = |seed: u64| {
        let sim = kernel::Sim::new(seed, fault::FaultPolicy::nemesis(), trace::TraceMode::HashOnly);
        build(&sim);
        sim.boot_all();
        let outcome = sim.run_until(horizon);
        (sim.trace_hash(), outcome, sim.stats(), sim.now())
    };
    let (h1, o1, s1, t1) = run(seed);
    let (h2, o2, s2, t2) = run(seed);
    DivergenceReport {
        seed,
        hash_a: h1,
        hash_b: h2,
        outcome_a: o1,
        outcome_b: o2,
        events_a: s1.events,
        events_b: s2.events,
        virtual_time_a: t1,
        virtual_time_b: t2,
    }
}

#[derive(Debug, Clone)]
pub struct DivergenceReport {
    pub seed: u64,
    pub hash_a: u64,
    pub hash_b: u64,
    pub outcome_a: kernel::Outcome,
    pub outcome_b: kernel::Outcome,
    pub events_a: u64,
    pub events_b: u64,
    pub virtual_time_a: time::Nanos,
    pub virtual_time_b: time::Nanos,
}

impl DivergenceReport {
    pub fn is_deterministic(&self) -> bool {
        self.hash_a == self.hash_b
            && self.outcome_a == self.outcome_b
            && self.events_a == self.events_b
            && self.virtual_time_a == self.virtual_time_b
    }
}

impl std::fmt::Display for DivergenceReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_deterministic() {
            write!(
                f,
                "seed {:#018x} deterministic: {} events, {} virtual, hash {:016x}",
                self.seed, self.events_a, self.virtual_time_a, self.hash_a
            )
        } else {
            write!(
                f,
                "seed {:#018x} DIVERGED\n  run A: hash {:016x} events {} time {} outcome {:?}\n  \
                 run B: hash {:016x} events {} time {} outcome {:?}\n  \
                 Something read entropy the kernel did not provide. Usual suspects: HashMap \
                 iteration order, Instant::now(), pointer-derived ordering, a thread.",
                self.seed,
                self.hash_a,
                self.events_a,
                self.virtual_time_a,
                self.outcome_a,
                self.hash_b,
                self.events_b,
                self.virtual_time_b,
                self.outcome_b,
            )
        }
    }
}
