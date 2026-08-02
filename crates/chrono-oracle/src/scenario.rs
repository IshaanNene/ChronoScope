//! One complete experiment: build a cluster, run a workload against it under
//! a fault policy, and hand every oracle the evidence.
//!
//! This is the unit the CLI and the swarm both operate on. `run(config)` is a
//! pure function of the config — most importantly of its seed — so a failing
//! run is reproduced by passing the same number back in.
//!
//! # Where the oracles run
//!
//! Outside the simulation, between slices of virtual time. This matters: an
//! oracle running *as* a simulated task would spawn events, consume PRNG draws,
//! and change the trace hash — so turning checking on would change the
//! execution being checked. Sampling from outside keeps the run under
//! observation identical to the run without it, which is what makes
//! `chronoscope run` and `chronoscope check` comparable.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono_sim::fault::FaultPolicy;
use chrono_sim::kernel::{Outcome, Role, Sim, Stats};
use chrono_sim::time::Nanos;
use chrono_sim::trace::TraceMode;
use chrono_sim::traits::{Host, NodeId};
use chronolog::client::{CallResult, Client, Op, Outcome as ClientOutcome, ReadMode};
use chronolog::msg::{AdminResult, Wire};
use chronolog::node::{self, NodeHandle, NodeOptions};
use chronolog::raft::RaftOptions;
use chronolog::types::{Config, ConfigChange};
use chronolog::wal::WalOptions;

use crate::history::{Event, History, Op as HOp, Ret};
use crate::invariants::{ClusterView, Invariants};
use crate::linearizability::{self, Limits, Verdict};
use crate::liveness::{Budget, Watchdog};

/// Node handles keyed by id, each with the boot generation the observer has
/// counted for it. See `PublicState::generation` for why the generation is
/// tracked out here rather than by the node.
type Handles = Arc<Mutex<BTreeMap<NodeId, (u64, NodeHandle)>>>;

#[derive(Clone, Debug)]
pub struct ScenarioConfig {
    pub seed: u64,
    pub servers: u32,
    pub clients: u32,
    /// Key space. Fewer keys means more contention per key, which is what makes
    /// a linearizability violation reachable — and what makes the checker's job
    /// harder. This is the main tuning knob for bug-finding.
    pub keys: u32,
    /// A cap on how many operations each client issues, bounding the history's
    /// memory. It is a **cap**, not a target — `duration` is what ends the run.
    ///
    /// Getting this backwards makes the whole scenario vacuous: with clients
    /// stopping after N operations, a run finishes in a few simulated seconds
    /// and a chaos policy quoted in events-per-second never fires. A "clean"
    /// verdict then means nothing went wrong because nothing happened.
    pub max_ops_per_client: u32,
    /// Pause between a client's operations. Spreads the workload across the
    /// run so faults land *during* operations rather than between them.
    pub think_time: Nanos,
    /// How long to run before healing everything and checking recovery.
    pub duration: Nanos,
    /// Extra nodes that boot but are **not** initially voters, available for
    /// the membership workload to add.
    pub spares: u32,
    /// How often to attempt a membership change. `None` disables the workload.
    ///
    /// This is the fault surface the swarm exists to explore. Hand-written
    /// tests cover the transitions someone thought to write down; only a random
    /// schedule produces a joint transition that lands concurrently with a
    /// leader crash, a partition, or a snapshot install.
    pub reconfigure_every: Option<Nanos>,
    /// Never shrink the voter set below this.
    pub min_voters: u32,
    /// How long to allow for recovery after the chaos stops.
    pub recovery: Nanos,
    pub policy: FaultPolicy,
    /// Percent of operations that are reads.
    pub read_percent: u32,
    pub read_mode: ReadMode,
    pub trace: TraceMode,
    pub lease_reads: bool,
    /// Cap on the linearizability search.
    pub limits: Limits,
}

impl Default for ScenarioConfig {
    fn default() -> Self {
        Self {
            seed: 0,
            servers: 3,
            clients: 4,
            keys: 6,
            max_ops_per_client: 20_000,
            think_time: Nanos::from_millis(25),
            spares: 0,
            reconfigure_every: None,
            min_voters: 3,
            duration: Nanos::from_secs(4 * 3600),
            recovery: Nanos::from_secs(120),
            policy: FaultPolicy::nemesis(),
            read_percent: 40,
            read_mode: ReadMode::Linearizable,
            trace: TraceMode::HashOnly,
            lease_reads: false,
            limits: Limits::default(),
        }
    }
}

/// Everything a run produced.
#[derive(Debug)]
pub struct RunReport {
    pub seed: u64,
    pub outcome: Outcome,
    pub trace_hash: u64,
    pub stats: Stats,
    pub virtual_time: Nanos,
    pub wall_time: Duration,
    pub history: History,
    pub linearizability: Verdict,
    pub invariants: Invariants,
    pub watchdog: Watchdog,
    pub ops_ok: u64,
    pub ops_unknown: u64,
    /// Membership changes the controller got accepted.
    pub reconfigurations: u64,
    /// Retained event trace, when the trace mode kept one. This is what
    /// `chronoscope replay` renders.
    pub trace: Vec<chrono_sim::trace::Entry>,
    /// True when nothing found anything wrong.
    pub ok: bool,
}

impl RunReport {
    /// How much simulated node-time this run covered. The headline number:
    /// "we compressed N node-hours into M seconds".
    pub fn node_hours(&self, servers: u32) -> f64 {
        (self.virtual_time.as_nanos() as f64 / 3.6e12) * servers as f64
    }

    /// Ratio of simulated time to wall time.
    pub fn speedup(&self) -> f64 {
        let wall = self.wall_time.as_secs_f64().max(1e-9);
        (self.virtual_time.as_nanos() as f64 / 1e9) / wall
    }

    /// Why the run failed, if it did.
    pub fn failure(&self) -> Option<String> {
        if self.outcome.is_failure() {
            return Some(format!("{:?}", self.outcome));
        }
        if let Verdict::NotLinearizable(v) = &self.linearizability {
            return Some(v.to_string());
        }
        if !self.invariants.ok() {
            return Some(self.invariants.to_string());
        }
        if !self.watchdog.ok() {
            return Some(self.watchdog.to_string());
        }
        None
    }
}

impl std::fmt::Display for RunReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "seed        {:#018x}", self.seed)?;
        writeln!(f, "outcome     {:?}", self.outcome)?;
        writeln!(f, "trace       {:016x}", self.trace_hash)?;
        writeln!(
            f,
            "time        {} simulated in {:.2?} wall  ({:.0}x)",
            self.virtual_time,
            self.wall_time,
            self.speedup()
        )?;
        writeln!(
            f,
            "faults      {} crashes, {} partitions, {} pauses, {} torn writes, {} lost writes",
            self.stats.crashes,
            self.stats.partitions,
            self.stats.pauses,
            self.stats.torn_writes,
            self.stats.lost_writes
        )?;
        writeln!(
            f,
            "network     {} sent, {} delivered, {} dropped, {} duplicated",
            self.stats.msgs_sent,
            self.stats.msgs_delivered,
            self.stats.msgs_dropped,
            self.stats.msgs_duplicated
        )?;
        writeln!(
            f,
            "client      {} ops ({} ok, {} unknown), max concurrency {}",
            self.history.len(),
            self.ops_ok,
            self.ops_unknown,
            self.history.max_concurrency()
        )?;
        if self.reconfigurations > 0 {
            writeln!(
                f,
                "membership       {} changes accepted",
                self.reconfigurations
            )?;
        }
        writeln!(f, "linearizability  {}", self.linearizability)?;
        writeln!(f, "invariants       {}", self.invariants)?;
        writeln!(f, "liveness         {}", self.watchdog)?;
        Ok(())
    }
}

const CLIENT_BASE: NodeId = 1000;

/// Run one scenario end to end.
pub fn run(config: &ScenarioConfig) -> RunReport {
    run_with_probe(config, |_, _| {})
}

/// Run a scenario, calling `probe` with the cluster view after every slice.
/// Used by diagnostics and by the trace TUI.
pub fn run_with_probe(
    config: &ScenarioConfig,
    mut probe: impl FnMut(Nanos, &ClusterView),
) -> RunReport {
    let started = std::time::Instant::now(); // ci-allow: harness timing its own run
    let sim = Sim::new(config.seed, config.policy.clone(), config.trace);
    sim.set_notes(!matches!(config.trace, TraceMode::HashOnly));

    let bootstrap = Config::simple(0..config.servers);
    let opts = NodeOptions {
        raft: RaftOptions {
            election_ticks: 8,
            heartbeat_ticks: 2,
            snapshot_interval: 500,
            lease_reads: config.lease_reads,
            ..RaftOptions::default()
        },
        wal: WalOptions {
            segment_bytes: 64 * 1024,
            compact_slack_bytes: 16 * 1024,
        },
        tick_interval: Nanos::from_millis(20),
        bootstrap,
        inspect: true,
    };

    // Handles are collected as nodes boot. A restart replaces the handle, which
    // is why this is a shared map rather than a vector built once.
    // Keyed with a generation counter, bumped on every boot, so oracles can
    // tell a restart from a regression.
    let handles: Handles = Arc::new(Mutex::new(BTreeMap::new()));
    let hs = Arc::clone(&handles);
    let total_servers = config.servers + config.spares;
    sim.set_boot(move |host: Host| {
        if host.node < total_servers {
            let h = node::start(host.clone(), opts.clone());
            let mut map = hs.lock().unwrap();
            let generation = map.get(&host.node).map(|(g, _)| g + 1).unwrap_or(0);
            map.insert(host.node, (generation, h));
        }
    });
    for id in 0..(config.servers + config.spares) {
        sim.add_node(id, Role::Server);
    }

    // --- the workload ----------------------------------------------------
    let history = Arc::new(Mutex::new(History::new()));
    let inflight = Arc::new(AtomicU64::new(0));
    let finished = Arc::new(AtomicU64::new(0));
    // Clients run until the run ends, not until a counter runs out.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    for c in 0..config.clients {
        let node_id = CLIENT_BASE + c;
        let host = sim.add_node(node_id, Role::Client);
        let hist = Arc::clone(&history);
        let inf = Arc::clone(&inflight);
        let fin = Arc::clone(&finished);
        let cfg = config.clone();
        let stop_flag = Arc::clone(&stop);
        let client_id = (c + 1) as u64;
        let servers_list: Vec<NodeId> = (0..config.servers).collect();

        host.spawn_with("workload", move |h| async move {
            let mut client = Client::new(h.clone(), client_id, servers_list)
                .with_timeout(Nanos::from_millis(600))
                .with_max_attempts(20);
            for i in 0..cfg.max_ops_per_client {
                if stop_flag.load(Ordering::SeqCst) {
                    break;
                }
                if cfg.think_time.as_nanos() > 0 {
                    // Jittered, so clients do not march in lockstep and every
                    // operation lands at a different point in the fault
                    // schedule.
                    let jitter = h.rng.next_u64() % cfg.think_time.as_nanos().max(1);
                    h.sleep(Nanos(cfg.think_time.as_nanos() / 2 + jitter)).await;
                }
                let key = format!("k{}", h.rng.next_u64() % cfg.keys.max(1) as u64).into_bytes();
                let is_read = (h.rng.next_u64() % 100) < cfg.read_percent as u64;
                let (op, hop) = if is_read {
                    (
                        Op::Get {
                            key: key.clone(),
                            mode: cfg.read_mode,
                        },
                        HOp::Read { key: key.clone() },
                    )
                } else {
                    let value = format!("c{client_id}-{i}").into_bytes();
                    (
                        Op::Put {
                            key: key.clone(),
                            value: value.clone(),
                        },
                        HOp::Write {
                            key: key.clone(),
                            value,
                        },
                    )
                };

                // Timestamps come from the client's own clock, which the
                // simulator gives zero skew and zero drift precisely so a
                // history is orderable by an external observer.
                let invoked = h.monotonic().as_nanos();
                inf.fetch_add(1, Ordering::SeqCst);
                let result = client.call(op).await;
                inf.fetch_sub(1, Ordering::SeqCst);
                let returned = h.monotonic().as_nanos();

                let ret = match result {
                    CallResult::Ok(ClientOutcome::Value(v)) => Ret::Value(v),
                    CallResult::Ok(ClientOutcome::Applied { .. }) => Ret::Ok,
                    CallResult::Ok(ClientOutcome::CasFailed { .. }) => Ret::CasFailed,
                    // A redirect or unavailable answer that survived every
                    // retry tells the client nothing about whether the write
                    // landed. That is genuinely unknown.
                    CallResult::Ok(_) | CallResult::Unknown => Ret::Unknown,
                };
                hist.lock().unwrap().push(Event {
                    client: client_id,
                    op: hop,
                    ret,
                    invoked,
                    returned,
                });
            }
            fin.fetch_add(1, Ordering::SeqCst);
        });
    }

    // --- the membership controller ---------------------------------------
    //
    // Runs on its own client node so chaos never touches it, and drives one
    // change at a time: propose a new voter set, wait, propose the next. The
    // leader finishes each joint transition itself via
    // `maybe_finish_config_change`; the controller only ever asks to *enter*
    // one.
    let reconfigs = Arc::new(AtomicU64::new(0));
    if let Some(period) = config.reconfigure_every {
        let host = sim.add_node(CLIENT_BASE + 500, Role::Client);
        let cfg = config.clone();
        let stop_flag = Arc::clone(&stop);
        let counter = Arc::clone(&reconfigs);
        host.spawn_with("membership-controller", move |h| async move {
            let all: Vec<NodeId> = (0..(cfg.servers + cfg.spares)).collect();
            let mut voters: BTreeSet<NodeId> = (0..cfg.servers).collect();
            // Replies land here so the controller can follow leader hints
            // rather than broadcasting blindly.
            let inbox: chronolog::chan::Chan<(NodeId, AdminResult)> = chronolog::chan::Chan::new();
            let rx = inbox.clone();
            h.spawn_with("controller-rx", |hh| async move {
                while let Some(env) = hh.net.recv().await {
                    if let Ok(Wire::AdminReply(r)) = Wire::decode(&env.payload) {
                        rx.send((env.from, r));
                    }
                }
                rx.close();
            });

            let mut leader_hint: Option<NodeId> = None;
            let mut learners: BTreeSet<NodeId> = BTreeSet::new();
            loop {
                h.sleep(period).await;
                if stop_flag.load(Ordering::SeqCst) {
                    return;
                }

                // Pick the next voter set: add an outsider or drop a voter.
                let outsiders: Vec<NodeId> = all
                    .iter()
                    .copied()
                    .filter(|id| !voters.contains(id) && !learners.contains(id))
                    .collect();
                // Promote a learner that has caught up, before starting
                // anything new.
                //
                // Adding a voter *directly* is the obvious move and it costs
                // availability: the moment it joins, the quorum rises — three
                // voters need two, four need three — while the new node has
                // nothing and cannot help. The cluster's failure tolerance
                // drops to zero until it catches up. Staging through a learner
                // replicates to it without counting it, so the quorum only
                // rises once it can actually contribute.
                if let Some(&pending) = learners.iter().next() {
                    let mut target = voters.clone();
                    target.insert(pending);
                    let change = ConfigChange::EnterJoint {
                        incoming: target.clone(),
                        learners: BTreeSet::new(),
                    };
                    if send_change(&h, &inbox, &mut leader_hint, &voters, change).await {
                        voters = target;
                        learners.remove(&pending);
                        counter.fetch_add(1, Ordering::SeqCst);
                    }
                    continue;
                }

                let can_grow = !outsiders.is_empty();
                let can_shrink = voters.len() > cfg.min_voters as usize;
                // Grow when shrinking is not allowed, otherwise flip a coin.
                let grow = can_grow && (!can_shrink || h.rng.next_u64() % 2 == 0);
                let mut target = voters.clone();
                if grow {
                    // Join as a learner: replicated to, not counted.
                    let i = (h.rng.next_u64() % outsiders.len() as u64) as usize;
                    let joining = outsiders[i];
                    let change = ConfigChange::EnterJoint {
                        incoming: voters.clone(),
                        learners: [joining].into_iter().collect(),
                    };
                    if send_change(&h, &inbox, &mut leader_hint, &voters, change).await {
                        learners.insert(joining);
                        counter.fetch_add(1, Ordering::SeqCst);
                    }
                    continue;
                } else if can_shrink {
                    let current: Vec<NodeId> = voters.iter().copied().collect();
                    let i = (h.rng.next_u64() % current.len() as u64) as usize;
                    target.remove(&current[i]);
                } else {
                    continue;
                }
                if target == voters || target.is_empty() {
                    continue;
                }
                let change = ConfigChange::EnterJoint {
                    incoming: target.clone(),
                    learners: BTreeSet::new(),
                };
                if send_change(&h, &inbox, &mut leader_hint, &voters, change).await {
                    voters = target;
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
    }

    sim.boot_all();

    // --- run, sampling the oracles between slices ------------------------
    let mut invariants = Invariants::new();
    let mut watchdog = Watchdog::new(Budget::default());
    let slice = Nanos::from_millis(250);
    let mut outcome = Outcome::HorizonReached;

    while sim.now() < config.duration {
        let target = (sim.now() + slice).min(config.duration);
        outcome = sim.run_until(target);
        let view = collect_live(&handles, &sim);
        probe(sim.now(), &view);
        invariants.check(&view);
        // "Healthy" has to mean a quorum of the *current* configuration is up.
        //
        // Comparing against the number of servers the scenario started with is
        // the obvious shortcut and breaks the moment membership changes: a
        // cluster grown to four voters needs three of them, and counting five
        // booted processes against an initial three says everything is fine
        // while the cluster genuinely has no quorum. The watchdog then demands
        // a leader that cannot legally be elected.
        let healthy = !sim.has_partitions() && quorum_alive(&view, &sim, config.servers);
        watchdog.observe(
            sim.now(),
            &view,
            healthy,
            inflight.load(Ordering::SeqCst) > 0,
            config.servers as usize,
        );

        if outcome.is_failure() || !invariants.ok() {
            break;
        }
        // Every client hit its cap. Not the normal ending — `duration` is.
        if finished.load(Ordering::SeqCst) == config.clients as u64 {
            break;
        }
    }
    stop.store(true, Ordering::SeqCst);

    // --- recovery phase ---------------------------------------------------
    //
    // Stop the chaos, heal everything, restart whatever is down, and give the
    // cluster a generous window. A system that is safe but never recovers is
    // still broken, and this is the only phase where demanding progress is
    // fair.
    if !outcome.is_failure() && invariants.ok() {
        sim.set_chaos(false);
        sim.heal_all();
        for id in 0..config.servers {
            if !sim.is_up(id) {
                sim.restart(id);
            }
        }
        let deadline = sim.now() + config.recovery;
        while sim.now() < deadline {
            let target = (sim.now() + slice).min(deadline);
            outcome = sim.run_until(target);
            let view = collect_live(&handles, &sim);
            invariants.check(&view);
            watchdog.observe(sim.now(), &view, true, false, config.servers as usize);
            if outcome.is_failure() || !invariants.ok() {
                break;
            }
        }
    }

    let history = history.lock().unwrap().clone();
    let ops_unknown = history.unknown_count() as u64;
    let ops_ok = history.len() as u64 - ops_unknown;
    let verdict = linearizability::check(&history, config.limits);

    let trace: Vec<chrono_sim::trace::Entry> = sim.with_trace(|r| r.entries().cloned().collect());

    let ok = !outcome.is_failure() && invariants.ok() && watchdog.ok() && !verdict.is_violation();

    RunReport {
        seed: config.seed,
        outcome,
        trace_hash: sim.trace_hash(),
        stats: sim.stats(),
        virtual_time: sim.now(),
        wall_time: started.elapsed(),
        history,
        linearizability: verdict,
        invariants,
        watchdog,
        ops_ok,
        ops_unknown,
        reconfigurations: reconfigs.load(Ordering::SeqCst),
        trace,
        ok,
    }
}

/// Submit one membership change and wait, briefly, for the answer.
///
/// Returns whether it was appended. A dropped request is not an error — the
/// next tick tries again, which is exactly how an operator retries.
async fn send_change(
    h: &Host,
    inbox: &chronolog::chan::Chan<(NodeId, AdminResult)>,
    leader_hint: &mut Option<NodeId>,
    voters: &BTreeSet<NodeId>,
    change: ConfigChange,
) -> bool {
    let target_node = leader_hint.unwrap_or_else(|| {
        let live: Vec<NodeId> = voters.iter().copied().collect();
        live[(h.rng.next_u64() % live.len().max(1) as u64) as usize]
    });
    h.net.send(target_node, Wire::Admin(change).encode());

    let deadline = inbox.clone();
    h.spawn_with("controller-timeout", move |hh| async move {
        hh.sleep(Nanos::from_secs(2)).await;
        deadline.send((NodeId::MAX, AdminResult::Rejected));
    });
    match inbox.recv().await {
        Some((_, AdminResult::Accepted { .. })) => true,
        Some((_, AdminResult::NotLeader { hint })) => {
            *leader_hint = hint;
            false
        }
        _ => {
            *leader_hint = None;
            false
        }
    }
}

/// Whether a majority of the current voter set is running.
fn quorum_alive(view: &ClusterView, sim: &Sim, fallback: u32) -> bool {
    let members = view.members();
    if members.is_empty() {
        return sim.alive_servers() * 2 > fallback as usize;
    }
    let alive = members.iter().filter(|id| sim.is_up(**id)).count();
    alive * 2 > members.len()
}

fn collect(handles: &Handles) -> ClusterView {
    let handles = handles.lock().unwrap();
    let mut nodes = BTreeMap::new();
    for (id, (generation, h)) in handles.iter() {
        let mut state = h.state.lock().unwrap().clone();
        state.generation = *generation;
        nodes.insert(*id, state);
    }
    ClusterView { nodes }
}

fn collect_live(handles: &Handles, sim: &Sim) -> ClusterView {
    let mut view = collect(handles);
    for (id, state) in view.nodes.iter_mut() {
        state.up = sim.is_up(*id);
    }
    view
}

/// Run a seed twice and confirm the two universes were identical.
///
/// The determinism guard, applied to the real system. A divergence means
/// `chronolog` read entropy the kernel did not hand it, and every reproduction
/// claim in `BUGS.md` is void until it is fixed.
pub fn check_determinism(config: &ScenarioConfig) -> DeterminismReport {
    let a = run(config);
    let b = run(config);
    DeterminismReport {
        seed: config.seed,
        hash_a: a.trace_hash,
        hash_b: b.trace_hash,
        events_a: a.stats.events,
        events_b: b.stats.events,
        time_a: a.virtual_time,
        time_b: b.virtual_time,
        ops_a: a.history.len(),
        ops_b: b.history.len(),
    }
}

#[derive(Clone, Debug)]
pub struct DeterminismReport {
    pub seed: u64,
    pub hash_a: u64,
    pub hash_b: u64,
    pub events_a: u64,
    pub events_b: u64,
    pub time_a: Nanos,
    pub time_b: Nanos,
    pub ops_a: usize,
    pub ops_b: usize,
}

impl DeterminismReport {
    pub fn is_deterministic(&self) -> bool {
        self.hash_a == self.hash_b
            && self.events_a == self.events_b
            && self.time_a == self.time_b
            && self.ops_a == self.ops_b
    }
}

impl std::fmt::Display for DeterminismReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_deterministic() {
            write!(
                f,
                "seed {:#018x}  deterministic  ({} events, {} ops, {} simulated, hash {:016x})",
                self.seed, self.events_a, self.ops_a, self.time_a, self.hash_a
            )
        } else {
            write!(
                f,
                "seed {:#018x}  DIVERGED\n  \
                 run A: hash {:016x}  events {}  ops {}  time {}\n  \
                 run B: hash {:016x}  events {}  ops {}  time {}\n\n  \
                 Something read entropy the kernel did not provide. Usual suspects:\n    \
                 - hash-map or hash-set iteration order (use the BTree equivalents)\n    \
                 - a real system clock read outside the `real` runtime\n    \
                 - ordering derived from a pointer or address\n    \
                 - a thread, or a f64 whose libm differs across targets",
                self.seed,
                self.hash_a,
                self.events_a,
                self.ops_a,
                self.time_a,
                self.hash_b,
                self.events_b,
                self.ops_b,
                self.time_b,
            )
        }
    }
}
