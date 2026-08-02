//! `chronoscope` — drive the simulator.
//!
//! ```text
//! chronoscope run    --seed 0x8f3a2b1c     one execution, every oracle
//! chronoscope replay --seed 0x8f3a2b1c     the identical execution, with a timeline
//! chronoscope check  --seeds 64            prove determinism: each seed twice, hashes diffed
//! chronoscope swarm  --seeds 10000 -j 32   thousands of executions; failures filed as artifacts
//! chronoscope bench                        throughput and latency
//! ```
//!
//! Every subcommand is a pure function of its arguments. That is the point:
//! `run` printing a violation and `replay` showing you why are the same
//! execution, not two similar ones.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use chrono_oracle::linearizability::Limits;
use chrono_oracle::scenario::{self, RunReport, ScenarioConfig};
use chrono_sim::fault::FaultPolicy;
use chrono_sim::time::Nanos;
use chrono_sim::trace::TraceMode;
use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "chronoscope",
    about = "A deterministic simulation testbed for Chronolog",
    long_about = "Every execution is a pure function of a 64-bit seed. A violation found at \
                  a seed is reproduced by passing that seed back in — as many times as you like."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run one seed and report what every oracle found.
    Run {
        #[arg(long, value_parser = parse_seed, default_value = "0")]
        seed: u64,
        #[command(flatten)]
        world: World,
        /// Print the client history that the checker examined.
        #[arg(long)]
        history: bool,
    },
    /// Replay a seed with a full event trace and a timeline.
    Replay {
        #[arg(long, value_parser = parse_seed)]
        seed: u64,
        #[command(flatten)]
        world: World,
        /// Show every event rather than a summarized timeline.
        #[arg(long)]
        full: bool,
        /// Only show events involving this node.
        #[arg(long)]
        node: Option<u32>,
        /// Cap on printed events.
        #[arg(long, default_value_t = 400)]
        limit: usize,
    },
    /// The determinism guard: run each seed twice and diff the trace hash.
    Check {
        /// How many seeds to verify.
        #[arg(long, default_value_t = 32)]
        seeds: u64,
        #[arg(long, value_parser = parse_seed, default_value = "0")]
        from: u64,
        #[command(flatten)]
        world: World,
        #[arg(long, short = 'j', default_value_t = 0)]
        parallel: usize,
    },
    /// Run many seeds in parallel; file any failure as a reproducible artifact.
    Swarm {
        #[arg(long, default_value_t = 1000)]
        seeds: u64,
        #[arg(long, value_parser = parse_seed, default_value = "0")]
        from: u64,
        #[arg(long, short = 'j', default_value_t = 0)]
        parallel: usize,
        #[command(flatten)]
        world: World,
        /// Where failing seeds are written.
        #[arg(long, default_value = "artifacts")]
        out: PathBuf,
        /// Stop at the first failure instead of running every seed.
        #[arg(long)]
        fail_fast: bool,
    },
    /// Measure throughput and latency under a quiet network.
    Bench {
        #[arg(long, value_parser = parse_seed, default_value = "1")]
        seed: u64,
        #[arg(long, default_value_t = 3)]
        servers: u32,
        #[arg(long, default_value_t = 16)]
        clients: u32,
        /// Simulated seconds to measure.
        #[arg(long, default_value_t = 60)]
        secs: u64,
    },
}

/// The shape of the universe, shared by every subcommand so that a seed means
/// the same thing everywhere.
#[derive(Args, Debug, Clone)]
struct World {
    /// Fault policy: benign, nemesis, torture, network, storage.
    #[arg(long, default_value = "nemesis")]
    policy: String,
    #[arg(long, default_value_t = 3)]
    servers: u32,
    #[arg(long, default_value_t = 4)]
    clients: u32,
    /// Key space. Fewer keys means more contention and better bug-finding.
    #[arg(long, default_value_t = 5)]
    keys: u32,
    /// Simulated seconds per run.
    #[arg(long, default_value_t = 600)]
    secs: u64,
    /// Percent of operations that are reads.
    #[arg(long, default_value_t = 40)]
    reads: u32,
    /// Serve reads from the leader's lease. Not linearizable under clock skew —
    /// turn it on to watch the simulator demonstrate that.
    #[arg(long)]
    lease_reads: bool,
}

impl World {
    fn config(&self, seed: u64, trace: TraceMode) -> ScenarioConfig {
        let policy = FaultPolicy::preset(&self.policy).unwrap_or_else(|| {
            eprintln!(
                "unknown policy {:?}; known: {}",
                self.policy,
                FaultPolicy::PRESETS.join(", ")
            );
            std::process::exit(2);
        });
        ScenarioConfig {
            seed,
            servers: self.servers,
            clients: self.clients,
            keys: self.keys,
            max_ops_per_client: 20_000,
            think_time: Nanos::from_millis(25),
            duration: Nanos::from_secs(self.secs),
            recovery: Nanos::from_secs(120),
            policy,
            read_percent: self.reads,
            read_mode: chronolog::client::ReadMode::Linearizable,
            trace,
            lease_reads: self.lease_reads,
            limits: Limits::default(),
        }
    }
}

/// Accepts `0x8f3a2b1c`, a plain decimal, or bare hex.
fn parse_seed(s: &str) -> Result<u64, String> {
    let t = s.trim();
    if let Some(hex) = t.strip_prefix("0x").or_else(|| t.strip_prefix("0X")) {
        return u64::from_str_radix(hex, 16).map_err(|e| e.to_string());
    }
    t.parse::<u64>()
        .or_else(|_| u64::from_str_radix(t, 16))
        .map_err(|e| e.to_string())
}

fn threads(requested: usize) -> usize {
    if requested > 0 {
        return requested;
    }
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

fn main() {
    let cli = Cli::parse();
    let code = match cli.command {
        Command::Run {
            seed,
            world,
            history,
        } => cmd_run(seed, &world, history),
        Command::Replay {
            seed,
            world,
            full,
            node,
            limit,
        } => cmd_replay(seed, &world, full, node, limit),
        Command::Check {
            seeds,
            from,
            world,
            parallel,
        } => cmd_check(seeds, from, &world, threads(parallel)),
        Command::Swarm {
            seeds,
            from,
            parallel,
            world,
            out,
            fail_fast,
        } => cmd_swarm(seeds, from, threads(parallel), &world, &out, fail_fast),
        Command::Bench {
            seed,
            servers,
            clients,
            secs,
        } => cmd_bench(seed, servers, clients, secs),
    };
    std::process::exit(code);
}

// ---------------------------------------------------------------------------
// run
// ---------------------------------------------------------------------------

fn cmd_run(seed: u64, world: &World, show_history: bool) -> i32 {
    let config = world.config(seed, TraceMode::Tail(4096));
    println!(
        "chronoscope run  seed {seed:#018x}  policy {}",
        world.policy
    );
    println!();
    let report = scenario::run(&config);
    print!("{report}");
    println!();
    println!(
        "compressed {:.1} node-hours across {} nodes into {:.2?} of wall clock",
        report.node_hours(world.servers),
        world.servers,
        report.wall_time
    );

    if show_history {
        println!("\n--- client history ---");
        for e in report.history.events().iter().take(200) {
            println!("{e}");
        }
    }

    match report.failure() {
        None => {
            println!("\nPASS");
            0
        }
        Some(why) => {
            println!("\nFAIL\n{why}");
            println!(
                "reproduce:  chronoscope replay --seed {seed:#018x} --policy {} --servers {} \
                 --clients {} --keys {} --secs {}",
                world.policy, world.servers, world.clients, world.keys, world.secs
            );
            1
        }
    }
}

// ---------------------------------------------------------------------------
// replay
// ---------------------------------------------------------------------------

fn cmd_replay(seed: u64, world: &World, full: bool, node: Option<u32>, limit: usize) -> i32 {
    let mode = if full {
        TraceMode::Full
    } else {
        TraceMode::Tail(limit.max(64) * 8)
    };
    let config = world.config(seed, mode);
    println!(
        "chronoscope replay  seed {seed:#018x}  policy {}",
        world.policy
    );
    println!();
    let report = scenario::run(&config);

    println!("--- timeline ---");
    let mut shown = 0usize;
    let mut skipped = 0usize;
    for entry in &report.trace {
        if let Some(n) = node {
            if entry.event.node() != Some(n) {
                continue;
            }
        }
        // Polls and disk reads are the bulk of any trace and almost never the
        // story. `--full` keeps them.
        if !full && !is_interesting(&entry.event) {
            skipped += 1;
            continue;
        }
        if shown >= limit {
            skipped += 1;
            continue;
        }
        println!("{entry}");
        shown += 1;
    }
    if skipped > 0 {
        println!("... {skipped} events not shown (raise --limit, or pass --full)");
    }

    println!();
    print!("{report}");
    match report.failure() {
        None => {
            println!("\nPASS — this seed is clean");
            0
        }
        Some(why) => {
            println!("\nFAIL\n{why}");
            1
        }
    }
}

/// The events that tell the story of a failure.
fn is_interesting(e: &chrono_sim::trace::Event) -> bool {
    use chrono_sim::trace::Event as E;
    matches!(
        e,
        E::Boot { .. }
            | E::Crash { .. }
            | E::Restart { .. }
            | E::Partition { .. }
            | E::Heal { .. }
            | E::Pause { .. }
            | E::Resume { .. }
            | E::ClockStep { .. }
            | E::TornWrite { .. }
            | E::LostWrite { .. }
            | E::Enospc { .. }
            | E::Note { .. }
    )
}

// ---------------------------------------------------------------------------
// check — the determinism guard
// ---------------------------------------------------------------------------

fn cmd_check(seeds: u64, from: u64, world: &World, jobs: usize) -> i32 {
    println!(
        "chronoscope check  {seeds} seeds from {from:#018x}  policy {}  {jobs} threads",
        world.policy
    );
    println!("each seed runs twice; the event traces must hash identically\n");

    let next = Arc::new(AtomicU64::new(0));
    let failures: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let done = Arc::new(AtomicU64::new(0));
    let started = std::time::Instant::now();

    std::thread::scope(|scope| {
        for _ in 0..jobs {
            let (next, failures, done) =
                (Arc::clone(&next), Arc::clone(&failures), Arc::clone(&done));
            let world = world.clone();
            scope.spawn(move || loop {
                let i = next.fetch_add(1, Ordering::SeqCst);
                if i >= seeds {
                    return;
                }
                let seed = from.wrapping_add(i);
                let config = world.config(seed, TraceMode::HashOnly);
                let report = scenario::check_determinism(&config);
                if !report.is_deterministic() {
                    failures.lock().unwrap().push(report.to_string());
                }
                let n = done.fetch_add(1, Ordering::SeqCst) + 1;
                if n % 8 == 0 || n == seeds {
                    print!("\r  {n}/{seeds} verified");
                    let _ = std::io::stdout().flush();
                }
            });
        }
    });
    println!();

    let failures = failures.lock().unwrap();
    println!("\nchecked {seeds} seeds in {:.2?}", started.elapsed());
    if failures.is_empty() {
        println!("DETERMINISTIC — every seed reproduced exactly");
        0
    } else {
        println!("{} SEED(S) DIVERGED\n", failures.len());
        for f in failures.iter().take(5) {
            println!("{f}\n");
        }
        1
    }
}

// ---------------------------------------------------------------------------
// swarm
// ---------------------------------------------------------------------------

fn cmd_swarm(
    seeds: u64,
    from: u64,
    jobs: usize,
    world: &World,
    out: &PathBuf,
    fail_fast: bool,
) -> i32 {
    println!(
        "chronoscope swarm  {seeds} seeds from {from:#018x}  policy {}  {jobs} threads",
        world.policy
    );
    if let Err(e) = std::fs::create_dir_all(out) {
        eprintln!("cannot create {}: {e}", out.display());
        return 2;
    }

    let next = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let virtual_nanos = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let started = std::time::Instant::now();
    let servers = world.servers;

    std::thread::scope(|scope| {
        for _ in 0..jobs {
            let (next, done, failed, virtual_nanos, stop) = (
                Arc::clone(&next),
                Arc::clone(&done),
                Arc::clone(&failed),
                Arc::clone(&virtual_nanos),
                Arc::clone(&stop),
            );
            let world = world.clone();
            let out = out.clone();
            scope.spawn(move || loop {
                if stop.load(Ordering::SeqCst) {
                    return;
                }
                let i = next.fetch_add(1, Ordering::SeqCst);
                if i >= seeds {
                    return;
                }
                let seed = from.wrapping_add(i);
                // `Tail` rather than `HashOnly`: a failing seed is worth far
                // more with the last few thousand events attached, and the
                // memory stays bounded.
                let config = world.config(seed, TraceMode::Tail(2048));
                let report = scenario::run(&config);
                virtual_nanos.fetch_add(report.virtual_time.as_nanos(), Ordering::Relaxed);

                if let Some(why) = report.failure() {
                    failed.fetch_add(1, Ordering::SeqCst);
                    write_artifact(&out, seed, &world, &report, &why);
                    println!("\n  FAIL seed {seed:#018x}  {}", first_line(&why));
                    if fail_fast {
                        stop.store(true, Ordering::SeqCst);
                        return;
                    }
                }

                let n = done.fetch_add(1, Ordering::SeqCst) + 1;
                if n % 16 == 0 || n == seeds {
                    let hours =
                        virtual_nanos.load(Ordering::Relaxed) as f64 / 3.6e12 * servers as f64;
                    print!(
                        "\r  {n}/{seeds} runs  {} failures  {hours:.0} node-hours  {:.0?} elapsed",
                        failed.load(Ordering::SeqCst),
                        started.elapsed()
                    );
                    let _ = std::io::stdout().flush();
                }
            });
        }
    });
    println!();

    let n_failed = failed.load(Ordering::SeqCst);
    let hours = virtual_nanos.load(Ordering::Relaxed) as f64 / 3.6e12 * servers as f64;
    let wall = started.elapsed();
    println!("\n--- swarm complete ---");
    println!("  runs            {}", done.load(Ordering::SeqCst));
    println!("  failures        {n_failed}");
    println!("  simulated       {hours:.1} node-hours");
    println!("  wall clock      {wall:.2?}");
    println!(
        "  compression     {:.0}x",
        (hours * 3600.0) / wall.as_secs_f64().max(1e-9)
    );
    if n_failed > 0 {
        println!("\n  artifacts in {}/", out.display());
        1
    } else {
        0
    }
}

fn first_line(s: &str) -> &str {
    s.lines().next().unwrap_or(s)
}

/// A failing seed is only useful if it arrives with everything needed to
/// reproduce it. That is the whole artifact.
fn write_artifact(out: &Path, seed: u64, world: &World, report: &RunReport, why: &str) {
    let path = out.join(format!("seed-{seed:016x}.txt"));
    let mut buf = String::new();
    buf.push_str(&format!("# chronoscope failure: seed {seed:#018x}\n\n"));
    buf.push_str("## reproduce\n\n");
    buf.push_str(&format!(
        "chronoscope replay --seed {seed:#018x} --policy {} --servers {} --clients {} \
         --keys {} --secs {}{}\n\n",
        world.policy,
        world.servers,
        world.clients,
        world.keys,
        world.secs,
        if world.lease_reads {
            " --lease-reads"
        } else {
            ""
        }
    ));
    buf.push_str("## what failed\n\n");
    buf.push_str(why);
    buf.push_str("\n\n## run summary\n\n");
    buf.push_str(&report.to_string());
    buf.push_str("\n## trace tail\n\n");
    for e in report.trace.iter().rev().take(300).rev() {
        buf.push_str(&format!("{e}\n"));
    }
    if let Err(e) = std::fs::write(&path, buf) {
        eprintln!("could not write {}: {e}", path.display());
    }
}

// ---------------------------------------------------------------------------
// bench
// ---------------------------------------------------------------------------

fn cmd_bench(seed: u64, servers: u32, clients: u32, secs: u64) -> i32 {
    println!("chronoscope bench  {servers} servers, {clients} clients, {secs} simulated seconds");
    println!("quiet network and disk; this measures the system, not the faults\n");

    let config = ScenarioConfig {
        seed,
        servers,
        clients,
        keys: 64,
        max_ops_per_client: 1_000_000,
        // No think time: offer as much load as the clients can generate.
        think_time: Nanos::ZERO,
        duration: Nanos::from_secs(secs),
        recovery: Nanos::from_secs(1),
        policy: FaultPolicy::benign(),
        read_percent: 0,
        read_mode: chronolog::client::ReadMode::Linearizable,
        trace: TraceMode::HashOnly,
        lease_reads: false,
        limits: Limits::default(),
    };
    let report = scenario::run(&config);

    let secs_f = report.virtual_time.as_nanos() as f64 / 1e9;
    let ops = report.ops_ok as f64;
    let mut latencies: Vec<u64> = report
        .history
        .events()
        .iter()
        .map(|e| e.returned.saturating_sub(e.invoked))
        .collect();
    latencies.sort_unstable();
    let pct = |p: f64| -> f64 {
        if latencies.is_empty() {
            return 0.0;
        }
        let i = ((latencies.len() as f64 - 1.0) * p) as usize;
        latencies[i] as f64 / 1e6
    };

    println!("  writes           {}", report.ops_ok);
    println!("  simulated        {:.1}s", secs_f);
    println!(
        "  throughput       {:.0} writes/sec",
        ops / secs_f.max(1e-9)
    );
    println!("  latency p50      {:.2} ms", pct(0.50));
    println!("  latency p99      {:.2} ms", pct(0.99));
    println!("  latency p99.9    {:.2} ms", pct(0.999));
    println!("  fsyncs           {}", report.stats.fsyncs);
    println!(
        "  entries/fsync    {:.1}   <- group commit; 1.0 means it is not batching",
        ops / report.stats.fsyncs.max(1) as f64
    );
    println!(
        "  wall clock       {:.2?}  ({:.0}x real time)",
        report.wall_time,
        report.speedup()
    );
    0
}
