//! What the universe is allowed to do to you.
//!
//! Every field here is a knob the kernel reads through the seeded PRNG. Nothing
//! samples entropy on its own, so the entire fault schedule of a run — which
//! packet was dropped at 3.7 simulated hours, which sector tore — is a pure
//! function of the seed and this struct.
//!
//! The policies are deliberately harsher than production. A simulator tuned to
//! realistic failure rates finds realistic bugs at a realistic rate, which is
//! to say almost never. The point is to compress a decade of bad luck into a
//! few simulated hours.

use crate::rng::LatencyDist;
use crate::time::Nanos;

/// Per-link behaviour.
#[derive(Clone, Debug)]
pub struct LinkPolicy {
    pub latency: LatencyDist,
    /// Independent packet loss, on top of any partition.
    pub loss_ppm: u32,
    /// Chance the link delivers a packet twice, at independently sampled times.
    /// Raft must be idempotent under this; most hand-rolled implementations are
    /// not, on the vote path.
    pub duplicate_ppm: u32,
    /// Flip a bit in flight. Exists to prove the wire decoder never panics and
    /// never accepts a corrupt frame.
    pub corrupt_ppm: u32,
}

impl Default for LinkPolicy {
    fn default() -> Self {
        Self {
            latency: LatencyDist::datacenter(),
            loss_ppm: 2_000, // 0.2%
            duplicate_ppm: 500,
            corrupt_ppm: 0,
        }
    }
}

/// Disk behaviour, and the reason crash-consistency bugs are findable at all.
#[derive(Clone, Debug)]
pub struct DiskPolicy {
    pub write_latency: LatencyDist,
    pub fsync_latency: LatencyDist,
    pub read_latency: LatencyDist,
    /// The atomicity unit on power loss. Real drives are 512B or 4K; a write
    /// spanning sectors can land partially.
    pub sector_size: u64,
    /// On power loss, chance an un-fsynced write lands *partially* — some
    /// sectors written, some not. This is the fault that finds torn-tail bugs.
    pub torn_write_ppm: u32,
    /// On power loss, chance an un-fsynced write is simply gone.
    pub lost_write_ppm: u32,
    /// Bit rot on durable data. Should be caught by the per-record CRC.
    pub corrupt_ppm: u32,
    /// Fail writes once this many bytes have been written to the node.
    pub enospc_after_bytes: Option<u64>,
    /// Chance any single operation hits a latency spike.
    pub slow_ppm: u32,
    pub slow_multiplier: u64,
}

impl Default for DiskPolicy {
    fn default() -> Self {
        Self {
            // An NVMe write is fast; the fsync is what costs you.
            write_latency: LatencyDist::new(vec![(980, 8_000, 40_000), (20, 40_000, 900_000)]),
            fsync_latency: LatencyDist::new(vec![
                (900, 200_000, 800_000),
                (95, 800_000, 5_000_000),
                (5, 5_000_000, 90_000_000), // the p99.9 that stalls group commit
            ]),
            read_latency: LatencyDist::new(vec![(1, 5_000, 30_000)]),
            sector_size: 4096,
            torn_write_ppm: 300_000, // 30% of un-fsynced writes tear on crash
            lost_write_ppm: 400_000,
            corrupt_ppm: 0,
            enospc_after_bytes: None,
            slow_ppm: 1_000,
            slow_multiplier: 40,
        }
    }
}

/// Per-node clock error.
#[derive(Clone, Debug)]
pub struct ClockPolicy {
    /// Initial wall-clock offset, sampled in `[-max_skew, +max_skew]`.
    pub max_skew: Nanos,
    /// Rate error in parts per million, sampled in `[-max, +max]`. 100ppm is
    /// about 8 seconds a day, which is a bad but real crystal.
    pub max_drift_ppm: i64,
    /// Chance per simulated second that the wall clock steps (an NTP correction
    /// applied as a jump rather than a slew).
    pub step_ppm_per_sec: u32,
    pub max_step: Nanos,
}

impl Default for ClockPolicy {
    fn default() -> Self {
        Self {
            max_skew: Nanos::from_millis(50),
            max_drift_ppm: 200,
            step_ppm_per_sec: 0,
            max_step: Nanos::from_millis(500),
        }
    }
}

/// Environmental chaos: the things an operator, a kernel, or a backhoe does.
#[derive(Clone, Debug)]
pub struct ChaosPolicy {
    /// Chance per simulated second of starting a partition.
    pub partition_ppm_per_sec: u32,
    pub partition_duration: LatencyDist,
    /// Fraction of partitions that are one-way. Asymmetric partitions are
    /// nastier than symmetric ones and are why pre-vote exists.
    pub asymmetric_ppm: u32,
    /// Chance per simulated second that a node is hard-killed (power loss, no
    /// unwinding, no flush).
    pub crash_ppm_per_sec: u32,
    pub restart_delay: LatencyDist,
    /// Chance per simulated second that a node's process freezes (GC pause, VM
    /// migration, `SIGSTOP`). The node's clock keeps running; its tasks do not.
    pub pause_ppm_per_sec: u32,
    pub pause_duration: LatencyDist,
    /// Never leave fewer than this many nodes alive. Set to a quorum to test
    /// safety under continuous churn; set to 0 to test recovery from total
    /// cluster loss.
    pub min_alive: usize,
}

impl ChaosPolicy {
    /// Whether any chaos source can actually fire.
    ///
    /// Worth checking: an always-on ticker keeps the event heap non-empty
    /// forever, so a run can never report `Quiesced` and always costs
    /// `horizon / chaos_tick` events even when every rate is zero. At a 100ms
    /// tick, a benign 48-hour run would burn 1.7 million no-op events.
    pub fn is_active(&self) -> bool {
        self.partition_ppm_per_sec > 0 || self.crash_ppm_per_sec > 0 || self.pause_ppm_per_sec > 0
    }
}

impl Default for ChaosPolicy {
    fn default() -> Self {
        Self {
            partition_ppm_per_sec: 40_000,
            partition_duration: LatencyDist::new(vec![
                (700, Nanos::from_millis(200).0, Nanos::from_secs(3).0),
                (300, Nanos::from_secs(3).0, Nanos::from_secs(30).0),
            ]),
            asymmetric_ppm: 350_000,
            crash_ppm_per_sec: 25_000,
            restart_delay: LatencyDist::new(vec![
                (800, Nanos::from_millis(50).0, Nanos::from_secs(2).0),
                (200, Nanos::from_secs(2).0, Nanos::from_secs(20).0),
            ]),
            pause_ppm_per_sec: 20_000,
            pause_duration: LatencyDist::new(vec![
                (850, Nanos::from_millis(20).0, Nanos::from_millis(400).0),
                (150, Nanos::from_millis(400).0, Nanos::from_secs(6).0),
            ]),
            min_alive: 0,
        }
    }
}

/// The complete description of a hostile universe.
#[derive(Clone, Debug)]
pub struct FaultPolicy {
    pub link: LinkPolicy,
    pub disk: DiskPolicy,
    pub clock: ClockPolicy,
    pub chaos: ChaosPolicy,
    /// How often the kernel samples chaos. Finer means more faithful rates and
    /// more events; 100ms is a good balance.
    pub chaos_tick: Nanos,
}

impl Default for FaultPolicy {
    fn default() -> Self {
        Self {
            link: LinkPolicy::default(),
            disk: DiskPolicy::default(),
            clock: ClockPolicy::default(),
            chaos: ChaosPolicy::default(),
            chaos_tick: Nanos::from_millis(100),
        }
    }
}

impl FaultPolicy {
    /// Nothing goes wrong. Used to prove the system works at all before
    /// proving it works under duress, and as the control for `BUGS.md`
    /// reproductions: if a bug repros under `benign`, it is not a fault bug.
    pub fn benign() -> Self {
        Self {
            link: LinkPolicy {
                latency: LatencyDist::uniform(100_000, 400_000),
                loss_ppm: 0,
                duplicate_ppm: 0,
                corrupt_ppm: 0,
            },
            disk: DiskPolicy {
                torn_write_ppm: 0,
                lost_write_ppm: 0,
                corrupt_ppm: 0,
                slow_ppm: 0,
                ..DiskPolicy::default()
            },
            clock: ClockPolicy {
                max_skew: Nanos::ZERO,
                max_drift_ppm: 0,
                step_ppm_per_sec: 0,
                max_step: Nanos::ZERO,
            },
            chaos: ChaosPolicy {
                partition_ppm_per_sec: 0,
                crash_ppm_per_sec: 0,
                pause_ppm_per_sec: 0,
                ..ChaosPolicy::default()
            },
            chaos_tick: Nanos::from_millis(100),
        }
    }

    /// The default working policy: everything on, at rates that produce a few
    /// dozen faults per simulated minute.
    pub fn nemesis() -> Self {
        Self::default()
    }

    /// Absurd. Partitions constantly, crashes constantly, tears most writes.
    /// Nothing should make progress here — but nothing should violate safety
    /// either, and that is what this mode tests.
    pub fn torture() -> Self {
        Self {
            link: LinkPolicy {
                latency: LatencyDist::new(vec![
                    (600, 150_000, 2_000_000),
                    (300, 2_000_000, 60_000_000),
                    (100, 60_000_000, 900_000_000),
                ]),
                loss_ppm: 80_000,
                duplicate_ppm: 40_000,
                corrupt_ppm: 0,
            },
            disk: DiskPolicy {
                torn_write_ppm: 600_000,
                lost_write_ppm: 500_000,
                slow_ppm: 30_000,
                slow_multiplier: 200,
                ..DiskPolicy::default()
            },
            clock: ClockPolicy {
                max_skew: Nanos::from_millis(400),
                max_drift_ppm: 3_000,
                step_ppm_per_sec: 20_000,
                max_step: Nanos::from_secs(2),
            },
            chaos: ChaosPolicy {
                partition_ppm_per_sec: 200_000,
                crash_ppm_per_sec: 120_000,
                pause_ppm_per_sec: 90_000,
                asymmetric_ppm: 500_000,
                ..ChaosPolicy::default()
            },
            chaos_tick: Nanos::from_millis(50),
        }
    }

    /// Network-only chaos with a pristine disk: isolates consensus bugs from
    /// storage bugs when triaging a failing seed.
    pub fn network_only() -> Self {
        let mut p = Self::nemesis();
        p.disk = FaultPolicy::benign().disk;
        p.chaos.crash_ppm_per_sec = 0;
        p
    }

    /// Disk-only chaos with a pristine network: the converse.
    pub fn storage_only() -> Self {
        let mut p = Self::nemesis();
        p.link = FaultPolicy::benign().link;
        p.chaos.partition_ppm_per_sec = 0;
        p.chaos.pause_ppm_per_sec = 0;
        p
    }

    pub fn preset(name: &str) -> Option<Self> {
        match name {
            "benign" => Some(Self::benign()),
            "nemesis" => Some(Self::nemesis()),
            "torture" => Some(Self::torture()),
            "network" => Some(Self::network_only()),
            "storage" => Some(Self::storage_only()),
            _ => None,
        }
    }

    pub const PRESETS: &'static [&'static str] =
        &["benign", "nemesis", "torture", "network", "storage"];
}
