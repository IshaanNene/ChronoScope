//! The event trace, and the rolling hash that makes determinism falsifiable.
//!
//! Claiming a simulator is deterministic is easy. Proving it is what this
//! module is for: every scheduling decision, packet, and disk operation is
//! folded into a 64-bit rolling hash. Run a seed twice, compare hashes. If they
//! differ, something in the system read entropy the kernel did not hand it —
//! `HashMap` iteration, a pointer address, a real clock — and CI fails.
//!
//! The hash is order-sensitive and position-sensitive by construction, so a
//! divergence in *ordering alone* (the same events in a different sequence,
//! which is exactly what a scheduling nondeterminism bug produces) is caught.

use std::fmt;

use crate::time::Nanos;
use crate::traits::NodeId;

/// Why a packet did not arrive. Recorded because "the message vanished" is the
/// single most common thing to be confused by when reading a failing trace.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropReason {
    Partitioned,
    RandomLoss,
    NodeDown,
    NodeGone,
}

impl DropReason {
    fn tag(self) -> u8 {
        match self {
            DropReason::Partitioned => 0,
            DropReason::RandomLoss => 1,
            DropReason::NodeDown => 2,
            DropReason::NodeGone => 3,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            DropReason::Partitioned => "partitioned",
            DropReason::RandomLoss => "loss",
            DropReason::NodeDown => "node-down",
            DropReason::NodeGone => "no-such-node",
        }
    }
}

/// One thing that happened, at one virtual instant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    Boot {
        node: NodeId,
    },
    Spawn {
        node: NodeId,
        task: u64,
    },
    Poll {
        node: NodeId,
        task: u64,
    },
    TaskDone {
        node: NodeId,
        task: u64,
    },
    Sleep {
        node: NodeId,
        task: u64,
        until: Nanos,
    },
    Send {
        from: NodeId,
        to: NodeId,
        msg: u64,
        len: usize,
    },
    Deliver {
        from: NodeId,
        to: NodeId,
        msg: u64,
        len: usize,
    },
    Dropped {
        from: NodeId,
        to: NodeId,
        msg: u64,
        why: DropReason,
    },
    Duplicated {
        from: NodeId,
        to: NodeId,
        msg: u64,
    },
    Corrupted {
        from: NodeId,
        to: NodeId,
        msg: u64,
        byte: usize,
    },
    DiskWrite {
        node: NodeId,
        file: u64,
        offset: u64,
        len: usize,
    },
    DiskRead {
        node: NodeId,
        file: u64,
        offset: u64,
        len: usize,
    },
    Fsync {
        node: NodeId,
        file: u64,
        pending: usize,
    },
    Truncate {
        node: NodeId,
        file: u64,
        to: u64,
    },
    TornWrite {
        node: NodeId,
        file: u64,
        offset: u64,
        kept: u64,
        of: u64,
    },
    LostWrite {
        node: NodeId,
        file: u64,
        offset: u64,
        len: usize,
    },
    Enospc {
        node: NodeId,
        file: u64,
    },
    Crash {
        node: NodeId,
    },
    Restart {
        node: NodeId,
    },
    Partition {
        a: NodeId,
        b: NodeId,
        one_way: bool,
    },
    Heal {
        a: NodeId,
        b: NodeId,
    },
    Pause {
        node: NodeId,
        until: Nanos,
    },
    Resume {
        node: NodeId,
    },
    ClockStep {
        node: NodeId,
        delta: i64,
    },
    /// Application-level annotation. `chronolog` uses these to mark elections,
    /// commits, and client operations so a failing trace reads as a story
    /// rather than as packet soup.
    Note {
        node: NodeId,
        text: String,
    },
}

impl Event {
    fn tag(&self) -> u8 {
        match self {
            Event::Boot { .. } => 1,
            Event::Spawn { .. } => 2,
            Event::Poll { .. } => 3,
            Event::TaskDone { .. } => 4,
            Event::Sleep { .. } => 5,
            Event::Send { .. } => 6,
            Event::Deliver { .. } => 7,
            Event::Dropped { .. } => 8,
            Event::Duplicated { .. } => 9,
            Event::Corrupted { .. } => 10,
            Event::DiskWrite { .. } => 11,
            Event::DiskRead { .. } => 12,
            Event::Fsync { .. } => 13,
            Event::Truncate { .. } => 14,
            Event::TornWrite { .. } => 15,
            Event::LostWrite { .. } => 16,
            Event::Enospc { .. } => 17,
            Event::Crash { .. } => 18,
            Event::Restart { .. } => 19,
            Event::Partition { .. } => 20,
            Event::Heal { .. } => 21,
            Event::Pause { .. } => 22,
            Event::Resume { .. } => 23,
            Event::ClockStep { .. } => 24,
            Event::Note { .. } => 25,
        }
    }

    /// Which node this is "about", for filtering a trace down to one node.
    pub fn node(&self) -> Option<NodeId> {
        match *self {
            Event::Boot { node }
            | Event::Spawn { node, .. }
            | Event::Poll { node, .. }
            | Event::TaskDone { node, .. }
            | Event::Sleep { node, .. }
            | Event::DiskWrite { node, .. }
            | Event::DiskRead { node, .. }
            | Event::Fsync { node, .. }
            | Event::Truncate { node, .. }
            | Event::TornWrite { node, .. }
            | Event::LostWrite { node, .. }
            | Event::Enospc { node, .. }
            | Event::Crash { node }
            | Event::Restart { node }
            | Event::Pause { node, .. }
            | Event::Resume { node }
            | Event::ClockStep { node, .. }
            | Event::Note { node, .. } => Some(node),
            Event::Send { from, .. }
            | Event::Deliver { from, .. }
            | Event::Dropped { from, .. }
            | Event::Duplicated { from, .. }
            | Event::Corrupted { from, .. } => Some(from),
            Event::Partition { a, .. } | Event::Heal { a, .. } => Some(a),
        }
    }

    /// Canonical bytes for hashing. Deliberately hand-rolled rather than
    /// derived: a `Hash` impl would depend on `DefaultHasher`, whose output is
    /// explicitly not stable across Rust releases, which would make the
    /// determinism guard itself nondeterministic.
    fn feed(&self, h: &mut Fnv) {
        h.byte(self.tag());
        let mut u = |v: u64| h.u64(v);
        match self {
            Event::Boot { node }
            | Event::Crash { node }
            | Event::Restart { node }
            | Event::Resume { node } => u(*node as u64),
            Event::Spawn { node, task }
            | Event::Poll { node, task }
            | Event::TaskDone { node, task } => {
                u(*node as u64);
                u(*task);
            }
            Event::Sleep { node, task, until } => {
                u(*node as u64);
                u(*task);
                u(until.0);
            }
            Event::Send { from, to, msg, len } | Event::Deliver { from, to, msg, len } => {
                u(*from as u64);
                u(*to as u64);
                u(*msg);
                u(*len as u64);
            }
            Event::Dropped { from, to, msg, why } => {
                u(*from as u64);
                u(*to as u64);
                u(*msg);
                h.byte(why.tag());
            }
            Event::Duplicated { from, to, msg } => {
                u(*from as u64);
                u(*to as u64);
                u(*msg);
            }
            Event::Corrupted {
                from,
                to,
                msg,
                byte,
            } => {
                u(*from as u64);
                u(*to as u64);
                u(*msg);
                u(*byte as u64);
            }
            Event::DiskWrite {
                node,
                file,
                offset,
                len,
            }
            | Event::DiskRead {
                node,
                file,
                offset,
                len,
            } => {
                u(*node as u64);
                u(*file);
                u(*offset);
                u(*len as u64);
            }
            Event::Fsync {
                node,
                file,
                pending,
            } => {
                u(*node as u64);
                u(*file);
                u(*pending as u64);
            }
            Event::Truncate { node, file, to } => {
                u(*node as u64);
                u(*file);
                u(*to);
            }
            Event::TornWrite {
                node,
                file,
                offset,
                kept,
                of,
            } => {
                u(*node as u64);
                u(*file);
                u(*offset);
                u(*kept);
                u(*of);
            }
            Event::LostWrite {
                node,
                file,
                offset,
                len,
            } => {
                u(*node as u64);
                u(*file);
                u(*offset);
                u(*len as u64);
            }
            Event::Enospc { node, file } => {
                u(*node as u64);
                u(*file);
            }
            Event::Partition { a, b, one_way } => {
                u(*a as u64);
                u(*b as u64);
                h.byte(*one_way as u8);
            }
            Event::Heal { a, b } => {
                u(*a as u64);
                u(*b as u64);
            }
            Event::Pause { node, until } => {
                u(*node as u64);
                u(until.0);
            }
            Event::ClockStep { node, delta } => {
                u(*node as u64);
                u(*delta as u64);
            }
            Event::Note { node, text } => {
                u(*node as u64);
                h.bytes(text.as_bytes());
            }
        }
    }
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Event::Boot { node } => write!(f, "n{node} boot"),
            Event::Spawn { node, task } => write!(f, "n{node} spawn t{task}"),
            Event::Poll { node, task } => write!(f, "n{node} poll t{task}"),
            Event::TaskDone { node, task } => write!(f, "n{node} done t{task}"),
            Event::Sleep { node, task, until } => write!(f, "n{node} t{task} sleep -> {until}"),
            Event::Send { from, to, msg, len } => write!(f, "n{from} -> n{to} send #{msg} {len}B"),
            Event::Deliver { from, to, msg, len } => {
                write!(f, "n{from} -> n{to} recv #{msg} {len}B")
            }
            Event::Dropped { from, to, msg, why } => {
                write!(f, "n{from} -> n{to} DROP #{msg} ({})", why.as_str())
            }
            Event::Duplicated { from, to, msg } => write!(f, "n{from} -> n{to} DUP #{msg}"),
            Event::Corrupted {
                from,
                to,
                msg,
                byte,
            } => {
                write!(f, "n{from} -> n{to} CORRUPT #{msg} @byte {byte}")
            }
            Event::DiskWrite {
                node,
                file,
                offset,
                len,
            } => {
                write!(f, "n{node} write f{file:x} @{offset} {len}B")
            }
            Event::DiskRead {
                node,
                file,
                offset,
                len,
            } => {
                write!(f, "n{node} read f{file:x} @{offset} {len}B")
            }
            Event::Fsync {
                node,
                file,
                pending,
            } => {
                write!(f, "n{node} fsync f{file:x} ({pending} pending)")
            }
            Event::Truncate { node, file, to } => write!(f, "n{node} truncate f{file:x} -> {to}"),
            Event::TornWrite {
                node,
                file,
                offset,
                kept,
                of,
            } => {
                write!(f, "n{node} TORN f{file:x} @{offset} kept {kept}/{of}B")
            }
            Event::LostWrite {
                node,
                file,
                offset,
                len,
            } => {
                write!(f, "n{node} LOST WRITE f{file:x} @{offset} {len}B")
            }
            Event::Enospc { node, file } => write!(f, "n{node} ENOSPC f{file:x}"),
            Event::Crash { node } => write!(f, "n{node} *** CRASH ***"),
            Event::Restart { node } => write!(f, "n{node} *** RESTART ***"),
            Event::Partition { a, b, one_way } => {
                write!(
                    f,
                    "PARTITION n{a} {} n{b}",
                    if *one_way { "-/->" } else { "<-/->" }
                )
            }
            Event::Heal { a, b } => write!(f, "HEAL n{a} <--> n{b}"),
            Event::Pause { node, until } => write!(f, "n{node} PAUSE until {until}"),
            Event::Resume { node } => write!(f, "n{node} RESUME"),
            Event::ClockStep { node, delta } => write!(f, "n{node} clock step {delta}ns"),
            Event::Note { node, text } => write!(f, "n{node} | {text}"),
        }
    }
}

/// A trace entry: when it happened and what it was.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub at: Nanos,
    pub seq: u64,
    pub event: Event,
}

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{:>14}] {}", self.at.to_string(), self.event)
    }
}

/// FNV-1a. Chosen over anything fancier because the whole determinism argument
/// rests on the hash being specified rather than "whatever the standard library
/// does this release".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fnv(pub u64);

impl Default for Fnv {
    fn default() -> Self {
        Fnv::new()
    }
}

impl Fnv {
    pub const fn new() -> Self {
        Fnv(0xcbf2_9ce4_8422_2325)
    }

    #[inline]
    pub fn byte(&mut self, b: u8) {
        self.0 ^= b as u64;
        self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
    }

    #[inline]
    pub fn u64(&mut self, v: u64) {
        for i in 0..8 {
            self.byte((v >> (i * 8)) as u8);
        }
    }

    pub fn bytes(&mut self, b: &[u8]) {
        self.u64(b.len() as u64);
        for &x in b {
            self.byte(x);
        }
    }

    pub fn get(&self) -> u64 {
        self.0
    }
}

/// How much of the trace to keep in memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TraceMode {
    /// Hash only. What the swarm uses: constant memory across a four-hour
    /// simulated run, and still enough to detect divergence.
    HashOnly,
    /// Keep every entry. What `replay --trace` uses.
    Full,
    /// Keep the last N entries — enough to see what led to a violation without
    /// holding a million events.
    Tail(usize),
}

/// Folds events into a hash, and optionally retains them.
#[derive(Debug)]
pub struct Recorder {
    mode: TraceMode,
    hash: Fnv,
    count: u64,
    entries: std::collections::VecDeque<Entry>,
    /// Checkpoints of the rolling hash, so a divergence can be bisected to a
    /// range of events instead of just "somewhere in the run".
    checkpoints: Vec<(u64, u64)>,
    checkpoint_every: u64,
}

impl Recorder {
    pub fn new(mode: TraceMode) -> Self {
        Self {
            mode,
            hash: Fnv::new(),
            count: 0,
            entries: std::collections::VecDeque::new(),
            checkpoints: Vec::new(),
            checkpoint_every: 1024,
        }
    }

    pub fn record(&mut self, at: Nanos, seq: u64, event: Event) {
        self.hash.u64(at.0);
        self.hash.u64(seq);
        event.feed(&mut self.hash);
        self.count += 1;
        if self.count % self.checkpoint_every == 0 {
            self.checkpoints.push((self.count, self.hash.get()));
        }
        match self.mode {
            TraceMode::HashOnly => {}
            TraceMode::Full => self.entries.push_back(Entry { at, seq, event }),
            // `VecDeque`, not `Vec`: this runs on every event of a multi-million
            // event trace, and `Vec::remove(0)` would make it quadratic.
            TraceMode::Tail(n) => {
                if n > 0 {
                    while self.entries.len() >= n {
                        self.entries.pop_front();
                    }
                    self.entries.push_back(Entry { at, seq, event });
                }
            }
        }
    }

    pub fn hash(&self) -> u64 {
        self.hash.get()
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn entries(&self) -> impl Iterator<Item = &Entry> {
        self.entries.iter()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn checkpoints(&self) -> &[(u64, u64)] {
        &self.checkpoints
    }

    /// First event index at which two runs of the same seed diverged. `None`
    /// means the checkpoint streams agree as far as both go.
    pub fn first_divergence(a: &Recorder, b: &Recorder) -> Option<u64> {
        for (x, y) in a.checkpoints.iter().zip(b.checkpoints.iter()) {
            if x != y {
                return Some(x.0);
            }
        }
        if a.hash() != b.hash() {
            let last = a.checkpoints.last().map(|c| c.0).unwrap_or(0);
            return Some(last);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(mode: TraceMode) -> Recorder {
        let mut r = Recorder::new(mode);
        for i in 0..100u64 {
            r.record(
                Nanos(i * 1000),
                i,
                Event::Poll {
                    node: (i % 3) as u32,
                    task: i,
                },
            );
        }
        r
    }

    #[test]
    fn identical_streams_hash_identically() {
        assert_eq!(
            rec(TraceMode::HashOnly).hash(),
            rec(TraceMode::HashOnly).hash()
        );
    }

    #[test]
    fn trace_mode_does_not_affect_the_hash() {
        assert_eq!(rec(TraceMode::HashOnly).hash(), rec(TraceMode::Full).hash());
        assert_eq!(
            rec(TraceMode::HashOnly).hash(),
            rec(TraceMode::Tail(5)).hash()
        );
    }

    #[test]
    fn reordering_two_events_changes_the_hash() {
        let mut a = Recorder::new(TraceMode::HashOnly);
        let mut b = Recorder::new(TraceMode::HashOnly);
        a.record(Nanos(1), 0, Event::Poll { node: 0, task: 1 });
        a.record(Nanos(1), 1, Event::Poll { node: 0, task: 2 });
        b.record(Nanos(1), 0, Event::Poll { node: 0, task: 2 });
        b.record(Nanos(1), 1, Event::Poll { node: 0, task: 1 });
        assert_ne!(a.hash(), b.hash(), "swapped poll order must be detectable");
    }

    #[test]
    fn a_single_differing_field_changes_the_hash() {
        let mut a = Recorder::new(TraceMode::HashOnly);
        let mut b = Recorder::new(TraceMode::HashOnly);
        a.record(
            Nanos(1),
            0,
            Event::Send {
                from: 0,
                to: 1,
                msg: 7,
                len: 64,
            },
        );
        b.record(
            Nanos(1),
            0,
            Event::Send {
                from: 0,
                to: 1,
                msg: 7,
                len: 65,
            },
        );
        assert_ne!(a.hash(), b.hash());
    }

    #[test]
    fn tail_mode_bounds_memory() {
        let r = rec(TraceMode::Tail(5));
        assert_eq!(r.len(), 5);
        assert_eq!(r.count(), 100);
        // and keeps the *last* five, which is what you want when triaging
        assert_eq!(r.entries().next().unwrap().seq, 95);
    }

    #[test]
    fn divergence_is_localised_to_a_checkpoint() {
        let mut a = Recorder::new(TraceMode::HashOnly);
        let mut b = Recorder::new(TraceMode::HashOnly);
        for i in 0..5000u64 {
            a.record(Nanos(i), i, Event::Poll { node: 0, task: i });
            let task = if i == 3000 { i + 1 } else { i };
            b.record(Nanos(i), i, Event::Poll { node: 0, task });
        }
        let at = Recorder::first_divergence(&a, &b).expect("must detect divergence");
        assert!((3000..=4096).contains(&at), "divergence localised to {at}");
    }

    #[test]
    fn no_divergence_when_identical() {
        assert_eq!(
            Recorder::first_divergence(&rec(TraceMode::HashOnly), &rec(TraceMode::HashOnly)),
            None
        );
    }
}
