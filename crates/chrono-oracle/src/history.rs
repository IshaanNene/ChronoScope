//! Client histories: what was asked, when, and what came back.
//!
//! A history is the only evidence an external observer has. It deliberately
//! knows nothing about Raft, terms, or logs — if a violation is visible here,
//! it is visible to a user, which is the only kind of violation that matters
//! to one.
//!
//! # The three-state outcome
//!
//! Every operation is `Ok`, `Fail`, or **`Unknown`**, and the third one is the
//! reason naive checkers report phantom violations. When a client times out,
//! the write may have committed and the response been lost. The operation
//! neither definitely happened nor definitely did not, so a checker must be
//! free to place it either way. Treating `Unknown` as "did not happen" invents
//! violations; treating it as "happened" hides them.

use std::collections::BTreeMap;
use std::fmt;

/// A value in the store. `None` is absent.
pub type Value = Option<Vec<u8>>;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Op {
    Read {
        key: Vec<u8>,
    },
    Write {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    /// Compare and swap: succeeds only if the current value is `expect`.
    Cas {
        key: Vec<u8>,
        expect: Value,
        value: Value,
    },
}

impl Op {
    pub fn key(&self) -> &[u8] {
        match self {
            Op::Read { key } | Op::Write { key, .. } | Op::Delete { key } | Op::Cas { key, .. } => {
                key
            }
        }
    }

    pub fn is_read(&self) -> bool {
        matches!(self, Op::Read { .. })
    }
}

/// What the client observed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Ret {
    /// A read returned this value.
    Value(Value),
    /// A write or delete was acknowledged.
    Ok,
    /// A CAS whose precondition did not hold.
    CasFailed,
    /// The client never learned the outcome — timeout, crash, lost reply.
    ///
    /// Not a failure. The operation may have taken effect. A checker must be
    /// allowed to linearize it anywhere, or not at all.
    Unknown,
}

/// One completed (or abandoned) client operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub client: u64,
    pub op: Op,
    pub ret: Ret,
    /// When the request left the client, in true virtual nanoseconds.
    pub invoked: u64,
    /// When the response arrived. For `Unknown`, when the client gave up —
    /// which is why an `Unknown` has a genuinely wide window.
    pub returned: u64,
}

impl Event {
    pub fn is_unknown(&self) -> bool {
        self.ret == Ret::Unknown
    }

    /// Does this operation's window overlap the other's? Non-overlapping
    /// operations have a forced order and cannot be permuted.
    pub fn concurrent_with(&self, other: &Event) -> bool {
        self.invoked < other.returned && other.invoked < self.returned
    }
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let show = |v: &Value| match v {
            None => "nil".to_string(),
            Some(b) => String::from_utf8_lossy(b).to_string(),
        };
        let k = String::from_utf8_lossy(self.op.key());
        let op = match &self.op {
            Op::Read { .. } => format!("read({k})"),
            Op::Write { value, .. } => format!("write({k}, {})", String::from_utf8_lossy(value)),
            Op::Delete { .. } => format!("delete({k})"),
            Op::Cas { expect, value, .. } => {
                format!("cas({k}, {} -> {})", show(expect), show(value))
            }
        };
        let ret = match &self.ret {
            Ret::Value(v) => show(v),
            Ret::Ok => "ok".to_string(),
            Ret::CasFailed => "cas-failed".to_string(),
            Ret::Unknown => "UNKNOWN".to_string(),
        };
        write!(
            f,
            "c{:<3} [{:>12}..{:<12}] {:<32} = {ret}",
            self.client, self.invoked, self.returned, op
        )
    }
}

/// A recorded execution, in the order operations were invoked.
#[derive(Clone, Debug, Default)]
pub struct History {
    events: Vec<Event>,
}

impl History {
    pub fn new() -> History {
        History::default()
    }

    pub fn push(&mut self, e: Event) {
        self.events.push(e);
    }

    pub fn events(&self) -> &[Event] {
        &self.events
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn unknown_count(&self) -> usize {
        self.events.iter().filter(|e| e.is_unknown()).count()
    }

    /// Sort by invocation time. The checker relies on this ordering to prune.
    pub fn sorted(&self) -> History {
        let mut events = self.events.clone();
        // Ties broken by client id so the result is a total order and the
        // checker's exploration is reproducible — the same reason the kernel's
        // event heap breaks ties on a sequence number.
        events.sort_by(|a, b| {
            a.invoked
                .cmp(&b.invoked)
                .then(a.returned.cmp(&b.returned))
                .then(a.client.cmp(&b.client))
        });
        History { events }
    }

    /// Split into one sub-history per key.
    ///
    /// This is the optimization that makes checking tractable. Linearizability
    /// checking is NP-complete in general, and the cost is superexponential in
    /// the number of concurrent operations. But operations on *different* keys
    /// commute — a register per key is a composable object — so a history is
    /// linearizable exactly when each per-key projection is. Splitting a
    /// 10,000-operation history across 50 keys turns one intractable problem
    /// into 50 easy ones.
    pub fn by_key(&self) -> BTreeMap<Vec<u8>, History> {
        let mut out: BTreeMap<Vec<u8>, History> = BTreeMap::new();
        for e in &self.events {
            out.entry(e.op.key().to_vec()).or_default().push(e.clone());
        }
        for h in out.values_mut() {
            *h = h.sorted();
        }
        out
    }

    /// The widest number of operations concurrent at any instant. The checker's
    /// cost is driven by this, not by the history's length.
    pub fn max_concurrency(&self) -> usize {
        let mut points: Vec<(u64, i32)> = Vec::with_capacity(self.events.len() * 2);
        for e in &self.events {
            points.push((e.invoked, 1));
            points.push((e.returned, -1));
        }
        points.sort_unstable();
        let (mut cur, mut best) = (0i32, 0i32);
        for (_, delta) in points {
            cur += delta;
            best = best.max(cur);
        }
        best as usize
    }
}

impl fmt::Display for History {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for e in &self.events {
            writeln!(f, "{e}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(client: u64, op: Op, ret: Ret, invoked: u64, returned: u64) -> Event {
        Event {
            client,
            op,
            ret,
            invoked,
            returned,
        }
    }

    fn w(k: &str, v: &str) -> Op {
        Op::Write {
            key: k.as_bytes().to_vec(),
            value: v.as_bytes().to_vec(),
        }
    }

    fn r(k: &str) -> Op {
        Op::Read {
            key: k.as_bytes().to_vec(),
        }
    }

    #[test]
    fn concurrency_is_window_overlap() {
        let a = ev(1, w("k", "1"), Ret::Ok, 0, 10);
        let b = ev(2, r("k"), Ret::Value(None), 5, 15);
        let c = ev(3, r("k"), Ret::Value(None), 20, 30);
        assert!(a.concurrent_with(&b));
        assert!(b.concurrent_with(&a));
        assert!(
            !a.concurrent_with(&c),
            "disjoint windows are not concurrent"
        );
    }

    #[test]
    fn splitting_by_key_partitions_the_history() {
        let mut h = History::new();
        h.push(ev(1, w("a", "1"), Ret::Ok, 0, 10));
        h.push(ev(2, w("b", "2"), Ret::Ok, 1, 11));
        h.push(ev(1, r("a"), Ret::Value(Some(b"1".to_vec())), 12, 20));
        let split = h.by_key();
        assert_eq!(split.len(), 2);
        assert_eq!(split[&b"a".to_vec()].len(), 2);
        assert_eq!(split[&b"b".to_vec()].len(), 1);
    }

    #[test]
    fn max_concurrency_counts_the_widest_overlap() {
        let mut h = History::new();
        // Three overlapping, then one alone.
        h.push(ev(1, w("k", "1"), Ret::Ok, 0, 100));
        h.push(ev(2, w("k", "2"), Ret::Ok, 10, 110));
        h.push(ev(3, w("k", "3"), Ret::Ok, 20, 120));
        h.push(ev(4, r("k"), Ret::Value(None), 200, 210));
        assert_eq!(h.max_concurrency(), 3);
    }

    #[test]
    fn sorting_is_a_total_order() {
        let mut h = History::new();
        h.push(ev(3, r("k"), Ret::Value(None), 5, 9));
        h.push(ev(1, w("k", "1"), Ret::Ok, 5, 9));
        h.push(ev(2, w("k", "2"), Ret::Ok, 1, 2));
        let s = h.sorted();
        assert_eq!(s.events()[0].client, 2);
        // Identical windows fall back to client id, so the order is stable.
        assert_eq!(s.events()[1].client, 1);
        assert_eq!(s.events()[2].client, 3);
        assert_eq!(
            s.events(),
            h.sorted().events(),
            "sorting must be deterministic"
        );
    }

    #[test]
    fn unknowns_are_counted_not_dropped() {
        let mut h = History::new();
        h.push(ev(1, w("k", "1"), Ret::Ok, 0, 10));
        h.push(ev(2, w("k", "2"), Ret::Unknown, 5, 500));
        assert_eq!(h.unknown_count(), 1);
        assert_eq!(h.len(), 2, "an unknown operation stays in the history");
    }
}
