//! A linearizability checker.
//!
//! # The question
//!
//! A history is linearizable if you can pick, for every operation, a single
//! instant inside its invocation/response window such that the operations —
//! executed one at a time in that order — produce exactly the results the
//! clients observed. That is the definition of "behaves like one machine",
//! and it is the property the whole cluster exists to provide.
//!
//! # Why this is hard
//!
//! Deciding it is NP-complete in general (Gibbons & Korach 1997). The search
//! space is every interleaving of concurrent operations, which is factorial in
//! the width of the concurrency. Three things make it tractable here:
//!
//! 1. **Per-key decomposition.** Operations on different keys commute, so a
//!    history is linearizable exactly when each per-key projection is. This is
//!    done by the caller ([`crate::history::History::by_key`]) and is worth
//!    orders of magnitude — it is the difference between one intractable
//!    problem and fifty easy ones.
//!
//! 2. **Wing & Gong search.** Walk the history left to right maintaining a set
//!    of *pending* operations. At each step either linearize a pending
//!    operation now (if the model accepts it) or advance past it. Backtrack on
//!    failure. Critically, only operations whose windows are still open are
//!    candidates, so the branching factor is the concurrency, not the length.
//!
//! 3. **Memoization.** The reachable configurations are `(model state, set of
//!    linearized operations)`. The same configuration is reached by many
//!    different orders, and revisiting it can only fail again. Caching visited
//!    configurations collapses the factorial into something polynomial in
//!    practice — and it is the single change that takes a checker from
//!    "hangs on 20 concurrent ops" to "returns on 10,000 operations".
//!
//! # Unknown operations
//!
//! An operation whose outcome the client never learned may have taken effect
//! or not. The checker tries both: linearizing it (with its effect applied and
//! its return value unconstrained) and skipping it entirely. Getting this
//! wrong in either direction is the classic way to produce a checker that
//! reports violations that are not there.

use std::collections::BTreeSet;
use std::fmt;

use crate::history::{Event, History, Op, Ret, Value};

/// The sequential specification being checked against: a single register per
/// key, which is what a linearizable KV store must behave like.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Model {
    value: Value,
}

impl Model {
    fn new() -> Model {
        Model { value: None }
    }

    /// Apply an operation. Returns the new state if the observed return value
    /// is consistent with applying it here, `None` if it is not.
    fn step(&self, e: &Event) -> Option<Model> {
        match (&e.op, &e.ret) {
            // --- reads --------------------------------------------------
            (Op::Read { .. }, Ret::Value(observed)) => {
                if observed == &self.value {
                    Some(self.clone())
                } else {
                    None
                }
            }
            // --- writes -------------------------------------------------
            (Op::Write { value, .. }, Ret::Ok) => Some(Model { value: Some(value.clone()) }),
            (Op::Delete { .. }, Ret::Ok) => Some(Model { value: None }),
            // --- compare and swap ---------------------------------------
            (Op::Cas { expect, value, .. }, Ret::Ok) => {
                if expect == &self.value {
                    Some(Model { value: value.clone() })
                } else {
                    None
                }
            }
            (Op::Cas { expect, .. }, Ret::CasFailed) => {
                // A reported failure is itself an observation: it says the
                // value was *not* `expect` at that instant.
                if expect == &self.value {
                    None
                } else {
                    Some(self.clone())
                }
            }
            // --- unknown ------------------------------------------------
            // The client never learned the outcome, so the return value
            // constrains nothing. The effect still has to be legal.
            (Op::Read { .. }, Ret::Unknown) => Some(self.clone()),
            (Op::Write { value, .. }, Ret::Unknown) => Some(Model { value: Some(value.clone()) }),
            (Op::Delete { .. }, Ret::Unknown) => Some(Model { value: None }),
            (Op::Cas { expect, value, .. }, Ret::Unknown) => {
                if expect == &self.value {
                    Some(Model { value: value.clone() })
                } else {
                    // The CAS would have failed; a failure is a legal outcome
                    // and leaves the state alone.
                    Some(self.clone())
                }
            }
            // A read that returned Ok, or a write that returned a value, is
            // not something the system should ever produce.
            _ => None,
        }
    }
}

/// Why a history is not linearizable, in terms a person can act on.
#[derive(Clone, Debug)]
pub struct Violation {
    /// The operation that could not be placed anywhere legal.
    pub culprit: Event,
    /// The prefix the checker did manage to linearize, in order.
    pub linearized: Vec<Event>,
    /// The register's value at the point the culprit was rejected.
    pub model_value: Value,
    /// Everything concurrent with the culprit — the operations whose ordering
    /// relative to it was in play.
    pub concurrent: Vec<Event>,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let show = |v: &Value| match v {
            None => "nil".to_string(),
            Some(b) => String::from_utf8_lossy(b).to_string(),
        };
        writeln!(f, "LINEARIZABILITY VIOLATION on key {:?}", String::from_utf8_lossy(self.culprit.op.key()))?;
        writeln!(f)?;
        writeln!(f, "  no valid ordering places this operation:")?;
        writeln!(f, "    {}", self.culprit)?;
        writeln!(f)?;
        writeln!(f, "  the register held {} at that point", show(&self.model_value))?;
        writeln!(f)?;
        if !self.linearized.is_empty() {
            writeln!(f, "  the longest legal prefix found ({} ops):", self.linearized.len())?;
            for e in self.linearized.iter().rev().take(8).rev() {
                writeln!(f, "    {e}")?;
            }
            writeln!(f)?;
        }
        if !self.concurrent.is_empty() {
            writeln!(f, "  concurrent with the culprit:")?;
            for e in self.concurrent.iter().take(8) {
                writeln!(f, "    {e}")?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub enum Verdict {
    Linearizable,
    NotLinearizable(Box<Violation>),
    /// The search exceeded its budget. Not a pass and not a fail — say so
    /// rather than guessing, because reporting either would be a lie.
    Inconclusive { explored: u64, max_concurrency: usize },
}

impl Verdict {
    pub fn is_linearizable(&self) -> bool {
        matches!(self, Verdict::Linearizable)
    }

    pub fn is_violation(&self) -> bool {
        matches!(self, Verdict::NotLinearizable(_))
    }
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Verdict::Linearizable => write!(f, "linearizable"),
            Verdict::NotLinearizable(v) => write!(f, "{v}"),
            Verdict::Inconclusive { explored, max_concurrency } => write!(
                f,
                "INCONCLUSIVE: exhausted the search budget after {explored} configurations \
                 (max concurrency {max_concurrency}). Neither a pass nor a fail."
            ),
        }
    }
}

/// Configuration limits. The defaults comfortably handle the histories the
/// swarm produces; raise them when triaging a specific seed.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Maximum configurations to explore per key before giving up.
    pub max_states: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self { max_states: 2_000_000 }
    }
}

/// Check a whole history, one key at a time.
pub fn check(history: &History, limits: Limits) -> Verdict {
    for (_key, sub) in history.by_key() {
        match check_one_key(&sub, limits) {
            Verdict::Linearizable => continue,
            other => return other,
        }
    }
    Verdict::Linearizable
}

/// Wing & Gong over a single key's operations.
///
/// # State encoding
///
/// The naive state is "the set of operations linearized so far", which needs a
/// bitset as wide as the history and cannot be memoized cheaply. But in this
/// search that set is always *almost* a prefix: every operation before the
/// frontier is linearized, and the only ones that can be linearized ahead of it
/// are those concurrent with it. So the state compresses to
/// `(frontier, mask of the few ahead of it)`, which is small, hashable, and
/// independent of how long the history is.
///
/// That encoding is what lets this check a 10,000-operation history directly
/// instead of in windows. Windowing was the obvious alternative and it is
/// quietly wrong: a window starting mid-history would begin with a fresh `nil`
/// register, so the first read of any window that returns a previously-written
/// value gets reported as a violation. A checker that invents violations is
/// worse than no checker, because every seed it files wastes an investigation.
fn check_one_key(history: &History, limits: Limits) -> Verdict {
    let events: Vec<Event> = history.sorted().events().to_vec();
    let n = events.len();
    if n == 0 {
        return Verdict::Linearizable;
    }

    /// How far ahead of the frontier an operation may be linearized. Bounded by
    /// the concurrency, not the history length.
    const AHEAD: usize = 63;

    #[derive(Clone)]
    struct Frame {
        model: Model,
        frontier: usize,
        ahead: u64,
        /// Cursor over candidate moves: `offset = cursor >> 1` from the
        /// frontier, `skip_effect = cursor & 1`.
        ///
        /// The second bit exists entirely for `Unknown` operations. A client
        /// that timed out does not know whether its write took effect, so the
        /// checker has to try both — applying it and discarding it. Without the
        /// discard branch, a history where the write genuinely never happened is
        /// reported as a violation.
        cursor: usize,
        chose: Option<usize>,
    }

    let mut visited: BTreeSet<(Value, usize, u64)> = BTreeSet::new();
    let mut explored: u64 = 0;
    // Set if the `AHEAD` window ever cut off a candidate that was still legally
    // orderable before the frontier operation. When that happens the search is
    // no longer exhaustive, so "no ordering exists" becomes "no ordering exists
    // *within the window I looked at*" — which is not a violation and must not
    // be reported as one.
    //
    // This is not hypothetical. A client operation that retries through a
    // partition can stay open for seconds, overlapping hundreds of other
    // operations on the same key; placing it last would need all of them ahead
    // of it. Reporting that as a violation would send someone hunting a
    // consensus bug that does not exist.
    let mut bounded_out = false;
    let mut best_depth = 0usize;
    let mut best_trace: Vec<usize> = Vec::new();
    let mut best_model = Model::new();

    // Explicit stack rather than recursion: a long history with wide
    // concurrency will blow a default stack, and a checker that crashes is a
    // checker nobody trusts.
    let mut stack: Vec<Frame> =
        vec![Frame { model: Model::new(), frontier: 0, ahead: 0, cursor: 0, chose: None }];

    while let Some(frame) = stack.last_mut() {
        explored += 1;
        if explored > limits.max_states {
            return Verdict::Inconclusive { explored, max_concurrency: history.max_concurrency() };
        }
        if frame.frontier == n {
            return Verdict::Linearizable;
        }

        // Nothing invoked after the frontier operation must return can be
        // linearized before it. Events are sorted by invocation, so this is a
        // clean cut rather than a filter.
        let horizon = events[frame.frontier].returned;

        let mut advanced = false;
        while frame.cursor < 2 * (AHEAD + 2) {
            let offset = frame.cursor >> 1;
            let skip_effect = frame.cursor & 1 == 1;
            frame.cursor += 1;

            let i = frame.frontier + offset;
            if i >= n {
                break;
            }
            if offset > AHEAD {
                if events[i].invoked <= horizon {
                    bounded_out = true;
                }
                break;
            }
            // Already linearized ahead of the frontier?
            if offset > 0 && frame.ahead & (1 << (offset - 1)) != 0 {
                continue;
            }
            if events[i].invoked > horizon {
                break;
            }
            if skip_effect && !events[i].is_unknown() {
                continue;
            }

            let next_model = if skip_effect {
                // The operation never took effect. Still a history the client
                // could have observed.
                frame.model.clone()
            } else {
                let Some(m) = frame.model.step(&events[i]) else { continue };
                m
            };

            // Advance the frontier over any run of already-linearized ops.
            //
            // The bookkeeping: bit `k` of `ahead` means "index `frontier+1+k`
            // is already linearized". Linearizing the frontier itself consumes
            // index `frontier`, plus the run of `t` trailing set bits above it —
            // so the frontier moves by `1 + t` and the mask must shift by
            // `t + 1`, not by `t`.
            //
            // Shifting one position short leaves a stale bit at position 0,
            // which then claims the operation *after* the new frontier is
            // already placed. The search silently explores a corrupted state
            // space and reports histories as non-linearizable that are
            // perfectly fine — a false positive, and the worst possible failure
            // for an oracle whose whole job is deciding what counts as a bug.
            let (mut frontier, mut ahead) = (frame.frontier, frame.ahead);
            if offset == 0 {
                let t = ahead.trailing_ones() as usize;
                frontier += 1 + t;
                ahead = if t + 1 >= 64 { 0 } else { ahead >> (t + 1) };
            } else {
                ahead |= 1 << (offset - 1);
            }

            if !visited.insert((next_model.value.clone(), frontier, ahead)) {
                // Reached by another order; it failed then and will fail now.
                continue;
            }
            // Track the furthest *frontier*, not the largest set of linearized
            // operations. The frontier is the first operation with nothing
            // before it left to place, so the operation sitting at the deepest
            // frontier reached is the one actually blocking the linearization.
            // Ranking by set size instead points at whichever operation happens
            // to be unplaced in the widest state — frequently a write, which by
            // construction can never be the blocker.
            if frontier > best_depth {
                best_depth = frontier;
                best_trace = stack.iter().filter_map(|f| f.chose).collect();
                best_trace.push(i);
                best_model = next_model.clone();
            }
            stack.push(Frame {
                model: next_model,
                frontier,
                ahead,
                cursor: 0,
                chose: Some(i),
            });
            advanced = true;
            break;
        }

        if !advanced {
            stack.pop();
        }
    }

    if bounded_out {
        return Verdict::Inconclusive { explored, max_concurrency: history.max_concurrency() };
    }

    // Every ordering was tried and none linearized the whole history.
    let linearized: Vec<Event> = best_trace.iter().map(|&i| events[i].clone()).collect();
    // The operation at the deepest frontier: everything before it was placed,
    // and no ordering gets past it.
    let culprit = events[best_depth.min(n - 1)].clone();
    let concurrent: Vec<Event> =
        events.iter().filter(|e| e.concurrent_with(&culprit) && **e != culprit).cloned().collect();

    Verdict::NotLinearizable(Box::new(Violation {
        culprit,
        linearized,
        model_value: best_model.value,
        concurrent,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(client: u64, op: Op, ret: Ret, invoked: u64, returned: u64) -> Event {
        Event { client, op, ret, invoked, returned }
    }

    fn w(v: &str) -> Op {
        Op::Write { key: b"k".to_vec(), value: v.as_bytes().to_vec() }
    }

    fn r() -> Op {
        Op::Read { key: b"k".to_vec() }
    }

    fn val(v: &str) -> Ret {
        Ret::Value(Some(v.as_bytes().to_vec()))
    }

    fn hist(events: Vec<Event>) -> History {
        let mut h = History::new();
        for e in events {
            h.push(e);
        }
        h
    }

    #[test]
    fn an_empty_history_is_linearizable() {
        assert!(check(&History::new(), Limits::default()).is_linearizable());
    }

    #[test]
    fn a_sequential_history_is_linearizable() {
        let h = hist(vec![
            ev(1, w("a"), Ret::Ok, 0, 10),
            ev(1, r(), val("a"), 20, 30),
            ev(1, w("b"), Ret::Ok, 40, 50),
            ev(1, r(), val("b"), 60, 70),
        ]);
        assert!(check(&h, Limits::default()).is_linearizable());
    }

    #[test]
    fn reading_a_value_that_was_never_written_is_a_violation() {
        let h = hist(vec![
            ev(1, w("a"), Ret::Ok, 0, 10),
            ev(1, r(), val("ghost"), 20, 30),
        ]);
        let v = check(&h, Limits::default());
        assert!(v.is_violation(), "expected a violation, got {v}");
    }

    #[test]
    fn a_stale_read_after_a_completed_write_is_a_violation() {
        // The canonical stale-read bug: the write returned before the read was
        // invoked, so no ordering can put the read first.
        let h = hist(vec![
            ev(1, w("new"), Ret::Ok, 0, 10),
            ev(2, r(), Ret::Value(None), 20, 30),
        ]);
        let v = check(&h, Limits::default());
        assert!(v.is_violation(), "expected a violation, got {v}");
        if let Verdict::NotLinearizable(viol) = &v {
            assert!(viol.to_string().contains("LINEARIZABILITY VIOLATION"));
        }
    }

    #[test]
    fn concurrent_operations_may_be_ordered_either_way() {
        // Two overlapping writes and a read that sees the second: legal,
        // because the read can be placed after whichever write it observed.
        let h = hist(vec![
            ev(1, w("a"), Ret::Ok, 0, 100),
            ev(2, w("b"), Ret::Ok, 10, 110),
            ev(3, r(), val("a"), 20, 120),
        ]);
        assert!(check(&h, Limits::default()).is_linearizable());
    }

    #[test]
    fn a_read_cannot_go_back_in_time_between_two_reads() {
        // c2 reads "b", then later c3 reads "a" — but the write of "b" had
        // already been observed. No single order explains it.
        let h = hist(vec![
            ev(1, w("a"), Ret::Ok, 0, 10),
            ev(1, w("b"), Ret::Ok, 20, 30),
            ev(2, r(), val("b"), 40, 50),
            ev(3, r(), val("a"), 60, 70),
        ]);
        assert!(check(&h, Limits::default()).is_violation());
    }

    #[test]
    fn an_unknown_write_may_be_treated_as_having_happened() {
        // The client timed out, but the write did commit and a later read sees
        // it. Any checker that treats Unknown as "did not happen" reports a
        // phantom violation here.
        let h = hist(vec![
            ev(1, w("a"), Ret::Unknown, 0, 500),
            ev(2, r(), val("a"), 600, 700),
        ]);
        let v = check(&h, Limits::default());
        assert!(v.is_linearizable(), "an unknown write must be placeable: {v}");
    }

    #[test]
    fn an_unknown_write_may_also_be_treated_as_not_having_happened() {
        // The mirror image: the write never took effect and the read sees the
        // old value. Also legal.
        let h = hist(vec![
            ev(1, w("a"), Ret::Ok, 0, 10),
            ev(2, w("b"), Ret::Unknown, 20, 500),
            ev(3, r(), val("a"), 600, 700),
        ]);
        let v = check(&h, Limits::default());
        assert!(v.is_linearizable(), "an unknown write must be skippable: {v}");
    }

    #[test]
    fn an_unknown_does_not_excuse_an_impossible_read() {
        // Unknown gives freedom, not immunity: "ghost" was never written by
        // anyone, so no placement of the unknown makes the read legal.
        let h = hist(vec![
            ev(1, w("a"), Ret::Unknown, 0, 500),
            ev(2, r(), val("ghost"), 600, 700),
        ]);
        assert!(check(&h, Limits::default()).is_violation());
    }

    #[test]
    fn cas_semantics_are_enforced() {
        let cas = |expect: Option<&str>, value: Option<&str>| Op::Cas {
            key: b"k".to_vec(),
            expect: expect.map(|s| s.as_bytes().to_vec()),
            value: value.map(|s| s.as_bytes().to_vec()),
        };
        // A successful CAS from nil to "a", then a read of "a".
        let ok = hist(vec![
            ev(1, cas(None, Some("a")), Ret::Ok, 0, 10),
            ev(1, r(), val("a"), 20, 30),
        ]);
        assert!(check(&ok, Limits::default()).is_linearizable());

        // A CAS that reports success but whose precondition could not hold.
        let bad = hist(vec![
            ev(1, w("x"), Ret::Ok, 0, 10),
            ev(1, cas(None, Some("a")), Ret::Ok, 20, 30),
        ]);
        assert!(check(&bad, Limits::default()).is_violation());
    }

    #[test]
    fn a_reported_cas_failure_is_itself_an_observation() {
        // The CAS said the value was not nil. But nothing had been written, so
        // it was nil. That is a violation even though nothing "changed".
        let h = hist(vec![ev(
            1,
            Op::Cas { key: b"k".to_vec(), expect: None, value: Some(b"a".to_vec()) },
            Ret::CasFailed,
            0,
            10,
        )]);
        assert!(check(&h, Limits::default()).is_violation());
    }

    #[test]
    fn keys_are_checked_independently() {
        // A violation on one key must be found even when another key is busy.
        let mut h = History::new();
        for i in 0..20u64 {
            h.push(ev(
                1,
                Op::Write { key: b"quiet".to_vec(), value: vec![i as u8] },
                Ret::Ok,
                i * 10,
                i * 10 + 5,
            ));
        }
        h.push(ev(2, w("a"), Ret::Ok, 0, 10));
        h.push(ev(2, r(), Ret::Value(None), 20, 30));
        assert!(check(&h, Limits::default()).is_violation());
    }

    #[test]
    fn a_wide_concurrent_history_still_terminates() {
        // Without memoization this is a factorial search and would hang. With
        // it, it returns immediately. This test is the reason the visited set
        // exists.
        let mut h = History::new();
        for i in 0..24u64 {
            h.push(ev(i, w(&format!("v{i}")), Ret::Ok, 0, 10_000));
        }
        h.push(ev(99, r(), val("v7"), 1, 9_999));
        let started = std::time::Instant::now();
        let v = check(&h, Limits::default());
        assert!(v.is_linearizable(), "{v}");
        assert!(
            started.elapsed().as_secs() < 5,
            "24 fully concurrent writes took {:?}; memoization is not working",
            started.elapsed()
        );
    }

    #[test]
    fn a_long_sequential_history_is_checked_by_chunking() {
        let mut h = History::new();
        for i in 0..500u64 {
            h.push(ev(1, w(&format!("v{i}")), Ret::Ok, i * 10, i * 10 + 5));
            h.push(ev(1, r(), val(&format!("v{i}")), i * 10 + 6, i * 10 + 9));
        }
        assert!(check(&h, Limits::default()).is_linearizable());
    }

    #[test]
    fn a_violation_deep_in_a_long_history_is_still_found() {
        let mut h = History::new();
        for i in 0..300u64 {
            h.push(ev(1, w(&format!("v{i}")), Ret::Ok, i * 10, i * 10 + 5));
            h.push(ev(1, r(), val(&format!("v{i}")), i * 10 + 6, i * 10 + 9));
        }
        // A stale read 200 operations in.
        h.push(ev(2, r(), val("v0"), 2005, 2008));
        assert!(check(&h, Limits::default()).is_violation());
    }

    #[test]
    fn an_operation_linearized_ahead_of_the_frontier_keeps_its_place() {
        // Regression. Reduced from a real `nemesis` run the checker wrongly
        // called non-linearizable.
        //
        // The shape: a read that must be ordered *before* a concurrent write,
        // even though it was invoked after it. That forces the search to
        // linearize an operation ahead of the frontier, and then to advance the
        // frontier past it — the exact path where the `ahead` mask was shifted
        // one position too few, leaving a stale bit that claimed the wrong
        // operation was already placed.
        let h = hist(vec![
            ev(1, w("old"), Ret::Ok, 0, 100),
            // Concurrent write, and two reads inside its window that disagree.
            ev(2, w("new"), Ret::Ok, 200, 900),
            ev(3, r(), val("new"), 250, 950),
            ev(1, r(), val("old"), 300, 400),
        ]);
        let v = check(&h, Limits::default());
        assert!(
            v.is_linearizable(),
            "read(old) can be placed before write(new); both are concurrent with it:\n{v}"
        );
    }

    #[test]
    fn a_deep_history_with_interleaved_reordering_stays_linearizable() {
        // The same shape repeated, to exercise frontier advancement over runs
        // of already-placed operations rather than single ones.
        let mut h = History::new();
        let mut t = 0u64;
        for i in 0..200u64 {
            h.push(ev(1, w(&format!("v{i}")), Ret::Ok, t, t + 100));
            h.push(ev(2, w(&format!("w{i}")), Ret::Ok, t + 20, t + 400));
            h.push(ev(3, r(), val(&format!("w{i}")), t + 50, t + 450));
            h.push(ev(4, r(), val(&format!("v{i}")), t + 60, t + 90));
            t += 500;
        }
        let v = check(&h, Limits::default());
        assert!(v.is_linearizable(), "{v}");
    }

    #[test]
    fn checking_is_deterministic() {
        let h = hist(vec![
            ev(1, w("a"), Ret::Ok, 0, 100),
            ev(2, w("b"), Ret::Ok, 10, 110),
            ev(3, r(), val("a"), 20, 120),
            ev(4, r(), Ret::Value(None), 200, 210),
        ]);
        let first = format!("{}", check(&h, Limits::default()));
        for _ in 0..5 {
            assert_eq!(format!("{}", check(&h, Limits::default())), first);
        }
    }
}
