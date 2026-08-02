//! Raft's safety properties, checked against every node at once.
//!
//! The linearizability checker asks whether the system looked correct *from
//! outside*. These oracles ask whether it is correct *inside*, and they are
//! complementary: an internal invariant can break long before a client
//! observes anything wrong, and catching it at that moment gives a trace that
//! points at the cause rather than at a symptom thousands of events later.
//!
//! An omniscient observer like this is impossible in production — no process
//! can read every node's log atomically. It is trivial in a simulator, where
//! there is one thread and one clock. That asymmetry is much of why
//! deterministic simulation finds things that integration tests do not.

use std::collections::BTreeMap;
use std::fmt;

use chrono_sim::traits::NodeId;
use chronolog::node::PublicState;
use chronolog::types::{Index, Term};

/// A safety property that did not hold.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violated {
    pub property: &'static str,
    pub detail: String,
}

impl fmt::Display for Violated {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} VIOLATED: {}", self.property, self.detail)
    }
}

/// Everything an oracle needs to know about the cluster at one instant.
#[derive(Clone, Debug, Default)]
pub struct ClusterView {
    pub nodes: BTreeMap<NodeId, PublicState>,
}

impl ClusterView {
    pub fn leaders(&self) -> Vec<(NodeId, Term)> {
        self.nodes
            .iter()
            .filter(|(_, s)| s.role == "leader")
            .map(|(id, s)| (*id, s.term))
            .collect()
    }
}

/// Accumulates history across the run, because some properties are about the
/// *sequence* of states rather than any single one.
#[derive(Debug, Default)]
pub struct Invariants {
    /// term -> the leader seen in it. Election Safety.
    leaders_by_term: BTreeMap<Term, NodeId>,
    /// `(node, generation)` -> highest commit index reported in that process
    /// lifetime. Keyed by generation because a restart may legitimately lower
    /// it — see `PublicState::generation`.
    high_commit: BTreeMap<(NodeId, u64), Index>,
    /// `(node, generation)` -> highest applied index.
    high_applied: BTreeMap<(NodeId, u64), Index>,
    /// (index, term) pairs that some node has committed, so a later leader
    /// overwriting one is detectable. Leader Completeness.
    committed_terms: BTreeMap<Index, Term>,
    violations: Vec<Violated>,
    checks: u64,
}

impl Invariants {
    pub fn new() -> Invariants {
        Invariants::default()
    }

    pub fn violations(&self) -> &[Violated] {
        &self.violations
    }

    pub fn checks(&self) -> u64 {
        self.checks
    }

    pub fn ok(&self) -> bool {
        self.violations.is_empty()
    }

    fn fail(&mut self, property: &'static str, detail: String) {
        // Record each property once. A broken invariant usually stays broken,
        // and a thousand copies of the same line buries the first occurrence —
        // which is the only one whose trace context is useful.
        if !self.violations.iter().any(|v| v.property == property) {
            self.violations.push(Violated { property, detail });
        }
    }

    /// A node whose driver loop has terminated.
    ///
    /// Worth a named property because of how it presents: the node stays up,
    /// keeps accepting messages, and keeps reporting its last known state
    /// forever. Nothing else here fires — its log matches, it overwrites
    /// nothing, it simply stops. Without this check the symptom is a follower
    /// that mysteriously stops replicating, thousands of events after the
    /// cause.
    fn driver_alive(&mut self, view: &ClusterView) {
        for (id, s) in &view.nodes {
            if let Some(e) = &s.driver_error {
                self.fail("DRIVER STOPPED", format!("n{id}: {e}"));
            }
        }
    }

    /// Check every property against the current cluster state.
    pub fn check(&mut self, view: &ClusterView) {
        self.checks += 1;
        self.driver_alive(view);
        self.election_safety(view);
        self.monotonicity(view);
        self.log_matching(view);
        self.state_machine_safety(view);
        self.leader_completeness(view);
        self.commit_is_backed_by_a_log(view);
    }

    /// **Election Safety** (§5.2): at most one leader per term.
    ///
    /// The foundation. Two leaders in one term can accept conflicting writes at
    /// the same index, and every other property collapses.
    fn election_safety(&mut self, view: &ClusterView) {
        for (id, term) in view.leaders() {
            match self.leaders_by_term.get(&term) {
                Some(&prev) if prev != id => {
                    self.fail(
                        "ELECTION SAFETY",
                        format!("term {term} has two leaders: n{prev} and n{id}"),
                    );
                }
                _ => {
                    self.leaders_by_term.insert(term, id);
                }
            }
        }
    }

    /// Commit and applied indices never move backwards.
    ///
    /// Not in the paper as a named property, but it is the cheapest possible
    /// detector for a whole family of bugs: a node that forgets what it
    /// committed, a restart that trusts a stale hard state, a snapshot install
    /// that regresses the applied point.
    fn monotonicity(&mut self, view: &ClusterView) {
        for (id, s) in &view.nodes {
            let key = (*id, s.generation);
            let prev_c = self.high_commit.get(&key).copied().unwrap_or(0);
            if s.commit_index < prev_c {
                self.fail(
                    "COMMIT MONOTONICITY",
                    format!(
                        "n{id} commit index went {prev_c} -> {} without restarting",
                        s.commit_index
                    ),
                );
            }
            self.high_commit.insert(key, prev_c.max(s.commit_index));

            let prev_a = self.high_applied.get(&key).copied().unwrap_or(0);
            if s.applied_index < prev_a {
                self.fail(
                    "APPLY MONOTONICITY",
                    format!(
                        "n{id} applied index went {prev_a} -> {} without restarting",
                        s.applied_index
                    ),
                );
            }
            self.high_applied.insert(key, prev_a.max(s.applied_index));
        }
    }

    /// **Log Matching** (§5.3): if two logs hold an entry with the same index
    /// and term, they are identical in every preceding entry.
    fn log_matching(&mut self, view: &ClusterView) {
        let nodes: Vec<(&NodeId, &PublicState)> =
            view.nodes.iter().filter(|(_, s)| !s.log_terms.is_empty()).collect();
        for i in 0..nodes.len() {
            for j in (i + 1)..nodes.len() {
                let (a_id, a) = nodes[i];
                let (b_id, b) = nodes[j];
                let a_map: BTreeMap<Index, Term> = a.log_terms.iter().copied().collect();
                // Walk down from the highest shared index looking for the first
                // agreement; from there down, everything must match.
                let mut agree_at = None;
                for (idx, term) in b.log_terms.iter().rev() {
                    if a_map.get(idx) == Some(term) {
                        agree_at = Some(*idx);
                        break;
                    }
                }
                let Some(m) = agree_at else { continue };
                for (idx, b_term) in &b.log_terms {
                    if *idx > m {
                        continue;
                    }
                    if let Some(a_term) = a_map.get(idx) {
                        if a_term != b_term {
                            self.fail(
                                "LOG MATCHING",
                                format!(
                                    "n{a_id} and n{b_id} agree at index {m} but differ at \
                                     index {idx}: terms {a_term} vs {b_term}"
                                ),
                            );
                            return;
                        }
                    }
                }
            }
        }
    }

    /// **State Machine Safety** (§5.4.3): no two nodes apply different entries
    /// at the same index.
    ///
    /// Compared by per-entry digest rather than by whole applied histories, so
    /// this stays cheap enough to run continuously — and, crucially, stays
    /// comparable between a node that replayed the log and one that was caught
    /// up by snapshot. See `chronolog::node::ApplyDigest` for why a cumulative
    /// chain would produce a false positive on every snapshot install.
    fn state_machine_safety(&mut self, view: &ClusterView) {
        let nodes: Vec<(&NodeId, &PublicState)> =
            view.nodes.iter().filter(|(_, s)| !s.apply_checkpoints.is_empty()).collect();
        for i in 0..nodes.len() {
            for j in (i + 1)..nodes.len() {
                let (a_id, a) = nodes[i];
                let (b_id, b) = nodes[j];
                let a_map: BTreeMap<Index, u64> = a.apply_checkpoints.iter().copied().collect();
                for (idx, b_hash) in &b.apply_checkpoints {
                    if let Some(a_hash) = a_map.get(idx) {
                        if a_hash != b_hash {
                            self.fail(
                                "STATE MACHINE SAFETY",
                                format!(
                                    "n{a_id} and n{b_id} applied different histories through \
                                     index {idx} (digests {a_hash:#018x} vs {b_hash:#018x})"
                                ),
                            );
                            return;
                        }
                    }
                }
            }
        }
    }

    /// **Leader Completeness** (§5.4): an entry committed in one term is
    /// present, with the same term, in the log of every future leader.
    ///
    /// Checked by remembering the term of every committed index and objecting
    /// if a node ever holds a *different* term at that index. Committed means
    /// decided; a decided index whose term changed is the loudest possible
    /// signal that consensus broke.
    fn leader_completeness(&mut self, view: &ClusterView) {
        // Record what is committed, per node, up to its commit index.
        for s in view.nodes.values() {
            for (idx, term) in &s.log_terms {
                if *idx > s.commit_index {
                    break;
                }
                match self.committed_terms.get(idx) {
                    Some(&known) if known != *term => {
                        self.fail(
                            "LEADER COMPLETENESS",
                            format!(
                                "index {idx} was committed in term {known} but n{} now holds \
                                 term {term} there — a committed entry was overwritten",
                                s.node
                            ),
                        );
                        return;
                    }
                    _ => {
                        self.committed_terms.insert(*idx, *term);
                    }
                }
            }
        }
    }

    /// A node must never claim to have committed past the end of its own log.
    ///
    /// The specific failure this catches: a restart that trusts a persisted
    /// commit index after a torn write truncated the log tail. The node would
    /// then report entries as committed that it does not have, and could serve
    /// them as a leader.
    fn commit_is_backed_by_a_log(&mut self, view: &ClusterView) {
        for (id, s) in &view.nodes {
            if s.commit_index > s.last_index {
                self.fail(
                    "COMMIT BEYOND LOG",
                    format!(
                        "n{id} reports commit index {} but its log ends at {}",
                        s.commit_index, s.last_index
                    ),
                );
            }
            if s.applied_index > s.commit_index {
                self.fail(
                    "APPLIED BEYOND COMMIT",
                    format!(
                        "n{id} applied {} but has only committed {}",
                        s.applied_index, s.commit_index
                    ),
                );
            }
        }
    }
}

impl fmt::Display for Invariants {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.violations.is_empty() {
            write!(f, "all Raft safety invariants held across {} checks", self.checks)
        } else {
            for v in &self.violations {
                writeln!(f, "{v}")?;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: NodeId, role: &'static str, term: Term) -> PublicState {
        PublicState { node: id, role, term, ..Default::default() }
    }

    fn view(states: Vec<PublicState>) -> ClusterView {
        ClusterView { nodes: states.into_iter().map(|s| (s.node, s)).collect() }
    }

    #[test]
    fn a_healthy_cluster_passes() {
        let mut inv = Invariants::new();
        inv.check(&view(vec![
            node(0, "leader", 3),
            node(1, "follower", 3),
            node(2, "follower", 3),
        ]));
        assert!(inv.ok(), "{inv}");
    }

    #[test]
    fn two_leaders_in_one_term_is_caught() {
        let mut inv = Invariants::new();
        inv.check(&view(vec![node(0, "leader", 5), node(1, "leader", 5)]));
        assert!(!inv.ok());
        assert_eq!(inv.violations()[0].property, "ELECTION SAFETY");
    }

    #[test]
    fn two_leaders_in_different_terms_is_fine() {
        // A deposed leader that has not yet noticed is normal, not a violation.
        let mut inv = Invariants::new();
        inv.check(&view(vec![node(0, "leader", 4), node(1, "leader", 5)]));
        assert!(inv.ok(), "{inv}");
    }

    #[test]
    fn a_commit_index_going_backwards_is_caught() {
        let mut inv = Invariants::new();
        let mut a = node(0, "follower", 1);
        a.commit_index = 10;
        a.last_index = 10;
        inv.check(&view(vec![a.clone()]));
        a.commit_index = 4;
        inv.check(&view(vec![a]));
        assert!(!inv.ok());
        assert_eq!(inv.violations()[0].property, "COMMIT MONOTONICITY");
    }

    #[test]
    fn committing_past_the_end_of_the_log_is_caught() {
        let mut inv = Invariants::new();
        let mut a = node(0, "follower", 1);
        a.commit_index = 50;
        a.last_index = 12;
        inv.check(&view(vec![a]));
        assert!(!inv.ok());
        assert_eq!(inv.violations()[0].property, "COMMIT BEYOND LOG");
    }

    #[test]
    fn divergent_logs_that_agree_somewhere_are_caught() {
        let mut inv = Invariants::new();
        let mut a = node(0, "follower", 4);
        let mut b = node(1, "follower", 4);
        // Agree at index 4, differ at index 2 — impossible under Log Matching.
        a.log_terms = vec![(1, 1), (2, 1), (3, 2), (4, 3)];
        b.log_terms = vec![(1, 1), (2, 2), (3, 2), (4, 3)];
        inv.check(&view(vec![a, b]));
        assert!(!inv.ok());
        assert_eq!(inv.violations()[0].property, "LOG MATCHING");
    }

    #[test]
    fn logs_that_merely_diverge_at_the_tail_are_fine() {
        // A follower with uncommitted entries the leader does not have is
        // completely normal — it will be truncated.
        let mut inv = Invariants::new();
        let mut a = node(0, "leader", 4);
        let mut b = node(1, "follower", 4);
        a.log_terms = vec![(1, 1), (2, 1), (3, 4)];
        b.log_terms = vec![(1, 1), (2, 1), (3, 2), (4, 2)];
        inv.check(&view(vec![a, b]));
        assert!(inv.ok(), "{inv}");
    }

    #[test]
    fn divergent_applied_histories_are_caught() {
        let mut inv = Invariants::new();
        let mut a = node(0, "follower", 1);
        let mut b = node(1, "follower", 1);
        a.apply_checkpoints = vec![(1, 0xAAAA), (2, 0xBBBB)];
        b.apply_checkpoints = vec![(1, 0xAAAA), (2, 0xCCCC)];
        inv.check(&view(vec![a, b]));
        assert!(!inv.ok());
        assert_eq!(inv.violations()[0].property, "STATE MACHINE SAFETY");
    }

    #[test]
    fn a_node_caught_up_by_snapshot_does_not_trip_state_machine_safety() {
        // Digests are per-entry, so the node that skipped indices 1..50 via a
        // snapshot still agrees with its peer on every index it did apply.
        // With a cumulative chain this case was a guaranteed false positive.
        let mut inv = Invariants::new();
        let mut a = node(0, "leader", 1);
        let mut b = node(1, "follower", 1);
        a.apply_checkpoints = vec![(1, 0x11), (2, 0x22), (50, 0x33), (51, 0x99)];
        b.apply_checkpoints = vec![(51, 0x99)];
        inv.check(&view(vec![a, b]));
        assert!(inv.ok(), "{inv}");
    }

    #[test]
    fn overwriting_a_committed_entry_is_caught() {
        let mut inv = Invariants::new();
        let mut a = node(0, "leader", 2);
        a.log_terms = vec![(1, 1), (2, 1), (3, 2)];
        a.commit_index = 3;
        a.last_index = 3;
        inv.check(&view(vec![a]));

        // Later, another node holds a different term at a committed index.
        let mut b = node(1, "leader", 5);
        b.log_terms = vec![(1, 1), (2, 1), (3, 5)];
        b.commit_index = 3;
        b.last_index = 3;
        inv.check(&view(vec![b]));
        assert!(!inv.ok());
        assert_eq!(inv.violations()[0].property, "LEADER COMPLETENESS");
    }

    #[test]
    fn each_property_is_only_reported_once() {
        let mut inv = Invariants::new();
        for _ in 0..100 {
            inv.check(&view(vec![node(0, "leader", 5), node(1, "leader", 5)]));
        }
        assert_eq!(inv.violations().len(), 1, "repeated failures must not bury the first");
    }
}
