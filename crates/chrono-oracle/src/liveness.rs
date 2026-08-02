//! The liveness watchdog.
//!
//! Safety oracles catch the system doing something wrong. This one catches it
//! doing *nothing*, which is a different and surprisingly common failure: a
//! cluster that has deadlocked, or livelocked in an election that can never
//! resolve, violates no safety property at all. Every log matches, no entry is
//! overwritten, and the service is completely down.
//!
//! # Why this needs virtual time
//!
//! In a real integration test, "the cluster failed to elect a leader in 10
//! seconds" is a flaky assertion — CI was slow, the VM was descheduled. Here,
//! ten virtual seconds is exactly ten virtual seconds, and the only thing that
//! can consume it is the system's own behaviour. The assertion becomes precise.
//!
//! The watchdog only starts its clock once the environment is healthy: every
//! partition healed, a quorum alive. Demanding progress from a cluster that is
//! still partitioned would be demanding a violation of CAP.

use std::fmt;

use chrono_sim::time::Nanos;
use chrono_sim::traits::NodeId;

use crate::invariants::ClusterView;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Stall {
    /// No leader for too long after the environment became healthy.
    NoLeader { healthy_for: Nanos, alive: usize, quorum: usize },
    /// A leader exists, but the commit index has not moved despite offered work.
    NoCommitProgress { leader: NodeId, stuck_at: u64, healthy_for: Nanos },
    /// A leader exists and commits, but replicas are not converging.
    NoConvergence { leader: NodeId, laggard: NodeId, gap: u64, healthy_for: Nanos },
}

impl fmt::Display for Stall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Stall::NoLeader { healthy_for, alive, quorum } => write!(
                f,
                "LIVENESS: no leader for {healthy_for} of healthy time with {alive} nodes alive \
                 (quorum is {quorum}) — the cluster is down without violating safety"
            ),
            Stall::NoCommitProgress { leader, stuck_at, healthy_for } => write!(
                f,
                "LIVENESS: n{leader} has led for {healthy_for} with commit index stuck at \
                 {stuck_at} despite pending work"
            ),
            Stall::NoConvergence { leader, laggard, gap, healthy_for } => write!(
                f,
                "LIVENESS: n{laggard} is {gap} entries behind leader n{leader} after \
                 {healthy_for} of healthy time — replication is not converging"
            ),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Budget {
    /// How long a healthy cluster may go without a leader.
    ///
    /// Generous by design. An election takes an election timeout plus a round
    /// trip, and split votes can legitimately cost several rounds. The
    /// watchdog is hunting for "never", not for "slow".
    pub elect_within: Nanos,
    /// How long a leader may sit at the same commit index while work is offered.
    pub commit_within: Nanos,
    /// How long a follower may lag before replication is considered stuck.
    pub converge_within: Nanos,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            elect_within: Nanos::from_secs(30),
            commit_within: Nanos::from_secs(30),
            converge_within: Nanos::from_secs(60),
        }
    }
}

/// Tracks how long the cluster has been healthy and whether it is progressing.
#[derive(Debug)]
pub struct Watchdog {
    budget: Budget,
    /// When the environment last became healthy. `None` while unhealthy.
    healthy_since: Option<Nanos>,
    /// Last observed maximum commit index, and when it last changed.
    best_commit: u64,
    commit_moved_at: Nanos,
    /// When the last leader was seen.
    saw_leader_at: Nanos,
    converged_at: Nanos,
    stalls: Vec<Stall>,
}

impl Watchdog {
    pub fn new(budget: Budget) -> Watchdog {
        Watchdog {
            budget,
            healthy_since: None,
            best_commit: 0,
            commit_moved_at: Nanos::ZERO,
            saw_leader_at: Nanos::ZERO,
            converged_at: Nanos::ZERO,
            stalls: Vec::new(),
        }
    }

    pub fn stalls(&self) -> &[Stall] {
        &self.stalls
    }

    pub fn ok(&self) -> bool {
        self.stalls.is_empty()
    }

    fn record(&mut self, stall: Stall) {
        let tag = std::mem::discriminant(&stall);
        if !self.stalls.iter().any(|s| std::mem::discriminant(s) == tag) {
            self.stalls.push(stall);
        }
    }

    /// Feed the watchdog one observation.
    ///
    /// `healthy` means the environment is not currently sabotaging the cluster:
    /// no partitions in force and a quorum of nodes running. `work_pending`
    /// means a client is waiting on something — without it, a commit index that
    /// does not move is just an idle cluster.
    pub fn observe(
        &mut self,
        now: Nanos,
        view: &ClusterView,
        healthy: bool,
        work_pending: bool,
        voters: usize,
    ) {
        if !healthy {
            // The clock only runs while the environment is cooperating.
            // Restarting it on every disruption is what keeps this from
            // flagging a legitimately partitioned cluster.
            self.healthy_since = None;
            self.saw_leader_at = now;
            self.commit_moved_at = now;
            self.converged_at = now;
            return;
        }
        let since = *self.healthy_since.get_or_insert(now);
        let healthy_for = now.saturating_sub(since);

        let alive = view.nodes.len();
        let quorum = voters / 2 + 1;

        // --- is there a leader? ------------------------------------------
        let leaders = view.leaders();
        if leaders.is_empty() {
            if now.saturating_sub(self.saw_leader_at) > self.budget.elect_within {
                self.record(Stall::NoLeader { healthy_for, alive, quorum });
            }
        } else {
            self.saw_leader_at = now;
        }

        // --- is it committing? -------------------------------------------
        let best = view.nodes.values().map(|s| s.commit_index).max().unwrap_or(0);
        if best > self.best_commit {
            self.best_commit = best;
            self.commit_moved_at = now;
        } else if work_pending
            && !leaders.is_empty()
            && now.saturating_sub(self.commit_moved_at) > self.budget.commit_within
        {
            self.record(Stall::NoCommitProgress {
                leader: leaders[0].0,
                stuck_at: best,
                healthy_for,
            });
        }

        // --- are replicas converging? ------------------------------------
        if let Some((leader, _)) = leaders.first() {
            let leader_last = view.nodes.get(leader).map(|s| s.last_index).unwrap_or(0);
            let worst = view
                .nodes
                .iter()
                .filter(|(id, _)| *id != leader)
                .map(|(id, s)| (*id, leader_last.saturating_sub(s.last_index)))
                .max_by_key(|(_, gap)| *gap);
            match worst {
                Some((laggard, gap)) if gap > 0 => {
                    if now.saturating_sub(self.converged_at) > self.budget.converge_within {
                        self.record(Stall::NoConvergence {
                            leader: *leader,
                            laggard,
                            gap,
                            healthy_for,
                        });
                    }
                }
                _ => self.converged_at = now,
            }
        } else {
            self.converged_at = now;
        }
    }
}

impl fmt::Display for Watchdog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.stalls.is_empty() {
            write!(f, "no liveness stalls")
        } else {
            for s in &self.stalls {
                writeln!(f, "{s}")?;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chronolog::node::PublicState;

    fn v(states: Vec<PublicState>) -> ClusterView {
        ClusterView { nodes: states.into_iter().map(|s| (s.node, s)).collect() }
    }

    fn n(id: NodeId, role: &'static str, commit: u64, last: u64) -> PublicState {
        PublicState {
            node: id,
            role,
            commit_index: commit,
            last_index: last,
            ..Default::default()
        }
    }

    #[test]
    fn a_progressing_cluster_never_stalls() {
        let mut w = Watchdog::new(Budget::default());
        for i in 0..200u64 {
            let now = Nanos::from_secs(i);
            w.observe(
                now,
                &v(vec![n(0, "leader", i, i), n(1, "follower", i, i), n(2, "follower", i, i)]),
                true,
                true,
                3,
            );
        }
        assert!(w.ok(), "{w}");
    }

    #[test]
    fn a_healthy_cluster_with_no_leader_is_flagged() {
        let mut w = Watchdog::new(Budget::default());
        for i in 0..200u64 {
            w.observe(
                Nanos::from_secs(i),
                &v(vec![n(0, "follower", 5, 5), n(1, "follower", 5, 5), n(2, "candidate", 5, 5)]),
                true,
                true,
                3,
            );
        }
        assert!(!w.ok());
        assert!(matches!(w.stalls()[0], Stall::NoLeader { .. }), "{w}");
    }

    #[test]
    fn a_partitioned_cluster_with_no_leader_is_not_flagged() {
        // The crucial negative case. A cluster that cannot elect because it is
        // partitioned is behaving correctly; demanding otherwise would be
        // demanding a violation of CAP.
        let mut w = Watchdog::new(Budget::default());
        for i in 0..500u64 {
            w.observe(
                Nanos::from_secs(i),
                &v(vec![n(0, "follower", 5, 5), n(1, "candidate", 5, 5)]),
                false, // unhealthy: partition in force
                true,
                3,
            );
        }
        assert!(w.ok(), "a partitioned cluster must not be reported as stalled: {w}");
    }

    #[test]
    fn an_idle_cluster_is_not_flagged_for_a_static_commit_index() {
        // No work offered, so a commit index that does not move is correct.
        let mut w = Watchdog::new(Budget::default());
        for i in 0..500u64 {
            w.observe(
                Nanos::from_secs(i),
                &v(vec![n(0, "leader", 7, 7), n(1, "follower", 7, 7)]),
                true,
                false, // nothing pending
                3,
            );
        }
        assert!(w.ok(), "{w}");
    }

    #[test]
    fn a_leader_that_stops_committing_under_load_is_flagged() {
        let mut w = Watchdog::new(Budget::default());
        for i in 0..200u64 {
            w.observe(
                Nanos::from_secs(i),
                &v(vec![n(0, "leader", 7, 7), n(1, "follower", 7, 7)]),
                true,
                true, // work is waiting and nothing is happening
                3,
            );
        }
        assert!(!w.ok());
        assert!(matches!(w.stalls()[0], Stall::NoCommitProgress { .. }), "{w}");
    }

    #[test]
    fn a_permanently_lagging_follower_is_flagged() {
        let mut w = Watchdog::new(Budget::default());
        for i in 0..300u64 {
            w.observe(
                Nanos::from_secs(i),
                &v(vec![n(0, "leader", i, i), n(1, "follower", 0, 0)]),
                true,
                true,
                3,
            );
        }
        assert!(!w.ok());
        assert!(w.stalls().iter().any(|s| matches!(s, Stall::NoConvergence { .. })), "{w}");
    }

    #[test]
    fn the_clock_restarts_when_the_environment_breaks_again() {
        let mut w = Watchdog::new(Budget::default());
        // Healthy but leaderless for a while — not yet over budget.
        for i in 0..20u64 {
            w.observe(Nanos::from_secs(i), &v(vec![n(0, "follower", 1, 1)]), true, true, 3);
        }
        // A disruption resets the clock.
        w.observe(Nanos::from_secs(21), &v(vec![n(0, "follower", 1, 1)]), false, true, 3);
        for i in 22..40u64 {
            w.observe(Nanos::from_secs(i), &v(vec![n(0, "follower", 1, 1)]), true, true, 3);
        }
        assert!(w.ok(), "the budget must restart after a disruption: {w}");
    }
}
