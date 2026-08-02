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

use std::collections::BTreeMap;
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
    /// Per node: the highest `last_index` seen, and when it last advanced.
    ///
    /// Convergence has to mean "the follower is making progress", not "the gap
    /// is zero right now". Under a continuous write workload the gap is
    /// essentially never zero — the leader is always a few entries ahead — so
    /// an oracle keyed on an instantaneous gap reports every busy cluster as
    /// broken. What actually matters is whether the follower is moving.
    progress: BTreeMap<NodeId, (u64, Nanos)>,
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
            progress: BTreeMap::new(),
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
            for (_, at) in self.progress.values_mut() {
                *at = now;
            }
            return;
        }
        let since = *self.healthy_since.get_or_insert(now);
        let healthy_for = now.saturating_sub(since);

        // Only running nodes count. A crashed node's last published state
        // lingers, and treating it as current makes a stopped node look like a
        // stuck cluster.
        let live: Vec<(&NodeId, &chronolog::node::PublicState)> =
            view.nodes.iter().filter(|(_, s)| s.up).collect();
        let alive = live.len();
        let quorum = voters / 2 + 1;

        // --- is there a leader? ------------------------------------------
        let leaders: Vec<(NodeId, u64)> = live
            .iter()
            .filter(|(_, s)| s.role == "leader")
            .map(|(id, s)| (**id, s.term))
            .collect();
        if leaders.is_empty() {
            if now.saturating_sub(self.saw_leader_at) > self.budget.elect_within {
                self.record(Stall::NoLeader { healthy_for, alive, quorum });
            }
        } else {
            self.saw_leader_at = now;
        }

        // --- is it committing? -------------------------------------------
        let best = live.iter().map(|(_, s)| s.commit_index).max().unwrap_or(0);
        // Any change restarts the clock, including a decrease. The maximum is
        // taken over live nodes only, so it drops when the furthest-ahead node
        // crashes — which is a change in the cluster, not a stall in it.
        if best != self.best_commit {
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
        //
        // A follower counts as converging while its own `last_index` advances,
        // however far behind it is. Only a follower that is behind *and frozen*
        // is stuck.
        for (id, s) in &live {
            let entry = self.progress.entry(**id).or_insert((s.last_index, now));
            if s.last_index > entry.0 {
                *entry = (s.last_index, now);
            }
        }
        if let Some((leader, _)) = leaders.first() {
            let leader_last =
                live.iter().find(|(id, _)| **id == *leader).map(|(_, s)| s.last_index).unwrap_or(0);
            for (id, s) in &live {
                if **id == *leader {
                    continue;
                }
                let gap = leader_last.saturating_sub(s.last_index);
                if gap == 0 {
                    continue;
                }
                let frozen_since = self.progress.get(id).map(|(_, at)| *at).unwrap_or(now);
                if now.saturating_sub(frozen_since) > self.budget.converge_within {
                    self.record(Stall::NoConvergence {
                        leader: *leader,
                        laggard: **id,
                        gap,
                        healthy_for,
                    });
                }
            }
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
            up: true,
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
    fn a_follower_that_is_behind_but_advancing_is_not_flagged() {
        // The common case under load: the leader is always a few entries ahead
        // and the gap is never momentarily zero. That is a healthy cluster, and
        // an oracle that reports it is worse than useless — in a 400-seed swarm
        // it buried every real failure under false ones.
        let mut w = Watchdog::new(Budget::default());
        for i in 0..600u64 {
            w.observe(
                Nanos::from_secs(i),
                &v(vec![n(0, "leader", i + 40, i + 40), n(1, "follower", i, i)]),
                true,
                true,
                3,
            );
        }
        assert!(w.ok(), "a follower that is behind but keeping up must not be flagged: {w}");
    }

    #[test]
    fn a_crashed_node_does_not_make_the_cluster_look_stuck() {
        // A crashed node's published state lingers at whatever index it last
        // reported. If the oracle counts it, the cluster appears pinned there.
        let mut w = Watchdog::new(Budget::default());
        let mut dead = n(2, "follower", 9_000, 9_000);
        dead.up = false;
        for i in 0..600u64 {
            w.observe(
                Nanos::from_secs(i),
                &v(vec![n(0, "leader", i, i), n(1, "follower", i, i), dead.clone()]),
                true,
                true,
                3,
            );
        }
        assert!(w.ok(), "{w}");
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
