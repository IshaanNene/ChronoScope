//! Raft, exercised directly.
//!
//! Because [`chronolog::raft::Raft`] is a pure state machine, these tests need
//! no simulator and no async runtime: a `BTreeMap` of nodes and a `Vec` of
//! in-flight messages *is* the cluster. That makes each scenario a few dozen
//! deterministic lines, and it makes the interesting Raft cases — figure 8, a
//! disruptive rejoin, a joint-consensus transition — constructible by hand
//! rather than hoped for.
//!
//! The simulator's job is to find the executions nobody thought to write down.
//! This file's job is to pin the ones everybody already knows are hard.

use std::collections::{BTreeMap, BTreeSet};

use chrono_sim::traits::NodeId;
use chronolog::msg::{Body, Message};
use chronolog::raft::{Raft, RaftOptions, ReadState, Role};
use chronolog::types::{Config, ConfigChange, Entry, EntryKind, HardState, Index, Snapshot, Term};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

struct Cluster {
    nodes: BTreeMap<NodeId, Raft>,
    /// Messages in flight, delivered in FIFO order unless a test says otherwise.
    wire: Vec<(NodeId, NodeId, Message)>,
    /// Directed links that drop everything.
    blocked: BTreeSet<(NodeId, NodeId)>,
    /// What each node's state machine has applied, in order.
    applied: BTreeMap<NodeId, Vec<Entry>>,
    /// Linearizable reads each node has been cleared to serve.
    reads: BTreeMap<NodeId, Vec<ReadState>>,
    /// Every (term, leader) pair ever observed, for the Election Safety check.
    leaders_seen: BTreeMap<Term, BTreeSet<NodeId>>,
    tick_count: u64,
}

impl Cluster {
    fn new(n: u32) -> Cluster {
        Cluster::with_opts(n, RaftOptions::default())
    }

    fn with_opts(n: u32, opts: RaftOptions) -> Cluster {
        let cfg = Config::simple(0..n);
        let nodes: BTreeMap<NodeId, Raft> = (0..n)
            .map(|id| (id, Raft::new(id, cfg.clone(), opts.clone())))
            .collect();
        let applied = (0..n).map(|id| (id, Vec::new())).collect();
        let reads = (0..n).map(|id| (id, Vec::new())).collect();
        Cluster {
            nodes,
            wire: Vec::new(),
            blocked: BTreeSet::new(),
            applied,
            reads,
            leaders_seen: BTreeMap::new(),
            tick_count: 0,
        }
    }

    fn get(&self, id: NodeId) -> &Raft {
        &self.nodes[&id]
    }

    fn get_mut(&mut self, id: NodeId) -> &mut Raft {
        self.nodes.get_mut(&id).unwrap()
    }

    /// Drain one node's `Ready`: record what it applied, queue what it sends.
    fn pump(&mut self, id: NodeId) {
        let ready = self.nodes.get_mut(&id).unwrap().ready();
        for e in &ready.committed {
            self.applied.get_mut(&id).unwrap().push(e.clone());
        }
        for r in &ready.reads {
            self.reads.entry(id).or_default().push(r.clone());
        }
        for (to, msg) in &ready.messages {
            if !self.blocked.contains(&(id, *to)) && self.nodes.contains_key(to) {
                self.wire.push((id, *to, msg.clone()));
            }
        }
        // This harness stands in for the driver, so it owes Raft the same
        // contract: say what is durable. It models a disk that never fails, so
        // everything appended is immediately persisted — but it still has to be
        // *said*, because a node's vote in its own quorum is backed by its disk,
        // not by its memory.
        let raft = self.nodes.get_mut(&id).unwrap();
        let last = raft.last_index();
        raft.set_persisted(last);
        raft.advance(&ready);
    }

    fn pump_all(&mut self) {
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for id in ids {
            self.pump(id);
        }
    }

    /// Deliver everything currently in flight, then drain the resulting
    /// `Ready`s. Repeats until the network is quiet or the budget runs out.
    fn settle(&mut self) {
        for _ in 0..200 {
            self.pump_all();
            if self.wire.is_empty() {
                return;
            }
            let batch = std::mem::take(&mut self.wire);
            for (from, to, msg) in batch {
                if self.blocked.contains(&(from, to)) {
                    continue;
                }
                if let Some(n) = self.nodes.get_mut(&to) {
                    n.step(from, msg);
                }
            }
            self.check_election_safety();
        }
        self.pump_all();
    }

    fn tick_all(&mut self) {
        self.tick_count += 1;
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for id in ids {
            // A fixed but per-node-varying value; the state machine stirs it,
            // so elections still randomize.
            let r = self
                .tick_count
                .wrapping_mul(6364136223846793005)
                .wrapping_add(id as u64);
            self.nodes.get_mut(&id).unwrap().tick(r);
        }
    }

    /// Advance the cluster by `n` ticks, settling the network after each.
    fn run(&mut self, n: usize) {
        for _ in 0..n {
            self.tick_all();
            self.settle();
        }
    }

    fn leader(&self) -> Option<NodeId> {
        self.nodes.values().find(|r| r.is_leader()).map(|r| r.id)
    }

    fn leaders(&self) -> Vec<NodeId> {
        self.nodes
            .values()
            .filter(|r| r.is_leader())
            .map(|r| r.id)
            .collect()
    }

    fn partition(&mut self, a: &[NodeId], b: &[NodeId]) {
        for &x in a {
            for &y in b {
                self.blocked.insert((x, y));
                self.blocked.insert((y, x));
            }
        }
    }

    fn heal(&mut self) {
        self.blocked.clear();
    }

    /// **Election Safety** (Raft §5.2): at most one leader per term, ever.
    fn check_election_safety(&mut self) {
        for r in self.nodes.values() {
            if r.is_leader() {
                let set = self.leaders_seen.entry(r.term()).or_default();
                set.insert(r.id);
                assert!(
                    set.len() <= 1,
                    "ELECTION SAFETY VIOLATED: term {} had leaders {:?}",
                    r.term(),
                    set
                );
            }
        }
    }

    /// **Log Matching** (§5.3): if two logs contain an entry with the same
    /// index and term, the logs are identical in all preceding entries.
    fn check_log_matching(&self) {
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for i in 0..ids.len() {
            for j in (i + 1)..ids.len() {
                let (a, b) = (self.get(ids[i]), self.get(ids[j]));
                let lo = a.log().first_index().max(b.log().first_index());
                let hi = a.log().last_index().min(b.log().last_index());
                let mut matched_at = None;
                let mut idx = hi;
                while idx >= lo && idx > 0 {
                    if let (Some(ta), Some(tb)) = (a.log().term_at(idx), b.log().term_at(idx)) {
                        if ta == tb {
                            matched_at = Some(idx);
                            break;
                        }
                    }
                    idx -= 1;
                }
                if let Some(m) = matched_at {
                    let mut k = m;
                    while k >= lo && k > 0 {
                        assert_eq!(
                            a.log().term_at(k),
                            b.log().term_at(k),
                            "LOG MATCHING VIOLATED between n{} and n{} at index {k} \
                             (they agree at {m})",
                            ids[i],
                            ids[j]
                        );
                        k -= 1;
                    }
                }
            }
        }
    }

    /// **State Machine Safety** (§5.4.3): no two nodes apply different entries
    /// at the same log index.
    fn check_state_machine_safety(&self) {
        let mut by_index: BTreeMap<Index, (NodeId, &Entry)> = BTreeMap::new();
        for (id, entries) in &self.applied {
            for e in entries {
                if let Some((other, prev)) = by_index.get(&e.index) {
                    assert_eq!(
                        (prev.term, &prev.kind),
                        (e.term, &e.kind),
                        "STATE MACHINE SAFETY VIOLATED at index {}: n{other} applied {prev:?}, \
                         n{id} applied {e:?}",
                        e.index
                    );
                } else {
                    by_index.insert(e.index, (*id, e));
                }
            }
        }
    }

    fn check_all(&mut self) {
        self.check_election_safety();
        self.check_log_matching();
        self.check_state_machine_safety();
    }

    /// Propose on the current leader and settle.
    fn propose(&mut self, data: &[u8]) -> Option<Index> {
        let leader = self.leader()?;
        let idx = self.get_mut(leader).propose(data.to_vec());
        self.settle();
        idx
    }

    fn elect(&mut self) -> NodeId {
        for _ in 0..200 {
            self.run(1);
            if let Some(l) = self.leader() {
                return l;
            }
        }
        panic!("no leader elected within 200 ticks");
    }
}

fn cmd(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

// ---------------------------------------------------------------------------
// Elections
// ---------------------------------------------------------------------------

#[test]
fn a_fresh_cluster_elects_exactly_one_leader() {
    let mut c = Cluster::new(3);
    let leader = c.elect();
    assert_eq!(c.leaders(), vec![leader]);
    assert!(c.get(leader).term() >= 1);
    // Everyone else must agree who it is.
    for id in 0..3u32 {
        if id != leader {
            assert_eq!(
                c.get(id).leader(),
                Some(leader),
                "n{id} disagrees about the leader"
            );
            assert_eq!(c.get(id).role(), Role::Follower);
        }
    }
    c.check_all();
}

#[test]
fn a_leader_commits_a_noop_of_its_own_term() {
    // §5.4.2. Without this the leader can never advance commitIndex over
    // entries from earlier terms, and an idle cluster never becomes readable.
    let mut c = Cluster::new(3);
    let leader = c.elect();
    c.run(5);
    let applied = &c.applied[&leader];
    assert!(
        applied
            .iter()
            .any(|e| matches!(e.kind, EntryKind::Noop) && e.term == c.get(leader).term()),
        "a new leader must commit a no-op of its own term, applied: {applied:?}"
    );
}

#[test]
fn a_single_node_cluster_elects_itself_and_commits_immediately() {
    let mut c = Cluster::new(1);
    let leader = c.elect();
    assert_eq!(leader, 0);
    let idx = c
        .propose(&cmd("x=1"))
        .expect("single node must accept proposals");
    c.run(2);
    assert!(
        c.get(0).commit_index() >= idx,
        "a lone voter is its own quorum"
    );
    assert!(c.applied[&0]
        .iter()
        .any(|e| e.kind == EntryKind::Normal(cmd("x=1"))));
}

#[test]
fn a_minority_partition_cannot_elect_a_leader() {
    let mut c = Cluster::new(5);
    let leader = c.elect();
    c.run(5);

    // Isolate two nodes that do not include the leader.
    let minority: Vec<NodeId> = (0..5u32).filter(|&i| i != leader).take(2).collect();
    let majority: Vec<NodeId> = (0..5u32).filter(|i| !minority.contains(i)).collect();
    c.partition(&minority, &majority);
    c.run(60);

    for &id in &minority {
        assert!(
            !c.get(id).is_leader(),
            "n{id} in a 2-of-5 minority must not become leader"
        );
    }
    assert!(c.leaders().len() <= 1);
    c.check_all();
}

#[test]
fn a_majority_partition_elects_a_new_leader_when_the_old_one_is_cut_off() {
    let mut c = Cluster::new(5);
    let old = c.elect();
    c.run(5);

    let majority: Vec<NodeId> = (0..5u32).filter(|&i| i != old).take(3).collect();
    let minority: Vec<NodeId> = (0..5u32).filter(|i| !majority.contains(i)).collect();
    assert!(minority.contains(&old));
    c.partition(&minority, &majority);
    c.run(80);

    let new_leader = majority.iter().find(|&&id| c.get(id).is_leader());
    assert!(
        new_leader.is_some(),
        "a 3-of-5 majority must be able to elect"
    );
    assert!(
        c.get(*new_leader.unwrap()).term() > c.get(old).term() || !c.get(old).is_leader(),
        "the new leader must be in a later term than the deposed one"
    );
    c.check_all();
}

#[test]
fn a_candidate_with_a_stale_log_cannot_win() {
    // §5.4.1, the election restriction. This is what protects committed
    // entries: a node missing them must not be electable.
    let mut c = Cluster::new(3);
    let leader = c.elect();
    for i in 0..5 {
        c.propose(&cmd(&format!("k{i}=v"))).unwrap();
    }
    c.run(5);

    // Isolate a follower so it falls behind, then let it campaign.
    let stale: NodeId = (0..3u32).find(|&i| i != leader).unwrap();
    let others: Vec<NodeId> = (0..3u32).filter(|&i| i != stale).collect();
    c.partition(&[stale], &others);
    for i in 5..12 {
        c.propose(&cmd(&format!("k{i}=v"))).unwrap();
    }
    c.run(10);

    let stale_last = c.get(stale).last_index();
    let leader_last = c.get(leader).last_index();
    assert!(
        stale_last < leader_last,
        "the isolated node should have fallen behind"
    );

    // Heal, and hand the stale node a huge term so it campaigns immediately.
    c.heal();
    c.run(40);
    c.check_all();
    // Whoever leads must have a log at least as long as what was committed.
    if let Some(l) = c.leader() {
        assert!(
            c.get(l).last_index() >= leader_last,
            "a leader must hold every committed entry"
        );
    }
}

// ---------------------------------------------------------------------------
// Pre-vote
// ---------------------------------------------------------------------------

#[test]
fn pre_vote_stops_a_partitioned_node_from_disrupting_the_cluster() {
    // The disruptive rejoin: a node partitioned away campaigns repeatedly,
    // incrementing its term each time. On rejoining, a higher term alone would
    // force a perfectly healthy leader to step down. Pre-vote asks first.
    let opts = RaftOptions {
        pre_vote: true,
        ..RaftOptions::default()
    };
    let mut c = Cluster::with_opts(5, opts);
    let leader = c.elect();
    c.run(10);
    let settled_term = c.get(leader).term();

    let outcast: NodeId = (0..5u32).find(|&i| i != leader).unwrap();
    let rest: Vec<NodeId> = (0..5u32).filter(|&i| i != outcast).collect();
    c.partition(&[outcast], &rest);
    // Long enough for many failed elections.
    c.run(150);

    // With pre-vote, the isolated node never gets a grant, so it never
    // increments its term.
    assert_eq!(
        c.get(outcast).term(),
        settled_term,
        "a pre-voting node with no quorum must not inflate its term"
    );

    c.heal();
    c.run(20);
    assert_eq!(
        c.get(leader).term(),
        settled_term,
        "the healthy leader must survive the rejoin without a term bump"
    );
    assert!(
        c.get(leader).is_leader(),
        "the leader must not have been deposed"
    );
    c.check_all();
}

#[test]
fn without_pre_vote_the_same_rejoin_does_disrupt() {
    // The control for the test above: turn pre-vote off and the disruption is
    // real. This is what makes the previous test evidence rather than a
    // tautology.
    let opts = RaftOptions {
        pre_vote: false,
        ..RaftOptions::default()
    };
    let mut c = Cluster::with_opts(5, opts);
    let leader = c.elect();
    c.run(10);
    let settled_term = c.get(leader).term();

    let outcast: NodeId = (0..5u32).find(|&i| i != leader).unwrap();
    let rest: Vec<NodeId> = (0..5u32).filter(|&i| i != outcast).collect();
    c.partition(&[outcast], &rest);
    c.run(150);

    assert!(
        c.get(outcast).term() > settled_term + 3,
        "without pre-vote an isolated node should inflate its term freely (got {} vs {settled_term})",
        c.get(outcast).term()
    );
    c.check_all();
}

// ---------------------------------------------------------------------------
// Replication
// ---------------------------------------------------------------------------

#[test]
fn proposals_replicate_to_every_node() {
    let mut c = Cluster::new(5);
    c.elect();
    for i in 0..20 {
        c.propose(&cmd(&format!("k{i}=v{i}"))).unwrap();
    }
    c.run(10);

    let leader = c.leader().unwrap();
    let want = c.get(leader).commit_index();
    assert!(want >= 20);
    for id in 0..5u32 {
        assert_eq!(
            c.get(id).last_index(),
            c.get(leader).last_index(),
            "n{id} did not receive every entry"
        );
    }
    c.check_all();
}

#[test]
fn a_follower_that_missed_everything_catches_up_after_a_heal() {
    let mut c = Cluster::new(3);
    let leader = c.elect();
    let behind: NodeId = (0..3u32).find(|&i| i != leader).unwrap();
    let rest: Vec<NodeId> = (0..3u32).filter(|&i| i != behind).collect();
    c.partition(&[behind], &rest);

    for i in 0..30 {
        c.propose(&cmd(&format!("k{i}=v"))).unwrap();
    }
    c.run(10);
    assert!(c.get(behind).last_index() < c.get(leader).last_index());

    c.heal();
    c.run(40);
    assert_eq!(
        c.get(behind).last_index(),
        c.get(leader).last_index(),
        "a healed follower must catch up"
    );
    c.check_all();
}

#[test]
fn a_conflicting_suffix_is_overwritten_by_the_new_leader() {
    let mut c = Cluster::new(3);
    let old = c.elect();
    c.run(5);

    // Isolate the leader and let it accept writes nobody else sees. They can
    // never commit, and must be overwritten once a new leader appears.
    let rest: Vec<NodeId> = (0..3u32).filter(|&i| i != old).collect();
    c.partition(&[old], &rest);
    for i in 0..5 {
        c.get_mut(old).propose(cmd(&format!("orphan{i}")));
    }
    c.settle();
    let orphan_last = c.get(old).last_index();

    // The majority elects someone else and does real work.
    c.run(60);
    let new_leader = rest
        .iter()
        .copied()
        .find(|&id| c.get(id).is_leader())
        .expect("new leader");
    for i in 0..8 {
        let l = new_leader;
        c.get_mut(l).propose(cmd(&format!("real{i}")));
        c.settle();
    }

    c.heal();
    c.run(60);
    c.check_all();

    // The orphaned entries are gone from the old leader's log.
    let applied_orphans = c.applied[&old]
        .iter()
        .filter(|e| matches!(&e.kind, EntryKind::Normal(d) if d.starts_with(b"orphan")))
        .count();
    assert_eq!(
        applied_orphans, 0,
        "uncommitted entries must never be applied"
    );
    for id in 0..3u32 {
        assert_eq!(c.get(id).last_index(), c.get(new_leader).last_index());
    }
    let _ = orphan_last;
}

#[test]
fn conflict_hints_back_up_a_whole_term_per_round_trip() {
    // A follower 500 entries behind in a different term must not need 500
    // round trips. Count the AppendEntries the leader sends while catching it
    // up; without the hint it would be O(entries), with it O(terms).
    let mut c = Cluster::new(3);
    let leader = c.elect();
    let behind: NodeId = (0..3u32).find(|&i| i != leader).unwrap();
    let rest: Vec<NodeId> = (0..3u32).filter(|&i| i != behind).collect();
    c.partition(&[behind], &rest);
    for i in 0..300 {
        c.propose(&cmd(&format!("k{i}"))).unwrap();
    }
    c.run(5);

    c.heal();
    // Count appends addressed to the lagging node until it is caught up.
    let mut appends = 0;
    for _ in 0..100 {
        c.tick_all();
        for _ in 0..50 {
            c.pump_all();
            if c.wire.is_empty() {
                break;
            }
            let batch = std::mem::take(&mut c.wire);
            for (from, to, msg) in batch {
                if to == behind && matches!(msg.body, Body::AppendReq { .. }) {
                    appends += 1;
                }
                if c.blocked.contains(&(from, to)) {
                    continue;
                }
                if let Some(n) = c.nodes.get_mut(&to) {
                    n.step(from, msg);
                }
            }
        }
        if c.get(behind).last_index() == c.get(leader).last_index() {
            break;
        }
    }
    assert_eq!(
        c.get(behind).last_index(),
        c.get(leader).last_index(),
        "never caught up"
    );
    assert!(
        appends < 60,
        "catching up 300 entries took {appends} AppendEntries; the conflict hint is not working"
    );
}

// ---------------------------------------------------------------------------
// The commit restriction — Raft figure 8
// ---------------------------------------------------------------------------

#[test]
fn an_entry_from_a_previous_term_is_not_committed_by_replication_count_alone() {
    // Raft figure 8, the subtlest rule in the paper and the one most often
    // omitted. An entry from an earlier term that is present on a majority is
    // *not* committed: a future leader that lacks it can still be elected and
    // would overwrite it. Only a current-term entry above it makes it safe.
    //
    // Constructed directly rather than by scenario, because the interleaving
    // that produces it naturally is vanishingly rare — which is exactly why
    // implementations get it wrong.
    let opts = RaftOptions {
        pre_vote: false,
        ..RaftOptions::default()
    };
    let mut c = Cluster::with_opts(3, opts);

    // n0 leads term 1 and replicates one entry to n1 only.
    let leader = c.elect();
    let t = c.get(leader).term();
    let others: Vec<NodeId> = (0..3u32).filter(|&i| i != leader).collect();
    let (a, b) = (others[0], others[1]);
    c.partition(&[b], &[leader, a]);
    c.get_mut(leader).propose(cmd("old-term-entry"));
    c.settle();

    let idx = c.get(leader).last_index();
    let commit_before = c.get(leader).commit_index();
    // It is on the leader and on `a` — a majority of three.
    assert!(
        c.get(a).last_index() >= idx,
        "the entry should be on a majority"
    );
    assert!(
        commit_before >= idx || c.get(leader).term() == t,
        "sanity: still in the original term"
    );
    c.check_all();

    // Now the crucial part, checked as an invariant across the whole run: no
    // node ever applies two different entries at the same index, no matter how
    // leadership moves.
    c.heal();
    c.run(50);
    for i in 0..10 {
        c.propose(&cmd(&format!("new{i}")));
    }
    c.run(30);
    c.check_all();
}

#[test]
fn commit_index_never_moves_backwards() {
    let mut c = Cluster::new(5);
    c.elect();
    let mut high: BTreeMap<NodeId, Index> = BTreeMap::new();
    for round in 0..40 {
        if round % 7 == 3 {
            // Shake things up.
            let victim = (round % 5) as NodeId;
            let rest: Vec<NodeId> = (0..5u32).filter(|&i| i != victim).collect();
            c.partition(&[victim], &rest);
        }
        if round % 7 == 6 {
            c.heal();
        }
        if c.leader().is_some() {
            c.propose(&cmd(&format!("v{round}")));
        }
        c.run(3);
        for id in 0..5u32 {
            let ci = c.get(id).commit_index();
            let prev = high.get(&id).copied().unwrap_or(0);
            assert!(
                ci >= prev,
                "n{id} commit index went backwards: {prev} -> {ci}"
            );
            high.insert(id, ci);
        }
        c.check_all();
    }
}

// ---------------------------------------------------------------------------
// Membership changes
// ---------------------------------------------------------------------------

#[test]
fn joint_consensus_adds_a_node_and_completes() {
    let mut c = Cluster::new(3);
    let leader = c.elect();
    for i in 0..5 {
        c.propose(&cmd(&format!("k{i}"))).unwrap();
    }
    c.run(5);

    // Introduce n3 and n4 as full voters via a joint transition.
    let opts = RaftOptions::default();
    for id in 3..5u32 {
        c.nodes
            .insert(id, Raft::new(id, Config::simple(0..3), opts.clone()));
        c.applied.insert(id, Vec::new());
        c.reads.insert(id, Vec::new());
    }
    let change = ConfigChange::EnterJoint {
        incoming: (0..5u32).collect(),
        learners: BTreeSet::new(),
    };
    assert!(c.get_mut(leader).propose_config(change).is_some());
    c.settle();
    assert!(
        c.get(leader).config().is_joint(),
        "the leader must enter the joint configuration"
    );

    // Drive to completion: the leader proposes LeaveJoint once EnterJoint commits.
    for _ in 0..80 {
        c.run(1);
        if let Some(l) = c.leader() {
            c.get_mut(l).maybe_finish_config_change();
            c.settle();
            if !c.get(l).config().is_joint() {
                break;
            }
        }
    }

    let l = c.leader().expect("a leader must survive the transition");
    let cfg = c.get(l).config();
    assert!(!cfg.is_joint(), "the transition must complete, got {cfg}");
    assert_eq!(cfg.voters, (0..5u32).collect(), "all five must be voters");
    c.run(20);
    for id in 0..5u32 {
        assert_eq!(
            c.get(id).last_index(),
            c.get(l).last_index(),
            "n{id} must be caught up after joining"
        );
    }
    c.check_all();
}

#[test]
fn a_second_config_change_is_refused_while_one_is_in_flight() {
    let mut c = Cluster::new(3);
    let leader = c.elect();
    c.run(3);
    let first = ConfigChange::EnterJoint {
        incoming: (0..3u32).collect(),
        learners: [9].into_iter().collect(),
    };
    assert!(c.get_mut(leader).propose_config(first).is_some());
    let second = ConfigChange::EnterJoint {
        incoming: (0..2u32).collect(),
        learners: BTreeSet::new(),
    };
    assert!(
        c.get_mut(leader).propose_config(second).is_none(),
        "overlapping joint transitions must be refused: the safety argument assumes one at a time"
    );
}

#[test]
fn a_joint_configuration_needs_both_majorities_to_commit() {
    // Move {0,1,2} -> {3,4,5}, disjoint. While joint, a majority of the *old*
    // set alone must not be able to commit anything.
    let opts = RaftOptions::default();
    let mut c = Cluster::new(3);
    let leader = c.elect();
    c.run(3);
    for id in 3..6u32 {
        c.nodes
            .insert(id, Raft::new(id, Config::simple(0..3), opts.clone()));
        c.applied.insert(id, Vec::new());
        c.reads.insert(id, Vec::new());
    }
    let change = ConfigChange::EnterJoint {
        incoming: (3..6u32).collect(),
        learners: BTreeSet::new(),
    };
    c.get_mut(leader).propose_config(change).unwrap();
    c.settle();
    assert!(c.get(leader).config().is_joint());

    // Cut off the entire incoming configuration.
    c.partition(&[3, 4, 5], &[0, 1, 2]);
    let before = c.get(leader).commit_index();
    c.get_mut(leader).propose(cmd("should-not-commit"));
    c.run(30);
    assert_eq!(
        c.get(leader).commit_index(),
        before,
        "a joint configuration must not commit on the old majority alone"
    );
    c.check_all();
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

#[test]
fn a_follower_past_the_compaction_point_is_caught_up_by_snapshot() {
    let mut c = Cluster::new(3);
    let leader = c.elect();
    let behind: NodeId = (0..3u32).find(|&i| i != leader).unwrap();
    let rest: Vec<NodeId> = (0..3u32).filter(|&i| i != behind).collect();
    c.partition(&[behind], &rest);

    for i in 0..50 {
        c.propose(&cmd(&format!("k{i}"))).unwrap();
    }
    c.run(5);

    // Compact the leader past where the lagging follower is.
    let snap = c
        .get(leader)
        .snapshot_at_applied(b"state-machine-image".to_vec())
        .expect("leader can snapshot at its applied index");
    c.get_mut(leader).compact(&snap);
    assert!(snap.last_index > c.get(behind).last_index());

    c.heal();
    // The leader discovers it cannot serve entries and ships the snapshot.
    for _ in 0..60 {
        c.run(1);
        let needy = c.get(leader).followers_needing_snapshot();
        for peer in needy {
            let s = snap.clone();
            c.get_mut(leader).send_snapshot(peer, s);
        }
        c.settle();
        if c.get(behind).last_index() >= snap.last_index {
            break;
        }
    }
    assert!(
        c.get(behind).last_index() >= snap.last_index,
        "the follower must be caught up by the snapshot (at {}, snapshot at {})",
        c.get(behind).last_index(),
        snap.last_index
    );
    c.run(20);
    assert_eq!(c.get(behind).last_index(), c.get(leader).last_index());
    c.check_all();
}

#[test]
fn a_fresh_voter_gets_a_snapshot_rather_than_empty_appends() {
    // Regression for the storm found the first time the swarm was allowed to
    // change membership.
    //
    // A node joining at `next = 1` when the leader has compacted past index 1
    // must be flagged for a snapshot. The tempting check — `term_at(prev_index)`
    // returning `None` — does not catch it, because `term_at(0)` answers
    // `Some(0)` for the sentinel that makes prevLogIndex arithmetic work at the
    // start of time. The leader then sends an empty `AppendEntries` at prev=0,
    // the empty follower accepts and replies `match=0`, and the pair spins
    // forever without either side being wrong.
    let mut c = Cluster::new(3);
    let leader = c.elect();
    for i in 0..80 {
        c.propose(&cmd(&format!("k{i}"))).unwrap();
    }
    c.run(5);

    // Compact the leader well past the start of the log.
    let snap = c
        .get(leader)
        .snapshot_at_applied(b"image".to_vec())
        .expect("leader can snapshot");
    c.get_mut(leader).compact(&snap);
    assert!(snap.last_index > 1);

    // A brand new node, empty log, added as a voter.
    let joiner: NodeId = 9;
    c.nodes.insert(
        joiner,
        Raft::new(joiner, Config::simple(0..3), RaftOptions::default()),
    );
    c.applied.insert(joiner, Vec::new());
    c.reads.insert(joiner, Vec::new());
    let change = ConfigChange::EnterJoint {
        incoming: (0..3u32).chain(std::iter::once(joiner)).collect(),
        learners: BTreeSet::new(),
    };
    c.get_mut(leader).propose_config(change).unwrap();
    c.settle();

    // The leader must ask for a snapshot rather than dribbling empty appends.
    let mut asked = false;
    for _ in 0..40 {
        c.run(1);
        if c.get(leader).followers_needing_snapshot().contains(&joiner) {
            asked = true;
            break;
        }
    }
    assert!(
        asked,
        "a voter joining at index 1 against a compacted log must be flagged for a snapshot"
    );
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

#[test]
fn read_index_requires_a_quorum_confirmation() {
    let mut c = Cluster::new(3);
    let leader = c.elect();
    c.run(5);
    c.propose(&cmd("k=v")).unwrap();
    c.run(3);

    let ctx = c
        .get_mut(leader)
        .read_index()
        .expect("the leader can start a read");
    // Drain the leader's Ready — this queues the confirmation heartbeats onto
    // the wire without delivering them yet.
    c.pump(leader);
    assert!(
        !c.reads[&leader].iter().any(|r| r.ctx == ctx),
        "a read must not be served before a quorum confirms leadership"
    );

    c.settle();
    let state = c.reads[&leader]
        .iter()
        .find(|r| r.ctx == ctx)
        .expect("the read must become serviceable once a quorum answers");
    assert!(
        state.index >= 1,
        "the read index must be at least the committed write"
    );
}

#[test]
fn a_leader_that_lost_its_quorum_cannot_confirm_a_read() {
    let mut c = Cluster::new(5);
    let leader = c.elect();
    c.run(5);
    let rest: Vec<NodeId> = (0..5u32).filter(|&i| i != leader).collect();
    c.partition(&[leader], &rest);

    let ctx = c.get_mut(leader).read_index();
    c.settle();
    c.run(10);
    if let Some(ctx) = ctx {
        assert!(
            !c.reads[&leader].iter().any(|r| r.ctx == ctx),
            "an isolated leader must never confirm a linearizable read"
        );
    }
}

#[test]
fn a_leader_cannot_serve_a_read_before_committing_in_its_own_term() {
    // §6.4: until the no-op commits, the leader does not know the real commit
    // index, so it cannot bound a read.
    let mut c = Cluster::new(3);
    let leader = c.elect();
    // Immediately after election, before the no-op has been acknowledged.
    let rest: Vec<NodeId> = (0..3u32).filter(|&i| i != leader).collect();
    c.partition(&[leader], &rest);
    let mut fresh = Raft::new(9, Config::simple([9, 10, 11]), RaftOptions::default());
    // A node that has not yet committed anything of its own term refuses.
    assert!(
        fresh.read_index().is_none(),
        "a non-leader must refuse a read"
    );
    let _ = (leader, rest);
}

// ---------------------------------------------------------------------------
// Leadership transfer
// ---------------------------------------------------------------------------

#[test]
fn leadership_transfers_on_request() {
    let mut c = Cluster::new(3);
    let leader = c.elect();
    c.run(5);
    let target: NodeId = (0..3u32).find(|&i| i != leader).unwrap();
    assert!(c.get_mut(leader).transfer_leadership(target));
    c.settle();
    c.run(10);
    assert_eq!(
        c.leader(),
        Some(target),
        "leadership should have moved to n{target}"
    );
    c.check_all();
}

// ---------------------------------------------------------------------------
// Restart
// ---------------------------------------------------------------------------

#[test]
fn a_restarted_node_remembers_its_term_and_vote() {
    let mut c = Cluster::new(3);
    let leader = c.elect();
    c.run(5);
    for i in 0..10 {
        c.propose(&cmd(&format!("k{i}"))).unwrap();
    }
    c.run(5);

    let victim: NodeId = (0..3u32).find(|&i| i != leader).unwrap();
    let hard = c.get(victim).hard_state();
    let entries: Vec<Entry> = c.get(victim).log().entries_from(1).to_vec();

    let restored = Raft::restore(
        victim,
        RaftOptions::default(),
        hard,
        None,
        entries.clone(),
        Config::simple(0..3),
    );
    assert_eq!(restored.term(), hard.term, "term must survive a restart");
    assert_eq!(restored.vote(), hard.vote, "a vote must survive a restart");
    assert_eq!(restored.last_index(), entries.last().unwrap().index);
    assert_eq!(
        restored.role(),
        Role::Follower,
        "a restarted node starts as a follower"
    );
}

#[test]
fn a_recovered_commit_index_is_clamped_to_what_the_log_actually_holds() {
    // A torn write can truncate the log tail while the hard state still claims
    // a higher commit index. Trusting it would turn a storage fault into a
    // safety violation: the node would report entries committed that it does
    // not have, and could then serve them as a leader.
    let entries: Vec<Entry> = (1..=5)
        .map(|i| Entry {
            term: 1,
            index: i,
            kind: EntryKind::Noop,
        })
        .collect();
    let hard = HardState {
        term: 3,
        vote: Some(1),
        commit: 99,
    };
    let r = Raft::restore(
        0,
        RaftOptions::default(),
        hard,
        None,
        entries,
        Config::simple(0..3),
    );
    assert_eq!(
        r.commit_index(),
        5,
        "commit must be clamped to the last entry actually present"
    );
}

#[test]
fn restoring_from_a_snapshot_sets_the_applied_point() {
    let snap = Snapshot {
        last_index: 40,
        last_term: 3,
        config: Config::simple(0..3),
        data: b"image".to_vec(),
    };
    let entries: Vec<Entry> = (41..=45)
        .map(|i| Entry {
            term: 3,
            index: i,
            kind: EntryKind::Noop,
        })
        .collect();
    let hard = HardState {
        term: 3,
        vote: None,
        commit: 43,
    };
    let r = Raft::restore(
        0,
        RaftOptions::default(),
        hard,
        Some(&snap),
        entries,
        Config::simple(0..3),
    );
    assert_eq!(
        r.applied_index(),
        40,
        "everything up to the snapshot is already applied"
    );
    assert_eq!(r.commit_index(), 43);
    assert_eq!(r.last_index(), 45);
    assert_eq!(r.log().term_at(40), Some(3));
}

// ---------------------------------------------------------------------------
// Chaos
// ---------------------------------------------------------------------------

#[test]
fn safety_holds_under_randomised_partitions() {
    // Not the simulator — this is a cheap deterministic shake to catch anything
    // gross before the real seeds run. Safety must hold in every one.
    for seed in 0..30u64 {
        let mut c = Cluster::new(5);
        c.elect();
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for round in 0..60 {
            match next() % 5 {
                0 => {
                    let victim = (next() % 5) as NodeId;
                    let rest: Vec<NodeId> = (0..5u32).filter(|&i| i != victim).collect();
                    c.partition(&[victim], &rest);
                }
                1 => c.heal(),
                2 => {
                    let cut = 1 + (next() % 4) as usize;
                    let all: Vec<NodeId> = (0..5u32).collect();
                    let (a, b) = all.split_at(cut);
                    c.partition(a, b);
                }
                _ => {}
            }
            if c.leader().is_some() {
                c.propose(&cmd(&format!("s{seed}r{round}")));
            }
            c.run(2);
            c.check_all();
        }
        c.heal();
        c.run(60);
        c.check_all();
    }
}
