//! The types Raft persists and replicates.
//!
//! Everything here has an explicit wire format. These bytes go on disk and
//! survive restarts, so the encoding is part of the on-disk contract and is
//! specified rather than derived.

use std::collections::BTreeSet;
use std::fmt;

use chrono_sim::traits::NodeId;

use crate::codec::{DecodeError, Reader, Result, Writer};

pub type Term = u64;
pub type Index = u64;

// ---------------------------------------------------------------------------
// Log entries
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EntryKind {
    /// A command for the state machine.
    Normal(Vec<u8>),
    /// A configuration change. Applied to the *configuration* the moment it is
    /// appended, not when it commits — see the note on [`Config`].
    Config(ConfigChange),
    /// Committed by a leader at the start of its term.
    ///
    /// This is not ceremony. A leader may not conclude that entries from
    /// previous terms are committed just because they are replicated on a
    /// majority (Raft §5.4.2, figure 8). Committing one entry of its *own*
    /// term is what lets it advance `commitIndex` over the earlier ones, and
    /// on an idle cluster there may be no client command to serve that role.
    Noop,
}

#[derive(Clone, PartialEq, Eq)]
pub struct Entry {
    pub term: Term,
    pub index: Index,
    pub kind: EntryKind,
}

impl fmt::Debug for Entry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let k = match &self.kind {
            EntryKind::Normal(d) => format!("cmd[{}B]", d.len()),
            EntryKind::Config(c) => format!("{c:?}"),
            EntryKind::Noop => "noop".to_string(),
        };
        write!(f, "({}.{} {})", self.term, self.index, k)
    }
}

impl Entry {
    pub fn encode(&self, w: &mut Writer) {
        w.u64(self.term).u64(self.index);
        match &self.kind {
            EntryKind::Normal(d) => {
                w.u8(0).bytes(d);
            }
            EntryKind::Config(c) => {
                w.u8(1);
                c.encode(w);
            }
            EntryKind::Noop => {
                w.u8(2);
            }
        }
    }

    pub fn decode(r: &mut Reader<'_>) -> Result<Entry> {
        let term = r.u64()?;
        let index = r.u64()?;
        let kind = match r.u8()? {
            0 => EntryKind::Normal(r.bytes()?),
            1 => EntryKind::Config(ConfigChange::decode(r)?),
            2 => EntryKind::Noop,
            t => return Err(DecodeError::BadTag(t)),
        };
        Ok(Entry { term, index, kind })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut w = Writer::new();
        self.encode(&mut w);
        w.finish()
    }
}

// ---------------------------------------------------------------------------
// Cluster configuration and joint consensus
// ---------------------------------------------------------------------------

/// The membership of the cluster, possibly mid-transition.
///
/// During a change, `outgoing` is non-empty and the cluster is in *joint
/// consensus*: a decision needs a majority of `voters` **and** a majority of
/// `outgoing` independently. That double requirement is the entire safety
/// argument for reconfiguration — it makes it impossible for C_old and C_new to
/// each elect a leader without overlapping.
///
/// Note that configuration changes take effect on *append*, not on commit.
/// This is Raft §6 as written, and it is unintuitive: a node must obey a
/// configuration it has not yet committed, because if the change commits, some
/// node must already have been counting under it. The alternative — waiting for
/// commit — deadlocks, since the commit itself needs the new quorum.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Config {
    pub voters: BTreeSet<NodeId>,
    /// Non-empty exactly while in joint consensus.
    pub outgoing: BTreeSet<NodeId>,
    /// Replicated to, but never counted toward a quorum, and never granted a
    /// vote. How a new node catches up before it can stall an election.
    pub learners: BTreeSet<NodeId>,
}

impl Config {
    pub fn simple(voters: impl IntoIterator<Item = NodeId>) -> Config {
        Config {
            voters: voters.into_iter().collect(),
            outgoing: BTreeSet::new(),
            learners: BTreeSet::new(),
        }
    }

    pub fn is_joint(&self) -> bool {
        !self.outgoing.is_empty()
    }

    /// Every node that must be replicated to, voters and learners alike.
    pub fn all_nodes(&self) -> BTreeSet<NodeId> {
        self.voters.iter().chain(self.outgoing.iter()).chain(self.learners.iter()).copied().collect()
    }

    pub fn is_voter(&self, id: NodeId) -> bool {
        self.voters.contains(&id) || self.outgoing.contains(&id)
    }

    /// Does this set of nodes constitute a quorum? In joint consensus it must
    /// be a majority of *both* configurations.
    pub fn has_quorum(&self, votes: &BTreeSet<NodeId>) -> bool {
        // A configuration with no voters at all has no quorum. Without this
        // guard the vacuous-truth reading of "a majority of the empty set"
        // makes an unconfigured node think every decision is unanimous — which
        // turns a bootstrap mistake into a safety violation rather than a hang.
        if self.voters.is_empty() && self.outgoing.is_empty() {
            return false;
        }
        fn majority(group: &BTreeSet<NodeId>, votes: &BTreeSet<NodeId>) -> bool {
            if group.is_empty() {
                return true; // this half is not constraining (non-joint case)
            }
            let got = group.iter().filter(|id| votes.contains(id)).count();
            got * 2 > group.len()
        }
        majority(&self.voters, votes) && (!self.is_joint() || majority(&self.outgoing, votes))
    }

    /// The highest index replicated on a quorum — the commit index a leader is
    /// entitled to advance to (subject to the term check in §5.4.2).
    ///
    /// In joint consensus this is the *minimum* of the two configurations'
    /// quorum indices, which is what stops a change from committing data that
    /// only one half of the transition has.
    pub fn quorum_index(&self, matched: impl Fn(NodeId) -> Index) -> Index {
        fn quorum_of(group: &BTreeSet<NodeId>, matched: &impl Fn(NodeId) -> Index) -> Option<Index> {
            if group.is_empty() {
                return None;
            }
            let mut idx: Vec<Index> = group.iter().map(|id| matched(*id)).collect();
            // Sort descending and take the element at the majority position:
            // with n nodes, that is the highest index at least ceil(n/2)+... —
            // concretely, `idx[n/2]` after a descending sort.
            idx.sort_unstable_by(|a, b| b.cmp(a));
            Some(idx[group.len() / 2])
        }
        match (quorum_of(&self.voters, &matched), quorum_of(&self.outgoing, &matched)) {
            (Some(a), Some(b)) => a.min(b),
            (Some(a), None) => a,
            (None, Some(b)) => b,
            (None, None) => 0,
        }
    }

    pub fn encode(&self, w: &mut Writer) {
        let enc = |w: &mut Writer, s: &BTreeSet<NodeId>| {
            let v: Vec<NodeId> = s.iter().copied().collect();
            w.seq(&v, |w, id| {
                w.u32(*id);
            });
        };
        enc(w, &self.voters);
        enc(w, &self.outgoing);
        enc(w, &self.learners);
    }

    pub fn decode(r: &mut Reader<'_>) -> Result<Config> {
        let dec = |r: &mut Reader<'_>| -> Result<BTreeSet<NodeId>> {
            Ok(r.seq(|r| r.u32())?.into_iter().collect())
        };
        Ok(Config { voters: dec(r)?, outgoing: dec(r)?, learners: dec(r)? })
    }
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let list = |s: &BTreeSet<NodeId>| {
            s.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(",")
        };
        if self.is_joint() {
            write!(f, "joint[{}|{}]", list(&self.voters), list(&self.outgoing))?;
        } else {
            write!(f, "[{}]", list(&self.voters))?;
        }
        if !self.learners.is_empty() {
            write!(f, "+learners[{}]", list(&self.learners))?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConfigChange {
    /// Step into `C_old,new`. `incoming` becomes the new voter set while the
    /// current voters become `outgoing`.
    EnterJoint { incoming: BTreeSet<NodeId>, learners: BTreeSet<NodeId> },
    /// Step out of joint consensus into `C_new`.
    LeaveJoint,
}

impl ConfigChange {
    pub fn encode(&self, w: &mut Writer) {
        match self {
            ConfigChange::EnterJoint { incoming, learners } => {
                w.u8(0);
                let v: Vec<NodeId> = incoming.iter().copied().collect();
                w.seq(&v, |w, id| {
                    w.u32(*id);
                });
                let l: Vec<NodeId> = learners.iter().copied().collect();
                w.seq(&l, |w, id| {
                    w.u32(*id);
                });
            }
            ConfigChange::LeaveJoint => {
                w.u8(1);
            }
        }
    }

    pub fn decode(r: &mut Reader<'_>) -> Result<ConfigChange> {
        match r.u8()? {
            0 => {
                let incoming = r.seq(|r| r.u32())?.into_iter().collect();
                let learners = r.seq(|r| r.u32())?.into_iter().collect();
                Ok(ConfigChange::EnterJoint { incoming, learners })
            }
            1 => Ok(ConfigChange::LeaveJoint),
            t => Err(DecodeError::BadTag(t)),
        }
    }
}

// ---------------------------------------------------------------------------
// Persistent state
// ---------------------------------------------------------------------------

/// The three fields Raft requires on stable storage before responding to any
/// RPC. Losing `term` or `vote` across a restart lets a node vote twice in one
/// term, which loses Election Safety and therefore everything.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HardState {
    pub term: Term,
    pub vote: Option<NodeId>,
    /// Not strictly required to be durable, but persisting it saves replaying
    /// the whole log to rediscover what was already applied.
    pub commit: Index,
}

/// A point-in-time image of the state machine, replacing the log prefix it
/// covers.
#[derive(Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub last_index: Index,
    pub last_term: Term,
    pub config: Config,
    pub data: Vec<u8>,
}

impl fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Snapshot(@{}.{} {} {}B)",
            self.last_term,
            self.last_index,
            self.config,
            self.data.len()
        )
    }
}

impl Snapshot {
    pub fn encode(&self, w: &mut Writer) {
        w.u64(self.last_index).u64(self.last_term);
        self.config.encode(w);
        w.bytes(&self.data);
    }

    pub fn decode(r: &mut Reader<'_>) -> Result<Snapshot> {
        let last_index = r.u64()?;
        let last_term = r.u64()?;
        let config = Config::decode(r)?;
        let data = r.bytes()?;
        Ok(Snapshot { last_index, last_term, config, data })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(ids: &[NodeId]) -> BTreeSet<NodeId> {
        ids.iter().copied().collect()
    }

    #[test]
    fn entries_round_trip() {
        let entries = vec![
            Entry { term: 3, index: 7, kind: EntryKind::Normal(b"put a=1".to_vec()) },
            Entry { term: 3, index: 8, kind: EntryKind::Noop },
            Entry {
                term: 4,
                index: 9,
                kind: EntryKind::Config(ConfigChange::EnterJoint {
                    incoming: set(&[1, 2, 3, 4]),
                    learners: set(&[9]),
                }),
            },
            Entry { term: 4, index: 10, kind: EntryKind::Config(ConfigChange::LeaveJoint) },
        ];
        for e in &entries {
            let bytes = e.to_bytes();
            let got = Entry::decode(&mut Reader::new(&bytes)).unwrap();
            assert_eq!(&got, e);
        }
    }

    #[test]
    fn a_simple_majority_is_a_quorum() {
        let c = Config::simple([0, 1, 2]);
        assert!(!c.has_quorum(&set(&[0])));
        assert!(c.has_quorum(&set(&[0, 1])));
        assert!(c.has_quorum(&set(&[0, 1, 2])));
        assert!(!c.is_joint());
    }

    #[test]
    fn joint_consensus_needs_both_majorities() {
        // Moving from {0,1,2} to {2,3,4} — deliberately overlapping in only one
        // node, which is the case that goes wrong if you check only one side.
        let c = Config {
            voters: set(&[2, 3, 4]),
            outgoing: set(&[0, 1, 2]),
            learners: BTreeSet::new(),
        };
        assert!(c.is_joint());
        // A majority of C_new alone is not enough.
        assert!(!c.has_quorum(&set(&[2, 3, 4])));
        // A majority of C_old alone is not enough.
        assert!(!c.has_quorum(&set(&[0, 1, 2])));
        // Both: {0,1} covers C_old, {3,4} covers C_new.
        assert!(c.has_quorum(&set(&[0, 1, 3, 4])));
        assert!(c.has_quorum(&set(&[0, 2, 3, 4])));
    }

    #[test]
    fn learners_never_count_toward_a_quorum() {
        let c = Config {
            voters: set(&[0, 1, 2]),
            outgoing: BTreeSet::new(),
            learners: set(&[7, 8, 9]),
        };
        assert!(!c.has_quorum(&set(&[0, 7, 8, 9])));
        assert!(c.has_quorum(&set(&[0, 1])));
        assert!(c.all_nodes().contains(&7));
        assert!(!c.is_voter(7));
    }

    #[test]
    fn quorum_index_is_the_majority_replicated_point() {
        let c = Config::simple([0, 1, 2]);
        // Matched: n0=10, n1=8, n2=3. Sorted desc: [10, 8, 3]; index 1 -> 8.
        let m = |id: NodeId| match id {
            0 => 10,
            1 => 8,
            _ => 3,
        };
        assert_eq!(c.quorum_index(m), 8);

        // Five voters, three of which have index >= 6.
        let c5 = Config::simple([0, 1, 2, 3, 4]);
        let m5 = |id: NodeId| match id {
            0 => 9,
            1 => 7,
            2 => 6,
            3 => 2,
            _ => 0,
        };
        assert_eq!(c5.quorum_index(m5), 6);
    }

    #[test]
    fn joint_quorum_index_is_the_lesser_of_the_two() {
        let c = Config {
            voters: set(&[3, 4, 5]),   // C_new, all caught up
            outgoing: set(&[0, 1, 2]), // C_old, lagging
            learners: BTreeSet::new(),
        };
        let m = |id: NodeId| if id >= 3 { 20 } else { 5 };
        // C_new says 20, C_old says 5. A change must not commit past what the
        // old configuration has.
        assert_eq!(c.quorum_index(m), 5);
    }

    #[test]
    fn config_and_snapshot_round_trip() {
        let cfg = Config {
            voters: set(&[1, 2, 3]),
            outgoing: set(&[0, 1, 2]),
            learners: set(&[42]),
        };
        let mut w = Writer::new();
        cfg.encode(&mut w);
        let buf = w.finish();
        assert_eq!(Config::decode(&mut Reader::new(&buf)).unwrap(), cfg);

        let snap =
            Snapshot { last_index: 99, last_term: 7, config: cfg, data: vec![1, 2, 3, 4] };
        let mut w = Writer::new();
        snap.encode(&mut w);
        let buf = w.finish();
        assert_eq!(Snapshot::decode(&mut Reader::new(&buf)).unwrap(), snap);
    }
}
