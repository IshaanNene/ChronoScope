//! The in-memory view of the Raft log.
//!
//! Entries live here from the moment they are appended until a snapshot
//! supersedes them; the [`crate::wal`] holds the durable copy. Keeping the
//! post-snapshot suffix in memory is what lets a leader serve follower
//! catch-up without a disk read, and compaction is what keeps it bounded.
//!
//! Indices are 1-based and contiguous. Index 0 is the "before the beginning"
//! sentinel with term 0, which makes the `prevLogIndex` arithmetic in
//! `AppendEntries` work at the start of time without a special case.

use crate::types::{Config, ConfigChange, Entry, EntryKind, Index, Snapshot, Term};

#[derive(Clone, Debug)]
pub struct Log {
    /// The log is compacted through this point; entries at or below it exist
    /// only inside the snapshot.
    snapshot_index: Index,
    snapshot_term: Term,
    /// The configuration as of the snapshot, before any config entry in
    /// `entries` is replayed on top of it.
    snapshot_config: Config,
    /// `entries[i].index == snapshot_index + 1 + i`, always.
    entries: Vec<Entry>,
}

impl Default for Log {
    fn default() -> Self {
        Log::new()
    }
}

impl Log {
    pub fn new() -> Log {
        Log {
            snapshot_index: 0,
            snapshot_term: 0,
            snapshot_config: Config::default(),
            entries: Vec::new(),
        }
    }

    /// A fresh log for a cluster whose membership is known up front.
    ///
    /// Deliberately separate from [`Log::install_snapshot`]. Bootstrapping via
    /// a zero-index snapshot looks equivalent and is not: `term_at(0)` returns
    /// the sentinel term 0, which matches the snapshot's term, so the install
    /// takes its compaction path — and compacting *to* index 0 is a no-op that
    /// silently discards the configuration. The result is a cluster where no
    /// node is a voter and no election ever starts.
    pub fn bootstrap(config: Config) -> Log {
        Log {
            snapshot_index: 0,
            snapshot_term: 0,
            snapshot_config: config,
            entries: Vec::new(),
        }
    }

    /// Rebuild from what recovery found on disk.
    pub fn restore(snapshot: Option<&Snapshot>, entries: Vec<Entry>) -> Log {
        let mut log = Log::new();
        if let Some(s) = snapshot {
            log.snapshot_index = s.last_index;
            log.snapshot_term = s.last_term;
            log.snapshot_config = s.config.clone();
        }
        debug_assert!(
            entries.first().map(|e| e.index) == Some(log.snapshot_index + 1) || entries.is_empty(),
            "recovered entries must start immediately after the snapshot"
        );
        log.entries = entries;
        log
    }

    pub fn first_index(&self) -> Index {
        self.snapshot_index + 1
    }

    pub fn last_index(&self) -> Index {
        self.entries
            .last()
            .map(|e| e.index)
            .unwrap_or(self.snapshot_index)
    }

    pub fn last_term(&self) -> Term {
        self.entries
            .last()
            .map(|e| e.term)
            .unwrap_or(self.snapshot_term)
    }

    pub fn snapshot_index(&self) -> Index {
        self.snapshot_index
    }

    pub fn snapshot_term(&self) -> Term {
        self.snapshot_term
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    fn slot(&self, index: Index) -> Option<usize> {
        if index <= self.snapshot_index || index > self.last_index() {
            return None;
        }
        Some((index - self.snapshot_index - 1) as usize)
    }

    pub fn get(&self, index: Index) -> Option<&Entry> {
        self.slot(index).map(|i| &self.entries[i])
    }

    /// The term of the entry at `index`.
    ///
    /// Returns `Some(0)` for index 0 (the sentinel) and the snapshot's term for
    /// the snapshot point. `None` means the entry has been compacted away and
    /// the caller must fall back to shipping a snapshot.
    pub fn term_at(&self, index: Index) -> Option<Term> {
        if index == 0 {
            return Some(0);
        }
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        self.get(index).map(|e| e.term)
    }

    /// Entries in `[from, to)`, clamped to what exists.
    pub fn slice(&self, from: Index, to: Index) -> &[Entry] {
        let lo = match self.slot(from.max(self.first_index())) {
            Some(i) => i,
            None => return &[],
        };
        let hi = if to > self.last_index() {
            self.entries.len()
        } else {
            match self.slot(to) {
                Some(i) => i,
                None => return &[],
            }
        };
        if lo >= hi {
            &[]
        } else {
            &self.entries[lo..hi]
        }
    }

    pub fn entries_from(&self, from: Index) -> &[Entry] {
        self.slice(from, self.last_index() + 1)
    }

    /// Is `(term, index)` at least as up to date as this log?
    ///
    /// Raft §5.4.1. This is the election restriction, and it is the reason a
    /// candidate missing committed entries cannot win: a higher last term wins
    /// outright, and on a tie the longer log wins.
    pub fn is_up_to_date(&self, term: Term, index: Index) -> bool {
        term > self.last_term() || (term == self.last_term() && index >= self.last_index())
    }

    /// Append entries, which must be contiguous with what is already here.
    pub fn append(&mut self, entries: &[Entry]) {
        for e in entries {
            debug_assert_eq!(
                e.index,
                self.last_index() + 1,
                "log appends must be contiguous"
            );
            self.entries.push(e.clone());
        }
    }

    /// Drop every entry with index >= `from`.
    pub fn truncate_from(&mut self, from: Index) {
        if let Some(i) = self.slot(from) {
            self.entries.truncate(i);
        } else if from <= self.snapshot_index {
            self.entries.clear();
        }
    }

    /// Merge entries from an `AppendEntries`, returning the index of the last
    /// new entry.
    ///
    /// Only the *conflicting* suffix is truncated. Blindly truncating at
    /// `prev_index + 1` and re-appending would be simpler and is a real bug:
    /// a delayed duplicate of an older `AppendEntries` would delete committed
    /// entries that the leader still believes this follower has.
    pub fn merge(&mut self, prev_index: Index, entries: &[Entry]) -> Index {
        if entries.is_empty() {
            return prev_index;
        }
        let last_new = entries.last().unwrap().index;
        for (i, e) in entries.iter().enumerate() {
            match self.term_at(e.index) {
                Some(t) if t == e.term => continue, // already have it, identical
                Some(_) => {
                    // A genuine conflict: same index, different term. Everything
                    // from here is wrong.
                    self.truncate_from(e.index);
                    self.append(&entries[i..]);
                    return last_new;
                }
                None => {
                    // Past the end of our log — append the rest.
                    self.append(&entries[i..]);
                    return last_new;
                }
            }
        }
        last_new
    }

    /// Replace the log with a snapshot, discarding everything it covers.
    pub fn install_snapshot(&mut self, snap: &Snapshot) {
        // If we already have the snapshot's last entry with a matching term,
        // the snapshot only lets us compact — it must not delete entries past
        // it that we legitimately hold.
        if self.term_at(snap.last_index) == Some(snap.last_term) {
            self.compact_through(snap.last_index, snap.last_term, snap.config.clone());
            return;
        }
        self.snapshot_index = snap.last_index;
        self.snapshot_term = snap.last_term;
        self.snapshot_config = snap.config.clone();
        self.entries.clear();
    }

    /// Discard entries at or below `through`, which the snapshot now covers.
    pub fn compact_through(&mut self, through: Index, term: Term, config: Config) {
        if through <= self.snapshot_index {
            return;
        }
        let keep_from = through.min(self.last_index());
        let drop_count = (keep_from - self.snapshot_index) as usize;
        self.entries.drain(..drop_count.min(self.entries.len()));
        self.snapshot_index = keep_from;
        self.snapshot_term = term;
        self.snapshot_config = config;
    }

    /// The configuration implied by the log: the snapshot's configuration with
    /// every config entry replayed on top.
    ///
    /// Recomputed rather than tracked incrementally. A config change takes
    /// effect on *append*, so a truncation can un-apply one, and threading that
    /// through every mutation is exactly the kind of bookkeeping that goes
    /// subtly wrong. The log is bounded by compaction, so a full replay is
    /// cheap and obviously correct.
    pub fn config(&self) -> Config {
        let mut cfg = self.snapshot_config.clone();
        for e in &self.entries {
            if let EntryKind::Config(change) = &e.kind {
                apply_change(&mut cfg, change);
            }
        }
        cfg
    }

    /// Find where a rejecting follower's log diverges, using the hint it sent.
    ///
    /// Without this, a leader backs up one index per round trip. A follower
    /// that is 10,000 entries behind then needs 10,000 round trips to catch
    /// up, which under a 50ms link is over eight minutes of unavailability.
    pub fn find_conflict_by_term(&self, index: Index, term: Term) -> Index {
        let mut i = index.min(self.last_index());
        while i > self.snapshot_index {
            match self.term_at(i) {
                Some(t) if t <= term => return i,
                Some(_) => i -= 1,
                None => break,
            }
        }
        i
    }
}

/// Apply a configuration change. Shared by [`Log::config`] and the snapshot
/// path so the two can never disagree about what a change means.
pub fn apply_change(cfg: &mut Config, change: &ConfigChange) {
    match change {
        ConfigChange::EnterJoint { incoming, learners } => {
            // The current voters become the outgoing half; both must agree for
            // anything to commit until the transition completes.
            cfg.outgoing = cfg.voters.clone();
            cfg.voters = incoming.clone();
            cfg.learners = learners.clone();
        }
        ConfigChange::LeaveJoint => {
            cfg.outgoing.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::EntryKind;

    fn e(term: Term, index: Index) -> Entry {
        Entry {
            term,
            index,
            kind: EntryKind::Normal(vec![index as u8]),
        }
    }

    fn log_with(entries: &[(Term, Index)]) -> Log {
        let mut l = Log::new();
        l.append(&entries.iter().map(|&(t, i)| e(t, i)).collect::<Vec<_>>());
        l
    }

    #[test]
    fn an_empty_log_reports_the_sentinel() {
        let l = Log::new();
        assert_eq!(l.last_index(), 0);
        assert_eq!(l.last_term(), 0);
        assert_eq!(l.term_at(0), Some(0));
        assert_eq!(l.term_at(1), None);
        assert_eq!(l.first_index(), 1);
    }

    #[test]
    fn slicing_is_clamped_not_panicking() {
        let l = log_with(&[(1, 1), (1, 2), (2, 3), (2, 4)]);
        assert_eq!(l.slice(2, 4).len(), 2);
        assert_eq!(l.slice(1, 99).len(), 4, "past the end clamps to the end");
        assert_eq!(l.slice(9, 12).len(), 0, "entirely past the end is empty");
        assert_eq!(l.slice(3, 3).len(), 0, "an empty range is empty");
        assert_eq!(l.entries_from(3).len(), 2);
    }

    #[test]
    fn up_to_date_implements_the_election_restriction() {
        let l = log_with(&[(1, 1), (2, 2), (2, 3)]);
        // Higher last term wins regardless of length.
        assert!(l.is_up_to_date(3, 1));
        // Same term, at least as long.
        assert!(l.is_up_to_date(2, 3));
        assert!(l.is_up_to_date(2, 9));
        // Same term but shorter loses.
        assert!(!l.is_up_to_date(2, 2));
        // Lower term loses even if longer — this is the case that protects
        // committed entries from a stale but voluminous candidate.
        assert!(!l.is_up_to_date(1, 100));
    }

    #[test]
    fn merge_keeps_matching_entries_and_replaces_only_the_conflict() {
        let mut l = log_with(&[(1, 1), (1, 2), (1, 3), (1, 4)]);
        // A leader in term 2 says 3 and 4 should be term 2.
        let incoming = vec![
            e(1, 2),
            e(1, 3),
            Entry {
                term: 2,
                index: 4,
                ..e(1, 4)
            },
        ];
        let last = l.merge(1, &incoming);
        assert_eq!(last, 4);
        assert_eq!(l.term_at(2), Some(1));
        assert_eq!(l.term_at(3), Some(1));
        assert_eq!(l.term_at(4), Some(2), "the conflicting entry is replaced");
        assert_eq!(l.last_index(), 4);
    }

    #[test]
    fn a_stale_duplicate_append_does_not_delete_later_entries() {
        // The bug this guards: truncating at prev_index+1 unconditionally. A
        // delayed duplicate of an old AppendEntries would then delete entries
        // the leader believes are safely replicated here.
        let mut l = log_with(&[(1, 1), (1, 2), (1, 3), (1, 4), (1, 5)]);
        let stale = vec![e(1, 2), e(1, 3)];
        let last = l.merge(1, &stale);
        assert_eq!(last, 3);
        assert_eq!(
            l.last_index(),
            5,
            "entries 4 and 5 must survive a stale duplicate"
        );
    }

    #[test]
    fn merge_appends_past_the_end() {
        let mut l = log_with(&[(1, 1), (1, 2)]);
        let last = l.merge(2, &[e(2, 3), e(2, 4)]);
        assert_eq!(last, 4);
        assert_eq!(l.last_index(), 4);
        assert_eq!(l.term_at(4), Some(2));
    }

    #[test]
    fn compaction_preserves_the_boundary_term() {
        let mut l = log_with(&[(1, 1), (1, 2), (2, 3), (2, 4), (3, 5)]);
        l.compact_through(3, 2, Config::simple([0, 1, 2]));
        assert_eq!(l.snapshot_index(), 3);
        assert_eq!(
            l.term_at(3),
            Some(2),
            "the boundary term must survive compaction"
        );
        assert_eq!(l.term_at(2), None, "compacted entries are gone");
        assert_eq!(l.first_index(), 4);
        assert_eq!(l.last_index(), 5);
        assert_eq!(l.entries_from(4).len(), 2);
    }

    #[test]
    fn installing_a_snapshot_we_already_cover_only_compacts() {
        let mut l = log_with(&[(1, 1), (1, 2), (2, 3), (2, 4), (2, 5)]);
        let snap = Snapshot {
            last_index: 3,
            last_term: 2,
            config: Config::simple([0, 1, 2]),
            data: vec![],
        };
        l.install_snapshot(&snap);
        assert_eq!(
            l.last_index(),
            5,
            "entries past the snapshot must not be discarded"
        );
        assert_eq!(l.snapshot_index(), 3);
    }

    #[test]
    fn installing_a_divergent_snapshot_replaces_the_log() {
        let mut l = log_with(&[(1, 1), (1, 2), (1, 3)]);
        let snap = Snapshot {
            last_index: 9,
            last_term: 4,
            config: Config::simple([0, 1, 2]),
            data: vec![],
        };
        l.install_snapshot(&snap);
        assert_eq!(l.last_index(), 9);
        assert_eq!(l.last_term(), 4);
        assert!(l.is_empty());
        assert_eq!(l.term_at(9), Some(4));
    }

    #[test]
    fn bootstrap_actually_installs_the_configuration() {
        // Regression: bootstrapping through `install_snapshot` at index 0 takes
        // the compaction path (the sentinel term 0 matches), which no-ops and
        // drops the config. Every node then reports no voters and no election
        // ever starts.
        let l = Log::bootstrap(Config::simple([0, 1, 2]));
        assert_eq!(l.config(), Config::simple([0, 1, 2]));
        assert!(l.config().is_voter(1));
    }

    #[test]
    fn config_is_replayed_from_the_log() {
        let mut l = Log::new();
        l.snapshot_config = Config::simple([0, 1, 2]);
        assert_eq!(l.config(), Config::simple([0, 1, 2]));

        l.append(&[Entry {
            term: 1,
            index: 1,
            kind: EntryKind::Config(ConfigChange::EnterJoint {
                incoming: [2, 3, 4].into_iter().collect(),
                learners: Default::default(),
            }),
        }]);
        let joint = l.config();
        assert!(joint.is_joint());
        assert_eq!(joint.voters, [2, 3, 4].into_iter().collect());
        assert_eq!(joint.outgoing, [0, 1, 2].into_iter().collect());

        l.append(&[Entry {
            term: 1,
            index: 2,
            kind: EntryKind::Config(ConfigChange::LeaveJoint),
        }]);
        let done = l.config();
        assert!(!done.is_joint());
        assert_eq!(done.voters, [2, 3, 4].into_iter().collect());
    }

    #[test]
    fn truncation_un_applies_a_config_change() {
        // Config changes take effect on append, so rolling back the log must
        // roll back the configuration. Recomputing rather than tracking makes
        // this fall out for free.
        let mut l = Log::new();
        l.snapshot_config = Config::simple([0, 1, 2]);
        l.append(&[Entry {
            term: 1,
            index: 1,
            kind: EntryKind::Config(ConfigChange::EnterJoint {
                incoming: [2, 3, 4].into_iter().collect(),
                learners: Default::default(),
            }),
        }]);
        assert!(l.config().is_joint());
        l.truncate_from(1);
        assert_eq!(
            l.config(),
            Config::simple([0, 1, 2]),
            "truncation must undo the change"
        );
    }

    #[test]
    fn conflict_search_skips_a_whole_term_at_a_time() {
        // Follower has term 1 at 1-3 and term 4 at 4-6. A leader probing at
        // index 6 with term 2 should land at 3, not walk back one at a time.
        let l = log_with(&[(1, 1), (1, 2), (1, 3), (4, 4), (4, 5), (4, 6)]);
        assert_eq!(l.find_conflict_by_term(6, 2), 3);
        assert_eq!(l.find_conflict_by_term(6, 4), 6);
        assert_eq!(l.find_conflict_by_term(2, 1), 2);
    }
}
