//! Raft, from the paper.
//!
//! # Shape
//!
//! This is a **pure state machine**. It performs no I/O, holds no `Host`, and
//! never awaits. You feed it inputs — a tick, a message, a proposal — and it
//! returns a [`Ready`] describing what must happen next. The driver in
//! [`crate::node`] does the actual persisting and sending.
//!
//! That shape is not stylistic. Raft's correctness rests on an ordering that is
//! invisible if the I/O is inline:
//!
//! > **Persist before you send.** A vote must be on stable storage before the
//! > `VoteResp` leaves. Entries must be fsynced before the `AppendResp` that
//! > acknowledges them. Get this backwards and a node that crashes and restarts
//! > can contradict something it already told the cluster — which loses
//! > Election Safety, and with it everything else.
//!
//! Returning a `Ready` makes that ordering a type-level fact rather than a
//! comment: the messages are in a field the driver cannot send until it has
//! handled the `hard_state` and `entries` fields.
//!
//! # What is implemented
//!
//! - Leader election with **pre-vote** (§9.6), so a partitioned node cannot
//!   disrupt a healthy cluster on rejoin
//! - Log replication with fast conflict backtracking (§5.3)
//! - The commit restriction of §5.4.2 — a leader may only advance `commitIndex`
//!   via an entry of *its own* term
//! - Snapshots and log compaction (§7)
//! - Membership changes via **joint consensus** (§6)
//! - `ReadIndex` and lease-based reads (§6.4)
//! - Leadership transfer via `TimeoutNow`

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use chrono_sim::traits::NodeId;

use crate::log::Log;
use crate::msg::{Body, Message};
use crate::types::{Config, ConfigChange, Entry, EntryKind, HardState, Index, Snapshot, Term};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct RaftOptions {
    /// Election timeout, in ticks. The actual timeout for each election is
    /// drawn uniformly from `[election_ticks, 2 * election_ticks)`.
    ///
    /// The randomization is load-bearing. With a fixed timeout, followers that
    /// lost the same heartbeat time out simultaneously, split the vote, and
    /// repeat — potentially forever. Randomizing makes a split vote a
    /// transient rather than a stable state.
    pub election_ticks: u32,
    /// Heartbeat interval, in ticks. Must be comfortably below
    /// `election_ticks`, or a healthy leader gets deposed by its own latency.
    pub heartbeat_ticks: u32,
    /// Maximum entries in a single `AppendEntries`.
    pub max_entries_per_append: usize,
    /// Maximum payload bytes in a single `AppendEntries`.
    pub max_bytes_per_append: usize,
    /// Run pre-vote before a real election.
    pub pre_vote: bool,
    /// Allow the leader to serve reads from its lease without a round trip.
    /// Off by default: it is not linearizable under clock skew.
    pub lease_reads: bool,
    /// Take a snapshot once this many entries have accumulated past the last
    /// snapshot point.
    pub snapshot_interval: u64,
}

impl Default for RaftOptions {
    fn default() -> Self {
        Self {
            election_ticks: 10,
            heartbeat_ticks: 3,
            max_entries_per_append: 64,
            max_bytes_per_append: 1 << 20,
            pre_vote: true,
            lease_reads: false,
            snapshot_interval: 2000,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Role {
    Follower,
    /// Polling for support without having incremented the term.
    PreCandidate,
    Candidate,
    Leader,
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Role::Follower => "follower",
            Role::PreCandidate => "pre-candidate",
            Role::Candidate => "candidate",
            Role::Leader => "leader",
        };
        f.write_str(s)
    }
}

/// How far along a follower is, from the leader's point of view.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProgressState {
    /// We do not know where this follower's log diverges. Send one probe at a
    /// time and wait for the answer; flooding it would waste bandwidth on
    /// entries it will reject anyway.
    Probe,
    /// We know where it is. Stream entries optimistically.
    Replicate,
    /// It is so far behind that the entries it needs are compacted away. A
    /// snapshot is in flight; do not send entries until it lands.
    Snapshot,
}

#[derive(Clone, Debug)]
struct Progress {
    next: Index,
    matched: Index,
    state: ProgressState,
    /// Index of the snapshot in flight, so we do not send it repeatedly.
    pending_snapshot: Index,
    /// Whether this follower responded since the last check. Used by the
    /// leader to notice it has lost quorum contact.
    active: bool,
}

impl Progress {
    fn new(next: Index) -> Progress {
        Progress {
            next,
            matched: 0,
            state: ProgressState::Probe,
            pending_snapshot: 0,
            active: false,
        }
    }
}

/// A pending linearizable read, waiting for its commit index to be applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadState {
    pub ctx: u64,
    pub index: Index,
}

/// Everything the driver must do before the state machine advances.
///
/// The field order is the required execution order.
#[derive(Clone, Debug, Default)]
pub struct Ready {
    /// Persist and fsync **first**, if present.
    pub hard_state: Option<HardState>,
    /// Append and fsync **second**.
    pub entries: Vec<Entry>,
    /// A snapshot to write and install.
    pub snapshot: Option<Snapshot>,
    /// Send only after everything above is durable.
    pub messages: Vec<(NodeId, Message)>,
    /// Feed to the state machine after the above.
    pub committed: Vec<Entry>,
    /// Reads that may now be answered.
    pub reads: Vec<ReadState>,
}

impl Ready {
    pub fn is_empty(&self) -> bool {
        self.hard_state.is_none()
            && self.entries.is_empty()
            && self.snapshot.is_none()
            && self.messages.is_empty()
            && self.committed.is_empty()
            && self.reads.is_empty()
    }

    /// Whether anything here has to reach the disk before the messages go out.
    pub fn needs_durability(&self) -> bool {
        self.hard_state.is_some() || !self.entries.is_empty() || self.snapshot.is_some()
    }
}

// ---------------------------------------------------------------------------
// The state machine
// ---------------------------------------------------------------------------

pub struct Raft {
    pub id: NodeId,
    opts: RaftOptions,

    // --- persistent ------------------------------------------------------
    term: Term,
    vote: Option<NodeId>,
    log: Log,

    // --- volatile --------------------------------------------------------
    commit: Index,
    applied: Index,
    role: Role,
    leader: Option<NodeId>,

    // --- election --------------------------------------------------------
    votes: BTreeMap<NodeId, bool>,
    election_elapsed: u32,
    heartbeat_elapsed: u32,
    /// Drawn fresh for each election from `[election_ticks, 2*election_ticks)`.
    election_timeout: u32,

    // --- leader ----------------------------------------------------------
    progress: BTreeMap<NodeId, Progress>,
    /// Reads waiting for a quorum of heartbeat confirmations.
    pending_reads: Vec<PendingRead>,
    read_ctx: u64,
    /// Ticks since a quorum of followers was last heard from, for lease reads.
    quorum_contact_elapsed: u32,
    /// Set once the leader has committed an entry of its own term. Until then
    /// it does not know the true commit index and must not serve reads.
    committed_own_term: bool,

    // --- outputs ---------------------------------------------------------
    pending: Ready,
    hard_state_dirty: bool,
    /// The last hard state handed to the driver, so we only ask for a
    /// (expensive, fsync-bearing) write when something actually changed.
    persisted_hard_state: HardState,
    /// Set when the log was truncated; the driver must mirror it in the WAL.
    pub truncate_to: Option<Index>,
    /// Entropy for election timeouts, refreshed by the driver on every tick.
    ///
    /// The state machine stays pure — it never *reads* a generator — but
    /// campaigns can start from an internal transition (a pre-vote succeeding,
    /// a `TimeoutNow`) where no fresh value is to hand. Stirring the stored
    /// value keeps those paths randomized too. Without it, a node that starts a
    /// real election after a pre-vote always picks the minimum timeout, and two
    /// nodes that split a vote would split the next one identically.
    rand: u64,
}

#[derive(Clone, Debug)]
struct PendingRead {
    ctx: u64,
    index: Index,
    acks: BTreeSet<NodeId>,
}

impl fmt::Debug for Raft {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Raft")
            .field("id", &self.id)
            .field("role", &self.role)
            .field("term", &self.term)
            .field("vote", &self.vote)
            .field("commit", &self.commit)
            .field("applied", &self.applied)
            .field("last", &self.log.last_index())
            .field("config", &self.log.config().to_string())
            .finish()
    }
}

impl Raft {
    pub fn new(id: NodeId, config: Config, opts: RaftOptions) -> Raft {
        // With no snapshot and no entries the configuration has to come from
        // somewhere; seeding the log's base config is that somewhere, and it is
        // superseded the moment a real config entry lands.
        let log = Log::bootstrap(config);
        Raft {
            id,
            election_timeout: opts.election_ticks,
            opts,
            term: 0,
            vote: None,
            log,
            commit: 0,
            applied: 0,
            role: Role::Follower,
            leader: None,
            votes: BTreeMap::new(),
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            progress: BTreeMap::new(),
            pending_reads: Vec::new(),
            read_ctx: 0,
            quorum_contact_elapsed: 0,
            committed_own_term: false,
            pending: Ready::default(),
            hard_state_dirty: false,
            persisted_hard_state: HardState::default(),
            truncate_to: None,
            rand: (id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1,
        }
    }

    /// SplitMix64's finalizer, used to stir the stored entropy.
    fn stir(&mut self) -> u64 {
        self.rand = self.rand.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.rand;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Rebuild after a restart from what the WAL recovered.
    pub fn restore(
        id: NodeId,
        opts: RaftOptions,
        hard: HardState,
        snapshot: Option<&Snapshot>,
        entries: Vec<Entry>,
        bootstrap: Config,
    ) -> Raft {
        let mut r = Raft::new(id, bootstrap.clone(), opts);
        r.log = Log::restore(snapshot, entries);
        // A log with neither a snapshot nor a config entry carries no
        // membership of its own; fall back to what the operator configured.
        if r.log.config().voters.is_empty() {
            let mut base = Log::bootstrap(bootstrap);
            base.append(r.log.entries_from(1));
            r.log = base;
        }
        r.term = hard.term;
        r.vote = hard.vote;
        // A recovered commit index may be ahead of what this node's log holds
        // if the tail was truncated by a torn write. Clamp it: claiming to have
        // committed entries you do not have is how a restart turns a storage
        // fault into a safety violation.
        r.commit = hard.commit.min(r.log.last_index()).max(r.log.snapshot_index());
        r.applied = r.log.snapshot_index();
        r.persisted_hard_state = HardState { term: r.term, vote: r.vote, commit: r.commit };
        r
    }

    // --- accessors -------------------------------------------------------

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn term(&self) -> Term {
        self.term
    }

    pub fn vote(&self) -> Option<NodeId> {
        self.vote
    }

    pub fn commit_index(&self) -> Index {
        self.commit
    }

    pub fn applied_index(&self) -> Index {
        self.applied
    }

    pub fn last_index(&self) -> Index {
        self.log.last_index()
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    pub fn config(&self) -> Config {
        self.log.config()
    }

    pub fn log(&self) -> &Log {
        &self.log
    }

    pub fn hard_state(&self) -> HardState {
        HardState { term: self.term, vote: self.vote, commit: self.commit }
    }

    /// A leader that has heard from a quorum recently enough to serve a lease
    /// read. See [`crate::client::ReadMode::Lease`] for why this is not
    /// linearizable.
    pub fn lease_valid(&self) -> bool {
        self.role == Role::Leader
            && self.opts.lease_reads
            && self.quorum_contact_elapsed < self.opts.election_ticks
    }

    // --- inputs ----------------------------------------------------------

    /// Advance the logical clock by one tick. `rand` supplies the randomness
    /// for election timeouts; the state machine stays pure by taking it as an
    /// input rather than owning a generator.
    pub fn tick(&mut self, rand: u64) {
        self.rand ^= rand;
        self.election_elapsed += 1;
        match self.role {
            Role::Leader => {
                self.heartbeat_elapsed += 1;
                self.quorum_contact_elapsed += 1;
                if self.heartbeat_elapsed >= self.opts.heartbeat_ticks {
                    self.heartbeat_elapsed = 0;
                    self.broadcast_append();
                }
                if self.election_elapsed >= self.opts.election_ticks {
                    self.election_elapsed = 0;
                    self.check_quorum();
                }
            }
            _ => {
                if self.promotable() && self.election_elapsed >= self.election_timeout {
                    self.start_election();
                }
            }
        }
    }

    /// Whether this node may stand for election: it must be a voter in the
    /// current configuration. A learner that campaigns can only waste time.
    fn promotable(&self) -> bool {
        self.log.config().is_voter(self.id)
    }

    /// Propose a command. Returns the index it will occupy, or `None` if this
    /// node is not the leader.
    pub fn propose(&mut self, data: Vec<u8>) -> Option<Index> {
        if self.role != Role::Leader {
            return None;
        }
        let index = self.log.last_index() + 1;
        let entry = Entry { term: self.term, index, kind: EntryKind::Normal(data) };
        self.append_to_own_log(&[entry]);
        self.broadcast_append();
        Some(index)
    }

    /// Propose a configuration change.
    ///
    /// Refused while a change is already in flight: two overlapping joint
    /// transitions cannot be reasoned about, and the paper's safety argument
    /// assumes one at a time.
    pub fn propose_config(&mut self, change: ConfigChange) -> Option<Index> {
        if self.role != Role::Leader {
            return None;
        }
        let current = self.log.config();
        match &change {
            ConfigChange::EnterJoint { .. } if current.is_joint() => return None,
            ConfigChange::LeaveJoint if !current.is_joint() => return None,
            _ => {}
        }
        // Also refuse if an uncommitted config change is already in the log,
        // even if it has not yet taken us into a joint state.
        if self.log.entries_from(self.commit + 1).iter().any(|e| matches!(e.kind, EntryKind::Config(_)))
        {
            return None;
        }
        let index = self.log.last_index() + 1;
        let entry = Entry { term: self.term, index, kind: EntryKind::Config(change) };
        self.append_to_own_log(&[entry]);
        // Appending a config change may have changed the voter set; make sure
        // every new member has a progress slot before we replicate to them.
        self.reset_progress_for_config();
        self.broadcast_append();
        Some(index)
    }

    /// Begin a linearizable read. The returned context appears in
    /// [`Ready::reads`] once the read is safe to serve.
    pub fn read_index(&mut self) -> Option<u64> {
        if self.role != Role::Leader {
            return None;
        }
        // §6.4: a leader that has not yet committed an entry of its own term
        // does not know the real commit index, so it cannot bound a read.
        if !self.committed_own_term {
            return None;
        }
        self.read_ctx += 1;
        let ctx = self.read_ctx;
        let index = self.commit;

        let cfg = self.log.config();
        // A single-voter cluster is its own quorum; no round trip needed.
        if cfg.has_quorum(&[self.id].into_iter().collect()) {
            self.pending.reads.push(ReadState { ctx, index });
            return Some(ctx);
        }
        let mut acks = BTreeSet::new();
        acks.insert(self.id);
        self.pending_reads.push(PendingRead { ctx, index, acks });
        for peer in cfg.all_nodes() {
            if peer != self.id {
                self.send(peer, Body::HeartbeatReq { commit: self.commit, ctx });
            }
        }
        Some(ctx)
    }

    /// Hand leadership to `target` by telling it to campaign immediately.
    pub fn transfer_leadership(&mut self, target: NodeId) -> bool {
        if self.role != Role::Leader || target == self.id {
            return false;
        }
        if !self.log.config().is_voter(target) {
            return false;
        }
        self.send(target, Body::TimeoutNow);
        true
    }

    /// Feed in a message from a peer.
    pub fn step(&mut self, from: NodeId, msg: Message) {
        // --- the term rule, first and always -----------------------------
        if msg.term > self.term && !msg.is_pre_vote() {
            // A `VoteReq` at a higher term must not clear a vote we are about
            // to grant, so becoming a follower here leaves `vote` to the
            // handler below. Any other message means there is a newer leader.
            let leader = match msg.body {
                Body::VoteReq { .. } | Body::VoteResp { .. } => None,
                _ => Some(from),
            };
            self.become_follower(msg.term, leader);
        }
        if msg.term < self.term {
            match msg.body {
                // Reply so the stale sender learns the current term and steps
                // down, rather than retrying forever.
                Body::AppendReq { .. } | Body::HeartbeatReq { .. } => {
                    self.send(
                        from,
                        Body::AppendResp {
                            success: false,
                            match_index: 0,
                            conflict_index: 0,
                            conflict_term: 0,
                        },
                    );
                }
                Body::PreVoteReq { .. } => {
                    self.send(from, Body::PreVoteResp { granted: false });
                }
                _ => {}
            }
            return;
        }

        match msg.body {
            Body::PreVoteReq { last_index, last_term } => {
                self.handle_pre_vote_req(from, msg.term, last_index, last_term)
            }
            Body::PreVoteResp { granted } => self.handle_pre_vote_resp(from, msg.term, granted),
            Body::VoteReq { last_index, last_term } => {
                self.handle_vote_req(from, last_index, last_term)
            }
            Body::VoteResp { granted } => self.handle_vote_resp(from, granted),
            Body::AppendReq { prev_index, prev_term, entries, commit } => {
                self.handle_append(from, prev_index, prev_term, entries, commit)
            }
            Body::AppendResp { success, match_index, conflict_index, conflict_term } => {
                self.handle_append_resp(from, success, match_index, conflict_index, conflict_term)
            }
            Body::SnapshotReq { snapshot } => self.handle_snapshot(from, snapshot),
            Body::SnapshotResp { success, index } => self.handle_snapshot_resp(from, success, index),
            Body::HeartbeatReq { commit, ctx } => self.handle_heartbeat(from, commit, ctx),
            Body::HeartbeatResp { ctx } => self.handle_heartbeat_resp(from, ctx),
            Body::TimeoutNow => {
                // Campaign at once, skipping pre-vote: the current leader has
                // explicitly stepped aside, so there is nothing to disrupt.
                if self.promotable() {
                    self.campaign(false);
                }
            }
        }
    }

    /// Collect everything the driver must do. The driver calls [`Raft::advance`]
    /// once it has done it.
    pub fn ready(&mut self) -> Ready {
        let mut r = std::mem::take(&mut self.pending);
        if self.hard_state_dirty {
            let hs = self.hard_state();
            if hs != self.persisted_hard_state {
                r.hard_state = Some(hs);
            }
            self.hard_state_dirty = false;
        }
        // Entries the state machine has not seen yet.
        if self.commit > self.applied {
            r.committed = self.log.slice(self.applied + 1, self.commit + 1).to_vec();
        }
        r
    }

    /// Acknowledge that a `Ready` has been fully processed.
    pub fn advance(&mut self, ready: &Ready) {
        if let Some(hs) = ready.hard_state {
            self.persisted_hard_state = hs;
        }
        if let Some(last) = ready.committed.last() {
            self.applied = last.index;
        }
        self.truncate_to = None;
    }

    /// Build a snapshot boundary. The caller supplies the state machine image;
    /// the log supplies the index, term, and configuration it corresponds to.
    pub fn snapshot_at_applied(&self, data: Vec<u8>) -> Option<Snapshot> {
        let index = self.applied;
        let term = self.log.term_at(index)?;
        Some(Snapshot { last_index: index, last_term: term, config: self.log.config(), data })
    }

    /// Discard log entries the snapshot covers.
    pub fn compact(&mut self, snap: &Snapshot) {
        self.log.compact_through(snap.last_index, snap.last_term, snap.config.clone());
    }

    pub fn should_snapshot(&self) -> bool {
        self.applied.saturating_sub(self.log.snapshot_index()) >= self.opts.snapshot_interval
    }

    // --- role transitions ------------------------------------------------

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        if term > self.term {
            self.term = term;
            self.vote = None;
            self.hard_state_dirty = true;
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.election_elapsed = 0;
        self.votes.clear();
        self.progress.clear();
        self.pending_reads.clear();
        self.committed_own_term = false;
    }

    fn start_election(&mut self) {
        self.campaign(self.opts.pre_vote);
    }

    fn campaign(&mut self, pre_vote: bool) {
        // A fresh random timeout for every attempt. Reusing one timeout means
        // two nodes that split a vote will split the next one identically.
        let span = self.opts.election_ticks.max(1) as u64;
        let draw = self.stir();
        self.election_timeout = self.opts.election_ticks + (draw % span) as u32;
        self.election_elapsed = 0;
        self.votes.clear();

        let (last_index, last_term) = (self.log.last_index(), self.log.last_term());
        let cfg = self.log.config();

        if pre_vote {
            self.role = Role::PreCandidate;
            // Poll at term+1 without adopting it — that is the whole point.
            let poll_term = self.term + 1;
            self.votes.insert(self.id, true);
            if self.tally(&cfg) {
                // Single-voter cluster: skip straight to the real election.
                self.campaign(false);
                return;
            }
            for peer in cfg.all_nodes() {
                if peer != self.id {
                    self.pending.messages.push((
                        peer,
                        Message::new(poll_term, Body::PreVoteReq { last_index, last_term }),
                    ));
                }
            }
        } else {
            self.role = Role::Candidate;
            self.term += 1;
            self.vote = Some(self.id);
            self.leader = None;
            self.hard_state_dirty = true;
            self.votes.insert(self.id, true);
            if self.tally(&cfg) {
                self.become_leader();
                return;
            }
            for peer in cfg.all_nodes() {
                if peer != self.id {
                    self.send(peer, Body::VoteReq { last_index, last_term });
                }
            }
        }
    }

    /// Do the granted votes constitute a quorum?
    fn tally(&self, cfg: &Config) -> bool {
        let granted: BTreeSet<NodeId> =
            self.votes.iter().filter(|(_, &g)| g).map(|(id, _)| *id).collect();
        cfg.has_quorum(&granted)
    }

    /// Has this election already failed beyond recovery?
    fn lost(&self, cfg: &Config) -> bool {
        let refused: BTreeSet<NodeId> =
            self.votes.iter().filter(|(_, &g)| !g).map(|(id, _)| *id).collect();
        // If the refusers alone are a quorum, no set of remaining votes can win.
        cfg.has_quorum(&refused)
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader = Some(self.id);
        self.heartbeat_elapsed = 0;
        self.election_elapsed = 0;
        self.quorum_contact_elapsed = 0;
        self.committed_own_term = false;
        self.pending_reads.clear();
        self.reset_progress_for_config();

        // §5.4.2, and the single most commonly omitted line in a hand-rolled
        // Raft: commit a no-op of the new term. Without it, a leader cannot
        // safely advance commitIndex over entries from previous terms, and on
        // an idle cluster there may be no client write to serve that purpose.
        let index = self.log.last_index() + 1;
        let noop = Entry { term: self.term, index, kind: EntryKind::Noop };
        self.append_to_own_log(&[noop]);
        self.broadcast_append();
    }

    fn reset_progress_for_config(&mut self) {
        let cfg = self.log.config();
        let next = self.log.last_index() + 1;
        let members = cfg.all_nodes();
        self.progress.retain(|id, _| members.contains(id));
        for id in members {
            self.progress.entry(id).or_insert_with(|| {
                let mut p = Progress::new(next);
                if id == self.id {
                    // We trivially have our own log.
                    p.matched = self.log.last_index();
                    p.state = ProgressState::Replicate;
                }
                p
            });
        }
        if let Some(p) = self.progress.get_mut(&self.id) {
            p.matched = self.log.last_index();
            p.next = self.log.last_index() + 1;
            p.state = ProgressState::Replicate;
            p.active = true;
        }
    }

    /// A leader that cannot reach a quorum must step down.
    ///
    /// Without this, a leader partitioned into a minority keeps believing it
    /// leads and keeps serving lease reads, long after the majority elected
    /// someone else. §6.2.
    fn check_quorum(&mut self) {
        let cfg = self.log.config();
        let active: BTreeSet<NodeId> = self
            .progress
            .iter()
            .filter(|(id, p)| p.active || **id == self.id)
            .map(|(id, _)| *id)
            .collect();
        for p in self.progress.values_mut() {
            p.active = false;
        }
        if cfg.has_quorum(&active) {
            self.quorum_contact_elapsed = 0;
        } else {
            self.become_follower(self.term, None);
        }
    }

    // --- election handlers -----------------------------------------------

    fn handle_pre_vote_req(
        &mut self,
        from: NodeId,
        poll_term: Term,
        last_index: Index,
        last_term: Term,
    ) {
        // Grant only if we would actually vote: the poll term must be ahead of
        // ours, the log must be sufficiently up to date, and — crucially — we
        // must not currently believe in a live leader. That last condition is
        // what makes pre-vote non-disruptive.
        let believes_in_leader =
            self.leader.is_some() && self.election_elapsed < self.opts.election_ticks;
        let granted = poll_term > self.term
            && self.log.is_up_to_date(last_term, last_index)
            && !believes_in_leader;
        // Answer at the poll's term so the candidate can match it up, but do
        // not adopt that term ourselves.
        self.pending
            .messages
            .push((from, Message::new(poll_term, Body::PreVoteResp { granted })));
    }

    fn handle_pre_vote_resp(&mut self, from: NodeId, resp_term: Term, granted: bool) {
        if self.role != Role::PreCandidate {
            return;
        }
        // A rejection carrying a term above our poll means we are behind.
        if !granted && resp_term > self.term + 1 {
            self.become_follower(resp_term, None);
            return;
        }
        self.votes.insert(from, granted);
        let cfg = self.log.config();
        if self.tally(&cfg) {
            // The poll says we can win. Now hold the real election.
            self.campaign(false);
        } else if self.lost(&cfg) {
            self.role = Role::Follower;
        }
    }

    fn handle_vote_req(&mut self, from: NodeId, last_index: Index, last_term: Term) {
        // At this point the term is equal to ours (higher terms already made us
        // a follower and cleared the vote).
        let can_vote = match self.vote {
            None => true,
            // Idempotent: re-granting to the same candidate is safe and is what
            // makes a duplicated VoteReq harmless.
            Some(v) => v == from,
        };
        let granted = can_vote && self.log.is_up_to_date(last_term, last_index);
        if granted {
            self.vote = Some(from);
            self.hard_state_dirty = true;
            // Only reset the election timer for a vote we actually granted.
            // Resetting on every request lets a node that can never win keep
            // the rest of the cluster from ever timing out.
            self.election_elapsed = 0;
        }
        self.send(from, Body::VoteResp { granted });
    }

    fn handle_vote_resp(&mut self, from: NodeId, granted: bool) {
        if self.role != Role::Candidate {
            return;
        }
        self.votes.insert(from, granted);
        let cfg = self.log.config();
        if self.tally(&cfg) {
            self.become_leader();
        } else if self.lost(&cfg) {
            self.become_follower(self.term, None);
        }
    }

    // --- replication handlers --------------------------------------------

    fn handle_append(
        &mut self,
        from: NodeId,
        prev_index: Index,
        prev_term: Term,
        entries: Vec<Entry>,
        leader_commit: Index,
    ) {
        // A live leader at our term: defer to it and restart the clock.
        if self.role != Role::Follower {
            self.become_follower(self.term, Some(from));
        }
        self.leader = Some(from);
        self.election_elapsed = 0;

        // The leader is behind our snapshot; it will catch up from our reply.
        if prev_index < self.log.snapshot_index() {
            self.send(
                from,
                Body::AppendResp {
                    success: true,
                    match_index: self.log.snapshot_index(),
                    conflict_index: 0,
                    conflict_term: 0,
                },
            );
            return;
        }

        match self.log.term_at(prev_index) {
            Some(t) if t == prev_term => {}
            Some(t) => {
                // We have that index but with a different term. Tell the leader
                // where our log's term changes so it can skip a whole term per
                // round trip rather than backing up one index at a time.
                let hint = self.log.find_conflict_by_term(prev_index, t);
                self.send(
                    from,
                    Body::AppendResp {
                        success: false,
                        match_index: 0,
                        conflict_index: hint,
                        conflict_term: t,
                    },
                );
                return;
            }
            None => {
                // Our log is too short.
                self.send(
                    from,
                    Body::AppendResp {
                        success: false,
                        match_index: 0,
                        conflict_index: self.log.last_index() + 1,
                        conflict_term: 0,
                    },
                );
                return;
            }
        }

        let last_new = self.log.merge(prev_index, &entries);
        if !entries.is_empty() {
            // Whatever the merge kept or replaced, the durable copy must match.
            self.truncate_to = Some(entries[0].index);
            self.pending.entries = self.log.entries_from(entries[0].index).to_vec();
        }

        // §5.3: never commit past what we actually hold.
        let new_commit = leader_commit.min(last_new);
        if new_commit > self.commit {
            self.commit = new_commit;
            self.hard_state_dirty = true;
        }

        self.send(
            from,
            Body::AppendResp {
                success: true,
                match_index: last_new,
                conflict_index: 0,
                conflict_term: 0,
            },
        );
    }

    fn handle_append_resp(
        &mut self,
        from: NodeId,
        success: bool,
        match_index: Index,
        conflict_index: Index,
        conflict_term: Term,
    ) {
        if self.role != Role::Leader {
            return;
        }
        let last = self.log.last_index();
        let Some(p) = self.progress.get_mut(&from) else { return };
        p.active = true;

        if success {
            if match_index >= p.matched {
                p.matched = match_index;
                p.next = match_index + 1;
                p.state = ProgressState::Replicate;
                p.pending_snapshot = 0;
            }
            self.maybe_advance_commit();
            // Keep streaming if this follower is still behind.
            if self.progress.get(&from).map(|p| p.matched < last).unwrap_or(false) {
                self.send_append_to(from);
            }
            return;
        }

        // Rejected. Use the follower's hint to jump back.
        let next = if conflict_term > 0 {
            // Find our last entry in a term at or below the follower's
            // conflicting term. If we have that term, we can resume just after
            // our last entry in it.
            let probe = self.log.find_conflict_by_term(conflict_index, conflict_term);
            probe + 1
        } else {
            conflict_index.max(1)
        };
        p.next = next.max(p.matched + 1).min(last + 1);
        p.state = ProgressState::Probe;
        self.send_append_to(from);
    }

    fn handle_snapshot(&mut self, from: NodeId, snapshot: Snapshot) {
        if self.role != Role::Follower {
            self.become_follower(self.term, Some(from));
        }
        self.leader = Some(from);
        self.election_elapsed = 0;

        if snapshot.last_index <= self.commit {
            // We already have everything it covers.
            self.send(from, Body::SnapshotResp { success: true, index: self.commit });
            return;
        }
        let index = snapshot.last_index;
        self.log.install_snapshot(&snapshot);
        self.commit = self.commit.max(index);
        self.applied = index;
        self.hard_state_dirty = true;
        self.pending.snapshot = Some(snapshot);
        self.send(from, Body::SnapshotResp { success: true, index });
    }

    fn handle_snapshot_resp(&mut self, from: NodeId, success: bool, index: Index) {
        if self.role != Role::Leader {
            return;
        }
        let Some(p) = self.progress.get_mut(&from) else { return };
        p.active = true;
        if success {
            p.matched = p.matched.max(index);
            p.next = p.matched + 1;
            p.state = ProgressState::Probe;
            p.pending_snapshot = 0;
            self.maybe_advance_commit();
        } else {
            p.pending_snapshot = 0;
            p.state = ProgressState::Probe;
        }
    }

    fn handle_heartbeat(&mut self, from: NodeId, commit: Index, ctx: u64) {
        if self.role != Role::Follower {
            self.become_follower(self.term, Some(from));
        }
        self.leader = Some(from);
        self.election_elapsed = 0;
        // Only adopt a commit index we can actually back with entries.
        let safe = commit.min(self.log.last_index());
        if safe > self.commit {
            self.commit = safe;
            self.hard_state_dirty = true;
        }
        self.send(from, Body::HeartbeatResp { ctx });
    }

    fn handle_heartbeat_resp(&mut self, from: NodeId, ctx: u64) {
        if self.role != Role::Leader {
            return;
        }
        if let Some(p) = self.progress.get_mut(&from) {
            p.active = true;
        }
        let cfg = self.log.config();
        let mut ready = Vec::new();
        for pending in &mut self.pending_reads {
            if pending.ctx == ctx || ctx == 0 {
                pending.acks.insert(from);
                if cfg.has_quorum(&pending.acks) {
                    ready.push(ReadState { ctx: pending.ctx, index: pending.index });
                }
            }
        }
        if !ready.is_empty() {
            let done: BTreeSet<u64> = ready.iter().map(|r| r.ctx).collect();
            self.pending_reads.retain(|p| !done.contains(&p.ctx));
            self.pending.reads.extend(ready);
            self.quorum_contact_elapsed = 0;
        }
    }

    // --- leader machinery ------------------------------------------------

    fn append_to_own_log(&mut self, entries: &[Entry]) {
        self.log.append(entries);
        self.pending.entries.extend_from_slice(entries);
        if let Some(p) = self.progress.get_mut(&self.id) {
            p.matched = self.log.last_index();
            p.next = p.matched + 1;
        }
        // A single-voter cluster commits the moment it appends. Without this,
        // a one-node cluster never makes progress, since nothing else will ever
        // acknowledge.
        self.maybe_advance_commit();
    }

    fn broadcast_append(&mut self) {
        for peer in self.log.config().all_nodes() {
            if peer != self.id {
                self.send_append_to(peer);
            }
        }
    }

    fn send_append_to(&mut self, peer: NodeId) {
        let Some(p) = self.progress.get(&peer).cloned() else { return };
        if p.state == ProgressState::Snapshot && p.pending_snapshot > 0 {
            return; // already shipping one
        }

        let prev_index = p.next.saturating_sub(1);
        let Some(prev_term) = self.log.term_at(prev_index) else {
            // The entries this follower needs have been compacted away. It
            // needs a snapshot, which only the driver can produce — flag it and
            // let the driver call `send_snapshot`.
            if let Some(p) = self.progress.get_mut(&peer) {
                p.state = ProgressState::Snapshot;
            }
            return;
        };

        let from = p.next;
        let to = (from + self.opts.max_entries_per_append as u64).min(self.log.last_index() + 1);
        let mut entries: Vec<Entry> = self.log.slice(from, to).to_vec();

        // Byte budget, so one enormous entry cannot produce a message that will
        // never fit through the network.
        let mut bytes = 0usize;
        let mut cut = entries.len();
        for (i, e) in entries.iter().enumerate() {
            bytes += e.to_bytes().len();
            if bytes > self.opts.max_bytes_per_append && i > 0 {
                cut = i;
                break;
            }
        }
        entries.truncate(cut);

        self.send(
            peer,
            Body::AppendReq { prev_index, prev_term, entries, commit: self.commit },
        );
    }

    /// Which followers need a snapshot because their entries are compacted away.
    pub fn followers_needing_snapshot(&self) -> Vec<NodeId> {
        self.progress
            .iter()
            .filter(|(id, p)| {
                **id != self.id
                    && p.state == ProgressState::Snapshot
                    && p.pending_snapshot == 0
            })
            .map(|(id, _)| *id)
            .collect()
    }

    /// Ship a snapshot to a follower that has fallen off the back of the log.
    pub fn send_snapshot(&mut self, peer: NodeId, snapshot: Snapshot) {
        if let Some(p) = self.progress.get_mut(&peer) {
            p.pending_snapshot = snapshot.last_index;
            p.state = ProgressState::Snapshot;
        }
        self.send(peer, Body::SnapshotReq { snapshot });
    }

    /// Advance `commitIndex` to the highest index replicated on a quorum —
    /// but only through an entry of the current term.
    ///
    /// The term check is §5.4.2 / figure 8, and omitting it is the classic
    /// hand-rolled-Raft bug. An entry from an earlier term replicated on a
    /// majority is *not* committed: a future leader that lacks it can still be
    /// elected, and would overwrite it. Only committing it indirectly, via a
    /// current-term entry above it, is safe.
    fn maybe_advance_commit(&mut self) {
        let cfg = self.log.config();
        let matched = |id: NodeId| {
            if id == self.id {
                self.log.last_index()
            } else {
                self.progress.get(&id).map(|p| p.matched).unwrap_or(0)
            }
        };
        let candidate = cfg.quorum_index(matched);
        if candidate <= self.commit {
            return;
        }
        if self.log.term_at(candidate) != Some(self.term) {
            return;
        }
        self.commit = candidate;
        self.committed_own_term = true;
        self.hard_state_dirty = true;
        self.quorum_contact_elapsed = 0;
    }

    /// Once `EnterJoint` commits, the leader must propose `LeaveJoint` to
    /// finish the transition. Called by the driver after applying entries.
    pub fn maybe_finish_config_change(&mut self) -> Option<Index> {
        if self.role != Role::Leader {
            return None;
        }
        let cfg = self.log.config();
        if !cfg.is_joint() {
            return None;
        }
        // Only once the joint configuration itself is committed — leaving early
        // would abandon C_old before it is guaranteed to be part of history.
        let joint_committed = self
            .log
            .slice(self.log.snapshot_index() + 1, self.commit + 1)
            .iter()
            .any(|e| matches!(&e.kind, EntryKind::Config(ConfigChange::EnterJoint { .. })));
        if !joint_committed {
            return None;
        }
        self.propose_config(ConfigChange::LeaveJoint)
    }

    fn send(&mut self, to: NodeId, body: Body) {
        self.pending.messages.push((to, Message::new(self.term, body)));
    }
}
