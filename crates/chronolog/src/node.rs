//! The driver: everything that turns a pure state machine into a server.
//!
//! This is the only file in `chronolog` that performs I/O, and it does so
//! exclusively through [`Host`]. That is the whole architectural bet — this
//! same code runs against the simulator's virtual world and against real
//! sockets and real `fsync`, unchanged.
//!
//! # The loop
//!
//! ```text
//! wait for one event
//! drain every other event already queued      <- this is group commit
//! feed them all to Raft
//! ready = raft.ready()
//!   persist + fsync   (hard state, entries, snapshot)
//!   send messages     (only now — never before the fsync)
//!   apply committed   (and answer clients)
//! raft.advance(ready)
//! ```
//!
//! The drain step is the entire throughput story. One event per iteration
//! would mean one `fsync` per proposal, and `fsync` is a millisecond. Draining
//! the queue turns a burst of a thousand proposals into a single append and a
//! single durability barrier, which is why the target is 100k writes/sec
//! rather than 1k.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono_sim::time::Nanos;
use chrono_sim::traits::{Envelope, Host, NodeId};

use crate::chan::Chan;
use crate::client::{Op, Outcome, ReadMode, Request, Response};
use crate::kv::KvStore;
use crate::msg::{Message, Wire};
use crate::raft::{Raft, RaftOptions, Ready, Role};
use crate::types::{Config, ConfigChange, Entry, EntryKind, Index, Snapshot};
use crate::wal::{Wal, WalOptions};

#[derive(Clone, Debug)]
pub struct NodeOptions {
    pub raft: RaftOptions,
    pub wal: WalOptions,
    /// Wall time per Raft logical tick. Election timeouts are expressed in
    /// ticks, so this sets the real-world election latency:
    /// `tick_interval * election_ticks`.
    pub tick_interval: Nanos,
    /// The cluster's initial membership, used only when there is nothing on
    /// disk to recover.
    pub bootstrap: Config,
    /// Publish the per-index log terms and applied-history digest that the
    /// Raft-invariant oracles need.
    ///
    /// Off by default because it copies the log on every driver cycle. That is
    /// fine for a simulation whose whole point is to be inspected, and wasteful
    /// for a production server that nobody is checking Log Matching on.
    pub inspect: bool,
}

impl Default for NodeOptions {
    fn default() -> Self {
        Self {
            raft: RaftOptions::default(),
            wal: WalOptions::default(),
            tick_interval: Nanos::from_millis(20),
            bootstrap: Config::default(),
            inspect: false,
        }
    }
}

/// What the driver loop reacts to.
enum Event {
    Tick,
    Wire { from: NodeId, msg: Message },
    Client { from: NodeId, req: Request },
}

/// Counters, exported as Prometheus metrics by the server and asserted on by
/// the oracles.
#[derive(Debug, Default)]
pub struct NodeMetrics {
    pub proposals: AtomicU64,
    pub commits: AtomicU64,
    pub applies: AtomicU64,
    pub batches: AtomicU64,
    pub batched_entries: AtomicU64,
    pub fsyncs: AtomicU64,
    pub elections_started: AtomicU64,
    pub leadership_gained: AtomicU64,
    pub leadership_lost: AtomicU64,
    pub snapshots_taken: AtomicU64,
    pub snapshots_installed: AtomicU64,
    pub client_requests: AtomicU64,
    pub not_leader_redirects: AtomicU64,
    pub reads_linearizable: AtomicU64,
    pub reads_lease: AtomicU64,
    pub reads_stale: AtomicU64,
}

impl NodeMetrics {
    fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(&self, name: &str) -> u64 {
        let c = match name {
            "proposals" => &self.proposals,
            "commits" => &self.commits,
            "applies" => &self.applies,
            "batches" => &self.batches,
            "batched_entries" => &self.batched_entries,
            "fsyncs" => &self.fsyncs,
            "elections_started" => &self.elections_started,
            "leadership_gained" => &self.leadership_gained,
            "leadership_lost" => &self.leadership_lost,
            "snapshots_taken" => &self.snapshots_taken,
            "snapshots_installed" => &self.snapshots_installed,
            "client_requests" => &self.client_requests,
            "not_leader_redirects" => &self.not_leader_redirects,
            "reads_linearizable" => &self.reads_linearizable,
            "reads_lease" => &self.reads_lease,
            "reads_stale" => &self.reads_stale,
            _ => return 0,
        };
        c.load(Ordering::Relaxed)
    }

    /// Mean entries per durability barrier — the group-commit ratio, and the
    /// single number that says whether batching is working.
    pub fn batch_ratio(&self) -> f64 {
        let b = self.batches.load(Ordering::Relaxed);
        if b == 0 {
            0.0
        } else {
            self.batched_entries.load(Ordering::Relaxed) as f64 / b as f64
        }
    }
}

/// A handle to a running node, for tests and the `/debug` endpoint.
#[derive(Clone, Debug)]
pub struct NodeHandle {
    pub metrics: Arc<NodeMetrics>,
    pub state: Arc<std::sync::Mutex<PublicState>>,
}

/// A snapshot of what the node believes, safe to read from outside the driver.
#[derive(Clone, Debug, Default)]
pub struct PublicState {
    pub node: NodeId,
    /// Which process lifetime this state belongs to. Bumped by the observer
    /// each time the node boots.
    ///
    /// Invariants like "the commit index never moves backwards" hold *within* a
    /// process, not across a crash: Raft persists the commit index only as an
    /// optimization, and a node that loses its un-fsynced tail to a torn write
    /// legitimately comes back with a lower one. The leader refills it. Without
    /// this field an oracle cannot tell that regression from a real one.
    pub generation: u64,
    pub role: &'static str,
    pub term: u64,
    pub leader: Option<NodeId>,
    pub commit_index: Index,
    pub applied_index: Index,
    pub last_index: Index,
    pub snapshot_index: Index,
    pub config: String,
    pub keys: usize,
    pub wal_segments: usize,
    pub wal_bytes: u64,
    /// Whether the process is running. Set by the observer, not the node —
    /// a crashed node cannot update its own state, and its last published
    /// values linger. A liveness oracle that trusts them concludes the cluster
    /// is stuck at whatever index the dead node last reported.
    pub up: bool,
    /// Set if the driver loop terminated. A node whose driver has stopped is
    /// the worst kind of failure: still up, still accepting connections, still
    /// reporting its last known state, and never making progress again. Making
    /// it visible is the difference between a five-minute diagnosis and an
    /// afternoon.
    pub driver_error: Option<String>,
    /// `(index, term)` for every entry still in the log. Populated only when
    /// `NodeOptions::inspect` is set. This is what the Log Matching oracle
    /// compares across nodes.
    pub log_terms: Vec<(Index, u64)>,
    /// Rolling digest of everything this node's state machine has applied,
    /// sampled periodically as `(applied_index, digest)`.
    ///
    /// Two nodes that applied the same sequence must agree at every shared
    /// checkpoint. Comparing digests rather than whole histories keeps this
    /// O(1) per apply instead of O(n) per check — which matters, because the
    /// oracle runs continuously.
    pub apply_checkpoints: Vec<(Index, u64)>,
}

/// Start a node. Returns immediately; the work happens in spawned tasks.
pub fn start(host: Host, opts: NodeOptions) -> NodeHandle {
    let handle = NodeHandle {
        metrics: Arc::new(NodeMetrics::default()),
        state: Arc::new(std::sync::Mutex::new(PublicState {
            node: host.node,
            role: "follower",
            ..Default::default()
        })),
    };
    let h = handle.clone();
    host.spawn_with("chronolog", move |host| async move {
        if let Err(e) = run(host.clone(), opts, h.clone()).await {
            let msg = e.to_string();
            host.note(|| format!("DRIVER STOPPED: {msg}"));
            h.state.lock().unwrap().driver_error = Some(msg);
        }
    });
    handle
}

async fn run(host: Host, opts: NodeOptions, handle: NodeHandle) -> std::io::Result<()> {
    // --- recover ---------------------------------------------------------
    let recovered = Wal::open(host.clone(), opts.wal.clone()).await?;
    let mut wal = recovered.wal;
    if !recovered.tail.is_clean() {
        host.note(|| {
            format!(
                "recovery: log tail was {:?}, {} entries discarded",
                recovered.tail, recovered.truncated
            )
        });
    }
    let mut kv = match &recovered.snapshot {
        Some(s) => KvStore::restore(&s.data).unwrap_or_else(|_| KvStore::new()),
        None => KvStore::new(),
    };
    let mut raft = Raft::restore(
        host.node,
        opts.raft.clone(),
        recovered.hard_state,
        recovered.snapshot.as_ref(),
        recovered.entries,
        opts.bootstrap.clone(),
    );
    host.note(|| {
        format!(
            "recovered: term={} vote={:?} commit={} last={} config={}",
            raft.term(),
            raft.vote(),
            raft.commit_index(),
            raft.last_index(),
            raft.config()
        )
    });

    // --- feeders ---------------------------------------------------------
    let events: Chan<Event> = Chan::new();

    let net_tx = events.clone();
    host.spawn_with("net-rx", |h| async move {
        while let Some(env) = h.net.recv().await {
            match decode(&env) {
                // A frame that does not decode is corruption or a stray packet.
                // Dropping it is correct: Raft treats a lost message as normal,
                // so there is nothing to recover and nothing to report.
                None => continue,
                Some(ev) => net_tx.send(ev),
            }
        }
        net_tx.close();
    });

    let tick_tx = events.clone();
    let interval = opts.tick_interval;
    host.spawn_with("ticker", |h| async move {
        loop {
            h.sleep(interval).await;
            tick_tx.send(Event::Tick);
        }
    });

    // --- driver ----------------------------------------------------------
    let mut pending_writes: BTreeMap<Index, (NodeId, Request)> = BTreeMap::new();
    let mut pending_reads: BTreeMap<u64, Vec<(NodeId, Request)>> = BTreeMap::new();
    let mut was_leader = false;
    let mut digest = ApplyDigest::new();

    while let Some(first) = events.recv().await {
        let mut batch = vec![first];
        // Drain whatever else is already queued. This is group commit: the
        // whole burst becomes one append and one fsync.
        while let Some(ev) = events.try_recv() {
            batch.push(ev);
            if batch.len() >= 4096 {
                break;
            }
        }

        for ev in batch {
            match ev {
                Event::Tick => {
                    let before = raft.role();
                    raft.tick(host.rng.next_u64());
                    if before == Role::Follower && raft.role() != Role::Follower {
                        NodeMetrics::inc(&handle.metrics.elections_started);
                    }
                }
                Event::Wire { from, msg } => raft.step(from, msg),
                Event::Client { from, req } => {
                    NodeMetrics::inc(&handle.metrics.client_requests);
                    handle_client(
                        &host,
                        &mut raft,
                        &mut kv,
                        &handle,
                        &mut pending_writes,
                        &mut pending_reads,
                        from,
                        req,
                    );
                }
            }
        }

        // --- leadership transitions ---------------------------------------
        let is_leader = raft.is_leader();
        if is_leader && !was_leader {
            NodeMetrics::inc(&handle.metrics.leadership_gained);
            host.note(|| format!("became leader in term {}", raft.term()));
        }
        if !is_leader && was_leader {
            NodeMetrics::inc(&handle.metrics.leadership_lost);
            host.note(|| format!("lost leadership in term {}", raft.term()));
            // Anything we accepted but never committed will never be answered
            // by us. Telling the client to look elsewhere is far better than
            // leaving it to time out — and it is honest: we genuinely do not
            // know whether those entries survived.
            for (_, (client, req)) in std::mem::take(&mut pending_writes) {
                reply(&host, client, req, Outcome::NotLeader { hint: raft.leader() });
            }
            for (_, waiting) in std::mem::take(&mut pending_reads) {
                for (client, req) in waiting {
                    reply(&host, client, req, Outcome::NotLeader { hint: raft.leader() });
                }
            }
        }
        was_leader = is_leader;

        // --- the Ready cycle ----------------------------------------------
        let truncate_to = raft.truncate_to();
        let ready = raft.ready();
        persist(&host, &handle, &mut wal, &mut kv, &ready, truncate_to).await?;

        // Reconcile the durable log with memory.
        //
        // The `Ready` watermark says which entries Raft *believes* need
        // writing, and threading that perfectly through every mutation —
        // append, merge, truncate, snapshot install, local compaction — is
        // exactly the kind of bookkeeping that goes subtly wrong. It did, three
        // separate ways, and each time the symptom appeared thousands of events
        // later as a follower that silently stopped replicating.
        //
        // So the watermark is treated as a fast path, not as the contract. The
        // contract is simply: **the WAL mirrors the log**. Checking that
        // directly costs two integer comparisons per cycle and cannot be got
        // wrong by a path nobody thought about.
        reconcile(&host, &handle, &mut wal, &raft).await?;

        // Only now, with everything durable, may anything be sent.
        for (to, msg) in &ready.messages {
            host.net.send(*to, Wire::Raft(msg.clone()).encode());
        }

        apply_committed(
            &host,
            &handle,
            &mut kv,
            &ready,
            &mut pending_writes,
            &mut digest,
        );

        for read in &ready.reads {
            if let Some(waiting) = pending_reads.remove(&read.ctx) {
                for (client, req) in waiting {
                    // The read index is the commit index observed when the read
                    // began. It is only safe to answer once the state machine
                    // has caught up to it; by this point in the cycle it has,
                    // because `apply_committed` ran above.
                    let outcome = serve_read(&kv, &req);
                    let _ = read.index;
                    reply(&host, client, req, outcome);
                }
            }
        }

        raft.advance(&ready);

        // --- housekeeping --------------------------------------------------
        if raft.is_leader() {
            if let Some(idx) = raft.maybe_finish_config_change() {
                host.note(|| format!("proposed LeaveJoint at index {idx}"));
            }
            let needy = raft.followers_needing_snapshot();
            if !needy.is_empty() {
                if let Some(snap) = raft.snapshot_at_applied(kv.snapshot()) {
                    for peer in needy {
                        host.note(|| {
                            format!("shipping snapshot @{} to n{peer}", snap.last_index)
                        });
                        raft.send_snapshot(peer, snap.clone());
                    }
                }
            }
        }

        if raft.should_snapshot() {
            if let Some(snap) = raft.snapshot_at_applied(kv.snapshot()) {
                wal.save_snapshot(&snap).await?;
                raft.compact(&snap);
                wal.compact_through(snap.last_index).await?;
                NodeMetrics::inc(&handle.metrics.snapshots_taken);
                host.note(|| format!("snapshot @{} taken, log compacted", snap.last_index));
            }
        }

        publish(&handle, &raft, &kv, &wal, &digest, opts.inspect);
    }
    Ok(())
}

/// Make the durable log match the in-memory log, whatever route either took.
async fn reconcile(
    host: &Host,
    handle: &NodeHandle,
    wal: &mut Wal,
    raft: &Raft,
) -> std::io::Result<()> {
    let log = raft.log();
    let (want_first, want_last) = (log.first_index(), log.last_index());
    if wal.last_index() == want_last && wal.last_index() + 1 >= want_first {
        return Ok(());
    }

    // The WAL is ahead, or its entries no longer connect to what the log holds.
    // Either way the only sound move is to restart it at the log's base.
    if wal.last_index() > want_last || wal.last_index() + 1 < want_first {
        wal.reset_to(want_first.saturating_sub(1)).await?;
    }

    let from = wal.last_index() + 1;
    let missing = log.entries_from(from).to_vec();
    if missing.is_empty() {
        return Ok(());
    }
    host.note(|| {
        format!("reconciling durable log: writing {}..={}", from, want_last)
    });
    wal.append(&missing).await?;
    NodeMetrics::add(&handle.metrics.batched_entries, missing.len() as u64);
    NodeMetrics::inc(&handle.metrics.batches);
    wal.sync().await?;
    NodeMetrics::inc(&handle.metrics.fsyncs);
    Ok(())
}

/// Persist everything in a `Ready`, then fsync exactly once.
async fn persist(
    host: &Host,
    handle: &NodeHandle,
    wal: &mut Wal,
    kv: &mut KvStore,
    ready: &Ready,
    truncate_to: Option<Index>,
) -> std::io::Result<()> {
    // A conflicting suffix must leave the durable log before the replacement
    // enters it, or a crash in between would leave both.
    //
    // Note this runs even when there is nothing to append. A merge can truncate
    // the log without producing any new entries — the leader rejected our tail
    // and sent nothing to replace it yet — and skipping the truncation there
    // leaves the durable log longer than memory, so the next append collides
    // with entries that no longer exist.
    if let Some(from) = truncate_to {
        wal.truncate_from(from).await?;
    }
    if !ready.needs_durability() {
        return Ok(());
    }
    if let Some(snap) = &ready.snapshot {
        wal.save_snapshot(snap).await?;
        match KvStore::restore(&snap.data) {
            Ok(restored) => *kv = restored,
            Err(e) => host.note(|| format!("snapshot payload did not decode: {e}")),
        }
        // `reset_to`, not `compact_through`. A snapshot accepted from the
        // leader replaces this node's log wholesale, so the WAL has to restart
        // at the snapshot point rather than merely dropping superseded
        // segments — otherwise the next append writes a gap the log can never
        // recover across. See `Wal::reset_to`.
        wal.reset_to(snap.last_index).await?;
        NodeMetrics::inc(&handle.metrics.snapshots_installed);
    }
    if let Some(hs) = ready.hard_state {
        wal.save_hard_state(hs).await?;
    }
    if !ready.entries.is_empty() {
        wal.append(&ready.entries).await?;
        NodeMetrics::add(&handle.metrics.batched_entries, ready.entries.len() as u64);
        NodeMetrics::inc(&handle.metrics.batches);
    }
    wal.sync().await?;
    NodeMetrics::inc(&handle.metrics.fsyncs);
    Ok(())
}

fn apply_committed(
    host: &Host,
    handle: &NodeHandle,
    kv: &mut KvStore,
    ready: &Ready,
    pending_writes: &mut BTreeMap<Index, (NodeId, Request)>,
    digest: &mut ApplyDigest,
) {
    for entry in &ready.committed {
        NodeMetrics::inc(&handle.metrics.commits);
        digest.feed(entry);
        match &entry.kind {
            EntryKind::Noop => {}
            EntryKind::Config(change) => {
                host.note(|| format!("applied config change {change:?} at {}", entry.index));
            }
            EntryKind::Normal(data) => {
                let Ok(req) = Request::decode(data) else {
                    // A committed entry that does not decode is a bug, not a
                    // fault: it was written by this code and checksummed on the
                    // way in. Skipping keeps the replica alive and consistent
                    // with its peers, which will skip it identically.
                    host.note(|| format!("undecodable command at index {}", entry.index));
                    continue;
                };
                let outcome = kv.apply(entry.index, &req);
                NodeMetrics::inc(&handle.metrics.applies);
                if let Some((client, original)) = pending_writes.remove(&entry.index) {
                    // Guard against the index being reused by a different
                    // entry after a leader change: only answer if it is
                    // genuinely the request we accepted.
                    if original.client_id == req.client_id && original.seq == req.seq {
                        reply(host, client, original, outcome);
                    } else {
                        reply(
                            host,
                            client,
                            original,
                            Outcome::NotLeader { hint: None },
                        );
                    }
                }
            }
        }
    }
}

fn handle_client(
    host: &Host,
    raft: &mut Raft,
    kv: &mut KvStore,
    handle: &NodeHandle,
    pending_writes: &mut BTreeMap<Index, (NodeId, Request)>,
    pending_reads: &mut BTreeMap<u64, Vec<(NodeId, Request)>>,
    from: NodeId,
    req: Request,
) {
    // A stale read is the only thing a non-leader will answer.
    if let Op::Get { mode: ReadMode::Stale, .. } = &req.op {
        NodeMetrics::inc(&handle.metrics.reads_stale);
        let outcome = serve_read(kv, &req);
        reply(host, from, req, outcome);
        return;
    }
    if let Op::GetAt { .. } = &req.op {
        let outcome = serve_read(kv, &req);
        reply(host, from, req, outcome);
        return;
    }

    if !raft.is_leader() {
        NodeMetrics::inc(&handle.metrics.not_leader_redirects);
        reply(host, from, req, Outcome::NotLeader { hint: raft.leader() });
        return;
    }

    match &req.op {
        Op::Get { mode: ReadMode::Lease, .. } if raft.lease_valid() => {
            NodeMetrics::inc(&handle.metrics.reads_lease);
            let outcome = serve_read(kv, &req);
            reply(host, from, req, outcome);
        }
        Op::Get { .. } => {
            NodeMetrics::inc(&handle.metrics.reads_linearizable);
            match raft.read_index() {
                Some(ctx) => pending_reads.entry(ctx).or_default().push((from, req)),
                // A leader that has not yet committed in its own term cannot
                // bound a read. Telling the client to retry is the honest
                // answer; serving from local state would not be linearizable.
                None => reply(host, from, req, Outcome::Unavailable),
            }
        }
        _ => {
            let data = req.encode();
            match raft.propose(data) {
                Some(index) => {
                    NodeMetrics::inc(&handle.metrics.proposals);
                    pending_writes.insert(index, (from, req));
                }
                None => reply(host, from, req, Outcome::NotLeader { hint: raft.leader() }),
            }
        }
    }
}

fn serve_read(kv: &KvStore, req: &Request) -> Outcome {
    match &req.op {
        Op::Get { key, .. } => Outcome::Value(kv.get(key).map(|v| v.to_vec())),
        Op::GetAt { key, version } => Outcome::Value(kv.get_at(key, *version).map(|v| v.to_vec())),
        _ => Outcome::Unavailable,
    }
}

fn reply(host: &Host, to: NodeId, req: Request, outcome: Outcome) {
    let resp = Response { client_id: req.client_id, seq: req.seq, outcome };
    host.net.send(to, Wire::Reply(resp).encode());
}

fn decode(env: &Envelope) -> Option<Event> {
    match Wire::decode(&env.payload).ok()? {
        Wire::Raft(msg) => Some(Event::Wire { from: env.from, msg }),
        Wire::Client(req) => Some(Event::Client { from: env.from, req }),
        // A node does not act on replies; only clients do.
        Wire::Reply(_) => None,
    }
}

/// A digest of what the state machine applied, one entry at a time.
///
/// Deliberately **not** a rolling hash over the applied history. A cumulative
/// chain is the obvious design and it is wrong here: a node caught up by
/// snapshot genuinely did not apply the entries the snapshot covers, so its
/// chain restarts and can never again match a peer that applied them all. The
/// State Machine Safety oracle would then report a violation every single time
/// a follower is caught up the fast way — a false positive on the most routine
/// event in the system.
///
/// Hashing each entry independently keeps the comparison meaningful across any
/// two nodes at any shared index, whatever route each took to get there. The
/// "same prefix" half of the property is Log Matching's job, and that oracle
/// checks it directly.
#[derive(Debug, Default)]
pub struct ApplyDigest {
    checkpoints: Vec<(Index, u64)>,
}

impl ApplyDigest {
    pub fn new() -> ApplyDigest {
        ApplyDigest::default()
    }

    pub fn feed(&mut self, entry: &Entry) {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        let mut mix = |v: u64| {
            for i in 0..8 {
                h ^= (v >> (i * 8)) & 0xFF;
                h = h.wrapping_mul(0x0000_0100_0000_01b3);
            }
        };
        mix(entry.index);
        mix(entry.term);
        match &entry.kind {
            EntryKind::Noop => mix(1),
            EntryKind::Config(_) => mix(2),
            EntryKind::Normal(d) => {
                mix(3);
                for b in d {
                    mix(*b as u64);
                }
            }
        }
        self.checkpoints.push((entry.index, h));
        // Bounded: the oracle only needs a recent overlapping window to compare.
        if self.checkpoints.len() > 4096 {
            self.checkpoints.drain(..2048);
        }
    }
}

fn publish(
    handle: &NodeHandle,
    raft: &Raft,
    kv: &KvStore,
    wal: &Wal,
    digest: &ApplyDigest,
    inspect: bool,
) {
    let mut s = handle.state.lock().unwrap();
    s.node = raft.id;
    s.role = match raft.role() {
        Role::Follower => "follower",
        Role::PreCandidate => "pre-candidate",
        Role::Candidate => "candidate",
        Role::Leader => "leader",
    };
    s.term = raft.term();
    s.leader = raft.leader();
    s.commit_index = raft.commit_index();
    s.applied_index = raft.applied_index();
    s.last_index = raft.last_index();
    s.snapshot_index = raft.log().snapshot_index();
    s.config = raft.config().to_string();
    s.keys = kv.len();
    s.wal_segments = wal.segment_count();
    s.wal_bytes = wal.total_bytes();
    if inspect {
        s.log_terms = raft
            .log()
            .entries_from(raft.log().first_index())
            .iter()
            .map(|e| (e.index, e.term))
            .collect();
        s.apply_checkpoints = digest.checkpoints.clone();
    }
}

/// Propose a membership change on whichever node is the leader. Exposed for
/// the CLI and tests; the driver picks it up like any other proposal.
pub fn encode_config_change(change: &ConfigChange) -> Entry {
    Entry { term: 0, index: 0, kind: EntryKind::Config(change.clone()) }
}

/// Build the snapshot a node would take right now. Exposed for tests.
pub fn snapshot_of(raft: &Raft, kv: &KvStore) -> Option<Snapshot> {
    raft.snapshot_at_applied(kv.snapshot())
}
