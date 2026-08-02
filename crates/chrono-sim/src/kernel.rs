//! The simulation kernel: one thread, one clock, one PRNG, one truth.
//!
//! # The loop
//!
//! ```text
//! loop {
//!     if any task is runnable -> poll one, chosen by the PRNG
//!     else                    -> jump virtual time to the next scheduled event and fire it
//!     if neither              -> the run is over (quiesced, or deadlocked)
//! }
//! ```
//!
//! Two consequences fall out of those four lines, and they are the whole
//! project.
//!
//! *Virtual time jumps.* Nothing ever waits. A 30-second election timeout
//! costs nothing but a heap pop, so thousands of simulated node-hours fit in
//! seconds of wall clock.
//!
//! *Interleaving is a PRNG draw.* Thread scheduling is not something the OS
//! does to us, it is something we decide — so it is reproducible. Given the
//! seed, the same task is polled in the same order, every time, forever.
//!
//! # The rule that keeps it honest
//!
//! No kernel method may invoke a `Waker` while holding the inner lock. Wakers
//! re-enter the kernel, and a re-entrant lock is a deadlock. Every method here
//! collects wakers into a `Vec`, drops the guard, and only then wakes. Deviate
//! from this and the simulator hangs in a way that looks like a bug in the
//! system under test.

use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
use std::cmp::{Ordering, Reverse};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use crate::fault::FaultPolicy;
use crate::rng::{Rng, RngExt, SeededRng};
use crate::time::Nanos;
use crate::trace::{DropReason, Event, Fnv, Recorder, TraceMode};
use crate::traits::{
    BoxFuture, Clock, Envelope, File, Host, Network, NodeId, Spawner, Storage, Tracer,
};

pub type TaskId = u64;

/// Servers are subject to chaos. Clients are not: a workload generator that
/// crashes mid-run tells you nothing about the system under test.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Server,
    Client,
}

// ---------------------------------------------------------------------------
// Timers
// ---------------------------------------------------------------------------

/// Shared state between a pending future and the timer that will complete it.
pub(crate) struct TimerSlot {
    fired: bool,
    waker: Option<Waker>,
}

impl TimerSlot {
    fn new() -> Arc<Mutex<TimerSlot>> {
        Arc::new(Mutex::new(TimerSlot { fired: false, waker: None }))
    }
}

enum Action {
    /// A sleep or an I/O completion.
    Fire(Arc<Mutex<TimerSlot>>),
    Deliver { from: NodeId, to: NodeId, msg: u64, payload: Vec<u8> },
    ChaosTick,
    Restart(NodeId),
    Heal(Vec<(NodeId, NodeId)>),
    Resume(NodeId),
}

struct Timer {
    at: Nanos,
    seq: u64,
    action: Action,
}

impl PartialEq for Timer {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at && self.seq == other.seq
    }
}
impl Eq for Timer {}
impl PartialOrd for Timer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Timer {
    fn cmp(&self, other: &Self) -> Ordering {
        // `seq` breaks ties, so the heap is a total order and the pop sequence
        // is a pure function of insertion order. Never compare on anything
        // address-derived here; that is exactly the class of bug the
        // determinism guard exists to catch.
        self.at.cmp(&other.at).then(self.seq.cmp(&other.seq))
    }
}

// ---------------------------------------------------------------------------
// Files
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum PendingOp {
    Write { offset: u64, data: Vec<u8> },
    Truncate(u64),
}

/// A simulated file, modelled as three things a real file also is: what is on
/// the platter, what the page cache shows you, and what is in between.
struct FileState {
    id: u64,
    /// Survives power loss.
    durable: Vec<u8>,
    /// What reads see. A buffered write is visible to this process immediately;
    /// that is not a promise it survives a crash.
    view: Vec<u8>,
    /// Written, acknowledged, not yet fsynced. The graveyard of crash bugs.
    pending: Vec<PendingOp>,
}

impl FileState {
    fn new(name: &str) -> Self {
        let mut h = Fnv::new();
        h.bytes(name.as_bytes());
        Self { id: h.get(), durable: Vec::new(), view: Vec::new(), pending: Vec::new() }
    }
}

fn splice(buf: &mut Vec<u8>, offset: u64, data: &[u8]) {
    let off = offset as usize;
    if buf.len() < off {
        buf.resize(off, 0);
    }
    let end = off + data.len();
    if buf.len() < end {
        buf.resize(end, 0);
    }
    buf[off..end].copy_from_slice(data);
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

struct NodeState {
    role: Role,
    up: bool,
    paused: bool,
    mailbox: VecDeque<Envelope>,
    recv_waiters: Vec<Waker>,
    files: BTreeMap<String, FileState>,
    /// Wall-clock offset from true virtual time. Signed: clocks run early too.
    skew: i64,
    /// Rate error, parts per million.
    drift_ppm: i64,
    written_bytes: u64,
    boots: u32,
    /// This node's own randomness substream, stable across restarts, so that a
    /// change to one node's number of draws does not shift every other node's.
    rng: Arc<SeededRng>,
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub polls: u64,
    pub tasks_spawned: u64,
    pub msgs_sent: u64,
    pub msgs_delivered: u64,
    pub msgs_dropped: u64,
    pub msgs_duplicated: u64,
    pub disk_writes: u64,
    pub disk_bytes: u64,
    pub fsyncs: u64,
    pub torn_writes: u64,
    pub lost_writes: u64,
    pub crashes: u64,
    pub restarts: u64,
    pub partitions: u64,
    pub pauses: u64,
    pub events: u64,
}

/// How a run ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Nothing left to do: no runnable tasks and no scheduled events. For a
    /// server that should idle forever this means everything died.
    Quiesced,
    /// Ran out the clock. The normal ending.
    HorizonReached,
    /// Someone called `stop()`, usually because an oracle found a violation.
    Stopped,
    /// Tasks kept being runnable without virtual time ever advancing — a busy
    /// loop. Reported rather than hung, because a hang in a swarm of 10,000
    /// seeds is indistinguishable from an infrastructure failure.
    Livelock { polls: u64 },
    /// A task panicked. Almost always an invariant assertion firing, which is
    /// to say: a bug, found.
    Panicked { node: NodeId, message: String },
}

impl Outcome {
    pub fn is_failure(&self) -> bool {
        matches!(self, Outcome::Livelock { .. } | Outcome::Panicked { .. })
    }
}

// ---------------------------------------------------------------------------
// The kernel
// ---------------------------------------------------------------------------

struct Inner {
    now: Nanos,
    seq: u64,
    timers: BinaryHeap<Reverse<Timer>>,
    tasks: BTreeMap<TaskId, TaskSlot>,
    ready: BTreeSet<TaskId>,
    next_task: TaskId,
    current: Option<TaskId>,
    rng: SeededRng,
    trace: Recorder,
    nodes: BTreeMap<NodeId, NodeState>,
    /// Directed, refcounted so overlapping partitions heal independently.
    blocked: BTreeMap<(NodeId, NodeId), u32>,
    policy: FaultPolicy,
    next_msg: u64,
    stats: Stats,
    stop: bool,
    chaos_on: bool,
}

struct TaskSlot {
    node: NodeId,
    #[allow(dead_code)]
    name: String,
    fut: Option<BoxFuture<'static, ()>>,
}

/// Manual, and deliberately shallow: a `Kernel` transitively owns every task
/// future in the run, and a derived `Debug` would either not compile or print
/// several megabytes.
impl std::fmt::Debug for Kernel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.lock();
        f.debug_struct("Kernel")
            .field("now", &inner.now)
            .field("tasks", &inner.tasks.len())
            .field("ready", &inner.ready.len())
            .field("timers", &inner.timers.len())
            .field("nodes", &inner.nodes.len())
            .finish()
    }
}

pub struct Kernel {
    inner: Mutex<Inner>,
    boot: Mutex<Option<Arc<dyn Fn(Host) + Send + Sync>>>,
    /// Set when a task panics, so the loop can stop and report.
    panic_msg: Mutex<Option<(NodeId, String)>>,
}

impl Inner {
    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    fn record(&mut self, event: Event) {
        let (at, seq) = (self.now, self.seq);
        self.stats.events += 1;
        self.trace.record(at, seq, event);
    }

    fn schedule(&mut self, delay: Nanos, action: Action) {
        let seq = self.next_seq();
        let at = self.now.saturating_add(delay);
        self.timers.push(Reverse(Timer { at, seq, action }));
    }

    fn is_blocked(&self, from: NodeId, to: NodeId) -> bool {
        self.blocked.get(&(from, to)).copied().unwrap_or(0) > 0
    }

    /// A task is runnable if it exists and its node is up and not frozen.
    fn runnable(&self) -> Vec<TaskId> {
        self.ready
            .iter()
            .copied()
            .filter(|id| match self.tasks.get(id) {
                Some(slot) => match self.nodes.get(&slot.node) {
                    Some(n) => n.up && !n.paused,
                    None => false,
                },
                None => false,
            })
            .collect()
    }

    fn servers(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|(_, n)| n.role == Role::Server)
            .map(|(id, _)| *id)
            .collect()
    }
}

impl Kernel {
    fn new(seed: u64, policy: FaultPolicy, mode: TraceMode) -> Arc<Kernel> {
        let inner = Inner {
            now: Nanos::ZERO,
            seq: 0,
            timers: BinaryHeap::new(),
            tasks: BTreeMap::new(),
            ready: BTreeSet::new(),
            next_task: 1,
            current: None,
            rng: SeededRng::new(seed),
            trace: Recorder::new(mode),
            nodes: BTreeMap::new(),
            blocked: BTreeMap::new(),
            policy,
            next_msg: 1,
            stats: Stats::default(),
            stop: false,
            chaos_on: true,
        };
        Arc::new(Kernel {
            inner: Mutex::new(inner),
            boot: Mutex::new(None),
            panic_msg: Mutex::new(None),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // A poisoned kernel means a task panicked while holding the lock. The
        // run is already a failure; recovering the guard lets us report it
        // properly instead of cascading into a second panic.
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn mark_ready(&self, id: TaskId) {
        self.lock().ready.insert(id);
    }
}

// ---------------------------------------------------------------------------
// Waker
// ---------------------------------------------------------------------------

struct TaskWake {
    id: TaskId,
    kernel: Weak<Kernel>,
}

impl TaskWake {
    fn wake(&self) {
        if let Some(k) = self.kernel.upgrade() {
            k.mark_ready(self.id);
        }
    }
}

/// The hand-rolled `Waker`.
///
/// `RawWakerVTable` is the one place a custom executor cannot avoid `unsafe`:
/// the waker is a type-erased pointer plus four function pointers, and keeping
/// the refcount honest across them is a manual obligation.
///
/// The contract every function below assumes, and the one `make_waker`
/// establishes: `p` is always a pointer obtained from `Arc::into_raw` on an
/// `Arc<TaskWake>` that has not yet been consumed. Ownership accounting is
/// `clone` +1, `wake` -1, `drop` -1, `wake_by_ref` ±0.
static VTABLE: RawWakerVTable = RawWakerVTable::new(vt_clone, vt_wake, vt_wake_by_ref, vt_drop);

unsafe fn vt_clone(p: *const ()) -> RawWaker {
    // SAFETY: `p` came from `Arc::into_raw`. Reconstituting it would consume
    // the reference, so `forget` hands it straight back; the clone is the new
    // reference this call is required to produce.
    let arc = unsafe { Arc::from_raw(p as *const TaskWake) };
    let cloned = Arc::clone(&arc);
    std::mem::forget(arc);
    RawWaker::new(Arc::into_raw(cloned) as *const (), &VTABLE)
}

unsafe fn vt_wake(p: *const ()) {
    // SAFETY: as above. `wake` consumes the waker, so this reference is meant
    // to be dropped at the end of the call.
    let arc = unsafe { Arc::from_raw(p as *const TaskWake) };
    arc.wake();
}

unsafe fn vt_wake_by_ref(p: *const ()) {
    // SAFETY: as above, but `wake_by_ref` must *not* consume the reference.
    let arc = unsafe { Arc::from_raw(p as *const TaskWake) };
    arc.wake();
    std::mem::forget(arc);
}

unsafe fn vt_drop(p: *const ()) {
    // SAFETY: as above. This is the reference's final release.
    drop(unsafe { Arc::from_raw(p as *const TaskWake) });
}

fn make_waker(id: TaskId, kernel: &Arc<Kernel>) -> Waker {
    let handle = Arc::new(TaskWake { id, kernel: Arc::downgrade(kernel) });
    // SAFETY: `handle` is a freshly created `Arc<TaskWake>` and `into_raw`
    // transfers exactly one reference to the waker, which is what `VTABLE`'s
    // accounting assumes. The pointer is only ever read back as
    // `*const TaskWake`, by the four functions above.
    unsafe { Waker::from_raw(RawWaker::new(Arc::into_raw(handle) as *const (), &VTABLE)) }
}

// ---------------------------------------------------------------------------
// Futures
// ---------------------------------------------------------------------------

/// A future completed by a kernel timer: a sleep, or an I/O completion.
struct TimerFuture {
    slot: Arc<Mutex<TimerSlot>>,
}

impl Future for TimerFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut slot = self.slot.lock().unwrap();
        if slot.fired {
            Poll::Ready(())
        } else {
            slot.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

/// Yields exactly once. In simulation this is a scheduling point: the task goes
/// back into the ready set and the PRNG may pick someone else first. Placing
/// these deliberately is how you make a narrow race wide enough to hit.
struct YieldOnce {
    done: bool,
}

impl Future for YieldOnce {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.done {
            Poll::Ready(())
        } else {
            self.done = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// Waits for the next message addressed to this node.
struct RecvFuture {
    kernel: Weak<Kernel>,
    node: NodeId,
}

impl Future for RecvFuture {
    type Output = Option<Envelope>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Envelope>> {
        let Some(k) = self.kernel.upgrade() else {
            return Poll::Ready(None);
        };
        let mut inner = k.lock();
        let Some(n) = inner.nodes.get_mut(&self.node) else {
            return Poll::Ready(None);
        };
        if !n.up {
            return Poll::Ready(None);
        }
        match n.mailbox.pop_front() {
            Some(env) => Poll::Ready(Some(env)),
            None => {
                n.recv_waiters.push(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Handles handed to the system under test
// ---------------------------------------------------------------------------

struct SimClock {
    kernel: Weak<Kernel>,
    node: NodeId,
}

/// `true_time * (1 + ppm/1e6)`, in exact integer arithmetic. A node whose
/// crystal runs fast measures more elapsed time than actually elapsed.
fn apply_drift(t: Nanos, ppm: i64) -> Nanos {
    if ppm == 0 {
        return t;
    }
    let delta = (t.0 as i128 * ppm as i128) / 1_000_000i128;
    Nanos((t.0 as i128 + delta).clamp(0, u64::MAX as i128) as u64)
}

impl Clock for SimClock {
    fn now(&self) -> Nanos {
        let Some(k) = self.kernel.upgrade() else { return Nanos::ZERO };
        let inner = k.lock();
        let (skew, ppm) =
            inner.nodes.get(&self.node).map(|n| (n.skew, n.drift_ppm)).unwrap_or((0, 0));
        apply_drift(inner.now, ppm).offset(skew)
    }

    fn monotonic(&self) -> Nanos {
        let Some(k) = self.kernel.upgrade() else { return Nanos::ZERO };
        let inner = k.lock();
        let ppm = inner.nodes.get(&self.node).map(|n| n.drift_ppm).unwrap_or(0);
        apply_drift(inner.now, ppm)
    }

    fn sleep(&self, dur: Nanos) -> BoxFuture<'static, ()> {
        let Some(k) = self.kernel.upgrade() else {
            return Box::pin(std::future::pending());
        };
        let slot = TimerSlot::new();
        {
            let mut inner = k.lock();
            let ppm = inner.nodes.get(&self.node).map(|n| n.drift_ppm).unwrap_or(0);
            // The sleeper measures duration on its *own* clock. A node running
            // 3000ppm fast wakes 0.3% early in true time — which is precisely
            // how a follower's election timer fires before the leader's
            // heartbeat interval, on a healthy network.
            let true_dur = if ppm == 0 {
                dur.0
            } else {
                ((dur.0 as i128 * 1_000_000i128) / (1_000_000i128 + ppm as i128)).max(0) as u128
                    as u64
            };
            let task = inner.current.unwrap_or(0);
            let until = inner.now.saturating_add(Nanos(true_dur));
            inner.record(Event::Sleep { node: self.node, task, until });
            inner.schedule(Nanos(true_dur), Action::Fire(Arc::clone(&slot)));
        }
        Box::pin(TimerFuture { slot })
    }
}

struct SimNet {
    kernel: Weak<Kernel>,
    node: NodeId,
}

impl Network for SimNet {
    fn send(&self, to: NodeId, payload: Vec<u8>) {
        let Some(k) = self.kernel.upgrade() else { return };
        let mut inner = k.lock();
        let from = self.node;
        let msg = inner.next_msg;
        inner.next_msg += 1;
        inner.stats.msgs_sent += 1;
        let len = payload.len();
        inner.record(Event::Send { from, to, msg, len });

        if !inner.nodes.contains_key(&to) {
            inner.stats.msgs_dropped += 1;
            inner.record(Event::Dropped { from, to, msg, why: DropReason::NodeGone });
            return;
        }
        // Partition is evaluated when the packet enters the link, not when it
        // leaves it. A packet already in flight when a partition begins still
        // arrives — that is what a real network does, and the in-flight window
        // is where the interesting reorderings live.
        if inner.is_blocked(from, to) {
            inner.stats.msgs_dropped += 1;
            inner.record(Event::Dropped { from, to, msg, why: DropReason::Partitioned });
            return;
        }
        let loss = inner.policy.link.loss_ppm;
        if inner.rng.ppm(loss) {
            inner.stats.msgs_dropped += 1;
            inner.record(Event::Dropped { from, to, msg, why: DropReason::RandomLoss });
            return;
        }

        let deliver = |inner: &mut Inner, payload: Vec<u8>| {
            let d = inner.policy.link.latency.sample(&inner.rng);
            inner.schedule(Nanos(d), Action::Deliver { from, to, msg, payload });
        };

        let mut payload = payload;
        let corrupt = inner.policy.link.corrupt_ppm;
        if inner.rng.ppm(corrupt) && !payload.is_empty() {
            let idx = inner.rng.below(payload.len() as u64) as usize;
            let bit = inner.rng.below(8) as u32;
            payload[idx] ^= 1 << bit;
            inner.record(Event::Corrupted { from, to, msg, byte: idx });
        }

        let dup = inner.policy.link.duplicate_ppm;
        if inner.rng.ppm(dup) {
            inner.stats.msgs_duplicated += 1;
            inner.record(Event::Duplicated { from, to, msg });
            let copy = payload.clone();
            deliver(&mut inner, copy);
        }
        deliver(&mut inner, payload);
    }

    fn recv(&self) -> BoxFuture<'static, Option<Envelope>> {
        Box::pin(RecvFuture { kernel: Weak::clone(&self.kernel), node: self.node })
    }

    fn local(&self) -> NodeId {
        self.node
    }
}

#[derive(Debug)]
struct SimStorage {
    kernel: Weak<Kernel>,
    node: NodeId,
}

impl Storage for SimStorage {
    fn open(&self, name: &str) -> BoxFuture<'static, std::io::Result<Arc<dyn File>>> {
        let kernel = Weak::clone(&self.kernel);
        let node = self.node;
        let name = name.to_string();
        Box::pin(async move {
            let Some(k) = kernel.upgrade() else {
                return Err(crate::traits::eio("kernel gone"));
            };
            {
                let mut inner = k.lock();
                let Some(n) = inner.nodes.get_mut(&node) else {
                    return Err(crate::traits::eio("no such node"));
                };
                n.files.entry(name.clone()).or_insert_with(|| FileState::new(&name));
            }
            let f: Arc<dyn File> = Arc::new(SimFile { kernel, node, name });
            Ok(f)
        })
    }

    fn list(&self) -> BoxFuture<'static, std::io::Result<Vec<String>>> {
        let kernel = Weak::clone(&self.kernel);
        let node = self.node;
        Box::pin(async move {
            let Some(k) = kernel.upgrade() else { return Ok(Vec::new()) };
            let inner = k.lock();
            Ok(inner
                .nodes
                .get(&node)
                .map(|n| n.files.keys().cloned().collect())
                .unwrap_or_default())
        })
    }

    fn remove(&self, name: &str) -> BoxFuture<'static, std::io::Result<()>> {
        let kernel = Weak::clone(&self.kernel);
        let node = self.node;
        let name = name.to_string();
        Box::pin(async move {
            let Some(k) = kernel.upgrade() else { return Ok(()) };
            let mut inner = k.lock();
            if let Some(n) = inner.nodes.get_mut(&node) {
                n.files.remove(&name);
            }
            Ok(())
        })
    }

    fn sync_dir(&self) -> BoxFuture<'static, std::io::Result<()>> {
        let kernel = Weak::clone(&self.kernel);
        let node = self.node;
        Box::pin(async move {
            let Some(k) = kernel.upgrade() else { return Ok(()) };
            let slot = TimerSlot::new();
            {
                let mut inner = k.lock();
                let d = inner.policy.disk.fsync_latency.sample(&inner.rng);
                inner.schedule(Nanos(d), Action::Fire(Arc::clone(&slot)));
            }
            TimerFuture { slot }.await;
            let _ = node;
            Ok(())
        })
    }
}

#[derive(Debug)]
struct SimFile {
    kernel: Weak<Kernel>,
    node: NodeId,
    name: String,
}

impl SimFile {
    /// Sample an operation latency, including the occasional spike that makes
    /// group commit interesting.
    fn latency(inner: &mut Inner, which: u8) -> Nanos {
        let p = &inner.policy.disk;
        let base = match which {
            0 => p.write_latency.sample(&inner.rng),
            1 => p.fsync_latency.sample(&inner.rng),
            _ => p.read_latency.sample(&inner.rng),
        };
        let (slow_ppm, mult) = (p.slow_ppm, p.slow_multiplier);
        if inner.rng.ppm(slow_ppm) {
            Nanos(base.saturating_mul(mult))
        } else {
            Nanos(base)
        }
    }
}

impl File for SimFile {
    fn len(&self) -> u64 {
        let Some(k) = self.kernel.upgrade() else { return 0 };
        let inner = k.lock();
        inner
            .nodes
            .get(&self.node)
            .and_then(|n| n.files.get(&self.name))
            .map(|f| f.view.len() as u64)
            .unwrap_or(0)
    }

    fn write_at(&self, offset: u64, data: Vec<u8>) -> BoxFuture<'static, std::io::Result<()>> {
        let kernel = Weak::clone(&self.kernel);
        let node = self.node;
        let name = self.name.clone();
        Box::pin(async move {
            let Some(k) = kernel.upgrade() else { return Err(crate::traits::eio("kernel gone")) };
            let slot = TimerSlot::new();
            {
                let mut inner = k.lock();
                let quota = inner.policy.disk.enospc_after_bytes;
                let Some(n) = inner.nodes.get_mut(&node) else {
                    return Err(crate::traits::eio("no such node"));
                };
                if !n.up {
                    return Err(crate::traits::eio("node down"));
                }
                let would_be = n.written_bytes + data.len() as u64;
                if let Some(limit) = quota {
                    if would_be > limit {
                        let fid = n.files.get(&name).map(|f| f.id).unwrap_or(0);
                        inner.record(Event::Enospc { node, file: fid });
                        return Err(crate::traits::enospc());
                    }
                }
                n.written_bytes = would_be;
                let Some(f) = n.files.get_mut(&name) else {
                    return Err(crate::traits::eio("file closed"));
                };
                let fid = f.id;
                let len = data.len();
                // Visible to this process immediately (page cache), durable
                // only after a successful fsync. The gap between those two
                // sentences is where crash-consistency bugs live.
                splice(&mut f.view, offset, &data);
                f.pending.push(PendingOp::Write { offset, data });
                inner.stats.disk_writes += 1;
                inner.stats.disk_bytes += len as u64;
                inner.record(Event::DiskWrite { node, file: fid, offset, len });
                let d = SimFile::latency(&mut inner, 0);
                inner.schedule(d, Action::Fire(Arc::clone(&slot)));
            }
            TimerFuture { slot }.await;
            Ok(())
        })
    }

    fn read_at(&self, offset: u64, len: usize) -> BoxFuture<'static, std::io::Result<Vec<u8>>> {
        let kernel = Weak::clone(&self.kernel);
        let node = self.node;
        let name = self.name.clone();
        Box::pin(async move {
            let Some(k) = kernel.upgrade() else { return Err(crate::traits::eio("kernel gone")) };
            let slot = TimerSlot::new();
            {
                let mut inner = k.lock();
                let fid = inner
                    .nodes
                    .get(&node)
                    .and_then(|n| n.files.get(&name))
                    .map(|f| f.id)
                    .unwrap_or(0);
                inner.record(Event::DiskRead { node, file: fid, offset, len });
                let d = SimFile::latency(&mut inner, 2);
                inner.schedule(d, Action::Fire(Arc::clone(&slot)));
            }
            TimerFuture { slot }.await;
            let inner = k.lock();
            let Some(f) = inner.nodes.get(&node).and_then(|n| n.files.get(&name)) else {
                return Err(crate::traits::eio("file gone"));
            };
            let start = (offset as usize).min(f.view.len());
            let end = (start + len).min(f.view.len());
            Ok(f.view[start..end].to_vec())
        })
    }

    fn fsync(&self) -> BoxFuture<'static, std::io::Result<()>> {
        let kernel = Weak::clone(&self.kernel);
        let node = self.node;
        let name = self.name.clone();
        Box::pin(async move {
            let Some(k) = kernel.upgrade() else { return Err(crate::traits::eio("kernel gone")) };
            let slot = TimerSlot::new();
            let covered;
            {
                let mut inner = k.lock();
                let Some(f) = inner.nodes.get(&node).and_then(|n| n.files.get(&name)) else {
                    return Err(crate::traits::eio("file gone"));
                };
                let fid = f.id;
                // Snapshot how much this fsync is responsible for. Writes
                // issued after this point are explicitly *not* covered, which
                // is what makes the barrier a barrier.
                covered = f.pending.len();
                inner.stats.fsyncs += 1;
                inner.record(Event::Fsync { node, file: fid, pending: covered });
                let d = SimFile::latency(&mut inner, 1);
                inner.schedule(d, Action::Fire(Arc::clone(&slot)));
            }
            TimerFuture { slot }.await;
            let mut inner = k.lock();
            let Some(n) = inner.nodes.get_mut(&node) else {
                return Err(crate::traits::eio("node gone"));
            };
            if !n.up {
                return Err(crate::traits::eio("crashed during fsync"));
            }
            let Some(f) = n.files.get_mut(&name) else {
                return Err(crate::traits::eio("file gone"));
            };
            let take = covered.min(f.pending.len());
            for op in f.pending.drain(..take).collect::<Vec<_>>() {
                match op {
                    PendingOp::Write { offset, data } => splice(&mut f.durable, offset, &data),
                    PendingOp::Truncate(len) => f.durable.truncate(len as usize),
                }
            }
            Ok(())
        })
    }

    fn truncate(&self, to: u64) -> BoxFuture<'static, std::io::Result<()>> {
        let kernel = Weak::clone(&self.kernel);
        let node = self.node;
        let name = self.name.clone();
        Box::pin(async move {
            let Some(k) = kernel.upgrade() else { return Err(crate::traits::eio("kernel gone")) };
            let slot = TimerSlot::new();
            {
                let mut inner = k.lock();
                let Some(n) = inner.nodes.get_mut(&node) else {
                    return Err(crate::traits::eio("no such node"));
                };
                let Some(f) = n.files.get_mut(&name) else {
                    return Err(crate::traits::eio("file gone"));
                };
                let fid = f.id;
                f.view.truncate(to as usize);
                f.pending.push(PendingOp::Truncate(to));
                inner.record(Event::Truncate { node, file: fid, to });
                let d = SimFile::latency(&mut inner, 0);
                inner.schedule(d, Action::Fire(Arc::clone(&slot)));
            }
            TimerFuture { slot }.await;
            Ok(())
        })
    }
}

struct SimSpawner {
    kernel: Weak<Kernel>,
    node: NodeId,
}

impl Spawner for SimSpawner {
    fn spawn(&self, name: &str, fut: BoxFuture<'static, ()>) {
        let Some(k) = self.kernel.upgrade() else { return };
        let mut inner = k.lock();
        let id = inner.next_task;
        inner.next_task += 1;
        inner.stats.tasks_spawned += 1;
        inner.tasks.insert(id, TaskSlot { node: self.node, name: name.to_string(), fut: Some(fut) });
        inner.ready.insert(id);
        inner.record(Event::Spawn { node: self.node, task: id });
    }

    fn yield_now(&self) -> BoxFuture<'static, ()> {
        Box::pin(YieldOnce { done: false })
    }
}

struct SimTracer {
    kernel: Weak<Kernel>,
    node: NodeId,
    enabled: Arc<AtomicBool>,
}

impl Tracer for SimTracer {
    fn note(&self, text: &str) {
        let Some(k) = self.kernel.upgrade() else { return };
        let mut inner = k.lock();
        let node = self.node;
        inner.record(Event::Note { node, text: text.to_string() });
    }

    fn enabled(&self) -> bool {
        self.enabled.load(AtomicOrdering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// The public simulator
// ---------------------------------------------------------------------------

/// One simulated universe.
#[derive(Debug)]
pub struct Sim {
    k: Arc<Kernel>,
    notes_enabled: Arc<AtomicBool>,
    seed: u64,
}

impl Sim {
    pub fn new(seed: u64, policy: FaultPolicy, mode: TraceMode) -> Sim {
        let notes_enabled = Arc::new(AtomicBool::new(!matches!(mode, TraceMode::HashOnly)));
        Sim { k: Kernel::new(seed, policy, mode), notes_enabled, seed }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Application notes cost a `String` each. Off in swarm runs, on when
    /// replaying a failing seed. They still participate in the trace hash when
    /// enabled, so `check` compares like with like.
    pub fn set_notes(&self, on: bool) {
        self.notes_enabled.store(on, AtomicOrdering::Relaxed);
    }

    /// Register the node's entry point. Called once at boot and again after
    /// every restart, with a fresh `Host` for the same node id — exactly like a
    /// process restarting on the same machine, with the same disk.
    pub fn set_boot<F>(&self, f: F)
    where
        F: Fn(Host) + Send + Sync + 'static,
    {
        *self.k.boot.lock().unwrap() = Some(Arc::new(f));
    }

    pub fn add_node(&self, id: NodeId, role: Role) -> Host {
        {
            let mut inner = self.k.lock();
            // Clients get a perfect clock. They are not the system under
            // test, and a workload generator whose own clock drifts cannot
            // produce a history an oracle can reason about: the operation
            // timestamps a linearizability checker orders by would themselves
            // be wrong.
            let (max_skew, max_drift) = if role == Role::Client {
                (0, 0)
            } else {
                (inner.policy.clock.max_skew.0, inner.policy.clock.max_drift_ppm)
            };
            let skew = if max_skew == 0 {
                0
            } else {
                inner.rng.range(0, 2 * max_skew + 1) as i64 - max_skew as i64
            };
            let drift_ppm = if max_drift == 0 {
                0
            } else {
                inner.rng.range(0, (2 * max_drift + 1) as u64) as i64 - max_drift
            };
            let node_rng = Arc::new(inner.rng.fork());
            inner.nodes.insert(
                id,
                NodeState {
                    role,
                    up: false,
                    paused: false,
                    mailbox: VecDeque::new(),
                    recv_waiters: Vec::new(),
                    files: BTreeMap::new(),
                    skew,
                    drift_ppm,
                    written_bytes: 0,
                    boots: 0,
                    rng: node_rng,
                },
            );
        }
        self.host(id)
    }

    pub fn host(&self, node: NodeId) -> Host {
        let w = Arc::downgrade(&self.k);
        let rng: Arc<dyn Rng> = {
            let inner = self.k.lock();
            match inner.nodes.get(&node) {
                Some(n) => Arc::clone(&n.rng) as Arc<dyn Rng>,
                None => Arc::new(SeededRng::new(node as u64)),
            }
        };
        Host {
            node,
            clock: Arc::new(SimClock { kernel: Weak::clone(&w), node }),
            net: Arc::new(SimNet { kernel: Weak::clone(&w), node }),
            storage: Arc::new(SimStorage { kernel: Weak::clone(&w), node }),
            rng,
            spawner: Arc::new(SimSpawner { kernel: Weak::clone(&w), node }),
            tracer: Arc::new(SimTracer {
                kernel: w,
                node,
                enabled: Arc::clone(&self.notes_enabled),
            }),
        }
    }

    /// Bring every registered node up and arm the chaos ticker.
    pub fn boot_all(&self) {
        let ids: Vec<NodeId> = self.k.lock().nodes.keys().copied().collect();
        for id in ids {
            self.boot_node(id);
        }
        let mut inner = self.k.lock();
        // Only arm the ticker if it has something to do — see
        // `ChaosPolicy::is_active`.
        if inner.policy.chaos.is_active() || inner.policy.clock.step_ppm_per_sec > 0 {
            let tick = inner.policy.chaos_tick;
            inner.schedule(tick, Action::ChaosTick);
        }
    }

    fn boot_node(&self, id: NodeId) {
        {
            let mut inner = self.k.lock();
            let Some(n) = inner.nodes.get_mut(&id) else { return };
            if n.up {
                return;
            }
            n.up = true;
            n.paused = false;
            n.boots += 1;
            let first = n.boots == 1;
            inner.record(if first { Event::Boot { node: id } } else { Event::Restart { node: id } });
            if !first {
                inner.stats.restarts += 1;
            }
        }
        // Boot outside the lock: the entry point spawns tasks, which re-enters.
        let boot = self.k.boot.lock().unwrap().clone();
        if let Some(boot) = boot {
            boot(self.host(id));
        }
    }

    pub fn now(&self) -> Nanos {
        self.k.lock().now
    }

    pub fn trace_hash(&self) -> u64 {
        self.k.lock().trace.hash()
    }

    pub fn stats(&self) -> Stats {
        self.k.lock().stats
    }

    pub fn stop(&self) {
        self.k.lock().stop = true;
    }

    /// Turn chaos off — used to let a cluster recover so the liveness oracle
    /// can ask whether it *does*.
    pub fn set_chaos(&self, on: bool) {
        self.k.lock().chaos_on = on;
    }

    /// Heal every partition and resume every paused node, leaving crashed nodes
    /// crashed. The liveness watchdog calls this and then starts its clock.
    pub fn heal_all(&self) {
        let mut wake = Vec::new();
        {
            let mut inner = self.k.lock();
            let links: Vec<(NodeId, NodeId)> = inner.blocked.keys().copied().collect();
            for l in links {
                inner.record(Event::Heal { a: l.0, b: l.1 });
            }
            inner.blocked.clear();
            let ids: Vec<NodeId> = inner.nodes.keys().copied().collect();
            let mut resumed = Vec::new();
            for id in ids {
                if let Some(n) = inner.nodes.get_mut(&id) {
                    if n.paused {
                        n.paused = false;
                        wake.append(&mut n.recv_waiters);
                        resumed.push(id);
                    }
                }
            }
            for node in resumed {
                inner.record(Event::Resume { node });
            }
        }
        for w in wake {
            w.wake();
        }
    }

    /// Snapshot of who is reachable from whom, for oracle use.
    pub fn alive(&self) -> Vec<NodeId> {
        let inner = self.k.lock();
        inner.nodes.iter().filter(|(_, n)| n.up && !n.paused).map(|(id, _)| *id).collect()
    }

    /// Whether any link is currently cut.
    ///
    /// The liveness watchdog needs this: demanding progress from a partitioned
    /// cluster is demanding a violation of CAP, so its budget only runs while
    /// the environment is cooperating.
    pub fn has_partitions(&self) -> bool {
        !self.k.lock().blocked.is_empty()
    }

    /// Servers that are up and not frozen.
    pub fn alive_servers(&self) -> usize {
        let inner = self.k.lock();
        inner
            .nodes
            .values()
            .filter(|n| n.role == Role::Server && n.up && !n.paused)
            .count()
    }

    pub fn is_up(&self, node: NodeId) -> bool {
        self.k.lock().nodes.get(&node).map(|n| n.up && !n.paused).unwrap_or(false)
    }

    pub fn crash(&self, node: NodeId) {
        self.do_crash(node);
    }

    pub fn restart(&self, node: NodeId) {
        self.boot_node(node);
    }

    pub fn partition(&self, group_a: &[NodeId], group_b: &[NodeId], one_way: bool) {
        let mut inner = self.k.lock();
        for &a in group_a {
            for &b in group_b {
                *inner.blocked.entry((a, b)).or_insert(0) += 1;
                if !one_way {
                    *inner.blocked.entry((b, a)).or_insert(0) += 1;
                }
                inner.record(Event::Partition { a, b, one_way });
            }
        }
        inner.stats.partitions += 1;
    }

    /// Note into the trace from outside any node — used by the harness to mark
    /// phases of a scenario.
    pub fn note(&self, node: NodeId, text: impl Into<String>) {
        if !self.notes_enabled.load(AtomicOrdering::Relaxed) {
            return;
        }
        let mut inner = self.k.lock();
        inner.record(Event::Note { node, text: text.into() });
    }

    pub fn with_trace<R>(&self, f: impl FnOnce(&Recorder) -> R) -> R {
        let inner = self.k.lock();
        f(&inner.trace)
    }

    fn do_crash(&self, node: NodeId) {
        let mut wake = Vec::new();
        {
            let mut inner = self.k.lock();
            let Some(n) = inner.nodes.get_mut(&node) else { return };
            if !n.up {
                return;
            }
            n.up = false;
            n.paused = false;
            n.mailbox.clear();
            wake.append(&mut n.recv_waiters);
            inner.stats.crashes += 1;
            inner.record(Event::Crash { node });

            // Reap this node's tasks. A hard crash does not unwind, does not
            // run destructors, and does not get to flush anything.
            let doomed: Vec<TaskId> = inner
                .tasks
                .iter()
                .filter(|(_, s)| s.node == node)
                .map(|(id, _)| *id)
                .collect();
            for id in doomed {
                inner.tasks.remove(&id);
                inner.ready.remove(&id);
            }
            power_loss(&mut inner, node);
        }
        // Wake the reaped receivers so their futures resolve to `None` rather
        // than leaking. They belong to dead tasks and will simply be dropped.
        for w in wake {
            w.wake();
        }
    }

    /// Run until the horizon, or until something ends the run.
    pub fn run_until(&self, horizon: Nanos) -> Outcome {
        // A busy-wake loop must be reported, not hung on. The bound scales with
        // the task count so a large cluster is not falsely accused.
        let mut polls_since_time_moved: u64 = 0;
        let livelock_budget: u64 = 2_000_000;

        loop {
            if self.k.lock().stop {
                return Outcome::Stopped;
            }
            if let Some((node, msg)) = self.k.panic_msg.lock().unwrap().take() {
                return Outcome::Panicked { node, message: msg };
            }

            if let Some(id) = self.pick_runnable() {
                self.poll_task(id);
                polls_since_time_moved += 1;
                if polls_since_time_moved > livelock_budget {
                    return Outcome::Livelock { polls: polls_since_time_moved };
                }
                continue;
            }

            match self.next_timer(horizon) {
                Some(timer) => {
                    polls_since_time_moved = 0;
                    self.fire(timer);
                }
                None => {
                    let inner = self.k.lock();
                    return if inner.now >= horizon {
                        Outcome::HorizonReached
                    } else {
                        Outcome::Quiesced
                    };
                }
            }
        }
    }

    /// Pick the next task to poll. This single line is why the whole thing is
    /// reproducible: the interleaving is a PRNG draw, not an OS decision.
    fn pick_runnable(&self) -> Option<TaskId> {
        let mut inner = self.k.lock();
        let runnable = inner.runnable();
        // Also garbage-collect ready entries for tasks that no longer exist
        // (reaped by a crash) so the set does not grow without bound.
        if runnable.is_empty() {
            let stale: Vec<TaskId> =
                inner.ready.iter().copied().filter(|id| !inner.tasks.contains_key(id)).collect();
            for id in stale {
                inner.ready.remove(&id);
            }
            return None;
        }
        let idx = inner.rng.pick_index(runnable.len())?;
        let id = runnable[idx];
        inner.ready.remove(&id);
        Some(id)
    }

    fn poll_task(&self, id: TaskId) {
        let (mut fut, node) = {
            let mut inner = self.k.lock();
            let Some(slot) = inner.tasks.get_mut(&id) else { return };
            let node = slot.node;
            let Some(fut) = slot.fut.take() else {
                // Already being polled: impossible in a single-threaded kernel,
                // but cheap to be explicit about.
                return;
            };
            inner.current = Some(id);
            inner.stats.polls += 1;
            inner.record(Event::Poll { node, task: id });
            (fut, node)
        };

        let waker = make_waker(id, &self.k);
        let mut cx = Context::from_waker(&waker);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            fut.as_mut().poll(&mut cx)
        }));

        let mut inner = self.k.lock();
        inner.current = None;
        match result {
            Ok(Poll::Ready(())) => {
                inner.tasks.remove(&id);
                inner.record(Event::TaskDone { node, task: id });
            }
            Ok(Poll::Pending) => {
                if let Some(slot) = inner.tasks.get_mut(&id) {
                    slot.fut = Some(fut);
                } // else: the task was reaped mid-poll by a crash. Drop it.
            }
            Err(payload) => {
                let msg = panic_message(&payload);
                inner.tasks.remove(&id);
                drop(inner);
                *self.k.panic_msg.lock().unwrap() = Some((node, msg));
            }
        }
    }

    /// Pop the next scheduled event, advancing virtual time to meet it.
    ///
    /// Events sharing an instant are chosen among at random rather than in
    /// insertion order. Ties are rare at nanosecond resolution, but when they
    /// happen — two heartbeats scheduled by the same tick — the order matters
    /// and should be explored rather than fixed by an implementation accident.
    fn next_timer(&self, horizon: Nanos) -> Option<Timer> {
        let mut inner = self.k.lock();
        let at = inner.timers.peek()?.0.at;
        if at > horizon {
            inner.now = horizon;
            return None;
        }
        let mut tied: Vec<Timer> = Vec::new();
        loop {
            // Bound the drain: a pathological instant with thousands of tied
            // events should not turn one pop into a full heap rebuild.
            if tied.len() >= 64 {
                break;
            }
            match inner.timers.peek() {
                Some(Reverse(t)) if t.at == at => {}
                _ => break,
            }
            match inner.timers.pop() {
                Some(Reverse(t)) => tied.push(t),
                None => break,
            }
        }
        if tied.is_empty() {
            return None;
        }
        let idx = if tied.len() == 1 { 0 } else { inner.rng.below(tied.len() as u64) as usize };
        let chosen = tied.swap_remove(idx);
        for t in tied {
            inner.timers.push(Reverse(t));
        }
        inner.now = at;
        Some(chosen)
    }

    fn fire(&self, timer: Timer) {
        let mut wake: Vec<Waker> = Vec::new();
        let mut restart: Option<NodeId> = None;
        {
            let mut inner = self.k.lock();
            inner.seq = timer.seq;
            match timer.action {
                Action::Fire(slot) => {
                    let mut s = slot.lock().unwrap();
                    s.fired = true;
                    if let Some(w) = s.waker.take() {
                        wake.push(w);
                    }
                }
                Action::Deliver { from, to, msg, payload } => {
                    let len = payload.len();
                    let up = inner.nodes.get(&to).map(|n| n.up).unwrap_or(false);
                    if !up {
                        inner.stats.msgs_dropped += 1;
                        inner.record(Event::Dropped { from, to, msg, why: DropReason::NodeDown });
                    } else {
                        inner.stats.msgs_delivered += 1;
                        inner.record(Event::Deliver { from, to, msg, len });
                        if let Some(n) = inner.nodes.get_mut(&to) {
                            n.mailbox.push_back(Envelope { from, to, payload, msg_id: msg });
                            wake.append(&mut n.recv_waiters);
                        }
                    }
                }
                Action::ChaosTick => {
                    let tick = chaos_tick(&mut inner);
                    inner.schedule(tick, Action::ChaosTick);
                }
                Action::Restart(node) => {
                    restart = Some(node);
                }
                Action::Heal(links) => {
                    for (a, b) in links {
                        if let Some(c) = inner.blocked.get_mut(&(a, b)) {
                            *c = c.saturating_sub(1);
                            if *c == 0 {
                                inner.blocked.remove(&(a, b));
                                inner.record(Event::Heal { a, b });
                            }
                        }
                    }
                }
                Action::Resume(node) => {
                    if let Some(n) = inner.nodes.get_mut(&node) {
                        if n.paused {
                            n.paused = false;
                            inner.record(Event::Resume { node });
                        }
                    }
                }
            }
        }
        for w in wake {
            w.wake();
        }
        // Booting spawns tasks, which re-enters the kernel — so it must happen
        // after the guard is gone.
        if let Some(node) = restart {
            self.boot_node(node);
        }
    }
}

/// Applies power-loss semantics to every un-fsynced write on a node.
///
/// This is the most important twenty lines in the simulator. A write that was
/// acknowledged but not fsynced may land whole, vanish, or land as a prefix of
/// its sectors. A write-ahead log that assumes anything else is a write-ahead
/// log that loses committed data, and this is what proves it.
fn power_loss(inner: &mut Inner, node: NodeId) {
    let (torn_ppm, lost_ppm, sector) = {
        let d = &inner.policy.disk;
        (d.torn_write_ppm, d.lost_write_ppm, d.sector_size.max(1))
    };
    let names: Vec<String> = match inner.nodes.get(&node) {
        Some(n) => n.files.keys().cloned().collect(),
        None => return,
    };
    for name in names {
        let (pending, fid) = {
            let Some(n) = inner.nodes.get_mut(&node) else { return };
            let Some(f) = n.files.get_mut(&name) else { continue };
            (std::mem::take(&mut f.pending), f.id)
        };
        for op in pending {
            match op {
                PendingOp::Truncate(len) => {
                    // A truncate either happened or it did not; it cannot tear.
                    if inner.rng.ppm(lost_ppm) {
                        inner.record(Event::LostWrite { node, file: fid, offset: len, len: 0 });
                    } else if let Some(f) =
                        inner.nodes.get_mut(&node).and_then(|n| n.files.get_mut(&name))
                    {
                        f.durable.truncate(len as usize);
                    }
                }
                PendingOp::Write { offset, data } => {
                    let total = data.len() as u64;
                    if inner.rng.ppm(lost_ppm) {
                        inner.stats.lost_writes += 1;
                        inner.record(Event::LostWrite {
                            node,
                            file: fid,
                            offset,
                            len: data.len(),
                        });
                        continue;
                    }
                    if inner.rng.ppm(torn_ppm) && total > 0 {
                        // Sector-aligned prefix. Drives retire writes roughly in
                        // order, so a partial write is a prefix far more often
                        // than it is a random scatter of sectors.
                        let first = offset / sector;
                        let last = (offset + total - 1) / sector;
                        let sectors = last - first + 1;
                        let keep_sectors = inner.rng.below(sectors);
                        let keep_bytes = if keep_sectors == 0 {
                            0
                        } else {
                            let boundary = (first + keep_sectors) * sector;
                            boundary.saturating_sub(offset).min(total)
                        };
                        inner.stats.torn_writes += 1;
                        inner.record(Event::TornWrite {
                            node,
                            file: fid,
                            offset,
                            kept: keep_bytes,
                            of: total,
                        });
                        if keep_bytes > 0 {
                            if let Some(f) =
                                inner.nodes.get_mut(&node).and_then(|n| n.files.get_mut(&name))
                            {
                                splice(&mut f.durable, offset, &data[..keep_bytes as usize]);
                            }
                        }
                        continue;
                    }
                    if let Some(f) = inner.nodes.get_mut(&node).and_then(|n| n.files.get_mut(&name))
                    {
                        splice(&mut f.durable, offset, &data);
                    }
                }
            }
        }
        // The page cache is gone. What you see after a reboot is the platter.
        if let Some(f) = inner.nodes.get_mut(&node).and_then(|n| n.files.get_mut(&name)) {
            f.view = f.durable.clone();
        }
    }
}

/// Samples environmental chaos for one tick, returning the delay until the next
/// one. Restarts are scheduled as their own events rather than performed here,
/// because booting a node re-enters the kernel and this runs under the lock.
fn chaos_tick(inner: &mut Inner) -> Nanos {
    let tick = inner.policy.chaos_tick;
    if !inner.chaos_on {
        return tick;
    }
    // Rates are per simulated second; scale them to the tick.
    let scale = |ppm_per_sec: u32| -> u32 {
        ((ppm_per_sec as u64 * tick.0) / crate::time::NANOS_PER_SEC).min(1_000_000) as u32
    };
    let policy = inner.policy.chaos.clone();
    let servers = inner.servers();
    if servers.len() < 2 {
        return tick;
    }

    // --- partitions -------------------------------------------------------
    if inner.rng.ppm(scale(policy.partition_ppm_per_sec)) {
        let mut shuffled = servers.clone();
        inner.rng.shuffle(&mut shuffled);
        // A random non-empty proper subset versus its complement. Splitting the
        // cluster is far more productive than cutting one link: it is how you
        // manufacture the two-leaders-at-once window that Raft must survive.
        let cut = inner.rng.range(1, shuffled.len() as u64) as usize;
        let (a, b) = shuffled.split_at(cut);
        let one_way = inner.rng.ppm(policy.asymmetric_ppm);
        let mut links = Vec::new();
        for &x in a {
            for &y in b {
                *inner.blocked.entry((x, y)).or_insert(0) += 1;
                links.push((x, y));
                if !one_way {
                    *inner.blocked.entry((y, x)).or_insert(0) += 1;
                    links.push((y, x));
                }
                inner.record(Event::Partition { a: x, b: y, one_way });
            }
        }
        inner.stats.partitions += 1;
        let dur = policy.partition_duration.sample(&inner.rng);
        inner.schedule(Nanos(dur), Action::Heal(links));
    }

    // --- pauses (GC, VM migration, SIGSTOP) --------------------------------
    if inner.rng.ppm(scale(policy.pause_ppm_per_sec)) {
        let candidates: Vec<NodeId> = servers
            .iter()
            .copied()
            .filter(|id| inner.nodes.get(id).map(|n| n.up && !n.paused).unwrap_or(false))
            .collect();
        if let Some(i) = inner.rng.pick_index(candidates.len()) {
            let node = candidates[i];
            let dur = policy.pause_duration.sample(&inner.rng);
            let until = inner.now.saturating_add(Nanos(dur));
            if let Some(n) = inner.nodes.get_mut(&node) {
                n.paused = true;
            }
            inner.stats.pauses += 1;
            inner.record(Event::Pause { node, until });
            inner.schedule(Nanos(dur), Action::Resume(node));
        }
    }

    // --- clock steps ------------------------------------------------------
    if inner.rng.ppm(scale(policy_step_ppm(inner))) {
        let max_step = inner.policy.clock.max_step.0;
        if max_step > 0 {
            if let Some(i) = inner.rng.pick_index(servers.len()) {
                let node = servers[i];
                let delta = inner.rng.range(0, 2 * max_step + 1) as i64 - max_step as i64;
                if let Some(n) = inner.nodes.get_mut(&node) {
                    n.skew = n.skew.saturating_add(delta);
                }
                inner.record(Event::ClockStep { node, delta });
            }
        }
    }

    // --- crashes ----------------------------------------------------------
    if inner.rng.ppm(scale(policy.crash_ppm_per_sec)) {
        let alive: Vec<NodeId> =
            servers.iter().copied().filter(|id| inner.nodes[id].up).collect();
        if alive.len() > policy.min_alive {
            if let Some(i) = inner.rng.pick_index(alive.len()) {
                let node = alive[i];
                crash_in_lock(inner, node);
                let delay = policy.restart_delay.sample(&inner.rng);
                inner.schedule(Nanos(delay), Action::Restart(node));
            }
        }
    }

    tick
}

fn policy_step_ppm(inner: &Inner) -> u32 {
    inner.policy.clock.step_ppm_per_sec
}

/// Crash a node while already holding the kernel lock.
///
/// Deliberately does *not* wake the reaped receivers: their tasks no longer
/// exist, and waking under the lock is the one thing this kernel must never do.
fn crash_in_lock(inner: &mut Inner, node: NodeId) {
    {
        let Some(n) = inner.nodes.get_mut(&node) else { return };
        if !n.up {
            return;
        }
        n.up = false;
        n.paused = false;
        n.mailbox.clear();
        n.recv_waiters.clear();
    }
    inner.stats.crashes += 1;
    inner.record(Event::Crash { node });
    let doomed: Vec<TaskId> =
        inner.tasks.iter().filter(|(_, s)| s.node == node).map(|(id, _)| *id).collect();
    for id in doomed {
        inner.tasks.remove(&id);
        inner.ready.remove(&id);
    }
    power_loss(inner, node);
}

fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "task panicked (non-string payload)".to_string()
    }
}
