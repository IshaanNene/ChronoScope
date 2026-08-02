//! The seam.
//!
//! `chronolog` is written against these five traits and nothing else. It never
//! names a socket, a file descriptor, a clock, or a thread. Swapping the
//! implementations swaps the entire universe the system runs in, and that swap
//! is the whole trick: the code under simulation is byte-for-byte the code that
//! runs in production.
//!
//! Every trait is object-safe. Generic parameters would be zero-cost, but they
//! would also infect every type in `chronolog` with an `<E: Env>` parameter and
//! make the Raft module unreadable. One `Arc<dyn _>` indirection per syscall is
//! a price worth paying, and the syscalls are simulated anyway.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::rng::Rng;
use crate::time::Nanos;

pub type NodeId = u32;

/// Boxed because the traits must stay object-safe. `Send` because the *real*
/// runtime is multi-threaded even though the simulator is not.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Time, as seen by one node.
///
/// The split between `now` and `monotonic` is not pedantry — it is the entire
/// lease-safety bug class. A leader that renews its lease against a wall clock
/// that jumped backwards will believe it still holds a lease it lost.
pub trait Clock: Send + Sync {
    /// Wall-clock time on this node: subject to skew, drift, and step changes.
    /// Two nodes will disagree. That disagreement is the point.
    fn now(&self) -> Nanos;

    /// Monotonic time on this node: subject to drift, never steps backwards.
    /// Lease and timeout arithmetic must use this.
    fn monotonic(&self) -> Nanos;

    /// Resolves after `dur` has elapsed on this node's monotonic clock.
    fn sleep(&self, dur: Nanos) -> BoxFuture<'static, ()>;
}

/// A message that arrived.
#[derive(Clone, Debug)]
pub struct Envelope {
    pub from: NodeId,
    pub to: NodeId,
    pub payload: Vec<u8>,
    /// Monotonically increasing per send. Duplicates share an id, which is how
    /// the trace distinguishes "the network duplicated it" from "the sender
    /// retried".
    pub msg_id: u64,
}

/// An unreliable, unordered datagram layer, scoped to one node.
///
/// Deliberately *not* a stream abstraction. Raft is specified against a network
/// that may drop, reorder, and duplicate, and building the system against the
/// weaker model means the production TCP transport can never be load-bearing
/// for correctness. It also means a dropped packet in simulation is a single
/// PRNG draw rather than a modelled connection reset.
pub trait Network: Send + Sync {
    /// Fire and forget. Delivery is not promised, ordering is not promised, and
    /// exactly-once is definitely not promised.
    fn send(&self, to: NodeId, payload: Vec<u8>);

    /// Next message for this node. `None` means the node is shutting down.
    fn recv(&self) -> BoxFuture<'static, Option<Envelope>>;

    /// Who this handle belongs to.
    fn local(&self) -> NodeId;
}

/// A file. The API is intentionally narrow — this is what a write-ahead log
/// actually needs, and nothing else.
///
/// Note what is missing: there is no "write and it is durable" call. `write_at`
/// resolving means the bytes are in the page cache, and that is *all* it means.
/// Only `fsync` promises anything, and even that promise is bounded by
/// [`crate::fault::DiskPolicy`].
pub trait File: Send + Sync + std::fmt::Debug {
    /// Logical length, including bytes not yet fsynced.
    fn len(&self) -> u64;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Write at an offset. Resolving means "accepted into the page cache",
    /// which after a power cut may mean nothing at all.
    fn write_at(&self, offset: u64, data: Vec<u8>) -> BoxFuture<'static, std::io::Result<()>>;

    /// Read `len` bytes at `offset`. Short reads at EOF return what exists.
    fn read_at(&self, offset: u64, len: usize) -> BoxFuture<'static, std::io::Result<Vec<u8>>>;

    /// Durability barrier. After this resolves, every previously-resolved write
    /// to this file survives a crash.
    fn fsync(&self) -> BoxFuture<'static, std::io::Result<()>>;

    fn truncate(&self, len: u64) -> BoxFuture<'static, std::io::Result<()>>;
}

/// A per-node filesystem namespace.
pub trait Storage: Send + Sync + std::fmt::Debug {
    fn open(&self, name: &str) -> BoxFuture<'static, std::io::Result<Arc<dyn File>>>;
    fn list(&self) -> BoxFuture<'static, std::io::Result<Vec<String>>>;
    fn remove(&self, name: &str) -> BoxFuture<'static, std::io::Result<()>>;
    /// Durability barrier for the *directory* — without this, a freshly created
    /// segment can vanish on power loss even though its contents were fsynced.
    fn sync_dir(&self) -> BoxFuture<'static, std::io::Result<()>>;
}

/// Structured concurrency, scoped to a node, so that killing a node reaps
/// exactly the tasks that node spawned.
pub trait Spawner: Send + Sync {
    /// `name` shows up in traces; make it descriptive.
    fn spawn(&self, name: &str, fut: BoxFuture<'static, ()>);

    /// Cooperative yield. In simulation this is a scheduling point where the
    /// PRNG may interleave another task — sprinkling these is how you make a
    /// race reachable rather than theoretical.
    fn yield_now(&self) -> BoxFuture<'static, ()>;
}

/// Application-level annotation of the trace.
///
/// This is the "deterministic-time subscriber": in simulation a note lands in
/// the event trace stamped with *virtual* time, so a failing run reads as a
/// story ("n2 became leader in term 7", "n0 committed index 41") rather than as
/// packet soup. In production the same calls go to stderr with a real
/// timestamp. Notes participate in the trace hash, so they also serve as
/// application-level determinism assertions.
pub trait Tracer: Send + Sync {
    fn note(&self, text: &str);
    /// Whether anyone is listening. Guard expensive formatting with this.
    fn enabled(&self) -> bool {
        true
    }
}

/// A `Tracer` that discards everything, for benchmarks.
#[derive(Debug)]
pub struct NullTracer;

impl Tracer for NullTracer {
    fn note(&self, _text: &str) {}
    fn enabled(&self) -> bool {
        false
    }
}

/// Everything one node is allowed to touch.
///
/// `chronolog` takes a `Host` and can reach nothing else: no globals, no
/// `std::fs`, no `std::net`, no `Instant::now()`. If it compiles against
/// `Host`, it is simulatable.
#[derive(Clone)]
pub struct Host {
    pub node: NodeId,
    pub clock: Arc<dyn Clock>,
    pub net: Arc<dyn Network>,
    pub storage: Arc<dyn Storage>,
    pub rng: Arc<dyn Rng>,
    pub spawner: Arc<dyn Spawner>,
    pub tracer: Arc<dyn Tracer>,
}

impl Host {
    /// Annotate the trace. The closure is only called if anyone is listening,
    /// so this costs nothing in a swarm run.
    pub fn note<F: FnOnce() -> String>(&self, f: F) {
        if self.tracer.enabled() {
            self.tracer.note(&f());
        }
    }

    pub fn now(&self) -> Nanos {
        self.clock.now()
    }

    pub fn monotonic(&self) -> Nanos {
        self.clock.monotonic()
    }

    pub fn sleep(&self, dur: Nanos) -> BoxFuture<'static, ()> {
        self.clock.sleep(dur)
    }

    pub fn spawn<F>(&self, name: &str, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawner.spawn(name, Box::pin(fut));
    }

    /// Spawn a task that needs its own `Host`.
    ///
    /// The obvious spelling — `host.spawn("t", async move { host.sleep(..) })`
    /// — does not compile: the async block moves `host` while `spawn` is still
    /// borrowing it. Handing the closure a clone sidesteps that, and since
    /// nearly every task in `chronolog` needs a `Host`, this is the form
    /// actually used.
    ///
    /// ```no_run
    /// # use chrono_sim::prelude::*;
    /// # fn f(host: Host) {
    /// host.spawn_with("heartbeat", |h| async move {
    ///     loop {
    ///         h.sleep(Nanos::from_millis(50)).await;
    ///     }
    /// });
    /// # }
    /// ```
    pub fn spawn_with<F, Fut>(&self, name: &str, f: F)
    where
        F: FnOnce(Host) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let fut = f(self.clone());
        self.spawner.spawn(name, Box::pin(fut));
    }

    /// Cooperative yield — a scheduling point the simulator may interleave at.
    pub fn yield_now(&self) -> BoxFuture<'static, ()> {
        self.spawner.yield_now()
    }
}

impl std::fmt::Debug for Host {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Host")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

/// Errors the storage layer raises that callers are expected to handle rather
/// than panic on. `ENOSPC` in particular: a WAL that panics when the disk fills
/// is a WAL that loses the cluster.
pub fn enospc() -> std::io::Error {
    std::io::Error::other("ENOSPC: simulated disk full")
}

pub fn eio(what: &str) -> std::io::Error {
    std::io::Error::other(format!("EIO: {what}"))
}

pub fn is_enospc(e: &std::io::Error) -> bool {
    e.to_string().contains("ENOSPC")
}
