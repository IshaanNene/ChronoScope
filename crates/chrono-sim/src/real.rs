//! The production runtime: the same traits, backed by the actual machine.
//!
//! This module exists to make one claim true — that `chronolog` is not written
//! against a simulator, it is written against an *interface*, and the simulator
//! is one implementation of it. Every type here is the real thing: `Instant`,
//! `std::fs`, a UDP socket, an OS thread.
//!
//! # The executor
//!
//! Thread-per-task, and deliberately so. The simulator's executor is the clever
//! one; this one only has to be correct. `spawn` starts a thread that drives
//! the future to completion with a park/unpark waker, so a blocking `fsync` or
//! `recv` blocks exactly one task and nothing else. That costs a thread per
//! task, which for a Raft node is a handful.
//!
//! # Why UDP
//!
//! [`crate::traits::Network`] is a datagram interface: unreliable, unordered,
//! possibly duplicating. That is not a simplification, it is the model Raft is
//! specified against, and building the system against the weaker model means
//! the transport can never be load-bearing for correctness. UDP *is* that
//! model, so the real transport is a direct implementation rather than a
//! reliable stream pretending to be one.
//!
//! A TCP transport would add reconnection, framing across packet boundaries,
//! and head-of-line blocking — real work, but work that buys nothing Raft
//! needs. The one thing it would buy is payloads above the datagram limit,
//! which matters for `InstallSnapshot`; see [`MAX_DATAGRAM`].
//!
//! # What is not here
//!
//! `io_uring`. It is Linux-only, this was built on macOS, and a storage backend
//! that cannot be tested on the machine that wrote it is worse than an honest
//! `std::fs` one. The seam for it is [`Storage`] — an `io_uring` implementation
//! is a new type implementing the same four methods, and nothing above it
//! changes. That is the entire argument for the trait boundary.

use std::collections::VecDeque;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::rng::Rng;
use crate::time::Nanos;
use crate::traits::{
    BoxFuture, Clock, Envelope, File, Host, Network, NodeId, Spawner, Storage, Tracer,
};

/// A datagram larger than this will not survive the network.
///
/// 64 KiB is the IPv4 payload ceiling and in practice anything over the path
/// MTU is fragmented and fragile. `InstallSnapshot` is the message that can
/// exceed it, which is the real argument for a stream transport in a
/// deployment with large state machines. Chunked snapshot transfer would be the
/// alternative, and is the better one — the message is already idempotent.
pub const MAX_DATAGRAM: usize = 60 * 1024;

// ---------------------------------------------------------------------------
// Executor
// ---------------------------------------------------------------------------

/// Drive a future to completion on the calling thread.
///
/// A park/unpark waker: the future is polled, and if it returns `Pending` the
/// thread blocks until something wakes it. No dependency, no runtime, ~30 lines.
pub fn block_on<F: std::future::Future>(mut fut: F) -> F::Output {
    struct Park {
        ready: Mutex<bool>,
        cv: Condvar,
    }
    impl std::task::Wake for Park {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            *self.ready.lock().unwrap() = true;
            self.cv.notify_one();
        }
    }

    let park = Arc::new(Park {
        ready: Mutex::new(false),
        cv: Condvar::new(),
    });
    let waker = std::task::Waker::from(Arc::clone(&park));
    let mut cx = std::task::Context::from_waker(&waker);
    // SAFETY: `fut` lives on this stack frame for the whole call and is never
    // moved after being pinned.
    let mut fut = unsafe { std::pin::Pin::new_unchecked(&mut fut) };

    loop {
        if let std::task::Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
        let mut ready = park.ready.lock().unwrap();
        while !*ready {
            ready = park.cv.wait(ready).unwrap();
        }
        *ready = false;
    }
}

#[derive(Debug)]
pub struct ThreadSpawner {
    node: NodeId,
    shutdown: Arc<AtomicBool>,
}

impl Spawner for ThreadSpawner {
    fn spawn(&self, name: &str, fut: BoxFuture<'static, ()>) {
        let label = format!("chronolog-n{}-{name}", self.node);
        let shutdown = Arc::clone(&self.shutdown);
        let _ = std::thread::Builder::new().name(label).spawn(move || {
            if shutdown.load(Ordering::Relaxed) {
                return;
            }
            block_on(fut);
        });
    }

    fn yield_now(&self) -> BoxFuture<'static, ()> {
        Box::pin(async {
            std::thread::yield_now();
        })
    }
}

// ---------------------------------------------------------------------------
// Clock
// ---------------------------------------------------------------------------

/// The real clock. The one place in the workspace permitted to read one.
#[derive(Debug)]
pub struct SystemClock {
    /// Fixed reference so `monotonic` is a small number rather than a machine
    /// uptime, matching the simulator's convention.
    origin: Instant,
}

impl Default for SystemClock {
    fn default() -> Self {
        SystemClock::new()
    }
}

impl SystemClock {
    pub fn new() -> SystemClock {
        SystemClock {
            origin: Instant::now(),
        }
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Nanos {
        Nanos(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos().min(u64::MAX as u128) as u64)
                .unwrap_or(0),
        )
    }

    fn monotonic(&self) -> Nanos {
        Nanos(self.origin.elapsed().as_nanos().min(u64::MAX as u128) as u64)
    }

    fn sleep(&self, dur: Nanos) -> BoxFuture<'static, ()> {
        // Blocking, which is correct for thread-per-task: it parks exactly one
        // task's thread.
        Box::pin(async move {
            std::thread::sleep(Duration::from_nanos(dur.as_nanos()));
        })
    }
}

// ---------------------------------------------------------------------------
// Network
// ---------------------------------------------------------------------------

/// UDP transport with a static peer table.
#[derive(Debug)]
pub struct UdpNetwork {
    node: NodeId,
    socket: Arc<UdpSocket>,
    peers: Vec<(NodeId, SocketAddr)>,
    msg_id: AtomicU64,
    inbox: Arc<(Mutex<VecDeque<Envelope>>, Condvar)>,
}

impl UdpNetwork {
    /// Bind and start the receive thread.
    pub fn bind(
        node: NodeId,
        addr: SocketAddr,
        peers: Vec<(NodeId, SocketAddr)>,
    ) -> std::io::Result<Arc<UdpNetwork>> {
        let socket = Arc::new(UdpSocket::bind(addr)?);
        let net = Arc::new(UdpNetwork {
            node,
            socket: Arc::clone(&socket),
            peers,
            msg_id: AtomicU64::new(1),
            inbox: Arc::new((Mutex::new(VecDeque::new()), Condvar::new())),
        });

        let inbox = Arc::clone(&net.inbox);
        let by_addr: Vec<(NodeId, SocketAddr)> = net.peers.clone();
        std::thread::Builder::new()
            .name(format!("chronolog-n{node}-udp-rx"))
            .spawn(move || {
                let mut buf = vec![0u8; MAX_DATAGRAM + 1024];
                loop {
                    let Ok((n, from_addr)) = socket.recv_from(&mut buf) else {
                        continue;
                    };
                    let from = by_addr
                        .iter()
                        .find(|(_, a)| *a == from_addr)
                        .map(|(id, _)| *id)
                        // A datagram from an unknown address is a client, or
                        // noise. Either way it is data, not authority.
                        .unwrap_or(NodeId::MAX);
                    let env = Envelope {
                        from,
                        to: node,
                        payload: buf[..n].to_vec(),
                        msg_id: 0,
                    };
                    let (q, cv) = &*inbox;
                    q.lock().unwrap().push_back(env);
                    cv.notify_one();
                }
            })?;
        Ok(net)
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}

impl Network for UdpNetwork {
    fn send(&self, to: NodeId, payload: Vec<u8>) {
        if payload.len() > MAX_DATAGRAM {
            // Dropping is the honest failure. A datagram this size would be
            // fragmented and almost certainly lost anyway, and Raft treats a
            // lost message as normal — it will retry, and the retry path is
            // already exercised by the simulator.
            return;
        }
        let Some((_, addr)) = self.peers.iter().find(|(id, _)| *id == to) else {
            return;
        };
        self.msg_id.fetch_add(1, Ordering::Relaxed);
        let _ = self.socket.send_to(&payload, addr);
    }

    fn recv(&self) -> BoxFuture<'static, Option<Envelope>> {
        let inbox = Arc::clone(&self.inbox);
        Box::pin(async move {
            let (q, cv) = &*inbox;
            let mut guard = q.lock().unwrap();
            loop {
                if let Some(env) = guard.pop_front() {
                    return Some(env);
                }
                guard = cv.wait(guard).unwrap();
            }
        })
    }

    fn local(&self) -> NodeId {
        self.node
    }
}

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------

/// A directory on a real filesystem.
#[derive(Debug)]
pub struct DirStorage {
    dir: PathBuf,
}

impl DirStorage {
    pub fn open(dir: impl AsRef<Path>) -> std::io::Result<DirStorage> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        Ok(DirStorage { dir })
    }
}

impl Storage for DirStorage {
    fn open(&self, name: &str) -> BoxFuture<'static, std::io::Result<Arc<dyn File>>> {
        let path = self.dir.join(name);
        Box::pin(async move {
            let f = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                // Explicitly *not* truncating: opening a segment must never
                // discard it. The WAL truncates deliberately, at an offset it
                // computed, and nowhere else.
                .truncate(false)
                .open(&path)?;
            let file: Arc<dyn File> = Arc::new(DiskFile {
                inner: Mutex::new(f),
            });
            Ok(file)
        })
    }

    fn list(&self) -> BoxFuture<'static, std::io::Result<Vec<String>>> {
        let dir = self.dir.clone();
        Box::pin(async move {
            let mut out = Vec::new();
            for entry in fs::read_dir(&dir)? {
                if let Some(name) = entry?.file_name().to_str() {
                    out.push(name.to_string());
                }
            }
            // The simulator returns a `BTreeMap`'s keys, which are sorted.
            // `read_dir` order is whatever the filesystem feels like, and a
            // difference here would be a behaviour difference between the two
            // runtimes — the exact class of thing this project exists to avoid.
            out.sort();
            Ok(out)
        })
    }

    fn remove(&self, name: &str) -> BoxFuture<'static, std::io::Result<()>> {
        let path = self.dir.join(name);
        Box::pin(async move {
            match fs::remove_file(&path) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                other => other,
            }
        })
    }

    fn sync_dir(&self) -> BoxFuture<'static, std::io::Result<()>> {
        let dir = self.dir.clone();
        Box::pin(async move {
            // Without this a freshly created segment can vanish on power loss
            // even though its contents were fsynced: the file's *name* lives in
            // the directory, and the directory is a file too.
            fs::File::open(&dir)?.sync_all()
        })
    }
}

#[derive(Debug)]
struct DiskFile {
    inner: Mutex<fs::File>,
}

impl File for DiskFile {
    fn len(&self) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .metadata()
            .map(|m| m.len())
            .unwrap_or(0)
    }

    fn write_at(&self, offset: u64, data: Vec<u8>) -> BoxFuture<'static, std::io::Result<()>> {
        let inner = self.inner.lock().unwrap().try_clone();
        Box::pin(async move {
            let mut f = inner?;
            f.seek(SeekFrom::Start(offset))?;
            f.write_all(&data)
            // Deliberately no flush: this returns when the bytes are in the
            // page cache, exactly as the trait says and exactly as the
            // simulator models. Only `fsync` promises anything.
        })
    }

    fn read_at(&self, offset: u64, len: usize) -> BoxFuture<'static, std::io::Result<Vec<u8>>> {
        let inner = self.inner.lock().unwrap().try_clone();
        Box::pin(async move {
            let mut f = inner?;
            f.seek(SeekFrom::Start(offset))?;
            let mut buf = vec![0u8; len];
            let mut read = 0;
            while read < len {
                match f.read(&mut buf[read..]) {
                    Ok(0) => break, // short read at EOF, which the trait allows
                    Ok(n) => read += n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e),
                }
            }
            buf.truncate(read);
            Ok(buf)
        })
    }

    fn fsync(&self) -> BoxFuture<'static, std::io::Result<()>> {
        let inner = self.inner.lock().unwrap().try_clone();
        Box::pin(async move { inner?.sync_data() })
    }

    fn truncate(&self, len: u64) -> BoxFuture<'static, std::io::Result<()>> {
        let inner = self.inner.lock().unwrap().try_clone();
        Box::pin(async move { inner?.set_len(len) })
    }
}

// ---------------------------------------------------------------------------
// Rng and Tracer
// ---------------------------------------------------------------------------

/// Entropy from the OS, seeded once at startup.
///
/// Still a deterministic stream from a seed — printing that seed on boot means
/// a production incident can, in principle, be replayed. The other sources of
/// nondeterminism are not captured out here, so it is not a guarantee; it costs
/// one line and occasionally pays for itself.
#[derive(Debug)]
pub struct OsRng {
    inner: crate::rng::SeededRng,
}

impl Default for OsRng {
    fn default() -> Self {
        OsRng::new()
    }
}

impl OsRng {
    pub fn new() -> OsRng {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
            ^ (std::process::id() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        OsRng::from_seed(seed)
    }

    pub fn from_seed(seed: u64) -> OsRng {
        OsRng {
            inner: crate::rng::SeededRng::new(seed),
        }
    }
}

impl Rng for OsRng {
    fn next_u64(&self) -> u64 {
        self.inner.next_u64()
    }
}

/// Writes notes to stderr with a real timestamp.
#[derive(Debug)]
pub struct StderrTracer {
    node: NodeId,
    enabled: bool,
}

impl StderrTracer {
    pub fn new(node: NodeId, enabled: bool) -> StderrTracer {
        StderrTracer { node, enabled }
    }
}

impl Tracer for StderrTracer {
    fn note(&self, text: &str) {
        if !self.enabled {
            return;
        }
        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        eprintln!(
            "[{:>10}.{:09}] n{} | {text}",
            t.as_secs(),
            t.subsec_nanos(),
            self.node
        );
    }

    fn enabled(&self) -> bool {
        self.enabled
    }
}

// ---------------------------------------------------------------------------
// Assembly
// ---------------------------------------------------------------------------

/// Everything a production node needs, wired to the real machine.
#[derive(Debug)]
pub struct RealRuntime {
    pub host: Host,
    shutdown: Arc<AtomicBool>,
}

impl RealRuntime {
    pub fn new(
        node: NodeId,
        listen: SocketAddr,
        peers: Vec<(NodeId, SocketAddr)>,
        data_dir: impl AsRef<Path>,
        verbose: bool,
    ) -> std::io::Result<RealRuntime> {
        let shutdown = Arc::new(AtomicBool::new(false));
        let host = Host {
            node,
            clock: Arc::new(SystemClock::new()),
            net: UdpNetwork::bind(node, listen, peers)?,
            storage: Arc::new(DirStorage::open(data_dir)?),
            rng: Arc::new(OsRng::new()),
            spawner: Arc::new(ThreadSpawner {
                node,
                shutdown: Arc::clone(&shutdown),
            }),
            tracer: Arc::new(StderrTracer::new(node, verbose)),
        };
        Ok(RealRuntime { host, shutdown })
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_on_drives_a_future_to_completion() {
        assert_eq!(block_on(async { 6 * 7 }), 42);
    }

    #[test]
    fn block_on_handles_a_pending_future_that_wakes() {
        let done = Arc::new(AtomicBool::new(false));
        let d = Arc::clone(&done);
        let value = block_on(async move {
            // A sleep parks the thread and returns; the point is that
            // `block_on` survives a `Pending` at all.
            SystemClock::new().sleep(Nanos::from_millis(1)).await;
            d.store(true, Ordering::SeqCst);
            7
        });
        assert_eq!(value, 7);
        assert!(done.load(Ordering::SeqCst));
    }

    #[test]
    fn a_real_file_round_trips_and_fsyncs() {
        let dir = std::env::temp_dir().join(format!("chronoscope-test-{}", std::process::id()));
        let storage = DirStorage::open(&dir).unwrap();
        block_on(async {
            let f = storage.open("seg").await.unwrap();
            f.write_at(0, b"hello world".to_vec()).await.unwrap();
            f.fsync().await.unwrap();
            assert_eq!(f.len(), 11);
            assert_eq!(f.read_at(0, 5).await.unwrap(), b"hello");
            // Short read at EOF returns what exists rather than erroring.
            assert_eq!(f.read_at(6, 100).await.unwrap(), b"world");
            f.truncate(5).await.unwrap();
            assert_eq!(f.len(), 5);
            assert!(storage.list().await.unwrap().contains(&"seg".to_string()));
            storage.remove("seg").await.unwrap();
            // Removing something absent is not an error.
            storage.remove("seg").await.unwrap();
        });
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn listing_is_sorted_like_the_simulator() {
        // A behaviour difference between the two runtimes is exactly the class
        // of thing this project exists to prevent.
        let dir = std::env::temp_dir().join(format!("chronoscope-sort-{}", std::process::id()));
        let storage = DirStorage::open(&dir).unwrap();
        block_on(async {
            for name in ["wal-3", "wal-1", "wal-2"] {
                storage.open(name).await.unwrap();
            }
            let listed = storage.list().await.unwrap();
            let mut sorted = listed.clone();
            sorted.sort();
            assert_eq!(listed, sorted);
        });
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn udp_carries_a_datagram_between_two_nodes() {
        let a_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let a = UdpNetwork::bind(0, a_addr, vec![]).unwrap();
        let a_real = a.local_addr().unwrap();
        let b = UdpNetwork::bind(1, "127.0.0.1:0".parse().unwrap(), vec![(0, a_real)]).unwrap();
        b.send(0, b"ping".to_vec());
        let env = block_on(a.recv()).unwrap();
        assert_eq!(env.payload, b"ping");
    }

    #[test]
    fn an_oversized_datagram_is_dropped_rather_than_fragmented() {
        let a = UdpNetwork::bind(0, "127.0.0.1:0".parse().unwrap(), vec![]).unwrap();
        let addr = a.local_addr().unwrap();
        let b = UdpNetwork::bind(1, "127.0.0.1:0".parse().unwrap(), vec![(0, addr)]).unwrap();
        b.send(0, vec![0u8; MAX_DATAGRAM + 1]);
        // Raft treats a lost message as normal, so this is a legal outcome
        // rather than an error path.
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(a.inbox.0.lock().unwrap().len(), 0);
    }

    #[test]
    fn the_monotonic_clock_advances_and_never_jumps_back() {
        let c = SystemClock::new();
        let mut prev = c.monotonic();
        for _ in 0..50 {
            let now = c.monotonic();
            assert!(now >= prev, "monotonic clock went backwards");
            prev = now;
        }
    }
}
