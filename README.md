# Chronoscope

**A deterministic simulation testbed, and a replicated log built inside it.**

Two things that only make sense together:

**`chrono-sim`** is a deterministic simulation runtime. A single-threaded
scheduler where the network, the disk, the clocks, and thread interleaving are
all virtualized behind traits and driven by one seeded PRNG. Given a seed, a
run is bit-for-bit reproducible.

**`chronolog`** is a real Raft-replicated, crash-consistent write-ahead log
written against those traits. It never names a socket, a file descriptor, a
clock, or a thread — so the same code runs against the simulator's virtual
world and against real hardware, unchanged.

The payoff is [`BUGS.md`](BUGS.md): nine real consensus and storage bugs the
simulator found in this implementation, each with what it looked like, what it
actually was, and why it was hard to see. Eight fixed, one open.

```
$ chronoscope swarm --seeds 200 --secs 400

  200/200 runs  0 failures  87 node-hours  95s elapsed

--- swarm complete ---
  simulated       86.7 node-hours
  wall clock      95.07s
  compression     3282x
```

---

## Why bother

Because time is virtual, nothing ever waits. A 30-second election timeout costs
a heap pop, so thousands of simulated node-hours compress into seconds of wall
clock: **~3,300x** on this workload.

Because runs are deterministic, a violation found at seed `0x8f3a2b1c` after
four simulated hours replays in a second, under a debugger, as many times as
you like.

Deterministic simulation testing is how FoundationDB survived Jepsen without a
single bug found, how TigerBeetle is built, and what Antithesis sells. This is
that technique, applied to a Raft implementation written from the paper, with
the bug ledger to show for it.

---

## Try it

```bash
cargo build --release

# One execution, checked by every oracle.
./target/release/chronoscope run --seed 0x8f3a2b1c

# The identical execution, with a timeline of what the universe did to it.
./target/release/chronoscope replay --seed 0x8f3a2b1c

# The determinism guard: each seed twice, event-trace hashes diffed.
./target/release/chronoscope check --seeds 64

# The swarm. Any failure is filed with the command that reproduces it.
./target/release/chronoscope swarm --seeds 10000 -j 32

# Throughput and latency on a quiet network.
./target/release/chronoscope bench --clients 256
```

A failing seed writes an artifact containing the exact reproduction command,
what failed, the run summary, and the tail of the event trace.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  chrono-oracle    linearizability · Raft invariants ·        │
│                   liveness watchdog · scenario harness       │
├─────────────────────────────────────────────────────────────┤
│  chronolog        Raft · segmented WAL · MVCC KV · client    │
│                   ── writes only against the traits below ── │
├─────────────────────────────────────────────────────────────┤
│  chrono-sim       Clock · Network · Storage · Rng · Spawner  │
│                   sim implementations │ real implementations │
└─────────────────────────────────────────────────────────────┘
```

### Layer 1 — `chrono-sim`

The kernel is four lines, and everything follows from them:

```text
loop {
    if any task is runnable -> poll one, chosen by the PRNG
    else                    -> jump virtual time to the next event and fire it
    if neither              -> the run is over
}
```

Task interleaving is a PRNG draw rather than an OS decision, via a hand-written
`Waker`. That is what makes a run reproducible down to the order two tasks were
polled in.

What the universe is allowed to do to you:

- **Network** — per-link latency distributions, loss, duplication, reordering,
  asymmetric partitions, whole-cluster splits
- **Disk** — sector-granular torn writes on power loss, lost un-fsynced writes,
  `fsync` as the *only* durability barrier, latency spikes, `ENOSPC`
- **Clocks** — per-node skew and drift, so nodes genuinely disagree
- **Process** — hard crash with task reaping, restart, `SIGSTOP`-style freeze

**No floats anywhere.** Sampling latency as `-mean * ln(u)` is the natural
choice and it is wrong here: `f64::ln` is not correctly rounded and differs
between libm versions and between aarch64 and x86-64. A seed that reproduced on
a laptop would diverge in CI, defeating the entire point. Every distribution is
integer-only.

### Layer 2 — `chronolog`

Raft is a **pure state machine**. It performs no I/O, holds no `Host`, and never
awaits: inputs go in, a `Ready` comes out, and the driver does the persisting
and sending.

That is not a style choice. Raft's correctness depends on persisting before
sending — a vote must be durable before the `VoteResp` leaves — and a `Ready`
whose `messages` field the driver cannot reach until it has handled
`hard_state` and `entries` makes that ordering structural rather than a comment
someone deletes.

It also means all of consensus is testable with a `BTreeMap` of nodes and a
`Vec` of in-flight messages. No async, no simulator, no sleeps. Figure 8, a
disruptive rejoin, a joint transition between disjoint voter sets — all
constructible by hand.

Implemented from the paper: pre-vote elections, log replication with
term-based conflict backtracking, the §5.4.2 commit restriction, snapshots and
compaction, joint-consensus membership changes, `ReadIndex` and lease reads,
leadership transfer, check-quorum step-down.

The WAL is append-only segments with a CRC32C per record. Recovery stops at the
first bad record and truncates there — it does **not** skip forward looking for
the next record that checksums. A short log is repairable by the leader; a log
that silently skips an index is undetectably wrong.

### Layer 3 — the oracles

Three, each answering a different question:

| Oracle | Question | Catches |
|---|---|---|
| Linearizability | Did it look correct from outside? | Stale reads, lost writes, phantom values |
| Raft invariants | Is it correct inside? | Election Safety, Log Matching, Leader Completeness, State Machine Safety |
| Liveness watchdog | Is it doing anything at all? | Deadlock, stranded followers, stalled commits |

The invariant oracle is an omniscient observer reading every node's log at
once — impossible in production, trivial in a simulator. That asymmetry is much
of why deterministic simulation finds things integration tests do not.

The liveness watchdog earns its place on its own: [CS-006](BUGS.md) is a
follower stranded permanently by one dropped packet, violating **no safety
property whatsoever**. Every log matched. Nothing was overwritten. The cluster
was quietly one node from data loss and only a liveness oracle could see it.

The linearizability checker is Wing & Gong with per-key decomposition and
memoization, encoding state as `(frontier, mask of operations placed ahead of
it)` — small and hashable regardless of history length, so a 15,000-operation
history is checked directly rather than in windows. (Windowing is the obvious
alternative and is quietly unsound; see `BUGS.md`.)

---

## Measurements

All from this repository, on an M-series laptop.

```
$ chronoscope bench --clients 256 --secs 30

  writes           229334
  throughput       7398 writes/sec
  latency p50      30.31 ms
  latency p99      100.46 ms
  entries/fsync    7.9        <- group commit
```

Group commit falls out of the driver's structure: it waits for one event, then
drains every other event already queued, and the whole burst becomes one append
and one `fsync`. At 256 clients that is ~8 entries per durability barrier.

**On the throughput number.** The spec target of 50k–150k writes/sec is a
figure for real `io_uring` on real hardware. This is a simulated disk whose
`fsync` distribution is deliberately pessimistic (200–800µs typical, with a
5ms tail at p95 and 90ms at p99.5) and a driver that takes one barrier per
cycle — so ~2,000 cycles/sec × ~8 entries is the ceiling being measured. It is
an honest number for what it measures, and it is a measurement of the
simulator's disk model as much as of the code.

Halving the barriers was worth 18%: Raft only requires `term` and `vote` to be
durable, so the commit index is no longer fsynced on every cycle.

---

## Determinism, and how it is enforced

Every event folds into a rolling FNV-1a hash. `chronoscope check` runs each
seed twice and diffs. CI fails the build on any divergence.

A test deliberately smuggles `SystemTime::now()` into a task to confirm the
guard catches it — otherwise the guard is only asserted, not demonstrated.

Rules the codebase holds itself to:

1. Core crates take **zero third-party dependencies**. Every dependency is
   ambient nondeterminism waiting to happen.
2. `HashMap`/`HashSet` are banned in simulated paths; their iteration order is
   randomized per process.
3. `SystemTime::now()` and `Instant::now()` appear only in the `real` runtime.
4. Every random decision draws from the one PRNG the kernel owns.
5. No floats in any sampled distribution.

---

## Layout

```
crates/
  chrono-sim/       the deterministic core     (zero deps)
  chronolog/        Raft, WAL, KV, client      (zero deps)
  chrono-oracle/    the oracles and harness    (zero deps)
  chronoscope/      the CLI
  chronolog-server/ the production binary
```

```bash
cargo test --workspace              # 192 tests
cargo test -- --ignored             # the open bug, CS-009
```

---

## Status

Layers 1–3 and the swarm are complete and green. The production runtime
(`io_uring` storage, Prometheus, `/debug`) and the deployment manifests are
scaffolded; see [`task.md`](task.md) for the running build log, including the
decisions and the dead ends.
