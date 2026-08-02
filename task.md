# Chronoscope — build ledger

Living document, updated as work lands. Status: `[ ]` todo · `[~]` in progress · `[x]` done · `[!]` deferred, with reason.

## Ground rules adopted

1. **Core crates take zero third-party dependencies.** `chrono-sim`, `chronolog`, and `chrono-oracle` compile against `std` alone. Every dependency is ambient nondeterminism waiting to happen (hash seeds, thread pools, clocks behind an abstraction). Deps are allowed only in the binaries.
2. **`std::collections::HashMap` is banned in simulated paths.** Its iteration order is randomized per process. `BTreeMap`/`Vec` everywhere iterable.
3. **`SystemTime::now()` / `Instant::now()` are banned outside the `real` runtime module.** Enforced by a CI grep.
4. **Every random decision draws from one PRNG owned by the kernel.** No component holds its own entropy.
5. Host is arm64 macOS; `io_uring` is Linux-only, so the production storage path is feature-gated with a portable `std` fallback behind the same trait.

## Layer 0 — workspace

- [x] Cargo workspace, crates, pinned toolchain
- [x] `rustfmt.toml`, `.gitignore`, licence

## Layer 1 — `chrono-sim` (deterministic core)

- [x] Trait surface: `Clock`, `Rng`, `Network`, `Storage`/`File`, `Spawner`
- [x] Seeded PRNG (SplitMix64 seeding → xoshiro256\*\*), no float paths
- [x] Event kernel: binary heap over `(virtual_time, seq, Event)`, virtual time jumps
- [x] Custom async executor: hand-written `Waker` via `RawWakerVTable`, PRNG-chosen poll order
- [x] Virtual clock: per-node skew + drift
- [x] Virtual network: per-link latency, drops, duplication, reordering, asymmetric partitions, heal
- [x] Virtual disk: sector-granular torn writes, `fsync` reordering, power-loss partial writes, latency spikes, `ENOSPC`
- [x] Process lifecycle: kill / restart, task reaping, crash semantics on unsynced pages
- [x] Fault policy sampled from the same PRNG
- [x] Trace recorder + rolling trace hash
- [x] Determinism guard — run each seed twice, diff rolling hash
- [ ] Real runtime implementations of every trait

## Layer 2 — `chronolog` (system under test)

- [x] Wire codec: length-prefixed framing, hand-rolled encode/decode
- [x] Segmented WAL: append-only segments, per-record CRC32C, group commit, `fsync` batching
- [x] WAL crash recovery: CRC scan, torn-tail truncation, segment rollover
- [x] Raft core: persistent state, log matching, election with pre-vote
- [x] Raft replication: `AppendEntries`, `nextIndex`/`matchIndex`, quorum commit advance
- [x] Snapshotting + log compaction + `InstallSnapshot`
- [x] Membership changes via joint consensus (`C_old,new` → `C_new`)
- [x] Linearizable reads: leader lease + `ReadIndex`
- [ ] KV state machine with per-key MVCC
- [~] Client session layer: types + codec done; dedup table lands with the KV state machine

## Layer 3 — oracles

- [ ] Linearizability checker (Wing & Gong, memoised, per-key decomposition)
- [ ] Raft invariants: Election Safety, Log Matching, Leader Completeness, State Machine Safety
- [ ] Liveness watchdog
- [ ] Durability oracle: acknowledged writes survive crash

## Layer 4 — production runtime

- [ ] `chronolog-server` binary on the `real` traits
- [ ] Prometheus `/metrics`, `/debug/raft`, `/health`
- [ ] `io_uring` storage backend, feature-gated for Linux

## Layer 5 — the swarm

- [ ] `chronoscope run --seed` / `replay` / `swarm` / `check`
- [ ] Swarm: N seeds across J workers, failing seeds as artifacts
- [ ] GitHub Actions: determinism guard + swarm matrix

## Evidence

- [ ] `BUGS.md` — bug ledger, each entry with a reproducing seed
- [ ] `README.md` with architecture + demo script
- [ ] Benchmarks: throughput / p99

---

## Work log

### 2026-08-02 — Layer 2, part 1 (`9db8574`, `5fb8182`)

**WAL + Raft are in and green: 75 tests across the two crates.**

The WAL's headline property, checked over 60 randomized seeds against a
disk that tears 40% and loses 30% of un-fsynced writes: *the recovered log
is always a prefix of what was appended, and always includes everything a
completed fsync acknowledged.*

Raft is a pure state machine (`step` → `Ready`), so all of consensus is
testable with a `BTreeMap` of nodes and a `Vec` of messages — no async, no
simulator, no sleeps. 26 scenarios including figure 8, disruptive rejoin,
and a joint transition between disjoint voter sets.

**Two real bugs, caught the moment the harness ran:**

1. `Raft::new` bootstrapped the config through `install_snapshot` at index 0.
   `term_at(0)` returns the sentinel term 0, which *matches* the snapshot's
   term, so the install took its compaction path — and compacting to index 0
   early-returns. The config was silently dropped, no node was a voter, and no
   election ever started. Fixed with an explicit `Log::bootstrap`.
2. `Config::has_quorum` treated "a majority of the empty set" as vacuously
   true, so an unconfigured node considered every decision unanimous. That
   turns a bootstrap mistake into a safety violation rather than a hang.

Neither needed the simulator — which is the argument for the pure-state-machine
split. Cheap tests should catch cheap bugs; the seeds are for the executions
nobody would think to write down.

**Notes:**
- A torn write only exercises the CRC if records *span sectors*. With 50-byte
  entries and 512-byte sectors, tearing drops whole records and leaves a
  legitimately clean, shorter log. That test now writes 8 KiB entries.
- The pre-vote test has a control (`pre_vote: false`) asserting the disruption
  is real. Without it the test proves nothing.
- `Raft` keeps a stirred entropy word rather than taking `rand` at every call
  site: campaigns can start from internal transitions (a pre-vote succeeding, a
  `TimeoutNow`) where no fresh value is to hand, and always picking the minimum
  timeout there would make two nodes split every vote identically.

### 2026-08-02 — Layer 1 landed (`20cd6f1`)

**`chrono-sim` is done and green: 41 tests.** The kernel loop is four lines and
everything falls out of them: virtual time jumps to the next event, and task
interleaving is a PRNG draw rather than an OS decision.

Measured, not asserted:
- 24 simulated hours of a sleeping node run in ~200ms of wall clock.
- A 5-node cluster under `nemesis` for 120 simulated seconds produces ~90k
  events; the same seed reproduces the trace hash, stats, and end time exactly.
- The determinism guard catches a task that smuggles in `SystemTime::now()`.
- fsynced data survives power loss; un-fsynced data does not; torn writes land
  on sector boundaries.

Decisions worth recording:
- **Raft will be a pure state machine** (`step(input) -> Ready`), with all I/O
  in a driver. etcd-raft's shape. It makes the persist-before-send ordering
  explicit rather than incidental, and that ordering is where real bugs live.
- **`Host::spawn_with`** exists because `host.spawn("t", async move { host.. })`
  cannot borrow-check. Every task needs a `Host`, so the clone-into-closure form
  is the one that gets used.
- **The chaos ticker only arms when a chaos rate is non-zero.** Otherwise the
  event heap is never empty, `Quiesced` is unreachable, and a benign 48-hour run
  burns 1.7M no-op events.
- **Partitions are evaluated when a packet enters the link**, not when it
  leaves. A packet already in flight when a partition begins still arrives,
  which is what a real network does and is a useful source of reordering.

### 2026-08-02 — setup

- Verified toolchain: rustc 1.94.0, arm64 macOS. crates.io reachable.
- Target repo `~/Desktop/ChronoScope` was an empty git repo on `main`, remote `git@github.com:IshaanNene/ChronoScope.git`.
- Adopted the zero-dependency rule for core crates (see Ground rules).
</content>
