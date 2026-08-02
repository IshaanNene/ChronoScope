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
- [x] Real runtime implementations of every trait

## Layer 2 — `chronolog` (system under test)

- [x] Wire codec: length-prefixed framing, hand-rolled encode/decode
- [x] Segmented WAL: append-only segments, per-record CRC32C, group commit, `fsync` batching
- [x] WAL crash recovery: CRC scan, torn-tail truncation, segment rollover
- [x] Raft core: persistent state, log matching, election with pre-vote
- [x] Raft replication: `AppendEntries`, `nextIndex`/`matchIndex`, quorum commit advance
- [x] Snapshotting + log compaction + `InstallSnapshot`
- [x] Membership changes via joint consensus (`C_old,new` → `C_new`)
- [x] Linearizable reads: leader lease + `ReadIndex`
- [x] KV state machine with per-key MVCC
- [x] Client session layer: idempotent request IDs, dedup table, `NotLeader` redirect, jittered backoff

## Layer 3 — oracles

- [x] Linearizability checker (Wing & Gong, memoised, per-key decomposition)
- [x] Raft invariants: Election Safety, Log Matching, Leader Completeness, State Machine Safety
- [x] Liveness watchdog
- [x] Durability oracle: acknowledged writes survive crash

## Layer 4 — production runtime

- [x] `chronolog-server` binary on the `real` traits
- [x] Prometheus `/metrics`, `/debug/raft`, `/health`
- [!] `io_uring` storage backend — **not built**, see the work log for why

## Layer 5 — the swarm

- [x] `chronoscope run --seed` / `replay` / `swarm` / `check`
- [x] Swarm: N seeds across J workers, failing seeds as artifacts
- [x] GitHub Actions: determinism guard + swarm matrix

## Deployment

- [x] Dockerfile (distroless, non-root)
- [x] Kubernetes StatefulSet + headless Service
- [x] Helm chart
- [x] Terraform (namespace, PDB, odd-replica validation)

## Layer 2 — membership under chaos

- [x] Admin wire path (`Wire::Admin`) so a controller can submit changes
- [x] Membership controller in the scenario harness: add/remove voters under chaos
- [x] New voters staged through a learner, measured at half the failure rate
- [x] Oracles taught the difference between "not a member" and "not converging"

## Layer 1 — fault modes that were modelled but never fired

- [x] `corrupt_ppm` — a `corrupting` preset; 2,610 corrupt frames, nothing broke
- [x] `enospc_after_bytes` — remodelled as live usage so compaction frees space,
      then a `diskfull` preset. Three bugs, each exposed by fixing the last.

## Layer 3 — the oracle that found the root cause

- [x] Durability oracle: what a node fsynced *and* committed must survive a
      restart. Found CS-016 in one line after three oracles had spent the whole
      project reporting its symptom.

## Known gaps

Audited against the spec in [`ROADMAP.md`](ROADMAP.md). The three that matter:

- [x] ~~The swarm never proposes a membership change~~ — done, and it found
      CS-010, CS-011, and CS-012 immediately.
- [x] ~~`corrupt_ppm` and `enospc_after_bytes` never fire~~ — both now have
      presets. ENOSPC needed a modelling fix first: it counted cumulative bytes
      written, so a node that tripped the quota once could never write again.
- [x] ~~CS-009 and CS-012 open, probably one cause~~ — they were one cause,
      CS-016, and both now pass as regression tests.
- [ ] CS-018 open: a residual 1-2 entry durable-and-committed loss, 2-5% of
      seeds, not a sampling artifact. Suspect CS-003's clamp masks it.
- [ ] The Helm chart has no templates; throughput is 7.4k/sec against a
      50k-150k target.

## Evidence

- [x] `BUGS.md` — bug ledger, each entry with a reproducing seed
- [x] `README.md` with architecture + demo script
- [x] Benchmarks: throughput / p99

---

## Work log

### 2026-08-02 — Layers 3–5, and the bug hunt (`c915553`, `3c9804c`, + this)

**Done and green: 199 tests, 0 clippy warnings, a 150-seed swarm at 0 failures.**

Measured, on this machine:

| | |
|---|---|
| swarm | 150 seeds, 65.0 node-hours simulated in 70.6s — **3315x** |
| determinism | 48 seeds x2, every trace hash identical |
| throughput | 7,398 writes/sec, p50 30ms, 7.9 entries/fsync |
| real cluster | 3 nodes on real UDP + real `fsync`, leader elected in term 1 |

**Nine bugs, eight fixed, one open — all in [`BUGS.md`](BUGS.md).** The pattern
worth recording: *almost none of them presented anywhere near their cause.* A
follower that silently stopped replicating turned out to be a WAL gap written
four minutes earlier. Two replicas disagreeing at index 7567 turned out to be
the `ReadIndex` heartbeat path. The diagnostic technique that actually worked
was always the same — assert the invariant at the point it should hold, not
where the symptom appears — and it is why the driver now reconciles the WAL
against the log every cycle instead of trusting a watermark.

**Two of the nine were bugs in the oracles**, which is worth being loud about.
A checker that reports a correct history as a violation is worse than no
checker: in the first 400-seed swarm the false positives buried every real
failure. Both are fixed with regression tests reduced from the runs that
exposed them.

**The biggest single lesson** came from the first scenario run: it finished in
0.27 seconds and reported everything clean. The workload was ending after a
fixed operation count — a few simulated seconds — so a chaos policy quoted in
events-per-second never fired once. A green result meant nothing had happened,
not that nothing was wrong. `duration` now drives the run and
`max_ops_per_client` is only a memory cap.

**Decisions worth recording:**
- **`io_uring` was not built.** It is Linux-only, this was written on macOS,
  and a storage backend that cannot be tested on the machine that wrote it is
  worse than an honest `std::fs` one. The seam is the `Storage` trait — an
  `io_uring` backend is a new type implementing four methods with nothing above
  it changing, which is the whole argument for the trait boundary. Claiming it
  worked would have been the one dishonest thing in the repository.
- **The real transport is UDP, not TCP.** `Network` is a datagram interface
  because that is the model Raft is specified against; UDP implements it
  directly rather than having a reliable stream pretend. The real cost is
  `InstallSnapshot` above the datagram limit, which is documented and argues
  for chunked snapshot transfer rather than for TCP.
- **Hard state is only fsynced when term or vote changes.** Raft requires those
  to be durable; the commit index is an optimization. Worth 18% throughput.
- **Seeds are only reproducible for a fixed binary.** Changing the code shifts
  the PRNG draws, so a seed is a handle on a bug *at a commit*. `BUGS.md` says
  so out loud rather than letting half its reproductions quietly rot.

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
