# Chronoscope — build ledger

Living document, updated as work lands. Status: `[ ]` todo · `[~]` in progress · `[x]` done · `[!]` deferred, with reason.

## Ground rules adopted

1. **Core crates take zero third-party dependencies.** `chrono-sim`, `chronolog`, and `chrono-oracle` compile against `std` alone. Every dependency is ambient nondeterminism waiting to happen (hash seeds, thread pools, clocks behind an abstraction). Deps are allowed only in the binaries.
2. **`std::collections::HashMap` is banned in simulated paths.** Its iteration order is randomized per process. `BTreeMap`/`Vec` everywhere iterable.
3. **`SystemTime::now()` / `Instant::now()` are banned outside the `real` runtime module.** Enforced by a CI grep.
4. **Every random decision draws from one PRNG owned by the kernel.** No component holds its own entropy.
5. Host is arm64 macOS; `io_uring` is Linux-only, so the production storage path is feature-gated with a portable `std` fallback behind the same trait.

## Layer 0 — workspace

- [ ] Cargo workspace, crates, pinned toolchain
- [ ] `rustfmt.toml`, `.gitignore`, licence

## Layer 1 — `chrono-sim` (deterministic core)

- [ ] Trait surface: `Clock`, `Rng`, `Network`, `Storage`/`File`, `Spawner`
- [ ] Seeded PRNG (SplitMix64 seeding → xoshiro256\*\*), no float paths
- [ ] Event kernel: binary heap over `(virtual_time, seq, Event)`, virtual time jumps
- [ ] Custom async executor: hand-written `Waker` via `RawWakerVTable`, PRNG-chosen poll order
- [ ] Virtual clock: per-node skew + drift
- [ ] Virtual network: per-link latency, drops, duplication, reordering, asymmetric partitions, heal
- [ ] Virtual disk: sector-granular torn writes, `fsync` reordering, power-loss partial writes, latency spikes, `ENOSPC`
- [ ] Process lifecycle: kill / restart, task reaping, crash semantics on unsynced pages
- [ ] Fault policy sampled from the same PRNG
- [ ] Trace recorder + rolling trace hash
- [ ] Determinism guard — run each seed twice, diff rolling hash
- [ ] Real runtime implementations of every trait

## Layer 2 — `chronolog` (system under test)

- [ ] Wire codec: length-prefixed framing, hand-rolled encode/decode
- [ ] Segmented WAL: append-only segments, per-record CRC32C, group commit, `fsync` batching
- [ ] WAL crash recovery: CRC scan, torn-tail truncation, segment rollover
- [ ] Raft core: persistent state, log matching, election with pre-vote
- [ ] Raft replication: `AppendEntries`, `nextIndex`/`matchIndex`, quorum commit advance
- [ ] Snapshotting + log compaction + `InstallSnapshot`
- [ ] Membership changes via joint consensus (`C_old,new` → `C_new`)
- [ ] Linearizable reads: leader lease + `ReadIndex`
- [ ] KV state machine with per-key MVCC
- [ ] Client session layer: idempotent request IDs, dedup table, `NotLeader` redirect

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

### 2026-08-02

- Verified toolchain: rustc 1.94.0, arm64 macOS. crates.io reachable.
- Target repo `~/Desktop/ChronoScope` was an empty git repo on `main`, remote `git@github.com:IshaanNene/ChronoScope.git`.
- Adopted the zero-dependency rule for core crates (see Ground rules).
</content>
