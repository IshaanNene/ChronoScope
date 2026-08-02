# ROADMAP.md

Pending work, audited against the original specification and ordered by what
actually advances the goal.

## The goal, restated

The deliverable is not the code. It is **[`BUGS.md`](BUGS.md)** — a ledger of
real consensus and storage bugs this simulator found in this implementation,
each with a reproducing seed. Everything below is ranked by one question:

> **Does this find more real bugs, or make the ones already found more
> convincing?**

Work that does neither is listed last, honestly labelled, and should probably
stay unbuilt. A simulator with more features and fewer findings is a worse
artifact than this one.

Current state: **18 bugs, 16 fixed, 2 open.** 205 tests, 0 clippy warnings.

Swarm results by fault mode, 200 seeds each, 400 simulated seconds:

| Preset | Failures | Of which durability (CS-018) |
|---|---|---|
| `nemesis` (static) | 8 / 200 | 8 |
| `corrupting` | 2 / 200 | 2 |
| `diskfull` | 16 / 200 | 5 |
| membership, every 15s | 18 / 200 | 4 |

The counts went *up* this round because a new oracle started reporting a class
of failure nothing was previously looking for. That is the right direction: an
oracle that finds nothing is indistinguishable from one that is not running.

---

# P0 — the fault surface the swarm never touches

These are the highest-value items in the file. Each one is a *class of
execution the simulator currently cannot reach*, which means any bug living
there is invisible today.

The first one is now done, and it is worth reading the result before picking
the next: enabling it found two real bugs within minutes, plus one open safety
violation and three mistakes in the oracle itself. The pattern generalises —
**the cheapest bugs to find are the ones behind a fault the swarm is not yet
allowed to inject.** P0.2 and P0.3 are both single constants.

## ~~P0.1 — The swarm never proposes a membership change~~ — **DONE**

**Done, and it paid immediately.** The swarm now runs a membership controller
that adds and removes voters under chaos, staging new voters through a learner
first. It found two real bugs within minutes ([CS-010](BUGS.md),
[CS-011](BUGS.md)), one open safety violation ([CS-012](BUGS.md)), three
mistakes in the liveness oracle, and a measured availability cost for
reconfiguration.

Remaining work in this area:
- [ ] Chase CS-012 — likely shares a cause with CS-009.
- [ ] The controller only ever grows via learner; removing a voter is still
      direct. Staging removals (demote to learner, then remove) is the
      symmetric improvement and is untested.
- [ ] `min_voters` is fixed at 3. Exercising a 1-voter cluster and the
      degenerate transitions around it would be worthwhile.

The original justification, kept because it is why this was ranked first:

**This was the single biggest gap in the project.**

`crates/chronolog/tests/raft.rs` exercises joint consensus by hand: entering
`C_old,new`, refusing overlapping transitions, requiring both majorities to
commit across disjoint voter sets. All of it passes. But
`chrono_oracle::scenario` — the harness the swarm actually runs — **never
proposes a configuration change at all.** Every one of those 150 seeds runs a
static three-node cluster.

That matters more than any other gap here, because the spec names this exact
risk:

> *"Raft membership changes. Joint consensus is where most hand-rolled Raft
> implementations are subtly wrong. Your simulator will prove yours is."*

It has not had the chance. Hand-written scenarios test the transitions someone
thought to write down; the swarm's entire value is the interleavings nobody
would. A joint transition *concurrent with* a leader crash, a partition landing
mid-transition, or a snapshot install while the configuration is in flight are
precisely the executions that are unreasonable to construct by hand and cheap
for a seeded PRNG to stumble into.

**Do this.** Add a membership workload to `ScenarioConfig`: periodically add
or remove a voter, drive `maybe_finish_config_change`, and let chaos run
throughout. Extend the invariant oracle to check quorum intersection across the
joint configuration. Expect bugs — this is where they are.

## ~~P0.2 — Wire corruption is modelled but never enabled~~ — **DONE**

A `corrupting` preset now flips bits in flight. The result is a **negative**
one and worth having: 2,610 corrupt frames in a 200-second run, nothing broken.
The CRC rejects them and Raft treats each as a lost message. One failure in 200
seeds, and it was the CS-009 shape rather than anything corruption-specific.

Remaining: this tests *rejection*, not Byzantine behaviour. A node that lies —
different entries to different peers, or a forged term — is a much larger
change, and Raft is not designed to survive it, so the interesting question is
what it degrades to. See P3.2.

The original reasoning:

## P0.2 (original) — Wire corruption is modelled but never enabled

`LinkPolicy::corrupt_ppm` exists, flips a bit in flight, and is set to `0` in
every preset. The decoder is tested against adversarial bytes in unit tests and
has never seen a corrupt frame arrive *in a running cluster*.

The interesting case is not "does the decoder panic" — that is covered. It is
what a node does when a *structurally valid but semantically wrong* message
arrives: a flipped bit inside a term number that survives the frame CRC because
the CRC was computed after the flip, or an `AppendEntries` whose `prev_index`
decodes to something plausible.

**Do this.** Enable `corrupt_ppm` in `torture`, add a `byzantine` preset, and
check that the invariant oracles still hold. Cheap to add, and it exercises a
path the whole protocol assumes cannot happen.

## ~~P0.3 — `ENOSPC` is modelled but never enabled~~ — **DONE**

The highest-yield item so far: **three bugs** (CS-013, CS-014, CS-015), each
one exposed by fixing the one before it.

It also needed a modelling fix first. `enospc_after_bytes` was checked against
*cumulative bytes ever written*, which never decreases — so a node that tripped
the quota once could never write again even after compaction deleted half its
segments. ENOSPC was a wall rather than pressure, every node died at the same
byte count, and the interesting question was never asked. Usage is now measured
live, so compaction genuinely frees space.

The prediction in the original text was right, and understated:

> *A leader that cannot append is a specific and nasty failure — it must step
> down rather than acknowledge, and nothing currently proves it does.*

It did not step down. It died. And the two bugs behind that one were only
reachable once it stopped dying.

Remaining: the `diskfull` swarm sits at 10 failures in 200 seeds, all liveness
stalls, which a full disk plausibly causes. Worth confirming rather than
assuming.

## P0.4 — DNS is not virtualized

The spec lists DNS among the things behind a trait. It is not. Peers are a
static table in both runtimes. Real clusters resolve peers, resolution fails,
and stale DNS points a node at an address that now belongs to a *different*
node — which is a genuinely interesting fault, not a chore.

Lower than the three above because it needs a new trait rather than flipping a
constant, but it is a real hole in the "every source of nondeterminism is
captured" claim.

## ~~P0.5 — Close CS-009 and CS-012~~ — **DONE**

Both closed, and they did share one cause: [CS-016](BUGS.md), a durability
barrier that covered only the active segment. A batch crossing a rollover left
the earlier segment's tail in the page cache while the node reported it durable
and acknowledged it to its leader. One missing `fsync` cost 455 entries.

The three reproductions were right to be treated as one bug.

**The method is the transferable part.** Three oracles had been reporting
divergent applied histories thousands of events downstream of the fault. What
found it was a fourth oracle checking the actual contract — *acknowledged means
durable* — which named the node and the two indices in a single line. Assert
the invariant where it should hold, not where the symptom appears.

It also turned up [CS-017](BUGS.md) immediately: `persisted` was inferred from
what Raft asked to be written and could only rise, so a node went on offering a
quorum index its disk no longer held.

### What is left: CS-018

A residual **one-to-two entry** loss, in every fault mode, at 2-5% of seeds.
Confirmed not to be a sampling artifact — narrowing the oracle's sampling
interval 125-fold reproduces it identically.

The leading suspicion is that [CS-003](BUGS.md)'s commit clamp is *masking* a
Leader Completeness violation rather than preventing one: it lowers
`commitIndex` when a merge truncates, which keeps the node locally consistent
and removes the evidence before any other oracle looks. Worth being sure before
changing, because the alternative fix is the opposite of what CS-003 did.

The original CS-009 detail, kept for the record:

CS-009's detail: A follower applies entries a later term overwrites, at seed
`0x1`. What is known and what is not is written up in `BUGS.md`; the remaining
suspects are the election restriction across a snapshot boundary and quorum
counting after a restart. An ignored test reproduces it and will start passing
when it is fixed.

---

# P1 — making the evidence more convincing

The bugs are found. These make them land.

## P1.1 — The demo recording

The spec is right that this is the moment that sells the project:

> *"The moment that sells it is step 2 — reproducing a distributed-systems bug
> on demand is something most working engineers have never seen."*

Everything needed already works: `run` prints a violation, `replay` reproduces
it with a timeline, `swarm` shows the live node-hour counter, and the
production cluster survives `kubectl delete pod`. What is missing is the three
minutes of screen capture and the GIF in the README.

**This is the highest ratio of impact to effort in the entire file.** It is
also the only item that requires no engineering at all.

One honest snag: seeds only reproduce for a fixed binary (see `BUGS.md`), so
"fix the bug and rerun the same seed" needs a bug whose fix does not shift the
schedule before the violation. Pick the demo bug deliberately, or record
against a tagged commit.

## P1.2 — Fuzz the wire decoder for real

`cargo-fuzz` / AFL++ are specified and absent. The decoder has a deterministic
pseudo-fuzz loop in its unit tests (~50k inputs per run), which is genuinely
useful and is *not* coverage-guided. A real fuzzer reaches the deep-nesting and
length-prefix cases a xorshift never will.

Cheap: `cargo fuzz add decode_wire` plus a target that calls `Wire::decode` and
asserts it never panics.

## P1.3 — Throughput is 7.4k writes/sec against a 50k–150k target

Honest measurement, well short of the spec. Two contributors, and it is worth
separating them:

1. **The simulated disk is deliberately pessimistic** — `fsync` at 200–800µs
   typical, 5ms at p95, 90ms at p99.5. That is a modelling choice that finds
   bugs, not a performance bug.
2. **One durability barrier per driver cycle** is a real ceiling. At ~2,000
   cycles/sec and ~8 entries per batch, ~16k/sec is the structural limit.

The honest fix is a pipelined WAL: let the next batch accumulate while the
current `fsync` is in flight, rather than serialising cycle → fsync → cycle.
Halving the barriers (only fsyncing hard state on term/vote change) was already
worth 18%; pipelining is the next real step, and it is exactly the kind of
change the simulator should be pointed at afterwards.

Until then, the README states the measured number and explains what it
measures. **Do not quote the spec's target as if it were achieved.**

## P1.4 — The Helm chart has no templates

`deploy/helm/` has `Chart.yaml`, `values.yaml`, and `NOTES.txt` — and no actual
templates, so `helm install` renders nothing. The raw
`deploy/statefulset.yaml` is complete and correct; the chart is a shell around
it. Either finish it (parameterise the StatefulSet from `values.yaml`) or
delete it and keep the raw manifest. A chart that does not install is worse
than no chart.

## P1.5 — `chronoscope trace`, a real TUI

There is a text timeline in `replay` with an `--interesting` filter. The spec
asks for a visual timeline showing the partition, the split-brain window, and
the message that caused it. For the demo, a lane-per-node view with time on one
axis would be far more legible than scrolling text.

---

# P2 — specified, absent, and honestly optional

Each of these is in the spec. None of them will find a bug.

| Item | Status | Assessment |
|---|---|---|
| `io_uring` storage | Not built | Linux-only; written on macOS. The `Storage` trait is the seam — a new type implementing four methods, nothing above it changes. Worth doing on a Linux box, mostly for the resume line. |
| TCP transport | UDP instead | `Network` is a datagram interface because that is the model Raft is specified against. The real cost is `InstallSnapshot` above the datagram limit — which argues for **chunked snapshot transfer**, not for TCP. Chunking is the better fix and would also help P0.1. |
| OpenTelemetry → Jaeger | Not built | Distributed tracing across three processes you can already replay deterministically is redundant. Low value here specifically. |
| `tracing` crate integration | Custom `Tracer` | The deterministic-time subscriber exists and works; it is just not the `tracing` crate. Swapping it buys ecosystem compatibility and nothing else. |
| `proptest` | Hand-rolled | Property tests are deterministic loops over a seeded xorshift. `proptest` would add shrinking, which is genuinely useful for the codec. |
| `criterion` | Custom `bench` | The bench subcommand reports throughput and p50/p99/p99.9. `criterion` adds statistical rigour to microbenchmarks that do not currently exist. |
| `perf` / flamegraph / heaptrack | Not done | Would inform P1.3. Do this *before* optimising, not after. |
| Grafana dashboards | Not built | Prometheus metrics are exported and correct. The dashboard is a demo asset (step 5) more than an engineering one. |
| Block cache for segments | Not built | The post-snapshot suffix is memory-resident, so follower catch-up never reads a segment. The cache would have nothing to cache until logs outgrow memory. Correctly deferred. |

---

# P3 — stretch goals

From the spec, in the order they are worth doing.

1. **TLA+ specification of the Raft variant, and a written comparison of what
   the model checker found versus what the simulator found.** By far the most
   valuable item on this list, and the spec is right that the comparison is the
   interesting artifact. It is also the most honest possible follow-up: two
   independent methods pointed at one implementation, with the disagreements
   written down. Given nine bugs already exist as a baseline, the comparison
   has real content.

2. **Byzantine fault injection.** Partly reachable via P0.2. Full Byzantine
   behaviour (a node that lies about its term, or sends different entries to
   different peers) is a bigger change, and Raft is not designed to survive it
   — so the interesting question is what it degrades to, not whether it holds.

3. **Multi-Raft with a placement driver.** A large build. Worth noting that it
   is a *scaling* feature, and this project's thesis is about *correctness* —
   it would add a lot of surface without sharpening the argument.

4. **Flexible Paxos quorums.** Small, interesting, and directly testable with
   the existing oracles: intersecting read and write quorums of different sizes
   is exactly the kind of thing an invariant checker can falsify.

5. **`LD_PRELOAD` syscall interception** to run someone else's system under the
   simulator. The most ambitious item in the spec and effectively a different
   project.

---

# Deliberately not doing

Stated so these read as decisions rather than oversights.

- **Chasing the 50k–150k writes/sec figure by weakening the disk model.**
  The pessimistic `fsync` distribution is why CS-001 and CS-005 were found.
  Trading bug-finding for a benchmark number inverts the project's thesis.
- **Windowing the linearizability checker to make long histories conclusive.**
  Tried, measured at ~40x faster, and reverted as unsound — the reasoning is
  left in `linearizability.rs` so it does not get re-added. A fast checker that
  can miss a violation is worth less than a slow one that cannot.
- **Widening the regression suite to hide CS-009.** It is an ignored test that
  names the open bug and will start passing when it is fixed.

---

# If only three things get done

1. **P0.1 — membership changes in the swarm.** The largest unexplored fault
   surface, and the one the spec explicitly predicts will contain bugs.
2. **P1.1 — the demo recording.** No engineering, highest visible impact.
3. **P0.5 — close CS-009.** An open bug with a known reproduction is a loose
   thread on the one artifact that matters.
