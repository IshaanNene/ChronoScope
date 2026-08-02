# BUGS.md

Real correctness bugs the simulator found in Chronolog. Eighteen so far:
sixteen fixed, two open.

This file is the point of the project. Anyone can assert their distributed
system is correct; this is the evidence that mine was not, in specific and
findable ways, and that a deterministic simulator is what found them.

---

## How to read an entry

Each bug records **how it presented**, **what it actually was**, and **why it
was hard to see**. That last part is the interesting one. Almost none of these
presented anywhere near their cause: the symptom was a follower that quietly
stopped replicating, or two replicas that disagreed thousands of events after
the mistake that made them disagree.

## A caveat about seeds, stated up front

A seed reproduces an execution **for a fixed binary**. Change the code — even
in a way unrelated to the bug — and the same seed explores a different
schedule, because the PRNG draws shift. So a seed is not a permanent handle on
a bug; it is a handle on a bug *at a commit*.

Each entry therefore records the commit at which it reproduced. Pretending a
seed is portable across code changes would be the single easiest way to make
this file dishonest, and it is worth saying plainly rather than discovering
later that half the reproductions no longer reproduce.

What *is* durable is the regression test each fix carries.

---

## CS-001 — WAL writes a gap when a follower installs a snapshot

| | |
|---|---|
| **Found by** | Raft invariants (State Machine Safety), `nemesis`, seed `0x0` |
| **Commit** | `c915553` |
| **Severity** | Data loss — acknowledged entries silently discarded |
| **Fixed** | `Wal::reset_to`, `crates/chronolog/src/wal.rs` |

**Presented as** two replicas with divergent applied histories at index 4169,
noticed thousands of events after the fact.

**Actually was** a follower accepting a snapshot from the leader. Installing it
replaces the node's log wholesale, but the driver only *compacted* the WAL —
and compaction deliberately never touches the active segment, because
rewriting a segment in place is not crash-safe. So the WAL still ended
wherever the node had got to, the in-memory log now started at the snapshot
point, and the next append wrote a record whose index was hundreds past the
previous one.

**Why it was hard to see.** Nothing failed at the time. Recovery is what
detects a gap, and it does the only safe thing: stops at the first
non-contiguous record and truncates. So the node came back having *silently
discarded entries it had already acknowledged and applied* — and only then, on
the next comparison against a peer, did anything look wrong.

In the common case the indices happen to line up and compaction is fine, which
is why the wrong call site survived: it works until a follower falls far enough
behind to need a snapshot.

**Fix.** A separate `reset_to`, used only on the install path, which discards
every segment and restarts the log immediately after the snapshot index. The
contiguity check also became a hard error rather than a `debug_assert`, since
in release builds the gap was being written silently.

---

## CS-002 — Stale pending entries survive the snapshot that supersedes them

| | |
|---|---|
| **Found by** | Raft invariants (Driver Stopped), `benign`, seed `0x1` |
| **Commit** | `c915553` |
| **Severity** | Node permanently wedged |
| **Fixed** | `Raft::handle_snapshot`, `crates/chronolog/src/raft.rs` |

**Presented as** a follower frozen at exactly index 996 for an entire run — on
a **benign** policy, with no faults injected at all. The other two nodes
tracked each other perfectly for twelve simulated minutes.

**Actually was** a consequence of group commit. The driver drains a whole burst
of events before taking one `Ready`, so an `AppendEntries` at index 997 can
land in the same batch as a snapshot at 1000. The append queued its entries;
the snapshot then replaced the log but left that queue untouched. The driver
reset the WAL to 1000 and tried to append entry 997.

**Why it was hard to see.** The *simplest* configuration failed while hostile
ones passed, which is exactly backwards from intuition. And the failure mode
was silence: an I/O error killed the driver loop, and a node whose driver has
stopped is the worst kind of broken — still up, still accepting messages, still
reporting its last known state, never making progress again.

**Fix.** `handle_snapshot` clears the pending queue. More importantly, entry
accumulation was replaced with a *watermark* (the lowest index whose durable
copy is stale), which cannot have this class of bug by construction — and which
also fixed the mirror case, where a second `AppendEntries` in one batch
discarded the first one's entries.

Two diagnostics came out of this and stayed: the driver publishes its error
instead of dying quietly, and a stopped driver is itself a reported violation.

---

## CS-003 — `commitIndex` not lowered when a merge truncates the log

| | |
|---|---|
| **Found by** | Raft invariants (State Machine Safety + Leader Completeness), `nemesis`, seed `0x0` |
| **Commit** | `c915553` |
| **Severity** | Safety — uncommitted entries reach the state machine |
| **Fixed** | `Raft::handle_append`, `crates/chronolog/src/raft.rs` |

**Presented as** a follower reporting index 7567 as committed while holding a
term-4 entry there, when the cluster's entry at 7567 was term 5.

**Actually was** a missing clamp. `AppendEntries` handling raises `commit` and
nothing ever lowers it, but a merge can truncate a conflicting suffix out from
under it. The follower then went on claiming an index was committed after
replacing the entry that had been there.

The damage came one step later: `ready()` hands the state machine
`log.slice(applied + 1, commit + 1)`, and `slice` silently clamps to the log's
end. So instead of erroring, it quietly handed over the *replacement*
entries — uncommitted ones — and they were applied.

**Fix.** Clamp `commit` to `last_index` after every merge, and again as a last
gate in `ready()`. Lowering it locally is safe: a durable commit index is an
optimization to avoid replaying the log, not a promise to the cluster.

---

## CS-004 — `ReadIndex` heartbeats leak a commit index to a divergent follower

| | |
|---|---|
| **Found by** | Raft invariants (State Machine Safety), `nemesis`, seed `0x0` |
| **Commit** | `c915553` |
| **Severity** | Safety — State Machine Safety lost via the *read* path |
| **Fixed** | `Raft::read_index`, `crates/chronolog/src/raft.rs` |

**Presented as** a follower's commit index jumping 7566 → 7567 while its entry
at 7567 was from an older term than the cluster's.

**Actually was** the linearizable-read protocol. `ReadIndex` confirms
leadership by sending heartbeats carrying the leader's commit index. A
heartbeat has no `prevLogIndex`/`prevLogTerm`, so it performs **no consistency
check** — it proves nothing about whether the logs agree anywhere. The follower
clamped the value to its own `lastIndex` and committed.

Clamping to `lastIndex` looks like enough and is not: a follower holding a
stale uncommitted tail from a previous term has a perfectly plausible
`lastIndex`. It committed, and applied, an entry that was about to be
overwritten.

**Why it was hard to see.** The read path is not where anyone looks for a
write-safety bug. Reads are supposed to be the safe operation.

**Fix.** The leader caps the heartbeat's commit index to that follower's
`matchIndex` — the only index it has actually *proven* they share.

---

## CS-005 — `Wal::truncate_from` was not total

| | |
|---|---|
| **Found by** | The WAL/memory reconciliation assertion, `nemesis` swarm |
| **Commit** | `3c9804c` |
| **Severity** | Data loss — a whole segment silently dropped |
| **Fixed** | `Wal::truncate_from`, `crates/chronolog/src/wal.rs` |

**Presented as** `WAL appends must be contiguous: got 11145, expected 10892`,
long after the damage.

**Actually was** an arithmetic edge case. The truncation loop refuses to pop
the last remaining segment — there must always be one to append into. So a cut
falling *below* that segment's first index fell through to
`from.saturating_sub(seg.first_index)`, which saturates to "keep zero entries"
and left the log ending at `first_index - 1`: a boundary the caller never asked
for and had no way to learn about.

Memory and disk then disagreed by however many entries that segment held, and
nothing noticed until a later append landed on the seam.

**Fix.** A cut at or below the log's start is now an explicit full reset, and
`truncate_from` is total: every input leaves the log ending exactly where the
caller asked.

---

## CS-006 — A dropped `InstallSnapshot` strands a follower forever

| | |
|---|---|
| **Found by** | Liveness watchdog, `nemesis` swarm — 16 of 60 seeds |
| **Commit** | `3c9804c` |
| **Severity** | Liveness — permanent, silent, unrecoverable without a restart |
| **Fixed** | `Raft::expire_pending_snapshots`, `crates/chronolog/src/raft.rs` |

**Presented as** a follower thousands of entries behind for the rest of the
run, on a cluster that was otherwise healthy.

**Actually was** a missing timeout. `pending_snapshot` is set when a snapshot
is sent and cleared only when the follower replies. `send_append_to` refuses to
send entries while a snapshot is supposedly in flight — sensible, since they
would be rejected. One dropped packet and both conditions hold forever: the
follower is up, connected, voting, and permanently frozen.

**Why it was hard to see, and why this one matters most.** *No safety property
is violated.* Every log matches. Nothing is overwritten. Every invariant
oracle passes. The cluster is quietly running at reduced redundancy, one node
away from data loss, and only a liveness oracle can see it.

This is the entry that justifies the liveness watchdog existing at all.

**Fix.** In-flight snapshots age out after an election timeout and the follower
is re-probed, which either resends entries or re-flags the snapshot depending
on where it actually is.

---

## CS-007 — The durable log tracked by watermark instead of by definition

| | |
|---|---|
| **Found by** | Three separate manifestations of CS-001/002/005 |
| **Commit** | `3c9804c` |
| **Severity** | Design defect underlying several bugs |
| **Fixed** | `node::reconcile`, `crates/chronolog/src/node.rs` |

Not a single bug so much as the reason several existed. Threading "which
entries need writing" through every mutation — append, merge, truncate,
snapshot install, local compaction — is exactly the bookkeeping that goes
subtly wrong, and it did, three different ways, each presenting thousands of
events from its cause.

**Fix.** The watermark is now a fast path, not the contract. The contract is
that **the WAL mirrors the log**, checked directly every cycle for the cost of
two integer comparisons, and repaired if it ever does not. A whole class of
bugs stops being possible rather than being individually fixed.

---

## CS-008 — The leader counts its own *un-fsynced* tail toward the quorum

| | |
|---|---|
| **Found by** | Raft invariants, `nemesis`, seed `0x1` |
| **Commit** | *this commit* |
| **Severity** | Safety — committed entries can be lost entirely |
| **Fixed** | `Raft::persisted`, `crates/chronolog/src/raft.rs` |

**Actually was** the leader using `log.last_index()` as its own `matchIndex`
when computing the quorum commit point. That index is what is in *memory*.

On a three-node cluster the quorum is two. Leader plus one follower is a
quorum in which only the follower has the entry on stable storage. The leader
crashes, loses its un-fsynced tail, and the entry survives on exactly one node
of three — no longer a quorum, so a new leader can be elected without it, and
an entry that was reported committed is gone.

**Why it was hard to see.** The `Ready` design already enforces
persist-before-send, so the *message* ordering was correct throughout. The bug
was in what the leader counted, not in when it spoke. It also requires a crash
inside the window between append and fsync, which is narrow in wall-clock terms
and trivially reachable when the simulator controls both the schedule and the
disk.

**Fix.** A node's vote in its own quorum must be backed by its disk.
`Raft::persisted` tracks the highest index the driver has actually fsynced, and
that is what counts. A single-node cluster now commits one durability barrier
later than before — correctly, since for a lone voter its own disk *is* the
quorum.

---

## CS-009 — a follower applies entries a later term overwrites — **FIXED by CS-016**

| | |
|---|---|
| **Found by** | Raft invariants, `nemesis`, seed `0x1` |
| **Status** | Fixed — root cause was CS-016 |
| **Reproduce** | `cargo test -p chrono-oracle --release cs_009` (now passes) |

```
COMMIT MONOTONICITY VIOLATED: n0 commit index went 3854 -> 3825 without restarting
STATE MACHINE SAFETY VIOLATED: n0 and n1 applied different histories through index 3825
LEADER COMPLETENESS VIOLATED: index 3825 was committed in term 4 but n0 now holds term 5
APPLIED BEYOND COMMIT VIOLATED: n0 applied 3854 but has only committed 3825
```

**What is known.** n0 reaches commit 3854 legitimately — the message trace
shows leader n1 in term 3 streaming entries with a commit index tracking one
behind, exactly as it should. A term-5 leader later truncates n0 at 3825, and
the clamp added in CS-003 lowers n0's commit to match, at which point `applied`
(3854) is already past it.

So either the term-3 leader's commit was not backed by a real quorum, or a
term-5 leader was elected without entries that were genuinely committed. CS-008
was one mechanism for the first of those and did not resolve this seed.

**What is not known.** Which. The remaining suspects are the election
restriction under a snapshot boundary (`is_up_to_date` compares against
`last_term`, which after compaction comes from the snapshot) and the
interaction between joint-consensus quorum counting and a restarted node.

It is recorded here rather than quietly excluded because an honest ledger is
the entire deliverable. The regression suite runs seeds `0, 2..8`; seed `1` is
an ignored test that will start passing when this is fixed.

---

## CS-010 — a new voter and a compacted leader spin forever on empty appends

| | |
|---|---|
| **Found by** | The membership workload, first run, `nemesis`, seed `0x1` |
| **Severity** | Liveness + resource exhaustion — the node can never catch up |
| **Fixed** | `Raft::send_append_to`, `crates/chronolog/src/raft.rs` |

Found within minutes of letting the swarm change membership for the first time,
which is exactly what [`ROADMAP.md`](ROADMAP.md) predicted would happen.

**Presented as** a run that took 27 seconds instead of 1.2, having sent **17
million messages** where the same workload without a membership change sends
257 thousand.

**Actually was** the sentinel. `send_append_to` decides a follower needs a
snapshot when `term_at(prev_index)` returns `None` — but `term_at(0)` answers
`Some(0)`, because index 0 is the "before the beginning" sentinel that makes
`prevLogIndex` arithmetic work at the start of time. A node joining at
`next = 1` — exactly where a newly added voter starts — therefore passes the
check even when the leader has compacted through index 5000.

What follows is worse than a missed snapshot. `slice(1, 65)` clamps to the
log's real start and comes back **empty**, so the leader sends an empty
`AppendEntries` at `prev = 0`; the follower's log is also empty, so it accepts
and replies `match = 0`; the leader sees it is still behind and immediately
sends another. Neither side is wrong. Nobody makes progress. The pair spins as
fast as the network allows.

**Why it was hard to see.** It needs a node whose log starts at 1 *and* a
leader that has compacted past it. A static cluster never produces that
combination: every node is present from index 1 and stays roughly caught up.
Adding a voter produces it on the first try.

**Fix.** Ask the real question — are the entries this follower needs still in
the log — rather than a proxy for it: `p.next < self.log.first_index()`.

---

## CS-011 — a client cannot follow a redirect to a node it does not know

| | |
|---|---|
| **Found by** | The membership workload, `nemesis`, seed `0x1` |
| **Severity** | Liveness — the cluster is healthy and unreachable |
| **Fixed** | `Client::learn`, `crates/chronolog/src/client.rs` |

**Presented as** a leader that led for 30 seconds with its commit index frozen
"despite pending work", on a cluster where all four nodes were caught up and
agreed on everything.

**Actually was** entirely on the client side. It was configured with servers
`{0,1,2}`. Once the membership workload added n3 and leadership moved there,
every node correctly answered `NotLeader { hint: 3 }` — and the client
discarded each hint, because n3 was not in its list:

```rust
match self.leader_hint {
    Some(h) if self.servers.contains(&h) => h,   // n3 fails this
    _ => random_from(self.servers),               // ...so round-robin {0,1,2}
}
```

It round-robined the three nodes it already knew, forever, and reported the
cluster unavailable. The cluster was perfectly healthy the entire time.

**Why it was hard to see.** The guard looks like sensible input validation —
do not chase a hint naming a node you have never heard of. It is exactly
backwards: a redirect is *how* a client discovers membership added after it
started. And the symptom points at the server, because the server is the thing
that appears stuck.

**Fix.** Learn the node from the hint and follow it.

---

## CS-012 — State Machine Safety under membership churn — **FIXED by CS-016**

| | |
|---|---|
| **Found by** | Raft invariants, `nemesis` + membership, seed `0x89` |
| **Status** | Fixed — root cause was CS-016 |
| **Reproduce** | `chronoscope run --seed 0x89 --secs 400 --keys 4 --clients 4 --spares 2 --reconfigure-secs 15` (now passes) |

```
STATE MACHINE SAFETY VIOLATED: n1 and n2 applied different histories through index 9837
LEADER COMPLETENESS VIOLATED: index 9837 was committed in term 4 but n1 now holds term 5 there
```

One seed in 200 under aggressive reconfiguration. The same shape as
[CS-009](#cs-009--open--a-follower-applies-entries-a-later-term-overwrites) —
a committed entry overwritten across a term boundary — which suggests the two
share a cause, and that membership churn simply widens the window that produces
it.

**Both are now closed by [CS-016](#cs-016--the-durability-barrier-did-not-cover-the-whole-batch).**
Seeds `0x1` and `0x89` run clean, and both are regression tests. The write-up
below is kept because the reasoning is the useful part.

**A third reproduction had appeared** under the `corrupting` preset at a
different seed:

```
STATE MACHINE SAFETY VIOLATED: n0 and n1 applied different histories through index 5041
LEADER COMPLETENESS VIOLATED: index 5041 was committed in term 5 but n0 now holds term 6 there
```

Three independent reproductions — static churn, membership churn, wire
corruption — all with the same signature: a committed entry overwritten as the
term advances by exactly one. Whatever this is, it is not specific to any fault
mode; those only widen the window. That is a much stronger starting point for
finding it than any single seed.

---

## CS-013 — a full disk kills the node permanently

| | |
|---|---|
| **Found by** | The `diskfull` preset, first run, seed `0x3` |
| **Severity** | Availability — a recoverable fault made terminal |
| **Fixed** | `node::run` ENOSPC path, `crates/chronolog/src/node.rs` |

**Presented as** `DRIVER STOPPED: ENOSPC: simulated disk full`, two `ENOSPC`
events into a run.

**Actually was** the driver treating any persist failure as fatal. `persist`
returned `Err`, the loop returned, and the node became a zombie — up,
listening, voting, never progressing again — despite compaction being able to
free the space seconds later.

**Why it was hard to see.** Every other fault the simulator injects is either
transient by nature (a dropped packet, a partition that heals) or genuinely
terminal (a crash, which restarts cleanly). A full disk is the only one that is
*persistent but recoverable*, and the code had no category for that.

**Fix.** ENOSPC is pressure, not death. Nothing was made durable, so nothing is
sent and nothing is advanced — the same `Ready` is regenerated next cycle. A
leader stands down, because continuing to lead while unable to append refuses
every write *and* prevents anyone else being elected to serve them. Then the
node snapshots and compacts to free space, and carries on.

With that, the same seed completes 23,324 client operations through **5,432
ENOSPC events**, linearizable, all invariants holding.

---

## CS-014 — a failed append leaves a partial batch on disk

| | |
|---|---|
| **Found by** | The `diskfull` swarm — 26 of 200 seeds |
| **Severity** | Data loss — recoverable pressure turned fatal |
| **Fixed** | `Wal::append`, `crates/chronolog/src/wal.rs` |

Surfaced immediately *by fixing CS-013*: once the driver retried instead of
dying, the retries started colliding.

**Actually was** `append` writing a batch entry by entry with no rollback. A
failure part-way leaves a prefix of the batch on disk; the caller retries the
whole batch, collides with that prefix, and fails the contiguity check — which
is fatal, where the original error was merely pressure.

**Fix.** Roll the partial batch back so a failed append is a no-op and the
retry starts exactly where the previous attempt did.

---

## CS-015 — compaction outruns the durable log

| | |
|---|---|
| **Found by** | The `diskfull` swarm, after CS-014 — still 26 of 200 |
| **Severity** | Data loss — entries erased from memory *and* disk |
| **Fixed** | `node::safe_to_compact`, `crates/chronolog/src/node.rs` |

**Presented as** `WAL appends must be contiguous: got 7596, expected 7592` —
a four-entry hole, with the WAL at 7591 and the log starting at 7596.

**Actually was** the interaction between snapshotting and a full disk.
Snapshots are taken at the *applied* index, and applied normally trails what is
on disk. Not always: a node whose disk is full keeps applying committed entries
it could not write, so applied runs *ahead* of the WAL. Compacting there drops
those entries from memory while they are also absent from disk. They exist
nowhere, and the next append lands past the hole they left.

**Why it was hard to see.** On a healthy disk the invariant holds for free —
you have to be unable to write while still able to apply for it to break at
all.

**Fix.** State the invariant and check it: **compaction must never outrun the
durable log.** `safe_to_compact` requires `snap.last_index <= wal.last_index()`,
and it guards the ordinary snapshot path too, where the same hazard exists.

Together, CS-013 through CS-015 took the `diskfull` swarm from 37 failures in
200 seeds to 10, all of them liveness stalls — which a full disk genuinely
causes.

---

## CS-016 — the durability barrier did not cover the whole batch

| | |
|---|---|
| **Found by** | A purpose-built durability oracle, chasing CS-009 |
| **Severity** | **Data loss** — the foundational contract, broken |
| **Fixed** | `Wal::sync`, `crates/chronolog/src/wal.rs` |

**This is the cause of CS-009 and CS-012**, and it took three attempts to find
because every tool I had reported the symptom rather than the fault.

`sync()` fsynced `self.segments.last()` — the active segment. A batch that
crosses a rollover writes into *two* files, so the earlier segment's tail stayed
in the page cache while the caller was told the whole batch was durable. The
node then acknowledged those entries to its leader, they counted toward a
quorum, and a crash made them vanish.

The damage compounds. Recovery finds a one-entry hole at the segment boundary,
stops there — correctly, holes are worse than short logs — and discards every
later segment. In the reproducing seed the node fsynced through 3855 and came
back at 3400: **455 entries**, from one missing `fsync`. It then voted for a
candidate that had never held those entries, and the new leader truncated them
off a peer that did.

**How it was actually found.** Three rounds of instrumentation, each answering
one question the previous had raised:

1. *Was the entry genuinely committed, or was the commit inflated?* A detector
   in `handle_append` that fires on the message truncating below `commitIndex`,
   printing both sides. Answer: the leader's `matchIndex` for two of three
   nodes was legitimate, so the commit was real.
2. *Then who lost it?* A durability oracle comparing what each node claimed to
   have fsynced against what its next process lifetime recovered. It named the
   node and the two indices in one line — `n1 fsynced through 3855 ... came back
   with last=3400` — which is the whole diagnosis, six words long.
3. *Where?* A probe on segment lifecycle showed a segment opened at 3402 when
   the previous ended at 3400. One entry, exactly at a rollover.

The lesson worth keeping: **assert the invariant where it should hold, not
where the symptom appears.** Three oracles had been reporting divergent applied
histories thousands of events downstream. One oracle checking the actual
contract — *acknowledged means durable* — pointed straight at it.

**Fix.** Track a dirty flag per segment and fsync every one the batch touched.
The global `dirty` flag was removed entirely at the same time: a second source
of truth alongside the per-segment flags, which `truncate_from` and `reset_to`
both cleared, and which could therefore short-circuit a later `sync` while a
segment still held unsynced writes.

---

## CS-017 — a node's quorum contribution outlived its disk

| | |
|---|---|
| **Found by** | The same durability oracle, immediately after CS-016 |
| **Severity** | Safety — the CS-008 mistake, one step later |
| **Fixed** | `Raft::set_persisted`, `crates/chronolog/src/raft.rs` |

`persisted` — the index a node offers to its own quorum — was inferred from
what Raft *asked* to be written, and only ever raised.

Both halves were wrong. Inferring is a guess that happens to be right whenever
the write path does exactly what was requested, and misses every case where it
does not: a rollback, a partial batch, a repair. And a high-water mark cannot
express truncation — a conflicting `AppendEntries` removes durable entries, so
the mark went on claiming an index the disk no longer held.

This is [CS-008](#cs-008) one step later. There, the leader counted its
un-fsynced tail toward a quorum; here, it counts a tail that was fsynced and
then removed. Same consequence: if elected, it leads on a log shorter than it
advertised.

**Fix.** The WAL is the authority. The driver reports `wal.last_index()` after
a successful sync, and `set_persisted` assigns rather than raises.

---

## CS-018 — **OPEN** — a committed, durable entry lost across a restart

| | |
|---|---|
| **Found by** | The durability oracle, all fault modes |
| **Status** | **Open** |
| **Reproduce** | `chronoscope run --seed 0x1b --secs 400 --keys 4 --clients 4` |

```
DURABILITY VIOLATED: n2 had committed data durable through 20092 before
restarting but came back with last=20091 — acknowledged data was lost
```

What remains after CS-016. The loss is consistently **one or two entries**,
never the hundreds that the segment-boundary bug produced, and it appears in
every fault mode at roughly 2-5% of seeds.

**It is not a sampling artifact.** The oracle observes between slices, so a
legitimate truncation landing between the last observation and the crash would
look identical. Narrowing the sampling interval from 250ms to 2ms — 125 times
tighter — reproduces the same violation at the same index. (That the numbers
are *identical* across sampling rates is also a small proof the simulation is
independent of how often it is observed.)

**Why nothing else catches it.** The clamp added in [CS-003](#cs-003) lowers
`commitIndex` when a merge truncates, which keeps the node locally safe and
erases the evidence: by the time Leader Completeness looks, the entry is no
longer below the node's commit index. The durability oracle sees it only
because it remembers what was true before the restart.

The leading suspicion is therefore that CS-003's clamp is masking a genuine
Leader Completeness violation rather than preventing one — that a committed
entry really is being truncated, and the clamp makes the node quietly agree to
it. That is a different fix from the one CS-003 applied, and worth being sure
about before changing.

---

## What corruption on the wire did *not* break

Worth recording as a negative result. The `corrupting` preset flips a bit in
2,610 frames of a 200-second run — after the frame's CRC was computed, so every
one of them is detectably wrong.

Nothing broke. The decoder rejects them, Raft treats each as an ordinary lost
message and retries, and the cluster completes its workload. Across 200 seeds
there was exactly one failure, and it was the CS-009 shape rather than anything
corruption-specific.

That is the property the frame checksum exists to provide, and it had never
been demonstrated in a *running cluster* before — only against a decoder in a
unit test, which cannot tell you whether the protocol above it copes.

---

## What membership churn costs, measured

The swarm had never proposed a configuration change until now. Turning it on
was the single most productive change in the project, and it also quantified
something worth knowing.

Failures per 200 seeds, `nemesis`, 400 simulated seconds:

| Reconfiguration rate | Direct voter add | Learner first |
|---|---|---|
| never | 0 / 200 | — |
| every 60s | 2 / 200 | — |
| every 30s | 17 / 200 | **5 / 200** |
| every 15s | 30 / 200 | **17 / 200** |

Two conclusions.

**Reconfiguration costs availability, and the cost is real rather than a bug.**
Every failure above is a liveness stall — "no leader for N seconds" — and not
one is a safety violation, apart from CS-012. A configuration change is a
leadership disruption; doing one every 15 simulated seconds while nodes are
also crashing means the cluster spends much of its life in transition.

**Adding a voter directly is measurably worse than staging through a learner,**
and the mechanism is clean: the moment a node joins as a voter the quorum
rises — three voters need two, four need three — while the new node has nothing
and cannot help. Failure tolerance drops to zero until it catches up. A learner
is replicated to without being counted, so the quorum only rises once the node
can actually contribute. Halving the failure rate at both churn rates is what
that argument looks like when it is measured instead of asserted.

---

## What the simulator did not find

Worth stating, because a bug ledger without one reads like advertising.

- **No linearizability violation survives.** Every one the checker reported
  turned out to be a bug *in the checker* (below). The Raft-level oracles found
  everything real, and always earlier — an internal invariant breaks long
  before a client can observe anything wrong, which is the argument for having
  both.

  Worth being precise about what that does and does not establish. The checker
  concludes on histories up to roughly 15,000 operations; a long run producing
  more than that reports `Inconclusive` rather than guessing. So "no
  linearizability violation" means *none in the runs where the checker reached
  a verdict*, not none anywhere. Raising the budget makes long runs conclusive
  at roughly 7x the wall-clock cost, which is worth it when triaging one seed
  and not worth it across a swarm.
- **No determinism divergence in the final tree.** 32 seeds × 2 runs, identical
  event-trace hashes. This is the claim everything else rests on, so it is
  checked in CI on every push.

### And two bugs in the oracles themselves

Recorded because an oracle that cries wolf is worse than no oracle: every false
report costs an investigation, and in a 400-seed swarm they buried the real
failures completely.

1. **The linearizability checker's frontier mask was shifted one position too
   few** when advancing over a run of already-placed operations, leaving a
   stale bit that claimed the wrong operation was placed. It reported perfectly
   linearizable histories as violations. Regression test reduced from the run
   that exposed it.

2. **Windowing a long history is unsound.** A window starting mid-history
   begins with a fresh `nil` register, so the first read in every window that
   returns a previously-written value is reported as a violation. Replaced with
   a compact `(frontier, ahead-mask)` encoding that checks a 15,000-operation
   history directly.

Plus two liveness oracles that were simply too strict — "converging" now means
the follower is *advancing*, not that the gap is momentarily zero (under load
it never is), and a crashed node's stale published state no longer pins the
cluster's apparent commit index.

---

## Current state

```
determinism guard   32 seeds x2, all identical
test suite          205 passing, 0 ignored

200-seed swarms, 400 simulated seconds each:
  nemesis        8 / 200      corrupting     2 / 200
  diskfull      16 / 200      membership    18 / 200

of which  19 durability (CS-018, open)
          23 liveness stalls
           2 WAL contiguity under a full disk
```

The durability oracle is new in this round and accounts for most of those
numbers — it reports a class of failure nothing was previously looking for.
