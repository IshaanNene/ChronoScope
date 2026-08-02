# BUGS.md

Real correctness bugs the simulator found in Chronolog. Nine so far: eight
fixed, one open.

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

## CS-009 — **OPEN** — a follower applies entries a later term overwrites

| | |
|---|---|
| **Found by** | Raft invariants, `nemesis`, seed `0x1` |
| **Status** | **Open** |
| **Reproduce** | `cargo test -p chrono-oracle --release -- --ignored cs_009` |

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
200-seed nemesis swarm      0 failures, 86.7 node-hours simulated in 95s (3282x)
determinism guard           32 seeds x2, all identical
test suite                  192 passing, 1 ignored (CS-009)
```
