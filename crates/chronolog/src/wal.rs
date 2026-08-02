//! The segmented write-ahead log.
//!
//! # On-disk layout
//!
//! ```text
//! wal-00000000000000000001.seg   segment, entries 1..=4096
//! wal-00000000000000004097.seg   segment, entries 4097..
//! state                          hard state, double-buffered
//! snapshot.0 / snapshot.1        alternating snapshot slots
//! ```
//!
//! A segment is a sequence of records:
//!
//! ```text
//! ┌────────────┬────────────┬──────────────┐
//! │ len: u32   │ crc32c:u32 │ body: len B  │
//! └────────────┴────────────┴──────────────┘
//! body = term:u64 index:u64 kind:u8 [payload]
//! ```
//!
//! # What "crash-consistent" actually requires
//!
//! POSIX promises far less than people assume. A `write` that returns
//! successfully has told you the bytes are in the page cache, and nothing
//! more. After a power cut, that write may have landed whole, not at all, or —
//! the case everyone forgets — as a *prefix of its sectors*. The last record in
//! a segment is therefore always suspect.
//!
//! Three properties make recovery sound:
//!
//! 1. **Every record carries a CRC32C over its own body.** A torn record fails
//!    its checksum, because the checksum lives at the front and the missing
//!    bytes are at the back.
//! 2. **Recovery stops at the first bad record and truncates there.** It does
//!    not skip forward looking for the next valid one. A log with a hole is
//!    worse than a short log: Raft's Log Matching property lets a short log be
//!    repaired by the leader, but a log that silently skips an index is
//!    undetectably wrong.
//! 3. **Indices must be contiguous.** A record whose index is not its
//!    predecessor's plus one means the file is not what this log wrote, no
//!    matter how good its checksum is.
//!
//! The simulator tears these writes on purpose. `BUGS.md` records what it found.

use std::sync::Arc;

use chrono_sim::traits::{File, Host, NodeId, Storage};

use crate::codec::{crc32c, Reader, Writer};
use crate::types::{Entry, HardState, Index, Snapshot};

/// Bytes of framing per record: the length and the checksum.
const HEADER: usize = 8;

/// A record longer than this is a corrupt length field, not a record.
const MAX_RECORD: u32 = 32 * 1024 * 1024;

/// Each hard-state slot is padded to this so the two never share a sector, and
/// a torn write to one cannot damage the other.
const STATE_SLOT: u64 = 4096;

#[derive(Clone, Debug)]
pub struct WalOptions {
    /// Roll to a new segment once the active one passes this size. Smaller
    /// segments mean finer-grained compaction and more files.
    pub segment_bytes: u64,
    /// Keep this many bytes of log after the snapshot point before compacting,
    /// so a slightly-lagging follower can still be caught up from the log
    /// rather than by shipping a whole snapshot.
    pub compact_slack_bytes: u64,
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            segment_bytes: 1 << 20,
            compact_slack_bytes: 1 << 18,
        }
    }
}

/// Why recovery stopped where it did. Recorded so a failing seed's `BUGS.md`
/// entry can say *what* the disk did, not just that the node came back wrong.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TailReason {
    /// Read the whole file cleanly.
    Clean,
    /// Not enough bytes left for a record header.
    ShortHeader,
    /// The header claims more bytes than the file holds.
    ShortBody,
    /// The body does not match its checksum — a torn or rotted write.
    BadChecksum,
    /// The body checksummed but did not decode.
    Undecodable,
    /// The record's index is not the previous index plus one.
    IndexGap { expected: Index, found: Index },
    /// A zero or implausible length field.
    BadLength,
}

impl TailReason {
    pub fn is_clean(self) -> bool {
        matches!(self, TailReason::Clean)
    }
}

#[derive(Debug)]
struct Segment {
    name: String,
    file: Arc<dyn File>,
    /// Index of the first entry this segment holds.
    first_index: Index,
    /// Index of the last entry, or `first_index - 1` when empty.
    last_index: Index,
    size: u64,
    /// Byte offset of entry `first_index + i`. Kept in memory so a read is one
    /// seek rather than a scan.
    offsets: Vec<u64>,
}

impl Segment {
    fn is_empty(&self) -> bool {
        self.last_index < self.first_index
    }

    fn name_for(first: Index) -> String {
        format!("wal-{first:020}.seg")
    }

    fn index_from_name(name: &str) -> Option<Index> {
        let s = name.strip_prefix("wal-")?.strip_suffix(".seg")?;
        s.parse().ok()
    }
}

/// What was on disk when the process came back.
#[derive(Debug)]
pub struct Recovered {
    pub wal: Wal,
    pub hard_state: HardState,
    pub snapshot: Option<Snapshot>,
    /// Entries after the snapshot point, in index order, contiguous.
    pub entries: Vec<Entry>,
    /// How the log ended. Anything but `Clean` means the tail was truncated.
    pub tail: TailReason,
    /// How many entries were discarded from the tail.
    pub truncated: u64,
}

/// The write-ahead log for one node.
#[derive(Debug)]
pub struct Wal {
    host: Host,
    opts: WalOptions,
    segments: Vec<Segment>,
    /// Slot the next hard-state write goes to, and the sequence number it gets.
    state_file: Arc<dyn File>,
    state_seq: u64,
    /// Which snapshot slot to write next.
    snapshot_slot: u8,
    snapshot_seq: u64,
    /// Segments touched since the last `sync`.
    dirty: bool,
    stats: WalStats,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WalStats {
    pub appends: u64,
    pub entries_written: u64,
    pub bytes_written: u64,
    pub fsyncs: u64,
    pub truncations: u64,
    pub rollovers: u64,
    pub segments_deleted: u64,
    pub snapshots_written: u64,
}

fn frame(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER + body.len());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32c(body).to_le_bytes());
    out.extend_from_slice(body);
    out
}

impl Wal {
    /// Open the log, replaying whatever survived.
    ///
    /// This is the function that has to be right. Everything about the node's
    /// correctness after a power cut is decided here.
    pub async fn open(host: Host, opts: WalOptions) -> std::io::Result<Recovered> {
        let storage: Arc<dyn Storage> = Arc::clone(&host.storage);

        // --- hard state ---------------------------------------------------
        let state_file = storage.open("state").await?;
        let (hard_state, state_seq) = read_hard_state(&state_file).await;

        // --- snapshot -----------------------------------------------------
        let mut snapshot: Option<Snapshot> = None;
        let mut snapshot_seq = 0u64;
        let mut snapshot_slot = 0u8;
        for slot in 0..2u8 {
            let f = storage.open(&format!("snapshot.{slot}")).await?;
            if let Some((seq, snap)) = read_snapshot(&f).await {
                if seq >= snapshot_seq {
                    snapshot_seq = seq;
                    snapshot = Some(snap);
                    // Write the *next* snapshot to the other slot, so a torn
                    // write can never destroy the one we just recovered.
                    snapshot_slot = 1 - slot;
                }
            }
        }

        // --- segments -----------------------------------------------------
        let mut names: Vec<(Index, String)> = storage
            .list()
            .await?
            .into_iter()
            .filter_map(|n| Segment::index_from_name(&n).map(|i| (i, n)))
            .collect();
        names.sort_unstable();

        let snap_index = snapshot.as_ref().map(|s| s.last_index).unwrap_or(0);
        let mut segments: Vec<Segment> = Vec::new();
        let mut entries: Vec<Entry> = Vec::new();
        let mut tail = TailReason::Clean;
        let mut truncated = 0u64;

        for (first_index, name) in names {
            // A segment entirely covered by the snapshot is garbage left by a
            // compaction that was interrupted. Drop it.
            let file = storage.open(&name).await?;
            let mut seg = Segment {
                name: name.clone(),
                file,
                first_index,
                last_index: first_index.saturating_sub(1),
                size: 0,
                offsets: Vec::new(),
            };

            // Once the tail is broken, every later segment is unreachable —
            // recovery must not skip a hole.
            if !tail.is_clean() {
                let _ = storage.remove(&name).await;
                continue;
            }

            let expected_first = entries.last().map(|e| e.index + 1).unwrap_or(first_index);
            if first_index != expected_first && !entries.is_empty() {
                // A gap between segments. Everything from here on is
                // unreachable; stop and let the leader refill.
                tail = TailReason::IndexGap {
                    expected: expected_first,
                    found: first_index,
                };
                let _ = storage.remove(&name).await;
                continue;
            }

            let (mut seg_entries, seg_tail, valid_bytes) =
                scan_segment(&seg.file, first_index, entries.last().map(|e| e.index)).await;

            // Rebuild the in-memory offset index with a running total. The
            // obvious `offsets.push(offset_of(&entries[..i]))` is quadratic and
            // re-encodes every entry to measure it.
            let mut running = 0u64;
            for e in &seg_entries {
                seg.offsets.push(running);
                running += (HEADER + e.to_bytes().len()) as u64;
            }
            seg.size = valid_bytes;
            seg.last_index = seg_entries
                .last()
                .map(|e| e.index)
                .unwrap_or_else(|| first_index.saturating_sub(1));

            if !seg_tail.is_clean() {
                // Cut the file back to the last record that verified. Leaving
                // the garbage in place would work — recovery would re-detect it
                // — but the next append would write *after* it, permanently
                // baking a hole into the log.
                truncated += 1;
                tail = seg_tail;
                let _ = seg.file.truncate(valid_bytes).await;
                let _ = seg.file.fsync().await;
            }

            entries.append(&mut seg_entries);
            segments.push(seg);
        }

        // Discard anything at or below the snapshot point: the snapshot
        // supersedes it.
        if snap_index > 0 {
            entries.retain(|e| e.index > snap_index);
        }

        let mut wal = Wal {
            host,
            opts,
            segments,
            state_file,
            state_seq,
            snapshot_slot,
            snapshot_seq,
            dirty: false,
            stats: WalStats::default(),
        };
        if wal.segments.is_empty() {
            let first = snap_index + 1;
            wal.open_segment(first).await?;
        }
        wal.stats.truncations = truncated;

        Ok(Recovered {
            wal,
            hard_state,
            snapshot,
            entries,
            tail,
            truncated,
        })
    }

    pub fn stats(&self) -> WalStats {
        self.stats
    }

    /// Index of the first entry still in the log.
    pub fn first_index(&self) -> Index {
        self.segments.first().map(|s| s.first_index).unwrap_or(1)
    }

    /// Index of the last entry, or `first_index - 1` when empty.
    pub fn last_index(&self) -> Index {
        self.segments
            .last()
            .map(|s| s.last_index)
            .unwrap_or_else(|| self.first_index().saturating_sub(1))
    }

    pub fn total_bytes(&self) -> u64 {
        self.segments.iter().map(|s| s.size).sum()
    }

    async fn open_segment(&mut self, first_index: Index) -> std::io::Result<()> {
        let name = Segment::name_for(first_index);
        let file = self.host.storage.open(&name).await?;
        let _ = file.truncate(0).await;
        self.segments.push(Segment {
            name,
            file,
            first_index,
            last_index: first_index.saturating_sub(1),
            size: 0,
            offsets: Vec::new(),
        });
        // A freshly created file is a directory-entry change. Without syncing
        // the directory it can vanish on power loss even though its contents
        // were fsynced — a genuinely surprising failure, and one the simulator
        // reproduces.
        self.host.storage.sync_dir().await?;
        Ok(())
    }

    /// Append entries. Does **not** make them durable — call [`Wal::sync`].
    ///
    /// The split is the point: a batch of proposals is appended individually
    /// and fsynced once, which is what group commit means and why throughput
    /// is not bounded by one fsync per write.
    pub async fn append(&mut self, entries: &[Entry]) -> std::io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.stats.appends += 1;
        for entry in entries {
            let expected = self.last_index() + 1;
            // A hard error, not a `debug_assert`. A gap written here is silent
            // and catastrophic: recovery stops at the first non-contiguous
            // record, so the node comes back with a log truncated to *before*
            // the gap, having already acknowledged and applied entries past it.
            // The observable symptom is two replicas with divergent applied
            // histories, thousands of events and one restart later — a
            // spectacularly hard trail to follow back here. Refuse the write.
            if entry.index != expected {
                return Err(crate::codec_io_error(format!(
                    "WAL appends must be contiguous: got {}, expected {expected} \
                     (wal first={} last={} segments={} batch={}..={})",
                    entry.index,
                    self.first_index(),
                    self.last_index(),
                    self.segments.len(),
                    entries[0].index,
                    entries[entries.len() - 1].index,
                )));
            }

            if self.segments.last().map(|s| s.size).unwrap_or(0) >= self.opts.segment_bytes {
                self.stats.rollovers += 1;
                self.open_segment(entry.index).await?;
            }

            let body = entry.to_bytes();
            let record = frame(&body);
            let seg = self
                .segments
                .last_mut()
                .expect("always at least one segment");
            let at = seg.size;
            seg.file.write_at(at, record.clone()).await?;
            seg.offsets.push(at);
            seg.size += record.len() as u64;
            seg.last_index = entry.index;
            self.stats.entries_written += 1;
            self.stats.bytes_written += record.len() as u64;
            self.dirty = true;
        }
        Ok(())
    }

    /// The durability barrier. Until this resolves, nothing appended is safe.
    pub async fn sync(&mut self) -> std::io::Result<()> {
        if !self.dirty {
            return Ok(());
        }
        if let Some(seg) = self.segments.last() {
            seg.file.fsync().await?;
        }
        self.stats.fsyncs += 1;
        self.dirty = false;
        Ok(())
    }

    /// Persist term/vote/commit to the inactive slot, then fsync.
    ///
    /// Double-buffered because overwriting the live copy in place is not
    /// crash-safe: a torn write to a single-slot state file loses the vote
    /// entirely, and a node that forgets its vote can vote twice in one term.
    pub async fn save_hard_state(&mut self, hs: HardState) -> std::io::Result<()> {
        self.state_seq += 1;
        let slot = (self.state_seq % 2) * STATE_SLOT;
        let mut w = Writer::with_capacity(64);
        w.u64(self.state_seq).u64(hs.term).u64(hs.commit);
        w.opt(&hs.vote, |w, v| {
            w.u32(*v);
        });
        let body = w.finish();
        self.state_file.write_at(slot, frame(&body)).await?;
        self.state_file.fsync().await?;
        self.stats.fsyncs += 1;
        Ok(())
    }

    /// Write a snapshot to the slot the last one did not use, then fsync.
    pub async fn save_snapshot(&mut self, snap: &Snapshot) -> std::io::Result<()> {
        self.snapshot_seq += 1;
        let mut w = Writer::new();
        w.u64(self.snapshot_seq);
        snap.encode(&mut w);
        let body = w.finish();
        let name = format!("snapshot.{}", self.snapshot_slot);
        let f = self.host.storage.open(&name).await?;
        f.truncate(0).await?;
        f.write_at(0, frame(&body)).await?;
        f.fsync().await?;
        self.host.storage.sync_dir().await?;
        // Only now is it safe to aim the next write at the other slot.
        self.snapshot_slot = 1 - self.snapshot_slot;
        self.stats.snapshots_written += 1;
        self.stats.fsyncs += 1;
        Ok(())
    }

    /// Drop every entry with index >= `from`.
    ///
    /// Raft calls for this when a follower's log conflicts with the leader's.
    /// Whole segments past the cut are deleted; the segment containing the cut
    /// is truncated at the byte offset of that entry, which is why `offsets`
    /// is maintained.
    pub async fn truncate_from(&mut self, from: Index) -> std::io::Result<()> {
        if from > self.last_index() {
            return Ok(());
        }
        // A cut at or below where the log now starts removes everything.
        //
        // The subtle case, and the one that was wrong: the loop below refuses
        // to pop the last remaining segment, so a cut below *its* first index
        // fell through to `from.saturating_sub(seg.first_index)`, which
        // saturates to "keep zero entries" and silently leaves the log ending
        // at `first_index - 1` — a boundary the caller never asked for and does
        // not know about. Memory and disk then disagree by however many entries
        // that segment held, and nothing notices until a later append lands on
        // the seam.
        if from <= self.first_index() {
            return self.reset_to(from.saturating_sub(1)).await;
        }
        self.stats.truncations += 1;

        while let Some(seg) = self.segments.last() {
            if seg.first_index >= from && self.segments.len() > 1 {
                let name = seg.name.clone();
                self.segments.pop();
                self.host.storage.remove(&name).await?;
                self.stats.segments_deleted += 1;
            } else {
                break;
            }
        }

        if let Some(seg) = self.segments.last_mut() {
            if from <= seg.last_index {
                let keep = from.saturating_sub(seg.first_index) as usize;
                let cut = seg.offsets.get(keep).copied().unwrap_or(seg.size);
                seg.file.truncate(cut).await?;
                seg.offsets.truncate(keep);
                seg.size = cut;
                // `keep` entries remain, so the last one is
                // `first_index + keep - 1`; zero remaining means empty, which
                // this log spells `first_index - 1`.
                seg.last_index = if keep == 0 {
                    seg.first_index.saturating_sub(1)
                } else {
                    seg.first_index + keep as u64 - 1
                };
                seg.file.fsync().await?;
                self.stats.fsyncs += 1;
                self.dirty = false;
            }
        }
        Ok(())
    }

    /// Discard the entire log and restart it immediately after `index`.
    ///
    /// This is what installing a snapshot *from the leader* requires, and it is
    /// distinct from [`Wal::compact_through`]. Compaction assumes the log
    /// continues contiguously and therefore only drops whole superseded
    /// segments, never the active one. But a follower that accepts a leader's
    /// snapshot has had its log replaced wholesale: the in-memory log now
    /// starts at `index + 1` while the WAL still ends wherever the node had
    /// got to, which may be thousands of entries earlier. The next append then
    /// writes a gap.
    ///
    /// Using compaction for both cases is the natural mistake, because in the
    /// common case — a node only slightly behind — the indices happen to line
    /// up and nothing goes wrong.
    pub async fn reset_to(&mut self, index: Index) -> std::io::Result<()> {
        for seg in std::mem::take(&mut self.segments) {
            self.host.storage.remove(&seg.name).await?;
            self.stats.segments_deleted += 1;
        }
        self.dirty = false;
        self.open_segment(index + 1).await?;
        Ok(())
    }

    /// Delete whole segments that the snapshot at `through` has superseded.
    ///
    /// Only whole segments, and only ones ending strictly before the snapshot
    /// point — a partially compacted segment would need rewriting, and
    /// rewriting a WAL segment in place is exactly the operation that is not
    /// crash-safe.
    pub async fn compact_through(&mut self, through: Index) -> std::io::Result<()> {
        let mut keep_from = 0usize;
        let mut freed = 0u64;
        for (i, seg) in self.segments.iter().enumerate() {
            // Never drop the active segment.
            if i + 1 == self.segments.len() {
                break;
            }
            if seg.last_index < through {
                // Retain some slack so a lagging follower can be caught up from
                // the log instead of by shipping the whole snapshot.
                if self.total_bytes() - freed - seg.size < self.opts.compact_slack_bytes {
                    break;
                }
                freed += seg.size;
                keep_from = i + 1;
            } else {
                break;
            }
        }
        for seg in self.segments.drain(..keep_from).collect::<Vec<_>>() {
            self.host.storage.remove(&seg.name).await?;
            self.stats.segments_deleted += 1;
        }
        if keep_from > 0 {
            self.host.storage.sync_dir().await?;
        }
        Ok(())
    }

    /// Read one entry back off disk. Used by recovery and by follower catch-up
    /// when the entry is no longer in memory.
    pub async fn read(&self, index: Index) -> std::io::Result<Option<Entry>> {
        let Some(seg) = self
            .segments
            .iter()
            .find(|s| !s.is_empty() && index >= s.first_index && index <= s.last_index)
        else {
            return Ok(None);
        };
        let slot = (index - seg.first_index) as usize;
        let Some(&at) = seg.offsets.get(slot) else {
            return Ok(None);
        };
        let header = seg.file.read_at(at, HEADER).await?;
        if header.len() < HEADER {
            return Ok(None);
        }
        let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        if len == 0 || len > MAX_RECORD {
            return Ok(None);
        }
        let body = seg.file.read_at(at + HEADER as u64, len as usize).await?;
        if body.len() != len as usize || crc32c(&body) != crc {
            return Ok(None);
        }
        Ok(Entry::decode(&mut Reader::new(&body)).ok())
    }

    /// Number of segment files currently on disk.
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    pub fn node(&self) -> NodeId {
        self.host.node
    }
}

/// Read every valid record from a segment, stopping at the first one that is
/// not. Returns the entries, why it stopped, and how many bytes verified.
async fn scan_segment(
    file: &Arc<dyn File>,
    first_index: Index,
    prev_index: Option<Index>,
) -> (Vec<Entry>, TailReason, u64) {
    let size = file.len();
    let Ok(buf) = file.read_at(0, size as usize).await else {
        return (Vec::new(), TailReason::ShortHeader, 0);
    };

    let mut entries = Vec::new();
    let mut pos = 0usize;
    let mut expected = prev_index.map(|i| i + 1).unwrap_or(first_index);
    let reason = loop {
        if pos == buf.len() {
            break TailReason::Clean;
        }
        if buf.len() - pos < HEADER {
            break TailReason::ShortHeader;
        }
        let len = u32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]);
        let crc = u32::from_le_bytes([buf[pos + 4], buf[pos + 5], buf[pos + 6], buf[pos + 7]]);
        if len == 0 || len > MAX_RECORD {
            break TailReason::BadLength;
        }
        let body_start = pos + HEADER;
        let body_end = body_start + len as usize;
        if body_end > buf.len() {
            break TailReason::ShortBody;
        }
        let body = &buf[body_start..body_end];
        if crc32c(body) != crc {
            break TailReason::BadChecksum;
        }
        let Ok(entry) = Entry::decode(&mut Reader::new(body)) else {
            break TailReason::Undecodable;
        };
        if entry.index != expected {
            break TailReason::IndexGap {
                expected,
                found: entry.index,
            };
        }
        expected = entry.index + 1;
        entries.push(entry);
        pos = body_end;
    };
    (entries, reason, pos as u64)
}

async fn read_hard_state(file: &Arc<dyn File>) -> (HardState, u64) {
    let mut best = (HardState::default(), 0u64);
    for slot in 0..2u64 {
        let at = slot * STATE_SLOT;
        let Ok(header) = file.read_at(at, HEADER).await else {
            continue;
        };
        if header.len() < HEADER {
            continue;
        }
        let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        if len == 0 || len > 256 {
            continue;
        }
        let Ok(body) = file.read_at(at + HEADER as u64, len as usize).await else {
            continue;
        };
        if body.len() != len as usize || crc32c(&body) != crc {
            continue;
        }
        let mut r = Reader::new(&body);
        let (Ok(seq), Ok(term), Ok(commit)) = (r.u64(), r.u64(), r.u64()) else {
            continue;
        };
        let Ok(vote) = r.opt(|r| r.u32()) else {
            continue;
        };
        // Highest sequence number wins: that is the most recent write that
        // completed. A torn newer slot simply fails its CRC and the older one
        // is used, which is the whole reason for two slots.
        if seq >= best.1 {
            best = (HardState { term, vote, commit }, seq);
        }
    }
    best
}

async fn read_snapshot(file: &Arc<dyn File>) -> Option<(u64, Snapshot)> {
    let size = file.len();
    if size < HEADER as u64 {
        return None;
    }
    let buf = file.read_at(0, size as usize).await.ok()?;
    if buf.len() < HEADER {
        return None;
    }
    let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let crc = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
    if len == 0 || len > MAX_RECORD || HEADER + len as usize > buf.len() {
        return None;
    }
    let body = &buf[HEADER..HEADER + len as usize];
    if crc32c(body) != crc {
        return None;
    }
    let mut r = Reader::new(body);
    let seq = r.u64().ok()?;
    let snap = Snapshot::decode(&mut r).ok()?;
    Some((seq, snap))
}
