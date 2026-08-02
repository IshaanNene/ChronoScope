//! The wire protocol.
//!
//! Every message carries a term, because the first thing any Raft node does
//! with any message is compare terms. Hoisting it out of the body makes that
//! rule impossible to forget in a new variant.

use chrono_sim::traits::NodeId;

use crate::codec::{crc32c, DecodeError, Reader, Result, Writer, MAX_FRAME};
use crate::types::{Entry, Index, Snapshot, Term};

/// Bumped whenever the encoding changes incompatibly. A node that receives a
/// frame from a different version drops it rather than misparsing it.
pub const PROTOCOL_VERSION: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub term: Term,
    pub body: Body,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Body {
    // --- elections ------------------------------------------------------
    /// A poll, not an election.
    ///
    /// Pre-vote exists because of the *disruptive rejoin*: a node partitioned
    /// away increments its term on every failed election, and on rejoining
    /// forces a healthy leader to step down purely by having a higher term.
    /// A pre-vote asks "would you vote for me?" without anyone changing term,
    /// so a node with no chance of winning cannot disturb a working cluster.
    PreVoteReq {
        last_index: Index,
        last_term: Term,
    },
    PreVoteResp {
        granted: bool,
    },

    VoteReq {
        last_index: Index,
        last_term: Term,
    },
    VoteResp {
        granted: bool,
    },

    // --- replication ----------------------------------------------------
    AppendReq {
        prev_index: Index,
        prev_term: Term,
        entries: Vec<Entry>,
        commit: Index,
    },
    AppendResp {
        success: bool,
        /// On success, the follower's new last index.
        match_index: Index,
        /// On failure, where the follower thinks the divergence is. Lets the
        /// leader skip a whole term per round trip instead of one index.
        conflict_index: Index,
        conflict_term: Term,
    },

    // --- snapshots ------------------------------------------------------
    SnapshotReq {
        snapshot: Snapshot,
    },
    SnapshotResp {
        success: bool,
        index: Index,
    },

    // --- linearizable reads ---------------------------------------------
    /// A heartbeat carrying an opaque context, used to confirm leadership for
    /// a `ReadIndex` without appending anything to the log.
    HeartbeatReq {
        commit: Index,
        ctx: u64,
    },
    HeartbeatResp {
        ctx: u64,
    },

    // --- leadership transfer --------------------------------------------
    /// Instructs the target to start an election immediately, skipping its
    /// election timeout. Used for graceful handover before a planned shutdown.
    TimeoutNow,
}

impl Message {
    pub fn new(term: Term, body: Body) -> Message {
        Message { term, body }
    }

    /// Pre-vote messages are the one exception to "a higher term makes you a
    /// follower". Treating them as term-bearing would defeat the entire point
    /// of pre-vote, since the poll is sent at `term + 1`.
    pub fn is_pre_vote(&self) -> bool {
        matches!(
            self.body,
            Body::PreVoteReq { .. } | Body::PreVoteResp { .. }
        )
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::with_capacity(64);
        w.u64(self.term);
        match &self.body {
            Body::PreVoteReq {
                last_index,
                last_term,
            } => {
                w.u8(1).u64(*last_index).u64(*last_term);
            }
            Body::PreVoteResp { granted } => {
                w.u8(2).bool(*granted);
            }
            Body::VoteReq {
                last_index,
                last_term,
            } => {
                w.u8(3).u64(*last_index).u64(*last_term);
            }
            Body::VoteResp { granted } => {
                w.u8(4).bool(*granted);
            }
            Body::AppendReq {
                prev_index,
                prev_term,
                entries,
                commit,
            } => {
                w.u8(5).u64(*prev_index).u64(*prev_term).u64(*commit);
                w.seq(entries, |w, e| e.encode(w));
            }
            Body::AppendResp {
                success,
                match_index,
                conflict_index,
                conflict_term,
            } => {
                w.u8(6)
                    .bool(*success)
                    .u64(*match_index)
                    .u64(*conflict_index)
                    .u64(*conflict_term);
            }
            Body::SnapshotReq { snapshot } => {
                w.u8(7);
                snapshot.encode(&mut w);
            }
            Body::SnapshotResp { success, index } => {
                w.u8(8).bool(*success).u64(*index);
            }
            Body::HeartbeatReq { commit, ctx } => {
                w.u8(9).u64(*commit).u64(*ctx);
            }
            Body::HeartbeatResp { ctx } => {
                w.u8(10).u64(*ctx);
            }
            Body::TimeoutNow => {
                w.u8(11);
            }
        }
        w.finish()
    }

    pub fn decode(buf: &[u8]) -> Result<Message> {
        let mut r = Reader::new(buf);
        let term = r.u64()?;
        let body = match r.u8()? {
            1 => Body::PreVoteReq {
                last_index: r.u64()?,
                last_term: r.u64()?,
            },
            2 => Body::PreVoteResp { granted: r.bool()? },
            3 => Body::VoteReq {
                last_index: r.u64()?,
                last_term: r.u64()?,
            },
            4 => Body::VoteResp { granted: r.bool()? },
            5 => {
                let prev_index = r.u64()?;
                let prev_term = r.u64()?;
                let commit = r.u64()?;
                let entries = r.seq(Entry::decode)?;
                Body::AppendReq {
                    prev_index,
                    prev_term,
                    entries,
                    commit,
                }
            }
            6 => Body::AppendResp {
                success: r.bool()?,
                match_index: r.u64()?,
                conflict_index: r.u64()?,
                conflict_term: r.u64()?,
            },
            7 => Body::SnapshotReq {
                snapshot: Snapshot::decode(&mut r)?,
            },
            8 => Body::SnapshotResp {
                success: r.bool()?,
                index: r.u64()?,
            },
            9 => Body::HeartbeatReq {
                commit: r.u64()?,
                ctx: r.u64()?,
            },
            10 => Body::HeartbeatResp { ctx: r.u64()? },
            11 => Body::TimeoutNow,
            t => return Err(DecodeError::BadTag(t)),
        };
        Ok(Message { term, body })
    }

    /// A one-line summary for traces.
    pub fn summary(&self) -> String {
        match &self.body {
            Body::PreVoteReq {
                last_index,
                last_term,
            } => {
                format!("PreVoteReq t{} log={last_term}.{last_index}", self.term)
            }
            Body::PreVoteResp { granted } => format!("PreVoteResp t{} {granted}", self.term),
            Body::VoteReq {
                last_index,
                last_term,
            } => {
                format!("VoteReq t{} log={last_term}.{last_index}", self.term)
            }
            Body::VoteResp { granted } => format!("VoteResp t{} {granted}", self.term),
            Body::AppendReq {
                prev_index,
                entries,
                commit,
                ..
            } => {
                format!(
                    "Append t{} prev={prev_index} n={} commit={commit}",
                    self.term,
                    entries.len()
                )
            }
            Body::AppendResp {
                success,
                match_index,
                conflict_index,
                ..
            } => {
                if *success {
                    format!("AppendResp t{} ok match={match_index}", self.term)
                } else {
                    format!("AppendResp t{} REJECT hint={conflict_index}", self.term)
                }
            }
            Body::SnapshotReq { snapshot } => format!("Snapshot t{} {snapshot:?}", self.term),
            Body::SnapshotResp { success, index } => {
                format!("SnapshotResp t{} {success} @{index}", self.term)
            }
            Body::HeartbeatReq { commit, .. } => {
                format!("Heartbeat t{} commit={commit}", self.term)
            }
            Body::HeartbeatResp { .. } => format!("HeartbeatResp t{}", self.term),
            Body::TimeoutNow => format!("TimeoutNow t{}", self.term),
        }
    }
}

// ---------------------------------------------------------------------------
// Framing
// ---------------------------------------------------------------------------

/// A frame as it appears on the wire or in a datagram.
///
/// ```text
/// ┌─────────┬──────────┬──────────┬─────────────┐
/// │ ver: u8 │ len: u32 │ crc: u32 │ payload     │
/// └─────────┴──────────┴──────────┴─────────────┘
/// ```
///
/// The checksum is not redundant with TCP's. TCP's 16-bit checksum misses
/// roughly one error in 65,536, and at datacenter packet rates that is a
/// corruption every few hours — well documented, and the reason every serious
/// storage system checksums its own payloads.
pub fn frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(9 + payload.len());
    out.push(PROTOCOL_VERSION);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32c(payload).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

/// Decode a frame, verifying the version and checksum.
pub fn unframe(buf: &[u8]) -> Result<&[u8]> {
    if buf.len() < 9 {
        return Err(DecodeError::Truncated);
    }
    if buf[0] != PROTOCOL_VERSION {
        return Err(DecodeError::BadTag(buf[0]));
    }
    let len = u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    let crc = u32::from_le_bytes([buf[5], buf[6], buf[7], buf[8]]);
    if len > MAX_FRAME {
        return Err(DecodeError::BadLength(len as u64));
    }
    if buf.len() < 9 + len {
        return Err(DecodeError::Truncated);
    }
    let payload = &buf[9..9 + len];
    let actual = crc32c(payload);
    if actual != crc {
        return Err(DecodeError::BadChecksum {
            expected: crc,
            actual,
        });
    }
    Ok(payload)
}

/// How a membership change turned out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdminResult {
    /// Appended to the leader's log at this index. Not yet committed — the
    /// caller must observe the configuration to know it took effect.
    Accepted { index: Index },
    NotLeader {
        hint: Option<crate::types::NodeIdRepr>,
    },
    /// Refused. Overlapping transitions cannot be reasoned about, so exactly
    /// one change may be in flight.
    Rejected,
}

/// Everything a peer can send us: Raft traffic, a client request, or a
/// membership change.
///
/// Membership is deliberately a separate channel from the client protocol.
/// Reconfiguration is an operator action with a completely different
/// authorization story and a completely different failure mode — a client
/// write that is refused is retried, a reconfiguration that is refused needs a
/// human to look at why.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Wire {
    Raft(Message),
    Client(crate::client::Request),
    Reply(crate::client::Response),
    Admin(crate::types::ConfigChange),
    AdminReply(AdminResult),
}

impl Wire {
    pub fn encode(&self) -> Vec<u8> {
        let mut payload = Vec::new();
        match self {
            Wire::Raft(m) => {
                payload.push(0);
                payload.extend_from_slice(&m.encode());
            }
            Wire::Client(r) => {
                payload.push(1);
                payload.extend_from_slice(&r.encode());
            }
            Wire::Reply(r) => {
                payload.push(2);
                payload.extend_from_slice(&r.encode());
            }
            Wire::Admin(c) => {
                payload.push(3);
                let mut w = Writer::new();
                c.encode(&mut w);
                payload.extend_from_slice(&w.finish());
            }
            Wire::AdminReply(r) => {
                payload.push(4);
                let mut w = Writer::new();
                match r {
                    AdminResult::Accepted { index } => {
                        w.u8(0).u64(*index);
                    }
                    AdminResult::NotLeader { hint } => {
                        w.u8(1);
                        w.opt(hint, |w, h| {
                            w.u32(*h);
                        });
                    }
                    AdminResult::Rejected => {
                        w.u8(2);
                    }
                }
                payload.extend_from_slice(&w.finish());
            }
        }
        frame(&payload)
    }

    pub fn decode(buf: &[u8]) -> Result<Wire> {
        let payload = unframe(buf)?;
        let (tag, rest) = payload.split_first().ok_or(DecodeError::Truncated)?;
        match tag {
            0 => Ok(Wire::Raft(Message::decode(rest)?)),
            1 => Ok(Wire::Client(crate::client::Request::decode(rest)?)),
            2 => Ok(Wire::Reply(crate::client::Response::decode(rest)?)),
            3 => Ok(Wire::Admin(crate::types::ConfigChange::decode(
                &mut Reader::new(rest),
            )?)),
            4 => {
                let mut r = Reader::new(rest);
                let result = match r.u8()? {
                    0 => AdminResult::Accepted { index: r.u64()? },
                    1 => AdminResult::NotLeader {
                        hint: r.opt(|r| r.u32())?,
                    },
                    2 => AdminResult::Rejected,
                    t => return Err(DecodeError::BadTag(t)),
                };
                Ok(Wire::AdminReply(result))
            }
            t => Err(DecodeError::BadTag(*t)),
        }
    }
}

/// Where a message came from and what it says.
#[derive(Clone, Debug)]
pub struct Incoming {
    pub from: NodeId,
    pub msg: Message,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Config, EntryKind};

    fn sample_messages() -> Vec<Message> {
        vec![
            Message::new(
                3,
                Body::PreVoteReq {
                    last_index: 9,
                    last_term: 2,
                },
            ),
            Message::new(3, Body::PreVoteResp { granted: true }),
            Message::new(
                4,
                Body::VoteReq {
                    last_index: 9,
                    last_term: 2,
                },
            ),
            Message::new(4, Body::VoteResp { granted: false }),
            Message::new(
                4,
                Body::AppendReq {
                    prev_index: 9,
                    prev_term: 2,
                    commit: 8,
                    entries: vec![
                        Entry {
                            term: 4,
                            index: 10,
                            kind: EntryKind::Noop,
                        },
                        Entry {
                            term: 4,
                            index: 11,
                            kind: EntryKind::Normal(b"x=1".to_vec()),
                        },
                    ],
                },
            ),
            Message::new(
                4,
                Body::AppendResp {
                    success: false,
                    match_index: 0,
                    conflict_index: 7,
                    conflict_term: 2,
                },
            ),
            Message::new(
                5,
                Body::SnapshotReq {
                    snapshot: Snapshot {
                        last_index: 50,
                        last_term: 4,
                        config: Config::simple([0, 1, 2]),
                        data: vec![9; 128],
                    },
                },
            ),
            Message::new(
                5,
                Body::SnapshotResp {
                    success: true,
                    index: 50,
                },
            ),
            Message::new(
                5,
                Body::HeartbeatReq {
                    commit: 49,
                    ctx: 0xABCD,
                },
            ),
            Message::new(5, Body::HeartbeatResp { ctx: 0xABCD }),
            Message::new(6, Body::TimeoutNow),
        ]
    }

    #[test]
    fn every_message_round_trips() {
        for m in sample_messages() {
            let bytes = m.encode();
            assert_eq!(
                Message::decode(&bytes).unwrap(),
                m,
                "failed on {}",
                m.summary()
            );
        }
    }

    #[test]
    fn frames_round_trip_and_verify() {
        for m in sample_messages() {
            let w = Wire::Raft(m.clone());
            let bytes = w.encode();
            assert_eq!(Wire::decode(&bytes).unwrap(), w);
        }
    }

    #[test]
    fn a_flipped_bit_anywhere_in_a_frame_is_rejected() {
        let bytes = Wire::Raft(Message::new(7, Body::TimeoutNow)).encode();
        for i in 0..bytes.len() {
            for bit in 0..8 {
                let mut bad = bytes.clone();
                bad[i] ^= 1 << bit;
                if bad == bytes {
                    continue;
                }
                // Either it fails to decode, or — for a flip inside the CRC
                // field itself — it fails the checksum. Never a wrong message.
                match Wire::decode(&bad) {
                    Err(_) => {}
                    Ok(other) => panic!(
                        "byte {i} bit {bit}: corruption decoded as a valid message {other:?}"
                    ),
                }
            }
        }
    }

    #[test]
    fn truncated_frames_are_rejected_not_panicked() {
        let bytes = Wire::Raft(Message::new(
            4,
            Body::AppendReq {
                prev_index: 1,
                prev_term: 1,
                commit: 1,
                entries: vec![Entry {
                    term: 1,
                    index: 2,
                    kind: EntryKind::Normal(vec![7; 40]),
                }],
            },
        ))
        .encode();
        for n in 0..bytes.len() {
            assert!(
                Wire::decode(&bytes[..n]).is_err(),
                "prefix of length {n} decoded"
            );
        }
        assert!(Wire::decode(&bytes).is_ok());
    }

    #[test]
    fn arbitrary_bytes_never_panic_the_decoder() {
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        for _ in 0..50_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let len = (state % 96) as usize;
            let buf: Vec<u8> = (0..len).map(|i| (state >> ((i % 8) * 8)) as u8).collect();
            let _ = Wire::decode(&buf);
        }
    }

    #[test]
    fn a_wrong_protocol_version_is_rejected() {
        let mut bytes = Wire::Raft(Message::new(1, Body::TimeoutNow)).encode();
        bytes[0] = PROTOCOL_VERSION.wrapping_add(1);
        assert!(Wire::decode(&bytes).is_err());
    }

    #[test]
    fn pre_vote_messages_are_flagged() {
        assert!(Message::new(
            1,
            Body::PreVoteReq {
                last_index: 0,
                last_term: 0
            }
        )
        .is_pre_vote());
        assert!(Message::new(1, Body::PreVoteResp { granted: true }).is_pre_vote());
        assert!(!Message::new(
            1,
            Body::VoteReq {
                last_index: 0,
                last_term: 0
            }
        )
        .is_pre_vote());
    }
}
