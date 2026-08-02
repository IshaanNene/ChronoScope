//! The client protocol and session layer.
//!
//! # Why requests carry a sequence number
//!
//! The network may duplicate, and a client that times out will retry. Without
//! deduplication, `balance += 100` applied twice is a bug that no amount of
//! consensus will catch — Raft guarantees every replica applies the *same*
//! sequence of commands, not that the sequence is the one the client intended.
//!
//! So each client has an id, each request a monotonically increasing sequence
//! number, and the state machine keeps the last sequence number and response
//! per client. A replayed request returns the remembered response without
//! re-applying. This is Raft §6.3, and it is the difference between
//! at-least-once and exactly-once from the caller's point of view.

use chrono_sim::traits::NodeId;

use crate::codec::{DecodeError, Reader, Result, Writer};
use crate::types::Index;

/// How much staleness the caller will tolerate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadMode {
    /// `ReadIndex`: the leader confirms it is still the leader by hearing from
    /// a quorum, then waits for its state machine to catch up to the commit
    /// index it observed. Linearizable, and costs one round trip.
    Linearizable,
    /// The leader answers from local state if it heard from a quorum within
    /// the last election timeout.
    ///
    /// This is **not** linearizable, and the simulator demonstrates rather than
    /// assumes it: the lease is measured on the leader's own clock, so a leader
    /// whose clock runs slow believes its lease is valid after a new leader has
    /// already been elected, and serves a stale read. Free, and wrong under
    /// clock skew — which is exactly the trade being offered.
    Lease,
    /// Read local state on whatever node you asked. Sequentially consistent at
    /// best. Useful for caches, catastrophic for anything else.
    Stale,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Get { key: Vec<u8>, mode: ReadMode },
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    /// Compare and swap. `expect` of `None` means "only if absent".
    Cas { key: Vec<u8>, expect: Option<Vec<u8>>, value: Option<Vec<u8>> },
    /// Read the value at a specific MVCC version.
    GetAt { key: Vec<u8>, version: Index },
}

impl Op {
    /// Reads never go through the log; writes always do.
    pub fn is_read(&self) -> bool {
        matches!(self, Op::Get { .. } | Op::GetAt { .. })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    pub client_id: u64,
    /// Strictly increasing per client. The dedup key, together with `client_id`.
    pub seq: u64,
    pub op: Op,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// A read result: the value, or `None` if absent.
    Value(Option<Vec<u8>>),
    /// A write applied. Carries the log index, which is the MVCC version.
    Applied { version: Index },
    /// A CAS whose precondition did not hold, with what was actually there.
    CasFailed { actual: Option<Vec<u8>> },
    /// Ask someone else. The hint is the leader this node last heard from.
    NotLeader { hint: Option<NodeId> },
    /// The node could not confirm leadership in time — retry.
    Unavailable,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Response {
    pub client_id: u64,
    pub seq: u64,
    pub outcome: Outcome,
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

fn mode_tag(m: ReadMode) -> u8 {
    match m {
        ReadMode::Linearizable => 0,
        ReadMode::Lease => 1,
        ReadMode::Stale => 2,
    }
}

fn tag_mode(t: u8) -> Result<ReadMode> {
    match t {
        0 => Ok(ReadMode::Linearizable),
        1 => Ok(ReadMode::Lease),
        2 => Ok(ReadMode::Stale),
        t => Err(DecodeError::BadTag(t)),
    }
}

impl Request {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::with_capacity(48);
        w.u64(self.client_id).u64(self.seq);
        match &self.op {
            Op::Get { key, mode } => {
                w.u8(0).bytes(key).u8(mode_tag(*mode));
            }
            Op::Put { key, value } => {
                w.u8(1).bytes(key).bytes(value);
            }
            Op::Delete { key } => {
                w.u8(2).bytes(key);
            }
            Op::Cas { key, expect, value } => {
                w.u8(3).bytes(key);
                w.opt(expect, |w, v| {
                    w.bytes(v);
                });
                w.opt(value, |w, v| {
                    w.bytes(v);
                });
            }
            Op::GetAt { key, version } => {
                w.u8(4).bytes(key).u64(*version);
            }
        }
        w.finish()
    }

    pub fn decode(buf: &[u8]) -> Result<Request> {
        let mut r = Reader::new(buf);
        let client_id = r.u64()?;
        let seq = r.u64()?;
        let op = match r.u8()? {
            0 => Op::Get { key: r.bytes()?, mode: tag_mode(r.u8()?)? },
            1 => Op::Put { key: r.bytes()?, value: r.bytes()? },
            2 => Op::Delete { key: r.bytes()? },
            3 => Op::Cas {
                key: r.bytes()?,
                expect: r.opt(|r| r.bytes())?,
                value: r.opt(|r| r.bytes())?,
            },
            4 => Op::GetAt { key: r.bytes()?, version: r.u64()? },
            t => return Err(DecodeError::BadTag(t)),
        };
        Ok(Request { client_id, seq, op })
    }
}

impl Response {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::with_capacity(32);
        w.u64(self.client_id).u64(self.seq);
        match &self.outcome {
            Outcome::Value(v) => {
                w.u8(0);
                w.opt(v, |w, b| {
                    w.bytes(b);
                });
            }
            Outcome::Applied { version } => {
                w.u8(1).u64(*version);
            }
            Outcome::CasFailed { actual } => {
                w.u8(2);
                w.opt(actual, |w, b| {
                    w.bytes(b);
                });
            }
            Outcome::NotLeader { hint } => {
                w.u8(3);
                w.opt(hint, |w, h| {
                    w.u32(*h);
                });
            }
            Outcome::Unavailable => {
                w.u8(4);
            }
        }
        w.finish()
    }

    pub fn decode(buf: &[u8]) -> Result<Response> {
        let mut r = Reader::new(buf);
        let client_id = r.u64()?;
        let seq = r.u64()?;
        let outcome = match r.u8()? {
            0 => Outcome::Value(r.opt(|r| r.bytes())?),
            1 => Outcome::Applied { version: r.u64()? },
            2 => Outcome::CasFailed { actual: r.opt(|r| r.bytes())? },
            3 => Outcome::NotLeader { hint: r.opt(|r| r.u32())? },
            4 => Outcome::Unavailable,
            t => return Err(DecodeError::BadTag(t)),
        };
        Ok(Response { client_id, seq, outcome })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn samples() -> Vec<Request> {
        vec![
            Request {
                client_id: 1,
                seq: 1,
                op: Op::Get { key: b"a".to_vec(), mode: ReadMode::Linearizable },
            },
            Request {
                client_id: 2,
                seq: 9,
                op: Op::Get { key: b"a".to_vec(), mode: ReadMode::Lease },
            },
            Request { client_id: 3, seq: 2, op: Op::Put { key: b"k".to_vec(), value: vec![1; 40] } },
            Request { client_id: 4, seq: 3, op: Op::Delete { key: b"k".to_vec() } },
            Request {
                client_id: 5,
                seq: 4,
                op: Op::Cas {
                    key: b"k".to_vec(),
                    expect: Some(b"old".to_vec()),
                    value: Some(b"new".to_vec()),
                },
            },
            Request {
                client_id: 6,
                seq: 5,
                op: Op::Cas { key: b"k".to_vec(), expect: None, value: None },
            },
            Request { client_id: 7, seq: 6, op: Op::GetAt { key: b"k".to_vec(), version: 42 } },
        ]
    }

    #[test]
    fn requests_round_trip() {
        for req in samples() {
            assert_eq!(Request::decode(&req.encode()).unwrap(), req);
        }
    }

    #[test]
    fn responses_round_trip() {
        let outcomes = vec![
            Outcome::Value(Some(vec![1, 2, 3])),
            Outcome::Value(None),
            Outcome::Applied { version: 77 },
            Outcome::CasFailed { actual: Some(b"other".to_vec()) },
            Outcome::CasFailed { actual: None },
            Outcome::NotLeader { hint: Some(2) },
            Outcome::NotLeader { hint: None },
            Outcome::Unavailable,
        ];
        for outcome in outcomes {
            let resp = Response { client_id: 3, seq: 4, outcome };
            assert_eq!(Response::decode(&resp.encode()).unwrap(), resp);
        }
    }

    #[test]
    fn reads_and_writes_are_distinguished() {
        assert!(Op::Get { key: vec![], mode: ReadMode::Stale }.is_read());
        assert!(Op::GetAt { key: vec![], version: 1 }.is_read());
        assert!(!Op::Put { key: vec![], value: vec![] }.is_read());
        assert!(!Op::Cas { key: vec![], expect: None, value: None }.is_read());
        assert!(!Op::Delete { key: vec![] }.is_read());
    }

    #[test]
    fn truncated_requests_are_rejected_not_panicked() {
        for req in samples() {
            let bytes = req.encode();
            for n in 0..bytes.len() {
                let _ = Request::decode(&bytes[..n]);
            }
        }
    }
}
