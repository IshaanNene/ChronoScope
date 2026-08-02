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
    Get {
        key: Vec<u8>,
        mode: ReadMode,
    },
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    /// Compare and swap. `expect` of `None` means "only if absent".
    Cas {
        key: Vec<u8>,
        expect: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
    },
    /// Read the value at a specific MVCC version.
    GetAt {
        key: Vec<u8>,
        version: Index,
    },
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
            0 => Op::Get {
                key: r.bytes()?,
                mode: tag_mode(r.u8()?)?,
            },
            1 => Op::Put {
                key: r.bytes()?,
                value: r.bytes()?,
            },
            2 => Op::Delete { key: r.bytes()? },
            3 => Op::Cas {
                key: r.bytes()?,
                expect: r.opt(|r| r.bytes())?,
                value: r.opt(|r| r.bytes())?,
            },
            4 => Op::GetAt {
                key: r.bytes()?,
                version: r.u64()?,
            },
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
            2 => Outcome::CasFailed {
                actual: r.opt(|r| r.bytes())?,
            },
            3 => Outcome::NotLeader {
                hint: r.opt(|r| r.u32())?,
            },
            4 => Outcome::Unavailable,
            t => return Err(DecodeError::BadTag(t)),
        };
        Ok(Response {
            client_id,
            seq,
            outcome,
        })
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
                op: Op::Get {
                    key: b"a".to_vec(),
                    mode: ReadMode::Linearizable,
                },
            },
            Request {
                client_id: 2,
                seq: 9,
                op: Op::Get {
                    key: b"a".to_vec(),
                    mode: ReadMode::Lease,
                },
            },
            Request {
                client_id: 3,
                seq: 2,
                op: Op::Put {
                    key: b"k".to_vec(),
                    value: vec![1; 40],
                },
            },
            Request {
                client_id: 4,
                seq: 3,
                op: Op::Delete { key: b"k".to_vec() },
            },
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
                op: Op::Cas {
                    key: b"k".to_vec(),
                    expect: None,
                    value: None,
                },
            },
            Request {
                client_id: 7,
                seq: 6,
                op: Op::GetAt {
                    key: b"k".to_vec(),
                    version: 42,
                },
            },
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
            Outcome::CasFailed {
                actual: Some(b"other".to_vec()),
            },
            Outcome::CasFailed { actual: None },
            Outcome::NotLeader { hint: Some(2) },
            Outcome::NotLeader { hint: None },
            Outcome::Unavailable,
        ];
        for outcome in outcomes {
            let resp = Response {
                client_id: 3,
                seq: 4,
                outcome,
            };
            assert_eq!(Response::decode(&resp.encode()).unwrap(), resp);
        }
    }

    #[test]
    fn reads_and_writes_are_distinguished() {
        assert!(Op::Get {
            key: vec![],
            mode: ReadMode::Stale
        }
        .is_read());
        assert!(Op::GetAt {
            key: vec![],
            version: 1
        }
        .is_read());
        assert!(!Op::Put {
            key: vec![],
            value: vec![]
        }
        .is_read());
        assert!(!Op::Cas {
            key: vec![],
            expect: None,
            value: None
        }
        .is_read());
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

// ---------------------------------------------------------------------------
// The session driver
// ---------------------------------------------------------------------------

use chrono_sim::time::Nanos;
use chrono_sim::traits::Host;

use crate::chan::Chan;
use crate::msg::Wire;

/// What the receive task feeds the session loop.
enum Event {
    Reply(Response),
    /// A deadline fired. Carries the attempt token so a late timeout from an
    /// earlier attempt cannot abort the current one.
    Deadline(u64),
}

/// How a call ended, from the caller's point of view.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallResult {
    /// The cluster answered.
    Ok(Outcome),
    /// Every attempt timed out or was redirected. **This is not a failure to
    /// apply.** The write may have committed and the response been lost; a
    /// linearizability checker has to treat it as an operation of unknown
    /// outcome, and any checker that assumes otherwise will report phantom
    /// violations.
    Unknown,
}

/// A client session: leader discovery, retries, and idempotent request IDs.
pub struct Client {
    host: Host,
    id: u64,
    seq: u64,
    token: u64,
    servers: Vec<NodeId>,
    /// Cached from the last `NotLeader` redirect, so the common case is one hop.
    leader_hint: Option<NodeId>,
    timeout: Nanos,
    max_attempts: u32,
    inbox: Chan<Event>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("id", &self.id)
            .field("seq", &self.seq)
            .field("leader_hint", &self.leader_hint)
            .finish()
    }
}

impl Client {
    /// Spawns a receive task on `host`; the returned client owns the session.
    pub fn new(host: Host, id: u64, servers: Vec<NodeId>) -> Client {
        let inbox: Chan<Event> = Chan::new();
        let rx = inbox.clone();
        host.spawn_with("client-rx", |h| async move {
            while let Some(env) = h.net.recv().await {
                if let Ok(Wire::Reply(resp)) = Wire::decode(&env.payload) {
                    rx.send(Event::Reply(resp));
                }
            }
            rx.close();
        });
        Client {
            host,
            id,
            seq: 0,
            token: 0,
            servers,
            leader_hint: None,
            timeout: Nanos::from_millis(500),
            max_attempts: 12,
            inbox,
        }
    }

    pub fn with_timeout(mut self, timeout: Nanos) -> Client {
        self.timeout = timeout;
        self
    }

    pub fn with_max_attempts(mut self, n: u32) -> Client {
        self.max_attempts = n;
        self
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    /// Where the next attempt goes: the cached leader, else a random server.
    fn target(&self) -> NodeId {
        match self.leader_hint {
            Some(h) if self.servers.contains(&h) => h,
            _ => {
                let i = (self.host.rng.next_u64() % self.servers.len().max(1) as u64) as usize;
                self.servers[i]
            }
        }
    }

    /// How long to wait before the next attempt after a redirect.
    ///
    /// Full jitter: `random(0, min(cap, base * 2^attempt))`. Both halves earn
    /// their keep. The **backoff** is what stops a redirect storm: a cluster
    /// mid-election answers `NotLeader` in microseconds, so a client that
    /// retries immediately burns its entire attempt budget before the election
    /// finishes and reports failure on a perfectly healthy cluster. The
    /// **jitter** is what stops a thundering herd: without it every client
    /// blocked by the same election retries at the same instant forever.
    fn redirect_backoff(&self, attempt: u32) -> Nanos {
        let base = Nanos::from_millis(5).0;
        let cap = self.timeout.0.max(base);
        let ceiling = base.saturating_mul(1u64 << attempt.min(8)).min(cap);
        Nanos(self.host.rng.next_u64() % ceiling.max(1))
    }

    /// Issue one operation, retrying until it is answered or the attempts run
    /// out. The sequence number is fixed for the whole call, which is what
    /// makes a retry idempotent rather than a second write.
    pub async fn call(&mut self, op: Op) -> CallResult {
        self.seq += 1;
        let req = Request {
            client_id: self.id,
            seq: self.seq,
            op,
        };
        let payload = Wire::Client(req.clone()).encode();
        // Counts only redirects, not timeouts — a timeout has already waited.
        let mut redirects: u32 = 0;

        for _ in 0..self.max_attempts {
            let target = self.target();
            self.host.net.send(target, payload.clone());

            self.token += 1;
            let token = self.token;
            let tx = self.inbox.clone();
            let timeout = self.timeout;
            // A timer task rather than a select combinator: the trait surface
            // offers no select, and funnelling the deadline into the same queue
            // keeps the ordering explicit.
            self.host.spawn_with("client-timeout", move |h| async move {
                h.sleep(timeout).await;
                tx.send(Event::Deadline(token));
            });

            let mut redirected = false;
            loop {
                match self.inbox.recv().await {
                    None => return CallResult::Unknown,
                    Some(Event::Deadline(t)) if t == token => break,
                    Some(Event::Deadline(_)) => continue, // a stale attempt's timer
                    Some(Event::Reply(resp)) => {
                        if resp.client_id != self.id || resp.seq != self.seq {
                            continue; // a duplicate of an earlier call
                        }
                        match resp.outcome {
                            Outcome::NotLeader { hint } => {
                                self.leader_hint = hint;
                                redirected = true;
                                break;
                            }
                            Outcome::Unavailable => {
                                self.leader_hint = None;
                                redirected = true;
                                break;
                            }
                            other => return CallResult::Ok(other),
                        }
                    }
                }
            }

            if redirected {
                let wait = self.redirect_backoff(redirects);
                redirects += 1;
                self.host.sleep(wait).await;
            } else {
                // The attempt timed out; that is already a long enough pause,
                // and the redirect ladder should not carry over from it.
                redirects = 0;
            }
        }
        CallResult::Unknown
    }

    pub async fn put(&mut self, key: &[u8], value: &[u8]) -> CallResult {
        self.call(Op::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        })
        .await
    }

    pub async fn get(&mut self, key: &[u8], mode: ReadMode) -> CallResult {
        self.call(Op::Get {
            key: key.to_vec(),
            mode,
        })
        .await
    }

    pub async fn delete(&mut self, key: &[u8]) -> CallResult {
        self.call(Op::Delete { key: key.to_vec() }).await
    }

    pub async fn cas(
        &mut self,
        key: &[u8],
        expect: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
    ) -> CallResult {
        self.call(Op::Cas {
            key: key.to_vec(),
            expect,
            value,
        })
        .await
    }
}
