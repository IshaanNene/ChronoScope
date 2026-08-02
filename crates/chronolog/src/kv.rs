//! The replicated state machine: a linearizable key-value store with per-key
//! MVCC, and the session table that makes client retries idempotent.
//!
//! # Versions are log indices
//!
//! Every write is stamped with the Raft index that carried it. That is not
//! decoration — it means a version is globally meaningful across the cluster
//! (every replica applies the same index to the same value), it gives reads a
//! cheap consistency token, and it makes the MVCC history a direct picture of
//! the log. There is no separate version counter to get out of step.
//!
//! # Why the session table lives here
//!
//! Deduplication has to happen at *apply* time, inside the state machine, not
//! at the RPC layer. A retried request that reaches a different leader must be
//! deduplicated too, and only the replicated state machine is common to both.
//! Putting the table anywhere else means a leader change turns a retry into a
//! double-apply.
//!
//! That also means the table is part of the snapshot. A node that restores from
//! a snapshot and forgets which requests it has served will happily apply an
//! in-flight retry a second time.

use std::collections::BTreeMap;

use crate::client::{Op, Outcome, Request};
use crate::codec::{DecodeError, Reader, Result, Writer};
use crate::types::Index;

/// One version of one key. `None` is a tombstone — a delete has to be a
/// version rather than a removal, or a read at an earlier version could not
/// tell "deleted at v9" from "never existed".
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Version {
    pub version: Index,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Session {
    last_seq: u64,
    last_outcome: Outcome,
}

#[derive(Clone, Debug, Default)]
pub struct KvStore {
    /// Versions per key, oldest first.
    data: BTreeMap<Vec<u8>, Vec<Version>>,
    sessions: BTreeMap<u64, Session>,
    applied: Index,
    /// How many versions to retain per key. Unbounded MVCC is a memory leak
    /// with extra steps.
    keep_versions: usize,
}

impl KvStore {
    pub fn new() -> KvStore {
        KvStore {
            keep_versions: 16,
            ..Default::default()
        }
    }

    pub fn applied_index(&self) -> Index {
        self.applied
    }

    pub fn len(&self) -> usize {
        self.data
            .values()
            .filter(|vs| vs.last().map(|v| v.value.is_some()).unwrap_or(false))
            .count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The current value of `key`.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.data.get(key)?.last()?.value.as_deref()
    }

    /// The value as of `version` — the newest write at or below it.
    pub fn get_at(&self, key: &[u8], version: Index) -> Option<&[u8]> {
        let versions = self.data.get(key)?;
        versions
            .iter()
            .rev()
            .find(|v| v.version <= version)?
            .value
            .as_deref()
    }

    /// Every live key, for debugging and the `/debug` endpoint.
    pub fn keys(&self) -> impl Iterator<Item = &Vec<u8>> {
        self.data
            .iter()
            .filter(|(_, vs)| vs.last().map(|v| v.value.is_some()).unwrap_or(false))
            .map(|(k, _)| k)
    }

    fn write(&mut self, key: &[u8], value: Option<Vec<u8>>, version: Index) {
        let versions = self.data.entry(key.to_vec()).or_default();
        versions.push(Version { version, value });
        if versions.len() > self.keep_versions {
            let excess = versions.len() - self.keep_versions;
            versions.drain(..excess);
        }
    }

    /// Apply a committed request at log index `index`.
    ///
    /// Returns the outcome to send back to the client. Applying is
    /// deterministic given `(index, request)` and the current state — no
    /// clocks, no randomness, no iteration-order dependence — which is what
    /// makes every replica reach the same state and what makes the whole thing
    /// simulatable.
    pub fn apply(&mut self, index: Index, req: &Request) -> Outcome {
        self.applied = self.applied.max(index);

        // --- deduplication ------------------------------------------------
        if let Some(session) = self.sessions.get(&req.client_id) {
            if req.seq == session.last_seq {
                // The exact request we last served. Return the remembered
                // answer without re-applying: this is the difference between
                // at-least-once and exactly-once.
                return session.last_outcome.clone();
            }
            if req.seq < session.last_seq {
                // Older than what we have already served. The client has moved
                // on and is not waiting for this; we no longer hold the answer.
                return Outcome::Unavailable;
            }
        }

        let outcome = match &req.op {
            // Reads should not reach the log at all, but a client can put one
            // there. Serving it from the applied state is still correct.
            Op::Get { key, .. } => Outcome::Value(self.get(key).map(|v| v.to_vec())),
            Op::GetAt { key, version } => {
                Outcome::Value(self.get_at(key, *version).map(|v| v.to_vec()))
            }
            Op::Put { key, value } => {
                self.write(key, Some(value.clone()), index);
                Outcome::Applied { version: index }
            }
            Op::Delete { key } => {
                self.write(key, None, index);
                Outcome::Applied { version: index }
            }
            Op::Cas { key, expect, value } => {
                let actual = self.get(key).map(|v| v.to_vec());
                if actual.as_deref() == expect.as_deref() {
                    self.write(key, value.clone(), index);
                    Outcome::Applied { version: index }
                } else {
                    Outcome::CasFailed { actual }
                }
            }
        };

        self.sessions.insert(
            req.client_id,
            Session {
                last_seq: req.seq,
                last_outcome: outcome.clone(),
            },
        );
        outcome
    }

    /// Serialize for a snapshot. Includes the session table — a node that
    /// restores without it will double-apply any retry still in flight.
    pub fn snapshot(&self) -> Vec<u8> {
        let mut w = Writer::with_capacity(4096);
        w.u64(self.applied);
        let keys: Vec<&Vec<u8>> = self.data.keys().collect();
        w.u32(keys.len() as u32);
        for k in keys {
            w.bytes(k);
            let versions = &self.data[k];
            w.u32(versions.len() as u32);
            for v in versions {
                w.u64(v.version);
                w.opt(&v.value, |w, b| {
                    w.bytes(b);
                });
            }
        }
        let ids: Vec<u64> = self.sessions.keys().copied().collect();
        w.u32(ids.len() as u32);
        for id in ids {
            let s = &self.sessions[&id];
            w.u64(id).u64(s.last_seq);
            let bytes = crate::client::Response {
                client_id: id,
                seq: s.last_seq,
                outcome: s.last_outcome.clone(),
            }
            .encode();
            w.bytes(&bytes);
        }
        w.finish()
    }

    pub fn restore(buf: &[u8]) -> Result<KvStore> {
        let mut r = Reader::new(buf);
        let applied = r.u64()?;
        let mut data = BTreeMap::new();
        let n = r.u32()? as usize;
        if n > r.remaining() {
            return Err(DecodeError::BadLength(n as u64));
        }
        for _ in 0..n {
            let key = r.bytes()?;
            let vn = r.u32()? as usize;
            if vn > r.remaining() {
                return Err(DecodeError::BadLength(vn as u64));
            }
            let mut versions = Vec::with_capacity(vn);
            for _ in 0..vn {
                let version = r.u64()?;
                let value = r.opt(|r| r.bytes())?;
                versions.push(Version { version, value });
            }
            data.insert(key, versions);
        }
        let mut sessions = BTreeMap::new();
        let sn = r.u32()? as usize;
        if sn > r.remaining() {
            return Err(DecodeError::BadLength(sn as u64));
        }
        for _ in 0..sn {
            let id = r.u64()?;
            let last_seq = r.u64()?;
            let bytes = r.bytes()?;
            let resp = crate::client::Response::decode(&bytes)?;
            sessions.insert(
                id,
                Session {
                    last_seq,
                    last_outcome: resp.outcome,
                },
            );
        }
        Ok(KvStore {
            data,
            sessions,
            applied,
            keep_versions: 16,
        })
    }
}

/// A committed log entry's payload is an encoded [`Request`].
pub fn decode_command(data: &[u8]) -> Result<Request> {
    Request::decode(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(client: u64, seq: u64, k: &str, v: &str) -> Request {
        Request {
            client_id: client,
            seq,
            op: Op::Put {
                key: k.as_bytes().to_vec(),
                value: v.as_bytes().to_vec(),
            },
        }
    }

    fn del(client: u64, seq: u64, k: &str) -> Request {
        Request {
            client_id: client,
            seq,
            op: Op::Delete {
                key: k.as_bytes().to_vec(),
            },
        }
    }

    #[test]
    fn puts_and_gets_round_trip() {
        let mut kv = KvStore::new();
        assert_eq!(
            kv.apply(1, &put(1, 1, "a", "1")),
            Outcome::Applied { version: 1 }
        );
        assert_eq!(kv.get(b"a"), Some(&b"1"[..]));
        assert_eq!(kv.get(b"missing"), None);
        assert_eq!(kv.applied_index(), 1);
    }

    #[test]
    fn a_delete_is_a_tombstone_not_a_removal() {
        let mut kv = KvStore::new();
        kv.apply(1, &put(1, 1, "a", "one"));
        kv.apply(2, &del(1, 2, "a"));
        assert_eq!(kv.get(b"a"), None, "the key reads as absent");
        // But history is intact: a read at version 1 still sees the old value.
        assert_eq!(kv.get_at(b"a", 1), Some(&b"one"[..]));
        assert_eq!(kv.get_at(b"a", 2), None);
    }

    #[test]
    fn mvcc_reads_see_the_version_that_was_current() {
        let mut kv = KvStore::new();
        kv.apply(5, &put(1, 1, "k", "v5"));
        kv.apply(9, &put(1, 2, "k", "v9"));
        kv.apply(12, &put(1, 3, "k", "v12"));
        assert_eq!(
            kv.get_at(b"k", 4),
            None,
            "before the first write the key did not exist"
        );
        assert_eq!(kv.get_at(b"k", 5), Some(&b"v5"[..]));
        assert_eq!(
            kv.get_at(b"k", 8),
            Some(&b"v5"[..]),
            "no write between 5 and 9"
        );
        assert_eq!(kv.get_at(b"k", 11), Some(&b"v9"[..]));
        assert_eq!(kv.get_at(b"k", 99), Some(&b"v12"[..]));
        assert_eq!(kv.get(b"k"), Some(&b"v12"[..]));
    }

    #[test]
    fn old_versions_are_bounded() {
        let mut kv = KvStore::new();
        for i in 1..=100u64 {
            kv.apply(i, &put(1, i, "k", &format!("v{i}")));
        }
        assert_eq!(kv.get(b"k"), Some(&b"v100"[..]));
        assert_eq!(
            kv.data[&b"k".to_vec()].len(),
            16,
            "MVCC history must not grow unbounded"
        );
    }

    #[test]
    fn cas_applies_only_when_the_precondition_holds() {
        let mut kv = KvStore::new();
        // "only if absent"
        let create = Request {
            client_id: 1,
            seq: 1,
            op: Op::Cas {
                key: b"k".to_vec(),
                expect: None,
                value: Some(b"first".to_vec()),
            },
        };
        assert_eq!(kv.apply(1, &create), Outcome::Applied { version: 1 });

        // The same CAS again must fail, and report what is actually there.
        let again = Request {
            client_id: 2,
            seq: 1,
            ..create.clone()
        };
        assert_eq!(
            kv.apply(2, &again),
            Outcome::CasFailed {
                actual: Some(b"first".to_vec())
            }
        );

        let swap = Request {
            client_id: 3,
            seq: 1,
            op: Op::Cas {
                key: b"k".to_vec(),
                expect: Some(b"first".to_vec()),
                value: Some(b"second".to_vec()),
            },
        };
        assert_eq!(kv.apply(3, &swap), Outcome::Applied { version: 3 });
        assert_eq!(kv.get(b"k"), Some(&b"second"[..]));
    }

    #[test]
    fn a_retried_request_is_not_applied_twice() {
        // The property that makes client retries safe. Without it, a timeout
        // plus a retry silently doubles the write.
        let mut kv = KvStore::new();
        let req = put(7, 1, "counter", "incremented");
        let first = kv.apply(10, &req);
        assert_eq!(first, Outcome::Applied { version: 10 });

        // The same request arrives again at a later index — a retry that made
        // it into the log twice.
        let second = kv.apply(11, &req);
        assert_eq!(second, first, "a retry must return the remembered response");
        // And crucially, no new version was written.
        assert_eq!(
            kv.data[&b"counter".to_vec()].len(),
            1,
            "the retry must not write again"
        );
        assert_eq!(kv.get_at(b"counter", 10), Some(&b"incremented"[..]));
    }

    #[test]
    fn a_retried_cas_returns_the_original_answer_not_a_re_evaluation() {
        // The subtle case: re-evaluating a CAS on retry would report failure
        // for a request that actually succeeded, and the client would wrongly
        // conclude someone else won the race.
        let mut kv = KvStore::new();
        let cas = Request {
            client_id: 4,
            seq: 1,
            op: Op::Cas {
                key: b"k".to_vec(),
                expect: None,
                value: Some(b"mine".to_vec()),
            },
        };
        assert_eq!(kv.apply(1, &cas), Outcome::Applied { version: 1 });
        assert_eq!(
            kv.apply(2, &cas),
            Outcome::Applied { version: 1 },
            "the retry must report the original success, not a fresh CAS failure"
        );
    }

    #[test]
    fn a_stale_sequence_number_is_not_replayed() {
        let mut kv = KvStore::new();
        kv.apply(1, &put(1, 1, "a", "1"));
        kv.apply(2, &put(1, 2, "a", "2"));
        // seq 1 arriving after seq 2 must not resurrect the old value.
        let out = kv.apply(3, &put(1, 1, "a", "1"));
        assert_eq!(out, Outcome::Unavailable);
        assert_eq!(
            kv.get(b"a"),
            Some(&b"2"[..]),
            "a stale retry must not overwrite"
        );
    }

    #[test]
    fn different_clients_do_not_share_a_sequence_space() {
        let mut kv = KvStore::new();
        kv.apply(1, &put(1, 1, "a", "from-1"));
        // Client 2's seq 1 is unrelated and must apply normally.
        assert_eq!(
            kv.apply(2, &put(2, 1, "b", "from-2")),
            Outcome::Applied { version: 2 }
        );
        assert_eq!(kv.get(b"b"), Some(&b"from-2"[..]));
    }

    #[test]
    fn snapshots_round_trip_including_the_session_table() {
        let mut kv = KvStore::new();
        for i in 1..=20u64 {
            kv.apply(i, &put(i % 3, i, &format!("k{}", i % 5), &format!("v{i}")));
        }
        kv.apply(21, &del(0, 21, "k1"));

        let bytes = kv.snapshot();
        let restored = KvStore::restore(&bytes).expect("snapshot must decode");

        assert_eq!(restored.applied_index(), kv.applied_index());
        for k in kv.keys() {
            assert_eq!(
                restored.get(k),
                kv.get(k),
                "key {k:?} differs after restore"
            );
        }
        assert_eq!(restored.get(b"k1"), None, "the tombstone must survive");
        assert_eq!(
            restored.sessions, kv.sessions,
            "the session table is part of the snapshot"
        );
    }

    #[test]
    fn a_restored_node_still_deduplicates() {
        // The reason the session table is in the snapshot at all.
        let mut kv = KvStore::new();
        let req = put(9, 1, "x", "once");
        kv.apply(1, &req);
        let mut restored = KvStore::restore(&kv.snapshot()).unwrap();
        assert_eq!(
            restored.apply(2, &req),
            Outcome::Applied { version: 1 },
            "a node restored from a snapshot must still recognise a retry"
        );
        assert_eq!(restored.data[&b"x".to_vec()].len(), 1);
    }

    #[test]
    fn restoring_arbitrary_bytes_errors_rather_than_panicking() {
        let mut state = 0x1234_5678_9ABC_DEF0u64;
        for _ in 0..20_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let len = (state % 128) as usize;
            let buf: Vec<u8> = (0..len).map(|i| (state >> ((i % 8) * 8)) as u8).collect();
            let _ = KvStore::restore(&buf);
        }
    }

    #[test]
    fn applying_is_deterministic() {
        // Two stores fed the same sequence must be byte-identical. This is the
        // property every replica depends on.
        let build = || {
            let mut kv = KvStore::new();
            for i in 1..=200u64 {
                let r = match i % 4 {
                    0 => del(i % 7, i, &format!("k{}", i % 11)),
                    _ => put(i % 7, i, &format!("k{}", i % 11), &format!("v{i}")),
                };
                kv.apply(i, &r);
            }
            kv.snapshot()
        };
        assert_eq!(build(), build());
    }
}
