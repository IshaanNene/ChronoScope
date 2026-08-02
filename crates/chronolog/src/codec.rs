//! Hand-rolled binary encoding.
//!
//! No `serde`, no `bincode`. Three reasons, in order of how much they matter:
//!
//! 1. **The decoder is an attack surface and a fuzz target.** Every byte that
//!    arrives from the network or off a possibly-corrupt disk goes through
//!    here. Writing it by hand means every bounds check is visible, and
//!    `decode` is total: it returns `Err` for all 2^n inputs and panics for
//!    none. That is a property you can fuzz, and `fuzz/` does.
//! 2. **The format is pinned.** A derive macro's output is whatever the crate
//!    version produces. A replay from a seed recorded six months ago has to
//!    decode identically, so the layout is specified here rather than inferred.
//! 3. Zero dependencies in the core, per the project rule.
//!
//! Everything is little-endian and fixed-width. Varints would save bytes on the
//! wire and cost determinism-relevant branching; the WAL is fsync-bound, not
//! bandwidth-bound, so the trade is not close.

use std::fmt;

/// What went wrong decoding. Deliberately coarse — a decoder that reports
/// precisely which field was malformed is a decoder that tells an attacker
/// which field to malform next.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodeError {
    /// Ran off the end of the buffer.
    Truncated,
    /// A tag byte that does not correspond to any variant.
    BadTag(u8),
    /// A length prefix larger than the remaining buffer, or larger than the
    /// configured maximum.
    BadLength(u64),
    /// CRC mismatch: the bytes are not what was written.
    BadChecksum { expected: u32, actual: u32 },
    /// Well-formed but semantically impossible.
    Invalid(&'static str),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::Truncated => write!(f, "truncated"),
            DecodeError::BadTag(t) => write!(f, "unknown tag {t}"),
            DecodeError::BadLength(n) => write!(f, "implausible length {n}"),
            DecodeError::BadChecksum { expected, actual } => {
                write!(f, "checksum {actual:#010x} != {expected:#010x}")
            }
            DecodeError::Invalid(s) => write!(f, "invalid: {s}"),
        }
    }
}

impl std::error::Error for DecodeError {}

pub type Result<T> = std::result::Result<T, DecodeError>;

/// Anything longer than this is a corrupt length prefix, not a real message.
/// Without this bound, a torn 4-byte length field reading `0xFFFFFFFF` becomes
/// a 4 GiB allocation, and the node OOMs instead of reporting corruption.
pub const MAX_FRAME: usize = 64 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------

/// Append-only encoder over a `Vec<u8>`.
#[derive(Debug, Default)]
pub struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub fn with_capacity(n: usize) -> Self {
        Self {
            buf: Vec::with_capacity(n),
        }
    }

    pub fn u8(&mut self, v: u8) -> &mut Self {
        self.buf.push(v);
        self
    }

    pub fn u32(&mut self, v: u32) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    pub fn u64(&mut self, v: u64) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    pub fn bool(&mut self, v: bool) -> &mut Self {
        self.u8(v as u8)
    }

    /// Length-prefixed bytes.
    pub fn bytes(&mut self, v: &[u8]) -> &mut Self {
        self.u32(v.len() as u32);
        self.buf.extend_from_slice(v);
        self
    }

    pub fn str(&mut self, v: &str) -> &mut Self {
        self.bytes(v.as_bytes())
    }

    /// A length-prefixed sequence, written by `f` per element.
    pub fn seq<T, F>(&mut self, items: &[T], mut f: F) -> &mut Self
    where
        F: FnMut(&mut Writer, &T),
    {
        self.u32(items.len() as u32);
        for item in items {
            f(self, item);
        }
        self
    }

    /// `None` is a zero byte; `Some` is a one byte followed by the value.
    pub fn opt<T, F>(&mut self, v: &Option<T>, f: F) -> &mut Self
    where
        F: FnOnce(&mut Writer, &T),
    {
        match v {
            None => self.u8(0),
            Some(x) => {
                self.u8(1);
                f(self, x);
                self
            }
        }
    }

    pub fn finish(self) -> Vec<u8> {
        self.buf
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

/// Bounds-checked cursor. Every read either consumes exactly what it claims or
/// returns `Truncated` — there is no path that reads past the end.
#[derive(Debug)]
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub fn is_done(&self) -> bool {
        self.pos >= self.buf.len()
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(DecodeError::Truncated);
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub fn u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub fn u64(&mut self) -> Result<u64> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    pub fn bool(&mut self) -> Result<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            // A bool that is neither 0 nor 1 means the bytes are not what the
            // encoder wrote. Corruption, not a value.
            n => Err(DecodeError::BadTag(n)),
        }
    }

    pub fn bytes(&mut self) -> Result<Vec<u8>> {
        let n = self.u32()? as usize;
        // Check against what is actually here before allocating, so a corrupt
        // length prefix cannot turn into a huge allocation.
        if n > self.remaining() {
            return Err(DecodeError::BadLength(n as u64));
        }
        Ok(self.take(n)?.to_vec())
    }

    pub fn str(&mut self) -> Result<String> {
        let b = self.bytes()?;
        String::from_utf8(b).map_err(|_| DecodeError::Invalid("not utf-8"))
    }

    pub fn seq<T, F>(&mut self, mut f: F) -> Result<Vec<T>>
    where
        F: FnMut(&mut Reader<'a>) -> Result<T>,
    {
        let n = self.u32()? as usize;
        // Each element is at least one byte, so a count exceeding the bytes
        // remaining is corrupt. Bounds the allocation without knowing the
        // element size.
        if n > self.remaining() {
            return Err(DecodeError::BadLength(n as u64));
        }
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(f(self)?);
        }
        Ok(out)
    }

    pub fn opt<T, F>(&mut self, f: F) -> Result<Option<T>>
    where
        F: FnOnce(&mut Reader<'a>) -> Result<T>,
    {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(f(self)?)),
            n => Err(DecodeError::BadTag(n)),
        }
    }

    /// Everything not yet consumed.
    pub fn rest(&mut self) -> &'a [u8] {
        let s = &self.buf[self.pos..];
        self.pos = self.buf.len();
        s
    }
}

// ---------------------------------------------------------------------------
// CRC32C
// ---------------------------------------------------------------------------

/// CRC32C (Castagnoli, reflected polynomial `0x82F63B78`).
///
/// Castagnoli rather than the zip/gzip polynomial because it has better error
/// detection at the record sizes a WAL writes, and because it is the one with
/// hardware support (`crc32cx` on aarch64, `crc32` on SSE4.2) — so the
/// production path can be made fast later without changing the format.
///
/// The table is built at first use rather than written out as 1024 lines of
/// hex constants.
fn table() -> &'static [u32; 256] {
    use std::sync::OnceLock;
    static TABLE: OnceLock<[u32; 256]> = OnceLock::new();
    TABLE.get_or_init(|| {
        let mut t = [0u32; 256];
        for (i, slot) in t.iter_mut().enumerate() {
            let mut c = i as u32;
            for _ in 0..8 {
                c = if c & 1 != 0 {
                    0x82F6_3B78 ^ (c >> 1)
                } else {
                    c >> 1
                };
            }
            *slot = c;
        }
        t
    })
}

pub fn crc32c(data: &[u8]) -> u32 {
    crc32c_update(0, data)
}

/// Incremental, so a record's header and payload can be checksummed without
/// concatenating them into a temporary buffer.
pub fn crc32c_update(seed: u32, data: &[u8]) -> u32 {
    let t = table();
    let mut crc = !seed;
    for &b in data {
        crc = t[((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_matches_the_published_vectors() {
        // From RFC 3720 appendix B.4, the iSCSI CRC32C test vectors.
        assert_eq!(crc32c(&[]), 0x0000_0000);
        assert_eq!(crc32c(&[0u8; 32]), 0x8A91_36AA);
        assert_eq!(crc32c(&[0xFFu8; 32]), 0x62A8_AB43);
        let inc: Vec<u8> = (0u8..32).collect();
        assert_eq!(crc32c(&inc), 0x46DD_794E);
        let dec: Vec<u8> = (0u8..32).rev().collect();
        assert_eq!(crc32c(&dec), 0x113F_DB5C);
    }

    #[test]
    fn crc32c_incremental_matches_one_shot() {
        let data: Vec<u8> = (0..1000u32).map(|i| (i * 7) as u8).collect();
        let one_shot = crc32c(&data);
        let mut acc = 0;
        for chunk in data.chunks(37) {
            acc = crc32c_update(acc, chunk);
        }
        assert_eq!(acc, one_shot);
    }

    #[test]
    fn crc32c_detects_a_single_bit_flip() {
        let data = vec![0xA5u8; 4096];
        let good = crc32c(&data);
        for bit in 0..64 {
            let mut bad = data.clone();
            bad[bit / 8] ^= 1 << (bit % 8);
            assert_ne!(crc32c(&bad), good, "bit {bit} flip went undetected");
        }
    }

    #[test]
    fn round_trips_every_primitive() {
        let mut w = Writer::new();
        w.u8(7)
            .u32(0xDEAD_BEEF)
            .u64(u64::MAX)
            .bool(true)
            .bool(false)
            .bytes(b"hello")
            .str("wide");
        w.seq(&[1u64, 2, 3], |w, v| {
            w.u64(*v);
        });
        w.opt(&Some(42u64), |w, v| {
            w.u64(*v);
        });
        w.opt(&None::<u64>, |w, v| {
            w.u64(*v);
        });
        let buf = w.finish();

        let mut r = Reader::new(&buf);
        assert_eq!(r.u8().unwrap(), 7);
        assert_eq!(r.u32().unwrap(), 0xDEAD_BEEF);
        assert_eq!(r.u64().unwrap(), u64::MAX);
        assert!(r.bool().unwrap());
        assert!(!r.bool().unwrap());
        assert_eq!(r.bytes().unwrap(), b"hello");
        assert_eq!(r.str().unwrap(), "wide");
        assert_eq!(r.seq(|r| r.u64()).unwrap(), vec![1, 2, 3]);
        assert_eq!(r.opt(|r| r.u64()).unwrap(), Some(42));
        assert_eq!(r.opt(|r| r.u64()).unwrap(), None);
        assert!(r.is_done());
    }

    #[test]
    fn reading_past_the_end_errors_rather_than_panicking() {
        let buf = [1u8, 2, 3];
        let mut r = Reader::new(&buf);
        assert_eq!(r.u64(), Err(DecodeError::Truncated));
        let mut r = Reader::new(&buf);
        assert_eq!(r.u8().unwrap(), 1);
        assert_eq!(r.u32(), Err(DecodeError::Truncated));
    }

    #[test]
    fn a_corrupt_length_prefix_does_not_allocate() {
        // 0xFFFFFFFF bytes claimed, three available.
        let buf = [0xFF, 0xFF, 0xFF, 0xFF, 1, 2, 3];
        let mut r = Reader::new(&buf);
        assert_eq!(r.bytes(), Err(DecodeError::BadLength(0xFFFF_FFFF)));
        let mut r = Reader::new(&buf);
        assert_eq!(r.seq(|r| r.u64()), Err(DecodeError::BadLength(0xFFFF_FFFF)));
    }

    #[test]
    fn a_bool_that_is_not_zero_or_one_is_corruption() {
        let buf = [2u8];
        assert_eq!(Reader::new(&buf).bool(), Err(DecodeError::BadTag(2)));
    }

    #[test]
    fn decoding_is_total_over_arbitrary_bytes() {
        // The property the fuzzer checks, sampled deterministically here so it
        // also runs in CI without a fuzzing toolchain.
        let mut state = 0x243F_6A88_85A3_08D3u64;
        for _ in 0..20_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let len = (state % 64) as usize;
            let buf: Vec<u8> = (0..len).map(|i| (state >> (i % 8 * 8)) as u8).collect();
            let mut r = Reader::new(&buf);
            // Any sequence of reads must terminate with a value or an error.
            let _ = r.u8();
            let _ = r.bytes();
            let _ = r.seq(|r| r.u64());
            let _ = r.opt(|r| r.u32());
            let _ = r.str();
        }
    }
}
