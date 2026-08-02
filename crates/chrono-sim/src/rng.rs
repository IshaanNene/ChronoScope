//! The single source of randomness.
//!
//! Everything nondeterministic in a Chronoscope run — packet latency, whether a
//! disk write tears, which runnable task the executor polls next — is drawn
//! from one `xoshiro256**` stream seeded by a 64-bit value. No component is
//! permitted to hold its own entropy.
//!
//! # Why no floats
//!
//! It is tempting to sample latency as `-mean * ln(u)` for an exponential
//! distribution. Don't. `f64::ln` is not required to be correctly rounded, and
//! implementations differ between libm versions and between x86-64 and
//! aarch64. A seed that reproduces on a laptop would diverge in CI, which
//! defeats the entire point of the project. Every distribution here is
//! integer-only and therefore bit-identical everywhere.

use std::sync::Mutex;

/// Seeds the main generator. `xoshiro` needs a well-mixed 256-bit state, and
/// feeding it a small integer directly produces a poor first few outputs.
#[derive(Debug)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// xoshiro256\*\* — fast, 256-bit state, passes BigCrush, trivially portable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Xoshiro256ss {
    s: [u64; 4],
}

impl Xoshiro256ss {
    pub fn from_seed(seed: u64) -> Self {
        let mut sm = SplitMix64::new(seed);
        Self { s: [sm.next_u64(), sm.next_u64(), sm.next_u64(), sm.next_u64()] }
    }

    #[inline]
    pub fn next_u64(&mut self) -> u64 {
        let result = self.s[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = self.s[1] << 17;
        self.s[2] ^= self.s[0];
        self.s[3] ^= self.s[1];
        self.s[1] ^= self.s[2];
        self.s[0] ^= self.s[3];
        self.s[2] ^= t;
        self.s[3] = self.s[3].rotate_left(45);
        result
    }

    /// Jump 2^128 steps. Used to hand each node an independent, non-overlapping
    /// substream so that adding a node does not perturb the others' draws.
    pub fn jump(&mut self) {
        const JUMP: [u64; 4] =
            [0x180E_C6D3_3CFD_0ABA, 0xD5A6_1266_F0C9_392C, 0xA958_2618_E03F_C9AA, 0x39AB_DC45_29B1_661C];
        let mut s = [0u64; 4];
        for &j in JUMP.iter() {
            for b in 0..64 {
                if (j >> b) & 1 == 1 {
                    for (i, acc) in s.iter_mut().enumerate() {
                        *acc ^= self.s[i];
                    }
                }
                self.next_u64();
            }
        }
        self.s = s;
    }
}

/// Object-safe randomness, so the simulated and real runtimes are swappable.
pub trait Rng: Send + Sync {
    fn next_u64(&self) -> u64;
}

/// A `Rng` backed by the deterministic stream. Interior mutability because the
/// handle is shared by every subsystem.
#[derive(Debug)]
pub struct SeededRng {
    inner: Mutex<Xoshiro256ss>,
}

impl SeededRng {
    pub fn new(seed: u64) -> Self {
        Self { inner: Mutex::new(Xoshiro256ss::from_seed(seed)) }
    }

    pub fn from_state(state: Xoshiro256ss) -> Self {
        Self { inner: Mutex::new(state) }
    }

    /// Fork an independent substream. The child is 2^128 steps ahead, so the
    /// two streams cannot collide within any run this universe will host.
    pub fn fork(&self) -> SeededRng {
        let mut g = self.inner.lock().unwrap();
        let mut child = g.clone();
        child.jump();
        let mut next_parent = child.clone();
        next_parent.jump();
        *g = next_parent;
        SeededRng::from_state(child)
    }

    pub fn snapshot(&self) -> Xoshiro256ss {
        self.inner.lock().unwrap().clone()
    }
}

impl Rng for SeededRng {
    #[inline]
    fn next_u64(&self) -> u64 {
        self.inner.lock().unwrap().next_u64()
    }
}

/// Sampling helpers, blanket-implemented so they are available on `dyn Rng`.
pub trait RngExt: Rng {
    /// Uniform in `[0, n)`, unbiased. Rejection sampling on the high bits;
    /// integer-only so it is identical on every target.
    fn below(&self, n: u64) -> u64 {
        if n <= 1 {
            return 0;
        }
        // Largest multiple of n that fits in u64; reject above it.
        let zone = u64::MAX - (u64::MAX % n) - 1;
        loop {
            let v = self.next_u64();
            if v <= zone {
                return v % n;
            }
        }
    }

    /// Uniform in `[lo, hi)`. Returns `lo` if the range is empty.
    fn range(&self, lo: u64, hi: u64) -> u64 {
        if hi <= lo {
            lo
        } else {
            lo + self.below(hi - lo)
        }
    }

    /// True with probability `num / den`.
    fn chance(&self, num: u32, den: u32) -> bool {
        if num == 0 {
            return false;
        }
        if num >= den {
            return true;
        }
        self.below(den as u64) < num as u64
    }

    /// True with probability `ppm / 1_000_000` — the unit fault policies use,
    /// because "0.03% packet loss" needs finer grain than percent.
    fn ppm(&self, ppm: u32) -> bool {
        self.chance(ppm, 1_000_000)
    }

    fn pick_index(&self, len: usize) -> Option<usize> {
        if len == 0 {
            None
        } else {
            Some(self.below(len as u64) as usize)
        }
    }

    /// Fisher-Yates. Used to randomize iteration where order would otherwise
    /// leak an implementation detail into the schedule.
    fn shuffle<T>(&self, items: &mut [T]) {
        if items.len() < 2 {
            return;
        }
        for i in (1..items.len()).rev() {
            let j = self.below(i as u64 + 1) as usize;
            items.swap(i, j);
        }
    }

    /// Pick an index weighted by `weights`. Panics only if the slice is empty.
    fn weighted_index(&self, weights: &[u32]) -> usize {
        let total: u64 = weights.iter().map(|&w| w as u64).sum();
        if total == 0 {
            return 0;
        }
        let mut r = self.below(total);
        for (i, &w) in weights.iter().enumerate() {
            if r < w as u64 {
                return i;
            }
            r -= w as u64;
        }
        weights.len() - 1
    }
}

impl<T: Rng + ?Sized> RngExt for T {}

/// A latency distribution expressed as weighted uniform buckets.
///
/// This is how the simulator gets a heavy tail without touching a float. A
/// realistic datacenter link is roughly "usually 200-800us, sometimes 2-10ms,
/// rarely 50-400ms" — which is exactly three buckets. The tail is where the
/// interesting bugs are: it is what turns a healthy cluster into one that
/// believes its leader is dead.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LatencyDist {
    /// `(weight, lo_nanos, hi_nanos)`
    buckets: Vec<(u32, u64, u64)>,
}

impl LatencyDist {
    pub fn new(buckets: Vec<(u32, u64, u64)>) -> Self {
        assert!(!buckets.is_empty(), "latency distribution needs at least one bucket");
        Self { buckets }
    }

    /// Every packet takes exactly `nanos`.
    pub fn fixed(nanos: u64) -> Self {
        Self::new(vec![(1, nanos, nanos + 1)])
    }

    /// Uniform in `[lo, hi)`.
    pub fn uniform(lo: u64, hi: u64) -> Self {
        Self::new(vec![(1, lo, hi.max(lo + 1))])
    }

    /// A same-rack link: sub-millisecond, with a thin tail.
    pub fn datacenter() -> Self {
        Self::new(vec![
            (900, 150_000, 900_000),        // 150us - 900us
            (95, 900_000, 8_000_000),       // 900us - 8ms
            (5, 8_000_000, 120_000_000),    // 8ms - 120ms, the ugly tail
        ])
    }

    /// Cross-region: tens of milliseconds, fatter tail.
    pub fn wan() -> Self {
        Self::new(vec![
            (850, 20_000_000, 60_000_000),
            (140, 60_000_000, 250_000_000),
            (10, 250_000_000, 2_000_000_000),
        ])
    }

    /// Sampled on every packet, so this deliberately avoids allocating.
    pub fn sample(&self, rng: &dyn Rng) -> u64 {
        let total: u64 = self.buckets.iter().map(|b| b.0 as u64).sum();
        if total == 0 {
            return self.buckets[0].1;
        }
        let mut r = rng.below(total);
        for &(w, lo, hi) in &self.buckets {
            if r < w as u64 {
                return rng.range(lo, hi);
            }
            r -= w as u64;
        }
        let (_, lo, hi) = self.buckets[self.buckets.len() - 1];
        rng.range(lo, hi)
    }

    /// Upper bound of the distribution — the liveness watchdog needs to know how
    /// long "unlucky but legal" can last before it calls a hang a hang.
    pub fn max(&self) -> u64 {
        self.buckets.iter().map(|b| b.2).max().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_stream() {
        let a = SeededRng::new(0x8f3a_2b1c);
        let b = SeededRng::new(0x8f3a_2b1c);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seed_different_stream() {
        let a = SeededRng::new(1);
        let b = SeededRng::new(2);
        let diff = (0..64).filter(|_| a.next_u64() != b.next_u64()).count();
        assert!(diff > 60, "streams should not correlate");
    }

    #[test]
    fn below_is_in_range_and_covers_it() {
        let r = SeededRng::new(7);
        let mut seen = [false; 5];
        for _ in 0..1000 {
            let v = r.below(5);
            assert!(v < 5);
            seen[v as usize] = true;
        }
        assert!(seen.iter().all(|&s| s), "every value in range should appear");
    }

    #[test]
    fn below_zero_and_one_are_zero() {
        let r = SeededRng::new(7);
        assert_eq!(r.below(0), 0);
        assert_eq!(r.below(1), 0);
    }

    #[test]
    fn weighted_index_respects_weights() {
        let r = SeededRng::new(11);
        let weights = [1u32, 99];
        let hits = (0..2000).filter(|_| r.weighted_index(&weights) == 1).count();
        assert!(hits > 1900, "the 99% bucket should dominate, got {hits}/2000");
    }

    #[test]
    fn zero_weights_do_not_panic() {
        let r = SeededRng::new(11);
        assert_eq!(r.weighted_index(&[0, 0, 0]), 0);
    }

    #[test]
    fn fork_produces_independent_streams() {
        let parent = SeededRng::new(42);
        let a = parent.fork();
        let b = parent.fork();
        let sa: Vec<u64> = (0..32).map(|_| a.next_u64()).collect();
        let sb: Vec<u64> = (0..32).map(|_| b.next_u64()).collect();
        assert_ne!(sa, sb);
    }

    #[test]
    fn shuffle_is_a_permutation() {
        let r = SeededRng::new(3);
        let mut v: Vec<u32> = (0..50).collect();
        r.shuffle(&mut v);
        let mut sorted = v.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, (0..50).collect::<Vec<_>>());
        assert_ne!(v, sorted, "50 elements should not shuffle back to identity");
    }

    #[test]
    fn latency_samples_stay_within_the_distribution() {
        let r = SeededRng::new(99);
        let d = LatencyDist::datacenter();
        for _ in 0..5000 {
            let v = d.sample(&r);
            assert!(v >= 150_000 && v < d.max());
        }
    }
}
