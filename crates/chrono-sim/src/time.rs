//! Virtual time.
//!
//! Everything is nanoseconds since an arbitrary epoch, stored as `u64`. That is
//! ~584 years of range, which is more simulated time than any seed will use.
//! `std::time::Instant` is deliberately absent from this module: an `Instant`
//! can only come from the host clock, and the host clock is the enemy.

use std::fmt;

pub const NANOS_PER_MICRO: u64 = 1_000;
pub const NANOS_PER_MILLI: u64 = 1_000_000;
pub const NANOS_PER_SEC: u64 = 1_000_000_000;

/// A point on the simulated timeline, in nanoseconds.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Nanos(pub u64);

impl Nanos {
    pub const ZERO: Nanos = Nanos(0);
    pub const MAX: Nanos = Nanos(u64::MAX);

    pub const fn from_millis(ms: u64) -> Self {
        Nanos(ms.saturating_mul(NANOS_PER_MILLI))
    }

    pub const fn from_micros(us: u64) -> Self {
        Nanos(us.saturating_mul(NANOS_PER_MICRO))
    }

    pub const fn from_secs(s: u64) -> Self {
        Nanos(s.saturating_mul(NANOS_PER_SEC))
    }

    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    pub const fn as_millis(self) -> u64 {
        self.0 / NANOS_PER_MILLI
    }

    pub const fn as_secs(self) -> u64 {
        self.0 / NANOS_PER_SEC
    }

    /// Saturating, because a virtual timeline should never wrap into the past
    /// just because a fault injector added a century of clock skew.
    pub const fn saturating_add(self, other: Nanos) -> Nanos {
        Nanos(self.0.saturating_add(other.0))
    }

    pub const fn saturating_sub(self, other: Nanos) -> Nanos {
        Nanos(self.0.saturating_sub(other.0))
    }

    /// Signed offset, used for clock skew, which can run behind true time.
    pub fn offset(self, delta: i64) -> Nanos {
        if delta >= 0 {
            Nanos(self.0.saturating_add(delta as u64))
        } else {
            Nanos(self.0.saturating_sub(delta.unsigned_abs()))
        }
    }
}

impl fmt::Debug for Nanos {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

/// Human-scaled so that a 4-hour simulated run reads as `4h12m03.412s` in a
/// trace rather than as nineteen digits.
impl fmt::Display for Nanos {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let n = self.0;
        if n < NANOS_PER_MICRO {
            return write!(f, "{n}ns");
        }
        if n < NANOS_PER_MILLI {
            return write!(f, "{}.{:03}us", n / 1_000, n % 1_000);
        }
        if n < NANOS_PER_SEC {
            return write!(f, "{}.{:03}ms", n / NANOS_PER_MILLI, (n % NANOS_PER_MILLI) / 1_000);
        }
        let secs = n / NANOS_PER_SEC;
        let frac = (n % NANOS_PER_SEC) / NANOS_PER_MILLI;
        let (h, m, s) = (secs / 3600, (secs % 3600) / 60, secs % 60);
        if h > 0 {
            write!(f, "{h}h{m:02}m{s:02}.{frac:03}s")
        } else if m > 0 {
            write!(f, "{m}m{s:02}.{frac:03}s")
        } else {
            write!(f, "{s}.{frac:03}s")
        }
    }
}

impl std::ops::Add for Nanos {
    type Output = Nanos;
    fn add(self, rhs: Nanos) -> Nanos {
        self.saturating_add(rhs)
    }
}

impl std::ops::Sub for Nanos {
    type Output = Nanos;
    fn sub(self, rhs: Nanos) -> Nanos {
        self.saturating_sub(rhs)
    }
}

impl From<std::time::Duration> for Nanos {
    fn from(d: std::time::Duration) -> Self {
        Nanos(d.as_nanos().min(u64::MAX as u128) as u64)
    }
}

impl From<Nanos> for std::time::Duration {
    fn from(n: Nanos) -> Self {
        std::time::Duration::from_nanos(n.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_scales() {
        assert_eq!(Nanos(500).to_string(), "500ns");
        assert_eq!(Nanos(1_500).to_string(), "1.500us");
        assert_eq!(Nanos(2_500_000).to_string(), "2.500ms");
        assert_eq!(Nanos(90_500_000_000).to_string(), "1m30.500s");
        assert_eq!(Nanos(15_123_000_000_000).to_string(), "4h12m03.000s");
    }

    #[test]
    fn arithmetic_saturates_instead_of_wrapping() {
        assert_eq!(Nanos::ZERO - Nanos(5), Nanos::ZERO);
        assert_eq!(Nanos::MAX + Nanos(5), Nanos::MAX);
    }

    #[test]
    fn offset_handles_negative_skew() {
        assert_eq!(Nanos(1000).offset(-400), Nanos(600));
        assert_eq!(Nanos(1000).offset(400), Nanos(1400));
        assert_eq!(Nanos(100).offset(-400), Nanos::ZERO);
    }
}
