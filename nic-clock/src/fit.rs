//! The counter → PHC line from two reads, with a bound that holds if both clocks run at a
//! constant rate between and after them.
//!
//! A read says the PHC showed `phc` (±1 ns of truncation) at some counter value in
//! `before..=after`. With latest read A and an older read B, the rate R (ns per tick) lies in
//! `[(dp-2) / (A.after-B.before), (dp+2) / (A.before-B.after)]`, `dp = A.phc - B.phc`. Store
//! it as midpoint `r` and half-width `u`. A's counter value lies within `m ± h`. Then at
//! counter `c`, with `d = c - m`:
//! `|PHC(c) - (A.phc + r·d)| <= r·h + u·(|d| + h) + 1`, plus 1 for flooring `r·d`.

use crate::Read;

const Q: u32 = 32;

/// Rates are Q32 fixed point: ns per tick times 2^32.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Fit {
    /// Midpoint and half-width (ticks) of the latest read's counter window.
    pub(crate) m: u64,
    pub(crate) h: u64,
    /// The latest read's PHC ns.
    pub(crate) p: u64,
    pub(crate) r: u64,
    pub(crate) u: u64,
}

impl Fit {
    /// `None` unless `b` lies wholly before `a` on both clocks with more than 2 ns of PHC
    /// between them, and the rate fits Q32 in a u64.
    pub(crate) fn new(a: Read, b: Read) -> Option<Fit> {
        if b.after >= a.before || a.phc <= b.phc.checked_add(2)? {
            return None;
        }
        let dp = u128::from(a.phc - b.phc);
        let span_max = u128::from(a.after - b.before);
        let span_min = u128::from(a.before - b.after);
        let r_lo = ((dp - 2) << Q) / span_max;
        let r_hi = ((dp + 2) << Q).div_ceil(span_min);
        let r_lo = u64::try_from(r_lo).ok()?;
        let r_hi = u64::try_from(r_hi).ok()?;
        let r = r_lo + (r_hi - r_lo) / 2;
        Some(Fit {
            m: a.before + (a.after - a.before) / 2,
            h: (a.after - a.before).div_ceil(2),
            p: a.phc,
            r,
            u: r_hi - r,
        })
    }

    /// PHC ns at counter `c` and the bound on its error.
    pub(crate) fn at(&self, c: u64) -> (i128, u128) {
        let d = i128::from(c) - i128::from(self.m);
        let est = i128::from(self.p) + ((i128::from(self.r) * d) >> Q);
        let spread = u128::from(self.r) * u128::from(self.h)
            + u128::from(self.u) * (d.unsigned_abs() + u128::from(self.h));
        (est, spread.div_ceil(1 << Q) + 2)
    }

    /// Whether `read` is impossible under this fit: its PHC value lies outside what the fit
    /// allows anywhere in its counter window (±1 ns of truncation).
    pub(crate) fn contradicts(&self, read: Read) -> bool {
        let (lo, lo_bound) = self.at(read.before);
        let (hi, hi_bound) = self.at(read.after);
        let slack = lo_bound.max(hi_bound) as i128 + 1;
        let phc = i128::from(read.phc);
        phc < lo - slack || phc > hi + slack
    }

    /// Ticks in `ns` PHC nanoseconds, rounded up.
    pub(crate) fn ticks(&self, ns: u64) -> u64 {
        let ticks = (u128::from(ns) << Q).div_ceil(u128::from(self.r.max(1)));
        u64::try_from(ticks).unwrap_or(u64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(before: u64, phc: u64, after: u64) -> Read {
        Read { before, phc, after }
    }

    #[test]
    fn exact_reads_give_exact_rate() {
        // 1 GHz counter, PHC = counter + 5000.
        let fit = Fit::new(read(2_000_000_000, 2_000_005_000, 2_000_000_000), read(1_000_000_000, 1_000_005_000, 1_000_000_000))
            .unwrap();
        assert_eq!(fit.h, 0);
        // dp ± 2 over 1e9 ticks: u is 2e-9 ns/tick in Q32, about 9.
        assert!(fit.u <= 10, "{fit:?}");
        let (est, bound) = fit.at(2_100_000_000);
        assert!((est - 2_100_005_000).unsigned_abs() <= bound);
        assert!(bound <= 4, "{bound}");
    }

    #[test]
    fn rejects_overlapping_or_backward_reads() {
        assert!(Fit::new(read(10, 1_000, 20), read(15, 500, 25)).is_none());
        assert!(Fit::new(read(30, 500, 40), read(10, 500, 20)).is_none());
        assert!(Fit::new(read(30, 502, 40), read(10, 500, 20)).is_none());
        assert!(Fit::new(read(30, 503, 40), read(10, 500, 20)).is_some());
    }

    #[test]
    fn negative_and_large_distances() {
        let fit = Fit::new(read(1 << 40, 3_000_000_000, (1 << 40) + 4000), read(1 << 39, 1_000_000_000, (1 << 39) + 4000))
            .unwrap();
        let (before, b1) = fit.at(fit.m - 1_000_000);
        let (after, b2) = fit.at(fit.m + 1_000_000);
        assert!(before < i128::from(fit.p) && after > i128::from(fit.p));
        assert_eq!(b1, b2);
        let (_, far) = fit.at(u64::MAX);
        assert!(far > b2);
        let (_, zero) = fit.at(0);
        assert!(zero > b2);
    }

    #[test]
    fn contradiction_is_outside_the_bound() {
        let fit = Fit::new(read(2_000_000_000, 2_000_000_000, 2_000_001_000), read(1_000_000_000, 1_000_000_000, 1_000_001_000))
            .unwrap();
        let at = 2_100_000_000;
        let (est, bound) = fit.at(at);
        let inside = u64::try_from(est).unwrap() + u64::try_from(bound).unwrap();
        assert!(!fit.contradicts(read(at, inside, at)));
        assert!(fit.contradicts(read(at, inside + 2, at)));
        assert!(fit.contradicts(read(at, inside - 2 * u64::try_from(bound).unwrap() - 2, at)));
    }
}
