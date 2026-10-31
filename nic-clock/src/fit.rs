//! The counter → PHC line from two reads, with a bound that holds if both clocks run at a
//! constant rate between and after them.
//!
//! A read says the PHC showed `phc` (±1 ns of truncation) at some moment the counter read in
//! `before..=after`: counter time `before ..= after + 1`, the counter being whole ticks. With
//! latest read A and an older read B, the rate R (ns per tick) lies in
//! `[(dp-2) / (A.after+1-B.before), (dp+2) / (A.before-B.after-1)]`, `dp = A.phc - B.phc`.
//! Store it as midpoint `r` and half-width `u`. A's counter time lies within `m ± h`. A moment
//! the counter reads `c` lies in `c ..= c + 1`, so with `d = c - m`:
//! `|PHC - (A.phc + r·d)| <= r·(h+1) + u·(|d| + h + 1) + 1`, plus 1 for flooring `r·d`.

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
        let a_end = a.after.checked_add(1)?;
        let b_end = b.after.checked_add(1)?;
        if b_end >= a.before || a.phc <= b.phc.checked_add(2)? {
            return None;
        }
        let dp = u128::from(a.phc - b.phc);
        let span_max = u128::from(a_end - b.before);
        let span_min = u128::from(a.before - b_end);
        let r_lo = ((dp - 2) << Q) / span_max;
        let r_hi = ((dp + 2) << Q).div_ceil(span_min);
        let r_lo = u64::try_from(r_lo).ok()?;
        let r_hi = u64::try_from(r_hi).ok()?;
        let r = r_lo + (r_hi - r_lo) / 2;
        Some(Fit {
            m: a.before + (a_end - a.before) / 2,
            h: (a_end - a.before).div_ceil(2),
            p: a.phc,
            r,
            u: r_hi - r,
        })
    }

    /// PHC ns at a moment the counter read `c`, and the bound on its error; `None` if they
    /// do not fit the arithmetic.
    pub(crate) fn at(&self, c: u64) -> Option<(i128, u128)> {
        let d = i128::from(c) - i128::from(self.m);
        let est = i128::from(self.p) + (i128::from(self.r).checked_mul(d)? >> Q);
        let h = u128::from(self.h) + 1;
        let spread = (u128::from(self.r) * h)
            .checked_add(u128::from(self.u).checked_mul(d.unsigned_abs() + h)?)?;
        Some((est, spread.div_ceil(1 << Q) + 2))
    }

    /// Whether `read` is impossible under this fit: its PHC value lies outside what the fit
    /// allows anywhere in its counter window (±1 ns of truncation), or the fit cannot say.
    pub(crate) fn contradicts(&self, read: Read) -> bool {
        let (Some((lo, lo_bound)), Some((hi, hi_bound))) = (self.at(read.before), self.at(read.after))
        else {
            return true;
        };
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
        assert_eq!(fit.h, 1);
        // (dp ± 2) over 1e9 ± 1 ticks: u is about 3e-9 ns/tick, 13 in Q32.
        assert!(fit.u <= 13, "{fit:?}");
        let (est, bound) = fit.at(2_100_000_000).unwrap();
        assert!((est - 2_100_005_000).unsigned_abs() <= bound);
        assert!(bound <= 6, "{bound}");
    }

    #[test]
    fn rejects_overlapping_or_backward_reads() {
        assert!(Fit::new(read(10, 1_000, 20), read(15, 500, 25)).is_none());
        assert!(Fit::new(read(30, 500, 40), read(10, 500, 20)).is_none());
        assert!(Fit::new(read(30, 502, 40), read(10, 500, 20)).is_none());
        assert!(Fit::new(read(30, 503, 40), read(10, 500, 20)).is_some());
        // B's counter time reaches 21: a read starting there may overlap it.
        assert!(Fit::new(read(21, 503, 40), read(10, 500, 20)).is_none());
    }

    #[test]
    fn negative_and_large_distances() {
        let fit = Fit::new(read(1 << 40, 3_000_000_000, (1 << 40) + 4000), read(1 << 39, 1_000_000_000, (1 << 39) + 4000))
            .unwrap();
        let (before, b1) = fit.at(fit.m - 1_000_000).unwrap();
        let (after, b2) = fit.at(fit.m + 1_000_000).unwrap();
        assert!(before < i128::from(fit.p) && after > i128::from(fit.p));
        assert_eq!(b1, b2);
        let (_, far) = fit.at(u64::MAX).unwrap();
        assert!(far > b2);
        let (_, zero) = fit.at(0).unwrap();
        assert!(zero > b2);
    }

    /// 3e9 ns per tick, 2^64 ticks away: the estimate does not fit i128.
    #[test]
    fn unrepresentable_is_none() {
        let fit = Fit { m: u64::MAX - 1, h: 0, p: 0, r: 3_000_000_000 << Q, u: 0 };
        assert_eq!(fit.at(0), None);
        assert!(fit.contradicts(read(0, 0, 0)));
        let fit = Fit { m: u64::MAX - 1, h: u64::MAX, p: 0, r: 1 << Q, u: u64::MAX };
        assert_eq!(fit.at(0), None);
    }

    /// 100 MHz counter; both PHC samples land 9 ns into the tick after `after`. At counter
    /// 100_000_101 the true PHC is 1_000_001_010 (codex's case: the estimate is 509 ns off).
    #[test]
    fn sample_in_the_tick_after_the_window() {
        let fit = Fit::new(read(100_000_000, 1_000_001_009, 100_000_100), read(0, 1009, 100)).unwrap();
        let (est, bound) = fit.at(100_000_101).unwrap();
        let error = (est - 1_000_001_010).unsigned_abs();
        assert!(error <= bound, "error {error} bound {bound}");
    }

    #[test]
    fn contradiction_is_outside_the_bound() {
        let fit = Fit::new(read(2_000_000_000, 2_000_000_000, 2_000_001_000), read(1_000_000_000, 1_000_000_000, 1_000_001_000))
            .unwrap();
        let at = 2_100_000_000;
        let (est, bound) = fit.at(at).unwrap();
        let inside = u64::try_from(est).unwrap() + u64::try_from(bound).unwrap();
        assert!(!fit.contradicts(read(at, inside, at)));
        assert!(fit.contradicts(read(at, inside + 2, at)));
        assert!(fit.contradicts(read(at, inside - 2 * u64::try_from(bound).unwrap() - 2, at)));
    }
}
