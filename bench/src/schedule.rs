//! The open-loop send schedule: N senders, each sending every `interval_ns`, with their phases
//! spread evenly. Message k (counting all senders) belongs to sender `k mod N`, as its message
//! `k div N`, and is due at `origin + floor(k * interval_ns / N)`. Integer arithmetic: no drift.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Schedule {
    pub senders: u32,
    /// Each sender's interval between messages.
    pub interval_ns: u64,
    /// When message 0 is due, in the clock's ns.
    pub origin_ns: u64,
}

impl Schedule {
    /// The schedule offering `rate_pps` messages per second over `senders` senders; the
    /// per-sender interval is rounded to whole ns.
    pub fn for_rate(senders: u32, rate_pps: u64, origin_ns: u64) -> Schedule {
        assert!(senders > 0 && rate_pps > 0);
        let interval_ns = (u128::from(senders) * 1_000_000_000 / u128::from(rate_pps)) as u64;
        assert!(interval_ns > 0, "rate above 1 message per ns per sender");
        Schedule {
            senders,
            interval_ns,
            origin_ns,
        }
    }

    /// The offered rate this schedule actually has (after rounding the interval).
    pub fn offered_pps(&self) -> f64 {
        f64::from(self.senders) * 1e9 / self.interval_ns as f64
    }

    /// When message `k` is due.
    pub fn due_at(&self, k: u64) -> u64 {
        self.origin_ns
            + (u128::from(k) * u128::from(self.interval_ns) / u128::from(self.senders)) as u64
    }

    /// How many messages are due by `now_ns`: every k with `due_at(k) <= now_ns`.
    pub fn due_by(&self, now_ns: u64) -> u64 {
        if now_ns < self.origin_ns {
            return 0;
        }
        let elapsed = u128::from(now_ns - self.origin_ns);
        // largest k with floor(k * I / N) <= elapsed is floor(((elapsed + 1) * N - 1) / I)
        (((elapsed + 1) * u128::from(self.senders) - 1) / u128::from(self.interval_ns)) as u64 + 1
    }

    /// Sender and its sequence number of message `k`.
    pub fn split(&self, k: u64) -> (u32, u64) {
        let n = u64::from(self.senders);
        ((k % n) as u32, k / n)
    }

    /// Messages due before `end_ns` (exclusive): the count a window `[origin, end)` offers.
    pub fn count_before(&self, end_ns: u64) -> u64 {
        if end_ns == 0 {
            0
        } else {
            self.due_by(end_ns - 1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `due_by` agrees with a brute-force count over `due_at` at every ns around many
    /// boundaries, for intervals that do and do not divide by the sender count.
    #[test]
    fn due_by_matches_due_at_everywhere() {
        for (senders, interval_ns) in [(1, 1000), (3, 1000), (7, 333), (16, 10_000), (5, 1)] {
            let s = Schedule {
                senders,
                interval_ns,
                origin_ns: 500,
            };
            let mut k = 0;
            for now in 0..5 * interval_ns + 600 {
                while s.due_at(k) <= now {
                    k += 1;
                }
                let expected = if now < 500 { 0 } else { k };
                assert_eq!(
                    s.due_by(now),
                    expected,
                    "N={senders} I={interval_ns} now={now}"
                );
            }
        }
    }

    /// Each sender's own messages are exactly `interval_ns` apart (within the 1 ns floor of the
    /// global spread), and senders are interleaved round-robin.
    #[test]
    fn each_sender_keeps_its_interval_and_senders_interleave() {
        let s = Schedule {
            senders: 4,
            interval_ns: 10_000,
            origin_ns: 0,
        };
        for k in 0..400 {
            let (sender, seq) = s.split(k);
            assert_eq!(u64::from(sender), k % 4);
            assert_eq!(seq, k / 4);
            if k >= 4 {
                assert_eq!(s.due_at(k) - s.due_at(k - 4), 10_000);
            }
            if k >= 1 {
                assert_eq!(s.due_at(k) - s.due_at(k - 1), 2500);
            }
        }
    }

    /// Over a window the number of messages offered is the rate times the window.
    #[test]
    fn offered_count_matches_rate() {
        for (senders, rate) in [(1, 10_000), (16, 1_000_000), (3, 70_000)] {
            let s = Schedule::for_rate(senders, rate, 1_000);
            let window = 1_000_000_000;
            let count = s.count_before(s.origin_ns + window);
            let expected = s.offered_pps();
            assert!(
                (count as f64 - expected).abs() <= 1.0,
                "N={senders} rate={rate}: {count} vs {expected}"
            );
        }
    }
}
