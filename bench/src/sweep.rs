//! The rate sweep: start at `rate_min`, double while steps are good, then bisect between the last
//! good and the first bad rate. A step is bad when its p99 exceeds `p99_factor` times the first
//! step's p99, or more than `max_drop` of its measured messages were lost. The knee is the
//! highest good rate found.

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Limits {
    pub rate_min: u64,
    pub rate_max: u64,
    pub bisect_steps: u32,
    pub p99_factor: f64,
    pub max_drop: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Outcome {
    pub p99_ns: u64,
    pub drop_fraction: f64,
}

#[derive(Debug)]
pub struct Sweep {
    limits: Limits,
    reference_p99: Option<u64>,
    good: Option<u64>,
    bad: Option<u64>,
    bisected: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Next {
    Run(u64),
    /// The knee: the highest good rate, `None` if even `rate_min` was bad.
    Done {
        knee: Option<u64>,
    },
}

impl Sweep {
    pub fn new(limits: Limits) -> Sweep {
        assert!(0 < limits.rate_min && limits.rate_min <= limits.rate_max);
        Sweep {
            limits,
            reference_p99: None,
            good: None,
            bad: None,
            bisected: 0,
        }
    }

    pub fn first(&self) -> u64 {
        self.limits.rate_min
    }

    /// Whether `outcome` is good, against the first step's p99 (the first step sets it).
    pub fn judge(&mut self, outcome: Outcome) -> bool {
        let reference = *self.reference_p99.get_or_insert(outcome.p99_ns);
        outcome.drop_fraction <= self.limits.max_drop
            && outcome.p99_ns as f64 <= self.limits.p99_factor * reference as f64
    }

    /// Takes the result of running `rate`; returns whether that step was good and what to do
    /// next.
    pub fn next(&mut self, rate: u64, outcome: Outcome) -> (bool, Next) {
        let good = self.judge(outcome);
        (good, self.advance(rate, good))
    }

    fn advance(&mut self, rate: u64, good: bool) -> Next {
        if good {
            self.good = Some(self.good.map_or(rate, |g| g.max(rate)));
        } else {
            self.bad = Some(self.bad.map_or(rate, |b| b.min(rate)));
        }
        match (self.good, self.bad) {
            (None, Some(_)) => Next::Done { knee: None },
            (Some(g), None) if g >= self.limits.rate_max => Next::Done { knee: Some(g) },
            (Some(g), None) => Next::Run((g * 2).min(self.limits.rate_max)),
            (Some(g), Some(b)) => {
                let mid = g + (b - g) / 2;
                if self.bisected >= self.limits.bisect_steps || mid == g {
                    return Next::Done { knee: Some(g) };
                }
                self.bisected += 1;
                Next::Run(mid)
            }
            (None, None) => unreachable!("a step was just judged"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMITS: Limits = Limits {
        rate_min: 1000,
        rate_max: 1_000_000,
        bisect_steps: 4,
        p99_factor: 3.0,
        max_drop: 0.001,
    };

    /// Runs a sweep against a system whose knee is `capacity`: p99 is flat up to it and 10x
    /// beyond. Returns the rates run and the reported knee.
    fn simulate(limits: Limits, capacity: u64) -> (Vec<u64>, Option<u64>) {
        let mut sweep = Sweep::new(limits);
        let mut rate = sweep.first();
        let mut rates = vec![];
        loop {
            rates.push(rate);
            let p99_ns = if rate <= capacity { 10_000 } else { 100_000 };
            match sweep
                .next(
                    rate,
                    Outcome {
                        p99_ns,
                        drop_fraction: 0.0,
                    },
                )
                .1
            {
                Next::Run(r) => rate = r,
                Next::Done { knee } => return (rates, knee),
            }
        }
    }

    #[test]
    fn doubles_then_bisects_to_the_knee() {
        let (rates, knee) = simulate(LIMITS, 50_000);
        assert_eq!(
            rates,
            [
                1000, 2000, 4000, 8000, 16000, 32000, 64000, 48000, 56000, 52000, 50000
            ]
        );
        assert_eq!(knee, Some(50_000));
    }

    #[test]
    fn stops_at_rate_max_when_never_bad() {
        let (rates, knee) = simulate(LIMITS, u64::MAX);
        assert_eq!(rates.last(), Some(&1_000_000));
        assert_eq!(rates[rates.len() - 2], 512_000);
        assert_eq!(knee, Some(1_000_000));
    }

    #[test]
    fn bad_first_step_has_no_knee() {
        let mut sweep = Sweep::new(LIMITS);
        assert_eq!(
            sweep.next(
                1000,
                Outcome {
                    p99_ns: 5,
                    drop_fraction: 0.5
                }
            ),
            (false, Next::Done { knee: None })
        );
    }

    /// Drops above the limit make a step bad even at a flat p99; the reference p99 is the first
    /// step's, so a later step at exactly 3x is still good and above it is bad.
    #[test]
    fn drop_and_p99_thresholds() {
        let mut s = Sweep::new(LIMITS);
        assert!(s.judge(Outcome {
            p99_ns: 1000,
            drop_fraction: 0.0
        }));
        assert!(s.judge(Outcome {
            p99_ns: 3000,
            drop_fraction: 0.001
        }));
        assert!(!s.judge(Outcome {
            p99_ns: 3001,
            drop_fraction: 0.0
        }));
        assert!(!s.judge(Outcome {
            p99_ns: 1000,
            drop_fraction: 0.0011
        }));
    }

    #[test]
    fn bisection_is_bounded() {
        let limits = Limits {
            bisect_steps: 1,
            ..LIMITS
        };
        let (rates, knee) = simulate(limits, 50_000);
        assert_eq!(&rates[6..], [64000, 48000]);
        assert_eq!(knee, Some(48_000));
    }
}
