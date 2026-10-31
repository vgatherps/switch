//! The rate sweep: start at `rate_min`, double while steps are good, then bisect between the last
//! good and the first bad rate. A step is bad when its p99 exceeds `p99_factor` times the first
//! step's p99 (no echo at all counts as an infinite p99), or more than `max_drop` of its measured
//! messages were lost. Once the transition is bracketed, the last good and first bad rates are
//! each run `REPEATS` more times, then `PAST_KNEE` sustained points beyond the first bad rate, so
//! the curve shows the blade of the hockey stick. The knee is the highest rate run below the
//! lowest rate any of whose runs was bad. A sweep that never sees a bad step reports `rate_max`
//! as a lower bound.

use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Limits {
    pub rate_min: u64,
    pub rate_max: u64,
    pub bisect_steps: u32,
    pub p99_factor: f64,
    pub max_drop: f64,
}

/// More runs of the last good and the first bad rate once bracketed.
pub const REPEATS: usize = 2;
/// Sustained points past the first bad rate, as multiples of it (capped at `rate_max`).
pub const PAST_KNEE: [u64; 2] = [2, 4];

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Outcome {
    /// `None`: no message came back.
    pub p99_ns: Option<u64>,
    pub drop_fraction: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Knee {
    pub pps: u64,
    /// The sweep reached `rate_max` without a bad step: the knee is at least this.
    pub lower_bound: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Next {
    Run(u64),
    /// `None` if even `rate_min` was bad.
    Done(Option<Knee>),
}

#[derive(Debug)]
pub struct Sweep {
    limits: Limits,
    reference_p99: Option<u64>,
    good: Option<u64>,
    bad: Option<u64>,
    bisected: u32,
    /// Repeats and past-knee points still to run, once bracketed.
    extra: Option<VecDeque<u64>>,
    /// Every judged run: (rate, good).
    runs: Vec<(u64, bool)>,
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
            extra: None,
            runs: Vec::new(),
        }
    }

    pub fn first(&self) -> u64 {
        self.limits.rate_min
    }

    /// Whether `outcome` is good, against the first step's p99 (the first step sets it).
    pub fn judge(&mut self, outcome: Outcome) -> bool {
        let Some(p99) = outcome.p99_ns else {
            return false;
        };
        let reference = *self.reference_p99.get_or_insert(p99);
        outcome.drop_fraction <= self.limits.max_drop
            && p99 as f64 <= self.limits.p99_factor * reference as f64
    }

    /// Takes the result of running `rate`; returns whether that step was good and what to do
    /// next.
    pub fn next(&mut self, rate: u64, outcome: Outcome) -> (bool, Next) {
        let good = self.judge(outcome);
        self.runs.push((rate, good));
        (good, self.advance(rate, good))
    }

    /// The knee from the runs so far: the highest rate run below the lowest rate with a bad run;
    /// `None` until a bad run exists or if none ran below it.
    pub fn knee(&self) -> Option<u64> {
        let lowest_bad = self.runs.iter().filter(|r| !r.1).map(|r| r.0).min()?;
        self.runs
            .iter()
            .map(|r| r.0)
            .filter(|&r| r < lowest_bad)
            .max()
    }

    fn advance(&mut self, rate: u64, good: bool) -> Next {
        if let Some(extra) = &mut self.extra {
            return match extra.pop_front() {
                Some(r) => Next::Run(r),
                None => Next::Done(self.knee().map(|pps| Knee {
                    pps,
                    lower_bound: false,
                })),
            };
        }
        if good {
            self.good = Some(self.good.map_or(rate, |g| g.max(rate)));
        } else {
            self.bad = Some(self.bad.map_or(rate, |b| b.min(rate)));
        }
        match (self.good, self.bad) {
            (None, Some(_)) => Next::Done(None),
            (Some(g), None) if g >= self.limits.rate_max => Next::Done(Some(Knee {
                pps: g,
                lower_bound: true,
            })),
            (Some(g), None) => Next::Run((g * 2).min(self.limits.rate_max)),
            (Some(g), Some(b)) => {
                let mid = g + (b - g) / 2;
                if self.bisected < self.limits.bisect_steps && mid != g {
                    self.bisected += 1;
                    return Next::Run(mid);
                }
                let mut extra: VecDeque<u64> = [g, b].repeat(REPEATS).into();
                let mut last = b;
                for m in PAST_KNEE {
                    let r = (b * m).min(self.limits.rate_max);
                    if r > last {
                        extra.push_back(r);
                        last = r;
                    }
                }
                let first = extra.pop_front().expect("repeats");
                self.extra = Some(extra);
                Next::Run(first)
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

    fn flat(p99_ns: u64) -> Outcome {
        Outcome {
            p99_ns: Some(p99_ns),
            drop_fraction: 0.0,
        }
    }

    /// Runs a sweep against a system whose p99 is `p99(rate)`. Returns the rates run and the
    /// result.
    fn simulate(
        limits: Limits,
        mut p99: impl FnMut(u64) -> Option<u64>,
    ) -> (Vec<u64>, Option<Knee>) {
        let mut sweep = Sweep::new(limits);
        let mut rate = sweep.first();
        let mut rates = vec![];
        loop {
            rates.push(rate);
            let outcome = Outcome {
                p99_ns: p99(rate),
                drop_fraction: 0.0,
            };
            match sweep.next(rate, outcome).1 {
                Next::Run(r) => rate = r,
                Next::Done(knee) => return (rates, knee),
            }
        }
    }

    fn capacity(c: u64) -> impl Fn(u64) -> Option<u64> {
        move |rate| Some(if rate <= c { 10_000 } else { 100_000 })
    }

    /// Doubles, bisects, repeats the bracket twice, then runs two sustained points past it.
    #[test]
    fn doubles_bisects_repeats_then_runs_past_the_knee() {
        let (rates, knee) = simulate(LIMITS, capacity(50_000));
        assert_eq!(
            rates,
            [
                1000, 2000, 4000, 8000, 16000, 32000, 64000, 48000, 56000, 52000,
                50000, // search
                50000, 52000, 50000, 52000, // repeats of the last good and first bad
                104_000, 208_000, // past the knee
            ]
        );
        assert_eq!(
            knee,
            Some(Knee {
                pps: 50_000,
                lower_bound: false
            })
        );
    }

    /// A repeat of the knee rate that turns bad moves the knee down to the highest rate run
    /// below it.
    #[test]
    fn a_bad_repeat_lowers_the_knee() {
        let mut runs_at_50k = 0;
        let (_, knee) = simulate(LIMITS, |rate| {
            if rate == 50_000 {
                runs_at_50k += 1;
            }
            Some(if rate < 50_000 || (rate == 50_000 && runs_at_50k < 3) {
                10_000
            } else {
                100_000
            })
        });
        assert_eq!(knee.map(|k| k.pps), Some(48_000));
    }

    /// Never bad: the sweep stops at rate_max and says the knee is at least that.
    #[test]
    fn rate_max_is_a_lower_bound() {
        let (rates, knee) = simulate(LIMITS, capacity(u64::MAX));
        assert_eq!(rates.last(), Some(&1_000_000));
        assert_eq!(rates[rates.len() - 2], 512_000);
        assert_eq!(
            knee,
            Some(Knee {
                pps: 1_000_000,
                lower_bound: true
            })
        );
    }

    /// Past-knee points are capped at rate_max and not repeated.
    #[test]
    fn past_knee_points_stop_at_rate_max() {
        let limits = Limits {
            rate_max: 100_000,
            ..LIMITS
        };
        let (rates, _) = simulate(limits, capacity(50_000));
        assert_eq!(&rates[rates.len() - 1..], [100_000]);
        assert_eq!(rates.iter().filter(|&&r| r == 100_000).count(), 1);
    }

    #[test]
    fn bad_first_step_has_no_knee() {
        let mut sweep = Sweep::new(LIMITS);
        let outcome = Outcome {
            p99_ns: Some(5),
            drop_fraction: 0.5,
        };
        assert_eq!(sweep.next(1000, outcome), (false, Next::Done(None)));
    }

    /// A step where nothing came back (100% loss) is bad, whatever the reference.
    #[test]
    fn no_echo_at_all_is_bad() {
        let (rates, knee) = simulate(LIMITS, |rate| (rate <= 4000).then_some(10_000));
        assert_eq!(&rates[..4], [1000, 2000, 4000, 8000]);
        assert_eq!(knee.map(|k| k.pps), Some(4000));
    }

    /// Drops above the limit make a step bad even at a flat p99; the reference p99 is the first
    /// step's, so a later step at exactly 3x is still good and above it is bad.
    #[test]
    fn drop_and_p99_thresholds() {
        let mut s = Sweep::new(LIMITS);
        assert!(s.judge(flat(1000)));
        assert!(s.judge(Outcome {
            p99_ns: Some(3000),
            drop_fraction: 0.001
        }));
        assert!(!s.judge(flat(3001)));
        assert!(!s.judge(Outcome {
            p99_ns: Some(1000),
            drop_fraction: 0.0011
        }));
    }

    #[test]
    fn bisection_is_bounded() {
        let limits = Limits {
            bisect_steps: 1,
            ..LIMITS
        };
        let (rates, knee) = simulate(limits, capacity(50_000));
        assert_eq!(&rates[6..8], [64000, 48000]);
        assert_eq!(knee.map(|k| k.pps), Some(48_000));
    }
}
