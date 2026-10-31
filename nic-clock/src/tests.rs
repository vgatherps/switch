//! Fake clocks: the PHC is `phc0 + floor(t·k / 2^32)` ns at counter time `t` (ticks, with a
//! sub-tick phase: the counter reads `floor(t)`), and each read samples it at a counter time in
//! its `before ..= after + 1` window. A local `Mapping` stands in for the global one.

use std::sync::Arc;

use proptest::prelude::*;

use super::*;

const MS: u64 = 1_000_000;

#[derive(Clone, Copy, Debug)]
struct Line {
    phc0: u64,
    /// ns per tick, Q32.
    k: u64,
}

impl Line {
    fn phc(&self, c: u64) -> u64 {
        self.phc_sub(c, 0)
    }

    /// PHC ns `sub`/1024 of a tick after counter value `c`.
    fn phc_sub(&self, c: u64, sub: u64) -> u64 {
        let t = u128::from(c) * 1024 + u128::from(sub);
        self.phc0 + u64::try_from((t * u128::from(self.k)) >> 42).unwrap()
    }

    /// Ticks in `ns` ns, rounded up.
    fn ticks(&self, ns: u64) -> u64 {
        u64::try_from((u128::from(ns) << 32).div_ceil(u128::from(self.k))).unwrap()
    }

    /// A read sampling the PHC at counter `at`, its window `lead` ticks before and `lag` after.
    fn read(&self, at: u64, lead: u64, lag: u64) -> Read {
        Read { before: at - lead, phc: self.phc(at), after: at + lag }
    }
}

/// 3 GHz counter (1/3 ns per tick), PHC near 1.7e18 ns.
const LINE: Line = Line { phc0: 1_700_000_000_000_000_000, k: (1 << 32) / 3 };

#[derive(Default)]
struct Fixture {
    calibration: Calibration,
    mapping: Arc<Mapping>,
}

impl Default for Mapping {
    fn default() -> Self {
        Mapping::new()
    }
}

impl Fixture {
    fn add(&mut self, read: Read) {
        self.mapping.publish(self.calibration.add(read));
    }

    /// `count` exact reads (zero-width windows) 100 ms of PHC apart from counter `start`.
    /// Returns the counter value of the last.
    fn exact_reads(&mut self, line: Line, start: u64, count: u64) -> u64 {
        let mut at = start;
        for i in 0..count {
            at = start + i * line.ticks(100 * MS);
            self.add(line.read(at, 0, 0));
        }
        at
    }

    fn latency(&self, stamp: u64, now: u64) -> Option<Latency> {
        self.mapping.latency(stamp, now)
    }
}

fn check(latency: Option<Latency>, truth: u64) -> Latency {
    let latency = latency.expect("a latency");
    let error = (i128::from(latency.ns) - i128::from(truth)).unsigned_abs();
    assert!(error <= u128::from(latency.bound_ns), "{latency:?} vs truth {truth}");
    assert!(!latency.impossible());
    latency
}

#[test]
fn not_ready_until_one_second_of_reads() {
    let mut f = Fixture::default();
    assert_eq!(f.latency(LINE.phc(1 << 40), 1 << 40), None);
    let last = f.exact_reads(LINE, 1 << 40, 10);
    assert_eq!(f.latency(LINE.phc(last), last), None);
    let last = f.exact_reads(LINE, last + LINE.ticks(100 * MS), 1);
    let now = last + LINE.ticks(MS);
    let latency = check(f.latency(LINE.phc(now) - 20_000, now), 20_000);
    assert!(latency.bound_ns < 10, "{latency:?}");
}

#[test]
fn not_ready_300_ms_after_the_latest_read() {
    let mut f = Fixture::default();
    let last = f.exact_reads(LINE, 1 << 40, 11);
    let fresh = last + LINE.ticks(299 * MS);
    check(f.latency(LINE.phc(fresh) - 5_000, fresh), 5_000);
    let stale = last + LINE.ticks(301 * MS);
    assert_eq!(f.latency(LINE.phc(stale) - 5_000, stale), None);
}

#[test]
fn impossible_past_minus_bound() {
    let mut f = Fixture::default();
    let last = f.exact_reads(LINE, 1 << 40, 11);
    let now = last + LINE.ticks(MS);
    let latency = check(f.latency(LINE.phc(now), now), 0);
    // A stamp putting the latency at exactly -bound is possible; 1 ns later, impossible.
    let edge = i128::from(LINE.phc(now)) + i128::from(latency.ns) + i128::from(latency.bound_ns);
    let edge = u64::try_from(edge).unwrap();
    let at_edge = f.latency(edge, now).unwrap();
    assert_eq!(at_edge.ns, -i64::try_from(latency.bound_ns).unwrap());
    assert!(!at_edge.impossible());
    assert!(f.latency(edge + 1, now).unwrap().impossible());
}

#[test]
fn unrepresentable_latency_is_none() {
    // PHC near 1.7e19 ns: a stamp of 1 gives a latency beyond i64.
    let line = Line { phc0: 17_000_000_000_000_000_000, k: LINE.k };
    let mut f = Fixture::default();
    let now = f.exact_reads(line, 1 << 40, 11);
    check(f.latency(line.phc(now) - 5, now), 5);
    assert_eq!(f.latency(1, now), None);
}

#[test]
fn sparse_reads_still_fit() {
    // Every other read failed: 200 ms apart.
    let mut f = Fixture::default();
    let start = 1 << 40;
    let step = LINE.ticks(200 * MS);
    for i in 0..6 {
        f.add(LINE.read(start + i * step, 0, 0));
    }
    let now = start + 5 * step + LINE.ticks(MS);
    check(f.latency(LINE.phc(now) - 7, now), 7);
}

#[test]
fn a_contradicting_read_restarts_the_fit() {
    let mut f = Fixture::default();
    let last = f.exact_reads(LINE, 1 << 40, 11);
    // The PHC steps forward 1 ms.
    let stepped = Line { phc0: LINE.phc0 + MS, k: LINE.k };
    let next = last + LINE.ticks(100 * MS);
    f.add(stepped.read(next, 0, 0));
    assert_eq!(f.latency(stepped.phc(next), next), None);
    assert!(f.calibration.jumped);
    let last = f.exact_reads(stepped, next + LINE.ticks(100 * MS), 9);
    assert_eq!(f.latency(stepped.phc(last), last), None);
    let last = f.exact_reads(stepped, last + LINE.ticks(100 * MS), 1);
    check(f.latency(stepped.phc(last) - 3_000, last), 3_000);
}

#[test]
fn a_backward_read_restarts_the_fit() {
    // Before the first fit, a backward read restarts warm-up without marking a jump.
    let mut f = Fixture::default();
    let last = f.exact_reads(LINE, 1 << 40, 5);
    f.add(LINE.read(last - 1, 0, 0));
    assert!(!f.calibration.jumped);
    let mut f = Fixture::default();
    let last = f.exact_reads(LINE, 1 << 40, 11);
    f.add(LINE.read(last - 1, 0, 0));
    assert_eq!(f.latency(LINE.phc(last), last), None);
    assert!(f.calibration.jumped);
    let mut f = Fixture::default();
    let last = f.exact_reads(LINE, 1 << 40, 11);
    f.add(Read { before: last + 10, phc: LINE.phc(last + 5), after: last + 1 });
    assert_eq!(f.latency(LINE.phc(last), last), None);
    assert!(f.calibration.jumped);
}

/// A writer publishes fits whose fields all equal one value; a reader on another thread must
/// never see two fits mixed.
#[test]
fn seqlock_never_tears() {
    let fit = |v| Fit { m: v, h: v, p: v, r: v, u: v };
    let mapping = Arc::new(Mapping::new());
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let mapping = Arc::clone(&mapping);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut v = 1;
            while !stop.load(Ordering::Relaxed) {
                mapping.publish(Some(fit(v)));
                v += 1;
                // Back-to-back publishes would starve the reader; the real writer waits 100 ms.
                for _ in 0..100 {
                    std::hint::spin_loop();
                }
            }
            v
        })
    };
    let mut seen = 0;
    for _ in 0..100_000 {
        let Some([m, h, p, r, u, stale_at]) = mapping.load() else {
            continue;
        };
        if stale_at == 0 {
            continue;
        }
        seen += 1;
        assert_eq!([h, p, r, u], [m; 4]);
        assert_eq!(stale_at, m.saturating_add(fit(m).ticks(STALE_NS)));
    }
    stop.store(true, Ordering::Relaxed);
    assert!(writer.join().unwrap() > 1);
    assert!(seen > 0);
}

/// While the writer is mid-publish (odd `seq`) the reader gets no latency instead of waiting.
#[test]
fn mid_publish_is_not_ready() {
    let mut f = Fixture::default();
    let now = f.exact_reads(LINE, 1 << 40, 11);
    check(f.latency(LINE.phc(now) - 5, now), 5);
    f.mapping.seq.fetch_add(1, Ordering::Relaxed);
    assert_eq!(f.latency(LINE.phc(now) - 5, now), None);
}

/// `mark_jumped` sets the flag `jumped` reports, as a contradicting read does.
#[test]
fn mark_jumped_marks_the_run() {
    mark_jumped();
    assert!(jumped());
}

proptest! {
    /// Whatever the rates, windows, sample placement (sub-tick phase included) and
    /// extrapolation distance, the truth lies within the bound.
    #[test]
    fn truth_within_bound(
        // 0.25 to 10 ns per tick: 100 MHz to 4 GHz counters.
        k in (1u64 << 30)..(10u64 << 32),
        phc0 in 0u64..(1 << 62),
        start in (1u64 << 20)..(1 << 50),
        // Zero-width windows and zero distance half the time: there sub-tick phases are most
        // of the error.
        windows in prop::collection::vec(
            (
                prop_oneof![Just(0u64), 0u64..10_000],
                prop_oneof![Just(0u64), 0u64..10_000],
                0u64..1024,
                0u64..20 * MS,
            ),
            13..40,
        ),
        distance_ns in prop_oneof![Just(0u64), 0u64..250 * MS],
        now_sub in 0u64..1024,
        truth in 0u64..10 * MS,
    ) {
        let line = Line { phc0, k };
        let mut f = Fixture::default();
        let mut at = start;
        for &(lead_ns, lag_ns, sub, jitter_ns) in &windows {
            at += line.ticks(90 * MS + jitter_ns);
            let (lead, lag) = (line.ticks(lead_ns), line.ticks(lag_ns));
            f.add(Read { before: at - lead, phc: line.phc_sub(at, sub), after: at + lag });
        }
        let now = at + line.ticks(distance_ns);
        let phc = line.phc_sub(now, now_sub);
        prop_assume!(phc > truth);
        check(f.latency(phc - truth, now), truth);
    }
}
