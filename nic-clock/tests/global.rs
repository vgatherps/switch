//! `run` on a thread with CLOCK_MONOTONIC (`Instant`) standing in for the PHC, `since` on this one.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use nic_clock::{counter_invariant, since};

const TRUTH: u64 = 50_000;

#[test]
fn run_publishes_and_stop_retracts() {
    let start = Instant::now();
    let phc = || 1_700_000_000_000_000_000 + u64::try_from(start.elapsed().as_nanos()).unwrap();
    let stop = AtomicBool::new(false);
    let (early, ready) = std::thread::scope(|scope| {
        scope.spawn(|| nic_clock::run(|| Some(phc()), &stop));
        let early = since(phc());
        std::thread::sleep(Duration::from_millis(1_300));
        let ready = since(phc() - TRUTH);
        stop.store(true, Ordering::Relaxed);
        (early, ready)
    });
    assert_eq!(early, None, "ready before 1 s of reads");
    assert_eq!(since(phc()), None, "ready after stop");
    if !counter_invariant() {
        assert_eq!(ready, None);
        return;
    }
    let latency = ready.expect("ready after 1.3 s");
    // `since` read the counter after `phc()` did: the latency is TRUTH plus that gap.
    assert!(!latency.impossible(), "{latency:?}");
    assert!(i128::from(latency.ns) + i128::from(latency.bound_ns) >= i128::from(TRUTH), "{latency:?}");
    assert!(latency.ns < 1_000_000, "{latency:?}");
}
