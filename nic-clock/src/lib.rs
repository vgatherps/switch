//! Latency from a NIC's hardware RX stamp (ns on the NIC's PTP hardware clock, PHC) to now,
//! with a bound.
//!
//! One background thread runs [`run`]: every 100 ms it reads the PHC between two reads of the
//! CPU's invariant counter, fits the counter → PHC line, and stores it in a static global.
//! [`since`] on the receive path reads that global and the counter: no syscall, allocation or
//! lock. Until the fit is ready (1 s of consistent reads), and whenever it is not (no `run`, the
//! counter not invariant, the thread stopped, reads failing for 300 ms, a read contradicting
//! the fit), `since` returns `None`.

mod counter;
mod fit;
#[cfg(test)]
mod tests;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};
use std::time::Duration;

use counter::counter;
use fit::Fit;

/// Time between PHC reads (10 reads/s; ENA allows 125).
const PERIOD: Duration = Duration::from_millis(100);
/// The fit's base read is at least this many PHC ns before its latest read.
const BASELINE_NS: u64 = 1_000_000_000;
/// The fit is not ready this many PHC ns after its latest read.
const STALE_NS: u64 = 300_000_000;
/// Reads kept for choosing the base read: 3.2 s at `PERIOD`.
const KEPT: usize = 32;

/// The true latency lies in `ns - bound_ns ..= ns + bound_ns`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Latency {
    pub ns: i64,
    pub bound_ns: u64,
}

impl Latency {
    /// The latency is below `-bound_ns`: the stamp is after now. Never a sample: it means a
    /// wrong clock or a wrong stamp.
    pub fn impossible(&self) -> bool {
        i128::from(self.ns) < -i128::from(self.bound_ns)
    }
}

static MAPPING: Mapping = Mapping::new();
static JUMPED: AtomicBool = AtomicBool::new(false);

/// Whether a read ever contradicted a fit or went backwards (the NIC clock jumped). Samples
/// taken up to one read (100 ms) before that may be wrong by more than their bound, so the
/// run's latencies are not valid.
pub fn jumped() -> bool {
    JUMPED.load(Ordering::Relaxed)
}

/// Mark the run's latencies not valid, as a read contradicting the fit does: for a clock that
/// may have jumped without a read showing it (the NIC was reset).
pub fn mark_jumped() {
    JUMPED.store(true, Ordering::Relaxed);
}

/// Latency from `stamp` (PHC ns) to now; `None` if `stamp` is 0 or the fit is not ready.
#[inline]
pub fn since(stamp: u64) -> Option<Latency> {
    if stamp == 0 {
        return None;
    }
    MAPPING.latency(stamp, counter())
}

/// Reads the PHC with `read_phc` (`None`: this read failed) every [`PERIOD`] and updates the
/// global fit, until `stop` is set; then the fit is not ready. Run it on a thread off the
/// receive core. `Err`, publishing nothing, if the CPU counter is not invariant.
pub fn run(mut read_phc: impl FnMut() -> Option<u64>, stop: &AtomicBool) -> Result<(), &'static str> {
    counter::invariant()?;
    let mut calibration = Calibration::default();
    while !stop.load(Ordering::Relaxed) {
        let before = counter();
        // x86_64: later instructions (the PHC read) may start before rdtsc reads the counter.
        #[cfg(target_arch = "x86_64")]
        // SAFETY: lfence has no memory effects.
        unsafe {
            std::arch::x86_64::_mm_lfence()
        };
        let phc = read_phc();
        let after = counter();
        if let Some(phc) = phc {
            MAPPING.publish(calibration.add(Read { before, phc, after }));
            if calibration.jumped {
                JUMPED.store(true, Ordering::Relaxed);
            }
        }
        std::thread::sleep(PERIOD);
    }
    MAPPING.publish(None);
    Ok(())
}

/// The PHC showed `phc` ns at some counter value in `before..=after`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Read {
    before: u64,
    phc: u64,
    after: u64,
}

/// Consistent reads, oldest first, the fit through the latest, and whether a fit was dropped.
#[derive(Default)]
struct Calibration {
    reads: Vec<Read>,
    fit: Option<Fit>,
    jumped: bool,
}

impl Calibration {
    /// Adds `read` and returns the fit through it. A read that goes backwards or contradicts
    /// the fit drops every earlier read: no fit until 1 s of consistent reads.
    fn add(&mut self, read: Read) -> Option<Fit> {
        let backwards = read.after < read.before
            || self.reads.last().is_some_and(|last| read.before <= last.after || read.phc <= last.phc);
        if backwards || self.fit.is_some_and(|fit| fit.contradicts(read)) {
            self.reads.clear();
            if self.fit.take().is_some() {
                self.jumped = true;
            }
            if read.after < read.before {
                return None;
            }
        }
        if self.reads.len() == KEPT {
            self.reads.remove(0);
        }
        self.reads.push(read);
        let base = self
            .reads
            .iter()
            .filter(|old| read.phc - old.phc >= BASELINE_NS)
            .min_by_key(|old| old.after - old.before);
        self.fit = base.and_then(|base| Fit::new(read, *base));
        self.fit
    }
}

/// The published fit; one writer (`run`), a seqlock: `seq` is odd while writing. `stale_at`
/// is the counter value from which the fit is not ready; 0 when there is none.
#[repr(C, align(64))]
struct Mapping {
    seq: AtomicU64,
    m: AtomicU64,
    h: AtomicU64,
    p: AtomicU64,
    r: AtomicU64,
    u: AtomicU64,
    stale_at: AtomicU64,
}

impl Mapping {
    const fn new() -> Self {
        Mapping {
            seq: AtomicU64::new(0),
            m: AtomicU64::new(0),
            h: AtomicU64::new(0),
            p: AtomicU64::new(0),
            r: AtomicU64::new(0),
            u: AtomicU64::new(0),
            stale_at: AtomicU64::new(0),
        }
    }

    fn publish(&self, fit: Option<Fit>) {
        let fields = match fit {
            Some(fit) => [fit.m, fit.h, fit.p, fit.r, fit.u, fit.m.saturating_add(fit.ticks(STALE_NS))],
            None => [0; 6],
        };
        let seq = self.seq.load(Ordering::Relaxed);
        self.seq.store(seq + 1, Ordering::Relaxed);
        fence(Ordering::Release);
        for (field, value) in self.fields().into_iter().zip(fields) {
            field.store(value, Ordering::Relaxed);
        }
        self.seq.store(seq + 2, Ordering::Release);
    }

    fn fields(&self) -> [&AtomicU64; 6] {
        [&self.m, &self.h, &self.p, &self.r, &self.u, &self.stale_at]
    }

    /// The fields, or `None` while the writer is mid-publish.
    #[inline]
    fn load(&self) -> Option<[u64; 6]> {
        let seq = self.seq.load(Ordering::Acquire);
        let values = self.fields().map(|field| field.load(Ordering::Relaxed));
        fence(Ordering::Acquire);
        (seq.is_multiple_of(2) && self.seq.load(Ordering::Relaxed) == seq).then_some(values)
    }

    /// Latency from `stamp` to the moment the counter read `now`.
    #[inline]
    fn latency(&self, stamp: u64, now: u64) -> Option<Latency> {
        let [m, h, p, r, u, stale_at] = self.load()?;
        if now >= stale_at {
            return None;
        }
        let (phc, bound) = Fit { m, h, p, r, u }.at(now)?;
        Some(Latency { ns: i64::try_from(phc - i128::from(stamp)).ok()?, bound_ns: u64::try_from(bound).ok()? })
    }
}
