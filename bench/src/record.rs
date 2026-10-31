//! What the generator learns from echoes in one rate step: latency of every measured message
//! (receive time - scheduled time), which messages came back (a bitmap over the step's message
//! numbers), duplicates, and echoes that do not belong to this step.

use crate::hist::Histogram;
use crate::schedule::Schedule;
use crate::wire::Header;

pub struct Recorder {
    step: u32,
    schedule: Schedule,
    /// Messages `0..measured_from` are warmup: counted on return, not measured.
    measured_from: u64,
    /// Messages `measured_from..end` are measured.
    end: u64,
    seen: Vec<u64>,
    pub latency: Histogram,
    pub received: u64,
    pub warmup_received: u64,
    pub duplicates: u64,
    /// Echoes of another step, or of a message number outside this step's schedule.
    pub stale: u64,
    /// Shorter than a header, or a header whose scheduled time does not match the schedule.
    pub malformed: u64,
}

impl Recorder {
    /// For step `step` sending messages `0..end` of `schedule`, of which `measured_from..end`
    /// are measured. Allocates the bitmap here, not while recording.
    pub fn new(step: u32, schedule: Schedule, measured_from: u64, end: u64) -> Recorder {
        assert!(measured_from <= end);
        let words = (end - measured_from).div_ceil(64) as usize;
        Recorder {
            step,
            schedule,
            measured_from,
            end,
            seen: vec![0; words],
            latency: Histogram::new(),
            received: 0,
            warmup_received: 0,
            duplicates: 0,
            stale: 0,
            malformed: 0,
        }
    }

    /// One echo received at `now_ns` (the generator's clock).
    #[inline]
    pub fn record(&mut self, payload: &[u8], now_ns: u64) {
        let Some(header) = Header::read(payload) else {
            self.malformed += 1;
            return;
        };
        if header.step != self.step || header.sender >= self.schedule.senders {
            self.stale += 1;
            return;
        }
        let k = header.seq * u64::from(self.schedule.senders) + u64::from(header.sender);
        if k >= self.end {
            self.stale += 1;
            return;
        }
        if header.sched_ns != self.schedule.due_at(k) {
            self.malformed += 1;
            return;
        }
        if k < self.measured_from {
            self.warmup_received += 1;
            return;
        }
        let bit = k - self.measured_from;
        let (word, mask) = ((bit / 64) as usize, 1u64 << (bit % 64));
        if self.seen[word] & mask != 0 {
            self.duplicates += 1;
            return;
        }
        self.seen[word] |= mask;
        self.received += 1;
        self.latency.record(now_ns.saturating_sub(header.sched_ns));
    }

    /// Measured messages.
    pub fn measured(&self) -> u64 {
        self.end - self.measured_from
    }

    /// Measured messages never echoed (so far).
    pub fn lost(&self) -> u64 {
        self.measured() - self.received
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::HEADER;

    fn message(step: u32, s: &Schedule, k: u64) -> Vec<u8> {
        let (sender, seq) = s.split(k);
        let mut buf = vec![0; HEADER + 8];
        Header {
            step,
            sender,
            seq,
            sched_ns: s.due_at(k),
        }
        .write(&mut buf);
        buf
    }

    /// Latency is receive time minus the scheduled time; warmup echoes are counted apart and
    /// not measured; every measured message not echoed is lost.
    #[test]
    fn latency_warmup_and_loss() {
        let s = Schedule {
            senders: 3,
            interval_ns: 3000,
            origin_ns: 10_000,
        };
        let mut r = Recorder::new(1, s, 6, 30);
        for k in 0..30 {
            if k % 10 == 9 {
                continue; // lost: 9, 19, 29
            }
            r.record(&message(1, &s, k), s.due_at(k) + 500 + k);
        }
        assert_eq!(r.warmup_received, 6);
        assert_eq!((r.measured(), r.received, r.lost()), (24, 21, 3));
        assert_eq!(r.latency.count(), 21);
        assert_eq!(r.latency.min(), Some(506));
        assert_eq!(r.latency.max(), 528);
    }

    /// A second copy of a message is a duplicate, not a second receive; an echo of another
    /// step, or past the step's end, is stale; a mangled header is malformed.
    #[test]
    fn duplicates_stale_and_malformed() {
        let s = Schedule {
            senders: 2,
            interval_ns: 1000,
            origin_ns: 0,
        };
        let mut r = Recorder::new(5, s, 0, 10);
        r.record(&message(5, &s, 3), 10_000);
        r.record(&message(5, &s, 3), 10_001);
        r.record(&message(4, &s, 3), 10_002);
        r.record(&message(5, &s, 10), 10_003);
        let mut wrong_time = message(5, &s, 4);
        wrong_time[16] ^= 1;
        r.record(&wrong_time, 10_004);
        r.record(&[0; HEADER - 1], 10_005);
        let mut bad_sender = message(5, &s, 1);
        bad_sender[4] = 9;
        r.record(&bad_sender, 10_006);
        assert_eq!(
            (r.received, r.duplicates, r.stale, r.malformed),
            (1, 1, 3, 2)
        );
        assert_eq!(r.lost(), 9);
    }

    /// An echo that arrives before its scheduled time (clock oddity) measures 0, not a wrap.
    #[test]
    fn early_echo_measures_zero() {
        let s = Schedule {
            senders: 1,
            interval_ns: 1000,
            origin_ns: 5000,
        };
        let mut r = Recorder::new(0, s, 0, 1);
        r.record(&message(0, &s, 0), 4000);
        assert_eq!(r.latency.max(), 0);
    }
}
