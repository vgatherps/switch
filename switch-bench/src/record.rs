//! What the generator learns from echoes in one rate step: latency of every measured message
//! (receive time - scheduled time), which messages came back (a bitmap over the step's message
//! numbers; duplicates, echoes of another step and mangled headers are ignored), how many of this
//! step's echoes arrived within the measurement window (the achieved receive rate), and (TCP/ws)
//! the service round trip: echo time - actual send time.

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
    /// Messages `0..end` echoed so far: each counts at its first echo only.
    seen: Vec<u64>,
    pub latency: Histogram,
    pub received: u64,
    /// This step's echoes (warmup and measured, once each) that arrived within `window`.
    pub received_in_window: u64,
    /// Monotonic ns `[start, end)` of the measurement window.
    window: [u64; 2],
    /// Echo time - actual send time (TCP/ws; recorded by the connection's receive task).
    pub service_rtt: Histogram,
    /// Monotonic ns of this step's latest first echo of a message.
    pub last_echo_ns: Option<u64>,
    /// This step's messages echoed (first echo, warmup included), whenever they arrived.
    pub echoed: u64,
}

impl Recorder {
    /// For step `step` sending messages `0..end` of `schedule`, of which `measured_from..end`
    /// are measured within `window`. Allocates the bitmap here, not while recording.
    pub fn new(
        step: u32,
        schedule: Schedule,
        measured_from: u64,
        end: u64,
        window: [u64; 2],
    ) -> Recorder {
        assert!(measured_from <= end);
        let words = end.div_ceil(64) as usize;
        Recorder {
            step,
            schedule,
            measured_from,
            end,
            seen: vec![0; words],
            latency: Histogram::new(),
            received: 0,
            received_in_window: 0,
            window,
            service_rtt: Histogram::new(),
            last_echo_ns: None,
            echoed: 0,
        }
    }

    /// One echo received at `now_ns` (the generator's clock).
    #[inline]
    pub fn record(&mut self, payload: &[u8], now_ns: u64) {
        let Some(header) = Header::read(payload) else {
            return;
        };
        if header.step != self.step || header.sender >= self.schedule.senders {
            return;
        }
        let k = header.seq * u64::from(self.schedule.senders) + u64::from(header.sender);
        if k >= self.end || header.sched_ns != self.schedule.due_at(k) {
            return;
        }
        let (word, mask) = ((k / 64) as usize, 1u64 << (k % 64));
        if self.seen[word] & mask != 0 {
            return;
        }
        self.seen[word] |= mask;
        self.last_echo_ns = Some(now_ns);
        self.echoed += 1;
        self.received_in_window += u64::from((self.window[0]..self.window[1]).contains(&now_ns));
        if k < self.measured_from {
            return;
        }
        self.received += 1;
        self.latency.record(now_ns.saturating_sub(header.sched_ns));
    }

    /// Measured messages.
    pub fn measured(&self) -> u64 {
        self.end - self.measured_from
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

    /// Latency is receive time minus the scheduled time; warmup echoes are not measured; every
    /// measured message not echoed is lost.
    #[test]
    fn latency_warmup_and_loss() {
        let s = Schedule {
            senders: 3,
            interval_ns: 3000,
            origin_ns: 10_000,
        };
        let mut r = Recorder::new(1, s, 6, 30, [0, u64::MAX]);
        for k in 0..30 {
            if k % 10 == 9 {
                continue; // lost: 9, 19, 29
            }
            r.record(&message(1, &s, k), s.due_at(k) + 500 + k);
        }
        assert_eq!((r.measured(), r.received), (24, 21));
        assert_eq!(r.latency.count(), 21);
        assert_eq!(r.latency.max(), 528);
    }

    /// A second copy of a message, an echo of another step or past the step's end, and a
    /// mangled header are not receives.
    #[test]
    fn duplicates_stale_and_malformed_are_ignored() {
        let s = Schedule {
            senders: 2,
            interval_ns: 1000,
            origin_ns: 0,
        };
        let mut r = Recorder::new(5, s, 0, 10, [0, u64::MAX]);
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
        assert_eq!(r.received, 1);
    }

    /// An echo that arrives before its scheduled time (clock oddity) measures 0, not a wrap.
    #[test]
    fn early_echo_measures_zero() {
        let s = Schedule {
            senders: 1,
            interval_ns: 1000,
            origin_ns: 5000,
        };
        let mut r = Recorder::new(0, s, 0, 1, [0, u64::MAX]);
        r.record(&message(0, &s, 0), 4000);
        assert_eq!(r.latency.max(), 0);
    }

    /// The achieved receive count takes this step's first echoes (warmup or measured) that
    /// arrive inside the window; late, duplicate and stale echoes are not throughput of it.
    #[test]
    fn received_in_window_counts_first_echoes_inside_it() {
        let s = Schedule {
            senders: 1,
            interval_ns: 1000,
            origin_ns: 0,
        };
        let mut r = Recorder::new(2, s, 2, 10, [2000, 10_000]);
        r.record(&message(2, &s, 0), 1999); // warmup, before the window
        r.record(&message(2, &s, 1), 2000); // warmup, inside
        r.record(&message(2, &s, 2), 2500);
        r.record(&message(2, &s, 2), 2600); // duplicate
        r.record(&message(1, &s, 3), 3000); // stale
        r.record(&message(2, &s, 3), 10_000); // after the window
        assert_eq!(r.received_in_window, 2);
        assert_eq!(r.received, 2);
    }

    /// A warmup message echoed twice counts once in the window, at its first echo: a copy
    /// arriving inside the window after the first arrived before it, or a second copy inside
    /// it, is not another receive.
    #[test]
    fn a_duplicated_warmup_echo_counts_once() {
        let s = Schedule {
            senders: 1,
            interval_ns: 1000,
            origin_ns: 0,
        };
        let mut r = Recorder::new(0, s, 2, 10, [2000, 10_000]);
        r.record(&message(0, &s, 0), 1999); // before the window
        r.record(&message(0, &s, 0), 2500); // its duplicate, inside
        r.record(&message(0, &s, 1), 2100);
        r.record(&message(0, &s, 1), 2200); // its duplicate
        assert_eq!(r.received_in_window, 1);
        assert_eq!(r.received, 0);
    }
}
