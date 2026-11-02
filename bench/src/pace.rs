//! Which message the send task tries next, at most `budget` tries per poll so the loop turns the
//! Net between batches however far behind the generator falls.
//!
//! Messages are tried in schedule order as they fall due. A message whose send is refused for
//! the whole transport for now (`Outcome::Stop`: no TX slot) is retried first next poll. A
//! message refused for its own sender only (`Outcome::Blocked`: that TCP connection is full)
//! blocks just that sender: its later messages are owed, and after each poll's fresh messages
//! every blocked sender is retried once, its owed messages going out in order once it takes
//! sends again. So the generator's own backlog (`backlog_at_end`) does not count a receiver's flow
//! control.

use std::collections::VecDeque;

use crate::schedule::Schedule;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Sent,
    /// This sender cannot send now (its connection is full); other senders may.
    Blocked,
    /// Nothing can be sent now; retry this message next poll.
    Stop,
}

#[derive(Debug)]
pub struct Pacer {
    schedule: Schedule,
    /// Messages `0..end` make up the step.
    end: u64,
    budget: u64,
    /// Messages of each sender sent so far.
    sent: Vec<u64>,
    total_sent: u64,
    /// Every message below the cursor was sent, or is owed by a blocked sender.
    cursor: u64,
    blocked: VecDeque<u32>,
    is_blocked: Vec<bool>,
    // this poll
    due: u64,
    left: u64,
    visited_blocked: usize,
    stopped: bool,
    /// At the end of the first poll after the last message's scheduled time: due and not yet
    /// reached by the cursor.
    at_end: Option<u64>,
}

impl Pacer {
    pub fn new(schedule: Schedule, end: u64, budget: u32) -> Pacer {
        assert!(budget > 0);
        let n = schedule.senders as usize;
        Pacer {
            schedule,
            end,
            budget: u64::from(budget),
            sent: vec![0; n],
            total_sent: 0,
            cursor: 0,
            blocked: VecDeque::with_capacity(n),
            is_blocked: vec![false; n],
            due: 0,
            left: 0,
            visited_blocked: 0,
            stopped: false,
            at_end: None,
        }
    }

    /// Starts a poll at `now_ns`: releases what has fallen due and resets the budget.
    #[inline]
    pub fn begin(&mut self, now_ns: u64) {
        self.due = self.schedule.due_by(now_ns).min(self.end);
        self.left = self.budget;
        self.visited_blocked = 0;
        self.stopped = false;
    }

    /// Messages of sender `i` the cursor has passed: those it owes or has sent.
    fn released(&self, i: u32) -> u64 {
        self.count_below(self.cursor, i)
    }

    /// The next message to try this poll, `None` when the poll is over.
    #[inline]
    pub fn next_try(&mut self) -> Option<u64> {
        if self.stopped || self.left == 0 {
            return None;
        }
        let n = u64::from(self.schedule.senders);
        while self.cursor < self.due {
            let k = self.cursor;
            if !self.is_blocked[(k % n) as usize] {
                return Some(k);
            }
            self.cursor += 1; // owed by its blocked sender
        }
        // then each blocked sender's oldest owed message, once per poll unless it goes out
        let &i = self.blocked.get(self.visited_blocked)?;
        Some(self.sent[i as usize] * n + u64::from(i))
    }

    /// What became of message `k`, the one `next_try` returned.
    #[inline]
    pub fn result(&mut self, k: u64, outcome: Outcome) {
        let (sender, _) = self.schedule.split(k);
        let i = sender as usize;
        match outcome {
            Outcome::Sent => {
                self.sent[i] += 1;
                self.total_sent += 1;
                self.left -= 1;
                if self.is_blocked[i] {
                    if self.sent[i] == self.released(sender) {
                        self.blocked.remove(self.visited_blocked);
                        self.is_blocked[i] = false;
                    }
                } else {
                    self.cursor = k + 1;
                }
            }
            Outcome::Blocked => {
                self.left -= 1;
                if self.is_blocked[i] {
                    self.visited_blocked += 1;
                } else {
                    // queued behind the blocked senders not yet retried this poll
                    self.is_blocked[i] = true;
                    self.blocked.push_back(sender);
                    self.cursor = k + 1;
                }
            }
            Outcome::Stop => self.stopped = true,
        }
    }

    /// Ends the poll at `now_ns`: at the first poll after the last message's scheduled time,
    /// records what was due and not sent, split by cause.
    #[inline]
    pub fn finish(&mut self, now_ns: u64) {
        if self.at_end.is_none() && self.end > 0 && now_ns >= self.schedule.due_at(self.end - 1) {
            self.at_end = Some(self.due - self.cursor);
        }
    }

    pub fn done(&self) -> bool {
        self.total_sent == self.end
    }

    /// Due and not yet tried at the end of the first poll after the last message's scheduled
    /// time because of the generator itself (its budget, or a transport-wide refusal): 0 if it
    /// kept up.
    pub fn backlog_at_end(&self) -> u64 {
        self.at_end.unwrap_or(0)
    }

    /// Whether message `k` goes out as a blocked sender's owed message (its wait includes the
    /// connection's flow control) rather than fresh off the schedule.
    pub fn is_owed(&self, k: u64) -> bool {
        self.is_blocked[self.schedule.split(k).0 as usize]
    }

    /// Messages of sender `i` numbered below `k`.
    fn count_below(&self, k: u64, i: u32) -> u64 {
        let (n, i) = (u64::from(self.schedule.senders), u64::from(i));
        if k <= i { 0 } else { (k - i).div_ceil(n) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Drives a pacer: each poll at `now`, `outcome(k, poll)` decides each try. Returns every
    /// sent k in send order and the number of polls.
    fn drive(
        p: &mut Pacer,
        step_ns: u64,
        mut outcome: impl FnMut(u64, u64) -> Outcome,
    ) -> (Vec<u64>, u64) {
        let (mut now, mut polls, mut sent) = (0, 0, vec![]);
        while !p.done() {
            p.begin(now);
            let mut tries = 0;
            while let Some(k) = p.next_try() {
                tries += 1;
                let o = outcome(k, polls);
                if o == Outcome::Sent {
                    sent.push(k);
                }
                p.result(k, o);
            }
            assert!(tries <= p.budget, "{tries} tries in one poll");
            p.finish(now);
            polls += 1;
            now += step_ns;
            assert!(polls < 1_000_000, "no progress");
        }
        (sent, polls)
    }

    /// Within capacity each poll sends exactly what is due; nothing is left at the end.
    #[test]
    fn keeps_up_when_polled_faster_than_the_rate() {
        let s = Schedule {
            senders: 2,
            interval_ns: 2000,
            origin_ns: 0,
        };
        let mut p = Pacer::new(s, 100, 32);
        let (sent, _) = drive(&mut p, 300, |_, _| Outcome::Sent);
        assert_eq!(sent, (0..100).collect::<Vec<_>>());
        assert_eq!(p.backlog_at_end(), 0);
    }

    /// Overload: 100 messages fall due per poll, the budget is 32. Every poll tries at most the
    /// budget, so the loop turns the Net after each batch; the backlog grows and is flagged;
    /// the rest still goes out afterwards, oldest first.
    #[test]
    fn overload_holds_the_budget_and_flags_the_backlog() {
        let s = Schedule {
            senders: 4,
            interval_ns: 400,
            origin_ns: 0,
        };
        let mut p = Pacer::new(s, 10_000, 32);
        let (sent, polls) = drive(&mut p, 10_000, |_, _| Outcome::Sent);
        assert_eq!(sent, (0..10_000).collect::<Vec<_>>());
        assert!(polls >= 10_000 / 32);
        assert!(p.backlog_at_end() > 5_000, "backlog {}", p.backlog_at_end());
    }

    /// A transport-wide refusal stops the poll; the same message is retried next poll.
    #[test]
    fn stop_retries_the_same_message() {
        let s = Schedule {
            senders: 1,
            interval_ns: 1000,
            origin_ns: 0,
        };
        let mut p = Pacer::new(s, 10, 32);
        let mut refused = 0;
        let (sent, _) = drive(&mut p, 1000, |k, poll| {
            if k == 3 && poll < 8 {
                refused += 1;
                Outcome::Stop
            } else {
                Outcome::Sent
            }
        });
        assert_eq!(sent, (0..10).collect::<Vec<_>>());
        assert!(refused >= 4);
    }

    /// Sender 1's connection is full for 20 polls: the other senders' messages keep going out
    /// on time; sender 1's owed messages then go out in its own order, each exactly once.
    #[test]
    fn a_blocked_sender_does_not_hold_back_the_others() {
        let s = Schedule {
            senders: 3,
            interval_ns: 3000,
            origin_ns: 0,
        }; // one message per µs
        let mut p = Pacer::new(s, 300, 8);
        let mut first_poll_of = std::collections::HashMap::new();
        let (sent, _) = drive(&mut p, 1000, |k, poll| {
            if k % 3 == 1 && poll < 20 {
                return Outcome::Blocked;
            }
            first_poll_of.entry(k).or_insert(poll);
            Outcome::Sent
        });
        let mut sorted = sent.clone();
        sorted.sort_unstable();
        assert_eq!(
            sorted,
            (0..300).collect::<Vec<_>>(),
            "each message exactly once"
        );
        for sender in 0..3 {
            let own: Vec<u64> = sent.iter().copied().filter(|k| k % 3 == sender).collect();
            assert!(
                own.windows(2).all(|w| w[0] < w[1]),
                "sender {sender} out of order"
            );
        }
        // the others were sent in the poll they fell due (one message per µs = per poll)
        for k in (0..60).filter(|k| k % 3 != 1) {
            assert_eq!(first_poll_of[&k], k, "message {k} delayed");
        }
        assert!(first_poll_of[&1] >= 20);
    }

    /// At the end of the window, messages a full connection owes are not the generator's
    /// backlog.
    #[test]
    fn flow_control_is_owed_not_generator_backlog() {
        let s = Schedule {
            senders: 2,
            interval_ns: 2000,
            origin_ns: 0,
        };
        let mut p = Pacer::new(s, 100, 32);
        drive(&mut p, 500, |k, poll| {
            if k % 2 == 0 && poll < 400 {
                Outcome::Blocked
            } else {
                Outcome::Sent
            }
        });
        assert_eq!(p.backlog_at_end(), 0);
    }

    /// Sender 0 is blocked from the first poll. In the second poll sender 1 blocks on a fresh
    /// message; sender 0 must still get its one retry in that poll.
    #[test]
    fn a_new_block_does_not_skip_an_older_blocked_sender() {
        let s = Schedule {
            senders: 2,
            interval_ns: 2000,
            origin_ns: 0,
        }; // one message per µs
        let mut p = Pacer::new(s, 100, 32);
        p.begin(0);
        while let Some(k) = p.next_try() {
            assert_eq!(k, 0);
            p.result(k, Outcome::Blocked);
        }
        p.finish(0);

        p.begin(1000);
        assert_eq!(p.next_try(), Some(1));
        p.result(1, Outcome::Blocked);
        assert_eq!(p.next_try(), Some(0), "sender 0 skipped");
    }

    /// The send task stopped being polled at 10 µs (starved); the one poll at the cutoff tries
    /// one budget's worth of the 20 overdue messages, and the rest is the generator's backlog.
    /// Without that poll nothing would have been counted.
    #[test]
    fn a_starved_send_task_shows_its_backlog_at_the_cutoff_poll() {
        let s = Schedule {
            senders: 2,
            interval_ns: 2000,
            origin_ns: 0,
        }; // one message per µs
        let mut p = Pacer::new(s, 30, 8);
        for now in (0..10_000).step_by(1000) {
            p.begin(now);
            while let Some(k) = p.next_try() {
                p.result(k, Outcome::Sent);
            }
            p.finish(now);
        }
        assert_eq!(p.backlog_at_end(), 0, "not yet at the end");
        p.begin(30_000);
        while let Some(k) = p.next_try() {
            p.result(k, Outcome::Sent);
        }
        p.finish(30_000);
        assert_eq!(p.backlog_at_end(), 30 - 10 - 8);
    }
}
