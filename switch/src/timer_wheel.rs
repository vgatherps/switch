//! Intrusive timer wheel driven by caller-supplied absolute u64 ticks.
//! Each of thirteen levels uses one five-bit timestamp digit; the top digit has four bits.

use crate::intrusive::{ListHead, ListLinked, NIL, Timed};

pub const DEFAULT_LEVELS: usize = 13;
pub const DEFAULT_SLOTS: usize = 32;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WheelPosition {
    #[default]
    Unlinked,
    Linked {
        level: u8,
        slot: u8,
    },
}

pub trait WheelEntry: Timed + ListLinked {
    fn wheel_position(&self) -> WheelPosition;
    fn set_wheel_position(&mut self, position: WheelPosition);
}

pub struct TimerWheel {
    levels: [[ListHead; DEFAULT_SLOTS]; DEFAULT_LEVELS],
    pending: [u32; DEFAULT_LEVELS],
    current_tick: u64,
}

impl TimerWheel {
    pub const fn new(initial_tick: u64) -> Self {
        Self {
            levels: [[ListHead::new(); DEFAULT_SLOTS]; DEFAULT_LEVELS],
            pending: [0; DEFAULT_LEVELS],
            current_tick: initial_tick,
        }
    }

    pub const fn current_tick(&self) -> u64 {
        self.current_tick
    }

    #[cfg(test)]
    pub fn has_pending(&self) -> bool {
        self.pending.iter().any(|&bits| bits != 0)
    }

    /// The entry's deadline must be after the current tick.
    pub fn insert<T: WheelEntry>(&mut self, entries: &mut [T], idx: u32) {
        debug_assert!(idx != NIL, "invalid timer index");
        debug_assert!(
            matches!(
                entries[idx as usize].wheel_position(),
                WheelPosition::Unlinked
            ),
            "timer is already linked"
        );
        let deadline = entries[idx as usize].deadline();
        debug_assert!(deadline > self.current_tick, "timer is due");
        let (level, slot) = Self::place(self.current_tick, deadline);
        self.link(entries, idx, level, slot);
    }

    pub fn remove<T: WheelEntry>(&mut self, entries: &mut [T], idx: u32) -> bool {
        debug_assert!(idx != NIL, "invalid timer index");
        let WheelPosition::Linked { level, slot } = entries[idx as usize].wheel_position() else {
            return false;
        };
        let (level, slot) = (level as usize, slot as usize);
        debug_assert!(
            level < DEFAULT_LEVELS && slot < DEFAULT_SLOTS,
            "invalid wheel position"
        );
        self.levels[level][slot].remove(entries, idx);
        entries[idx as usize].set_wheel_position(WheelPosition::Unlinked);
        if self.levels[level][slot].is_empty() {
            self.pending[level] &= !(1 << slot);
        }
        true
    }

    /// `now_tick` must not be before the current tick.
    pub fn advance_to<T: WheelEntry>(
        &mut self,
        entries: &mut [T],
        now_tick: u64,
        expired: &mut Vec<u32>,
    ) {
        debug_assert!(now_tick >= self.current_tick, "timer tick regressed");
        if now_tick == self.current_tick {
            return;
        }
        loop {
            let Some(boundary) = self.next_boundary() else {
                self.current_tick = now_tick;
                return;
            };
            if boundary > now_tick {
                self.current_tick = now_tick;
                return;
            }
            self.current_tick = boundary;
            for level in (0..DEFAULT_LEVELS).rev() {
                let shift = 5 * level;
                let width = 1u64 << shift;
                if boundary & (width - 1) != 0 {
                    continue;
                }
                let slot = ((boundary >> shift) & 31) as usize;
                if self.pending[level] & (1 << slot) == 0 {
                    continue;
                }
                let mut detached =
                    std::mem::replace(&mut self.levels[level][slot], ListHead::new());
                self.pending[level] &= !(1 << slot);
                while let Some(idx) = detached.pop_front(entries) {
                    entries[idx as usize].set_wheel_position(WheelPosition::Unlinked);
                    let deadline = entries[idx as usize].deadline();
                    if deadline <= boundary {
                        debug_assert!(deadline == boundary, "timer passed its deadline");
                        expired.push(idx);
                    } else {
                        let (new_level, new_slot) = Self::place(boundary, deadline);
                        debug_assert!(new_level < level, "cascade did not descend");
                        self.link(entries, idx, new_level, new_slot);
                    }
                }
            }
        }
    }

    fn link<T: WheelEntry>(&mut self, entries: &mut [T], idx: u32, level: usize, slot: usize) {
        entries[idx as usize].set_wheel_position(WheelPosition::Linked {
            level: level as u8,
            slot: slot as u8,
        });
        self.levels[level][slot].push_back(entries, idx);
        self.pending[level] |= 1 << slot;
    }

    fn place(current: u64, deadline: u64) -> (usize, usize) {
        let bit = 63 - (current ^ deadline).leading_zeros();
        let level = (bit / 5) as usize;
        (level, ((deadline >> (5 * level)) & 31) as usize)
    }

    fn next_boundary(&self) -> Option<u64> {
        let mut next = None;
        for level in 0..DEFAULT_LEVELS {
            let pending = self.pending[level];
            if pending == 0 {
                continue;
            }
            let shift = 5 * level;
            let digit = ((self.current_tick >> shift) & 31) as u32;
            let above = if digit == 31 {
                0
            } else {
                u32::MAX << (digit + 1)
            };
            debug_assert!(
                pending & !above == 0,
                "occupied bucket is not ahead of current time"
            );
            debug_assert!(
                level != 12 || pending & 0xffff0000 == 0,
                "invalid top-level bucket"
            );
            let slot = pending.trailing_zeros();
            let prefix = if level == 12 {
                0
            } else {
                (self.current_tick >> (shift + 5)) << (shift + 5)
            };
            let boundary = prefix | (u64::from(slot) << shift);
            debug_assert!(
                boundary > self.current_tick,
                "bucket boundary is not future"
            );
            next = Some(next.map_or(boundary, |previous: u64| previous.min(boundary)));
        }
        next
    }
}

// The wheel has no unsafe code, so its tests (including the 1024-entry
// workload controls and the seeded model test) do not run under Miri.
#[cfg(all(test, not(miri)))]
#[path = "timer_wheel/benchmark_workloads.rs"]
pub mod benchmark_workloads;

#[cfg(all(test, not(miri)))]
#[path = "timer_wheel/membership_tests.rs"]
mod membership_tests;

#[cfg(all(test, not(miri)))]
#[path = "timer_wheel/boundary_tests.rs"]
mod boundary_tests;

#[cfg(all(test, not(miri)))]
#[path = "timer_wheel/model_tests.rs"]
mod model_tests;

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;
    use crate::intrusive::{IntrusiveLinks, Linked};

    #[derive(Default, Clone)]
    struct TestTimer {
        links: IntrusiveLinks,
        deadline: u64,
        position: WheelPosition,
    }

    impl Linked for TestTimer {
        fn links(&self) -> &IntrusiveLinks {
            &self.links
        }
        fn links_mut(&mut self) -> &mut IntrusiveLinks {
            &mut self.links
        }
    }

    impl Timed for TestTimer {
        fn deadline(&self) -> u64 {
            self.deadline
        }
    }

    impl WheelEntry for TestTimer {
        fn wheel_position(&self) -> WheelPosition {
            self.position
        }
        fn set_wheel_position(&mut self, position: WheelPosition) {
            self.position = position;
        }
    }

    fn make_timers(deadlines: &[u64]) -> Vec<TestTimer> {
        deadlines
            .iter()
            .map(|&d| TestTimer {
                links: IntrusiveLinks::new(),
                deadline: d,
                position: WheelPosition::Unlinked,
            })
            .collect()
    }

    #[test]
    fn test_empty_wheel() {
        let wheel: TimerWheel = TimerWheel::new(0);
        assert!(!wheel.has_pending());
        assert_eq!(wheel.current_tick(), 0);
    }

    #[test]
    fn test_insert_single() {
        let mut timers = make_timers(&[5]);
        let mut wheel: TimerWheel = TimerWheel::new(0);

        wheel.insert(&mut timers, 0);

        assert!(wheel.has_pending());
    }

    #[test]
    fn test_expire_single() {
        let mut timers = make_timers(&[3]);
        let mut wheel: TimerWheel = TimerWheel::new(0);

        wheel.insert(&mut timers, 0);

        let mut expired = Vec::new();
        wheel.advance_to(&mut timers, 5, &mut expired);

        assert_eq!(expired, vec![0]);
        assert!(!wheel.has_pending());
    }

    #[test]
    fn test_expire_order() {
        let mut timers = make_timers(&[10, 5, 15, 3, 20]);
        let mut wheel: TimerWheel = TimerWheel::new(0);

        for i in 0..5 {
            wheel.insert(&mut timers, i);
        }

        let mut expired = Vec::new();
        wheel.advance_to(&mut timers, 25, &mut expired);

        // Should expire in deadline order: 3, 5, 10, 15, 20
        let deadlines: Vec<_> = expired
            .iter()
            .map(|&i| timers[i as usize].deadline)
            .collect();
        assert_eq!(deadlines, vec![3, 5, 10, 15, 20]);
    }

    #[test]
    fn test_remove_before_expire() {
        let mut timers = make_timers(&[10, 20]);
        let mut wheel: TimerWheel = TimerWheel::new(0);

        wheel.insert(&mut timers, 0);
        wheel.insert(&mut timers, 1);

        // Remove the first one
        wheel.remove(&mut timers, 0);

        let mut expired = Vec::new();
        wheel.advance_to(&mut timers, 25, &mut expired);

        assert_eq!(expired, vec![1]);
    }

    #[test]
    fn test_multiple_per_slot() {
        let mut timers = make_timers(&[5, 5, 5]);
        let mut wheel: TimerWheel = TimerWheel::new(0);

        for i in 0..3 {
            wheel.insert(&mut timers, i);
        }

        let mut expired = Vec::new();
        wheel.advance_to(&mut timers, 10, &mut expired);

        assert_eq!(expired.len(), 3);
    }

    #[test]
    fn test_cascade_from_l1() {
        let mut timers = make_timers(&[40]); // Will go to L1 (> 32 ticks away)
        let mut wheel: TimerWheel = TimerWheel::new(0);

        wheel.insert(&mut timers, 0);

        // Should be in L1
        assert_eq!(
            timers[0].position,
            WheelPosition::Linked { level: 1, slot: 1 }
        );

        let mut expired = Vec::new();
        wheel.advance_to(&mut timers, 45, &mut expired);

        assert_eq!(expired, vec![0]);
    }

    #[test]
    fn test_advance_partial() {
        let mut timers = make_timers(&[5, 15, 25]);
        let mut wheel: TimerWheel = TimerWheel::new(0);

        for i in 0..3 {
            wheel.insert(&mut timers, i);
        }

        let mut expired1 = Vec::new();
        wheel.advance_to(&mut timers, 10, &mut expired1);
        assert_eq!(expired1, vec![0]);

        let mut expired2 = Vec::new();
        wheel.advance_to(&mut timers, 20, &mut expired2);
        assert_eq!(expired2, vec![1]);

        let mut expired3 = Vec::new();
        wheel.advance_to(&mut timers, 30, &mut expired3);
        assert_eq!(expired3, vec![2]);
    }

    #[test]
    fn test_large_deadline() {
        let mut timers = make_timers(&[100000]);
        let mut wheel: TimerWheel = TimerWheel::new(0);

        wheel.insert(&mut timers, 0);

        // Should be in highest level
        assert!(matches!(
            timers[0].position,
            WheelPosition::Linked { level: 2.., .. }
        ));
    }
}
