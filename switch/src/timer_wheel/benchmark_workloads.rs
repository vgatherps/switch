use crate::intrusive::{IntrusiveLinks, Linked, ListLinked, NIL, Timed};
use crate::timer_wheel::{TimerWheel, WheelEntry, WheelPosition};

pub struct Entry {
    links: IntrusiveLinks,
    deadline: u64,
    position: WheelPosition,
}

impl Linked for Entry {
    fn links(&self) -> &IntrusiveLinks {
        &self.links
    }
    fn links_mut(&mut self) -> &mut IntrusiveLinks {
        &mut self.links
    }
}

impl Timed for Entry {
    fn deadline(&self) -> u64 {
        self.deadline
    }
}

impl WheelEntry for Entry {
    fn wheel_position(&self) -> WheelPosition {
        self.position
    }
    fn set_wheel_position(&mut self, position: WheelPosition) {
        self.position = position;
    }
}

pub fn entries(deadlines: impl IntoIterator<Item = u64>) -> Vec<Entry> {
    deadlines
        .into_iter()
        .map(|deadline| Entry {
            links: IntrusiveLinks::new(),
            deadline,
            position: WheelPosition::Unlinked,
        })
        .collect()
}

pub fn near_entries(count: usize) -> Vec<Entry> {
    entries((0..count).map(|index| 1 + index as u64 % 31))
}

pub fn insert_all(wheel: &mut TimerWheel, entries: &mut [Entry]) {
    for index in 0..entries.len() {
        wheel.insert(entries, index as u32);
    }
}

pub fn remove_all(wheel: &mut TimerWheel, entries: &mut [Entry]) {
    for index in 0..entries.len() {
        wheel.remove(entries, index as u32);
    }
}

pub fn prepared(count: usize, deadline: u64) -> (TimerWheel, Vec<Entry>, Vec<u32>) {
    let mut entries = entries(std::iter::repeat_n(deadline, count));
    let mut wheel = TimerWheel::new(0);
    insert_all(&mut wheel, &mut entries);
    (wheel, entries, Vec::with_capacity(count))
}

pub fn advance(state: &mut (TimerWheel, Vec<Entry>, Vec<u32>), now: u64) {
    let (wheel, entries, expired) = state;
    wheel.advance_to(entries, now, expired);
}

fn assert_detached(entries: &[Entry]) {
    for entry in entries {
        assert_eq!(entry.wheel_position(), WheelPosition::Unlinked);
        assert_eq!(entry.prev(), NIL);
        assert_eq!(entry.next(), NIL);
    }
}

pub fn check_insert_remove(count: usize) {
    let mut wheel = TimerWheel::new(0);
    let mut entries = near_entries(count);
    insert_all(&mut wheel, &mut entries);
    remove_all(&mut wheel, &mut entries);
    let mut expired = Vec::new();
    wheel.advance_to(&mut entries, 31, &mut expired);
    assert!(expired.is_empty());
    assert!(!wheel.has_pending());
    assert_detached(&entries);
}

pub fn check_expiry(count: usize, deadline: u64, end: u64) {
    let mut state = prepared(count, deadline);
    advance(&mut state, deadline - 1);
    assert!(state.2.is_empty());
    assert!(state.0.has_pending());
    advance(&mut state, deadline);
    let mut actual = state.2.clone();
    actual.sort_unstable();
    assert_eq!(actual, (0..count as u32).collect::<Vec<_>>());
    assert!(!state.0.has_pending());
    advance(&mut state, end);
    assert_eq!(state.2.len(), count);
    assert_detached(&state.1);
}

#[cfg(test)]
mod tests {
    #[test]
    fn near_insert_remove_controls() {
        for count in [1, 32, 1024] {
            super::check_insert_remove(count);
        }
    }

    #[test]
    fn dense_l0_expiry_control() {
        super::check_expiry(1024, 7, 7);
    }

    #[test]
    fn aligned_cascade_expiry_control() {
        super::check_expiry(1024, 40, 40);
    }

    #[test]
    fn sparse_gap_expiry_controls() {
        for count in [1, 32] {
            super::check_expiry(count, 100001, 1048576);
        }
    }

    #[test]
    fn cancelled_near_entries_leave_exact_survivors() {
        let mut entries = super::near_entries(1024);
        let mut wheel = super::TimerWheel::new(0);
        super::insert_all(&mut wheel, &mut entries);
        for index in (0..1024).step_by(2) {
            wheel.remove(&mut entries, index);
        }
        let mut expired = Vec::new();
        wheel.advance_to(&mut entries, 31, &mut expired);
        expired.sort_unstable();
        assert_eq!(expired, (1..1024).step_by(2).collect::<Vec<u32>>());
        assert!(!wheel.has_pending());
        super::assert_detached(&entries);
    }
}
