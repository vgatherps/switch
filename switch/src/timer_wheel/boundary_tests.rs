use super::benchmark_workloads::Entry;
use super::*;
use crate::intrusive::{IntrusiveLinks, Linked};

fn check_future_positions(wheel: &TimerWheel, entries: &[Entry]) {
    let mut linked = 0;
    for entry in entries {
        let WheelPosition::Linked { level, slot } = entry.wheel_position() else {
            continue;
        };
        linked += 1;
        let shift = 5 * usize::from(level);
        assert!(entry.deadline() > wheel.current_tick());
        assert!(u64::from(slot) > ((wheel.current_tick() >> shift) & 31));
        assert_eq!(u64::from(slot), (entry.deadline() >> shift) & 31);
        if level == 12 {
            assert!(slot < 16);
        } else {
            assert_eq!(
                entry.deadline() >> (shift + 5),
                wheel.current_tick() >> (shift + 5)
            );
        }
    }
    assert_eq!(wheel.has_pending(), linked != 0);
    assert_eq!(wheel.pending[12] & 0xffff0000, 0);
}

#[test]
fn former_late_boundaries_expire_exactly() {
    let mut entries = super::benchmark_workloads::entries([64, 65, 1024, 1025]);
    let mut wheel = TimerWheel::new(0);
    for index in 0..4 {
        wheel.insert(&mut entries, index);
    }
    let mut expired = Vec::new();
    for (tick, expected) in [(63, 0), (64, 1), (65, 2), (1023, 2), (1024, 3), (1025, 4)] {
        wheel.advance_to(&mut entries, tick, &mut expired);
        assert_eq!(expired, (0..expected).collect::<Vec<u32>>());
        check_future_positions(&wheel, &entries);
    }
}

#[test]
fn equal_time_leaves_everything_untouched() {
    let mut entries = super::benchmark_workloads::entries([101]);
    let mut wheel = TimerWheel::new(100);
    wheel.insert(&mut entries, 0);
    let mut expired = Vec::with_capacity(1);
    expired.push(77);
    let capacity = expired.capacity();
    let pending = wheel.pending;
    let heads = wheel
        .levels
        .map(|buckets| buckets.map(|head| (head.first, head.last)));
    let position = entries[0].wheel_position();
    wheel.advance_to(&mut entries, 100, &mut expired);
    assert_eq!(wheel.current_tick(), 100);
    assert_eq!(wheel.pending, pending);
    assert_eq!(
        wheel
            .levels
            .map(|buckets| buckets.map(|head| (head.first, head.last))),
        heads
    );
    assert_eq!(entries[0].wheel_position(), position);
    assert_eq!(entries[0].prev(), NIL);
    assert_eq!(entries[0].next(), NIL);
    assert_eq!(expired, [77]);
    assert_eq!(expired.capacity(), capacity);
    check_future_positions(&wheel, &entries);
    wheel.advance_to(&mut entries, 101, &mut expired);
    assert_eq!(expired, [77, 0]);
}

#[test]
fn every_five_bit_transition_preserves_exact_expiry_and_future_positions() {
    for level in 0..13 {
        let boundary = 1u64 << (5 * level);
        for start in [boundary - 1, boundary, boundary + 1] {
            let deadlines = [start + 1, start + 2];
            let mut entries = super::benchmark_workloads::entries(deadlines);
            let mut wheel = TimerWheel::new(start);
            for index in 0..2 {
                wheel.insert(&mut entries, index);
            }
            check_future_positions(&wheel, &entries);
            let mut expired = Vec::new();
            wheel.advance_to(&mut entries, start, &mut expired);
            assert!(expired.is_empty());
            wheel.advance_to(&mut entries, deadlines[0], &mut expired);
            assert_eq!(expired, [0]);
            check_future_positions(&wheel, &entries);
            wheel.advance_to(&mut entries, deadlines[1], &mut expired);
            assert_eq!(expired, [0, 1]);
            check_future_positions(&wheel, &entries);
        }
    }
}

#[test]
fn all_sixteen_top_digits_are_supported() {
    for digit in 0..16u64 {
        let boundary = digit << 60;
        let start = if digit == 0 { 0 } else { boundary - 1 };
        let deadline = if digit == 0 { 1 } else { boundary };
        let mut entries = super::benchmark_workloads::entries([deadline]);
        let mut wheel = TimerWheel::new(start);
        wheel.insert(&mut entries, 0);
        if digit != 0 {
            assert_eq!(
                entries[0].wheel_position(),
                WheelPosition::Linked {
                    level: 12,
                    slot: digit as u8
                }
            );
        }
        check_future_positions(&wheel, &entries);
        let mut expired = Vec::new();
        wheel.advance_to(&mut entries, deadline - 1, &mut expired);
        assert!(expired.is_empty());
        wheel.advance_to(&mut entries, deadline, &mut expired);
        assert_eq!(expired, [0]);
        check_future_positions(&wheel, &entries);
    }
}

#[test]
fn arbitrary_starts_and_max_neighbor_use_absolute_deadlines() {
    for (start, deadlines) in [
        (1, [33, 1025]),
        (31, [32, 64]),
        (u64::MAX - 1, [u64::MAX, u64::MAX]),
    ] {
        let mut entries = super::benchmark_workloads::entries(deadlines);
        let mut wheel = TimerWheel::new(start);
        for index in 0..2 {
            wheel.insert(&mut entries, index);
        }
        check_future_positions(&wheel, &entries);
        let mut expired = Vec::new();
        wheel.advance_to(&mut entries, deadlines[0] - 1, &mut expired);
        assert!(expired.is_empty());
        wheel.advance_to(&mut entries, deadlines[0], &mut expired);
        assert_eq!(
            expired.len(),
            if deadlines[0] == deadlines[1] { 2 } else { 1 }
        );
        check_future_positions(&wheel, &entries);
        wheel.advance_to(&mut entries, deadlines[1], &mut expired);
        assert_eq!(expired, [0, 1]);
        check_future_positions(&wheel, &entries);
    }
}

struct CountedEntry {
    links: IntrusiveLinks,
    position: WheelPosition,
    dispositions: usize,
}

impl Linked for CountedEntry {
    fn links(&self) -> &IntrusiveLinks {
        &self.links
    }
    fn links_mut(&mut self) -> &mut IntrusiveLinks {
        &mut self.links
    }
}
impl Timed for CountedEntry {
    fn deadline(&self) -> u64 {
        u64::MAX
    }
}
impl WheelEntry for CountedEntry {
    fn wheel_position(&self) -> WheelPosition {
        self.position
    }
    fn set_wheel_position(&mut self, position: WheelPosition) {
        if position == WheelPosition::Unlinked {
            self.dispositions += 1;
        }
        self.position = position;
    }
}

#[test]
fn single_max_deadline_needs_at_most_thirteen_dispositions() {
    let mut entries = [CountedEntry {
        links: IntrusiveLinks::new(),
        position: WheelPosition::Unlinked,
        dispositions: 0,
    }];
    let mut wheel = TimerWheel::new(0);
    wheel.insert(&mut entries, 0);
    let mut expired = Vec::new();
    wheel.advance_to(&mut entries, u64::MAX, &mut expired);
    assert_eq!(expired, [0]);
    assert!((1..=13).contains(&entries[0].dispositions));
    assert_eq!(entries[0].wheel_position(), WheelPosition::Unlinked);
    assert!(!wheel.has_pending());
}

#[test]
fn empty_max_jump_has_no_bucket_to_process() {
    let mut wheel = TimerWheel::new(0);
    let mut entries: Vec<Entry> = Vec::new();
    let mut expired = Vec::new();
    assert_eq!(wheel.next_boundary(), None);
    wheel.advance_to(&mut entries, u64::MAX, &mut expired);
    assert_eq!(wheel.current_tick(), u64::MAX);
    assert_eq!(wheel.next_boundary(), None);
    assert!(wheel.levels.iter().flatten().all(ListHead::is_empty));
    assert!(expired.is_empty());
}
