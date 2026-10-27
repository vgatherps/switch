use super::benchmark_workloads::{Entry, entries};
use super::*;
use std::panic::{AssertUnwindSafe, catch_unwind};

fn check_structure(wheel: &TimerWheel, entries: &[Entry]) {
    let mut seen = vec![false; entries.len()];
    for (level, buckets) in wheel.levels.iter().enumerate() {
        let mut pending = 0;
        for (slot, bucket) in buckets.iter().enumerate() {
            let mut previous = NIL;
            let mut index = bucket.first;
            while index != NIL {
                let entry = &entries[index as usize];
                assert!(!seen[index as usize], "duplicate list membership");
                seen[index as usize] = true;
                assert_eq!(entry.prev(), previous);
                assert_eq!(
                    entry.wheel_position(),
                    WheelPosition::Linked {
                        level: level as u8,
                        slot: slot as u8,
                    }
                );
                previous = index;
                index = entry.next();
            }
            assert_eq!(bucket.last, previous);
            if bucket.first != NIL {
                pending |= 1 << slot;
            }
        }
        assert_eq!(wheel.pending[level], pending);
    }
    assert_eq!(
        seen.iter().filter(|&&linked| linked).count(),
        wheel.linked_count
    );
    for (index, entry) in entries.iter().enumerate() {
        assert_eq!(
            seen[index],
            entry.wheel_position() != WheelPosition::Unlinked
        );
        if !seen[index] {
            assert_eq!(entry.prev(), NIL);
            assert_eq!(entry.next(), NIL);
        }
    }
}

fn state(
    wheel: &TimerWheel,
    entries: &[Entry],
) -> (
    Vec<(u32, u32)>,
    Vec<(WheelPosition, u32, u32)>,
    Vec<u32>,
    u64,
    usize,
) {
    (
        wheel
            .levels
            .iter()
            .flatten()
            .map(|bucket| (bucket.first, bucket.last))
            .collect(),
        entries
            .iter()
            .map(|entry| (entry.wheel_position(), entry.prev(), entry.next()))
            .collect(),
        wheel.pending.to_vec(),
        wheel.current_tick(),
        wheel.linked_count,
    )
}

#[test]
fn never_inserted_cancel_preserves_occupied_bucket() {
    let mut entries = entries([0, 32]);
    let mut wheel = TimerWheel::new(0);
    assert_eq!(wheel.insert(&mut entries, 1), InsertOutcome::Linked);
    let before = state(&wheel, &entries);
    assert!(!wheel.remove(&mut entries, 0));
    assert_eq!(state(&wheel, &entries), before);
    check_structure(&wheel, &entries);
    let mut expired = Vec::new();
    wheel.advance_to(&mut entries, 32, &mut expired).unwrap();
    assert_eq!(expired, [1]);
    check_structure(&wheel, &entries);
}

#[test]
fn cancel_twice_preserves_shared_bucket_survivor() {
    let mut entries = entries([5, 5]);
    let mut wheel = TimerWheel::new(0);
    assert_eq!(wheel.insert(&mut entries, 0), InsertOutcome::Linked);
    assert_eq!(wheel.insert(&mut entries, 1), InsertOutcome::Linked);
    assert!(wheel.remove(&mut entries, 0));
    let before = state(&wheel, &entries);
    assert!(!wheel.remove(&mut entries, 0));
    assert_eq!(state(&wheel, &entries), before);
    check_structure(&wheel, &entries);
    let mut expired = Vec::new();
    wheel.advance_to(&mut entries, 5, &mut expired).unwrap();
    assert_eq!(expired, [1]);
    check_structure(&wheel, &entries);
}

#[test]
fn expired_cancel_preserves_another_timer_in_reused_bucket() {
    let mut entries = entries([3, 35]);
    let mut wheel = TimerWheel::new(0);
    assert_eq!(wheel.insert(&mut entries, 0), InsertOutcome::Linked);
    let mut expired = Vec::new();
    wheel.advance_to(&mut entries, 3, &mut expired).unwrap();
    assert_eq!(expired, [0]);
    assert_eq!(entries[0].wheel_position(), WheelPosition::Unlinked);
    wheel.advance_to(&mut entries, 32, &mut expired).unwrap();
    assert_eq!(wheel.insert(&mut entries, 1), InsertOutcome::Linked);
    assert_eq!(
        entries[1].wheel_position(),
        WheelPosition::Linked { level: 0, slot: 3 }
    );
    let before = state(&wheel, &entries);
    assert!(!wheel.remove(&mut entries, 0));
    assert_eq!(state(&wheel, &entries), before);
    check_structure(&wheel, &entries);
    wheel.advance_to(&mut entries, 35, &mut expired).unwrap();
    assert_eq!(expired, [0, 1]);
    check_structure(&wheel, &entries);
}

#[test]
fn linked_singleton_has_nil_links_and_remains_removable() {
    let mut entries = entries([5]);
    let mut wheel = TimerWheel::new(0);
    assert_eq!(wheel.insert(&mut entries, 0), InsertOutcome::Linked);
    assert_eq!(entries[0].prev(), NIL);
    assert_eq!(entries[0].next(), NIL);
    assert_eq!(
        entries[0].wheel_position(),
        WheelPosition::Linked { level: 0, slot: 5 }
    );
    check_structure(&wheel, &entries);
    assert!(wheel.remove(&mut entries, 0));
    assert_eq!(entries[0].wheel_position(), WheelPosition::Unlinked);
    assert!(!wheel.has_pending());
    check_structure(&wheel, &entries);
}

#[test]
fn duplicate_insert_panics_before_any_mutation() {
    let mut entries = entries([5, 5]);
    let mut wheel = TimerWheel::new(0);
    assert_eq!(wheel.insert(&mut entries, 0), InsertOutcome::Linked);
    assert_eq!(wheel.insert(&mut entries, 1), InsertOutcome::Linked);
    let before = state(&wheel, &entries);
    assert!(catch_unwind(AssertUnwindSafe(|| wheel.insert(&mut entries, 0))).is_err());
    assert_eq!(state(&wheel, &entries), before);
    check_structure(&wheel, &entries);
    let mut expired = Vec::new();
    wheel.advance_to(&mut entries, 5, &mut expired).unwrap();
    assert_eq!(expired, [0, 1]);
    check_structure(&wheel, &entries);
}

#[test]
fn formerly_overflowing_pair_expires_exactly_once() {
    let mut entries = entries([2097152, 2097152]);
    let mut wheel = TimerWheel::new(0);
    assert_eq!(wheel.insert(&mut entries, 0), InsertOutcome::Linked);
    assert_eq!(wheel.insert(&mut entries, 1), InsertOutcome::Linked);
    let mut expired = Vec::new();
    wheel
        .advance_to(&mut entries, 2097151, &mut expired)
        .unwrap();
    assert!(expired.is_empty());
    check_structure(&wheel, &entries);
    wheel
        .advance_to(&mut entries, 2097152, &mut expired)
        .unwrap();
    assert_eq!(expired, [0, 1]);
    assert!(!wheel.remove(&mut entries, 0));
    assert!(!wheel.remove(&mut entries, 1));
    wheel
        .advance_to(&mut entries, 2097153, &mut expired)
        .unwrap();
    assert_eq!(expired, [0, 1]);
    assert!(!wheel.has_pending());
    check_structure(&wheel, &entries);
}
