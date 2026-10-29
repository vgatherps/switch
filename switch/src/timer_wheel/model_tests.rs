use super::*;
use crate::intrusive::{IntrusiveLinks, Linked, ListLinked, NIL, Timed};

struct Entry {
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

fn entries(count: usize) -> Vec<Entry> {
    (0..count)
        .map(|_| Entry {
            links: IntrusiveLinks::new(),
            deadline: 0,
            position: WheelPosition::Unlinked,
        })
        .collect()
}

fn random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn deadline(state: &mut u64, now: u64) -> u64 {
    match random(state) % 4 {
        0 => now,
        1 => now.saturating_add(1 + random(state) % 4096),
        2 => random(state),
        _ => {
            let power = 1u64 << (random(state) % 64);
            if random(state) & 1 == 0 {
                power.checked_sub(1).expect("power is positive")
            } else {
                power.checked_add(1).expect("power plus one fits")
            }
        }
    }
}

fn check(wheel: &TimerWheel, entries: &[Entry], live: &[Option<u64>], now: u64) {
    assert_eq!(wheel.current_tick(), now);
    let count = live.iter().filter(|deadline| deadline.is_some()).count();
    assert_eq!(wheel.has_pending(), count != 0);

    let mut seen = vec![false; entries.len()];
    for (level, buckets) in wheel.levels.iter().enumerate() {
        let mut pending = 0;
        for (slot, bucket) in buckets.iter().enumerate() {
            let mut previous = NIL;
            let mut index = bucket.first;
            while index != NIL {
                let i = index as usize;
                assert!(i < entries.len());
                assert!(!seen[i]);
                seen[i] = true;
                assert_eq!(entries[i].prev(), previous);
                assert_eq!(
                    entries[i].wheel_position(),
                    WheelPosition::Linked {
                        level: level as u8,
                        slot: slot as u8,
                    }
                );
                previous = index;
                index = entries[i].next();
            }
            assert_eq!(bucket.last, previous);
            if bucket.first != NIL {
                pending |= 1 << slot;
            }
        }
        assert_eq!(wheel.pending[level], pending);
    }
    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(seen[i], live[i].is_some());
        if !seen[i] {
            assert_eq!(entry.wheel_position(), WheelPosition::Unlinked);
            assert_eq!(entry.prev(), NIL);
            assert_eq!(entry.next(), NIL);
        }
    }
}

fn insert(
    wheel: &mut TimerWheel,
    entries: &mut [Entry],
    live: &mut [Option<u64>],
    now: u64,
    index: usize,
) {
    assert!(live[index].is_none());
    assert_eq!(entries[index].wheel_position(), WheelPosition::Unlinked);
    let deadline = entries[index].deadline;
    // A due deadline violates insert's precondition; the store never inserts one.
    if deadline > now {
        wheel.insert(entries, index as u32);
        live[index] = Some(deadline);
    }
    check(wheel, entries, live, now);
}

fn advance(
    wheel: &mut TimerWheel,
    entries: &mut [Entry],
    live: &mut [Option<u64>],
    now: &mut u64,
    supplied: u64,
) -> Vec<u32> {
    // A regressed tick violates advance_to's precondition; Runtime::poll rejects it first.
    if supplied < *now {
        return Vec::new();
    }

    let mut expected: Vec<u32> = live
        .iter()
        .enumerate()
        .filter_map(|(i, deadline)| {
            deadline
                .filter(|deadline| *deadline <= supplied)
                .map(|_| i as u32)
        })
        .collect();
    let mut actual = Vec::new();
    wheel.advance_to(entries, supplied, &mut actual);
    expected.sort_unstable();
    actual.sort_unstable();
    assert_eq!(actual, expected);
    for index in expected {
        live[index as usize] = None;
    }
    *now = supplied;
    check(wheel, entries, live, *now);
    actual
}

fn run(initial: u64, seed: u64) {
    const BOUNDARIES: [u64; 12] = [
        31,
        32,
        1023,
        1024,
        32767,
        32768,
        (1 << 60) - 1,
        1 << 60,
        (1 << 60) + 1,
        u64::MAX - 64,
        u64::MAX - 1,
        u64::MAX,
    ];
    let mut deadlines = BOUNDARIES.to_vec();
    deadlines.extend([
        initial.saturating_sub(1),
        initial,
        initial.saturating_add(1),
    ]);
    deadlines.sort_unstable();
    deadlines.dedup();

    let mut wheel = TimerWheel::new(initial);
    let mut entries = entries(64);
    let mut live = vec![None; entries.len()];
    let mut now = initial;
    let mut rng = seed;
    for (i, value) in deadlines.iter().copied().enumerate() {
        entries[i].deadline = value;
        insert(&mut wheel, &mut entries, &mut live, now, i);
    }

    let reused = deadlines.len();
    entries[reused].deadline = now.checked_add(1).expect("start is below MAX");
    insert(&mut wheel, &mut entries, &mut live, now, reused);
    assert!(wheel.remove(&mut entries, reused as u32));
    live[reused] = None;
    check(&wheel, &entries, &live, now);
    assert!(!wheel.remove(&mut entries, reused as u32));
    check(&wheel, &entries, &live, now);
    entries[reused].deadline = now.checked_add(2).expect("start is below MAX");
    insert(&mut wheel, &mut entries, &mut live, now, reused);

    for _ in 0..768 {
        let choice = random(&mut rng) % 7;
        let i = random(&mut rng) as usize % entries.len();
        match choice {
            0 | 1 if live[i].is_none() => {
                entries[i].deadline = deadline(&mut rng, now);
                insert(&mut wheel, &mut entries, &mut live, now, i);
            }
            2 => {
                assert_eq!(wheel.remove(&mut entries, i as u32), live[i].is_some());
                live[i] = None;
                check(&wheel, &entries, &live, now);
            }
            3 if live[i].is_none() => {
                assert_eq!(entries[i].wheel_position(), WheelPosition::Unlinked);
                entries[i].deadline = deadline(&mut rng, now);
            }
            4 => {
                let supplied = now;
                advance(&mut wheel, &mut entries, &mut live, &mut now, supplied);
            }
            5 if now > 0 => {
                let supplied = now - 1;
                advance(&mut wheel, &mut entries, &mut live, &mut now, supplied);
            }
            _ => {
                let target = now
                    .saturating_add(1 + random(&mut rng) % 4096)
                    .min(u64::MAX - 1);
                advance(&mut wheel, &mut entries, &mut live, &mut now, target);
            }
        }
        check(&wheel, &entries, &live, now);
    }

    let first_boundary = now;
    for target in deadlines
        .into_iter()
        .filter(|target| *target >= first_boundary)
    {
        let expired = advance(&mut wheel, &mut entries, &mut live, &mut now, target);
        for i in expired {
            let i = i as usize;
            if now < u64::MAX {
                entries[i].deadline = now.checked_add(1).expect("now is below MAX");
                insert(&mut wheel, &mut entries, &mut live, now, i);
            }
        }
        check(&wheel, &entries, &live, now);
    }
    advance(&mut wheel, &mut entries, &mut live, &mut now, u64::MAX);
}

#[test]
fn seeded_timer_wheel_matches_independent_deadline_scan() {
    for initial in [0, 31, (1 << 60) + 123, u64::MAX - 64] {
        for seed in [0x5eed, 0xc0ffee, 0x7f4a_7c15_9e37_79b9] {
            run(initial, seed);
        }
    }
}
