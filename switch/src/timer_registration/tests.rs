use super::*;
use crate::task::block::TestBlock;
use std::panic::{AssertUnwindSafe, catch_unwind};

struct Fixture {
    store: TimerStore,
    tasks: Vec<*const TaskHeader>,
    _block: TestBlock,
}
impl Fixture {
    fn new() -> Self {
        let block = TestBlock::new();
        let tasks = (0..3).map(|position| block.task(position)).collect();
        Self {
            store: TimerStore::new(0, 0),
            tasks,
            _block: block,
        }
    }
    fn task(&self, task: usize) -> *const TaskHeader {
        self.tasks[task]
    }
    fn register(&mut self, task: usize, tick: u64) -> Token {
        unsafe { self.store.register(self.tasks[task], tick).unwrap() }
    }
    fn refresh(&mut self, token: Token, task: usize) -> bool {
        unsafe { self.store.refresh(token, self.tasks[task]) }
    }
    fn cancel(&mut self, token: Token) -> bool {
        self.store.cancel(token)
    }
}

#[test]
fn reuse_invalidates_old_cancel() {
    let mut f = Fixture::new();
    let old = f.register(0, 10);
    assert!(f.cancel(old));
    let replacement = f.register(1, 20);
    assert_eq!(old.index, replacement.index);
    assert_eq!(replacement.version, old.version + 1);
    assert!(!f.cancel(old));
    assert!(f.store.valid(replacement));
    assert!(f.cancel(replacement));
}

#[test]
fn checked_index_rejects_sentinel_and_overflow() {
    assert_eq!(TimerStore::checked_index(NIL as usize - 1), Ok(NIL - 1));
    assert_eq!(
        TimerStore::checked_index(NIL as usize),
        Err(RegistrationExhausted)
    );
    assert_eq!(
        TimerStore::checked_index(usize::MAX),
        Err(RegistrationExhausted)
    );
}

#[test]
fn refresh_moves_the_woken_task_and_keeps_the_deadline() {
    let mut f = Fixture::new();
    let token = f.register(0, 10);
    assert!(f.refresh(token, 1));
    assert_eq!(f.store.entries[token.index as usize].deadline, 10);
    f.store.advance_to(10);
    assert_eq!(f.store.pop_expired(), Some(f.task(1)));
    assert_eq!(f.store.pop_expired(), None);
    assert!(f.cancel(token));
    assert!(!f.refresh(token, 2));
}

#[test]
fn expired_stays_valid_until_cancel() {
    let mut f = Fixture::new();
    let a = f.register(0, 10);
    let b = f.register(0, 10);
    f.store.advance_to(10);
    assert_eq!(f.store.pop_expired(), Some(f.task(0)));
    assert_eq!(f.store.pop_expired(), Some(f.task(0)));
    assert_eq!(f.store.pop_expired(), None);
    for t in [a, b] {
        assert_eq!(f.store.entries[t.index as usize].state, State::Expired);
    }
    assert!(f.refresh(a, 1));
    assert_eq!(f.store.entries[a.index as usize].state, State::Expired);
    assert!(f.cancel(a));
    assert!(!f.store.valid(a));
    assert!(f.store.valid(b));
}

#[test]
#[cfg(debug_assertions)]
fn staged_handoff_blocks_reuse_before_any_mutation() {
    let mut f = Fixture::new();
    let token = f.register(0, 10);
    f.store.advance_to(10);
    let entry_len = f.store.entries.len();
    assert!(catch_unwind(AssertUnwindSafe(|| f.cancel(token))).is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| f.register(1, 20))).is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| f.refresh(token, 1))).is_err());
    assert_eq!(f.store.entries.len(), entry_len);
    assert_eq!(f.store.pop_expired(), Some(f.task(0)));
    assert!(f.cancel(token));
}

#[test]
fn full_range_and_equal_time_preserve_identity() {
    let mut f = Fixture::new();
    let first = f.register(0, 1);
    let last = f.register(1, u64::MAX);
    f.store.advance_to(0);
    assert!(f.store.expired.is_empty());
    f.store.advance_to(1);
    assert_eq!(f.store.pop_expired(), Some(f.task(0)));
    assert!(f.store.valid(first));
    assert!(f.store.valid(last));
    f.store.advance_to(u64::MAX);
    assert_eq!(f.store.pop_expired(), Some(f.task(1)));
    assert_eq!(f.store.pop_expired(), None);
    f.store.advance_to(u64::MAX);
    assert!(f.store.expired.is_empty());
}

#[test]
#[cfg(debug_assertions)]
fn due_registration_rejects_before_publication() {
    let mut f = Fixture::new();
    assert!(catch_unwind(AssertUnwindSafe(|| f.register(0, 0))).is_err());
    assert!(f.store.entries.is_empty());
}

#[test]
fn cancelled_entries_all_return_to_the_free_list() {
    let mut f = Fixture::new();
    let tokens: Vec<_> = (0..256).map(|i| f.register(i % 3, 20 + i as u64)).collect();
    for &token in &tokens {
        assert!(f.cancel(token));
    }
    assert!(f.store.entries.iter().all(|e| e.state == State::Free));
    let mut free = f.store.free_head;
    let mut seen = vec![false; f.store.entries.len()];
    while free != NIL {
        assert!(!seen[free as usize]);
        seen[free as usize] = true;
        free = f.store.entries[free as usize].next_free;
    }
    assert!(seen.into_iter().all(|x| x));
}
