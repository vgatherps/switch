use super::*;
use crate::task::TaskBox;
use std::future::pending;
use std::panic::{AssertUnwindSafe, catch_unwind};

struct Fixture {
    store: TimerStore,
    tasks: Vec<*mut TaskHeader>,
}
impl Fixture {
    fn new() -> Self {
        let tasks = (0..3)
            .map(|_| TaskBox::new(std::ptr::null_mut(), pending()).into_raw())
            .collect();
        Self {
            store: TimerStore::new(0, 0),
            tasks,
        }
    }
    fn register(&mut self, task: usize, tick: u64) -> Token {
        unsafe { self.store.register(self.tasks[task], tick).unwrap() }
    }
    fn cancel(&mut self, token: Token) -> bool {
        unsafe { self.store.cancel(token) }
    }
    fn retire(&mut self, task: usize) {
        unsafe {
            self.store.retire(self.tasks[task]);
        }
    }
    fn head(&self, task: usize) -> u32 {
        unsafe { (*self.tasks[task]).reg_head }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        while self.store.pop_expired().is_some() {}
        for &task in &self.tasks {
            unsafe {
                self.store.retire(task);
                TaskHeader::drop_future(task);
                TaskHeader::free(task);
            }
        }
    }
}

#[test]
fn two_memberships_cancel_independently_and_retire_forgotten_pending() {
    let mut f = Fixture::new();
    let a = f.register(0, 10);
    let b = f.register(0, 20);
    let c = f.register(1, 10);
    assert_eq!(f.head(0), b.index);
    assert_eq!(f.store.entries[b.index as usize].owner_next, a.index);
    assert!(f.cancel(a));
    assert_eq!(f.head(0), b.index);
    f.retire(0);
    assert_eq!(f.head(0), NIL);
    assert!(!f.cancel(a));
    assert!(!f.cancel(b));
    assert!(f.store.valid(c));
    f.store.advance_to(20).unwrap();
    assert_eq!(f.store.pop_expired(), Some(f.tasks[1]));
    assert_eq!(f.store.pop_expired(), None);
}

#[test]
fn reuse_invalidates_old_cancel_and_never_wraps_max_version() {
    let mut f = Fixture::new();
    let old = f.register(0, 10);
    assert!(f.cancel(old));
    let replacement = f.register(1, 20);
    assert_eq!(old.index, replacement.index);
    assert_eq!(replacement.version, old.version + 1);
    assert!(!f.cancel(old));
    assert!(f.store.valid(replacement));
    assert!(f.cancel(replacement));
    f.store.entries[old.index as usize].version = u64::MAX - 1;
    let last = f.register(0, 30);
    assert_eq!(last.version, u64::MAX);
    assert!(f.cancel(last));
    assert_eq!(f.store.entries[last.index as usize].state, State::Retired);
    let next = f.register(0, 40);
    assert_ne!(last.index, next.index);
    assert_eq!(f.store.entries[last.index as usize].version, u64::MAX);
    assert!(!f.cancel(last));
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
fn refresh_moves_owner_before_retirement_and_stale_token_reregisters_after() {
    let mut f = Fixture::new();
    let token = f.register(0, 10);
    assert!(unsafe { f.store.refresh(token, f.tasks[1]) });
    assert_eq!(f.head(0), NIL);
    assert_eq!(f.head(1), token.index);
    assert_eq!(f.store.entries[token.index as usize].deadline, 10);
    f.retire(0);
    assert!(f.store.valid(token));
    f.retire(1);
    assert!(!unsafe { f.store.refresh(token, f.tasks[2]) });
    let new = f.register(2, 10);
    assert_ne!(token, new);
    f.store.advance_to(10).unwrap();
    assert_eq!(f.store.pop_expired(), Some(f.tasks[2]));
    assert_eq!(f.store.pop_expired(), None);
}

#[test]
fn expired_keeps_owner_without_wake_until_cancel_transfer_or_retire() {
    let mut f = Fixture::new();
    let a = f.register(0, 10);
    let b = f.register(0, 10);
    f.store.advance_to(10).unwrap();
    assert_eq!(f.store.pop_expired(), Some(f.tasks[0]));
    assert_eq!(f.store.pop_expired(), Some(f.tasks[0]));
    assert_eq!(f.store.pop_expired(), None);
    assert_eq!(f.head(0), b.index);
    for t in [a, b] {
        assert_eq!(f.store.entries[t.index as usize].state, State::Expired);
        assert!(f.store.entries[t.index as usize].wake.is_none());
    }
    assert!(unsafe { f.store.refresh(a, f.tasks[1]) });
    assert!(f.store.entries[a.index as usize].wake.is_none());
    f.retire(0);
    assert!(f.store.valid(a));
    assert!(!f.store.valid(b));
    assert!(f.cancel(a));
    assert_eq!(f.head(1), NIL);
}

#[test]
fn staged_handoff_blocks_reuse_before_any_mutation() {
    let mut f = Fixture::new();
    let token = f.register(0, 10);
    f.store.advance_to(10).unwrap();
    let entry_len = f.store.entries.len();
    assert!(catch_unwind(AssertUnwindSafe(|| f.cancel(token))).is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| f.register(1, 20))).is_err());
    assert!(
        catch_unwind(AssertUnwindSafe(|| unsafe {
            f.store.refresh(token, f.tasks[1])
        }))
        .is_err()
    );
    assert!(catch_unwind(AssertUnwindSafe(|| f.retire(0))).is_err());
    assert_eq!(f.store.entries.len(), entry_len);
    assert_eq!(f.head(0), token.index);
    assert_eq!(f.store.pop_expired(), Some(f.tasks[0]));
    assert!(f.cancel(token));
}

#[test]
fn full_range_equal_and_regressed_time_preserve_identity_and_membership() {
    let mut f = Fixture::new();
    let first = f.register(0, 1);
    let last = f.register(1, u64::MAX);
    f.store.advance_to(0).unwrap();
    assert!(f.store.expired.is_empty());
    f.store.advance_to(1).unwrap();
    assert_eq!(f.store.pop_expired(), Some(f.tasks[0]));
    let old_position = f.store.entries[last.index as usize].position;
    assert_eq!(
        f.store.advance_to(0),
        Err(TimeRegressed {
            current: 1,
            supplied: 0
        })
    );
    assert_eq!(f.store.entries[last.index as usize].position, old_position);
    assert!(f.store.valid(first));
    assert!(f.store.valid(last));
    assert_eq!(f.head(1), last.index);
    f.store.advance_to(u64::MAX).unwrap();
    assert_eq!(f.store.pop_expired(), Some(f.tasks[1]));
    assert_eq!(f.store.pop_expired(), None);
    f.store.advance_to(u64::MAX).unwrap();
    assert!(f.store.expired.is_empty());
}

#[test]
fn due_or_terminal_registration_rejects_before_publication() {
    let mut f = Fixture::new();
    assert!(catch_unwind(AssertUnwindSafe(|| f.register(0, 0))).is_err());
    unsafe {
        (*f.tasks[0]).state = TaskState::Terminal;
    }
    assert!(catch_unwind(AssertUnwindSafe(|| f.register(0, 10))).is_err());
    assert!(f.store.entries.is_empty());
    assert_eq!(f.head(0), NIL);
}

#[test]
fn moving_store_and_growing_entries_preserves_stable_task_aliases() {
    let mut f = Fixture::new();
    let first = f.register(0, 10);
    let mut f = Box::new(f);
    for i in 0..256 {
        f.register(i % 3, 20 + i as u64);
    }
    assert!(f.store.expired.capacity() >= f.store.entries.len());
    assert!(unsafe { f.store.refresh(first, f.tasks[2]) });
    f.retire(0);
    f.retire(1);
    f.retire(2);
    assert!(
        f.store
            .entries
            .iter()
            .all(|e| e.state == State::Free && e.wake.is_none() && e.owner.is_null())
    );
    let mut free = f.store.free_head;
    let mut seen = vec![false; f.store.entries.len()];
    while free != NIL {
        assert!(!seen[free as usize]);
        seen[free as usize] = true;
        free = f.store.entries[free as usize].owner_next;
    }
    assert!(seen.into_iter().all(|x| x));
}
