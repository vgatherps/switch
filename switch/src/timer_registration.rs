//! Versioned timer subscriptions with independent wheel and task membership.

use crate::intrusive::{IntrusiveLinks, Linked, NIL, Timed};
use crate::task::{TaskHeader, TaskState};
use crate::timer_wheel::{InsertOutcome, TimeRegressed, TimerWheel, WheelEntry, WheelPosition};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Token {
    index: u32,
    version: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RegistrationExhausted;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Pending,
    Expired,
    Free,
    Retired,
}

struct Entry {
    wheel_links: IntrusiveLinks,
    position: WheelPosition,
    deadline: u64,
    version: u64,
    state: State,
    owner_prev: u32,
    owner_next: u32,
    owner: *mut TaskHeader,
    wake: Option<*mut TaskHeader>,
}

impl Linked for Entry {
    fn links(&self) -> &IntrusiveLinks {
        &self.wheel_links
    }
    fn links_mut(&mut self) -> &mut IntrusiveLinks {
        &mut self.wheel_links
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

pub(crate) struct TimerStore {
    wheel: TimerWheel,
    entries: Vec<Entry>,
    expired: Vec<u32>,
    free_head: u32,
}

impl TimerStore {
    pub(crate) fn current_tick(&self) -> u64 {
        self.wheel.current_tick()
    }

    pub(crate) fn new(tick: u64, capacity: usize) -> Self {
        Self {
            wheel: TimerWheel::new(tick),
            entries: Vec::with_capacity(capacity),
            expired: Vec::with_capacity(capacity),
            free_head: NIL,
        }
    }

    fn checked_index(len: usize) -> Result<u32, RegistrationExhausted> {
        let index = u32::try_from(len).map_err(|_| RegistrationExhausted)?;
        if index == NIL {
            Err(RegistrationExhausted)
        } else {
            Ok(index)
        }
    }

    fn drained(&self) {
        assert!(self.expired.is_empty(), "timer expiry handoff not drained");
    }

    fn valid(&self, token: Token) -> bool {
        self.entries.get(token.index as usize).is_some_and(|entry| {
            entry.version == token.version && matches!(entry.state, State::Pending | State::Expired)
        })
    }

    /// # Safety
    /// Stable live task on this store's owner thread. Its reg_head belongs
    /// exclusively to this store; retire here before freeing the task.
    pub(crate) unsafe fn register(
        &mut self,
        owner: *mut TaskHeader,
        deadline: u64,
    ) -> Result<Token, RegistrationExhausted> {
        self.drained();
        assert!(
            deadline > self.wheel.current_tick(),
            "registration deadline is due"
        );
        unsafe {
            assert!(
                (*owner).state != TaskState::Terminal,
                "terminal timer owner"
            );
        }
        let index = if self.free_head != NIL {
            let index = self.free_head;
            self.free_head = self.entries[index as usize].owner_next;
            self.entries[index as usize].version += 1;
            index
        } else {
            let index = Self::checked_index(self.entries.len())?;
            self.entries.reserve(1);
            let needed = self.entries.len() + 1;
            if self.expired.capacity() < needed {
                self.expired.reserve(needed - self.expired.len());
            }
            self.entries.push(Entry {
                wheel_links: IntrusiveLinks::new(),
                position: WheelPosition::Unlinked,
                deadline,
                version: 1,
                state: State::Free,
                owner_prev: NIL,
                owner_next: NIL,
                owner: std::ptr::null_mut(),
                wake: None,
            });
            index
        };
        let entry = &mut self.entries[index as usize];
        entry.deadline = deadline;
        entry.state = State::Pending;
        entry.wake = Some(owner);
        unsafe {
            self.attach(index, owner);
        }
        assert_eq!(
            self.wheel.insert(&mut self.entries, index),
            InsertOutcome::Linked
        );
        Ok(Token {
            index,
            version: self.entries[index as usize].version,
        })
    }

    /// # Safety
    /// Token originated in this store (possibly stale). Every referenced task
    /// remains stable, and its reg_head belongs exclusively to this store.
    /// Call on the owner thread with the expiry handoff fully drained.
    pub(crate) unsafe fn cancel(&mut self, token: Token) -> bool {
        self.drained();
        if !self.valid(token) {
            return false;
        }
        unsafe {
            self.release(token.index);
        }
        true
    }

    /// # Safety
    /// Token originated in this store (possibly stale). New owner is a stable
    /// live task on the owner thread; both task reg_heads belong exclusively
    /// to this store. All referenced tasks remain alive; handoff is drained.
    pub(crate) unsafe fn refresh(&mut self, token: Token, owner: *mut TaskHeader) -> bool {
        self.drained();
        if !self.valid(token) {
            return false;
        }
        unsafe {
            assert!(
                (*owner).state != TaskState::Terminal,
                "terminal timer owner"
            );
            if self.entries[token.index as usize].owner != owner {
                self.detach(token.index);
                self.attach(token.index, owner);
            }
        }
        let entry = &mut self.entries[token.index as usize];
        if entry.state == State::Pending {
            entry.wake = Some(owner);
        }
        true
    }

    /// No allocations or user callbacks.
    /// # Safety
    /// Stable task on the owner thread, with reg_head owned exclusively by this
    /// store. Every referenced task remains alive and the handoff is drained.
    pub(crate) unsafe fn retire(&mut self, owner: *mut TaskHeader) {
        self.drained();
        unsafe {
            while (*owner).reg_head != NIL {
                self.release((*owner).reg_head);
            }
        }
    }

    /// Stage without callbacks. Drain completely before mutation or user code.
    pub(crate) fn advance_to(&mut self, tick: u64) -> Result<(), TimeRegressed> {
        self.drained();
        self.wheel
            .advance_to(&mut self.entries, tick, &mut self.expired)
    }

    /// Normalize before returning a copied wake; no entry borrow escapes.
    pub(crate) fn pop_expired(&mut self) -> Option<*mut TaskHeader> {
        let index = self.expired.pop()?;
        let entry = &mut self.entries[index as usize];
        entry.state = State::Expired;
        Some(entry.wake.take().expect("pending timer has no wake"))
    }

    unsafe fn attach(&mut self, index: u32, owner: *mut TaskHeader) {
        unsafe {
            let next = (*owner).reg_head;
            let entry = &mut self.entries[index as usize];
            entry.owner = owner;
            entry.owner_prev = NIL;
            entry.owner_next = next;
            if next != NIL {
                self.entries[next as usize].owner_prev = index;
            }
            (*owner).reg_head = index;
        }
    }

    unsafe fn detach(&mut self, index: u32) {
        unsafe {
            let entry = &self.entries[index as usize];
            let (owner, prev, next) = (entry.owner, entry.owner_prev, entry.owner_next);
            if prev == NIL {
                (*owner).reg_head = next;
            } else {
                self.entries[prev as usize].owner_next = next;
            }
            if next != NIL {
                self.entries[next as usize].owner_prev = prev;
            }
            let entry = &mut self.entries[index as usize];
            entry.owner = std::ptr::null_mut();
            entry.owner_prev = NIL;
            entry.owner_next = NIL;
        }
    }

    unsafe fn release(&mut self, index: u32) {
        self.wheel.remove(&mut self.entries, index);
        unsafe {
            self.detach(index);
        }
        let entry = &mut self.entries[index as usize];
        entry.wake = None;
        if entry.version == u64::MAX {
            entry.state = State::Retired;
        } else {
            entry.state = State::Free;
            entry.owner_next = self.free_head;
            self.free_head = index;
        }
    }
}

#[cfg(test)]
#[path = "timer_registration/tests.rs"]
mod tests;
