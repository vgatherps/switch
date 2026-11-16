//! Versioned timer subscriptions: each records the task its expiry wakes.

use crate::intrusive::{IntrusiveLinks, Linked, NIL, Timed};
use crate::task::TaskHeader;
use crate::timer_wheel::{TimerWheel, WheelEntry, WheelPosition};

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
}

struct Entry {
    wheel_links: IntrusiveLinks,
    position: WheelPosition,
    deadline: u64,
    version: u64,
    state: State,
    /// Free-list link while `Free`.
    next_free: u32,
    /// The task the expiry wakes: the last task that polled the Sleep.
    owner: *const TaskHeader,
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

    pub(crate) fn new(tick: u64) -> Self {
        Self {
            wheel: TimerWheel::new(tick),
            entries: Vec::new(),
            expired: Vec::new(),
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
        debug_assert!(self.expired.is_empty(), "timer expiry handoff not drained");
    }

    fn valid(&self, token: Token) -> bool {
        self.entries.get(token.index as usize).is_some_and(|entry| {
            entry.version == token.version && matches!(entry.state, State::Pending | State::Expired)
        })
    }

    /// # Safety
    /// `owner` heads a task slot of this store's executor; its expiry wakes it,
    /// possibly after the task ended (a free slot ignores the wake).
    pub(crate) unsafe fn register(
        &mut self,
        owner: *const TaskHeader,
        deadline: u64,
    ) -> Result<Token, RegistrationExhausted> {
        self.drained();
        debug_assert!(
            deadline > self.wheel.current_tick(),
            "registration deadline is due"
        );
        let index = if self.free_head != NIL {
            let index = self.free_head;
            self.free_head = self.entries[index as usize].next_free;
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
                next_free: NIL,
                owner: std::ptr::null(),
            });
            index
        };
        let entry = &mut self.entries[index as usize];
        entry.deadline = deadline;
        entry.state = State::Pending;
        entry.owner = owner;
        self.wheel.insert(&mut self.entries, index);
        Ok(Token {
            index,
            version: self.entries[index as usize].version,
        })
    }

    /// Token originated in this store (possibly stale). Call with the expiry
    /// handoff fully drained.
    pub(crate) fn cancel(&mut self, token: Token) -> bool {
        self.drained();
        if !self.valid(token) {
            return false;
        }
        self.release(token.index);
        true
    }

    /// # Safety
    /// Token originated in this store (possibly stale); `owner` as for
    /// [`Self::register`]. Handoff is drained.
    pub(crate) unsafe fn refresh(&mut self, token: Token, owner: *const TaskHeader) -> bool {
        self.drained();
        if !self.valid(token) {
            return false;
        }
        self.entries[token.index as usize].owner = owner;
        true
    }

    /// Stage without callbacks. Drain completely before mutation or user code.
    pub(crate) fn advance_to(&mut self, tick: u64) {
        self.drained();
        self.wheel
            .advance_to(&mut self.entries, tick, &mut self.expired);
    }

    /// Marks every timer `advance_to` staged Expired and hands its task to `wake`, in
    /// deadline order. `wake` must not touch this store.
    #[inline]
    pub(crate) fn wake_expired(&mut self, mut wake: impl FnMut(*const TaskHeader)) {
        if self.expired.is_empty() {
            return;
        }
        for &index in &self.expired {
            let entry = &mut self.entries[index as usize];
            debug_assert_eq!(entry.state, State::Pending);
            entry.state = State::Expired;
            wake(entry.owner);
        }
        self.expired.clear();
    }

    fn release(&mut self, index: u32) {
        self.wheel.remove(&mut self.entries, index);
        let entry = &mut self.entries[index as usize];
        entry.state = State::Free;
        entry.next_free = self.free_head;
        self.free_head = index;
    }
}

#[cfg(test)]
#[path = "timer_registration/tests.rs"]
mod tests;
