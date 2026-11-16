//! Single-threaded async executor.
//!
//! Uses raw pointers and UnsafeCell to avoid RefCell overhead.
//! All access is single-threaded, enforced by design.
//!
//! All Executor methods take `*mut Self` (not `&mut self`) to avoid Stacked
//! Borrows violations: block metadata stores the executor pointer, and
//! `&mut self` borrows would invalidate those stored pointers.

use std::any::TypeId;
use std::cell::Cell;
use std::mem::ManuallyDrop;

use crate::__private::Task;
use crate::oneshot::Sender;
use crate::remote::Mailbox;
use crate::task::SpawnToken;
use crate::task::TaskHeader;
use crate::task::block::{BlockSpec, Meta};
use crate::task::header::{DEFERRED, DROP_STARTED, LIVE, QUEUED, TAKEN};
use crate::task::pool::RuntimePool;
use crate::timer_registration::TimerStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lifecycle {
    Open,
    Panicked,
    Closing,
}

/// Calling it marks a branch unlikely (`std::hint::cold_path` is unstable).
#[cold]
#[inline]
fn unlikely() {}

/// `Runtime::poll` starts another round only while fewer task polls than this
/// were made. The check runs between rounds and a started round always
/// finishes, so one call makes at most `POLL_BUDGET - 1` polls plus the length
/// of its last round.
pub(crate) const POLL_BUDGET: usize = 1024;

/// Single-threaded executor.
///
/// Three queues: wakes go to `incoming`, deferred wakes to `deferred` (appended
/// to `incoming` when a `Runtime::poll` starts), and each round swaps `incoming`
/// into `run_queue` and polls it from `cursor`. A turn request (`request_turn`)
/// stops the round after a task poll; the next `Runtime::poll` finishes
/// `run_queue[cursor..]` first. Simple iteration, no VecDeque.
pub struct Executor {
    /// Tasks to poll this round, by header; `run_queue[cursor..]` are still to
    /// poll, and keep QUEUED until then.
    run_queue: Vec<*const TaskHeader>,
    cursor: usize,
    /// Incoming wakes (spawns and wakes during poll).
    incoming: Vec<*const TaskHeader>,
    /// Tasks woken with `StoredWaker::wake_deferred`; moved into `incoming` at
    /// the start of the next `Runtime::poll`.
    deferred: Vec<*const TaskHeader>,
    /// Number of live tasks.
    task_count: usize,
    /// One pool per task function this executor has spawned.
    pools: Vec<*mut RuntimePool>,
    /// Spawns between slot reservation and commit (user factory code runs between).
    spawning: usize,
    active: bool,
    lifecycle: Lifecycle,
    /// The task being polled, for `Sleep`; null between polls.
    pub(crate) current: *const TaskHeader,
    /// Set by `request_turn` during a task poll: the drain stops after that poll
    /// and clears it.
    turn_requested: bool,
    pub(crate) timers: TimerStore,
    /// Wake requests from other threads; outlives this executor.
    mailbox: *const Mailbox,
}

impl Executor {
    pub(crate) fn new(initial_tick: u64) -> Self {
        Self {
            run_queue: Vec::new(),
            cursor: 0,
            incoming: Vec::new(),
            deferred: Vec::new(),
            task_count: 0,
            pools: Vec::new(),
            spawning: 0,
            active: false,
            lifecycle: Lifecycle::Open,
            current: std::ptr::null(),
            turn_requested: false,
            timers: TimerStore::new(initial_tick),
            mailbox: Mailbox::create(),
        }
    }

    /// Stable owner-thread pointer to a live executor.
    #[inline(always)]
    pub(crate) unsafe fn mailbox(this: *mut Self) -> *const Mailbox {
        unsafe { (*this).mailbox }
    }

    /// Stable owner-thread pointer to a live executor.
    pub(crate) unsafe fn lifecycle(this: *mut Self) -> Lifecycle {
        unsafe { (*this).lifecycle }
    }

    /// Spawns between slot reservation and commit. Stable owner-thread pointer.
    pub(crate) unsafe fn spawning(this: *mut Self) -> usize {
        unsafe { (*this).spawning }
    }

    /// Reject new work, turns and remote wakes ahead of a deferred cleanup.
    /// Stable owner-thread pointer.
    pub(crate) unsafe fn close(this: *mut Self) {
        unsafe {
            (*this).lifecycle = Lifecycle::Closing;
            Mailbox::close((*this).mailbox);
        }
    }

    /// Spawn a task.
    ///
    /// # Safety
    /// Stable owner-thread pointer; owner remains alive through all task uses.
    pub unsafe fn spawn<T: Task>(this: *mut Self, token: SpawnToken<T>, join: Option<Sender<T::Output>>) {
        unsafe {
            assert!((*this).lifecycle == Lifecycle::Open, "executor unavailable");
            token.into_payload().spawn_in(this, join);
        }
    }

    /// Reserve a slot for one spawn. The slot stays free of LIVE until
    /// [`Reservation::commit`], so stale wakes of it do nothing.
    ///
    /// # Safety
    /// Owner thread; open executor; `spec` is the shape of `key`'s future type.
    pub(crate) unsafe fn reserve(this: *mut Self, key: TypeId, spec: &BlockSpec) -> Reservation {
        unsafe {
            let pool = match (*this)
                .pools
                .iter()
                .copied()
                .find(|&pool| RuntimePool::key(pool) == key)
            {
                Some(pool) => pool,
                None => {
                    let pool = RuntimePool::new(this, key);
                    (*this).pools.push(pool);
                    pool
                }
            };
            let header = RuntimePool::reserve(pool, spec);
            (*this).spawning += 1;
            Reservation {
                executor: this,
                header,
            }
        }
    }

    /// Wake after the owner check: task flags first, then the executor.
    ///
    /// # Safety
    /// Owner thread; `header` heads a slot of a block.
    #[inline]
    pub(crate) unsafe fn wake(header: *const TaskHeader) {
        unsafe {
            let flags = TaskHeader::flags(header);
            let current = flags.get();
            if current & (LIVE | QUEUED | DEFERRED) != LIVE {
                // Laid out off the fresh-wake path.
                unlikely();
                if current & (LIVE | QUEUED | DEFERRED) == LIVE | QUEUED | DEFERRED {
                    Self::upgrade(header);
                }
                return;
            }
            // A LIVE slot's executor is alive: cleanup clears every slot's flags
            // before the executor is freed, after which `Meta::executor` dangles.
            // A Panicked or Closing executor still takes the wake: its queues are
            // never polled again and are freed with it.
            let this = Meta::executor(TaskHeader::meta(header));
            flags.set(current | QUEUED);
            (*this).incoming.push(header);
        }
    }

    /// An ordinary wake of a deferred task: its entry moves from `deferred` to
    /// `incoming`, so it runs in this poll's next round. The other deferred
    /// tasks keep their order. A task whose entry already left `deferred` at the
    /// start of this poll is in the current round: nothing to do.
    ///
    /// # Safety
    /// As for [`Self::wake`]; the task is LIVE, QUEUED and DEFERRED.
    #[cold]
    #[inline(never)]
    unsafe fn upgrade(header: *const TaskHeader) {
        unsafe {
            // Alive, and any lifecycle takes the move, as in `wake`.
            let this = Meta::executor(TaskHeader::meta(header));
            let deferred = &mut (*this).deferred;
            let Some(position) = deferred.iter().position(|&entry| entry == header) else {
                return;
            };
            deferred.remove(position);
            let flags = TaskHeader::flags(header);
            flags.set(flags.get() & !DEFERRED);
            (*this).incoming.push(header);
        }
    }

    /// As [`Self::wake`], into `deferred`: the task runs in the next poll. A
    /// task already queued (eagerly or deferred) stays as it is.
    ///
    /// # Safety
    /// Owner thread; `header` heads a slot of a block.
    #[inline]
    pub(crate) unsafe fn wake_deferred(header: *const TaskHeader) {
        unsafe {
            let flags = TaskHeader::flags(header);
            let current = flags.get();
            if current & (LIVE | QUEUED) != LIVE {
                return;
            }
            // Alive, and any lifecycle takes the wake, as in `wake`.
            let this = Meta::executor(TaskHeader::meta(header));
            flags.set(current | QUEUED | DEFERRED);
            (*this).deferred.push(header);
        }
    }

    /// Move deferred wakes to the end of `incoming`, at the start of a poll: they
    /// run after the tasks woken since the last poll.
    ///
    /// # Safety
    /// Stable owner-thread pointer to a live executor.
    #[inline]
    pub(crate) unsafe fn undefer(this: *mut Self) {
        unsafe {
            if !(*this).deferred.is_empty() {
                // DEFERRED stays set until the task's round clears it with QUEUED.
                (*this).incoming.append(&mut (*this).deferred);
            }
        }
    }

    /// Stable owner-thread pointer to a live executor; rejects before mutation.
    pub(crate) unsafe fn check_turn(this: *mut Self) {
        unsafe {
            assert!(!(*this).active, "executor reentrancy");
            assert!((*this).lifecycle == Lifecycle::Open, "executor unavailable");
        }
    }

    /// Caller checked inactive state and the operation's lifecycle preconditions.
    /// Stable owner-thread pointer must remain live until the guard is dropped.
    pub(crate) unsafe fn enter_turn(this: *mut Self) -> TurnGuard {
        unsafe {
            (*this).active = true;
        }
        TurnGuard {
            owner: this,
            finished: false,
            outer: POLLING.replace(this),
        }
    }

    /// Finish the round a turn request stopped, then poll rounds: each round
    /// polls a snapshot of `incoming`, and another round follows while `incoming`
    /// is not empty and fewer than [`POLL_BUDGET`] polls were made (the budget
    /// never cuts a round short). A turn request from a task stops before the
    /// next task poll.
    ///
    /// Guard owns the active turn; expiry handoff is fully normalized.
    pub(crate) unsafe fn poll_batch(turn: &mut TurnGuard) -> usize {
        unsafe {
            let this = turn.owner;
            let mut polled = 0;
            if !(*this).run_queue.is_empty() {
                polled += Self::drain(this);
            }
            // A drain that saw the request returned with `run_queue` not cleared.
            while (*this).run_queue.is_empty() && !(*this).incoming.is_empty() && polled < POLL_BUDGET {
                std::mem::swap(&mut (*this).run_queue, &mut (*this).incoming);
                polled += Self::drain(this);
            }
            polled
        }
    }

    /// Poll `run_queue` from `cursor` until its end, which clears it, or until a
    /// task poll requested a turn (cleared here); returns the futures polled.
    ///
    /// # Safety
    /// As for [`Self::poll_batch`].
    unsafe fn drain(this: *mut Self) -> usize {
        unsafe {
            let mut polled = 0;
            let batch_len = (*this).run_queue.len();
            // The cursor stays local and is stored only on an early return: task code
            // cannot reach it, and an unwind leaves the executor Panicked, whose
            // cleanup resets it.
            let mut cursor = (*this).cursor;
            while cursor < batch_len {
                let index = cursor;
                cursor += 1;
                // Wakes and spawns go to `incoming`, never to this Vec; still, no
                // buffer pointer spans user code.
                let header = *(*this).run_queue.as_ptr().add(index);
                let flags = TaskHeader::flags(header);
                let current = flags.get() & !(QUEUED | DEFERRED);
                flags.set(current);
                if current & LIVE == 0 {
                    RuntimePool::release(header);
                    continue;
                }
                polled += 1;
                // Published for `Sleep`; if the poll unwinds, `TurnGuard` ends the task.
                (*this).current = header;
                let result = Meta::poll_fn(TaskHeader::meta(header))(header);
                (*this).current = std::ptr::null();
                if result.is_ready() {
                    Self::complete(this, header);
                }
                if (*this).turn_requested {
                    // Possibly at the end: the next drain then only clears.
                    (*this).turn_requested = false;
                    (*this).cursor = cursor;
                    return polled;
                }
            }
            (*this).run_queue.clear();
            (*this).cursor = 0;
            polled
        }
    }

    /// The task returned `Ready`: end it and drop its future (the cold path).
    #[inline(never)]
    unsafe fn complete(this: *mut Self, header: *const TaskHeader) {
        unsafe {
            Self::terminal(this, header);
            // A self-wake during the final poll keeps the slot until its entry drains.
            let _slot = SlotGuard { header };
            let flags = TaskHeader::flags(header);
            flags.set(flags.get() | DROP_STARTED);
            Meta::drop_fn(TaskHeader::meta(header))(header);
        }
    }

    #[inline]
    unsafe fn terminal(this: *mut Self, header: *const TaskHeader) {
        unsafe {
            let flags = TaskHeader::flags(header);
            let current = flags.get();
            if current & LIVE != 0 {
                flags.set(current & !LIVE);
                (*this).task_count -= 1;
            }
        }
    }

    /// Owner-only cleanup. Stable raw pointer, no active poll/drop or escaped task uses.
    /// Scheduling closes before user Drop. A caught destructor panic permits retry.
    /// Futures drop in an unspecified order.
    pub(crate) unsafe fn cleanup(this: *mut Self) {
        unsafe {
            assert!(
                !(*this).active && (*this).spawning == 0,
                "executor reentrancy"
            );
            let turn = Self::enter_turn(this);
            (*this).lifecycle = Lifecycle::Closing;
            Mailbox::close((*this).mailbox);
            (*this).run_queue.clear();
            (*this).cursor = 0;
            (*this).incoming.clear();
            (*this).deferred.clear();
            // No spawn succeeds while Closing, and no reservation is open.
            for header in Self::slots(this) {
                if TaskHeader::flags(header).get() & TAKEN != 0 {
                    Self::terminal(this, header);
                    let flags = TaskHeader::flags(header);
                    flags.set(flags.get() & !(QUEUED | DEFERRED));
                }
            }
            for header in Self::slots(this) {
                let flags = TaskHeader::flags(header);
                let current = flags.get();
                if current & TAKEN == 0 {
                    continue;
                }
                let _slot = SlotGuard { header };
                if current & DROP_STARTED == 0 {
                    flags.set(current | DROP_STARTED);
                    Meta::drop_fn(TaskHeader::meta(header))(header);
                }
            }
            // Every slot's flags are now clear, so wakes of the leaked blocks
            // stop at the flags check and never read the freed executor or pool.
            for pool in std::mem::take(&mut (*this).pools) {
                RuntimePool::free(pool);
            }
            turn.finish();
        }
    }

    /// Every slot header ever handed out by this executor's pools. Runs no user code.
    unsafe fn slots(this: *mut Self) -> impl Iterator<Item = *const TaskHeader> {
        unsafe {
            (0..(*this).pools.len())
                .flat_map(move |pool| RuntimePool::headers((&(*this).pools)[pool]))
        }
    }

    /// Returns count of active tasks.
    #[inline]
    pub unsafe fn task_count(this: *mut Self) -> usize {
        unsafe { (*this).task_count }
    }
}

thread_local! {
    /// The executor whose turn (a `Runtime::poll`, or cleanup) runs on this thread;
    /// null outside one. `TurnGuard` restores the outer one.
    static POLLING: Cell<*mut Executor> = const { Cell::new(std::ptr::null_mut()) };
}

/// Ends the `Runtime::poll` running on this thread after the task being polled, so
/// the loop turns the network sooner; the next poll first finishes the rest of the
/// round. Lasts that one poll. Does nothing outside a `Runtime::poll`.
pub fn request_turn() {
    let executor = POLLING.get();
    if !executor.is_null() {
        // SAFETY: set by a live executor's `TurnGuard` for the duration of its turn.
        unsafe { (*executor).turn_requested = true };
    }
}

pub(crate) struct TurnGuard {
    owner: *mut Executor,
    finished: bool,
    /// `POLLING` before this turn.
    outer: *mut Executor,
}

impl TurnGuard {
    pub(crate) fn finish(mut self) {
        self.finished = true;
    }
}

impl Drop for TurnGuard {
    fn drop(&mut self) {
        unsafe {
            if !self.finished {
                if (*self.owner).lifecycle == Lifecycle::Open {
                    (*self.owner).lifecycle = Lifecycle::Panicked;
                }
                // A poll unwound: end that task; it is never polled again.
                let current = (*self.owner).current;
                if !current.is_null() {
                    Executor::terminal(self.owner, current);
                    (*self.owner).current = std::ptr::null();
                }
            }
            (*self.owner).active = false;
        }
        POLLING.set(self.outer);
    }
}

/// A reserved slot between reservation and commit. Dropped on unwind (a
/// panicking factory), it returns the slot without touching the future.
pub(crate) struct Reservation {
    executor: *mut Executor,
    header: *const TaskHeader,
}

impl Reservation {
    pub(crate) fn header(&self) -> *const TaskHeader {
        self.header
    }

    /// The future is written: make the task LIVE and queue it.
    ///
    /// # Safety
    /// The slot's future is initialized.
    pub(crate) unsafe fn commit(self) {
        let this = ManuallyDrop::new(self);
        unsafe {
            TaskHeader::flags(this.header).set(TAKEN | LIVE | QUEUED);
            let executor = this.executor;
            (*executor).spawning -= 1;
            (*executor).task_count += 1;
            (*executor).incoming.push(this.header);
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        unsafe {
            (*self.executor).spawning -= 1;
            RuntimePool::release(self.header);
        }
    }
}

/// Frees a slot after its future's drop (or unwind), unless a queue entry still
/// refers to it; that entry reclaims it when drained.
struct SlotGuard {
    header: *const TaskHeader,
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        unsafe {
            if TaskHeader::flags(self.header).get() & QUEUED == 0 {
                RuntimePool::release(self.header);
            }
        }
    }
}

#[cfg(test)]
#[path = "executor/ownership_tests.rs"]
mod ownership_tests;
