//! Single-threaded async executor.
//!
//! Uses raw pointers and UnsafeCell to avoid RefCell overhead.
//! All access is single-threaded, enforced by design.
//!
//! All Executor methods take `*mut Self` (not `&mut self`) to avoid Stacked
//! Borrows violations: block metadata stores the executor pointer, and
//! `&mut self` borrows would invalidate those stored pointers.

use std::any::TypeId;
use std::mem::ManuallyDrop;

use crate::__private::Task;
use crate::intrusive::NIL;
use crate::oneshot::Sender;
use crate::remote::Mailbox;
use crate::task::SpawnToken;
use crate::task::TaskHeader;
use crate::task::block::{BlockSpec, Meta};
use crate::task::header::{DEFERRED, DROP_STARTED, LIVE, QUEUED, TAKEN};
use crate::task::pool::RuntimePool;
use crate::timer_registration::TimerStore;

/// Initial ready-queue reservation.
pub const DEFAULT_CAPACITY: usize = 64;

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

/// Append to a wake queue without a capacity check.
///
/// # Safety
/// `queue.len() < queue.capacity()`.
#[inline]
unsafe fn append_reserved(queue: &mut Vec<*const TaskHeader>, header: *const TaskHeader) {
    let len = queue.len();
    debug_assert!(len < queue.capacity());
    unsafe {
        queue.as_mut_ptr().add(len).write(header);
        queue.set_len(len + 1);
    }
}

/// Most task polls in one `Runtime::poll`, checked between rounds.
pub(crate) const POLL_BUDGET: usize = 1024;

/// Single-threaded executor.
///
/// Three queues: wakes go to `incoming`, deferred wakes to `deferred` (moved to
/// the front of `incoming` when a `Runtime::poll` starts), and each round swaps
/// `incoming` into `run_queue` and polls it. Simple iteration, no VecDeque.
pub struct Executor {
    /// Tasks to poll this cycle, by header.
    run_queue: Vec<*const TaskHeader>,
    /// Incoming wakes (spawns and wakes during poll).
    incoming: Vec<*const TaskHeader>,
    /// Tasks woken with `StoredWaker::wake_deferred`; moved into `incoming` at
    /// the start of the next `Runtime::poll`.
    deferred: Vec<*const TaskHeader>,
    /// Number of live tasks.
    task_count: usize,
    /// Slots in use, including a finished task's slot until its queue entry
    /// drains. `reserve` grows all three queues to at least this capacity before
    /// counting a slot; capacities never shrink (rounds and `undefer` only trade
    /// buffers between the queues). Each entry in `incoming` and `deferred`
    /// belongs to a distinct QUEUED slot, and a QUEUED slot is not reclaimed
    /// until its entry drains, so `incoming.len() + deferred.len() <= allocated`.
    allocated: usize,
    /// One pool per task function this executor has spawned.
    pools: Vec<*mut RuntimePool>,
    /// Spawns between slot reservation and commit (user factory code runs between).
    spawning: usize,
    active: bool,
    lifecycle: Lifecycle,
    /// The task being polled, for `Sleep`; null between polls.
    pub(crate) current: *const TaskHeader,
    pub(crate) timers: TimerStore,
    /// Wake requests from other threads; outlives this executor.
    mailbox: *const Mailbox,
}

impl Executor {
    pub fn new() -> Self {
        Self::with_capacity(0, DEFAULT_CAPACITY, 0)
    }

    pub(crate) fn with_capacity(initial_tick: u64, tasks: usize, timers: usize) -> Self {
        Self {
            run_queue: Vec::with_capacity(tasks),
            incoming: Vec::with_capacity(tasks),
            deferred: Vec::with_capacity(tasks),
            task_count: 0,
            allocated: 0,
            pools: Vec::new(),
            spawning: 0,
            active: false,
            lifecycle: Lifecycle::Open,
            current: std::ptr::null(),
            timers: TimerStore::new(initial_tick, timers),
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

    /// Spawn a task; gives the token back when its function's capacity is used up.
    ///
    /// # Safety
    /// Stable owner-thread pointer; owner remains alive through all task uses.
    pub unsafe fn spawn<T: Task>(
        this: *mut Self,
        token: SpawnToken<T>,
        join: Option<Sender<T::Output>>,
    ) -> Result<(), SpawnToken<T>> {
        unsafe {
            assert!((*this).lifecycle == Lifecycle::Open, "executor unavailable");
            token
                .into_payload()
                .spawn_in(this, join)
                .map_err(SpawnToken::new)
        }
    }

    /// Reserve queue capacity and a slot for one spawn. The slot stays free of
    /// LIVE until [`Reservation::commit`], so stale wakes of it do nothing.
    /// `None` when the function already has `capacity` slots in use here.
    ///
    /// # Safety
    /// Owner thread; open executor; `spec` is the shape of `key`'s future type.
    pub(crate) unsafe fn reserve(
        this: *mut Self,
        key: TypeId,
        spec: &BlockSpec,
        capacity: u32,
    ) -> Option<Reservation> {
        unsafe {
            let needed = (*this)
                .allocated
                .checked_add(1)
                .expect("task count overflow");
            for queue in [
                &mut (*this).run_queue,
                &mut (*this).incoming,
                &mut (*this).deferred,
            ] {
                if queue.capacity() < needed {
                    queue.reserve(needed - queue.len());
                }
            }
            let pool = match (*this)
                .pools
                .iter()
                .copied()
                .find(|&pool| RuntimePool::key(pool) == key)
            {
                Some(pool) => pool,
                None => {
                    let pool = RuntimePool::new(this, key, capacity);
                    (*this).pools.push(pool);
                    pool
                }
            };
            let header = RuntimePool::reserve(pool, spec)?;
            (*this).allocated = needed;
            (*this).spawning += 1;
            Some(Reservation {
                executor: this,
                header,
            })
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
            // SAFETY: the slot is LIVE without QUEUED, so it is counted in
            // `allocated` and has no entry: `incoming.len() < allocated <=
            // incoming.capacity()` (see `allocated`).
            append_reserved(&mut (*this).incoming, header);
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
            // The entry moved: queue lengths sum to at most `allocated`.
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
            // SAFETY: as in `wake`, `deferred.len() < allocated <= deferred.capacity()`.
            append_reserved(&mut (*this).deferred, header);
        }
    }

    /// Move deferred wakes into `incoming`, at the start of a poll.
    ///
    /// # Safety
    /// Stable owner-thread pointer to a live executor.
    #[inline]
    pub(crate) unsafe fn undefer(this: *mut Self) {
        unsafe {
            if !(*this).deferred.is_empty() {
                // DEFERRED stays set until the task's round clears it with QUEUED.
                // Deferred wakes came first (during the last poll): they run first.
                // Every slot has at most one queue entry: no reallocation.
                (*this).deferred.append(&mut (*this).incoming);
                std::mem::swap(&mut (*this).deferred, &mut (*this).incoming);
            }
        }
    }

    /// Poll one round (one snapshot of `incoming`), for tests of round order.
    ///
    /// # Safety
    /// Stable owner-thread pointer; no active turn/cleanup; executor accepts work.
    #[cfg(test)]
    pub unsafe fn poll(this: *mut Self) -> bool {
        unsafe {
            Self::check_turn(this);
            if (*this).incoming.is_empty() {
                return false;
            }
            let turn = Self::enter_turn(this);
            Self::poll_round(this);
            turn.finish();
            true
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
        }
    }

    /// Poll rounds: each round polls a snapshot of `incoming`, and another round
    /// follows while `incoming` is not empty and fewer than [`POLL_BUDGET`] polls
    /// were made (the budget never cuts a round short).
    ///
    /// Guard owns the active turn; expiry handoff is fully normalized.
    pub(crate) unsafe fn poll_batch(turn: &mut TurnGuard) -> usize {
        unsafe {
            let this = turn.owner;
            let mut polled = 0;
            while !(*this).incoming.is_empty() && polled < POLL_BUDGET {
                polled += Self::poll_round(this);
            }
            polled
        }
    }

    /// Poll one snapshot of `incoming`; returns the futures polled.
    ///
    /// # Safety
    /// As for [`Self::poll_batch`].
    unsafe fn poll_round(this: *mut Self) -> usize {
        unsafe {
            let mut polled = 0;
            std::mem::swap(&mut (*this).run_queue, &mut (*this).incoming);
            let batch_len = (*this).run_queue.len();
            let mut cursor = 0;
            while cursor < batch_len {
                let index = cursor;
                cursor += 1;
                // Spawn may reserve this Vec, but cannot change this batch's length.
                // Reload its buffer each iteration; no buffer pointer spans user code.
                let header = *(*this).run_queue.as_ptr().add(index);
                let flags = TaskHeader::flags(header);
                let current = flags.get() & !(QUEUED | DEFERRED);
                flags.set(current);
                if current & LIVE == 0 {
                    Self::reclaim(this, header);
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
            }
            (*this).run_queue.clear();
            polled
        }
    }

    /// The task returned `Ready`: end it and drop its future (the cold path).
    #[inline(never)]
    unsafe fn complete(this: *mut Self, header: *const TaskHeader) {
        unsafe {
            Self::terminal(this, header);
            // A self-wake during the final poll keeps the slot until its entry drains.
            let _slot = SlotGuard {
                owner: this,
                header,
            };
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
                if TaskHeader::reg_head(header).get() != NIL {
                    (*this).timers.retire(header);
                }
            }
        }
    }

    unsafe fn reclaim(this: *mut Self, header: *const TaskHeader) {
        unsafe {
            (*this).allocated -= 1;
            RuntimePool::release(header);
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
                let _slot = SlotGuard {
                    owner: this,
                    header,
                };
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

    /// Entries in `incoming` and `deferred`.
    #[cfg(test)]
    pub(crate) unsafe fn queue_lengths(this: *mut Self) -> (usize, usize) {
        unsafe { ((*this).incoming.len(), (*this).deferred.len()) }
    }

    #[cfg(test)]
    pub(crate) unsafe fn active(this: *mut Self) -> bool {
        unsafe { (*this).active }
    }
}

pub(crate) struct TurnGuard {
    owner: *mut Executor,
    finished: bool,
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
            Executor::reclaim(self.executor, self.header);
        }
    }
}

/// Frees a slot after its future's drop (or unwind), unless a queue entry still
/// refers to it; that entry reclaims it when drained.
struct SlotGuard {
    owner: *mut Executor,
    header: *const TaskHeader,
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        unsafe {
            if TaskHeader::flags(self.header).get() & QUEUED == 0 {
                Executor::reclaim(self.owner, self.header);
            }
        }
    }
}

#[cfg(test)]
#[path = "executor/ownership_tests.rs"]
mod ownership_tests;

#[cfg(test)]
#[path = "executor/state_walk_tests.rs"]
mod state_walk_tests;
