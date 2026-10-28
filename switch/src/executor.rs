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
use crate::task::header::{DROP_STARTED, LIVE, QUEUED, TAKEN};
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
    /// Slots in use; all three queues keep at least this capacity (every slot
    /// has at most one queue entry), so queue pushes never reallocate.
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
            if current & (LIVE | QUEUED) != LIVE {
                return;
            }
            // A LIVE slot's executor is alive: cleanup clears every slot's flags
            // before the executor is freed, after which `Meta::executor` dangles.
            let this = Meta::executor(TaskHeader::meta(header));
            if (*this).lifecycle != Lifecycle::Open {
                return;
            }
            flags.set(current | QUEUED);
            (*this).incoming.push(header);
        }
    }

    /// As [`Self::wake`], into `deferred`: the task runs in the next poll.
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
            let this = Meta::executor(TaskHeader::meta(header));
            if (*this).lifecycle != Lifecycle::Open {
                return;
            }
            flags.set(current | QUEUED);
            (*this).deferred.push(header);
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
                let current = flags.get() & !QUEUED;
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
                    flags.set(flags.get() & !QUEUED);
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

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime::Runtime;
    use std::cell::Cell;
    use std::rc::Rc;

    #[switch::task]
    async fn set_flag(flag: Rc<Cell<bool>>) {
        flag.set(true);
    }

    #[switch::task]
    async fn yield_then_count(count: Rc<Cell<usize>>) {
        for _ in 0..3 {
            crate::yield_now().await;
        }
        count.set(count.get() + 1);
    }

    #[switch::task]
    async fn count_once(count: Rc<Cell<usize>>) {
        count.set(count.get() + 1);
    }

    #[test]
    fn test_spawn_and_poll() {
        let mut runtime = Runtime::new(0);
        let spawner = runtime.spawner();
        let ran = Rc::new(Cell::new(false));

        spawner.spawn(set_flag(ran.clone())).unwrap();

        let report = runtime.poll(0).unwrap();
        assert_eq!(report.tasks_polled, 1);
        assert_eq!(report.live_tasks, 0);
        assert!(ran.get());
    }

    #[test]
    fn test_yield_and_resume() {
        let mut runtime = Runtime::new(0);
        let spawner = runtime.spawner();
        let count = Rc::new(Cell::new(0));

        spawner.spawn(yield_then_count(count.clone())).unwrap();

        let mut polls = 0;
        let mut report = runtime.poll(0).unwrap();
        polls += report.tasks_polled;
        while report.live_tasks != 0 {
            report = runtime.poll(0).unwrap();
            polls += report.tasks_polled;
        }

        assert_eq!(polls, 4); // 3 yields + 1 completion
        assert_eq!(count.get(), 1);
    }

    #[test]
    fn test_multiple_tasks() {
        let mut runtime = Runtime::new(0);
        let spawner = runtime.spawner();
        let count = Rc::new(Cell::new(0));

        for _ in 0..10 {
            spawner.spawn(count_once(count.clone())).unwrap();
        }

        let report = runtime.poll(0).unwrap();
        assert_eq!(report.tasks_polled, 10);
        assert_eq!(report.live_tasks, 0);
        assert_eq!(count.get(), 10);
    }
}

#[cfg(test)]
#[path = "executor/ownership_tests.rs"]
mod ownership_tests;
