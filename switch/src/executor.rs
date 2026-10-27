//! Single-threaded async executor.
//!
//! Uses raw pointers and UnsafeCell to avoid RefCell overhead.
//! All access is single-threaded, enforced by design.
//!
//! All Executor methods take `*mut Self` (not `&mut self`) to avoid Stacked
//! Borrows violations: we store the executor pointer in task headers, and
//! `&mut self` borrows would invalidate those stored pointers.

use std::future::Future;
use std::mem::ManuallyDrop;
use std::task::{Context, Poll};

use crate::task::{TaskBox, TaskHeader, TaskState};
use crate::timer_registration::TimerStore;
use crate::waker::{WakeFrame, this_thread_tag, waker_from_frame};

/// Maximum tasks (for pre-allocation, not a hard limit now).
pub const DEFAULT_CAPACITY: usize = 64;

/// Wake a task. Called from waker vtable.
///
/// # Safety
/// - `header` must point to a valid TaskHeader
/// - `header.executor` must point to a valid Executor
/// - Call only on the executor's owner thread, with stable owner/task storage
#[inline]
pub unsafe fn wake_task(header: *mut TaskHeader) {
    unsafe {
        let executor = (*header).executor as *mut Executor;
        Executor::wake(executor, header);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lifecycle {
    Open,
    Panicked,
    Closing,
}

/// Single-threaded executor.
///
/// Two-vector swap model: wakes go to `incoming`, we iterate `run_queue`,
/// then swap at end of poll. Simple iteration, no VecDeque overhead.
pub struct Executor {
    /// Tasks to poll this cycle.
    run_queue: Vec<*mut TaskHeader>,
    /// Incoming wakes (spawns and wakes during poll).
    incoming: Vec<*mut TaskHeader>,
    /// Number of active tasks.
    task_count: usize,
    allocated: usize,
    all_head: *mut TaskHeader,
    active: bool,
    lifecycle: Lifecycle,
    owner: u16,
    pub(crate) active_frame: *const WakeFrame,
    pub(crate) timers: TimerStore,
}

impl Executor {
    pub fn new() -> Self {
        Self::with_capacity(0, DEFAULT_CAPACITY, 0)
    }

    pub(crate) fn with_capacity(initial_tick: u64, tasks: usize, timers: usize) -> Self {
        Self {
            run_queue: Vec::with_capacity(tasks),
            incoming: Vec::with_capacity(tasks),
            task_count: 0,
            allocated: 0,
            all_head: std::ptr::null_mut(),
            active: false,
            lifecycle: Lifecycle::Open,
            owner: this_thread_tag(),
            active_frame: std::ptr::null(),
            timers: TimerStore::new(initial_tick, timers),
        }
    }

    /// Stable owner-thread pointer to a live executor.
    pub(crate) unsafe fn lifecycle(this: *mut Self) -> Lifecycle {
        unsafe { (*this).lifecycle }
    }

    /// # Safety
    /// Stable owner-thread pointer; owner remains alive through all task uses.
    pub unsafe fn spawn<F>(this: *mut Self, future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        unsafe {
            assert!((*this).lifecycle == Lifecycle::Open, "executor unavailable");
            let needed = (*this)
                .allocated
                .checked_add(1)
                .expect("task count overflow");
            for queue in [&mut (*this).run_queue, &mut (*this).incoming] {
                if queue.capacity() < needed {
                    queue.reserve(needed - queue.len());
                }
            }
            let ptr = TaskBox::new(this.cast(), future).into_raw();
            (*ptr).next = (*this).all_head;
            if !(*this).all_head.is_null() {
                (*(*this).all_head).prev = ptr;
            }
            (*this).all_head = ptr;
            (*this).allocated = needed;
            (*this).task_count += 1;
            (*ptr).queued = true;
            (*this).incoming.push(ptr);
        }
    }

    #[inline]
    unsafe fn wake(this: *mut Self, header: *mut TaskHeader) {
        unsafe {
            if (*this).lifecycle != Lifecycle::Open
                || (*header).state == TaskState::Terminal
                || (*header).queued
            {
                return;
            }
            (*header).queued = true;
            (*header).state = TaskState::Scheduled;
            (*this).incoming.push(header);
        }
    }

    /// # Safety
    /// Stable owner-thread pointer; no active turn/cleanup; executor accepts work.
    #[cfg(test)]
    pub unsafe fn poll(this: *mut Self) -> bool {
        unsafe {
            Self::check_turn(this);
            if (*this).incoming.is_empty() {
                return false;
            }
            let mut turn = Self::enter_turn(this);
            let report = Self::poll_batch(&mut turn);
            turn.finish();
            report.queue_work
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
            current: std::ptr::null_mut(),
            finished: false,
        }
    }

    /// Guard owns the active turn; expiry handoff is fully normalized.
    pub(crate) unsafe fn poll_batch(turn: &mut TurnGuard) -> BatchReport {
        unsafe {
            let this = turn.owner;
            let mut report = BatchReport::default();
            if (*this).incoming.is_empty() {
                return report;
            }
            report.queue_work = true;
            std::mem::swap(&mut (*this).run_queue, &mut (*this).incoming);
            let batch_len = (*this).run_queue.len();
            let mut cursor = 0;
            while cursor < batch_len {
                let index = cursor;
                cursor += 1;
                // Spawn may reserve this Vec, but cannot change this batch's length.
                // Reload its buffer each iteration; no buffer pointer spans user code.
                let header = *(*this).run_queue.as_ptr().add(index);
                (*header).queued = false;
                if (*header).state == TaskState::Terminal {
                    Self::reclaim(this, header);
                    continue;
                }
                (*header).state = TaskState::Idle;
                turn.current = header;
                report.polled += 1;
                let result = {
                    let frame = WakeFrame {
                        owner: (*this).owner,
                        task: header,
                    };
                    (*this).active_frame = std::ptr::from_ref(&frame);
                    let _frame_scope = ActiveFrameGuard(this);
                    let waker = ManuallyDrop::new(waker_from_frame(&frame));
                    let mut cx = Context::from_waker(&waker);
                    TaskHeader::poll(header, &mut cx)
                };
                match result {
                    Poll::Ready(()) => {
                        report.completed += 1;
                        Self::terminal(this, header);
                        // Completion may free the header while unwinding its destructor.
                        turn.current = std::ptr::null_mut();
                        let _allocation = AllocationGuard {
                            owner: this,
                            header,
                        };
                        TaskHeader::drop_future(header);
                    }
                    Poll::Pending => {
                        turn.current = std::ptr::null_mut();
                    }
                }
            }
            (*this).run_queue.clear();
            report
        }
    }

    /// Stable owner-thread pointer; no user code runs while scanning claims.
    pub(crate) unsafe fn ready_count(this: *mut Self) -> usize {
        unsafe {
            (*this)
                .incoming
                .iter()
                .filter(|&&header| (*header).state != TaskState::Terminal)
                .count()
        }
    }

    #[inline]
    unsafe fn terminal(this: *mut Self, header: *mut TaskHeader) {
        unsafe {
            if (*header).state != TaskState::Terminal {
                (*header).state = TaskState::Terminal;
                (*this).task_count -= 1;
                (*this).timers.retire(header);
            }
        }
    }

    unsafe fn reclaim(this: *mut Self, header: *mut TaskHeader) {
        unsafe {
            let prev = (*header).prev;
            let next = (*header).next;
            if prev.is_null() {
                (*this).all_head = next;
            } else {
                (*prev).next = next;
            }
            if !next.is_null() {
                (*next).prev = prev;
            }
            (*this).allocated -= 1;
            TaskHeader::free(header);
        }
    }

    /// Owner-only cleanup. Stable raw pointer, no active poll/drop or escaped task uses.
    /// Scheduling closes before user Drop. A caught destructor panic permits retry.
    pub(crate) unsafe fn cleanup(this: *mut Self) {
        unsafe {
            assert!(!(*this).active, "executor reentrancy");
            let turn = Self::enter_turn(this);
            (*this).lifecycle = Lifecycle::Closing;
            (*this).run_queue.clear();
            (*this).incoming.clear();
            let mut header = (*this).all_head;
            while !header.is_null() {
                Self::terminal(this, header);
                (*header).queued = false;
                header = (*header).next;
            }
            while !(*this).all_head.is_null() {
                let header = (*this).all_head;
                let _allocation = AllocationGuard {
                    owner: this,
                    header,
                };
                TaskHeader::drop_future(header);
            }
            turn.finish();
        }
    }

    /// Returns count of active tasks.
    #[inline]
    pub unsafe fn task_count(this: *mut Self) -> usize {
        unsafe { (*this).task_count }
    }
}

#[derive(Default)]
pub(crate) struct BatchReport {
    pub(crate) polled: usize,
    pub(crate) completed: usize,
    pub(crate) queue_work: bool,
}

pub(crate) struct TurnGuard {
    owner: *mut Executor,
    current: *mut TaskHeader,
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
                if !self.current.is_null() {
                    Executor::terminal(self.owner, self.current);
                }
            }
            (*self.owner).active = false;
        }
    }
}

struct AllocationGuard {
    owner: *mut Executor,
    header: *mut TaskHeader,
}

impl Drop for AllocationGuard {
    fn drop(&mut self) {
        unsafe {
            if !(*self.header).queued {
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
    use super::*;
    use crate::runtime::Runtime;
    use std::cell::Cell;
    use std::pin::Pin;
    use std::rc::Rc;

    struct YieldN(u32);
    impl Future for YieldN {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.0 == 0 {
                Poll::Ready(())
            } else {
                self.0 -= 1;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    #[test]
    fn test_spawn_and_poll() {
        let mut runtime = Runtime::new(0);
        let spawner = runtime.spawner();
        let ran = Rc::new(Cell::new(false));
        let ran_clone = ran.clone();

        spawner
            .spawn(async move {
                ran_clone.set(true);
            })
            .unwrap();

        let report = runtime.poll(0).unwrap();
        assert_eq!(report.tasks_polled, 1);
        assert_eq!(report.tasks_completed, 1);
        assert_eq!(report.live_tasks, 0);
        assert!(ran.get());
    }

    #[test]
    fn test_yield_and_resume() {
        let mut runtime = Runtime::new(0);
        let spawner = runtime.spawner();
        let count = Rc::new(Cell::new(0));
        let count_clone = count.clone();

        spawner
            .spawn(async move {
                YieldN(3).await;
                count_clone.set(count_clone.get() + 1);
            })
            .unwrap();

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
            let count_clone = count.clone();
            spawner
                .spawn(async move {
                    count_clone.set(count_clone.get() + 1);
                })
                .unwrap();
        }

        let report = runtime.poll(0).unwrap();
        assert_eq!(report.tasks_polled, 10);
        assert_eq!(report.tasks_completed, 10);
        assert_eq!(report.live_tasks, 0);
        assert_eq!(count.get(), 10);
    }
}

#[cfg(test)]
#[path = "executor/ownership_tests.rs"]
mod ownership_tests;

// Declared after the stationary frame, so clearing precedes its end on unwind too.
struct ActiveFrameGuard(*mut Executor);

impl Drop for ActiveFrameGuard {
    fn drop(&mut self) {
        unsafe {
            (*self.0).active_frame = std::ptr::null();
        }
    }
}
