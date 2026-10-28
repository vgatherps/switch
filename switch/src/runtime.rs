//! Caller-driven runtime owner and local task submission capability.

use std::cell::{Cell, UnsafeCell};
use std::fmt;
use std::rc::Rc;

use crate::__private::Task;
use crate::executor::{DEFAULT_CAPACITY, Executor, Lifecycle};
use crate::task::SpawnToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Capacity {
    /// Ready-queue reservation; task slots come from per-function slabs.
    pub tasks: usize,
    /// Timer-registration and expiry-index reservation.
    pub timers: usize,
}

/// Runtime state or supplied-clock error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeError {
    Closed,
    Panicked,
    TimeRegressed { current: u64, supplied: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Unavailable {
    Closed,
    Panicked,
}

impl From<Unavailable> for RuntimeError {
    fn from(value: Unavailable) -> Self {
        match value {
            Unavailable::Closed => Self::Closed,
            Unavailable::Panicked => Self::Panicked,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PollReport {
    /// Calls made to task futures during this turn.
    pub tasks_polled: usize,
    /// Tasks that returned `Ready` during this turn.
    pub tasks_completed: usize,
    /// Timer registrations normalized as expired during this turn.
    pub timers_expired: usize,
    /// Nonterminal ready tasks left after this turn.
    pub ready_tasks: usize,
    /// All live tasks, including tasks waiting on timers or other wakes.
    pub live_tasks: usize,
}

/// A rejected task submission and the original token.
pub struct SpawnError<T> {
    pub reason: RuntimeError,
    pub token: SpawnToken<T>,
}

impl<T> fmt::Debug for SpawnError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpawnError")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

pub(crate) struct Control {
    core: UnsafeCell<Option<Box<Executor>>>,
    root: Cell<*mut Executor>,
    /// Shutdown was requested while a task factory ran; the outermost spawn finishes it.
    close_deferred: Cell<bool>,
}

impl Control {
    fn new(initial_tick: u64, capacity: Capacity) -> Rc<Self> {
        let control = Rc::new(Self {
            core: UnsafeCell::new(Some(Box::new(Executor::with_capacity(
                initial_tick,
                capacity.tasks,
                capacity.timers,
            )))),
            root: Cell::new(std::ptr::null_mut()),
            close_deferred: Cell::new(false),
        });
        // Derive once after final Control placement, before any task aliases exist.
        let root =
            unsafe { (&mut *control.core.get()).as_mut().unwrap().as_mut() as *mut Executor };
        control.root.set(root);
        control
    }

    pub(crate) fn open_core(&self) -> Result<*mut Executor, Unavailable> {
        let root = self.root.get();
        if root.is_null() {
            return Err(Unavailable::Closed);
        }
        match unsafe { Executor::lifecycle(root) } {
            Lifecycle::Open => Ok(root),
            Lifecycle::Panicked => Err(Unavailable::Panicked),
            Lifecycle::Closing => Err(Unavailable::Closed),
        }
    }

    /// Token must have originated in this Control's timer store.
    pub(crate) unsafe fn cancel_timer(&self, token: crate::timer_registration::Token) {
        let root = self.root.get();
        if !root.is_null() {
            unsafe {
                (*root).timers.cancel(token);
            }
        }
    }

    fn shutdown(&self) {
        let root = self.root.get();
        if root.is_null() {
            return;
        }
        unsafe {
            if Executor::spawning(root) > 0 {
                // A task factory runs between slot reservation and commit, inside
                // `Spawner::spawn`, which holds this Control. Close to new work now;
                // that spawn completes the shutdown once its reservation resolves.
                Executor::close(root);
                self.close_deferred.set(true);
                return;
            }
            // Owning Box stays in place throughout arbitrary future destruction.
            Executor::cleanup(root);
            self.root.set(std::ptr::null_mut());
            // Cleanup finished: no remaining task/frame/destructor aliases.
            drop((&mut *self.core.get()).take());
        }
    }
}

/// Caller-driven owner of local tasks and timers.
///
/// The supplied tick is the runtime's only clock. `Runtime` is `!Send + !Sync`.
pub struct Runtime {
    control: Rc<Control>,
}

impl Runtime {
    /// Create a runtime whose accepted clock starts at `initial_tick`.
    pub fn new(initial_tick: u64) -> Self {
        Self::with_capacity(
            initial_tick,
            Capacity {
                tasks: DEFAULT_CAPACITY,
                timers: 0,
            },
        )
    }

    /// Create a runtime with explicit task-queue and timer-store reservations.
    pub fn with_capacity(initial_tick: u64, capacity: Capacity) -> Self {
        Self {
            control: Control::new(initial_tick, capacity),
        }
    }

    /// Return a local capability for submitting tasks and creating sleeps.
    pub fn spawner(&self) -> Spawner {
        Spawner {
            control: self.control.clone(),
        }
    }

    /// Advance to `now` and poll one ready snapshot.
    ///
    /// Equal ticks are accepted. A lower tick returns `TimeRegressed` before state changes.
    /// This method never waits, though future code can block. Tasks woken or spawned before
    /// this call are polled in this ready-task snapshot.
    pub fn poll(&mut self, now: u64) -> Result<PollReport, RuntimeError> {
        let root = self.control.open_core()?;
        unsafe {
            let current = (*root).timers.current_tick();
            if now < current {
                return Err(RuntimeError::TimeRegressed {
                    current,
                    supplied: now,
                });
            }
            Executor::check_turn(root);
            let mut turn = Executor::enter_turn(root);
            (*root)
                .timers
                .advance_to(now)
                .expect("validated timer tick regressed");
            let mut timers_expired = 0;
            loop {
                let task = (*root).timers.pop_expired();
                let Some(task) = task else {
                    break;
                };
                timers_expired += 1;
                Executor::wake_task(task);
            }
            let batch = Executor::poll_batch(&mut turn);
            let report = PollReport {
                tasks_polled: batch.polled,
                tasks_completed: batch.completed,
                timers_expired,
                ready_tasks: Executor::ready_count(root),
                live_tasks: Executor::task_count(root),
            };
            turn.finish();
            Ok(report)
        }
    }

    /// Close the runtime and drop its remaining tasks, in an unspecified order.
    pub fn shutdown(&mut self) {
        self.control.shutdown();
    }
}

struct CleanupContinuation(Rc<Control>);

impl Drop for CleanupContinuation {
    fn drop(&mut self) {
        self.0.shutdown();
    }
}

/// Dropping a `Runtime` shuts it down: its remaining tasks are dropped, in an
/// unspecified order.
impl Drop for Runtime {
    fn drop(&mut self) {
        let _continuation = CleanupContinuation(self.control.clone());
        self.shutdown();
    }
}

#[derive(Clone)]
/// Local capability to submit tasks and create runtime-bound sleeps.
pub struct Spawner {
    control: Rc<Control>,
}

impl Spawner {
    /// Create a sleep for an absolute runtime tick.
    pub fn sleep_until(
        &self,
        deadline: u64,
    ) -> Result<crate::sleep::Sleep, crate::sleep::SleepError> {
        self.control.open_core()?;
        Ok(crate::sleep::Sleep::new(deadline, self.control.clone()))
    }

    /// Create a sleep after `ticks` from the latest accepted runtime tick.
    pub fn sleep(&self, ticks: u64) -> Result<crate::sleep::Sleep, crate::sleep::SleepError> {
        let root = self.control.open_core()?;
        let deadline = unsafe { (*root).timers.current_tick() }
            .checked_add(ticks)
            .ok_or(crate::sleep::SleepError::DeadlineOverflow)?;
        Ok(crate::sleep::Sleep::new(deadline, self.control.clone()))
    }

    /// Submit a task function's token. Rejection returns the token before any
    /// task storage is touched.
    ///
    /// If the task's construction shuts down or drops the runtime, the runtime
    /// closes at once and its tasks, this one included, are dropped when the
    /// construction returns, before `spawn` returns `Ok`.
    pub fn spawn<T: Task>(&self, token: SpawnToken<T>) -> Result<(), SpawnError<T>> {
        match self.control.open_core() {
            Ok(root) => {
                let _deferred = DeferredShutdown(&self.control);
                unsafe { Executor::spawn(root, token) };
                Ok(())
            }
            Err(reason) => Err(SpawnError {
                reason: reason.into(),
                token,
            }),
        }
    }
}

/// Completes a shutdown that a task factory requested during this spawn, once
/// no reservation is outstanding (also on unwind).
struct DeferredShutdown<'a>(&'a Control);

impl Drop for DeferredShutdown<'_> {
    fn drop(&mut self) {
        if self.0.close_deferred.get() {
            self.0.shutdown();
        }
    }
}

#[cfg(test)]
#[path = "runtime/tests.rs"]
mod tests;
