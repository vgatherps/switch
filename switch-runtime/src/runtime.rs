//! Caller-driven runtime owner and local task submission capability.

use std::cell::{Cell, UnsafeCell};
use std::fmt;
use std::rc::Rc;

use crate::__private::Task;
use crate::executor::{Executor, Lifecycle};
use crate::oneshot::{self, Sender};
use crate::remote::Mailbox;
use crate::task::SpawnToken;
use crate::task::join::JoinHandle;

/// Why [`Runtime::poll`] returned no report, or (as [`SpawnError::reason`]) why a spawn was
/// refused.
///
/// Implements `Debug` but not `Display` or `std::error::Error`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeError {
    /// The runtime was shut down or dropped. Permanent.
    Closed,
    /// A task of this runtime panicked in an earlier poll. Permanent; the runtime can still
    /// be shut down or dropped.
    Panicked,
    /// The tick passed to [`Runtime::poll`] is lower than the latest accepted one. No task
    /// ran and nothing changed; a later poll with a valid tick proceeds normally.
    TimeRegressed {
        /// The latest accepted tick.
        current: u64,
        /// The tick that was passed.
        supplied: u64,
    },
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

/// What one [`Runtime::poll`] did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PollReport {
    /// Calls this poll made into task futures. A task woken twice in one poll counts twice.
    pub tasks_polled: usize,
    /// Tasks that have not finished, including tasks parked on a timer or waiting for a
    /// wake. Zero once every spawned task has finished.
    pub live_tasks: usize,
}

/// A spawn the runtime refused, with the token handed back unspawned.
///
/// Implements `Debug` (which omits the token) but not `Display` or `std::error::Error`.
pub struct SpawnError<T> {
    /// [`RuntimeError::Closed`] or [`RuntimeError::Panicked`]. A spawn is never refused for
    /// lack of space: task storage grows as needed.
    pub reason: RuntimeError,
    /// The task, not started. Dropping it drops its arguments.
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
    fn new(initial_tick: u64) -> Rc<Self> {
        let executor = Box::new(Executor::new(initial_tick));
        let control = Rc::new(Self {
            core: UnsafeCell::new(Some(executor)),
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

    /// Cancel a token of this Control's timer store, if the executor still exists.
    pub(crate) fn cancel_timer(&self, token: crate::timer_registration::Token) {
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

/// Owner of a set of tasks and timers, run by the program calling [`Runtime::poll`].
///
/// The tick passed to `poll` is the runtime's only clock: a `u64` in a unit the program
/// chooses, which must never decrease. Timers ([`Sleep`](crate::Sleep)) fire against it.
/// Network timeouts given as `Duration`s convert by counting nanoseconds, so a network
/// program passes monotonic nanoseconds.
///
/// A `Runtime` is neither `Send` nor `Sync`: it is created, polled and dropped on one
/// thread, and its tasks run on that thread. Tasks are started through a [`Spawner`].
///
/// Task storage grows as tasks are spawned, so the number of live tasks has no limit. Each
/// task's future must fit in a 64 KiB block; a larger one is a compile error. That memory
/// is never returned to the system, also after the runtime is dropped, so a program
/// creates one runtime and keeps it until it exits.
///
/// Dropping a `Runtime` is the same as [`Runtime::shutdown`]. Dropping it from the
/// destructor of one of its own tasks while that shutdown runs aborts the process.
pub struct Runtime {
    control: Rc<Control>,
}

impl Runtime {
    /// Creates a runtime whose latest accepted tick is `initial_tick`. A
    /// [`Spawner::sleep`] created before the first poll counts from it.
    pub fn new(initial_tick: u64) -> Self {
        Self {
            control: Control::new(initial_tick),
        }
    }

    /// A handle for spawning tasks and creating sleeps on this runtime. Spawners are cheap
    /// to clone; every clone refers to this runtime.
    pub fn spawner(&self) -> Spawner {
        Spawner {
            control: self.control.clone(),
        }
    }

    /// Advances the runtime's clock to `now` and runs the tasks that are ready, then
    /// returns. It never waits; a task that blocks the thread blocks this call.
    ///
    /// One poll, in order:
    ///
    /// 1. A closed or panicked runtime returns [`RuntimeError::Closed`] or
    ///    [`RuntimeError::Panicked`].
    /// 2. `now` lower than the latest accepted tick returns
    ///    [`RuntimeError::TimeRegressed`], with no other effect. An equal tick is accepted.
    /// 3. Tasks woken from other threads since the last poll become ready, then the tasks
    ///    deferred in the last poll ([`yield_now`](crate::yield_now),
    ///    [`StoredWaker::wake_deferred`](crate::StoredWaker::wake_deferred)).
    /// 4. The clock moves to `now`. Every [`Sleep`](crate::Sleep) whose deadline is at or
    ///    before `now` wakes its task; these become ready after the tasks already ready,
    ///    earliest deadline first.
    /// 5. If the last poll was ended by [`request_turn`](crate::request_turn), the tasks
    ///    left in its round run first.
    /// 6. Ready tasks run in rounds. A round runs each task that was ready when the round
    ///    started, until it returns `Pending` or finishes; a task woken or spawned during a
    ///    round runs in the next one. Rounds continue until no task is ready, or until 1024
    ///    task polls were made. The limit is checked between rounds, so one poll can make
    ///    more than 1024. A task that calls `request_turn` ends the poll as soon as it
    ///    returns. Ready tasks left over run in the next poll.
    ///
    /// Tasks are never interrupted: a task keeps the thread until it awaits something not
    /// yet available, or finishes.
    ///
    /// # Panics
    ///
    /// A panic in a task unwinds out of `poll` with the task's original payload. That task
    /// is ended and its [`JoinHandle`] resolves to `Err(Canceled)` at once. The runtime
    /// then refuses work: `poll` returns [`RuntimeError::Panicked`], spawns fail with that
    /// reason, and sleeps fail with [`SleepError::Panicked`](crate::SleepError::Panicked).
    /// The other tasks stay until the runtime is shut down or dropped.
    ///
    /// Panics if called from inside a poll of the same runtime.
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
            let mailbox = Executor::mailbox(root);
            if Mailbox::pending(mailbox) {
                Mailbox::deliver(mailbox);
            }
            Executor::undefer(root);
            (*root).timers.advance_to(now);
            (*root).timers.wake_expired(|task| Executor::wake(task));
            let report = PollReport {
                tasks_polled: Executor::poll_batch(&mut turn),
                live_tasks: Executor::task_count(root),
            };
            turn.finish();
            Ok(report)
        }
    }

    /// Closes the runtime and drops every remaining task, in an unspecified order. A task's
    /// future is dropped with everything it owns, sockets included.
    ///
    /// Afterwards:
    ///
    /// | Call | Result |
    /// |---|---|
    /// | [`Runtime::poll`] | `Err(RuntimeError::Closed)` |
    /// | [`Spawner::spawn`], [`Spawner::spawn_join`] | [`SpawnError`] with reason `Closed` |
    /// | [`Spawner::sleep`], [`Spawner::sleep_until`], awaiting a [`Sleep`](crate::Sleep) | `Err(SleepError::Closed)` |
    /// | awaiting the [`JoinHandle`] of an unfinished task | `Err(Canceled)` |
    /// | a wake from another thread ([`RemoteWaker`](crate::RemoteWaker) or a task's `Waker`) | dropped |
    ///
    /// Calling it again does nothing. A task destructor run here must not drop the
    /// `Runtime`: that aborts the process.
    pub fn shutdown(&mut self) {
        self.control.shutdown();
    }
}

/// Finishes a shutdown that a task destructor's panic interrupted.
struct CleanupContinuation<'a>(&'a Control);

impl Drop for CleanupContinuation<'_> {
    fn drop(&mut self) {
        self.0.shutdown();
    }
}

/// Shuts the runtime down, as [`Runtime::shutdown`]. Dropping the `Runtime` from a task
/// destructor that this shutdown runs aborts the process.
impl Drop for Runtime {
    fn drop(&mut self) {
        let _continuation = CleanupContinuation(&self.control);
        self.control.shutdown();
    }
}

/// A handle for spawning tasks on a [`Runtime`] and creating its sleeps. From
/// [`Runtime::spawner`].
///
/// Cheap to clone; a task that spawns other tasks or sleeps usually takes a `Spawner` as an
/// argument. It cannot poll or shut down the runtime. Neither `Send` nor `Sync`.
#[derive(Clone)]
pub struct Spawner {
    control: Rc<Control>,
}

impl Spawner {
    /// A sleep that completes once the runtime's tick reaches `deadline`. A deadline at or
    /// before the current tick completes on its first poll.
    ///
    /// # Errors
    ///
    /// [`SleepError::Closed`](crate::SleepError::Closed) or
    /// [`SleepError::Panicked`](crate::SleepError::Panicked) when the runtime no longer
    /// takes work.
    pub fn sleep_until(
        &self,
        deadline: u64,
    ) -> Result<crate::sleep::Sleep, crate::sleep::SleepError> {
        self.control.open_core()?;
        Ok(crate::sleep::Sleep::new(deadline, self.control.clone()))
    }

    /// A sleep whose deadline is the latest accepted tick plus `ticks`: the tick of the
    /// running poll, else of the last poll, else the runtime's initial tick. No clock is
    /// read.
    ///
    /// # Errors
    ///
    /// [`SleepError::DeadlineOverflow`](crate::SleepError::DeadlineOverflow) when the
    /// deadline does not fit in a `u64`; `Closed` or `Panicked` when the runtime no longer
    /// takes work.
    pub fn sleep(&self, ticks: u64) -> Result<crate::sleep::Sleep, crate::sleep::SleepError> {
        let root = self.control.open_core()?;
        let deadline = unsafe { (*root).timers.current_tick() }
            .checked_add(ticks)
            .ok_or(crate::sleep::SleepError::DeadlineOverflow)?;
        Ok(crate::sleep::Sleep::new(deadline, self.control.clone()))
    }

    /// Starts a task, detached: its return value is dropped when it completes.
    ///
    /// The task does not run inside `spawn`. It first runs in the next [`Runtime::poll`],
    /// or, when a running task spawns it, in a later round of the current poll. For a task
    /// function written as a plain `fn` returning a future, the function's body runs here,
    /// to build the future.
    ///
    /// If that body shuts down or drops the runtime, the runtime closes at once and its
    /// tasks, this one included, are dropped before `spawn` returns `Ok`.
    ///
    /// # Errors
    ///
    /// A closed or panicked runtime returns [`SpawnError`] with the token.
    ///
    /// # Panics
    ///
    /// At most 65,535 distinct threads in a process can spawn tasks; a spawn on the next
    /// new thread panics.
    #[inline]
    pub fn spawn<T: Task>(&self, token: SpawnToken<T>) -> Result<(), SpawnError<T>> {
        self.submit(token, None)
    }

    /// As [`Self::spawn`], and also returns a [`JoinHandle`] that resolves to the task's
    /// return value. Allocates once per spawn.
    pub fn spawn_join<T: Task>(
        &self,
        token: SpawnToken<T>,
    ) -> Result<JoinHandle<T::Output>, SpawnError<T>> {
        let (sender, receiver) = oneshot::channel();
        self.submit(token, Some(sender))?;
        Ok(JoinHandle::new(receiver))
    }

    #[inline]
    fn submit<T: Task>(
        &self,
        token: SpawnToken<T>,
        join: Option<Sender<T::Output>>,
    ) -> Result<(), SpawnError<T>> {
        match self.control.open_core() {
            Ok(root) => {
                let _deferred = DeferredShutdown(&self.control);
                unsafe { Executor::spawn(root, token, join) };
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
            let _continuation = CleanupContinuation(self.0);
            self.0.shutdown();
        }
    }
}

/// The executor of an open runtime, for white-box tests of executor state.
#[cfg(test)]
pub(crate) fn core(runtime: &Runtime) -> *mut Executor {
    runtime.control.open_core().unwrap()
}

#[cfg(test)]
#[path = "runtime/tests.rs"]
mod tests;
