//! Lazy timer future, bound permanently to its originating runtime.

use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use crate::runtime::{Control, Unavailable};
use crate::timer_registration::Token;

/// Why a sleep could not be created, or why awaiting one failed. Returned by
/// [`Spawner::sleep`](crate::Spawner::sleep), [`Spawner::sleep_until`](crate::Spawner::sleep_until)
/// and awaiting a [`Sleep`].
///
/// Implements `Debug` but not `Display` or `std::error::Error`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SleepError {
    /// The runtime was shut down or dropped. At creation, or awaiting a sleep afterwards.
    Closed,
    /// A task of the runtime panicked, so it takes no more work. At creation, or when
    /// awaited.
    Panicked,
    /// `sleep(ticks)`: the latest accepted tick plus `ticks` does not fit in a `u64`.
    /// `sleep_until` never returns it.
    DeadlineOverflow,
    /// The sleep was polled, with its deadline still ahead, by something other than a task
    /// of its own runtime: a task of another runtime, or a future polled outside any task.
    WrongRuntime,
    /// The runtime cannot register another timer: about four billion (`u32::MAX`) timer
    /// entries already exist.
    RegistrationExhausted,
}

impl From<Unavailable> for SleepError {
    fn from(value: Unavailable) -> Self {
        match value {
            Unavailable::Closed => Self::Closed,
            Unavailable::Panicked => Self::Panicked,
        }
    }
}

/// A future that completes when its runtime's tick reaches a deadline. From
/// [`Spawner::sleep`](crate::Spawner::sleep) and
/// [`Spawner::sleep_until`](crate::Spawner::sleep_until).
///
/// It resolves to `Ok(())` during the first [`Runtime::poll`](crate::Runtime::poll) whose
/// tick is at or past the deadline; that poll wakes the polling task. A deadline already
/// reached completes on the first poll without parking. Nothing fires between polls, so a
/// sleep fires at the loop's first poll after the deadline.
///
/// Creating a `Sleep` registers nothing; its timer is registered when it is first polled
/// with the deadline still ahead. Dropping it cancels the timer. Each poll points the timer
/// at the polling task, so a sleep created in one task can be awaited in another. A sleep
/// that outlives the last task that polled it still fires at its deadline, giving at most
/// one extra poll to whichever task took that task's place.
///
/// Bound to the runtime that created it; neither `Send` nor `Sync`. See [`SleepError`]
/// for failures.
pub struct Sleep {
    deadline: u64,
    control: Rc<Control>,
    token: Option<Token>,
    result: Option<Result<(), SleepError>>,
}

impl Sleep {
    pub(crate) fn new(deadline: u64, control: Rc<Control>) -> Self {
        Self {
            deadline,
            control,
            token: None,
            result: None,
        }
    }

    fn cancel(&mut self) {
        if let Some(token) = self.token.take() {
            self.control.cancel_timer(token);
        }
    }

    fn complete(&mut self, result: Result<(), SleepError>) -> Poll<Result<(), SleepError>> {
        self.cancel();
        self.result = Some(result);
        Poll::Ready(result)
    }
}

impl Future for Sleep {
    type Output = Result<(), SleepError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(result) = this.result {
            return Poll::Ready(result);
        }
        let root = match this.control.open_core() {
            Ok(root) => root,
            Err(error) => return this.complete(Err(error.into())),
        };
        unsafe {
            if this.deadline <= (*root).timers.current_tick() {
                return this.complete(Ok(()));
            }
            let task = (*root).current;
            if task.is_null() || !crate::waker::is_task_waker(cx.waker(), task) {
                return this.complete(Err(SleepError::WrongRuntime));
            }
            if let Some(token) = this.token
                && (*root).timers.refresh(token, task)
            {
                return Poll::Pending;
            }
            match (*root).timers.register(task, this.deadline) {
                Ok(token) => {
                    this.token = Some(token);
                    Poll::Pending
                }
                Err(_) => this.complete(Err(SleepError::RegistrationExhausted)),
            }
        }
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[cfg(test)]
mod tests;
