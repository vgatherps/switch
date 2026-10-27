//! Lazy timer future, bound permanently to its originating runtime.

use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use crate::runtime::{Control, Unavailable};
use crate::timer_registration::Token;
use crate::waker::task_from_active_frame;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Failure to create, register, or poll a runtime-bound sleep.
pub enum SleepError {
    Closed,
    Panicked,
    DeadlineOverflow,
    WrongRuntime,
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

/// Future that completes when its runtime reaches its deadline.
///
/// A `Sleep` is local to its originating runtime and is `!Send + !Sync`.
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
            // Every token is registered through this Sleep's immutable Control.
            unsafe {
                self.control.cancel_timer(token);
            }
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
            let Some(task) = task_from_active_frame(cx.waker(), (*root).active_frame) else {
                return this.complete(Err(SleepError::WrongRuntime));
            };
            if let Some(token) = this.token
                && (*root).timers.refresh(token, task.as_ptr())
            {
                return Poll::Pending;
            }
            match (*root).timers.register(task.as_ptr(), this.deadline) {
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
