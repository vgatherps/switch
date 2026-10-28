//! A single-thread channel for one value.
//!
//! [`channel`] allocates one shared cell for the value and one waiting waker.
//! Neither half is `Send` or `Sync`; the receiver accepts any waker.

use std::cell::Cell;
use std::fmt;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

/// The sender was dropped without sending a value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Canceled;

impl fmt::Display for Canceled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("oneshot sender dropped without sending")
    }
}

impl std::error::Error for Canceled {}

struct Shared<T> {
    value: Cell<Option<T>>,
    waiter: Cell<Option<Waker>>,
}

/// A connected sender and receiver.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Rc::new(Shared {
        value: Cell::new(None),
        waiter: Cell::new(None),
    });
    (Sender(shared.clone()), Receiver(shared))
}

/// Sends the channel's one value. Dropping it without sending makes the
/// receiver resolve to `Err(Canceled)`.
pub struct Sender<T>(Rc<Shared<T>>);

impl<T> Sender<T> {
    /// Gives the value back when the receiver is gone.
    pub fn send(self, value: T) -> Result<(), T> {
        // Each half holds one count; there are no other references.
        if Rc::strong_count(&self.0) == 1 {
            return Err(value);
        }
        self.0.value.set(Some(value));
        // Dropping `self` wakes the receiver.
        Ok(())
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender").finish_non_exhaustive()
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if let Some(waiter) = self.0.waiter.take() {
            waiter.wake();
        }
    }
}

/// Resolves to the sent value, or `Err(Canceled)` once the sender is dropped
/// without sending. Dropping it makes a later send give the value back.
pub struct Receiver<T>(Rc<Shared<T>>);

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, Canceled>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let shared = &self.0;
        if let Some(value) = shared.value.take() {
            return Poll::Ready(Ok(value));
        }
        if Rc::strong_count(shared) == 1 {
            return Poll::Ready(Err(Canceled));
        }
        let waiter = match shared.waiter.take() {
            Some(waiter) if waiter.will_wake(cx.waker()) => waiter,
            _ => cx.waker().clone(),
        };
        shared.waiter.set(Some(waiter));
        Poll::Pending
    }
}
