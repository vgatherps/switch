//! A single-threaded channel for one value, received by a switch task.
//!
//! [`channel`] makes one allocation: whether one side left, the value and the
//! waiting task. Neither side is `Send` or `Sync`, so both stay on the
//! thread that made them. The [`Receiver`] must be polled by a switch task,
//! whose [`StoredWaker`] it keeps.
//!
//! ```compile_fail
//! fn require_send<T: Send>() {}
//! require_send::<switch::oneshot::Sender<u8>>();
//! ```
//! ```compile_fail
//! fn require_send<T: Send>() {}
//! require_send::<switch::oneshot::Receiver<u8>>();
//! ```

use std::cell::Cell;
use std::fmt;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll};

use crate::StoredWaker;

/// The sender was dropped without sending a value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Canceled;

impl fmt::Display for Canceled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("oneshot sender dropped without sending")
    }
}

impl std::error::Error for Canceled {}

/// The one allocation, freed by whichever side goes last (dropping an untaken
/// value with it).
struct Shared<T> {
    /// One side is gone: for the sender, the receiver dropped; for the
    /// receiver, the sender sent or dropped.
    alone: Cell<bool>,
    /// The receiving task, from its latest pending poll.
    waiter: Cell<Option<StoredWaker>>,
    /// The sent value, until the receiver takes it.
    value: Cell<Option<T>>,
}

/// A connected sender and receiver.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    // Freed by `free`, by whichever side goes last.
    let shared = Box::into_raw(Box::new(Shared {
        alone: Cell::new(false),
        waiter: Cell::new(None),
        value: Cell::new(None),
    }));
    let shared = NonNull::new(shared).expect("Box is never null");
    (Sender { shared }, Receiver { shared })
}

/// Free the allocation and an untaken value.
///
/// # Safety
/// The caller is the last side and does not use `shared` afterwards.
unsafe fn free<T>(shared: NonNull<Shared<T>>) {
    drop(unsafe { Box::from_raw(shared.as_ptr()) });
}

/// Wake the receiving task, if it polled. The receiver is alive.
fn wake<T>(shared: &Shared<T>) {
    if let Some(waiter) = shared.waiter.get() {
        // A task that finished or handed the receiver on gets a harmless wake.
        waiter.wake();
    }
}

/// Sends the channel's one value. Dropping it without sending makes the
/// receiver resolve to `Err(Canceled)`.
pub struct Sender<T> {
    shared: NonNull<Shared<T>>,
}

impl<T> Sender<T> {
    /// Gives the value back when the receiver is gone.
    pub fn send(self, value: T) -> Result<(), T> {
        let this = ManuallyDrop::new(self);
        let shared = this.shared;
        unsafe {
            if shared.as_ref().alone.get() {
                free(shared);
                return Err(value);
            }
            shared.as_ref().value.set(Some(value));
            wake(shared.as_ref());
            shared.as_ref().alone.set(true);
        }
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
        unsafe {
            if self.shared.as_ref().alone.get() {
                // The receiver is gone; its task may be too, its slot reused.
                free(self.shared);
            } else {
                wake(self.shared.as_ref());
                self.shared.as_ref().alone.set(true);
            }
        }
    }
}

/// Resolves to the sent value, or `Err(Canceled)` once the sender is dropped
/// without sending. Dropping it makes a later send give the value back.
///
/// # Panics
/// When polled outside a switch task (it keeps a [`StoredWaker`]) while the
/// sender is alive; once a value was sent or the sender dropped, any poller
/// gets `Ready`.
pub struct Receiver<T> {
    shared: NonNull<Shared<T>>,
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, Canceled>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let shared = unsafe { self.shared.as_ref() };
        // The sender leaves only by sending or by being dropped.
        if shared.alone.get() {
            return Poll::Ready(shared.value.take().ok_or(Canceled));
        }
        // Replaces the task of an earlier poll, if the receiver moved.
        shared.waiter.set(Some(StoredWaker::new(cx)));
        Poll::Pending
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        unsafe {
            if self.shared.as_ref().alone.get() {
                free(self.shared);
            } else {
                self.shared.as_ref().alone.set(true);
            }
        }
    }
}
