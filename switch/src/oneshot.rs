//! A channel for one value, received by a switch task.
//!
//! [`channel`] makes one allocation: a state word, the value and the waiting
//! task. The [`Sender`] is `Send` (for `T: Send`) and may send or be dropped on
//! any thread; the [`Receiver`] stays on its task's thread and must be polled by
//! a switch task, whose [`RemoteWaker`] it keeps.
//!
//! ```compile_fail
//! fn require_send<T: Send>() {}
//! require_send::<switch::oneshot::Receiver<u8>>();
//! ```

use std::cell::UnsafeCell;
use std::fmt;
use std::marker::PhantomData;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::pin::Pin;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicPtr, AtomicU8, Ordering};
use std::task::{Context, Poll};

use crate::RemoteWaker;
use crate::task::TaskHeader;

/// The sender was dropped without sending a value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Canceled;

impl fmt::Display for Canceled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("oneshot sender dropped without sending")
    }
}

impl std::error::Error for Canceled {}

/// The sender exists. Cleared by its drop (or `send`) as its last access.
const SENDER: u8 = 1;
/// The receiver exists. Cleared by its drop as its last access.
const RECEIVER: u8 = 2;
/// The sender sent or was dropped; set before its wake, while `SENDER` is set.
const CLOSED: u8 = 4;
/// `value` holds a sent value not yet taken.
const VALUE: u8 = 8;

/// The one allocation. Whoever clears the last of `SENDER` and `RECEIVER`
/// frees it (and drops an untaken value).
struct Shared<T> {
    state: AtomicU8,
    /// The receiving task's header, or null before its first poll.
    waiter: AtomicPtr<TaskHeader>,
    value: UnsafeCell<MaybeUninit<T>>,
}

/// A connected sender and receiver.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    // Freed by `release`, from whichever side drops last.
    let shared = Box::into_raw(Box::new(Shared {
        state: AtomicU8::new(SENDER | RECEIVER),
        waiter: AtomicPtr::new(ptr::null_mut()),
        value: UnsafeCell::new(MaybeUninit::uninit()),
    }));
    let shared = NonNull::new(shared).expect("Box is never null");
    (
        Sender { shared },
        Receiver {
            shared,
            _local: PhantomData,
        },
    )
}

/// Clear `side`'s liveness bit; free the allocation if the other side is gone.
///
/// # Safety
/// `shared` is live and `side` is the caller's own bit, cleared once; the caller
/// does not use `shared` afterwards.
unsafe fn release<T>(shared: NonNull<Shared<T>>, side: u8) {
    unsafe {
        // AcqRel: the freeing side sees every write the other side made.
        let previous = shared.as_ref().state.fetch_and(!side, Ordering::AcqRel);
        if previous & (SENDER | RECEIVER) == side {
            let shared = Box::from_raw(shared.as_ptr());
            if previous & VALUE != 0 {
                (*shared.value.get()).assume_init_drop();
            }
        }
    }
}

/// Sends the channel's one value, from any thread. Dropping it without sending
/// makes the receiver resolve to `Err(Canceled)`.
pub struct Sender<T> {
    shared: NonNull<Shared<T>>,
}

// SAFETY: the sender only writes the value (moved to the receiver's thread) and
// RMWs the state; `T: Send` lets the value cross.
unsafe impl<T: Send> Send for Sender<T> {}

impl<T> Sender<T> {
    /// Gives the value back when the receiver is gone.
    pub fn send(self, value: T) -> Result<(), T> {
        let this = ManuallyDrop::new(self);
        let shared = this.shared;
        unsafe {
            // The receiver reads `value` only after seeing VALUE.
            (*shared.as_ref().value.get()).write(value);
            let previous = shared
                .as_ref()
                .state
                .fetch_or(VALUE | CLOSED, Ordering::SeqCst);
            if previous & RECEIVER == 0 {
                // The receiver is gone and never reads it: take the value back.
                shared.as_ref().state.fetch_and(!VALUE, Ordering::Relaxed);
                let value = (*shared.as_ref().value.get()).assume_init_read();
                release(shared, SENDER);
                return Err(value);
            }
            wake(shared);
            release(shared, SENDER);
        }
        Ok(())
    }
}

/// Wake the receiving task, if it registered. `SENDER` is still set.
///
/// # Safety
/// `shared` is live.
unsafe fn wake<T>(shared: NonNull<Shared<T>>) {
    // SeqCst with the receiver's store-then-load: either it sees CLOSED or this
    // sees its task.
    let waiter = unsafe { shared.as_ref().waiter.load(Ordering::SeqCst) };
    if let Some(header) = NonNull::new(waiter) {
        // A stale header (the task finished or moved) gets a harmless wake.
        // SAFETY: only `Receiver::poll` stores it, from `RemoteWaker::header`.
        unsafe { RemoteWaker::from_header(header) }.wake();
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
            let previous = self
                .shared
                .as_ref()
                .state
                .fetch_or(CLOSED, Ordering::SeqCst);
            // A dropped receiver's task may be gone and its slot reused.
            if previous & RECEIVER != 0 {
                wake(self.shared);
            }
            release(self.shared, SENDER);
        }
    }
}

/// Resolves to the sent value, or `Err(Canceled)` once the sender is dropped
/// without sending. Dropping it makes a later send give the value back.
///
/// # Panics
/// When polled outside a switch task (it keeps a [`RemoteWaker`]) while the
/// channel is not done; once a value was sent or the sender dropped, any
/// poller gets `Ready`.
pub struct Receiver<T> {
    shared: NonNull<Shared<T>>,
    _local: PhantomData<*const ()>,
}

impl<T> Receiver<T> {
    /// The value or cancellation, if the sender is done.
    fn result(&self, state: u8) -> Option<Result<T, Canceled>> {
        if state & VALUE != 0 {
            unsafe {
                let shared = self.shared.as_ref();
                let value = (*shared.value.get()).assume_init_read();
                shared.state.fetch_and(!VALUE, Ordering::Relaxed);
                return Some(Ok(value));
            }
        }
        (state & CLOSED != 0).then_some(Err(Canceled))
    }
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
        if let Some(result) = self.result(shared.state.load(Ordering::Acquire)) {
            return Poll::Ready(result);
        }
        // Register (or replace) the waiting task, then look again: a send between
        // the two loads is seen here, or its wake reaches this task.
        let waiter = RemoteWaker::new(cx).header();
        shared.waiter.store(waiter.as_ptr(), Ordering::SeqCst);
        match self.result(shared.state.load(Ordering::SeqCst)) {
            Some(result) => Poll::Ready(result),
            None => Poll::Pending,
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        unsafe { release(self.shared, RECEIVER) }
    }
}
