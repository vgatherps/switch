//! Task results: the slot future wrapper and [`JoinHandle`].
//!
//! Every task slot holds a [`Joinable`]: the task's future and an optional
//! oneshot sender, so detached and joined spawns of one task function share one
//! future type and one pool. At completion the value is sent (or dropped when
//! there is no sender) and the slot is freed as usual. A future dropped before
//! completion drops its sender, which cancels the handle.

use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::oneshot::{Canceled, Receiver, Sender};

/// The slot future: a task's future and the sender of a joined spawn.
///
/// The sender is handled out of line, so a detached task's poll and drop stay
/// leaf code with one extra load and branch.
pub(crate) struct Joinable<Fut: Future> {
    future: Fut,
    join: ManuallyDrop<Option<Sender<Fut::Output>>>,
}

impl<Fut: Future> Joinable<Fut> {
    #[inline]
    pub(crate) fn new(future: Fut, join: Option<Sender<Fut::Output>>) -> Self {
        Self {
            future,
            join: ManuallyDrop::new(join),
        }
    }
}

#[cold]
#[inline(never)]
fn send<T>(join: &mut Option<Sender<T>>, value: T) {
    if let Some(join) = join.take() {
        // A dropped handle gives the value back; it is dropped here.
        let _ = join.send(value);
    }
}

#[cold]
#[inline(never)]
fn cancel<T>(join: &mut Option<Sender<T>>) {
    // Dropping the sender before it sent cancels the handle.
    drop(join.take());
}

impl<Fut: Future> Future for Joinable<Fut> {
    type Output = ();

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: `future` is structurally pinned: it is never moved out.
        let this = unsafe { self.get_unchecked_mut() };
        if this.join.is_some() {
            return poll_joined(this, cx);
        }
        // Detached: the value is dropped here.
        match unsafe { Pin::new_unchecked(&mut this.future) }.poll(cx) {
            Poll::Ready(_) => Poll::Ready(()),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Drops the sender if the task's poll unwinds, so the handle is canceled at
/// once rather than when the panicked runtime shuts down.
struct CancelOnUnwind<'a, T>(&'a mut Option<Sender<T>>);

impl<T> Drop for CancelOnUnwind<'_, T> {
    fn drop(&mut self) {
        cancel(self.0);
    }
}

#[inline(never)]
fn poll_joined<Fut: Future>(this: &mut Joinable<Fut>, cx: &mut Context<'_>) -> Poll<()> {
    let guard = CancelOnUnwind(&mut this.join);
    // SAFETY: as in `poll`.
    let poll = unsafe { Pin::new_unchecked(&mut this.future) }.poll(cx);
    std::mem::forget(guard);
    match poll {
        Poll::Ready(value) => {
            send(&mut this.join, value);
            Poll::Ready(())
        }
        Poll::Pending => Poll::Pending,
    }
}

impl<Fut: Future> Drop for Joinable<Fut> {
    #[inline]
    fn drop(&mut self) {
        if self.join.is_some() {
            cancel(&mut self.join);
        }
    }
}

/// The result of a task spawned with [`Spawner::spawn_join`](crate::Spawner::spawn_join).
///
/// Resolves to `Ok` with the task's return value, at once if the task already finished.
/// Resolves to `Err(Canceled)` if the task ends without a value: its runtime was shut down
/// or dropped first, or its poll panicked (then at once). Dropping the handle detaches the
/// task without stopping it: its value is dropped when it completes, or now if it already
/// did. Neither `Send` nor `Sync`.
///
/// # Panics
///
/// When polled by anything other than a switch task while the task is still running.
#[must_use = "dropping a JoinHandle detaches its task"]
pub struct JoinHandle<T>(Receiver<T>);

impl<T> std::fmt::Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinHandle").finish_non_exhaustive()
    }
}

impl<T> JoinHandle<T> {
    pub(crate) fn new(receiver: Receiver<T>) -> Self {
        Self(receiver)
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, Canceled>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}
