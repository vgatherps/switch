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
    /// The sender is gone; set before its wake.
    closed: Cell<bool>,
    waiter: Cell<Option<Waker>>,
}

impl<T> Shared<T> {
    fn result(&self) -> Option<Result<T, Canceled>> {
        if let Some(value) = self.value.take() {
            return Some(Ok(value));
        }
        self.closed.get().then_some(Err(Canceled))
    }
}

/// A connected sender and receiver.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Rc::new(Shared {
        value: Cell::new(None),
        closed: Cell::new(false),
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
        // Published first, so a waker that polls the receiver inline sees it.
        self.0.closed.set(true);
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
        if let Some(result) = shared.result() {
            return Poll::Ready(result);
        }
        let (waiter, replaced) = match shared.waiter.take() {
            Some(waiter) if waiter.will_wake(cx.waker()) => (waiter, None),
            replaced => (cx.waker().clone(), replaced),
        };
        shared.waiter.set(Some(waiter));
        // Dropping or cloning a waker runs foreign code, which may have sent or
        // dropped the sender while no waker was stored.
        drop(replaced);
        match shared.result() {
            Some(result) => Poll::Ready(result),
            None => Poll::Pending,
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        drop(self.0.waiter.take());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;

    /// Counts its wakes.
    #[derive(Default)]
    struct Count(AtomicUsize);

    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn poll<T>(receiver: &mut Receiver<T>, waker: &Waker) -> Poll<Result<T, Canceled>> {
        Pin::new(receiver).poll(&mut Context::from_waker(waker))
    }

    thread_local! {
        static INLINE: RefCell<Option<Receiver<u32>>> = const { RefCell::new(None) };
        static SEEN: Cell<Option<Poll<Result<u32, Canceled>>>> = const { Cell::new(None) };
    }

    /// Polls `INLINE` from inside its wake.
    struct PollsInline;

    impl Wake for PollsInline {
        fn wake(self: Arc<Self>) {
            let waker = Waker::noop();
            INLINE.with_borrow_mut(|receiver| {
                SEEN.set(Some(poll(receiver.as_mut().unwrap(), waker)));
            });
        }
    }

    #[test]
    fn a_waker_polling_inline_during_cancel_sees_canceled() {
        let (sender, receiver) = channel::<u32>();
        INLINE.set(Some(receiver));
        let waker = Waker::from(Arc::new(PollsInline));
        INLINE.with_borrow_mut(|receiver| {
            assert_eq!(poll(receiver.as_mut().unwrap(), &waker), Poll::Pending);
        });
        drop(sender);
        assert_eq!(SEEN.take(), Some(Poll::Ready(Err(Canceled))));
        INLINE.take();
    }

    #[test]
    fn a_replaced_waker_is_not_woken() {
        let (sender, mut receiver) = channel::<u32>();
        let (a, b) = (Arc::new(Count::default()), Arc::new(Count::default()));
        assert_eq!(poll(&mut receiver, &Waker::from(a.clone())), Poll::Pending);
        assert_eq!(poll(&mut receiver, &Waker::from(b.clone())), Poll::Pending);
        sender.send(7).unwrap();
        assert_eq!(a.0.load(Ordering::Relaxed), 0);
        assert_eq!(b.0.load(Ordering::Relaxed), 1);
        assert_eq!(poll(&mut receiver, Waker::noop()), Poll::Ready(Ok(7)));
    }

    thread_local! {
        static HELD: Cell<Option<Sender<u32>>> = const { Cell::new(None) };
    }

    /// Drops `HELD` (the sender) when the last reference to it goes.
    struct DropsSender;

    impl Wake for DropsSender {
        fn wake(self: Arc<Self>) {}
    }

    impl Drop for DropsSender {
        fn drop(&mut self) {
            HELD.take();
        }
    }

    #[test]
    fn cancel_while_the_waker_is_replaced_is_seen() {
        let (sender, mut receiver) = channel::<u32>();
        HELD.set(Some(sender));
        // Only the receiver keeps waker `a`; replacing it drops the sender.
        assert_eq!(
            poll(&mut receiver, &Waker::from(Arc::new(DropsSender))),
            Poll::Pending
        );
        let b = Arc::new(Count::default());
        assert_eq!(
            poll(&mut receiver, &Waker::from(b.clone())),
            Poll::Ready(Err(Canceled))
        );
    }

    #[test]
    fn a_dropped_receiver_releases_its_waker() {
        let (sender, mut receiver) = channel::<u32>();
        let count = Arc::new(Count::default());
        assert_eq!(
            poll(&mut receiver, &Waker::from(count.clone())),
            Poll::Pending
        );
        drop(receiver);
        assert_eq!(Arc::strong_count(&count), 1);
        assert_eq!(sender.send(3), Err(3));
        assert_eq!(count.0.load(Ordering::Relaxed), 0);
    }
}
