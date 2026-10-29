//! Thread tags, the task waker, and `StoredWaker`.
//!
//! Every task `Waker` shares one vtable; its data is the task header pointer, so
//! all wakers of one slot compare equal under `will_wake`. Every operation first
//! checks the block's owner thread tag and panics on a foreign thread; only then
//! does it touch the task or runtime.
//!
//! Ordinary Wakers remain shareable; local handles have stricter trait bounds.
//! ```
//! fn require_send_sync<T: Send + Sync>() {}
//! require_send_sync::<std::task::Waker>();
//! ```

use std::cell::Cell;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, Ordering};
use std::task::{Context, RawWaker, RawWakerVTable, Waker};

use crate::executor::Executor;
use crate::task::TaskHeader;
use crate::task::block::Meta;

thread_local! {
    pub(crate) static THREAD_TAG: Cell<u16> = const { Cell::new(0) };
}

static NEXT_TAG: AtomicU32 = AtomicU32::new(1);

/// Issue the next tag from `counter`. Tags are `1..=u16::MAX`; after the last one
/// the counter stops for good, so a tag is never reissued and 0 never issued.
fn issue_tag(counter: &AtomicU32) -> Option<u16> {
    let mut next = counter.load(Ordering::Relaxed);
    loop {
        // The counter starts at 1 and stops at `u16::MAX + 1`.
        let tag = u16::try_from(next).ok()?;
        match counter.compare_exchange_weak(next, next + 1, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return Some(tag),
            Err(current) => next = current,
        }
    }
}

#[inline]
pub(crate) fn this_thread_tag() -> u16 {
    THREAD_TAG.with(|tag| {
        let current = tag.get();
        if current != 0 {
            return current;
        }
        let next = issue_tag(&NEXT_TAG).expect("thread tag space exhausted");
        tag.set(next);
        next
    })
}

#[cold]
#[inline(never)]
fn foreign_thread() -> ! {
    panic!("waker used on foreign thread")
}

/// Reject a foreign thread before any other access to the task.
///
/// # Safety
/// `header` heads a slot of a block (blocks are never freed).
#[inline(always)]
pub(crate) unsafe fn check_owner(header: *const TaskHeader) {
    if unsafe { Meta::owner(TaskHeader::meta(header)) } != THREAD_TAG.with(Cell::get) {
        foreign_thread();
    }
}

pub(crate) static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

/// The raw waker of the task headed by `header`.
#[inline(always)]
pub(crate) fn raw(header: *const TaskHeader) -> RawWaker {
    RawWaker::new(header.cast(), &VTABLE)
}

unsafe fn clone(header: *const ()) -> RawWaker {
    unsafe { check_owner(header.cast()) };
    raw(header.cast())
}

unsafe fn wake(header: *const ()) {
    unsafe { wake_by_ref(header) }
}

unsafe fn wake_by_ref(header: *const ()) {
    unsafe {
        check_owner(header.cast());
        Executor::wake(header.cast());
    }
}

unsafe fn drop(header: *const ()) {
    unsafe { check_owner(header.cast()) };
}

/// A copyable wake handle for runtime primitives that park tasks. `wake`
/// enqueues the task directly, with no call through a waker vtable.
///
/// Like a stale `Waker`, a stale `StoredWaker` is harmless: when its slot is free
/// its wake does nothing, and when the slot holds a newer task that task gets one
/// more poll.
///
/// ```compile_fail
/// fn require_send<T: Send>() {}
/// require_send::<switch::StoredWaker>();
/// ```
/// ```compile_fail
/// fn require_sync<T: Sync>() {}
/// require_sync::<switch::StoredWaker>();
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoredWaker {
    header: NonNull<TaskHeader>,
    _local: PhantomData<*const ()>,
}

impl StoredWaker {
    /// The task whose waker `cx` carries.
    ///
    /// Safe because: a waker with switch's vtable was made by switch (a forged
    /// one needs `Waker::from_raw`, whose contract the forger breaks), so its
    /// data heads a slot of a task block. Blocks are never freed, so the header
    /// and its block metadata stay readable for the life of the process, whatever
    /// happens to the task or runtime. The owner check pins the handle to the
    /// block's thread, and being `!Send`/`!Sync` it never leaves; so every later
    /// `wake` runs on the only thread that writes the slot's flags and runtime.
    /// `wake` reads the runtime only when the slot is LIVE, and a LIVE slot's
    /// runtime is alive.
    ///
    /// # Panics
    /// If `cx`'s waker is not a switch task waker, or on a foreign thread.
    pub fn new(cx: &Context<'_>) -> Self {
        let waker = cx.waker();
        assert!(
            std::ptr::eq(waker.vtable(), &VTABLE),
            "StoredWaker needs a switch task waker"
        );
        let header = waker.data().cast::<TaskHeader>();
        unsafe { check_owner(header) };
        Self {
            header: NonNull::new(header.cast_mut()).expect("switch waker data is a header"),
            _local: PhantomData,
        }
    }

    /// Wake the task.
    #[inline]
    pub fn wake(self) {
        unsafe { Executor::wake(self.header.as_ptr()) }
    }

    /// Wake the task for the next [`crate::Runtime::poll`], not this one: unlike
    /// [`Self::wake`], it is not polled again in the current poll's later rounds,
    /// unless an ordinary wake already queued it (then this does nothing). A
    /// later ordinary wake on the task's thread (any `Waker`, `StoredWaker::wake`
    /// or `RemoteWaker`) upgrades the task: it runs in this poll's next round
    /// after all. A `RemoteWaker` wake from another thread lands at the start of
    /// the next poll, when the deferred task runs anyway.
    #[inline]
    pub fn wake_deferred(&self) {
        unsafe { Executor::wake_deferred(self.header.as_ptr()) }
    }
}

/// Lets the runtime's other tasks run: the first poll wakes the task for the
/// next [`crate::Runtime::poll`] ([`StoredWaker::wake_deferred`]) and returns
/// `Pending`, the second returns `Ready`.
///
/// # Panics
/// When polled outside a switch task, like [`StoredWaker::new`].
pub fn yield_now() -> YieldNow {
    YieldNow { yielded: false }
}

/// The future of [`yield_now`].
#[must_use = "futures do nothing unless awaited"]
#[derive(Debug)]
pub struct YieldNow {
    yielded: bool,
}

impl Future for YieldNow {
    type Output = ();

    #[inline]
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> std::task::Poll<()> {
        if self.yielded {
            return std::task::Poll::Ready(());
        }
        self.yielded = true;
        StoredWaker::new(cx).wake_deferred();
        std::task::Poll::Pending
    }
}

/// Whether `waker` is the waker of the task headed by `header`.
#[inline]
pub(crate) fn is_task_waker(waker: &Waker, header: *const TaskHeader) -> bool {
    std::ptr::eq(waker.vtable(), &VTABLE) && waker.data() == header.cast()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tag_exhaustion_is_permanent_and_never_reissues() {
        let counter = AtomicU32::new(u32::from(u16::MAX) - 1);
        assert_eq!(issue_tag(&counter), Some(u16::MAX - 1));
        assert_eq!(issue_tag(&counter), Some(u16::MAX));
        for _ in 0..4 {
            assert_eq!(issue_tag(&counter), None);
        }
        assert_eq!(counter.load(Ordering::Relaxed), u32::from(u16::MAX) + 1);
    }

    #[test]
    fn tags_start_at_one() {
        let counter = AtomicU32::new(1);
        assert_eq!(issue_tag(&counter), Some(1));
        assert_eq!(issue_tag(&counter), Some(2));
    }
}
