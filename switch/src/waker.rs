//! Waker implementation.
//!
//! # Design
//!
//! Ordinary Wakers borrow an immutable frame for one poll. Clone panics;
//! foreign-thread calls reject before accessing the local task or executor.
//!
//! User code uses `wake_by_ref()` for self-waking. Runtime primitives (reactor,
//! channels, timers) use the unsafe `StoredWaker` API.
//!
//! Ordinary Wakers remain shareable; local handles have stricter trait bounds.
//! ```
//! fn require_send_sync<T: Send + Sync>() {}
//! require_send_sync::<std::task::Waker>();
//! ```

use std::cell::Cell;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::task::{Context, RawWaker, RawWakerVTable, Waker};
use std::thread::ThreadId;

use crate::executor::wake_task;
use crate::task::TaskHeader;

thread_local! {
    static THREAD_ID: ThreadId = std::thread::current().id();
}

#[inline]
pub(crate) fn current_thread_id() -> ThreadId {
    THREAD_ID.with(|id| *id)
}

/// Immutable for the entire borrowed Context use, including scoped-thread joins.
pub(crate) struct WakeFrame {
    pub(crate) owner: ThreadId,
    pub(crate) task: *mut TaskHeader,
}

/// Read only immutable owner identity until foreign-thread access is rejected.
#[inline]
unsafe fn checked_task(data: *const ()) -> NonNull<TaskHeader> {
    unsafe {
        let frame = data.cast::<WakeFrame>();
        assert!(
            (*frame).owner == current_thread_id(),
            "waker used on foreign thread"
        );
        NonNull::new_unchecked((*frame).task)
    }
}

/// Thin waker - 1 word, no vtable indirection, !Send, !Sync.
///
/// Lifetime-bounded to the poll context. Cannot be stored or sent across threads.
/// Uses `NonNull` for niche optimization (`Option<ThinWaker>` is 1 word).
///
/// ```compile_fail
/// fn require_send<T: Send>() {}
/// require_send::<switch::ThinWaker<'static>>();
/// ```
/// ```compile_fail
/// fn require_sync<T: Sync>() {}
/// require_sync::<switch::ThinWaker<'static>>();
/// ```
#[derive(Clone, Copy, Debug)]
pub struct ThinWaker<'a> {
    ptr: NonNull<TaskHeader>,
    // NonNull excludes Send/Sync; the marker also excludes Sync and bounds lifetime.
    _marker: PhantomData<(&'a (), Cell<()>)>,
}

impl ThinWaker<'_> {
    /// Wake the task. Direct enqueue, no vtable indirection.
    #[inline]
    pub fn wake(self) {
        // SAFETY: Lifetime-bound to poll Context, task guaranteed alive.
        unsafe { wake_task(self.ptr.as_ptr()) }
    }
}

/// Storable waker for runtime primitives - 1 word, !Send, !Sync.
///
/// Unlike `ThinWaker`, this has no lifetime bound and can be stored in data structures.
/// It is unsafe to create because the caller must ensure the task outlives the waker.
/// Uses `NonNull` for niche optimization (`Option<StoredWaker>` is 1 word).
///
/// ```compile_fail
/// fn require_send<T: Send>() {}
/// require_send::<switch::StoredWaker>();
/// ```
/// ```compile_fail
/// fn require_sync<T: Sync>() {}
/// require_sync::<switch::StoredWaker>();
/// ```
#[derive(Clone, Copy, Debug)]
pub struct StoredWaker {
    ptr: NonNull<TaskHeader>,
    // !Send and !Sync - must stay on executor thread
    _marker: PhantomData<Cell<()>>,
}

impl StoredWaker {
    /// Create a storable waker from a poll context.
    ///
    /// # Safety
    ///
    /// The caller must ensure:
    /// - The `StoredWaker` is only used on the same thread as the executor
    /// - The `StoredWaker` is not used after the task completes
    /// - Typically: the runtime primitive cleans up stored wakers when tasks complete
    ///
    /// This is intended for reactor, timer, and channel implementations.
    #[inline]
    pub unsafe fn new(cx: &Context<'_>) -> Self {
        Self {
            ptr: task_from_waker(cx.waker()),
            _marker: PhantomData,
        }
    }

    /// Wake the task.
    #[inline]
    pub fn wake(self) {
        unsafe { wake_task(self.ptr.as_ptr()) }
    }
}

/// Get thin waker for current task from context.
///
/// The returned `ThinWaker` is:
/// - Lifetime-bounded to the context (cannot outlive poll)
/// - `!Send` and `!Sync` (cannot escape to other threads)
///
/// # Panics
/// Panics if the context's waker was not created by this executor, or if used
/// from a foreign thread.
#[inline]
pub fn current_task<'a>(cx: &'a Context<'_>) -> ThinWaker<'a> {
    ThinWaker {
        ptr: task_from_waker(cx.waker()),
        _marker: PhantomData,
    }
}

static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_waker, wake, wake_by_ref, drop_waker);

unsafe fn clone_waker(_data: *const ()) -> RawWaker {
    panic!(
        "Waker::clone() is not supported. Use cx.waker().wake_by_ref() for self-wake, \
         or StoredWaker::new() for runtime primitives."
    )
}

unsafe fn wake(data: *const ()) {
    unsafe { wake_by_ref(data) }
}

unsafe fn wake_by_ref(data: *const ()) {
    unsafe { wake_task(checked_task(data).as_ptr()) }
}

unsafe fn drop_waker(_: *const ()) {
    // No-op: no refcounting, task lifetime managed by executor
}

/// # Safety
/// Frame must remain stationary, immutable, and live through all Waker/Context
/// uses and scoped-thread joins, including unwind. Its task and stable owner core
/// must remain live until those uses end; owner must identify their owner thread.
/// Do not expose the owned Waker outside
/// that scope; clone is unsupported and only owner-thread wake may access task.
#[inline]
pub(crate) unsafe fn waker_from_frame(frame: &WakeFrame) -> Waker {
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::from_ref(frame).cast(), &VTABLE)) }
}

/// Active must be the caller core's currently registered, live poll frame.
/// Reject candidate identity without reading candidate data.
#[inline]
pub(crate) unsafe fn task_from_active_frame(
    waker: &Waker,
    active: *const WakeFrame,
) -> Option<NonNull<TaskHeader>> {
    if active.is_null() || !std::ptr::eq(waker.vtable(), &VTABLE) || waker.data() != active.cast() {
        return None;
    }
    Some(unsafe { checked_task(waker.data()) })
}

/// Extract task header from a waker (internal use).
#[inline]
pub(crate) fn task_from_waker(waker: &Waker) -> NonNull<TaskHeader> {
    assert!(
        std::ptr::eq(waker.vtable(), &VTABLE),
        "waker not from zhida executor"
    );
    // Only the internal constructor supplies this vtable, with a live frame.
    unsafe { checked_task(waker.data()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_waker_sizes() {
        assert_eq!(std::mem::size_of::<ThinWaker<'_>>(), 8);
        assert_eq!(std::mem::size_of::<StoredWaker>(), 8);
        assert_eq!(std::mem::size_of::<Waker>(), 16);
        // Niche optimization: Option<StoredWaker> is same size as StoredWaker
        assert_eq!(std::mem::size_of::<Option<StoredWaker>>(), 8);
        assert_eq!(std::mem::size_of::<Option<ThinWaker<'_>>>(), 8);
    }
}
