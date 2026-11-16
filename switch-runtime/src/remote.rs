//! `RemoteWaker` and the per-runtime mailbox of wake requests from other threads.
//!
//! Each runtime owns one [`Mailbox`], allocated with it and never freed: its
//! blocks' metadata points at it, and a foreign thread may wake a task long after
//! the runtime is gone. A foreign wake pushes the task header under the lock; the
//! owner takes the requests at the start of its next turn and wakes each task
//! locally, where the slot's flags make a stale or finished task's wake a no-op.

use std::cell::{Cell, UnsafeCell};
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::Context;

use crate::executor::Executor;
use crate::task::TaskHeader;
use crate::task::block::Meta;
use crate::waker::{THREAD_TAG, VTABLE, check_owner};

/// A task header queued by a foreign thread.
struct Request(*const TaskHeader);

// SAFETY: only the header address travels; the owner thread dereferences it.
unsafe impl Send for Request {}

pub(crate) struct Mailbox {
    /// Requests are queued: a hint for the owner, set and cleared under the lock.
    pending: AtomicBool,
    /// The runtime shut down: requests are discarded. Set under the lock.
    closed: AtomicBool,
    queue: Mutex<Vec<Request>>,
    /// Owner thread only: the queue's previous buffer, swapped in at each
    /// delivery so a steady rate of remote wakes does not allocate per turn.
    spare: UnsafeCell<Vec<Request>>,
    /// The previously created mailbox: every mailbox stays reachable from
    /// [`MAILBOXES`] (for leak checkers), since none is ever freed.
    next: *mut Mailbox,
}

/// The most recently created mailbox, heading the list of all mailboxes.
static MAILBOXES: AtomicPtr<Mailbox> = AtomicPtr::new(ptr::null_mut());

impl Mailbox {
    /// A new open mailbox, never freed.
    pub(crate) fn create() -> *const Self {
        let mailbox = Box::into_raw(Box::new(Self {
            pending: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            queue: Mutex::new(Vec::new()),
            spare: UnsafeCell::new(Vec::new()),
            next: ptr::null_mut(),
        }));
        let mut head = MAILBOXES.load(Ordering::Relaxed);
        loop {
            unsafe { (*mailbox).next = head };
            match MAILBOXES.compare_exchange_weak(
                head,
                mailbox,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return mailbox,
                Err(current) => head = current,
            }
        }
    }

    fn lock(&self) -> MutexGuard<'_, Vec<Request>> {
        // Nothing runs user code under the lock, so a poisoned queue is intact.
        self.queue.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Queue a wake of `header`, or drop it once the runtime has shut down.
    fn push(&self, header: *const TaskHeader) {
        let mut queue = self.lock();
        if !self.closed.load(Ordering::Relaxed) {
            queue.push(Request(header));
            self.pending.store(true, Ordering::Relaxed);
        }
    }

    /// Whether wake requests may be queued; one load at the start of a turn.
    ///
    /// # Safety
    /// `this` came from [`Mailbox::create`].
    #[inline(always)]
    pub(crate) unsafe fn pending(this: *const Self) -> bool {
        unsafe { (*this).pending.load(Ordering::Relaxed) }
    }

    /// Wake every queued task locally.
    ///
    /// # Safety
    /// Owner thread of the runtime that created `this`; its executor is open.
    #[cold]
    #[inline(never)]
    pub(crate) unsafe fn deliver(this: *const Self) {
        unsafe {
            let requests = &mut *(*this).spare.get();
            {
                let mut queue = (*this).lock();
                (*this).pending.store(false, Ordering::Relaxed);
                std::mem::swap(&mut *queue, requests);
            }
            for Request(header) in requests.drain(..) {
                // A header of one of this runtime's blocks: the flags check makes
                // a free, finished or newer task's wake harmless.
                Executor::wake(header);
            }
        }
    }

    /// Discard queued and future requests, and free the buffers.
    ///
    /// # Safety
    /// Owner thread of the runtime that created `this`.
    pub(crate) unsafe fn close(this: *const Self) {
        let requests = unsafe {
            let mut queue = (*this).lock();
            (*this).closed.store(true, Ordering::Relaxed);
            (*this).pending.store(false, Ordering::Relaxed);
            std::mem::take(&mut *queue)
        };
        drop(requests);
        unsafe { drop(std::mem::take(&mut *(*this).spare.get())) };
    }
}

/// A handle that wakes one task from any thread. `Copy`, `Send` and `Sync`.
///
/// On the task's own thread `wake` is an ordinary wake. On any other thread it queues the
/// task in its runtime's mailbox, and the runtime makes it ready at the start of its next
/// [`Runtime::poll`](crate::Runtime::poll); the waking thread runs nothing. A task's
/// standard `Waker` behaves the same way when cloned, dropped or woken on another thread,
/// so code that hands its waker to a background thread works unchanged.
///
/// A `RemoteWaker` may outlive its task. Waking it then does nothing, or gives one extra
/// poll to a newer task that took the old task's place. After the runtime shut down, the
/// wake is dropped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RemoteWaker {
    header: NonNull<TaskHeader>,
}

// SAFETY: a foreign thread only reads the block's owner tag and mailbox, both
// written once before the block's first task (and so before this handle) existed,
// and pushes the header address; the header is dereferenced on the owner thread.
unsafe impl Send for RemoteWaker {}
unsafe impl Sync for RemoteWaker {}

impl RemoteWaker {
    /// The task whose waker `cx` carries.
    ///
    /// # Panics
    /// If `cx`'s waker is not a switch task waker, or on a foreign thread.
    pub fn new(cx: &Context<'_>) -> Self {
        let waker = cx.waker();
        assert!(
            ptr::eq(waker.vtable(), &VTABLE),
            "RemoteWaker needs a switch task waker"
        );
        let header = waker.data().cast::<TaskHeader>();
        unsafe { check_owner(header) };
        Self {
            header: NonNull::new(header.cast_mut()).expect("switch waker data is a header"),
        }
    }

    /// Wake the task: at once on its own thread, at the start of its runtime's
    /// next turn from any other thread.
    pub fn wake(&self) {
        // SAFETY: the handle came from a switch task waker.
        unsafe { wake_on_any_thread(self.header.as_ptr()) }
    }
}

/// Wake the task headed by `header`: at once on its block's thread, through its
/// runtime's mailbox from any other thread (dropped once the runtime shut down).
///
/// # Safety
/// `header` heads a slot of a block. Blocks are never freed, so its metadata stays
/// readable; a foreign thread reads only the owner tag and mailbox, both written
/// before the block's first task existed.
#[inline]
pub(crate) unsafe fn wake_on_any_thread(header: *const TaskHeader) {
    unsafe {
        let meta = TaskHeader::meta(header);
        if Meta::owner(meta) == THREAD_TAG.with(Cell::get) {
            Executor::wake(header);
        } else {
            foreign_wake(meta, header);
        }
    }
}

/// # Safety
/// As for [`wake_on_any_thread`]; `meta` is `header`'s block metadata.
#[cold]
#[inline(never)]
unsafe fn foreign_wake(meta: *mut Meta, header: *const TaskHeader) {
    unsafe { (*Meta::mailbox(meta)).push(header) }
}

#[cfg(test)]
mod tests;
