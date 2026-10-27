//! Thread tags and per-function task wakers.
//!
//! A task `Waker`'s data is its task header pointer and its vtable is the task
//! function's `WAKER` static, so all wakers of one slot compare equal under
//! `will_wake`. Every operation first checks the slab's owner thread tag and
//! panics on a foreign thread; only then does it touch the task or runtime.
//!
//! Ordinary Wakers remain shareable; local handles have stricter trait bounds.
//! ```
//! fn require_send_sync<T: Send + Sync>() {}
//! require_send_sync::<std::task::Waker>();
//! ```

use std::cell::Cell;
use std::sync::atomic::{AtomicU32, Ordering};
use std::task::RawWaker;

use crate::__private::PoolKey;
use crate::executor::Executor;
use crate::task::TaskHeader;
use crate::task::slab::{Meta, Shape};

thread_local! {
    static THREAD_TAG: Cell<u16> = const { Cell::new(0) };
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

/// Mask `header` to its slab and reject a foreign thread before any other access.
#[inline(always)]
unsafe fn checked_meta<Fut>(header: *const ()) -> *mut Meta {
    let meta = Shape::<Fut>::meta_of(header.cast());
    if unsafe { Meta::owner(meta) } != THREAD_TAG.with(Cell::get) {
        foreign_thread();
    }
    meta
}

pub(crate) unsafe fn clone<K: PoolKey, Fut>(header: *const ()) -> RawWaker {
    unsafe { checked_meta::<Fut>(header) };
    RawWaker::new(header, K::waker())
}

pub(crate) unsafe fn wake<Fut>(header: *const ()) {
    unsafe { wake_by_ref::<Fut>(header) }
}

pub(crate) unsafe fn wake_by_ref<Fut>(header: *const ()) {
    unsafe {
        let meta = checked_meta::<Fut>(header);
        Executor::wake(meta, header.cast::<TaskHeader>());
    }
}

pub(crate) unsafe fn drop<Fut>(header: *const ()) {
    unsafe { checked_meta::<Fut>(header) };
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
