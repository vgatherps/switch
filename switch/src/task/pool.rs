//! Per-runtime block pools.
//!
//! Each runtime that spawns a task key owns one [`RuntimePool`], listed in its
//! executor: a depot of slot numbers and the blocks those slots live in. A task
//! key is the `TypeId` of (marker type, future type); `#[switch::task]` makes the
//! marker unique per function and instantiation, so a pool and its blocks only
//! ever hold one future type. A pool allocates its own blocks, which stay its
//! own: shutdown frees the pool and leaks its blocks (they are never freed).
//!
//! All access is on the owning thread, through raw pointers, with no reference
//! held across user code (a task factory may spawn recursively).

use std::any::TypeId;

use depot::DynDepot;

use super::block::{BlockSpec, Meta};
use super::header::TaskHeader;
use crate::executor::Executor;

/// One task function's slots in one runtime. Slot `s` is position `s % slots` of
/// block `s / slots`. Slots are handed out lowest first, so a block is added only
/// once every slot of the blocks before it is in use.
pub(crate) struct RuntimePool {
    key: TypeId,
    executor: *mut Executor,
    depot: DynDepot,
    /// One past the highest slot handed out so far; higher slots were never used.
    high: u32,
    /// Block ordinal -> block base.
    blocks: Vec<*mut Meta>,
}

impl RuntimePool {
    #[cold]
    #[inline(never)]
    pub(crate) fn new(executor: *mut Executor, key: TypeId) -> *mut Self {
        // Issue the thread tag (it may panic once tags are exhausted) before any
        // pool or reservation state exists; block allocation reuses it.
        crate::waker::this_thread_tag();
        Box::into_raw(Box::new(Self {
            key,
            executor,
            depot: DynDepot::new(),
            high: 0,
            blocks: Vec::new(),
        }))
    }

    /// Reserve the lowest free slot, adding a block when it is the first slot
    /// past the current blocks.
    ///
    /// # Safety
    /// Owner thread; `spec` describes this pool's function.
    pub(crate) unsafe fn reserve(this: *mut Self, spec: &BlockSpec) -> *const TaskHeader {
        unsafe {
            let slot = (*this).depot.alloc();
            (*this).high = (*this).high.max(slot + 1);
            let block = (slot / spec.slots) as usize;
            if block == (*this).blocks.len() {
                Self::grow(this, spec);
            }
            let header = Meta::header((&(*this).blocks)[block], slot % spec.slots);
            // A zeroed block's headers have no timer list yet; a freed slot keeps NIL.
            TaskHeader::reg_head(header).set(crate::intrusive::NIL);
            header
        }
    }

    unsafe fn grow(this: *mut Self, spec: &BlockSpec) {
        unsafe {
            let block = (*this).blocks.len() as u32;
            // Room first: once allocated, the block is always recorded here.
            (*this).blocks.reserve(1);
            let executor = (*this).executor;
            let meta = spec.allocate(executor, Executor::mailbox(executor), this, block);
            (*this).blocks.push(meta);
        }
    }

    /// Return a slot whose future is gone (or was never written) to its pool.
    ///
    /// # Safety
    /// Owner thread; the slot is reserved, no queue entry or timer registration
    /// refers to it, and its future is dropped or uninitialized.
    pub(crate) unsafe fn release(header: *const TaskHeader) {
        unsafe {
            let meta = TaskHeader::meta(header);
            debug_assert_eq!(TaskHeader::reg_head(header).get(), crate::intrusive::NIL);
            TaskHeader::flags(header).set(0);
            let slot = Meta::block(meta) * Meta::slots(meta) + Meta::position(meta, header);
            (*Meta::pool(meta)).depot.free(slot);
        }
    }

    #[cfg(test)]
    pub(crate) unsafe fn block_count(this: *mut Self) -> usize {
        unsafe { (*this).blocks.len() }
    }

    /// The header of every slot ever handed out here (so shutdown scans the
    /// peak in use, not the capacity).
    ///
    /// # Safety
    /// Owner thread; `this` stays live while iterating.
    pub(crate) unsafe fn headers(this: *mut Self) -> impl Iterator<Item = *const TaskHeader> {
        unsafe {
            (0..(*this).high).map(move |index| {
                let slots = Meta::slots((&(*this).blocks)[0]);
                let block = (&(*this).blocks)[(index / slots) as usize];
                Meta::header(block, index % slots)
            })
        }
    }

    /// # Safety
    /// `this` is live.
    #[inline]
    pub(crate) unsafe fn key(this: *mut Self) -> TypeId {
        unsafe { (*this).key }
    }

    /// Free this pool; its blocks are leaked, with every slot's flags clear, so
    /// a stale waker of them does nothing.
    ///
    /// # Safety
    /// Owner thread; every slot is free; nothing refers to `this` afterwards.
    pub(crate) unsafe fn free(this: *mut Self) {
        unsafe { drop(Box::from_raw(this)) }
    }
}
