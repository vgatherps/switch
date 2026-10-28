//! Per-function block pools.
//!
//! A [`FunctionPool`] exists per (task function, thread) and is never freed. It
//! holds the `spare` blocks released by runtimes that shut down. Each runtime that
//! spawns the function owns one [`RuntimePool`], listed in its executor: a depot
//! of slot numbers and the blocks those slots live in. Blocks are never freed and
//! never change function or thread; a block serves one runtime at a time.
//!
//! All access is on the owning thread, through raw pointers, with no reference
//! held across user code (a task factory may spawn recursively).

use std::cell::Cell;
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};

use depot::DynDepot;

use super::block::{BlockSpec, Meta};
use super::header::TaskHeader;
use crate::executor::Executor;

/// Thread-local root of one task function's pool.
///
/// Declared by each task function as a `const`-initialized `thread_local!`
/// without `Drop`. Such a thread-local stays accessible while other
/// thread-locals are destroyed, so tasks can be spawned and runtimes dropped from
/// thread-local destructors. That relies on native TLS; supported targets are
/// Linux (glibc) and macOS. Nothing reached after thread exit (wakers, blocks)
/// goes through this storage: blocks are reached from their own metadata.
pub struct Pool(Cell<*mut FunctionPool>);

impl Pool {
    #[allow(clippy::new_without_default)]
    pub const fn new() -> Self {
        Self(Cell::new(ptr::null_mut()))
    }

    /// This thread's function pool, created on first use.
    pub(crate) fn get(&self) -> *mut FunctionPool {
        let pool = self.0.get();
        if !pool.is_null() {
            return pool;
        }
        let pool = FunctionPool::register(Box::new(FunctionPool {
            spare: Vec::new(),
            registry_next: ptr::null_mut(),
            blocks: 0,
        }));
        self.0.set(pool);
        pool
    }
}

/// Every function pool ever created, so their memory stays reachable.
static POOLS: AtomicPtr<FunctionPool> = AtomicPtr::new(ptr::null_mut());

pub(crate) struct FunctionPool {
    spare: Vec<*mut Meta>,
    registry_next: *mut FunctionPool,
    /// Blocks ever allocated for this function on this thread.
    blocks: usize,
}

impl FunctionPool {
    fn register(pool: Box<Self>) -> *mut Self {
        let pool = Box::into_raw(pool);
        let mut head = POOLS.load(Ordering::Relaxed);
        loop {
            unsafe { (*pool).registry_next = head };
            match POOLS.compare_exchange_weak(head, pool, Ordering::Release, Ordering::Relaxed) {
                Ok(_) => return pool,
                Err(current) => head = current,
            }
        }
    }

    #[cfg(test)]
    pub(crate) unsafe fn spare_count(this: *mut Self) -> usize {
        unsafe { (*this).spare.len() }
    }
}

/// One task function's slots in one runtime. Depot slot `s` (1..=capacity; 0 is
/// the depot's sentinel) is position `(s - 1) % slots` of block `(s - 1) / slots`.
pub(crate) struct RuntimePool {
    function: *mut FunctionPool,
    executor: *mut Executor,
    depot: DynDepot,
    capacity: u32,
    /// Block ordinal -> block base.
    blocks: Vec<*mut Meta>,
}

impl RuntimePool {
    pub(crate) fn new(
        executor: *mut Executor,
        function: *mut FunctionPool,
        capacity: u32,
    ) -> *mut Self {
        Box::into_raw(Box::new(Self {
            function,
            executor,
            depot: DynDepot::new(capacity as usize + 1),
            capacity,
            blocks: Vec::new(),
        }))
    }

    /// Reserve a free slot, adding a block when the depot hands out the first
    /// slot past the current blocks. `None` when the function's capacity is used.
    ///
    /// # Safety
    /// Owner thread; `spec` describes this pool's function.
    pub(crate) unsafe fn reserve(this: *mut Self, spec: &BlockSpec) -> Option<*const TaskHeader> {
        unsafe {
            let slot = (*this).depot.alloc()?.slot();
            if slot > (*this).capacity {
                // The depot rounds its size up to 64 slots.
                (*this).depot.free_slot_unchecked(slot);
                return None;
            }
            let block = ((slot - 1) / spec.slots) as usize;
            if block == (*this).blocks.len() {
                Self::grow(this, spec);
            }
            let header = Meta::header((&(*this).blocks)[block], (slot - 1) % spec.slots);
            // A zeroed block's headers have no timer list yet; a freed slot keeps NIL.
            TaskHeader::reg_head(header).set(crate::intrusive::NIL);
            Some(header)
        }
    }

    unsafe fn grow(this: *mut Self, spec: &BlockSpec) {
        unsafe {
            let function = (*this).function;
            let meta = match (*function).spare.pop() {
                Some(meta) => meta,
                None => {
                    let meta = spec.allocate();
                    // Capacity for every block ever made: shutdown moves them to
                    // `spare` without allocating.
                    (*function).blocks += 1;
                    let spare = &mut (*function).spare;
                    spare.reserve((*function).blocks - spare.len());
                    meta
                }
            };
            Meta::block(meta).set((*this).blocks.len() as u32);
            Meta::executor(meta).set((*this).executor);
            Meta::pool(meta).set(this);
            (*this).blocks.push(meta);
        }
    }

    /// Return a slot whose future is gone (or was never written) to its depot.
    ///
    /// # Safety
    /// Owner thread; the slot is reserved, no queue entry or timer registration
    /// refers to it, and its future is dropped or uninitialized.
    pub(crate) unsafe fn release(header: *const TaskHeader) {
        unsafe {
            let meta = TaskHeader::meta(header);
            debug_assert_eq!(TaskHeader::reg_head(header).get(), crate::intrusive::NIL);
            TaskHeader::flags(header).set(0);
            let slot =
                Meta::block(meta).get() * Meta::slots(meta) + Meta::position(meta, header) + 1;
            (*Meta::pool(meta).get()).depot.free_slot_unchecked(slot);
        }
    }

    /// # Safety
    /// Owner thread.
    pub(crate) unsafe fn block_count(this: *mut Self) -> usize {
        unsafe { (*this).blocks.len() }
    }

    /// # Safety
    /// Owner thread; `block < block_count`.
    pub(crate) unsafe fn block(this: *mut Self, block: usize) -> *mut Meta {
        unsafe { (&(*this).blocks)[block] }
    }

    /// # Safety
    /// `this` is live.
    pub(crate) unsafe fn function_pool(this: *mut Self) -> *mut FunctionPool {
        unsafe { (*this).function }
    }

    /// Move every block to the function's `spare` list and free this pool.
    ///
    /// # Safety
    /// Owner thread; every slot is free; nothing refers to `this` afterwards.
    pub(crate) unsafe fn retire(this: *mut Self) {
        unsafe {
            let function = (*this).function;
            for &meta in &(*this).blocks {
                Meta::executor(meta).set(ptr::null_mut());
                Meta::pool(meta).set(ptr::null_mut());
                (*function).spare.push(meta);
            }
            drop(Box::from_raw(this));
        }
    }
}
