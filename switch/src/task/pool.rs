//! Per-function block pools.
//!
//! A [`FunctionPool`] exists per (task key, thread) and is never freed. It holds
//! the `spare` blocks released by runtimes that shut down. A task key is the
//! `TypeId` of (task function marker, future type), so a pool and its blocks
//! only ever hold one future type. Each runtime that
//! spawns the function owns one [`RuntimePool`], listed in its executor: a depot
//! of slot numbers and the blocks those slots live in. Blocks are never freed and
//! never change function or thread; a block serves one runtime at a time.
//!
//! All access is on the owning thread, through raw pointers, with no reference
//! held across user code (a task factory may spawn recursively).

use std::any::TypeId;
use std::cell::Cell;
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};

use depot::DynDepot;

use super::block::{BlockSpec, Meta};
use super::header::TaskHeader;
use crate::executor::Executor;

thread_local! {
    /// This thread's function pools. `const` and without `Drop`, so it stays
    /// accessible while other thread-locals are destroyed: tasks can be spawned and
    /// runtimes dropped from thread-local destructors. That relies on native TLS;
    /// supported targets are Linux (glibc) and macOS. Nothing reached after thread
    /// exit (wakers, blocks) goes through it: blocks are reached from their own
    /// metadata.
    static REGISTRY: Cell<*mut Registry> = const { Cell::new(ptr::null_mut()) };
}

/// One thread's function pools, sorted by key. Never freed.
struct Registry {
    pools: Vec<(TypeId, *mut FunctionPool)>,
    next: *mut Registry,
}

/// Every registry ever created, so their pools stay reachable after thread exit.
static REGISTRIES: AtomicPtr<Registry> = AtomicPtr::new(ptr::null_mut());

impl Registry {
    fn register() -> *mut Self {
        let registry = Box::into_raw(Box::new(Self {
            pools: Vec::new(),
            next: ptr::null_mut(),
        }));
        let mut head = REGISTRIES.load(Ordering::Relaxed);
        loop {
            unsafe { (*registry).next = head };
            match REGISTRIES.compare_exchange_weak(
                head,
                registry,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => head = current,
            }
        }
        REGISTRY.with(|cell| cell.set(registry));
        registry
    }
}

pub(crate) struct FunctionPool {
    spare: Vec<*mut Meta>,
    /// Blocks ever allocated for this function on this thread.
    blocks: usize,
}

impl FunctionPool {
    /// This thread's pool for `key`, created on first use. Only reached when a
    /// runtime spawns `key` for the first time.
    #[cold]
    #[inline(never)]
    pub(crate) fn for_key(key: TypeId) -> *mut FunctionPool {
        let registry = REGISTRY.with(Cell::get);
        let found = if registry.is_null() {
            Err(0)
        } else {
            unsafe {
                (*registry)
                    .pools
                    .binary_search_by_key(&key, |&(key, _)| key)
            }
        };
        match found {
            Ok(index) => unsafe { (&(*registry).pools)[index].1 },
            Err(index) => {
                // Issue the thread tag (it may panic once tags are exhausted) before
                // any pool state exists; block allocation reuses it.
                crate::waker::this_thread_tag();
                let registry = if registry.is_null() {
                    Registry::register()
                } else {
                    registry
                };
                let pool = Box::into_raw(Box::new(FunctionPool {
                    spare: Vec::new(),
                    blocks: 0,
                }));
                unsafe { (*registry).pools.insert(index, (key, pool)) };
                pool
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
    key: TypeId,
    function: *mut FunctionPool,
    executor: *mut Executor,
    depot: DynDepot,
    capacity: u32,
    /// Highest depot slot handed out so far; higher slots were never used.
    high: u32,
    /// Block ordinal -> block base.
    blocks: Vec<*mut Meta>,
}

impl RuntimePool {
    pub(crate) fn new(executor: *mut Executor, key: TypeId, capacity: u32) -> *mut Self {
        Box::into_raw(Box::new(Self {
            key,
            function: FunctionPool::for_key(key),
            executor,
            depot: DynDepot::new(capacity as usize + 1),
            capacity,
            high: 0,
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
            (*this).high = (*this).high.max(slot);
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

    /// # Safety
    /// `this` is live.
    #[cfg(test)]
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
