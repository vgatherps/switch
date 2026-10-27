//! Per-function slab pools.
//!
//! A [`FunctionPool`] exists per (task function, thread) and is never freed. It
//! holds one [`RuntimePool`] per runtime that has spawned the function, plus
//! `spare` slabs released by runtimes that shut down. Slabs are never freed and
//! never change function or thread; a slab serves one runtime at a time.
//!
//! All access is on the owning thread, through raw pointers, with no reference
//! held across user code (a task factory may spawn recursively).

use std::cell::Cell;
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};

use super::header::TaskHeader;
use super::slab::{Meta, SLOTS, SlabSpec};
use crate::executor::Executor;

const RING: usize = 16;

/// Thread-local root of one task function's pool.
///
/// Declared by each task function as a `const`-initialized `thread_local!`
/// without `Drop`. Such a thread-local stays accessible while other
/// thread-locals are destroyed, so tasks can be spawned and runtimes dropped from
/// thread-local destructors. That relies on native TLS; supported targets are
/// Linux (glibc) and macOS. Nothing reached after thread exit (wakers, slabs)
/// goes through this storage: slabs are reached from their own metadata.
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
            runtimes: Vec::new(),
            spare: Vec::new(),
            registry_next: ptr::null_mut(),
            slabs: 0,
        }));
        self.0.set(pool);
        pool
    }
}

/// Every function pool ever created, so their memory stays reachable.
static POOLS: AtomicPtr<FunctionPool> = AtomicPtr::new(ptr::null_mut());

pub(crate) struct FunctionPool {
    runtimes: Vec<*mut RuntimePool>,
    spare: Vec<*mut Meta>,
    registry_next: *mut FunctionPool,
    /// Slabs ever allocated for this function on this thread.
    slabs: usize,
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

    /// This runtime's pool for the function, created (and registered with the
    /// executor) on the runtime's first spawn of it.
    ///
    /// # Safety
    /// Owner thread; `executor` is live and open.
    pub(crate) unsafe fn runtime_pool(
        this: *mut Self,
        executor: *mut Executor,
    ) -> *mut RuntimePool {
        unsafe {
            if let Some(&pool) = (*this)
                .runtimes
                .iter()
                .find(|&&pool| (*pool).executor == executor)
            {
                return pool;
            }
            let pool = Box::into_raw(Box::new(RuntimePool {
                executor,
                owner: this,
                slabs: Vec::new(),
                has_free: Vec::new(),
                ring: [0; RING],
                head: 0,
                len: 0,
            }));
            (*this).runtimes.push(pool);
            Executor::add_pool(executor, pool);
            pool
        }
    }

    #[cfg(test)]
    pub(crate) unsafe fn counts(this: *mut Self) -> (usize, usize) {
        unsafe { ((*this).runtimes.len(), (*this).spare.len()) }
    }
}

pub(crate) struct RuntimePool {
    executor: *mut Executor,
    owner: *mut FunctionPool,
    /// Slab number -> slab base.
    slabs: Vec<*mut Meta>,
    /// Bit n: slab n has a free slot.
    has_free: Vec<u64>,
    /// Recently freed indices; `ring[head]` is the newest. Hints, re-checked against `alloc`.
    ring: [u32; RING],
    head: u8,
    len: u8,
}

impl RuntimePool {
    /// Reserve a slot: newest recently freed index, else the lowest free index,
    /// else a spare or new slab. Sets the slot's `alloc` bit.
    ///
    /// # Safety
    /// Owner thread; `spec` describes this pool's function.
    pub(crate) unsafe fn reserve(this: *mut Self, spec: &SlabSpec) -> (*mut Meta, u32) {
        unsafe {
            let index = match Self::pop_ring(this) {
                Some(index) => index,
                None => match Self::lowest_free(this) {
                    Some(index) => index,
                    None => Self::grow(this, spec),
                },
            };
            let meta = (&(*this).slabs)[index as usize / SLOTS];
            let slot = index % SLOTS as u32;
            let alloc = Meta::alloc(meta);
            let bits = alloc.get() | 1 << slot;
            alloc.set(bits);
            if bits == u64::MAX {
                let slab = index as usize / SLOTS;
                (&mut (*this).has_free)[slab / 64] &= !(1 << (slab % 64));
            }
            (meta, slot)
        }
    }

    unsafe fn pop_ring(this: *mut Self) -> Option<u32> {
        unsafe {
            while (*this).len > 0 {
                let index = (*this).ring[usize::from((*this).head)];
                (*this).head = ((usize::from((*this).head) + RING - 1) % RING) as u8;
                (*this).len -= 1;
                let meta = (&(*this).slabs)[index as usize / SLOTS];
                if Meta::alloc(meta).get() & 1 << (index % SLOTS as u32) == 0 {
                    return Some(index);
                }
            }
            None
        }
    }

    unsafe fn lowest_free(this: *mut Self) -> Option<u32> {
        unsafe {
            let (word, bits) = (*this)
                .has_free
                .iter()
                .enumerate()
                .find(|&(_, &bits)| bits != 0)?;
            let slab = word * 64 + bits.trailing_zeros() as usize;
            let free = !Meta::alloc((&(*this).slabs)[slab]).get();
            debug_assert_ne!(free, 0, "has_free bit set on a full slab");
            Some((slab * SLOTS) as u32 + free.trailing_zeros())
        }
    }

    /// Take a spare slab of this function, or allocate one; returns its first index.
    unsafe fn grow(this: *mut Self, spec: &SlabSpec) -> u32 {
        unsafe {
            let owner = (*this).owner;
            let meta = match (*owner).spare.pop() {
                Some(meta) => meta,
                None => {
                    // Capacity for every slab ever made: shutdown moves them to
                    // `spare` without allocating.
                    (*owner).slabs += 1;
                    let spare = &mut (*owner).spare;
                    spare.reserve((*owner).slabs - spare.len());
                    spec.allocate()
                }
            };
            let slab = (*this).slabs.len();
            let index = u32::try_from(slab * SLOTS).expect("task index space exhausted");
            (*this).slabs.push(meta);
            if (*this).has_free.len() * 64 <= slab {
                (*this).has_free.push(0);
            }
            (&mut (*this).has_free)[slab / 64] |= 1 << (slab % 64);
            Meta::executor(meta).set((*this).executor);
            Meta::pool(meta).set(this);
            Meta::slab_no(meta).set(slab as u32);
            index
        }
    }

    /// Return a slot whose future is gone (or was never written) to its pool.
    ///
    /// # Safety
    /// Owner thread; the slot's `alloc` bit is set, no queue entry or timer
    /// registration refers to it, and its future is dropped or uninitialized.
    pub(crate) unsafe fn release(meta: *mut Meta, slot: u32) {
        unsafe {
            let this = Meta::pool(meta).get();
            let header = Meta::header(meta, slot);
            debug_assert_eq!(TaskHeader::reg_head(header).get(), crate::intrusive::NIL);
            TaskHeader::flags(header).set(0);
            let alloc = Meta::alloc(meta);
            alloc.set(alloc.get() & !(1 << slot));
            let slab = Meta::slab_no(meta).get() as usize;
            (&mut (*this).has_free)[slab / 64] |= 1 << (slab % 64);
            (*this).head = ((usize::from((*this).head) + 1) % RING) as u8;
            (*this).ring[usize::from((*this).head)] = (slab * SLOTS) as u32 + slot;
            (*this).len = ((*this).len + 1).min(RING as u8);
        }
    }

    /// Number of slabs held.
    ///
    /// # Safety
    /// Owner thread.
    pub(crate) unsafe fn slab_count(this: *mut Self) -> usize {
        unsafe { (*this).slabs.len() }
    }

    /// # Safety
    /// Owner thread; `slab < slab_count`.
    pub(crate) unsafe fn slab(this: *mut Self, slab: usize) -> *mut Meta {
        unsafe { (&(*this).slabs)[slab] }
    }

    /// Move every slab to the function's `spare` list and free this pool.
    ///
    /// # Safety
    /// Owner thread; every slot is free; nothing refers to `this` afterwards.
    pub(crate) unsafe fn retire(this: *mut Self) {
        unsafe {
            let owner = (*this).owner;
            for &meta in &(*this).slabs {
                debug_assert_eq!(Meta::alloc(meta).get(), 0);
                Meta::executor(meta).set(ptr::null_mut());
                Meta::pool(meta).set(ptr::null_mut());
                Meta::slab_no(meta).set(0);
                (*owner).spare.push(meta);
            }
            let runtimes = &mut (*owner).runtimes;
            let position = runtimes
                .iter()
                .position(|&pool| pool == this)
                .expect("runtime pool not registered");
            runtimes.swap_remove(position);
            drop(Box::from_raw(this));
        }
    }

    #[cfg(test)]
    pub(crate) unsafe fn index_of(header: *const TaskHeader, meta: *mut Meta) -> u32 {
        unsafe { Meta::slab_no(meta).get() * SLOTS as u32 + u32::from(TaskHeader::slot(header)) }
    }

    #[cfg(test)]
    pub(crate) unsafe fn function_pool(this: *mut Self) -> *mut FunctionPool {
        unsafe { (*this).owner }
    }
}
