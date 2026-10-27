//! Slab layout: one metadata line, then 64 slots of one task function.
//!
//! ```text
//! base                    base + DATA_OFF
//! | Meta (64 B) | pad   | slot 0 | slot 1 | ... | slot 63 |      each slot: TaskHeader, future
//! ```
//! The slab is allocated at exactly `SLAB_SIZE` with alignment `SLAB_ALIGN`, the
//! next power of two, so a header pointer masks down to its `Meta` with no load.

use std::alloc::{self, Layout};
use std::cell::Cell;
use std::marker::PhantomData;
use std::mem::{MaybeUninit, align_of, size_of};
use std::ptr::{NonNull, addr_of, addr_of_mut};
use std::sync::atomic::{AtomicU16, Ordering};

use super::header::TaskHeader;
use super::pool::RuntimePool;
use crate::executor::{Executor, Step, TurnGuard};

pub(crate) const SLOTS: usize = 64;
/// `Meta` size and alignment; `TaskRef` keeps the slot in these low bits.
pub(crate) const META: usize = 64;

pub(crate) type RunFn = unsafe fn(*mut Executor, *mut Meta, u32, &mut TurnGuard) -> Step;
pub(crate) type DropFn = unsafe fn(*mut Meta, u32);

/// Slab metadata. Nothing forms `&Meta`/`&mut Meta`: fields are reached by raw
/// projection, because a foreign thread may read `owner` while the owner thread
/// writes the `Cell`s.
#[repr(C, align(64))]
pub(crate) struct Meta {
    /// Thread tag of the creating thread; the only field a foreign thread reads.
    owner: AtomicU16,
    data_off: u16,
    stride: u32,
    /// Bit n: slot n is in use (reserved, live, or awaiting reclaim).
    alloc: Cell<u64>,
    /// Runtime this slab is assigned to; null while spare.
    executor: Cell<*mut Executor>,
    run: RunFn,
    drop: DropFn,
    pool: Cell<*mut RuntimePool>,
    slab_no: Cell<u32>,
}

const _: () = assert!(size_of::<Meta>() == META && align_of::<Meta>() == META);

#[repr(C)]
pub(crate) struct Slot<Fut> {
    header: TaskHeader,
    future: MaybeUninit<Fut>,
}

const fn round_up(n: usize, to: usize) -> usize {
    n.div_ceil(to) * to
}

/// Per-future-type slab constants.
pub(crate) struct Shape<Fut>(PhantomData<Fut>);

impl<Fut> Shape<Fut> {
    pub(crate) const STRIDE: usize = round_up(size_of::<Slot<Fut>>(), 16);
    const SLOTS_LAYOUT: Layout =
        match Layout::from_size_align(Self::STRIDE * SLOTS, align_of::<Slot<Fut>>()) {
            Ok(layout) => layout,
            Err(_) => panic!("slab slot array too large"),
        };
    /// `(slab layout, slot array offset)`; the offset honours an over-aligned future.
    const EXTENDED: (Layout, usize) = match Layout::new::<Meta>().extend(Self::SLOTS_LAYOUT) {
        Ok(extended) => extended,
        Err(_) => panic!("slab too large"),
    };
    pub(crate) const DATA_OFF: usize = Self::EXTENDED.1;
    pub(crate) const SLAB_SIZE: usize = Self::EXTENDED.0.size();
    pub(crate) const SLAB_ALIGN: usize = Self::SLAB_SIZE.next_power_of_two();
    const CHECKED: () = {
        assert!(
            Self::DATA_OFF <= u16::MAX as usize,
            "future alignment too large"
        );
        assert!(Self::STRIDE <= u32::MAX as usize, "future too large");
        assert!(Self::SLAB_ALIGN >= META);
    };

    pub(crate) const fn spec(run: RunFn, drop: DropFn) -> SlabSpec {
        let () = Self::CHECKED;
        SlabSpec {
            layout: match Layout::from_size_align(Self::SLAB_SIZE, Self::SLAB_ALIGN) {
                Ok(layout) => layout,
                Err(_) => panic!("slab alignment too large"),
            },
            data_off: Self::DATA_OFF as u16,
            stride: Self::STRIDE as u32,
            run,
            drop,
        }
    }

    /// # Safety
    /// `meta` is the base of a slab built for `Fut`; `slot < 64`.
    #[inline(always)]
    pub(crate) unsafe fn slot(meta: *mut Meta, slot: u32) -> *mut Slot<Fut> {
        unsafe {
            meta.byte_add(Self::DATA_OFF + slot as usize * Self::STRIDE)
                .cast::<Slot<Fut>>()
        }
    }

    /// # Safety
    /// `slot` comes from [`Self::slot`].
    #[inline(always)]
    pub(crate) unsafe fn future(slot: *mut Slot<Fut>) -> *mut Fut {
        unsafe { addr_of_mut!((*slot).future).cast::<Fut>() }
    }

    /// Header pointer to its slab's metadata, without a load.
    #[inline(always)]
    pub(crate) fn meta_of(header: *const TaskHeader) -> *mut Meta {
        header
            .cast_mut()
            .map_addr(|addr| addr & !(Self::SLAB_ALIGN - 1))
            .cast::<Meta>()
    }
}

/// Type-erased slab description for the pool.
pub(crate) struct SlabSpec {
    layout: Layout,
    data_off: u16,
    stride: u32,
    run: RunFn,
    drop: DropFn,
}

impl SlabSpec {
    /// Allocate and initialize a slab owned by the calling thread. Never freed.
    pub(crate) fn allocate(&self) -> *mut Meta {
        unsafe {
            let base = alloc::alloc(self.layout);
            if base.is_null() {
                alloc::handle_alloc_error(self.layout);
            }
            let meta = base.cast::<Meta>();
            meta.write(Meta {
                owner: AtomicU16::new(crate::waker::this_thread_tag()),
                data_off: self.data_off,
                stride: self.stride,
                alloc: Cell::new(0),
                executor: Cell::new(std::ptr::null_mut()),
                run: self.run,
                drop: self.drop,
                pool: Cell::new(std::ptr::null_mut()),
                slab_no: Cell::new(0),
            });
            for slot in 0..SLOTS as u8 {
                Meta::header(meta, u32::from(slot))
                    .cast_mut()
                    .write(TaskHeader::new(slot));
            }
            meta
        }
    }

    #[cfg(test)]
    pub(crate) fn layout(&self) -> Layout {
        self.layout
    }
}

impl Meta {
    /// Safe from any thread: `owner` is written once before the slab is shared.
    ///
    /// # Safety
    /// `this` is the base of a slab (slabs are never freed).
    #[inline(always)]
    pub(crate) unsafe fn owner(this: *const Self) -> u16 {
        unsafe { (*addr_of!((*this).owner)).load(Ordering::Relaxed) }
    }

    /// Type-erased header address; the same offset arithmetic as [`Shape::slot`].
    ///
    /// # Safety
    /// Owner thread; `slot < 64`.
    #[inline]
    pub(crate) unsafe fn header(this: *mut Self, slot: u32) -> *const TaskHeader {
        unsafe {
            let data_off = usize::from(*addr_of!((*this).data_off));
            let stride = *addr_of!((*this).stride) as usize;
            this.byte_add(data_off + slot as usize * stride)
                .cast::<TaskHeader>()
                .cast_const()
        }
    }

    /// # Safety
    /// Owner thread.
    #[inline(always)]
    pub(crate) unsafe fn alloc<'a>(this: *mut Self) -> &'a Cell<u64> {
        unsafe { &*addr_of!((*this).alloc) }
    }

    /// # Safety
    /// Owner thread.
    #[inline(always)]
    pub(crate) unsafe fn executor<'a>(this: *mut Self) -> &'a Cell<*mut Executor> {
        unsafe { &*addr_of!((*this).executor) }
    }

    /// # Safety
    /// Owner thread.
    #[inline(always)]
    pub(crate) unsafe fn pool<'a>(this: *mut Self) -> &'a Cell<*mut RuntimePool> {
        unsafe { &*addr_of!((*this).pool) }
    }

    /// # Safety
    /// Owner thread.
    #[inline(always)]
    pub(crate) unsafe fn slab_no<'a>(this: *mut Self) -> &'a Cell<u32> {
        unsafe { &*addr_of!((*this).slab_no) }
    }

    /// # Safety
    /// Owner thread.
    #[inline(always)]
    pub(crate) unsafe fn run_fn(this: *mut Self) -> RunFn {
        unsafe { *addr_of!((*this).run) }
    }

    /// # Safety
    /// Owner thread.
    #[inline(always)]
    pub(crate) unsafe fn drop_fn(this: *mut Self) -> DropFn {
        unsafe { *addr_of!((*this).drop) }
    }
}

/// `meta | slot`: a slot reference for type-erased code (run queue, timers).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TaskRef(NonNull<Meta>);

impl TaskRef {
    /// # Safety
    /// `meta` is a slab base; `slot < 64`.
    #[inline(always)]
    pub(crate) unsafe fn new(meta: *mut Meta, slot: u32) -> Self {
        debug_assert!((slot as usize) < SLOTS);
        unsafe {
            Self(NonNull::new_unchecked(
                meta.map_addr(|addr| addr | slot as usize),
            ))
        }
    }

    #[inline(always)]
    pub(crate) fn meta(self) -> *mut Meta {
        self.0.as_ptr().map_addr(|addr| addr & !(META - 1))
    }

    #[inline(always)]
    pub(crate) fn slot(self) -> u32 {
        (self.0.as_ptr().addr() & (META - 1)) as u32
    }

    /// # Safety
    /// Owner thread.
    #[inline]
    pub(crate) unsafe fn header(self) -> *const TaskHeader {
        unsafe { Meta::header(self.meta(), self.slot()) }
    }
}

/// A standalone slab for unit tests of type-erased code; freed on drop.
#[cfg(test)]
pub(crate) struct TestSlab {
    meta: *mut Meta,
    layout: Layout,
}

#[cfg(test)]
impl TestSlab {
    pub(crate) fn new() -> Self {
        unsafe fn run(_: *mut Executor, _: *mut Meta, _: u32, _: &mut TurnGuard) -> Step {
            unreachable!("test slab is never polled")
        }
        unsafe fn drop(_: *mut Meta, _: u32) {}
        let spec = Shape::<std::future::Pending<()>>::spec(run, drop);
        Self {
            meta: spec.allocate(),
            layout: spec.layout(),
        }
    }

    /// A LIVE task in `slot`.
    pub(crate) fn task(&self, slot: u32) -> (TaskRef, *const TaskHeader) {
        unsafe {
            let task = TaskRef::new(self.meta, slot);
            let header = task.header();
            TaskHeader::flags(header).set(super::header::LIVE);
            (task, header)
        }
    }
}

#[cfg(test)]
impl Drop for TestSlab {
    fn drop(&mut self) {
        unsafe { alloc::dealloc(self.meta.cast(), self.layout) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_is_one_line_and_futures_up_to_8_aligned_start_at_offset_8() {
        assert_eq!((size_of::<Meta>(), align_of::<Meta>()), (META, META));
        assert_eq!(std::mem::offset_of!(Slot<u8>, future), 8);
        assert_eq!(std::mem::offset_of!(Slot<u64>, future), 8);
        assert_eq!(std::mem::offset_of!(Slot<[u64; 10]>, future), 8);
        assert_eq!(std::mem::offset_of!(Slot<u128>, future), 16);
    }
}
