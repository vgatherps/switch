//! Task blocks: 64 KiB, aligned to 64 KiB, holding slots of one task function
//! instantiation (one future type).
//!
//! ```text
//! base (64 KiB aligned)    base + data_off
//! | Meta (64 B) | pad |    slot 0 | slot 1 | ... | slot n-1 |    each slot: TaskHeader, future
//! ```
//! Every header masks to its block's `Meta` with one global constant. A block
//! belongs to one runtime pool for good: its metadata is written once, when the
//! pool allocates it. Blocks are never freed, so a waker always points at valid
//! memory; a dropped runtime's blocks are leaked by design.

use std::alloc::{self, Layout};
use std::marker::PhantomData;
use std::mem::{ManuallyDrop, MaybeUninit, align_of, size_of};
use std::pin::Pin;
use std::ptr::{addr_of, addr_of_mut};
use std::sync::atomic::{AtomicPtr, AtomicU16, Ordering};
use std::task::{Context, Poll, Waker};

use super::header::TaskHeader;
use super::pool::RuntimePool;
use crate::executor::Executor;
use crate::remote::Mailbox;

pub(crate) const BLOCK: usize = 64 * 1024;
pub(crate) const MASK: usize = !(BLOCK - 1);
const META: usize = 64;

pub(crate) type PollFn = unsafe fn(*const TaskHeader) -> Poll<()>;
pub(crate) type DropFn = unsafe fn(*const TaskHeader);

/// Block metadata, one cache line at the block base, written once before the
/// block is used. Nothing forms `&Meta`: fields are reached by raw projection.
#[repr(C, align(64))]
pub(crate) struct Meta {
    /// Thread tag of the creating thread. A foreign thread reads only this and
    /// `mailbox`, both written before the block's first task existed.
    owner: AtomicU16,
    data_off: u16,
    stride: u32,
    /// Slots in this block.
    slots: u32,
    /// Ordinal of this block in its runtime pool.
    block: u32,
    /// The block's runtime and pool. They dangle once that runtime is dropped,
    /// so they are read only for a TAKEN slot, whose runtime is alive: shutdown
    /// clears every slot's flags before the executor is freed.
    executor: *mut Executor,
    pool: *mut RuntimePool,
    poll: PollFn,
    drop: DropFn,
    /// The previously allocated block: every block stays reachable from
    /// [`BLOCKS`] (for leak checkers), since none is ever freed.
    next: *mut Meta,
    /// The runtime's mailbox for `RemoteWaker`s; never freed.
    mailbox: *const Mailbox,
}

/// The most recently allocated block, heading the list of all blocks.
static BLOCKS: AtomicPtr<Meta> = AtomicPtr::new(std::ptr::null_mut());

const _: () = assert!(size_of::<Meta>() == META);

#[repr(C)]
struct Slot<Fut> {
    header: TaskHeader,
    future: MaybeUninit<Fut>,
}

/// Per-future-type block constants.
pub(crate) struct Shape<Fut>(PhantomData<Fut>);

impl<Fut: Future<Output = ()>> Shape<Fut> {
    /// Rounded to 16 so a header and the start of its future share a cache line.
    const STRIDE: usize = size_of::<Slot<Fut>>().div_ceil(16) * 16;
    const DATA_OFF: usize = META.div_ceil(align_of::<Slot<Fut>>()) * align_of::<Slot<Fut>>();
    pub(crate) const SLOTS: usize = if Self::DATA_OFF < BLOCK {
        (BLOCK - Self::DATA_OFF) / Self::STRIDE
    } else {
        0
    };
    /// Evaluated in every task function's spawn path: a future that does not fit
    /// a block is a compile-time error.
    pub(crate) const FITS: () = assert!(
        Self::SLOTS >= 1,
        "task future does not fit a 64 KiB task block"
    );
    pub(crate) const SPEC: BlockSpec = BlockSpec {
        data_off: Self::DATA_OFF as u16,
        stride: Self::STRIDE as u32,
        slots: Self::SLOTS as u32,
        poll: poll_future::<Fut>,
        drop: drop_future::<Fut>,
    };

    /// # Safety
    /// `header` heads a slot of a block built for `Fut`.
    #[inline(always)]
    pub(crate) unsafe fn future(header: *const TaskHeader) -> *mut Fut {
        unsafe { addr_of_mut!((*header.cast_mut().cast::<Slot<Fut>>()).future).cast() }
    }
}

/// `meta.poll` for `Fut`.
///
/// # Safety
/// `header` heads a slot of a block built for `Fut` holding a live future.
unsafe fn poll_future<Fut: Future<Output = ()>>(header: *const TaskHeader) -> Poll<()> {
    // The waker is built here, in typed code, so its (constant) vtable is known
    // to the future's poll and a self-wake inlines instead of calling through it.
    let waker = ManuallyDrop::new(unsafe { Waker::from_raw(crate::waker::raw(header)) });
    let mut cx = Context::from_waker(&waker);
    unsafe { Pin::new_unchecked(&mut *Shape::<Fut>::future(header)).poll(&mut cx) }
}

/// `meta.drop` for `Fut`.
///
/// # Safety
/// As for [`poll_future`]; the future is not used afterwards.
unsafe fn drop_future<Fut: Future<Output = ()>>(header: *const TaskHeader) {
    unsafe { std::ptr::drop_in_place(Shape::<Fut>::future(header)) }
}

/// Type-erased description of one task function's blocks.
pub(crate) struct BlockSpec {
    data_off: u16,
    stride: u32,
    pub(crate) slots: u32,
    poll: PollFn,
    drop: DropFn,
}

impl BlockSpec {
    /// Allocate block number `block` of `pool` in `executor`, owned by the
    /// calling thread, all slots free (zeroed headers: no flags). Never freed.
    pub(crate) fn allocate(
        &self,
        executor: *mut Executor,
        mailbox: *const Mailbox,
        pool: *mut RuntimePool,
        block: u32,
    ) -> *mut Meta {
        // Before allocating: issuing the thread's first tag may panic.
        let owner = crate::waker::this_thread_tag();
        let layout = Layout::from_size_align(BLOCK, BLOCK).unwrap();
        unsafe {
            let meta = alloc::alloc_zeroed(layout).cast::<Meta>();
            if meta.is_null() {
                alloc::handle_alloc_error(layout);
            }
            meta.write(Meta {
                owner: AtomicU16::new(owner),
                data_off: self.data_off,
                stride: self.stride,
                slots: self.slots,
                block,
                executor,
                pool,
                poll: self.poll,
                drop: self.drop,
                next: std::ptr::null_mut(),
                mailbox,
            });
            let mut head = BLOCKS.load(Ordering::Relaxed);
            loop {
                addr_of_mut!((*meta).next).write(head);
                match BLOCKS.compare_exchange_weak(head, meta, Ordering::Release, Ordering::Relaxed)
                {
                    Ok(_) => return meta,
                    Err(current) => head = current,
                }
            }
        }
    }
}

impl Meta {
    /// Safe from any thread: `owner` is written once before the block is shared.
    ///
    /// # Safety
    /// `this` is a block base (blocks are never freed).
    #[inline(always)]
    pub(crate) unsafe fn owner(this: *const Self) -> u16 {
        unsafe { (*addr_of!((*this).owner)).load(Ordering::Relaxed) }
    }

    /// # Safety
    /// Owner thread; `position < slots`.
    #[inline]
    pub(crate) unsafe fn header(this: *mut Self, position: u32) -> *const TaskHeader {
        unsafe {
            let first = usize::from(*addr_of!((*this).data_off));
            let stride = *addr_of!((*this).stride) as usize;
            this.byte_add(first + position as usize * stride)
                .cast::<TaskHeader>()
                .cast_const()
        }
    }

    /// Position of `header`'s slot in its block.
    ///
    /// # Safety
    /// Owner thread; `header` heads a slot of this block.
    #[inline]
    pub(crate) unsafe fn position(this: *mut Self, header: *const TaskHeader) -> u32 {
        unsafe {
            let first = usize::from(*addr_of!((*this).data_off));
            let stride = *addr_of!((*this).stride) as usize;
            ((header.addr() - this.addr() - first) / stride) as u32
        }
    }

    /// # Safety
    /// Owner thread.
    #[inline(always)]
    pub(crate) unsafe fn slots(this: *mut Self) -> u32 {
        unsafe { *addr_of!((*this).slots) }
    }

    /// # Safety
    /// Owner thread.
    #[inline(always)]
    pub(crate) unsafe fn block(this: *mut Self) -> u32 {
        unsafe { *addr_of!((*this).block) }
    }

    /// Dangles once the block's runtime is dropped (see [`Meta`]).
    ///
    /// # Safety
    /// Owner thread.
    #[inline(always)]
    pub(crate) unsafe fn executor(this: *mut Self) -> *mut Executor {
        unsafe { *addr_of!((*this).executor) }
    }

    /// Dangles once the block's runtime is dropped (see [`Meta`]).
    ///
    /// # Safety
    /// Owner thread.
    #[inline(always)]
    pub(crate) unsafe fn pool(this: *mut Self) -> *mut RuntimePool {
        unsafe { *addr_of!((*this).pool) }
    }

    /// Never dangles: mailboxes are never freed. Safe from any thread that got
    /// a header of this block after the block's first task existed.
    ///
    /// # Safety
    /// `this` is a block base.
    #[inline(always)]
    pub(crate) unsafe fn mailbox(this: *mut Self) -> *const Mailbox {
        unsafe { *addr_of!((*this).mailbox) }
    }

    /// # Safety
    /// Owner thread.
    #[inline(always)]
    pub(crate) unsafe fn poll_fn(this: *mut Self) -> PollFn {
        unsafe { *addr_of!((*this).poll) }
    }

    /// # Safety
    /// Owner thread.
    #[inline(always)]
    pub(crate) unsafe fn drop_fn(this: *mut Self) -> DropFn {
        unsafe { *addr_of!((*this).drop) }
    }
}

/// A standalone block for unit tests of type-erased code, in no runtime.
#[cfg(test)]
pub(crate) struct TestBlock(*mut Meta);

#[cfg(test)]
impl TestBlock {
    pub(crate) fn new() -> Self {
        let spec = &Shape::<std::future::Pending<()>>::SPEC;
        use std::ptr::{null, null_mut};
        Self(spec.allocate(null_mut(), null(), null_mut(), 0))
    }

    /// A LIVE task header at `position`.
    pub(crate) fn task(&self, position: u32) -> *const TaskHeader {
        unsafe {
            let header = Meta::header(self.0, position);
            TaskHeader::reg_head(header).set(crate::intrusive::NIL);
            TaskHeader::flags(header).set(super::header::LIVE);
            header
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Bytes<const N: usize>([u8; N]);
    impl<const N: usize> Future for Bytes<N> {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            Poll::Ready(())
        }
    }
    #[repr(align(128))]
    struct Aligned;
    impl Future for Aligned {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            Poll::Ready(())
        }
    }

    #[test]
    fn slot_layout_constants() {
        // 8-byte header then an 8-aligned 80-byte future: 88 -> stride 96.
        assert_eq!(Shape::<Bytes<80>>::STRIDE, 16 * 6);
        assert_eq!(Shape::<Bytes<80>>::DATA_OFF, 64);
        assert_eq!(Shape::<Bytes<80>>::SLOTS, (BLOCK - 64) / 96);
        // Nine bytes at alignment 1: a 20-byte slot (alignment 4) rounds to 32.
        assert_eq!(Shape::<Bytes<9>>::STRIDE, 32);
        assert_eq!(Shape::<Aligned>::DATA_OFF, 128);
        assert_eq!(Shape::<Aligned>::STRIDE, 128);
        // The largest future that fits: one slot after the metadata line.
        assert_eq!(Shape::<Bytes<{ BLOCK - 64 - 8 }>>::SLOTS, 1);
        assert_eq!(Shape::<Bytes<{ BLOCK - 64 - 7 }>>::SLOTS, 0);
    }

    #[test]
    fn every_header_masks_to_its_block_and_position() {
        let block = TestBlock::new();
        let slots = Shape::<std::future::Pending<()>>::SLOTS as u32;
        for position in [0, 1, slots / 2, slots - 1] {
            let header = block.task(position);
            assert_eq!(header.addr() % 16, 0);
            assert_eq!(TaskHeader::meta(header), block.0);
            assert_eq!(unsafe { Meta::position(block.0, header) }, position);
        }
    }
}
