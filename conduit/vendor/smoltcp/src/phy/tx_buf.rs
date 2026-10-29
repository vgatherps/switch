#![allow(unsafe_code)]

use alloc::rc::Rc;
use core::{mem::ManuallyDrop, ptr::NonNull, slice};

pub use owner::TxPool;

/// Header bytes the stack may write in front of a TX buffer's payload.
pub const TX_HEADROOM: usize = 64;

/// One reference to a TX payload buffer. Payload and headers are written only while this is the
/// only reference.
///
/// With feature `dpdk` (conduit) the owner is a frame-pool mbuf (`zhida_buffer::FramePool`) and
/// reference operations are direct refcount accesses. Without it the owner is a heap buffer from
/// `TxPool`, which smoltcp's own tests use.
pub struct TxBuf {
    owner: owner::Owner,
    data: NonNull<u8>,
    len: u32,
    room: u32,
}

impl TxBuf {
    #[inline]
    pub fn len(&self) -> usize {
        self.len as usize
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.room as usize
    }

    /// The allocator identity: the pool this buffer came from.
    #[inline]
    pub fn origin(&self) -> NonNull<()> {
        owner::origin(self.owner)
    }

    /// Whether this is the only reference.
    #[inline]
    pub fn unique(&self) -> bool {
        owner::unique(self.owner)
    }

    #[inline]
    pub fn payload(&self) -> &[u8] {
        // SAFETY: data..data + room stays valid while this reference lives; len <= room.
        unsafe { slice::from_raw_parts(self.data.as_ptr(), self.len()) }
    }

    /// The whole payload room. Panics unless this is the only reference.
    pub fn room_mut(&mut self) -> &mut [u8] {
        assert!(self.unique(), "TX buffer is shared");
        // SAFETY: the only reference, and the mutable borrow prevents another view.
        unsafe { slice::from_raw_parts_mut(self.data.as_ptr(), self.capacity()) }
    }

    /// Set the payload length. Panics if it exceeds the room or the buffer is shared.
    pub fn set_len(&mut self, len: usize) {
        assert!(len <= self.capacity(), "TX payload length exceeds room");
        assert!(self.unique(), "TX buffer is shared");
        self.len = len as u32;
    }

    /// The `header_len` bytes in front of the payload, to write headers into, and the payload.
    /// Panics unless this is the only reference and `header_len <= TX_HEADROOM`.
    pub fn headroom_mut(&mut self, header_len: usize) -> (&mut [u8], &[u8]) {
        assert!(self.unique(), "TX buffer is shared");
        // SAFETY: checked unique just now.
        unsafe { self.headroom_unchecked(header_len) }
    }

    /// This buffer as a `UniqueTxBuf` if it holds the only reference: one refcount read.
    #[inline]
    pub(crate) fn into_unique(self) -> Result<UniqueTxBuf, TxBuf> {
        if self.unique() { Ok(UniqueTxBuf(self)) } else { Err(self) }
    }

    /// # Safety
    /// This is the only reference.
    #[inline]
    unsafe fn headroom_unchecked(&mut self, header_len: usize) -> (&mut [u8], &[u8]) {
        assert!(header_len <= TX_HEADROOM, "TX header exceeds the headroom");
        debug_assert!(self.unique());
        // SAFETY: every owner keeps TX_HEADROOM bytes in front of data in the same allocation;
        // the only reference may write them, and the two slices do not overlap.
        unsafe {
            let start = self.data.as_ptr().sub(header_len);
            (
                slice::from_raw_parts_mut(start, header_len),
                slice::from_raw_parts(self.data.as_ptr(), self.len()),
            )
        }
    }

    /// The `header_len` bytes in front of the payload followed by the payload: the packet a
    /// device publishes. Panics if `header_len > TX_HEADROOM`.
    pub fn packet(&self, header_len: usize) -> &[u8] {
        assert!(header_len <= TX_HEADROOM, "TX header exceeds the headroom");
        // SAFETY: the headroom and payload are one valid range of the owner's allocation.
        unsafe {
            slice::from_raw_parts(
                self.data.as_ptr().sub(header_len),
                header_len + self.len(),
            )
        }
    }
}

impl Clone for TxBuf {
    #[inline]
    fn clone(&self) -> TxBuf {
        owner::share(self.owner);
        TxBuf {
            owner: self.owner,
            data: self.data,
            len: self.len,
            room: self.room,
        }
    }
}

impl Drop for TxBuf {
    #[inline]
    fn drop(&mut self) {
        // SAFETY: this value holds one reference, given up exactly once here.
        unsafe { owner::release(self.owner) };
    }
}

#[cfg(feature = "dpdk")]
mod owner {
    use super::*;
    use zhida_dpdk_sys::{rte_mbuf, zhida_pktmbuf_free_seg};

    pub use zhida_buffer::FramePool as TxPool;

    /// A frame-pool mbuf; one TxBuf or published packet is one refcount.
    pub(super) type Owner = NonNull<rte_mbuf>;

    #[inline(always)]
    pub(super) fn unique(owner: Owner) -> bool {
        // SAFETY: a reference keeps the mbuf live; refcounts are non-atomic, one owner thread.
        unsafe { (*owner.as_ptr()).refcnt() == 1 }
    }

    #[inline(always)]
    pub(super) fn share(owner: Owner) {
        // SAFETY: as `unique`.
        unsafe { (*owner.as_ptr()).refcnt_inc() }
    }

    /// # Safety
    /// The caller gives up one reference.
    #[inline(always)]
    pub(super) unsafe fn release(owner: Owner) {
        // SAFETY: a direct, single-segment frame-pool mbuf; the last reference returns it.
        unsafe { zhida_pktmbuf_free_seg(owner.as_ptr()) }
    }

    #[inline(always)]
    pub(super) fn origin(owner: Owner) -> NonNull<()> {
        // SAFETY: as `unique`; a pool mbuf's `pool` is never null.
        unsafe { NonNull::new_unchecked((*owner.as_ptr()).pool).cast() }
    }

    #[inline(always)]
    pub(super) fn pool_origin(pool: &TxPool) -> NonNull<()> {
        NonNull::new(pool.mempool()).expect("a frame pool has a mempool").cast()
    }

    #[inline]
    pub(super) fn alloc(pool: &TxPool) -> Option<TxBuf> {
        let mbuf = pool.alloc()?;
        // SAFETY: FramePool::alloc returns a live mbuf with this reference only, its payload at
        // FRAME_DATA_OFF (>= TX_HEADROOM) followed by `room` bytes of its buffer.
        let data = unsafe { NonNull::new_unchecked((*mbuf.as_ptr()).data_ptr()) };
        Some(TxBuf {
            owner: mbuf,
            data,
            len: 0,
            room: u32::from(pool.room()),
        })
    }

    impl TxBuf {
        /// The frame's mbuf; it stays this value's reference.
        #[inline]
        pub fn mbuf(&self) -> NonNull<rte_mbuf> {
            self.owner
        }

        /// Give this value's reference to the caller as its mbuf.
        #[inline]
        pub fn into_mbuf(self) -> NonNull<rte_mbuf> {
            ManuallyDrop::new(self).owner
        }
    }

    impl TxFrame {
        /// The frame's mbuf; it stays this frame's reference.
        #[inline]
        pub fn mbuf(&self) -> NonNull<rte_mbuf> {
            self.0.owner
        }
    }
}

#[cfg(not(feature = "dpdk"))]
mod owner {
    use super::*;
    use alloc::{boxed::Box, vec};
    use core::cell::Cell;

    /// A pool's reference counters, for leak checks; buffers keep them past the pool.
    #[derive(Default)]
    struct Counts {
        shares: Cell<usize>,
        releases: Cell<usize>,
        live: Cell<usize>,
    }

    /// Heap TX buffers with reference and lifetime counters, for smoltcp's own tests. Each buffer
    /// keeps `TX_HEADROOM` bytes in front of `room` payload bytes.
    pub struct TxPool {
        room: usize,
        counts: Rc<Counts>,
    }

    pub(crate) struct Storage {
        bytes: Box<[u8]>,
        references: Cell<usize>,
        origin: NonNull<()>,
        counts: Rc<Counts>,
    }

    /// A Box-allocated Storage, created by TxPool::alloc and freed by the last release.
    pub(super) type Owner = NonNull<Storage>;

    #[inline]
    pub(super) fn unique(owner: Owner) -> bool {
        // SAFETY: a reference keeps the Storage live.
        unsafe { owner.as_ref() }.references.get() == 1
    }

    pub(super) fn share(owner: Owner) {
        // SAFETY: as `unique`.
        let storage = unsafe { owner.as_ref() };
        storage.references.set(storage.references.get().checked_add(1).expect("TX buffer reference count overflow"));
        storage.counts.shares.set(storage.counts.shares.get() + 1);
    }

    /// # Safety
    /// The caller gives up one reference.
    pub(super) unsafe fn release(owner: Owner) {
        // SAFETY: a reference keeps the Storage live.
        let storage = unsafe { owner.as_ref() };
        let counts = &storage.counts;
        counts.releases.set(counts.releases.get() + 1);
        let remaining = storage.references.get().checked_sub(1).expect("TX buffer reference count underflow");
        storage.references.set(remaining);
        if remaining == 0 {
            counts.live.set(counts.live.get() - 1);
            // SAFETY: the last reference to the Box created by TxPool::alloc.
            unsafe { drop(Box::from_raw(owner.as_ptr())) };
        }
    }

    pub(super) fn origin(owner: Owner) -> NonNull<()> {
        // SAFETY: as `unique`.
        unsafe { owner.as_ref() }.origin
    }

    pub(super) fn pool_origin(pool: &TxPool) -> NonNull<()> {
        NonNull::from(pool).cast()
    }

    pub(super) fn alloc(pool: &TxPool) -> Option<TxBuf> {
        pool.alloc()
    }

    impl TxPool {
        pub fn new(room: usize) -> Rc<TxPool> {
            Rc::new(TxPool {
                room,
                counts: Rc::default(),
            })
        }

        /// An empty buffer with one reference.
        pub fn alloc(&self) -> Option<TxBuf> {
            let total = TX_HEADROOM.checked_add(self.room).expect("TX storage size overflow");
            let mut storage = Box::new(Storage {
                bytes: vec![0; total].into_boxed_slice(),
                references: Cell::new(1),
                origin: pool_origin(self),
                counts: self.counts.clone(),
            });
            // SAFETY: TX_HEADROOM is within the allocation by construction.
            let data = unsafe { NonNull::new_unchecked(storage.bytes.as_mut_ptr().add(TX_HEADROOM)) };
            let owner = NonNull::new(Box::into_raw(storage)).expect("Box::into_raw returned null");
            self.counts.live.set(self.counts.live.get() + 1);
            Some(TxBuf {
                owner,
                data,
                len: 0,
                room: u32::try_from(self.room).expect("TX room exceeds u32"),
            })
        }

        /// Buffers whose storage is not yet freed.
        pub fn live(&self) -> usize {
            self.counts.live.get()
        }

        pub fn shares(&self) -> usize {
            self.counts.shares.get()
        }

        pub fn releases(&self) -> usize {
            self.counts.releases.get()
        }
    }

    impl TxBuf {
        /// Give this value's reference up without releasing it (tests that model a device
        /// holding it). `release_raw` gives it back.
        #[cfg(test)]
        pub(crate) fn into_raw(self) -> NonNull<Storage> {
            ManuallyDrop::new(self).owner
        }

        /// # Safety
        /// `owner` came from `into_raw` and its reference is given up here.
        #[cfg(test)]
        pub(crate) unsafe fn release_raw(owner: NonNull<Storage>) {
            unsafe { release(owner) }
        }
    }
}

/// A TxBuf holding the only reference. It cannot be cloned, so it stays the only reference and
/// its headers may be written without another refcount read.
pub(crate) struct UniqueTxBuf(TxBuf);

impl UniqueTxBuf {
    /// `TxBuf::room_mut` without the refcount read.
    #[inline]
    pub(crate) fn room_mut(&mut self) -> &mut [u8] {
        // SAFETY: the only reference (`into_unique`, `alloc_unique`), and the mutable borrow
        // prevents another view.
        unsafe { slice::from_raw_parts_mut(self.0.data.as_ptr(), self.0.capacity()) }
    }

    /// `TxBuf::set_len` without the refcount read. Panics if `len` exceeds the room.
    #[inline]
    pub(crate) fn set_len(&mut self, len: usize) {
        assert!(len <= self.0.capacity(), "TX payload length exceeds room");
        self.0.len = len as u32;
    }

    /// The `header_len` bytes in front of the payload, and the payload. Panics if
    /// `header_len > TX_HEADROOM`.
    #[inline]
    pub(crate) fn headroom_mut(&mut self, header_len: usize) -> (&mut [u8], &[u8]) {
        // SAFETY: a UniqueTxBuf is the only reference (`into_unique`) and cannot be cloned.
        unsafe { self.0.headroom_unchecked(header_len) }
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub(crate) fn payload(&self) -> &[u8] {
        self.0.payload()
    }

    #[inline]
    pub(crate) fn into_buf(self) -> TxBuf {
        self.0
    }

    #[inline]
    pub(crate) fn into_frame(self) -> TxFrame {
        TxFrame(self.0)
    }
}

/// Cloneable handle to a TX pool; each clone keeps the pool alive.
#[derive(Clone)]
pub struct TxSource(Rc<TxPool>);

impl TxSource {
    pub fn new(pool: Rc<TxPool>) -> TxSource {
        TxSource(pool)
    }

    /// An empty, unique buffer from the pool; `None` when it is empty.
    #[inline]
    pub fn alloc(&self) -> Option<TxBuf> {
        owner::alloc(&self.0)
    }

    /// `alloc` as a `UniqueTxBuf`: a fresh buffer holds the only reference.
    #[inline]
    pub(crate) fn alloc_unique(&self) -> Option<UniqueTxBuf> {
        owner::alloc(&self.0).map(UniqueTxBuf)
    }

    #[inline]
    pub fn origin(&self) -> NonNull<()> {
        owner::pool_origin(&self.0)
    }
}

impl core::fmt::Debug for TxSource {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("TxSource").field(&self.origin()).finish()
    }
}

/// The app's frame. Clone is cheap; after a clone the payload is read-only.
#[derive(Clone)]
pub struct TxFrame(TxBuf);

impl TxFrame {
    /// An empty frame from `pool`; `None` when the pool is empty.
    #[inline]
    pub fn alloc(pool: &TxPool) -> Option<TxFrame> {
        owner::alloc(pool).map(TxFrame)
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// The whole payload room. Panics if the frame is shared.
    #[inline]
    pub fn room_mut(&mut self) -> &mut [u8] {
        self.0.room_mut()
    }

    /// Panics if the frame is shared or `len` exceeds the capacity.
    #[inline]
    pub fn set_len(&mut self, len: usize) {
        self.0.set_len(len);
    }

    #[inline]
    pub fn payload(&self) -> &[u8] {
        self.0.payload()
    }

    #[inline]
    pub fn origin(&self) -> NonNull<()> {
        self.0.origin()
    }

    #[inline]
    pub(crate) fn unique(&self) -> bool {
        self.0.unique()
    }

    /// This frame as a `UniqueTxBuf` if it holds the only reference: one refcount read.
    #[inline]
    pub(crate) fn into_unique(self) -> Result<UniqueTxBuf, TxFrame> {
        self.0.into_unique().map_err(TxFrame)
    }

    #[inline]
    pub(crate) fn into_buf(self) -> TxBuf {
        self.0
    }

    /// Wrap a buffer from a test pool.
    #[cfg(test)]
    pub(crate) fn from_buf(buf: TxBuf) -> TxFrame {
        TxFrame(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    #[test]
    fn clone_shares_and_drop_releases_once() {
        let pool = TxPool::new(8);
        let buf = pool.alloc().unwrap();
        let clone = buf.clone();
        assert_eq!(pool.shares(), 1);
        assert!(!buf.unique());
        drop(clone);
        assert_eq!(pool.releases(), 1);
        assert!(buf.unique());
        drop(buf);
        assert_eq!(pool.releases(), 2);
        assert_eq!(pool.live(), 0);
    }

    #[test]
    fn room_mut_set_len_and_headroom_mut_panic_when_shared() {
        let pool = TxPool::new(8);
        let mut buf = pool.alloc().unwrap();
        let clone = buf.clone();
        assert!(catch_unwind(AssertUnwindSafe(|| {
            let _ = buf.room_mut();
        }))
        .is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| buf.set_len(1))).is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| {
            let _ = buf.headroom_mut(1);
        }))
        .is_err());
        drop(clone);
    }

    #[test]
    fn set_len_updates_this_buffer_at_refcount_1() {
        let pool = TxPool::new(8);
        let mut buf = pool.alloc().unwrap();
        buf.room_mut()[..3].copy_from_slice(&[1, 2, 3]);
        buf.set_len(3);
        assert_eq!(buf.payload(), &[1, 2, 3]);
    }

    #[test]
    fn dropping_original_then_clone_releases_storage_once() {
        let pool = TxPool::new(8);
        let buf = pool.alloc().unwrap();
        let clone = buf.clone();
        drop(buf);
        assert_eq!(pool.live(), 1);
        drop(clone);
        assert_eq!(pool.live(), 0);
        assert_eq!(pool.releases(), 2);
    }

    #[test]
    fn into_raw_keeps_its_reference() {
        let pool = TxPool::new(8);
        let buf = pool.alloc().unwrap();
        let owner = buf.into_raw();
        assert_eq!(pool.live(), 1);
        // SAFETY: into_raw gave up this sole reference.
        unsafe { TxBuf::release_raw(owner) };
        assert_eq!(pool.live(), 0);
        assert_eq!(pool.releases(), 1);
    }

    /// Headers written into the headroom form one packet with the payload; the headroom holds
    /// exactly TX_HEADROOM bytes.
    #[test]
    fn headroom_and_payload_form_the_packet() {
        let pool = TxPool::new(8);
        let mut buf = pool.alloc().unwrap();
        buf.room_mut()[..2].copy_from_slice(&[8, 9]);
        buf.set_len(2);
        let (headers, payload) = buf.headroom_mut(TX_HEADROOM);
        assert_eq!(payload, &[8, 9]);
        headers.fill(7);
        headers[TX_HEADROOM - 1] = 1;
        assert_eq!(buf.packet(1), &[1, 8, 9]);
        assert_eq!(buf.packet(TX_HEADROOM).len(), TX_HEADROOM + 2);
        assert_eq!(buf.packet(0), &[8, 9]);
        assert!(catch_unwind(AssertUnwindSafe(|| {
            let _ = buf.headroom_mut(TX_HEADROOM + 1);
        }))
        .is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| {
            let _ = buf.packet(TX_HEADROOM + 1);
        }))
        .is_err());
    }

    /// A buffer from `alloc_unique` is written and sized without refcount reads, keeps the
    /// room limit, and becomes an ordinary buffer with the written payload.
    #[test]
    fn alloc_unique_writes_payload_within_room() {
        let pool = TxPool::new(4);
        let source = TxSource::new(pool.clone());
        let mut buf = source.alloc_unique().unwrap();
        buf.room_mut()[..3].copy_from_slice(&[7, 8, 9]);
        buf.set_len(3);
        assert_eq!((buf.len(), buf.payload()), (3, &[7, 8, 9][..]));
        assert!(catch_unwind(AssertUnwindSafe(|| buf.set_len(5))).is_err());
        let buf = buf.into_buf();
        assert!(buf.unique());
        assert_eq!(buf.payload(), &[7, 8, 9]);
        drop(buf);
        assert_eq!(pool.live(), 0);
    }

    #[test]
    fn frames_are_empty_unique_and_of_their_pool() {
        let pool = TxPool::new(8);
        let source = TxSource::new(pool.clone());
        let frame = TxFrame::alloc(&pool).unwrap();
        assert_eq!((frame.len(), frame.capacity()), (0, 8));
        assert!(frame.unique());
        assert_eq!(frame.origin(), source.origin());
        assert_ne!(TxPool::new(8).alloc().unwrap().origin(), source.origin());
    }
}
