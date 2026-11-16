#![allow(unsafe_code)]

use alloc::rc::Rc;
use core::{ptr::NonNull, slice};
use switch_frame::{ImmutableFrame, OwnedFrame, RawParts};

pub use owner::TxPool;

/// Header bytes the stack may write in front of a TX frame's payload.
pub const TX_HEADROOM: usize = 64;

/// Allocate exclusive storage covering the pool's full payload room.
#[inline]
pub fn alloc_frame(pool: &TxPool) -> Option<OwnedFrame> {
    owner::alloc(pool)
}

// A borrowed snapshot neither acquires nor gives up a reference. RawParts has no Drop.
#[inline]
fn owned_parts(frame: &OwnedFrame) -> RawParts {
    // SAFETY: the copied handle is immediately disarmed, leaving frame's reference untouched.
    unsafe { core::ptr::read(frame) }.into_raw_parts()
}

#[inline]
fn shared_parts(frame: &ImmutableFrame) -> RawParts {
    // SAFETY: as owned_parts; no second live frame handle escapes.
    unsafe { core::ptr::read(frame) }.into_raw_parts()
}

#[cfg(test)]
pub(crate) fn frame_refcount(frame: &ImmutableFrame) -> u16 {
    // SAFETY: frame keeps its count alive on this thread.
    unsafe { shared_parts(frame).rc.as_ptr().read() }
}

/// Whether this producer-owned frame is at its original payload start.
///
/// # Safety
/// `frame` was allocated by this module.
#[inline]
pub(crate) unsafe fn frame_at_start(frame: &OwnedFrame) -> bool {
    let parts = owned_parts(frame);
    // SAFETY: the producer contract identifies the count's containing allocation.
    frame.data_ptr() == unsafe { owner::start(parts.rc) }
}

/// Write headers before an exclusively owned frame's original payload start.
///
/// # Safety
/// `frame` was allocated by this module's `alloc_frame` or `TxSource`.
#[inline]
pub unsafe fn frame_headroom(frame: &mut OwnedFrame, header_len: usize) -> (&mut [u8], &[u8]) {
    assert!(header_len <= TX_HEADROOM, "TX header exceeds the headroom");
    let parts = owned_parts(frame);
    let data = frame.data_ptr();
    // SAFETY: the producer contract gives this allocation's original payload start.
    assert!(unsafe { frame_at_start(frame) }, "TX frame start was sliced");
    // SAFETY: exclusive ownership covers this producer's headroom and payload, which do not overlap.
    unsafe {
        (
            slice::from_raw_parts_mut(data.as_ptr().sub(header_len), header_len),
            slice::from_raw_parts(data.as_ptr(), parts.len as usize),
        )
    }
}

/// The written header bytes followed by the payload.
///
/// # Safety
/// `frame` was allocated by this module and its preceding `header_len` bytes were initialized.
#[inline]
pub unsafe fn frame_packet(frame: &ImmutableFrame, header_len: usize) -> &[u8] {
    assert!(header_len <= TX_HEADROOM, "TX header exceeds the headroom");
    let parts = shared_parts(frame);
    let data = frame.data_ptr();
    // SAFETY: the producer contract identifies this allocation.
    assert_eq!(data, unsafe { owner::start(parts.rc) }, "TX frame start was sliced");
    // SAFETY: the checked start, producer's headroom and initialized payload form one range.
    unsafe { slice::from_raw_parts(data.as_ptr().sub(header_len), header_len + frame.len()) }
}

/// Extend an unsent, unique frame within its producer's room and append bytes.
///
/// # Safety
/// `frame` was allocated by this module.
#[inline]
pub(crate) unsafe fn append_frame(frame: &mut OwnedFrame, bytes: &[u8], cap: usize) -> usize {
    let parts = owned_parts(frame);
    // SAFETY: the producer contract identifies this allocation and its original payload room.
    let room = unsafe {
        let offset = frame.data_ptr().as_ptr() as usize - owner::start(parts.rc).as_ptr() as usize;
        owner::room(parts.rc) - offset
    };
    let old_len = frame.len();
    let count = room.saturating_sub(old_len).min(cap.saturating_sub(old_len)).min(bytes.len());
    // SAFETY: the new length stays within the producer's room, and the frame is exclusive.
    unsafe { frame.set_len((old_len + count) as u32) };
    frame[old_len..].copy_from_slice(&bytes[..count]);
    count
}

#[cfg(feature = "dpdk")]
pub use owner::frame_mbuf;

#[cfg(feature = "dpdk")]
mod owner {
    use super::*;
    use core::mem::offset_of;
    use switch_buffer::FRAME_DATA_OFF;
    use switch_dpdk_sys::{rte_mbuf, switch_pktmbuf_free_seg};

    pub use switch_buffer::FramePool as TxPool;

    const RC_OFF: usize = offset_of!(rte_mbuf, __bindgen_anon_1.__bindgen_anon_1.refcnt);

    #[inline]
    unsafe fn mbuf(rc: NonNull<u16>) -> NonNull<rte_mbuf> {
        // SAFETY: rc is the count embedded in a live frame-pool mbuf.
        unsafe { NonNull::new_unchecked(rc.as_ptr().cast::<u8>().sub(RC_OFF).cast()) }
    }

    /// Borrow the producer's mbuf without changing its reference count.
    ///
    /// # Safety
    /// `frame` was allocated by this module's DPDK producer.
    #[inline]
    pub unsafe fn frame_mbuf(frame: &ImmutableFrame) -> NonNull<rte_mbuf> {
        // SAFETY: the producer contract identifies the count as an mbuf field.
        unsafe { mbuf(shared_parts(frame).rc) }
    }

    unsafe extern "C" fn release(rc: NonNull<u16>, _: *mut ()) {
        // SAFETY: the last count remains 1; DPDK returns this direct, single-segment mbuf.
        unsafe { switch_pktmbuf_free_seg(mbuf(rc).as_ptr()) };
    }

    #[inline]
    pub(super) fn pool_origin(pool: &TxPool) -> *mut () {
        pool.mempool().cast()
    }

    #[inline]
    pub(super) fn alloc(pool: &TxPool) -> Option<OwnedFrame> {
        let mbuf = pool.alloc()?;
        // SAFETY: one live frame-pool allocation, its embedded count is 1. Derive both
        // pointers from the mbuf allocation so data retains provenance back to the count.
        unsafe {
            let raw = mbuf.as_ptr();
            let rc = NonNull::new_unchecked(core::ptr::addr_of_mut!((*raw).__bindgen_anon_1.__bindgen_anon_1.refcnt));
            let payload = (*raw).buf_addr.cast::<u8>().add(FRAME_DATA_OFF as usize);
            let data = NonNull::new_unchecked(raw.cast::<u8>().add(payload as usize - raw as usize));
            Some(OwnedFrame::from_raw_parts(data, u32::from(pool.room()), rc, release, pool_origin(pool)))
        }
    }

    pub(super) unsafe fn start(rc: NonNull<u16>) -> NonNull<u8> {
        // SAFETY: the count identifies this frame's mbuf; buf_addr never changes.
        unsafe {
            let raw = mbuf(rc).as_ptr();
            NonNull::new_unchecked((*raw).buf_addr.cast::<u8>().add(FRAME_DATA_OFF as usize))
        }
    }

    pub(super) unsafe fn room(rc: NonNull<u16>) -> usize {
        // SAFETY: this live direct mbuf's room extends from FRAME_DATA_OFF to buf_len.
        unsafe { usize::from((*mbuf(rc).as_ptr()).buf_len()) - FRAME_DATA_OFF as usize }
    }
}

#[cfg(not(feature = "dpdk"))]
mod owner {
    use super::*;
    use alloc::alloc::{alloc_zeroed, dealloc, handle_alloc_error, Layout};
    use core::{cell::Cell, mem::{offset_of, size_of}};

    #[derive(Default)]
    struct Counts {
        releases: Cell<usize>,
        live: Cell<usize>,
    }

    /// Heap TX storage for smoltcp tests, with a plain count before 64 bytes of headroom.
    pub struct TxPool {
        room: u32,
        counts: Rc<Counts>,
    }

    #[repr(C)]
    struct Storage {
        counts: Rc<Counts>,
        room: u32,
        references: u16,
    }

    fn layout(room: u32) -> Layout {
        Layout::from_size_align(size_of::<Storage>() + TX_HEADROOM + room as usize, core::mem::align_of::<Storage>()).unwrap()
    }

    unsafe fn storage(rc: NonNull<u16>) -> *mut Storage {
        // SAFETY: rc is this allocation's Storage::references field.
        unsafe { rc.as_ptr().cast::<u8>().sub(offset_of!(Storage, references)).cast() }
    }

    unsafe extern "C" fn release(rc: NonNull<u16>, _: *mut ()) {
        // SAFETY: the last frame owns the live allocation and its initialized header.
        unsafe {
            let raw = storage(rc);
            let header = raw.read();
            header.counts.releases.set(header.counts.releases.get() + 1);
            header.counts.live.set(header.counts.live.get() - 1);
            dealloc(raw.cast(), layout(header.room));
        }
    }

    pub(super) fn pool_origin(pool: &TxPool) -> *mut () {
        (pool as *const TxPool).cast_mut().cast()
    }

    pub(super) fn alloc(pool: &TxPool) -> Option<OwnedFrame> {
        pool.alloc()
    }

    pub(super) unsafe fn start(rc: NonNull<u16>) -> NonNull<u8> {
        // SAFETY: the initialized header is followed by headroom and payload in one allocation.
        unsafe { NonNull::new_unchecked(storage(rc).cast::<u8>().add(size_of::<Storage>() + TX_HEADROOM)) }
    }

    pub(super) unsafe fn room(rc: NonNull<u16>) -> usize {
        // SAFETY: the count keeps its containing Storage alive.
        unsafe { (*storage(rc)).room as usize }
    }

    impl TxPool {
        pub fn new(room: usize) -> Rc<TxPool> {
            let rc_off = size_of::<Storage>() + TX_HEADROOM - offset_of!(Storage, references);
            assert!(room <= u32::MAX as usize - rc_off, "TX room exceeds frame offset range");
            Rc::new(Self { room: u32::try_from(room).expect("TX room exceeds u32"), counts: Rc::default() })
        }

        pub fn alloc(&self) -> Option<OwnedFrame> {
            let layout = layout(self.room);
            // SAFETY: layout covers the header, count, headroom and initialized payload.
            unsafe {
                let raw = alloc_zeroed(layout).cast::<Storage>();
                if raw.is_null() { handle_alloc_error(layout); }
                raw.write(Storage { counts: self.counts.clone(), room: self.room, references: 1 });
                let rc = NonNull::new_unchecked(core::ptr::addr_of_mut!((*raw).references));
                self.counts.live.set(self.counts.live.get() + 1);
                Some(OwnedFrame::from_raw_parts(start(rc), self.room, rc, release, pool_origin(self)))
            }
        }

        pub fn live(&self) -> usize { self.counts.live.get() }

        /// Allocations returned by their final holder.
        pub fn releases(&self) -> usize { self.counts.releases.get() }
    }
}

/// Cloneable handle to a TX pool; each clone keeps the pool alive.
#[derive(Clone)]
pub struct TxSource(Rc<TxPool>);

impl TxSource {
    pub fn new(pool: Rc<TxPool>) -> TxSource { TxSource(pool) }

    /// Exclusive storage covering the full payload room, or `None` when the pool is empty.
    #[inline]
    pub fn alloc(&self) -> Option<OwnedFrame> { alloc_frame(&self.0) }

    #[inline]
    pub fn origin(&self) -> *mut () { owner::pool_origin(&self.0) }
}

impl core::fmt::Debug for TxSource {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("TxSource").field(&self.origin()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{catch_unwind, AssertUnwindSafe};


    #[test]
    fn clone_shares_and_drop_releases_once() {
        let pool = TxPool::new(8);
        let buf = pool.alloc().unwrap().freeze();
        let clone = buf.clone();
        assert_eq!(frame_refcount(&buf), 2);
        drop(clone);
        assert_eq!(frame_refcount(&buf), 1);
        assert_eq!(pool.releases(), 0);
        drop(buf);
        assert_eq!(pool.releases(), 1);
        assert_eq!(pool.live(), 0);
    }

    #[test]
    fn set_len_updates_this_buffer_at_refcount_1() {
        let pool = TxPool::new(8);
        let mut buf = pool.alloc().unwrap();
        buf[..3].copy_from_slice(&[1, 2, 3]);
        buf.truncate(3);
        assert_eq!(&*buf, &[1, 2, 3]);
    }

    #[test]
    fn dropping_original_then_clone_releases_storage_once() {
        let pool = TxPool::new(8);
        let buf = pool.alloc().unwrap().freeze();
        let clone = buf.clone();
        drop(buf);
        assert_eq!(frame_refcount(&clone), 1);
        assert_eq!(pool.live(), 1);
        drop(clone);
        assert_eq!(pool.live(), 0);
        assert_eq!(pool.releases(), 1);
    }

    #[test]
    fn into_raw_keeps_its_reference() {
        let pool = TxPool::new(8);
        let parts = pool.alloc().unwrap().into_raw_parts();
        assert_eq!(pool.live(), 1);
        // SAFETY: into_raw_parts transferred this sole reference.
        unsafe { (parts.release)(parts.rc, parts.ctx) };
        assert_eq!(pool.live(), 0);
        assert_eq!(pool.releases(), 1);
    }

    #[test]
    fn headroom_and_payload_form_the_packet() {
        let pool = TxPool::new(8);
        let mut buf = pool.alloc().unwrap();
        buf[..2].copy_from_slice(&[8, 9]);
        buf.truncate(2);
        // SAFETY: this frame is from our producer, still at its original start.
        let (headers, payload) = unsafe { frame_headroom(&mut buf, TX_HEADROOM) };
        assert_eq!(payload, &[8, 9]);
        headers.fill(7);
        headers[TX_HEADROOM - 1] = 1;
        let buf = buf.freeze();
        // SAFETY: all headroom was initialized above.
        unsafe {
            assert_eq!(frame_packet(&buf, 1), &[1, 8, 9]);
            assert_eq!(frame_packet(&buf, TX_HEADROOM).len(), TX_HEADROOM + 2);
            assert_eq!(frame_packet(&buf, 0), &[8, 9]);
        }
        let mut buf = buf.try_own().unwrap();
        assert!(catch_unwind(AssertUnwindSafe(|| {
            // SAFETY: this is our producer's frame; the excessive length must panic.
            let _ = unsafe { frame_headroom(&mut buf, TX_HEADROOM + 1) };
        })).is_err());
        let buf = buf.freeze();
        assert!(catch_unwind(AssertUnwindSafe(|| {
            // SAFETY: this is our producer's frame; the excessive length must panic.
            let _ = unsafe { frame_packet(&buf, TX_HEADROOM + 1) };
        })).is_err());
    }

    #[test]
    fn alloc_writes_payload_within_room() {
        let pool = TxPool::new(4);
        let source = TxSource::new(pool.clone());
        let mut buf = source.alloc().unwrap();
        buf[..3].copy_from_slice(&[7, 8, 9]);
        assert!(catch_unwind(AssertUnwindSafe(|| buf.truncate(5))).is_err());
        buf.truncate(3);
        assert_eq!((buf.len(), &*buf), (3, &[7, 8, 9][..]));
        let buf = buf.freeze();
        assert_eq!(frame_refcount(&buf), 1);
        assert_eq!(&*buf, &[7, 8, 9]);
        drop(buf);
        assert_eq!(pool.live(), 0);
    }

    #[test]
    fn frames_cover_full_room_and_are_of_their_pool() {
        let pool = TxPool::new(8);
        let source = TxSource::new(pool.clone());
        let frame = alloc_frame(&pool).unwrap().freeze();
        assert_eq!(frame.len(), 8);
        assert_eq!(frame_refcount(&frame), 1);
        assert_eq!(frame.ctx(), source.origin());
        assert_ne!(TxPool::new(8).alloc().unwrap().ctx(), source.origin());
    }

    #[test]
    #[should_panic(expected = "TX room exceeds frame offset range")]
    fn room_keeps_refcount_offset_representable() {
        let _ = TxPool::new(u32::MAX as usize);
    }

    #[test]
    fn sliced_frame_cannot_write_original_headroom() {
        let pool = TxPool::new(8);
        let mut frame = pool.alloc().unwrap().slice(1..8);
        assert!(catch_unwind(AssertUnwindSafe(|| {
            // SAFETY: it is this producer's allocation; the shifted start must be refused.
            let _ = unsafe { frame_headroom(&mut frame, 1) };
        })).is_err());
    }
}
