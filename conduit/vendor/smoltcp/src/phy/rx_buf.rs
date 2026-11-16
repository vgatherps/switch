#![allow(unsafe_code)]

use core::{fmt, mem::ManuallyDrop, ops::Range, ptr::NonNull, slice};

use alloc::{alloc::{alloc, dealloc, handle_alloc_error, Layout}, rc::Rc, vec::Vec};
use switch_frame::{OwnedFrame, Release};

use super::RxShared;

/// An owned receive frame with a storage release callback.
pub struct RxBuf {
    data: NonNull<u8>,
    len: usize,
    room: usize,
    rx_timestamp: u64,
    rc: NonNull<u16>,
    release: Release,
}

impl RxBuf {
    /// Take ownership of a frame and its storage.
    ///
    /// # Safety
    /// `data..data + len + room` must be readable, writable and exclusively owned.
    /// `rc` is an aligned u16 initialized to 1, preceding data in the same allocation.
    /// Both pointers derive from that allocation pointer, with
    /// `(data - rc) + len + room <= u32::MAX` and `len + room <= isize::MAX`.
    /// `release` must call [`Self::release_context`] with its context, then free the
    /// storage identified by `rc`. It runs once on the owning thread, with a null
    /// context for TCP or an undelivered buffer, or UDP socket accounting after hand-out.
    pub unsafe fn from_raw_parts(
        rc: NonNull<u16>,
        release: Release,
        data: NonNull<u8>,
        len: usize,
        room: usize,
    ) -> Self {
        Self {
            data,
            len,
            room,
            rx_timestamp: 0,
            rc,
            release,
        }
    }

    /// Copy a vector into storage with an embedded receive-frame count.
    pub fn from_vec(bytes: Vec<u8>) -> Self {
        heap_frame(&bytes, 0, #[cfg(test)] None)
    }

    /// Release the socket accounting passed to a receive producer's callback.
    ///
    /// # Safety
    /// `ctx` is the unmodified context supplied to the callback by this module,
    /// used exactly once, before freeing the receive storage.
    #[inline]
    pub unsafe fn release_context(ctx: *mut ()) {
        if !ctx.is_null() {
            // SAFETY: into_frame transferred exactly one Rc to this context.
            let shared = unsafe { Rc::from_raw(ctx.cast::<RxShared>()) };
            shared.release();
        }
    }

    #[inline]
    pub(crate) fn into_frame(self, shared: Option<Rc<RxShared>>) -> OwnedFrame {
        let this = ManuallyDrop::new(self);
        // SAFETY: the producer contract supplies unique storage, an embedded count
        // and its final release. A UDP reference keeps its accounting alive.
        unsafe {
            OwnedFrame::from_raw_parts(
                this.data, this.len as u32, this.rc, this.release,
                shared.map_or(core::ptr::null_mut(), |shared| Rc::into_raw(shared).cast_mut().cast()),
            )
        }
    }

    /// Raw value from the NIC driver; 0 means no hardware stamp.
    pub fn rx_timestamp(&self) -> u64 {
        self.rx_timestamp
    }

    /// Set the raw value from the NIC driver; 0 means no hardware stamp.
    pub fn set_rx_timestamp(&mut self, stamp: u64) {
        self.rx_timestamp = stamp;
    }

    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: the constructor requires this range to remain valid until drop.
        unsafe { slice::from_raw_parts(self.data.as_ptr(), self.len) }
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: the constructor requires exclusive ownership of this range,
        // and the mutable borrow prevents other views while this slice is live.
        unsafe { slice::from_raw_parts_mut(self.data.as_ptr(), self.len) }
    }

    /// Keep only the given range of the current view.
    /// Panics without changing the view if the range is out of bounds.
    pub(crate) fn narrow(&mut self, range: Range<usize>) {
        let len = self.as_slice()[range.clone()].len();
        let room = self
            .room
            .checked_add(self.len.checked_sub(range.end).expect("range exceeds frame"))
            .expect("frame room exceeds usize");
        // SAFETY: the slice index above checked range.start <= self.len. The pointer
        // keeps the constructor's provenance, which covers the room after the view.
        self.data = unsafe { self.data.add(range.start) };
        self.len = len;
        self.room = room;
    }


    #[cfg(test)]
    pub(crate) fn counted_frame(
        bytes: Vec<u8>,
        room: usize,
        releases: Rc<core::cell::Cell<usize>>,
    ) -> Self {
        heap_frame(&bytes, room, Some(releases))
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Drop for RxBuf {
    fn drop(&mut self) {
        // SAFETY: no hand-out occurred, so no socket accounting was transferred.
        unsafe { (self.release)(self.rc, core::ptr::null_mut()) };
    }
}

impl fmt::Debug for RxBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RxBuf")
            .field("len", &self.len)
            .field("room", &self.room)
            .finish()
    }
}

#[repr(C)]
struct HeapStorage {
    size: usize,
    #[cfg(test)]
    releases: Option<Rc<core::cell::Cell<usize>>>,
    refs: u16,
}

fn heap_layout(size: usize) -> Layout {
    Layout::from_size_align(size, core::mem::align_of::<HeapStorage>()).unwrap()
}

unsafe extern "C" fn release_heap(rc: NonNull<u16>, ctx: *mut ()) {
    // SAFETY: heap_frame embeds the count in this initialized allocation header.
    unsafe {
        RxBuf::release_context(ctx);
        let raw = rc.as_ptr().cast::<u8>()
            .sub(core::mem::offset_of!(HeapStorage, refs)).cast::<HeapStorage>();
        let header = raw.read();
        #[cfg(test)]
        if let Some(releases) = header.releases {
            releases.set(releases.get().checked_add(1).expect("release count overflow"));
        }
        dealloc(raw.cast(), heap_layout(header.size));
    }
}

fn heap_frame(
    bytes: &[u8],
    room: usize,
    #[cfg(test)] releases: Option<Rc<core::cell::Cell<usize>>>,
) -> RxBuf {
    let size = core::mem::size_of::<HeapStorage>()
        .checked_add(bytes.len()).and_then(|size| size.checked_add(room))
        .expect("receive storage exceeds usize");
    assert!(size <= u32::MAX as usize, "receive storage exceeds frame offset range");
    let layout = heap_layout(size);
    // SAFETY: one allocation covers its header, count, initialized data and room.
    unsafe {
        let raw = alloc(layout).cast::<HeapStorage>();
        if raw.is_null() { handle_alloc_error(layout); }
        raw.write(HeapStorage { size, #[cfg(test)] releases, refs: 1 });
        let data = NonNull::new_unchecked(raw.cast::<u8>().add(core::mem::size_of::<HeapStorage>()));
        core::ptr::copy_nonoverlapping(bytes.as_ptr(), data.as_ptr(), bytes.len());
        core::ptr::write_bytes(data.as_ptr().add(bytes.len()), 0, room);
        let rc = NonNull::new_unchecked(core::ptr::addr_of_mut!((*raw).refs));
        RxBuf::from_raw_parts(rc, release_heap, data, bytes.len(), room)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::{boxed::Box, rc::Rc};
    use core::cell::Cell;

    #[repr(C)]
    struct Storage {
        refs: u16,
        bytes: [u8; 6],
        releases: Rc<Cell<usize>>,
    }

    impl Drop for Storage {
        fn drop(&mut self) {
            self.releases
                .set(self.releases.get().checked_add(1).unwrap());
        }
    }

    unsafe extern "C" fn release_storage(rc: NonNull<u16>, ctx: *mut ()) {
        // SAFETY: owned_frame puts the count first in its repr(C) Storage.
        unsafe {
            RxBuf::release_context(ctx);
            drop(Box::from_raw(rc.cast::<Storage>().as_ptr()));
        }
    }

    #[test]
    fn rx_timestamp_kept_through_narrow() {
        let mut frame = RxBuf::counted_frame(alloc::vec![1, 2, 3], 2, Rc::new(Cell::new(0)));
        frame.set_rx_timestamp(7);
        frame.narrow(1..2);
        assert_eq!(frame.as_slice(), &[2]);
        assert_eq!(frame.rx_timestamp(), 7);
    }



    fn owned_frame(len: usize) -> (RxBuf, Rc<Cell<usize>>, NonNull<Storage>) {
        assert!(len <= 4);
        let releases = Rc::new(Cell::new(0));
        let storage = Storage {
            refs: 1,
            bytes: [10, 20, 30, 40, 50, 60],
            releases: releases.clone(),
        };
        let raw = Box::into_raw(Box::new(storage));
        let owner = NonNull::new(raw).unwrap();
        let data = unsafe {
            NonNull::new(core::ptr::addr_of_mut!((*raw).bytes).cast::<u8>().add(1)).unwrap()
        };
        // SAFETY: the box owns the range exclusively, len stays within bytes,
        // and release_storage drops the box once.
        let frame = unsafe { RxBuf::from_raw_parts(owner.cast(), release_storage, data, len, 0) };
        (frame, releases, owner)
    }

    #[test]
    fn release_runs_exactly_once() {
        let (frame, releases, _) = owned_frame(4);
        assert_eq!(releases.get(), 0);
        let moved = core::hint::black_box(frame);
        assert_eq!(releases.get(), 0);
        drop(moved);
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn view_ops_stay_in_bounds() {
        let (mut frame, releases, storage) = owned_frame(3);
        assert_eq!(frame.len(), 3);
        assert!(!frame.is_empty());
        assert_eq!(frame.as_slice(), &[20, 30, 40]);
        assert_eq!(frame.as_mut_slice().len(), 3);
        frame.as_mut_slice().copy_from_slice(&[21, 31, 41]);
        assert_eq!(frame.as_slice(), &[21, 31, 41]);
        // SAFETY: frame still owns the box and no mutable slice is live.
        assert_eq!(unsafe { storage.as_ref().bytes }, [10, 21, 31, 41, 50, 60]);
        drop(frame);
        assert_eq!(releases.get(), 1);

        let (mut empty, releases, storage) = owned_frame(0);
        assert_eq!(empty.len(), 0);
        assert!(empty.is_empty());
        assert!(empty.as_slice().is_empty());
        assert!(empty.as_mut_slice().is_empty());
        empty.as_mut_slice().fill(99);
        // SAFETY: empty still owns the box and no mutable slice is live.
        assert_eq!(unsafe { storage.as_ref().bytes }, [10, 20, 30, 40, 50, 60]);
        drop(empty);
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn from_vec_round_trips() {
        let mut bytes = Vec::with_capacity(32);
        bytes.extend_from_slice(&[1, 2, 3, 4]);
        assert!(bytes.capacity() > bytes.len());
        let data = bytes.as_ptr();
        let mut frame = RxBuf::from_vec(bytes);
        assert_ne!(frame.as_slice().as_ptr(), data);
        assert_eq!(frame.as_slice(), &[1, 2, 3, 4]);
        frame.as_mut_slice()[2] = 9;
        assert_eq!(frame.as_slice(), &[1, 2, 9, 4]);
        drop(frame);

        let empty = RxBuf::from_vec(Vec::new());
        assert!(empty.is_empty());
        assert!(empty.as_slice().is_empty());
        drop(empty);
    }

    #[test]
    fn narrow_stays_in_bounds() {
        let (mut frame, releases, storage) = owned_frame(4);
        let data = frame.as_slice().as_ptr();
        frame.narrow(0..4);
        assert_eq!(frame.as_slice().as_ptr(), data);
        assert_eq!(frame.as_slice(), &[20, 30, 40, 50]);
        frame.narrow(1..4);
        frame.narrow(1..2);
        assert_eq!(frame.as_slice().as_ptr(), data.wrapping_add(2));
        assert_eq!(frame.as_slice(), &[40]);
        frame.as_mut_slice()[0] = 41;
        // SAFETY: frame owns the box and no mutable slice is live.
        assert_eq!(unsafe { storage.as_ref().bytes }, [10, 20, 30, 41, 50, 60]);
        frame.narrow(1..1);
        assert_eq!(frame.as_slice().as_ptr(), data.wrapping_add(3));
        assert!(frame.is_empty());
        frame.narrow(0..0);
        assert!(frame.as_mut_slice().is_empty());
        assert_eq!(releases.get(), 0);
        drop(frame);
        assert_eq!(releases.get(), 1);

        let (mut frame, releases, _) = owned_frame(0);
        frame.narrow(0..0);
        assert!(frame.is_empty());
        drop(frame);
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn narrow_panic_leaves_view_unchanged() {
        let (mut frame, releases, _) = owned_frame(4);
        frame.narrow(1..3);
        let data = frame.as_slice().as_ptr();
        for range in [0..3, 3..3, 2..1, 0..usize::MAX] {
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| frame.narrow(range)))
                    .is_err()
            );
            assert_eq!(frame.as_slice().as_ptr(), data);
            assert_eq!(frame.as_slice(), &[30, 40]);
            assert_eq!(releases.get(), 0);
        }
        drop(frame);
        assert_eq!(releases.get(), 1);
    }

}
