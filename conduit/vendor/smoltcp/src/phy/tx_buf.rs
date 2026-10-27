#![allow(unsafe_code)]

use alloc::{rc::Rc, vec, vec::Vec};
use core::{mem::ManuallyDrop, ptr::NonNull, slice};

use super::TxChecksum;

/// Reference operations on a TxBuf's owner. One TxBuf value is one reference.
pub struct TxBufOps {
    pub release: unsafe fn(owner: NonNull<()>),
    pub share: unsafe fn(owner: NonNull<()>),
    pub unique: unsafe fn(owner: NonNull<()>) -> bool,
}

/// One reference to a TX payload buffer. Payload is immutable while another reference exists.
pub struct TxBuf {
    data: NonNull<u8>,
    len: u32,
    room: u32,
    owner: NonNull<()>,
    origin: NonNull<()>,
    ops: &'static TxBufOps,
}

impl TxBuf {
    /// # Safety
    /// `data .. data + room` is valid for reads and writes, lies in one allocation, and is
    /// reachable only through `owner`'s references. The caller transfers one reference of
    /// `owner`; `ops` matches it. The payload length starts at zero.
    pub unsafe fn from_raw_parts(
        owner: NonNull<()>,
        ops: &'static TxBufOps,
        origin: NonNull<()>,
        data: NonNull<u8>,
        room: usize,
    ) -> TxBuf {
        TxBuf {
            data,
            len: 0,
            room: room.try_into().expect("TX payload room exceeds u32"),
            owner,
            origin,
            ops,
        }
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn capacity(&self) -> usize {
        self.room as usize
    }

    pub fn origin(&self) -> NonNull<()> {
        self.origin
    }

    pub fn owner(&self) -> NonNull<()> {
        self.owner
    }

    pub fn unique(&self) -> bool {
        // SAFETY: from_raw_parts requires ops to match owner and owner remains live.
        unsafe { (self.ops.unique)(self.owner) }
    }

    pub fn payload(&self) -> &[u8] {
        // SAFETY: from_raw_parts requires data..data + room to stay valid. len never exceeds room.
        unsafe { slice::from_raw_parts(self.data.as_ptr(), self.len()) }
    }

    /// Return the whole payload room. Panics unless this is the only reference.
    pub fn room_mut(&mut self) -> &mut [u8] {
        assert!(self.unique(), "TX buffer is shared");
        // SAFETY: uniqueness gives exclusive access and the mutable borrow prevents another view.
        unsafe { slice::from_raw_parts_mut(self.data.as_ptr(), self.capacity()) }
    }

    /// Set the payload length. Panics if the length exceeds the room or the buffer is shared.
    pub fn set_len(&mut self, len: usize) {
        assert!(len <= self.capacity(), "TX payload length exceeds room");
        assert!(self.unique(), "TX buffer is shared");
        self.len = len.try_into().expect("TX payload length exceeds u32");
    }

    /// Give up this value's owner reference without releasing it.
    pub fn into_raw(self) -> NonNull<()> {
        ManuallyDrop::new(self).owner
    }
}

impl Clone for TxBuf {
    fn clone(&self) -> TxBuf {
        // SAFETY: from_raw_parts requires ops to match the live owner.
        unsafe { (self.ops.share)(self.owner) };
        TxBuf {
            data: self.data,
            len: self.len,
            room: self.room,
            owner: self.owner,
            origin: self.origin,
            ops: self.ops,
        }
    }
}

impl Drop for TxBuf {
    fn drop(&mut self) {
        // SAFETY: from_raw_parts transfers one owner reference to this value.
        unsafe { (self.ops.release)(self.owner) };
    }
}

/// Header bytes for one send, built by the stack and moved to the device.
#[derive(Clone, Copy)]
pub struct TxHeader {
    bytes: [u8; 64],
    len: u8,
    tx_checksum: Option<TxChecksum>,
}

impl TxHeader {
    pub fn new(len: usize, tx_checksum: Option<TxChecksum>) -> TxHeader {
        assert!(len <= 64, "TX header exceeds 64 bytes");
        TxHeader {
            bytes: [0; 64],
            len: len.try_into().expect("TX header length exceeds u8"),
            tx_checksum,
        }
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len)]
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        let len = usize::from(self.len);
        &mut self.bytes[..len]
    }

    pub fn tx_checksum(&self) -> Option<TxChecksum> {
        self.tx_checksum
    }
}

pub(crate) fn frame_bytes(header: TxHeader, frame: &TxBuf) -> Vec<u8> {
    assert!(frame.unique(), "TX buffer is shared");
    let mut bytes = Vec::with_capacity(
        header
            .bytes()
            .len()
            .checked_add(frame.len())
            .expect("TX frame length overflow"),
    );
    bytes.extend_from_slice(header.bytes());
    bytes.extend_from_slice(frame.payload());
    bytes
}

/// Allocator behind a TxSource. Every TxBuf it returns has length zero, is unique,
/// has the same origin as this allocator, and has at least 64 bytes of owner headroom.
pub trait TxAlloc {
    fn alloc(&self) -> Option<TxBuf>;
    fn origin(&self) -> NonNull<()>;
}

/// Cloneable allocator handle that keeps its backing alive.
#[derive(Clone)]
pub struct TxSource(Rc<dyn TxAlloc>);

impl TxSource {
    pub fn new(alloc: Rc<dyn TxAlloc>) -> TxSource {
        TxSource(alloc)
    }

    pub fn alloc(&self) -> Option<TxBuf> {
        self.0.alloc()
    }

    pub fn origin(&self) -> NonNull<()> {
        self.0.origin()
    }

}

/// The app's frame. Clone is cheap; after a clone the payload is read-only.
#[derive(Clone)]
pub struct TxFrame(TxBuf);

impl TxFrame {
    /// Wrap an empty, unique TX buffer.
    pub fn new(buf: TxBuf) -> TxFrame {
        assert!(buf.unique(), "TX frame buffer is shared");
        assert_eq!(buf.len(), 0, "TX frame buffer is not empty");
        TxFrame(buf)
    }

    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn room_mut(&mut self) -> &mut [u8] {
        self.0.room_mut()
    }

    pub fn set_len(&mut self, len: usize) {
        self.0.set_len(len);
    }

    pub fn payload(&self) -> &[u8] {
        self.0.payload()
    }

    pub fn origin(&self) -> NonNull<()> {
        self.0.origin()
    }

    pub(crate) fn unique(&self) -> bool {
        self.0.unique()
    }

    pub(crate) fn into_buf(self) -> TxBuf {
        self.0
    }
}

#[cfg(test)]
pub(crate) mod test_owner {
    use super::*;
    use alloc::boxed::Box;
    use core::cell::Cell;

    const HEADROOM: usize = 64;

    struct Counts {
        allocs: Cell<usize>,
        shares: Cell<usize>,
        releases: Cell<usize>,
        live: Cell<usize>,
    }

    pub(crate) struct CountingAlloc {
        room: usize,
        counts: Rc<Counts>,
    }

    struct Storage {
        bytes: Box<[u8]>,
        references: Cell<usize>,
        counts: Rc<Counts>,
    }

    unsafe fn release(owner: NonNull<()>) {
        // SAFETY: CountingAlloc::alloc creates owner with Box::into_raw and keeps it live
        // until the reference count reaches zero.
        let storage = unsafe { owner.cast::<Storage>().as_ref() };
        storage.counts.releases.set(storage.counts.releases.get().checked_add(1).expect("release count overflow"));
        let remaining = storage.references.get().checked_sub(1).expect("TX buffer reference count underflow");
        storage.references.set(remaining);
        if remaining == 0 {
            storage.counts.live.set(storage.counts.live.get().checked_sub(1).expect("live allocation count underflow"));
            // SAFETY: this is the last reference to the Box created by CountingAlloc::alloc.
            unsafe { drop(Box::from_raw(owner.cast::<Storage>().as_ptr())) };
        }
    }

    unsafe fn share(owner: NonNull<()>) {
        // SAFETY: owner points to a live Storage while a TxBuf reference exists.
        let storage = unsafe { owner.cast::<Storage>().as_ref() };
        storage.references.set(storage.references.get().checked_add(1).expect("TX buffer reference count overflow"));
        storage.counts.shares.set(storage.counts.shares.get().checked_add(1).expect("share count overflow"));
    }

    unsafe fn unique(owner: NonNull<()>) -> bool {
        // SAFETY: owner points to a live Storage while a TxBuf reference exists.
        unsafe { owner.cast::<Storage>().as_ref() }.references.get() == 1
    }

    pub(super) static OPS: TxBufOps = TxBufOps { release, share, unique };

    impl CountingAlloc {
        pub(crate) fn new(room: usize) -> Rc<CountingAlloc> {
            Rc::new(CountingAlloc {
                room,
                counts: Rc::new(Counts {
                    allocs: Cell::new(0),
                    shares: Cell::new(0),
                    releases: Cell::new(0),
                    live: Cell::new(0),
                }),
            })
        }

        pub(crate) fn live(&self) -> usize { self.counts.live.get() }
        pub(crate) fn shares(&self) -> usize { self.counts.shares.get() }
        pub(crate) fn releases(&self) -> usize { self.counts.releases.get() }
    }

    impl TxAlloc for CountingAlloc {
        fn alloc(&self) -> Option<TxBuf> {
            let total = HEADROOM.checked_add(self.room).expect("test TX storage size overflow");
            let mut storage = Box::new(Storage {
                bytes: vec![0; total].into_boxed_slice(),
                references: Cell::new(1),
                counts: self.counts.clone(),
            });
            let data = NonNull::new(storage.bytes.as_mut_ptr()).expect("boxed slice pointer is null");
            // SAFETY: HEADROOM is within the allocation by construction.
            let data = unsafe { data.add(HEADROOM) };
            let owner = NonNull::new(Box::into_raw(storage).cast::<()>()).expect("Box::into_raw returned null");
            self.counts.allocs.set(self.counts.allocs.get().checked_add(1).expect("allocation count overflow"));
            self.counts.live.set(self.counts.live.get().checked_add(1).expect("live allocation count overflow"));
            // SAFETY: Storage owns room bytes from data until OPS releases its last reference.
            Some(unsafe { TxBuf::from_raw_parts(owner, &OPS, self.origin(), data, self.room) })
        }

        fn origin(&self) -> NonNull<()> {
            NonNull::from(self).cast()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_owner::CountingAlloc;
    use super::*;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    #[test]
    fn clone_shares_and_drop_releases_once() {
        let alloc = CountingAlloc::new(8);
        let buf = alloc.alloc().unwrap();
        let clone = buf.clone();
        assert_eq!(alloc.shares(), 1);
        assert!(!buf.unique());
        drop(clone);
        assert_eq!(alloc.releases(), 1);
        assert!(buf.unique());
        drop(buf);
        assert_eq!(alloc.releases(), 2);
        assert_eq!(alloc.live(), 0);
    }

    #[test]
    fn room_mut_and_set_len_panic_when_shared() {
        let alloc = CountingAlloc::new(8);
        let mut buf = alloc.alloc().unwrap();
        let clone = buf.clone();
        assert!(catch_unwind(AssertUnwindSafe(|| {
            let _ = buf.room_mut();
        }))
        .is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| buf.set_len(1))).is_err());
        drop(clone);
    }

    #[test]
    fn set_len_updates_this_buffer_at_refcount_1() {
        let alloc = CountingAlloc::new(8);
        let mut buf = alloc.alloc().unwrap();
        buf.room_mut()[..3].copy_from_slice(&[1, 2, 3]);
        buf.set_len(3);
        assert_eq!(buf.payload(), &[1, 2, 3]);
    }

    #[test]
    fn dropping_original_then_clone_releases_storage_once() {
        let alloc = CountingAlloc::new(8);
        let buf = alloc.alloc().unwrap();
        let clone = buf.clone();
        drop(buf);
        assert_eq!(alloc.live(), 1);
        drop(clone);
        assert_eq!(alloc.live(), 0);
        assert_eq!(alloc.releases(), 2);
    }

    #[test]
    fn into_raw_keeps_its_reference() {
        let alloc = CountingAlloc::new(8);
        let buf = alloc.alloc().unwrap();
        let owner = buf.into_raw();
        assert_eq!(alloc.live(), 1);
        // SAFETY: into_raw transfers this sole reference to the caller.
        unsafe { (test_owner::OPS.release)(owner) };
        assert_eq!(alloc.live(), 0);
        assert_eq!(alloc.releases(), 1);
    }

    #[test]
    fn tx_frame_new_rejects_shared_or_nonempty_buffers() {
        let alloc = CountingAlloc::new(8);
        let buf = alloc.alloc().unwrap();
        let clone = buf.clone();
        assert!(catch_unwind(AssertUnwindSafe(|| TxFrame::new(buf))).is_err());
        drop(clone);

        let mut buf = alloc.alloc().unwrap();
        buf.set_len(1);
        assert!(catch_unwind(AssertUnwindSafe(|| TxFrame::new(buf))).is_err());
    }

    #[test]
    fn tx_header_len_bounds() {
        let mut header = TxHeader::new(64, None);
        header.bytes_mut()[63] = 7;
        assert_eq!(header.bytes().len(), 64);
        assert_eq!(header.bytes()[63], 7);
        assert!(catch_unwind(|| TxHeader::new(65, None)).is_err());
    }
}
