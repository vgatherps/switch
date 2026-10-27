#![allow(unsafe_code)]

use alloc::{rc::Rc, vec::Vec};
use core::{mem::ManuallyDrop, ptr::NonNull, slice};

/// Reference operations on a TxBuf's owner. One TxBuf value is one reference.
pub struct TxBufOps {
    pub release: unsafe fn(owner: NonNull<()>),
    pub share: unsafe fn(owner: NonNull<()>),
    pub unique: unsafe fn(owner: NonNull<()>) -> bool,
}

/// Owned reference to one TX payload buffer with writable headroom in front of it.
pub struct TxBuf {
    data: NonNull<u8>,
    len: u32,
    room: u32,
    headroom: u32,
    owner: NonNull<()>,
    origin: NonNull<()>,
    ops: &'static TxBufOps,
}

impl TxBuf {
    /// Build a TX buffer from storage owned by `owner`.
    ///
    /// # Safety
    /// `data - headroom .. data + room` is valid for reads and writes, lies in one allocation,
    /// and is reachable only through this owner's references. The caller transfers one reference
    /// of `owner`. `ops` matches `owner`. Payload length starts at zero.
    pub unsafe fn from_raw_parts(
        owner: NonNull<()>,
        ops: &'static TxBufOps,
        origin: NonNull<()>,
        data: NonNull<u8>,
        headroom: usize,
        room: usize,
    ) -> TxBuf {
        TxBuf {
            data,
            len: 0,
            room: room.try_into().expect("TX payload room exceeds u32"),
            headroom: headroom.try_into().expect("TX headroom exceeds u32"),
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

    pub fn headroom(&self) -> usize {
        self.headroom as usize
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

    /// Return the header room immediately before the payload and the payload itself.
    ///
    /// # Safety
    /// `unique()` holds, `n <= headroom()`, and no other borrow of the header bytes is live.
    pub unsafe fn headers_and_payload(&self, n: usize) -> (&mut [u8], &[u8]) {
        assert!(self.unique(), "TX buffer is shared");
        assert!(n <= self.headroom(), "TX header exceeds headroom");
        // SAFETY: the constructor guarantees the full headroom and payload ranges are valid.
        // The caller guarantees no header alias, and the two returned ranges do not overlap.
        unsafe {
            let headers = slice::from_raw_parts_mut(self.data.as_ptr().sub(n), n);
            let payload = slice::from_raw_parts(self.data.as_ptr(), self.len());
            (headers, payload)
        }
    }

    /// Add one owner reference and return it.
    pub fn share_raw(&self) -> NonNull<()> {
        // SAFETY: from_raw_parts requires ops to match the live owner.
        unsafe { (self.ops.share)(self.owner) };
        self.owner
    }

    /// Give up this value's owner reference without releasing it.
    pub fn into_raw(self) -> NonNull<()> {
        ManuallyDrop::new(self).owner
    }
}

impl Drop for TxBuf {
    fn drop(&mut self) {
        // SAFETY: from_raw_parts transfers one owner reference to this value.
        unsafe { (self.ops.release)(self.owner) };
    }
}

pub(crate) fn copy_with_headers<R, F>(frame: &TxBuf, header_len: usize, f: F) -> (R, Vec<u8>)
where
    F: FnOnce(&mut [u8], &[u8]) -> R,
{
    // SAFETY: headers_and_payload checks uniqueness and headroom. Safe code cannot hold
    // another header view, and the payload is borrowed read-only.
    let (headers, payload) = unsafe { frame.headers_and_payload(header_len) };
    let result = f(headers, payload);
    let mut bytes = Vec::with_capacity(
        header_len
            .checked_add(payload.len())
            .expect("TX frame length overflow"),
    );
    bytes.extend_from_slice(headers);
    bytes.extend_from_slice(payload);
    (result, bytes)
}

/// Allocator behind a TxSource. Every TxBuf it returns has length zero, is unique,
/// has at least 54 bytes of headroom, and has the same origin as this allocator.
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

/// The app's unique TX frame.
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

    pub(crate) fn into_buf(self) -> TxBuf {
        self.0
    }
}

#[cfg(test)]
pub(crate) mod test_owner {
    use super::*;
    use alloc::{boxed::Box, vec};
    use core::cell::Cell;

    struct Counts {
        allocs: Cell<usize>,
        shares: Cell<usize>,
        releases: Cell<usize>,
        live: Cell<usize>,
    }

    pub(crate) struct CountingAlloc {
        room: usize,
        headroom: usize,
        counts: Rc<Counts>,
    }

    struct Storage {
        bytes: Box<[u8]>,
        references: Cell<usize>,
        counts: Rc<Counts>,
    }

    pub(crate) unsafe fn release(owner: NonNull<()>) {
        // SAFETY: CountingAlloc::alloc creates owner with Box::into_raw and keeps it live
        // until the reference count reaches zero.
        let storage = unsafe { owner.cast::<Storage>().as_ref() };
        storage.counts.releases.set(
            storage
                .counts
                .releases
                .get()
                .checked_add(1)
                .expect("release count overflow"),
        );
        let remaining = storage
            .references
            .get()
            .checked_sub(1)
            .expect("TX buffer reference count underflow");
        storage.references.set(remaining);
        if remaining == 0 {
            storage.counts.live.set(
                storage
                    .counts
                    .live
                    .get()
                    .checked_sub(1)
                    .expect("live allocation count underflow"),
            );
            // SAFETY: this is the last reference to the Box created by CountingAlloc::alloc.
            unsafe { drop(Box::from_raw(owner.cast::<Storage>().as_ptr())) };
        }
    }

    unsafe fn share(owner: NonNull<()>) {
        // SAFETY: owner points to a live Storage while a TxBuf reference exists.
        let storage = unsafe { owner.cast::<Storage>().as_ref() };
        storage.references.set(
            storage
                .references
                .get()
                .checked_add(1)
                .expect("TX buffer reference count overflow"),
        );
        storage.counts.shares.set(
            storage
                .counts
                .shares
                .get()
                .checked_add(1)
                .expect("share count overflow"),
        );
    }

    unsafe fn unique(owner: NonNull<()>) -> bool {
        // SAFETY: owner points to a live Storage while a TxBuf reference exists.
        unsafe { owner.cast::<Storage>().as_ref() }.references.get() == 1
    }

    static OPS: TxBufOps = TxBufOps {
        release,
        share,
        unique,
    };

    impl CountingAlloc {
        pub(crate) fn new(room: usize, headroom: usize) -> Rc<CountingAlloc> {
            assert!(headroom >= 54, "test TX allocator needs 54 bytes of headroom");
            Rc::new(CountingAlloc {
                room,
                headroom,
                counts: Rc::new(Counts {
                    allocs: Cell::new(0),
                    shares: Cell::new(0),
                    releases: Cell::new(0),
                    live: Cell::new(0),
                }),
            })
        }

        pub(crate) fn live(&self) -> usize {
            self.counts.live.get()
        }

        pub(crate) fn shares(&self) -> usize {
            self.counts.shares.get()
        }

        pub(crate) fn releases(&self) -> usize {
            self.counts.releases.get()
        }
    }

    impl TxAlloc for CountingAlloc {
        fn alloc(&self) -> Option<TxBuf> {
            let total = self
                .headroom
                .checked_add(self.room)
                .expect("test TX storage size overflow");
            let mut storage = Box::new(Storage {
                bytes: vec![0; total].into_boxed_slice(),
                references: Cell::new(1),
                counts: self.counts.clone(),
            });
            let data = NonNull::new(storage.bytes.as_mut_ptr())
                .expect("boxed slice pointer is null");
            // SAFETY: headroom is within the allocation by construction.
            let data = unsafe { data.add(self.headroom) };
            let owner = NonNull::new(Box::into_raw(storage).cast::<()>())
                .expect("Box::into_raw returned null");
            self.counts.allocs.set(
                self.counts
                    .allocs
                    .get()
                    .checked_add(1)
                    .expect("allocation count overflow"),
            );
            self.counts.live.set(
                self.counts
                    .live
                    .get()
                    .checked_add(1)
                    .expect("live allocation count overflow"),
            );
            // SAFETY: Storage owns headroom + room bytes until OPS releases its last reference.
            Some(unsafe {
                TxBuf::from_raw_parts(owner, &OPS, self.origin(), data, self.headroom, self.room)
            })
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

    #[test]
    fn share_release_unique_counts() {
        let alloc = CountingAlloc::new(8, 64);
        let buf = alloc.alloc().unwrap();
        assert!(buf.unique());
        let owner = buf.share_raw();
        assert!(!buf.unique());
        // SAFETY: share_raw added one reference governed by the matching test owner ops.
        unsafe { (buf.ops.release)(owner) };
        assert!(buf.unique());
        assert_eq!(alloc.shares(), 1);
        assert_eq!(alloc.releases(), 1);
    }

    #[test]
    fn release_runs_once() {
        let alloc = CountingAlloc::new(8, 64);
        let buf = alloc.alloc().unwrap();
        assert_eq!(alloc.live(), 1);
        drop(buf);
        assert_eq!(alloc.live(), 0);
        assert_eq!(alloc.releases(), 1);
    }

    #[test]
    fn headroom_and_payload_disjoint() {
        let alloc = CountingAlloc::new(8, 64);
        let mut buf = alloc.alloc().unwrap();
        buf.room_mut()[..3].copy_from_slice(&[1, 2, 3]);
        buf.set_len(3);
        // SAFETY: buf is unique, two bytes fit in its headroom, and no header borrow exists.
        let (headers, payload) = unsafe { buf.headers_and_payload(2) };
        headers.copy_from_slice(&[4, 5]);
        assert_eq!(payload, &[1, 2, 3]);
        assert_eq!(buf.payload(), &[1, 2, 3]);
    }
}
