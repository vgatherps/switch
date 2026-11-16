//! The two handles to packet bytes, [`OwnedFrame`] and [`ImmutableFrame`]. Every layer of
//! switch hands received data and send buffers to applications as frames, which reach them as
//! `switch::net::OwnedFrame` and `switch::net::ImmutableFrame`. Libraries that manage packet
//! memory create frames over it with [`OwnedFrame::from_raw_parts`].

#![no_std]

use core::fmt;
use core::mem::{ManuallyDrop, align_of, size_of};
use core::ops::{Deref, DerefMut, Range};
use core::ptr::NonNull;

/// A producer's function that takes frame storage back.
///
/// Called exactly once, on the thread that drops the last handle, inside that `drop`. `rc`
/// points at the frame's count, still 1; `ctx` is the value given to
/// [`OwnedFrame::from_raw_parts`].
pub type Release = unsafe extern "C" fn(rc: NonNull<u16>, ctx: *mut ());

/// One reference to frame storage, taken out of a frame by `into_raw_parts` without changing
/// the count. Whoever holds the `RawParts` that carries the last reference finishes it by
/// calling `release(rc, ctx)`.
pub struct RawParts {
    pub data: NonNull<u8>,
    pub len: u32,
    pub rc: NonNull<u16>,
    pub release: Release,
    pub ctx: *mut (),
}

#[repr(C)]
struct RawFrame {
    data: NonNull<u8>,
    len: u32,
    rc_off: u32,
    release: Release,
    ctx: *mut (),
}

const _: () = {
    assert!(size_of::<RawFrame>() == 32);
    assert!(align_of::<RawFrame>() == 8);
};

impl RawFrame {
    fn rc(&self) -> NonNull<u16> {
        // SAFETY: the constructor's contract places the count rc_off bytes before data.
        unsafe { NonNull::new_unchecked(self.data.as_ptr().sub(self.rc_off as usize).cast()) }
    }

    fn slice(&mut self, range: Range<usize>) {
        assert!(range.start <= range.end && range.end <= self.len as usize);
        // SAFETY: the bounds check keeps the new pointer within the original payload.
        self.data = unsafe { NonNull::new_unchecked(self.data.as_ptr().add(range.start)) };
        self.len = (range.end - range.start) as u32;
        self.rc_off += range.start as u32;
    }

    fn into_raw_parts(self) -> RawParts {
        let frame = ManuallyDrop::new(self);
        RawParts {
            data: frame.data,
            len: frame.len,
            rc: frame.rc(),
            release: frame.release,
            ctx: frame.ctx,
        }
    }
}

impl Drop for RawFrame {
    fn drop(&mut self) {
        let rc = self.rc();
        // SAFETY: this thread exclusively accesses the count; each frame owns one reference.
        unsafe {
            let count = rc.as_ptr().read();
            if count == 1 {
                (self.release)(rc, self.ctx);
            } else {
                rc.as_ptr().write(count - 1);
            }
        }
    }
}

/// The only handle to a run of packet bytes; dereferences to `&[u8]` and `&mut [u8]`.
///
/// A frame is a handle to bytes that a producer owns: a network card's packet buffer, a
/// record in a shared-memory ring, a buffer in a pool. Holding any handle to the bytes keeps
/// them in use. When the last handle is dropped, the producer's [`Release`] function runs on
/// the dropping thread and the bytes go back to the producer. A frame stays valid after the
/// socket, network or session it came from is gone.
///
/// ```text
///                    freeze()
///    OwnedFrame  ----------------->  ImmutableFrame  --clone()-->  ImmutableFrame
///    (sole handle,                   (shared, read-only)            (same bytes)
///     writable)   <-----------------
///                    try_own()
///                    Ok only when this is the last handle
/// ```
///
/// Frames are single-threaded: neither frame type is `Send` or `Sync`, and the count of
/// handles is a plain integer, not an atomic. To pass packet data to another thread, copy the
/// bytes out.
#[repr(transparent)]
pub struct OwnedFrame(RawFrame);

impl OwnedFrame {
    /// Creates the first handle to a producer's packet storage. For libraries that manage
    /// packet memory.
    ///
    /// The count is a `u16` in the same allocation as the data, before it. Frames outlive the
    /// objects that handed them out, so the producer keeps everything `release` uses alive
    /// until the last handle is dropped; the usual way is to pass a counted reference
    /// (`Rc::into_raw`) as `ctx` and take it back in `release`.
    ///
    /// # Safety
    /// `*rc == 1` and the count is owned by this frame. `rc` must be aligned for
    /// `u16`, and the whole count must precede the payload:
    /// `rc.as_ptr().cast::<u8>().add(2) <= data.as_ptr()`. Both pointers must be
    /// derived from the same allocation pointer, so subtracting the stored offset
    /// from `data` can reach the count, with `(data - rc) + len <= u32::MAX`.
    /// `data..data+len` is valid for reads and writes while the count is nonzero.
    /// Other holders may share the count if they use the same plain increment/decrement
    /// protocol on this thread and the last holder calls `release(rc, ctx)` exactly once.
    /// Mutable access requires exclusive ownership of the storage.
    pub unsafe fn from_raw_parts(
        data: NonNull<u8>,
        len: u32,
        rc: NonNull<u16>,
        release: Release,
        ctx: *mut (),
    ) -> Self {
        Self(RawFrame {
            data,
            len,
            rc_off: (data.as_ptr() as usize - rc.as_ptr() as usize) as u32,
            release,
            ctx,
        })
    }

    /// Turns this into a shared, read-only handle, without copying or changing the count.
    pub fn freeze(self) -> ImmutableFrame {
        ImmutableFrame(self.0)
    }

    /// Narrows the frame to `range` of its bytes, without copying.
    ///
    /// # Panics
    /// Panics if `range.start > range.end` or `range.end` is past the end. The consumed frame
    /// is still released.
    pub fn slice(mut self, range: Range<usize>) -> Self {
        self.0.slice(range);
        self
    }

    /// Sets the length of producer-owned storage, for example to append after the current end.
    /// For libraries that manage packet memory.
    ///
    /// # Safety
    /// `data..data+len` is storage this frame owns; the producer knows its room.
    /// The count offset must also satisfy `(data - rc) + len <= u32::MAX`,
    /// as required by `OwnedFrame::from_raw_parts`.
    pub unsafe fn set_len(&mut self, len: u32) {
        self.0.len = len;
    }

    /// Shortens the frame to `len` bytes, keeping its start.
    ///
    /// # Panics
    /// Panics if `len` is longer than the frame.
    pub fn truncate(&mut self, len: usize) {
        assert!(len <= self.0.len as usize);
        self.0.len = len as u32;
    }

    /// The data pointer, with its original provenance. For producers.
    pub fn data_ptr(&self) -> NonNull<u8> {
        self.0.data
    }

    /// The `ctx` given to [`from_raw_parts`](Self::from_raw_parts). For producers.
    pub fn ctx(&self) -> *mut () {
        self.0.ctx
    }

    /// Turns this handle into a [`RawParts`] without changing the count. For producers.
    pub fn into_raw_parts(self) -> RawParts {
        self.0.into_raw_parts()
    }
}

impl Deref for OwnedFrame {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: the frame holds a reference to the valid payload.
        unsafe { core::slice::from_raw_parts(self.0.data.as_ptr(), self.0.len as usize) }
    }
}

impl DerefMut for OwnedFrame {
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: OwnedFrame holds the sole reference to the payload.
        unsafe { core::slice::from_raw_parts_mut(self.0.data.as_ptr(), self.0.len as usize) }
    }
}

impl fmt::Debug for OwnedFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedFrame").field("len", &self.0.len).finish()
    }
}

/// A shared, read-only handle to packet bytes; dereferences to `&[u8]`. See [`OwnedFrame`].
///
/// `clone` copies no bytes: it adds one to the count kept beside the data. The bytes can have
/// at most 65,535 live handles; the clone that would make one more panics and acquires
/// nothing. A slice of a shared frame still counts as a handle to the original bytes.
#[repr(transparent)]
pub struct ImmutableFrame(RawFrame);

impl ImmutableFrame {
    /// The data pointer, with its original provenance. For producers.
    pub fn data_ptr(&self) -> NonNull<u8> {
        self.0.data
    }

    /// The `ctx` given to [`OwnedFrame::from_raw_parts`]. For producers.
    pub fn ctx(&self) -> *mut () {
        self.0.ctx
    }

    /// Turns this handle into a [`RawParts`] without changing the count. For producers.
    pub fn into_raw_parts(self) -> RawParts {
        self.0.into_raw_parts()
    }

    /// Returns the frame as an [`OwnedFrame`] when no other handle to its bytes is alive, a
    /// clone or a slice anywhere included; gives it back as `Err` otherwise.
    pub fn try_own(self) -> Result<OwnedFrame, Self> {
        // SAFETY: the frame keeps the count alive and only this thread accesses it.
        if unsafe { self.0.rc().as_ptr().read() } == 1 {
            Ok(OwnedFrame(self.0))
        } else {
            Err(self)
        }
    }

    /// Narrows the frame to `range` of its bytes, without copying.
    ///
    /// # Panics
    /// Panics if `range.start > range.end` or `range.end` is past the end. The consumed frame
    /// is still released.
    pub fn slice(mut self, range: Range<usize>) -> Self {
        self.0.slice(range);
        self
    }
}

impl Clone for ImmutableFrame {
    fn clone(&self) -> Self {
        let rc = self.0.rc();
        // SAFETY: this thread exclusively accesses the count, and self keeps it alive.
        unsafe {
            let count = rc.as_ptr().read();
            assert!(count != u16::MAX, "frame reference count overflow");
            rc.as_ptr().write(count + 1);
            // Copy the handle only after acquiring its reference.
            Self(core::ptr::read(&self.0))
        }
    }
}

impl Deref for ImmutableFrame {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: the frame holds a reference to the valid payload.
        unsafe { core::slice::from_raw_parts(self.0.data.as_ptr(), self.0.len as usize) }
    }
}

impl fmt::Debug for ImmutableFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImmutableFrame").field("len", &self.0.len).finish()
    }
}

#[cfg(test)]
extern crate std;

#[cfg(test)]
mod tests {
    use super::*;
    use std::boxed::Box;
    use std::cell::Cell;

    // ANCHOR: producer
    #[repr(C)]
    struct Storage {
        refs: u16,
        bytes: [u8; 8],
    }

    #[derive(Default)]
    struct Producer {
        releases: Cell<usize>,
        released_count: Cell<u16>,
        storage: Cell<Option<NonNull<Storage>>>,
    }

    impl Producer {
        fn frame(&self) -> OwnedFrame {
            let storage = Box::into_raw(Box::new(Storage { refs: 1, bytes: *b"abcdefgh" }));
            self.storage.set(NonNull::new(storage));
            // SAFETY: both field pointers derive from the same live allocation.
            // The producer retains its raw pointer until the last release.
            unsafe {
                let rc = NonNull::new_unchecked(core::ptr::addr_of_mut!((*storage).refs));
                let data = NonNull::new_unchecked(core::ptr::addr_of_mut!((*storage).bytes).cast());
                OwnedFrame::from_raw_parts(data, 8, rc, release, (self as *const Self).cast_mut().cast())
            }
        }
    }

    unsafe extern "C" fn release(rc: NonNull<u16>, ctx: *mut ()) {
        // SAFETY: each test keeps its producer alive until all frames are dropped.
        let producer = unsafe { &*ctx.cast::<Producer>() };
        // SAFETY: the last frame still owns the storage during this callback.
        producer.released_count.set(unsafe { rc.as_ptr().read() });
        producer.releases.set(producer.releases.get() + 1);
        // SAFETY: this is the pointer returned by Box::into_raw, released once.
        drop(unsafe { Box::from_raw(producer.storage.take().unwrap().as_ptr()) });
    }
    // ANCHOR_END: producer

    // ANCHOR: clones
    #[test]
    fn clones_share_bytes_and_release_only_after_last_drop() {
        let producer = Producer::default();
        let mut owned = producer.frame();
        owned[0] = b'z';
        let frame = owned.freeze();
        let first = frame.clone();
        let last = frame.clone();
        assert_eq!(&*first, b"zbcdefgh");
        assert_eq!(frame.as_ptr(), first.as_ptr());
        drop(frame);
        assert_eq!(producer.releases.get(), 0);
        drop(first);
        assert_eq!(producer.releases.get(), 0);
        drop(last);
        assert_eq!(producer.releases.get(), 1);
        assert_eq!(producer.released_count.get(), 1);
    }
    // ANCHOR_END: clones

    #[test]
    fn try_own_requires_the_sole_reference() {
        let producer = Producer::default();
        let frame = producer.frame().freeze();
        let other = frame.clone();
        let frame = frame.try_own().expect_err("shared frame cannot be owned");
        drop(other);
        let mut owned = frame.try_own().expect("sole reference can be owned");
        owned[1] = b'z';
        assert_eq!(&*owned, b"azcdefgh");
        drop(owned);
        assert_eq!(producer.releases.get(), 1);
    }

    #[test]
    fn slices_share_the_original_count() {
        let producer = Producer::default();
        let frame = producer.frame().slice(2..8).freeze();
        let slice = frame.clone().slice(2..4);
        let clone = slice.clone();
        assert_eq!(&*frame, b"cdefgh");
        assert_eq!(&*slice, b"ef");
        assert_eq!(&*clone, b"ef");
        drop(slice);
        drop(frame);
        assert_eq!(producer.releases.get(), 0);
        drop(clone);
        assert_eq!(producer.releases.get(), 1);
    }

    #[test]
    fn clone_overflow_panics_without_acquiring_a_reference() {
        let producer = Producer::default();
        let frame = producer.frame().freeze();
        let clones: std::vec::Vec<_> = (1..u16::MAX).map(|_| frame.clone()).collect();
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            drop(frame.clone());
        })).is_err());
        drop(clones);
        assert_eq!(producer.releases.get(), 0);
        drop(frame);
        assert_eq!(producer.releases.get(), 1);
        assert_eq!(producer.released_count.get(), 1);
    }

    #[test]
    fn truncate_keeps_the_start() {
        let producer = Producer::default();
        let mut owned = producer.frame();
        let start = owned.as_ptr();
        owned.truncate(5);
        assert_eq!(&*owned, b"abcde");
        assert_eq!(owned.as_ptr(), start);
    }

    #[test]
    fn truncate_past_owned_length_panics() {
        let producer = Producer::default();
        let mut frame = producer.frame();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            frame.truncate(9);
        }));
        assert!(result.is_err());
        assert_eq!(frame.len(), 8);
    }

    #[test]
    fn raw_parts_transfer_references_without_releasing() {
        for immutable in [false, true] {
            let producer = Producer::default();
            let frame = producer.frame();
            let parts = if immutable { frame.freeze().into_raw_parts() } else { frame.into_raw_parts() };
            assert_eq!(producer.releases.get(), 0);
            // SAFETY: parts owns the sole live reference transferred by the frame.
            unsafe { (parts.release)(parts.rc, parts.ctx) };
            assert_eq!(producer.releases.get(), 1);
            assert_eq!(producer.released_count.get(), 1);
        }
    }

    #[test]
    fn raw_parts_transfer_preserves_shared_count() {
        let producer = Producer::default();
        let frame = producer.frame().freeze();
        let kept = frame.clone();
        let parts = frame.into_raw_parts();
        assert_eq!(producer.releases.get(), 0);
        // SAFETY: kept and parts each hold one live reference.
        assert_eq!(unsafe { parts.rc.as_ptr().read() }, 2);
        drop(kept);
        assert_eq!(producer.releases.get(), 0);
        // SAFETY: parts now holds the sole reference, released exactly once.
        unsafe { (parts.release)(parts.rc, parts.ctx) };
        assert_eq!(producer.releases.get(), 1);
        assert_eq!(producer.released_count.get(), 1);
    }

    #[test]
    fn out_of_range_slices_panic() {
        for (start, end) in [(0, 9), (5, 4), (9, 9)] {
            let range = start..end;
            let producer = Producer::default();
            assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                producer.frame().slice(range.clone());
            })).is_err());
            assert_eq!(producer.releases.get(), 1);
            assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                producer.frame().freeze().slice(range.clone());
            })).is_err());
            assert_eq!(producer.releases.get(), 2);
        }
    }
}
