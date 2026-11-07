//! Single-threaded packet frames with producer-owned storage.
//!
//! Each frame is 32 bytes, aligned to eight bytes: a data pointer, a u32 length,
//! a u32 offset back to its u16 reference count, a release function, and context.
//! The count must precede the data in the same allocation; slicing advances the
//! data pointer and the offset together. The count is plain, not atomic: frames
//! and their storage management stay on one thread. Neither frame type is Send
//! or Sync.
//!
//! The last release calls the producer's function with its context. Both conduit
//! and junction produce frames, so this crate cannot name either producer or
//! choose how their storage is released.

#![no_std]

use core::fmt;
use core::mem::{align_of, size_of};
use core::ops::{Deref, DerefMut, Range};
use core::ptr::NonNull;

/// Called once, on the last release. `rc` points at the frame's refcount (still 1).
pub type Release = unsafe extern "C" fn(rc: NonNull<u16>, ctx: *mut ());

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

/// An exclusively owned, mutable packet frame.
#[repr(transparent)]
pub struct OwnedFrame(RawFrame);

impl OwnedFrame {
    /// Takes ownership of a producer's packet storage.
    ///
    /// # Safety
    /// `*rc == 1` and the count is owned by this frame. `rc` must be aligned for
    /// `u16`, and the whole count must precede the payload:
    /// `rc.as_ptr().cast::<u8>().add(2) <= data.as_ptr()`. Both pointers must be
    /// derived from the same allocation pointer, so subtracting the stored offset
    /// from `data` can reach the count, with `(data - rc) + len <= u32::MAX`.
    /// `data..data+len` is valid for reads and writes while the count is nonzero.
    /// Nothing else touches the count. `release(rc, ctx)` releases the storage
    /// and runs once, on this thread.
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

    /// Shares this frame without changing its reference count.
    pub fn freeze(self) -> ImmutableFrame {
        ImmutableFrame(self.0)
    }

    /// Narrows the payload, panicking if the range is out of bounds.
    pub fn slice(mut self, range: Range<usize>) -> Self {
        self.0.slice(range);
        self
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

/// A possibly shared, immutable packet frame.
#[repr(transparent)]
pub struct ImmutableFrame(RawFrame);

impl ImmutableFrame {
    /// Recovers exclusive ownership only if this is the sole reference.
    pub fn try_own(self) -> Result<OwnedFrame, Self> {
        // SAFETY: the frame keeps the count alive and only this thread accesses it.
        if unsafe { self.0.rc().as_ptr().read() } == 1 {
            Ok(OwnedFrame(self.0))
        } else {
            Err(self)
        }
    }

    /// Narrows the payload, panicking if the range is out of bounds.
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
