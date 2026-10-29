//! Per-slot task header: 8 bytes in front of every future.

use std::cell::Cell;
use std::ptr::addr_of;

use super::block::{MASK, Meta};

/// Task can be polled; cleared at completion, poll panic, or shutdown.
pub(crate) const LIVE: u8 = 1;
/// One run-queue entry refers to this slot.
pub(crate) const QUEUED: u8 = 2;
/// The future's destructor has started; it never runs twice.
pub(crate) const DROP_STARTED: u8 = 16;
/// The slot holds a future (live, or awaiting drop or reclaim); set at commit,
/// cleared when the slot is freed.
pub(crate) const TAKEN: u8 = 8;
/// With QUEUED: the queue entry was put in `deferred` (`StoredWaker::wake_deferred`).
/// Cleared with QUEUED when the task's round takes the entry, or by an upgrade. Bit 2, next to LIVE and QUEUED:
/// the wake check's mask `LIVE | QUEUED | DEFERRED` (0b111) is then one AArch64
/// logical immediate.
pub(crate) const DEFERRED: u8 = 4;

/// Written only on the owner thread. Every mutable field is a `Cell`, so shared
/// access never conflicts with a wake or timer registration during a poll.
/// A new block is zeroed: no flags. Flags are 0 exactly when the slot is free
/// or reserved by a spawn in progress.
#[repr(C)]
pub(crate) struct TaskHeader {
    flags: Cell<u8>,
    _pad: [u8; 3],
    /// Timer registration list head (`timer_registration.rs`).
    reg_head: Cell<u32>,
}

impl TaskHeader {
    /// The block metadata of the slot `this` heads; no load.
    #[inline(always)]
    pub(crate) fn meta(this: *const Self) -> *mut Meta {
        this.cast_mut().map_addr(|addr| addr & MASK).cast()
    }

    /// # Safety
    /// `this` points to a header inside a block; only the owner thread calls.
    #[inline]
    pub(crate) unsafe fn flags<'a>(this: *const Self) -> &'a Cell<u8> {
        unsafe { &*addr_of!((*this).flags) }
    }

    /// # Safety
    /// As for [`Self::flags`].
    #[inline]
    pub(crate) unsafe fn reg_head<'a>(this: *const Self) -> &'a Cell<u32> {
        unsafe { &*addr_of!((*this).reg_head) }
    }

    /// # Safety
    /// As for [`Self::flags`].
    #[inline]
    pub(crate) unsafe fn is_live(this: *const Self) -> bool {
        unsafe { Self::flags(this).get() & LIVE != 0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_is_8_bytes_align_4_with_reg_head_at_4() {
        assert_eq!(std::mem::size_of::<TaskHeader>(), 8);
        assert_eq!(std::mem::align_of::<TaskHeader>(), 4);
        assert_eq!(std::mem::offset_of!(TaskHeader, flags), 0);
        assert_eq!(std::mem::offset_of!(TaskHeader, reg_head), 4);
    }
}
