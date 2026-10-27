//! Per-slot task header: 8 bytes in front of every future.

use std::cell::Cell;
use std::ptr::addr_of;

use crate::intrusive::NIL;

/// Task can be polled; cleared at completion, poll panic, or shutdown.
pub(crate) const LIVE: u8 = 1;
/// One run-queue entry refers to this slot.
pub(crate) const QUEUED: u8 = 2;
/// The future's destructor has started; it never runs twice.
pub(crate) const DROP_STARTED: u8 = 4;

/// Written only on the owner thread. Every mutable field is a `Cell`, so shared
/// access never conflicts with a wake or timer registration during a poll.
#[repr(C)]
pub(crate) struct TaskHeader {
    flags: Cell<u8>,
    /// Slot index in its slab; written once when the slab is created.
    slot: u8,
    _pad: [u8; 2],
    /// Timer registration list head (`timer_registration.rs`).
    reg_head: Cell<u32>,
}

impl TaskHeader {
    pub(crate) const fn new(slot: u8) -> Self {
        Self {
            flags: Cell::new(0),
            slot,
            _pad: [0; 2],
            reg_head: Cell::new(NIL),
        }
    }

    /// # Safety
    /// `this` points to a header inside a live slab; only the owner thread calls.
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
    pub(crate) unsafe fn slot(this: *const Self) -> u8 {
        unsafe { *addr_of!((*this).slot) }
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
        assert_eq!(std::mem::offset_of!(TaskHeader, slot), 1);
        assert_eq!(std::mem::offset_of!(TaskHeader, reg_head), 4);
    }
}
