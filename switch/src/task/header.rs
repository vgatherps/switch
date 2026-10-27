//! Task header and independently tracked allocation/future lifetime.

use std::task::{Context, Poll};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TaskState {
    Idle = 0,
    Scheduled = 1,
    Terminal = 3,
}

#[repr(C)]
pub struct TaskHeader {
    pub(crate) executor: *mut (),
    pub(super) poll_fn: unsafe fn(*mut TaskHeader, &mut Context<'_>) -> Poll<()>,
    pub(super) drop_fn: unsafe fn(*mut TaskHeader),
    pub(super) free_fn: unsafe fn(*mut TaskHeader),
    pub(crate) prev: *mut TaskHeader,
    pub(crate) next: *mut TaskHeader,
    pub(crate) state: TaskState,
    pub(crate) queued: bool,
    pub(crate) drop_started: bool,
    pub(crate) reg_head: u32,
}

impl TaskHeader {
    /// Caller owns a live initialized future, with no overlapping poll/drop.
    #[inline]
    pub(crate) unsafe fn poll(this: *mut Self, cx: &mut Context<'_>) -> Poll<()> {
        unsafe { ((*this).poll_fn)(this, cx) }
    }

    /// Mark before arbitrary user Drop; a panicking destructor must never repeat.
    #[inline]
    pub(crate) unsafe fn drop_future(this: *mut Self) {
        unsafe {
            if !(*this).drop_started {
                (*this).drop_started = true;
                ((*this).drop_fn)(this);
            }
        }
    }

    /// Caller has discharged all claims and finished (or unwound) future Drop.
    #[inline]
    pub(crate) unsafe fn free(this: *mut Self) {
        unsafe { ((*this).free_fn)(this) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_layout_and_state() {
        assert_eq!(std::mem::size_of::<TaskHeader>(), 56);
        assert_eq!(std::mem::align_of::<TaskHeader>(), 8);
        assert_eq!(std::mem::offset_of!(TaskHeader, reg_head), 52);
        assert_eq!(TaskState::Terminal as u8, 3);
    }
}
