use alloc::rc::Rc;
use core::cell::Cell;

#[derive(Debug)]
pub(crate) struct RxShared {
    pub(crate) held: Cell<usize>,
}

impl RxShared {
    /// Create an empty held-frame count.
    pub(crate) fn new() -> Rc<RxShared> {
        Rc::new(RxShared { held: Cell::new(0) })
    }

    /// Count one more UDP datagram, from the moment it is queued.
    #[inline]
    pub(crate) fn hold(&self) {
        self.held.set(self.held.get().checked_add(1).expect("held frame count overflow"));
    }

    /// Release one held frame.
    #[inline]
    pub(crate) fn release(&self) {
        self.held.set(self.held.get().checked_sub(1).expect("held frame count underflow"));
    }
}

/// Frames the app still holds; they keep their RX pool buffers until dropped.
#[derive(Debug)]
pub struct HeldFrames {
    shared: Rc<RxShared>,
}

impl HeldFrames {
    pub(crate) fn new(shared: Rc<RxShared>) -> Self {
        Self { shared }
    }

    pub fn count(&self) -> usize {
        self.shared.held.get()
    }
}
