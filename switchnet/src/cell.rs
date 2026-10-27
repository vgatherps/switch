//! The shared-stack cell. Access is closure-scoped so no borrow can outlive a
//! call, cross an await or run user code; that scoping is what lets this
//! become an `UnsafeCell` later without changing callers. Today a reentrant
//! access panics.

use std::cell::RefCell;

pub(crate) struct StackCell<T>(RefCell<T>);

impl<T> StackCell<T> {
    pub(crate) fn new(value: T) -> Self {
        Self(RefCell::new(value))
    }

    /// `body` must not run waker or other user code.
    #[inline]
    pub(crate) fn with<R>(&self, body: impl FnOnce(&mut T) -> R) -> R {
        body(&mut self.0.borrow_mut())
    }

    pub(crate) fn into_inner(self) -> T {
        self.0.into_inner()
    }
}
