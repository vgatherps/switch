//! Tasks: futures of marked task functions, stored in per-function slabs.
//!
//! A task is one call of a task function. Its future sits in a slot of a 64-slot
//! slab and never moves. A `Waker`'s data is the task header pointer.

pub(crate) mod header;
pub(crate) mod pool;
pub mod private;
pub(crate) mod slab;

#[cfg(test)]
mod coverage_tests;
#[cfg(test)]
mod tests;

use std::marker::PhantomData;

pub(crate) use header::TaskHeader;

/// A task function's packaged call, ready for [`crate::Spawner::spawn`].
///
/// Dropping an unspawned token drops only the arguments. Not `Send`, `Sync` or `Clone`.
#[must_use = "a task does nothing until spawned"]
pub struct SpawnToken<T> {
    payload: T,
    _local: PhantomData<*const ()>,
}

impl<T> SpawnToken<T> {
    pub(crate) fn new(payload: T) -> Self {
        Self {
            payload,
            _local: PhantomData,
        }
    }

    pub(crate) fn into_payload(self) -> T {
        self.payload
    }
}
