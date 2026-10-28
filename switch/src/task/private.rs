//! Items used by `#[switch::task]` expansions. Not a stable API.

use std::any::TypeId;
use std::marker::PhantomData;
use std::num::NonZeroU32;

use super::block::Shape;
use crate::executor::Executor;
use crate::task::SpawnToken;

/// Most occupied slots of one task function per runtime.
#[derive(Clone, Copy)]
pub struct Capacity(NonZeroU32);

impl Capacity {
    /// Panics unless `1 <= n < 2^25`; in a `const` block that is a compile error.
    pub const fn new(n: u32) -> Self {
        assert!(
            n >= 1 && n < 1 << 25,
            "task capacity must be at least 1 and below 2^25"
        );
        Self(NonZeroU32::new(n).unwrap())
    }
}

mod sealed {
    pub trait Sealed: Sized {
        /// Spawn into `executor`; gives the payload back when the function's
        /// capacity is used up.
        ///
        /// # Safety
        /// Owner thread; `executor` is live and open.
        unsafe fn spawn_in(self, executor: *mut crate::executor::Executor) -> Result<(), Self>;
    }
}

/// A spawnable task payload: one task function's packaged arguments.
pub trait Task: sealed::Sealed {}

/// # Safety
/// Owner thread; `executor` is live and open.
#[inline]
pub(crate) unsafe fn spawn_in<T: Task>(payload: T, executor: *mut Executor) -> Result<(), T> {
    unsafe { sealed::Sealed::spawn_in(payload, executor) }
}

struct Payload<K, C, Fut> {
    build: C,
    capacity: Option<Capacity>,
    _key: PhantomData<fn() -> (K, Fut)>,
}

impl<K, C, Fut> sealed::Sealed for Payload<K, C, Fut>
where
    K: 'static,
    C: FnOnce() -> Fut,
    Fut: Future<Output = ()> + 'static,
{
    #[inline]
    unsafe fn spawn_in(self, executor: *mut Executor) -> Result<(), Self> {
        let capacity = match self.capacity {
            Some(capacity) => capacity.0.get(),
            None => Shape::<Fut>::SLOTS as u32,
        };
        // The pool's key names `Fut`, and `Shape::<Fut>` is written into its blocks
        // here, in the same instantiation: a pool only ever holds one future type.
        let key = TypeId::of::<(K, Fut)>();
        unsafe {
            let Some(reservation) = Executor::reserve(executor, key, &Shape::<Fut>::SPEC, capacity)
            else {
                return Err(self);
            };
            Shape::<Fut>::future(reservation.header()).write((self.build)());
            reservation.commit();
            Ok(())
        }
    }
}

impl<K, C, Fut> Task for Payload<K, C, Fut>
where
    K: 'static,
    C: FnOnce() -> Fut,
    Fut: Future<Output = ()> + 'static,
{
}

/// Package a task function's call as a token; allocates nothing.
///
/// Spawns share a pool exactly when their `marker` type and future type are the
/// same, so tasks with different markers never share slots, and one marker used
/// with different future types gets one pool per future type. `#[switch::task]`
/// passes a closure, whose type is unique per function and instantiation.
/// `capacity` is the pool's most occupied slots per runtime (default: one 64 KiB
/// block's slots); the first spawn of a pool in a runtime sets it.
#[inline]
pub fn token<K: Copy + 'static, C: FnOnce() -> Fut, Fut: Future<Output = ()> + 'static>(
    _marker: K,
    capacity: Option<Capacity>,
    build: C,
) -> SpawnToken<impl Task> {
    let () = Shape::<Fut>::FITS;
    SpawnToken::new(Payload::<K, C, Fut> {
        build,
        capacity,
        _key: PhantomData,
    })
}
