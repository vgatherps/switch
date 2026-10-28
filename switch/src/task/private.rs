//! Items used by `#[switch::task]` expansions. Not a stable API.

use std::marker::PhantomData;

use super::block::Shape;
use crate::executor::Executor;
use crate::task::SpawnToken;

pub use super::pool::Pool;

/// Identifies one task function: its thread-local pool and its capacity.
///
/// # Safety
/// `pool()` is private to one task function that always produces one future type.
pub unsafe trait PoolKey: 'static {
    fn pool() -> &'static std::thread::LocalKey<Pool>;
    /// Most live tasks of this function per runtime; `None` is one block's slots.
    const CAPACITY: Option<u32> = None;
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
    _key: PhantomData<fn() -> (K, Fut)>,
}

impl<K, C, Fut> sealed::Sealed for Payload<K, C, Fut>
where
    K: PoolKey,
    C: FnOnce() -> Fut,
    Fut: Future<Output = ()> + 'static,
{
    #[inline]
    unsafe fn spawn_in(self, executor: *mut Executor) -> Result<(), Self> {
        let capacity = match K::CAPACITY {
            Some(capacity) => capacity,
            None => Shape::<Fut>::SLOTS as u32,
        };
        unsafe {
            let pool = K::pool().with(Pool::get);
            let Some(reservation) =
                Executor::reserve(executor, pool, &Shape::<Fut>::SPEC, capacity)
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
    K: PoolKey,
    C: FnOnce() -> Fut,
    Fut: Future<Output = ()> + 'static,
{
}

/// Package a task function's call as a token; allocates nothing.
///
/// # Safety
/// `K` belongs to the task function producing `Fut` (see [`PoolKey`]).
#[inline]
pub unsafe fn token<K: PoolKey, C: FnOnce() -> Fut, Fut: Future<Output = ()> + 'static>(
    c: C,
) -> SpawnToken<impl Task> {
    let () = Shape::<Fut>::FITS;
    SpawnToken::new(Payload::<K, C, Fut> {
        build: c,
        _key: PhantomData,
    })
}
