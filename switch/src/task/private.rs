//! Items used by `#[switch::task]` expansions. Not a stable API.

use std::marker::PhantomData;
use std::task::RawWakerVTable;

use super::slab::Shape;
use crate::executor::{self, Executor};
use crate::task::SpawnToken;

pub use super::pool::Pool;

/// Names the future type of a task function from its function item.
pub trait TaskFn<Args>: Copy {
    type Fut: Future<Output = ()> + 'static;
}

macro_rules! task_fn_impl {
    ($($arg:ident),*) => {
        impl<F, Fut, $($arg,)*> TaskFn<($($arg,)*)> for F
        where
            F: Copy + FnOnce($($arg,)*) -> Fut,
            Fut: Future<Output = ()> + 'static,
        {
            type Fut = Fut;
        }
    };
}

task_fn_impl!();
task_fn_impl!(A0);
task_fn_impl!(A0, A1);
task_fn_impl!(A0, A1, A2);
task_fn_impl!(A0, A1, A2, A3);
task_fn_impl!(A0, A1, A2, A3, A4);
task_fn_impl!(A0, A1, A2, A3, A4, A5);
task_fn_impl!(A0, A1, A2, A3, A4, A5, A6);
task_fn_impl!(A0, A1, A2, A3, A4, A5, A6, A7);
task_fn_impl!(A0, A1, A2, A3, A4, A5, A6, A7, A8);
task_fn_impl!(A0, A1, A2, A3, A4, A5, A6, A7, A8, A9);
task_fn_impl!(A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10);
task_fn_impl!(A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11);
task_fn_impl!(A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12);
task_fn_impl!(A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13);
task_fn_impl!(
    A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14
);
task_fn_impl!(
    A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15
);

/// Identifies one task function: its thread-local pool and its waker vtable.
///
/// # Safety
/// `pool()` is private to one task function that always produces one future
/// type, and `waker()` is that function's [`waker_vtable`].
pub unsafe trait PoolKey: 'static {
    fn pool() -> &'static std::thread::LocalKey<Pool>;
    fn waker() -> &'static RawWakerVTable;
}

/// The waker vtable of task function `f` (data = task header pointer).
pub const fn waker_vtable<K: PoolKey, F: TaskFn<Args>, Args>(_f: F) -> RawWakerVTable {
    RawWakerVTable::new(
        crate::waker::clone::<K, F::Fut>,
        crate::waker::wake::<F::Fut>,
        crate::waker::wake_by_ref::<F::Fut>,
        crate::waker::drop::<F::Fut>,
    )
}

mod sealed {
    pub trait Sealed {
        /// # Safety
        /// Owner thread; `executor` is live and open.
        unsafe fn spawn_in(self, executor: *mut crate::executor::Executor);
    }
}

/// A spawnable task payload: one task function's packaged arguments.
pub trait Task: sealed::Sealed {}

/// # Safety
/// Owner thread; `executor` is live and open.
#[inline]
pub(crate) unsafe fn spawn_in<T: Task>(payload: T, executor: *mut Executor) {
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
    unsafe fn spawn_in(self, executor: *mut Executor) {
        unsafe {
            let pool = K::pool().with(Pool::get);
            let spec = Shape::<Fut>::spec(executor::run::<K, Fut>, executor::drop_future::<Fut>);
            let reservation = Executor::reserve(executor, pool, &spec);
            let slot = Shape::<Fut>::slot(reservation.meta(), reservation.slot());
            Shape::<Fut>::future(slot).write((self.build)());
            reservation.commit();
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
    SpawnToken::new(Payload::<K, C, Fut> {
        build: c,
        _key: PhantomData,
    })
}
