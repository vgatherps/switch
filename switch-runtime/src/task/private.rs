//! Items used by `#[switch_runtime::task]` expansions. Not a stable API.

use std::any::TypeId;
use std::marker::PhantomData;

use super::block::Shape;
use super::join::Joinable;
use crate::executor::Executor;
use crate::oneshot::Sender;
use crate::task::SpawnToken;

mod sealed {
    /// Only this crate's payloads are tasks.
    pub trait Sealed {}
}

/// A spawnable task payload: one task function's packaged arguments.
pub trait Task: sealed::Sealed + Sized {
    /// The task function's return value.
    type Output: 'static;

    /// Spawn into `executor`; `join` receives the task's value.
    ///
    /// # Safety
    /// Owner thread; `executor` is live and open.
    #[doc(hidden)]
    unsafe fn spawn_in(self, executor: *mut Executor, join: Option<Sender<Self::Output>>);
}

struct Payload<K, C, Fut> {
    build: C,
    _key: PhantomData<fn() -> (K, Fut)>,
}

impl<K, C, Fut> sealed::Sealed for Payload<K, C, Fut> {}

impl<K, C, Fut> Task for Payload<K, C, Fut>
where
    K: 'static,
    C: FnOnce() -> Fut,
    Fut: Future + 'static,
    Fut::Output: 'static,
{
    type Output = Fut::Output;

    #[inline]
    unsafe fn spawn_in(self, executor: *mut Executor, join: Option<Sender<Fut::Output>>) {
        // The pool's key names `Fut`, and `Shape::<Joinable<Fut>>` is written into
        // its blocks here, in the same instantiation: a pool only ever holds one
        // future type, whether spawned detached or joined.
        let key = TypeId::of::<(K, Fut)>();
        unsafe {
            let reservation = Executor::reserve(executor, key, &Shape::<Joinable<Fut>>::SPEC);
            let future = (self.build)();
            Shape::<Joinable<Fut>>::future(reservation.header()).write(Joinable::new(future, join));
            reservation.commit();
        }
    }
}

/// Package a task function's call as a token; allocates nothing.
///
/// Spawns share a pool exactly when their `marker` type and future type are the
/// same, so tasks with different markers never share slots, and one marker used
/// with different future types gets one pool per future type. `#[switch_runtime::task]`
/// passes a closure, whose type is unique per function and instantiation.
#[inline]
pub fn token<K, C, Fut, T>(_marker: K, build: C) -> SpawnToken<impl Task<Output = T>>
where
    K: Copy + 'static,
    C: FnOnce() -> Fut,
    Fut: Future<Output = T> + 'static,
    T: 'static,
{
    let () = Shape::<Joinable<Fut>>::FITS;
    SpawnToken::new(Payload::<K, C, Fut> {
        build,
        _key: PhantomData,
    })
}
