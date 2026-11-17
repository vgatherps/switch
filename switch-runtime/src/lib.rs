//! # switch-runtime
//!
//! The caller-driven, single-threaded async runtime of switch. Tasks run only inside
//! [`Runtime::poll`], which the program calls with the current tick; the runtime never
//! sleeps, reads a clock or starts a thread. Applications reach these items at the root of
//! the `switch` crate (`switch::Runtime`, `switch::task`); the network crates of the
//! workspace depend on this crate directly.

// `#[task]` expansions name the crate `::switch_runtime`, here too (`boxed`, tests).
extern crate self as switch_runtime;

mod executor;
pub(crate) mod intrusive;
pub mod oneshot;
mod remote;
mod runtime;
mod sleep;
pub(crate) mod task;
mod timer_registration;
mod timer_wheel;
mod waker;

/// Turns a boxed future into a [`SpawnToken`], for spawns where writing a
/// [`#[task]`](../switch/attr.task.html) function is not practical. It costs one heap allocation per
/// spawn and one more dynamic call per poll than a task function, whose future is stored in
/// place. [`Spawner::spawn_dynamic`] spawns a boxed future of any output type and returns its
/// `JoinHandle`.
#[task]
pub fn boxed(future: std::pin::Pin<Box<dyn Future<Output = ()>>>) -> impl Future<Output = ()> {
    future
}

pub use remote::RemoteWaker;
pub use executor::request_turn;
pub use runtime::{PollReport, Runtime, RuntimeError, SpawnError, Spawner};
pub use sleep::{Sleep, SleepError};
pub use switch_macros::task;
pub use task::SpawnToken;
pub use task::join::JoinHandle;
pub use waker::{StoredWaker, YieldNow, yield_now};

#[doc(hidden)]
pub use task::private as __private;

#[cfg(doctest)]
mod api_guards {
    //! Spawning, running and joining:
    //!
    //! ```rust
    //! #[switch_runtime::task]
    //! async fn double(value: u32) -> u32 {
    //!     value * 2
    //! }
    //!
    //! #[switch_runtime::task]
    //! async fn report(handle: switch_runtime::JoinHandle<u32>) {
    //!     assert_eq!(handle.await, Ok(42));
    //! }
    //!
    //! let mut runtime = switch_runtime::Runtime::new(0);
    //! let spawner = runtime.spawner();
    //! let handle = spawner.spawn_join(double(21)).unwrap();
    //! spawner.spawn(report(handle)).unwrap();
    //! while runtime.poll(0).unwrap().live_tasks != 0 {}
    //! ```
    //!
    //! ```compile_fail
    //! fn require_send<T: Send>() {}
    //! require_send::<switch_runtime::Runtime>();
    //! ```
    //! ```compile_fail
    //! fn require_send<T: Send>() {}
    //! require_send::<switch_runtime::JoinHandle<u8>>();
    //! ```
    //! ```compile_fail
    //! fn require_send<T: Send>() {}
    //! require_send::<switch_runtime::StoredWaker>();
    //! ```
    //! ```compile_fail
    //! fn require_sync<T: Sync>() {}
    //! require_sync::<switch_runtime::StoredWaker>();
    //! ```
    //!
    //! A task future that does not fit a 64 KiB task block is a compile-time error:
    //!
    //! ```compile_fail,E0080
    //! #[switch_runtime::task]
    //! async fn too_big() {
    //!     let bulk = [0u8; 70_000];
    //!     std::future::ready(()).await;
    //!     std::hint::black_box(&bulk);
    //! }
    //! drop(too_big());
    //! ```
    //!
    //! ```compile_fail
    //! fn require_sync<T: Sync>() {}
    //! require_sync::<switch_runtime::Runtime>();
    //! ```
    //! ```compile_fail
    //! fn require_send<T: Send>() {}
    //! require_send::<switch_runtime::Spawner>();
    //! ```
    //! ```compile_fail
    //! fn require_sync<T: Sync>() {}
    //! require_sync::<switch_runtime::Spawner>();
    //! ```
    //! ```compile_fail
    //! fn require_send<T: Send>() {}
    //! require_send::<switch_runtime::Sleep>();
    //! ```
    //! ```compile_fail
    //! fn require_sync<T: Sync>() {}
    //! require_sync::<switch_runtime::Sleep>();
    //! ```
    //! ```compile_fail
    //! fn require_send<T: Send>() {}
    //! require_send::<switch_runtime::SpawnToken<()>>();
    //! ```
    //! ```compile_fail
    //! fn require_sync<T: Sync>() {}
    //! require_sync::<switch_runtime::SpawnToken<()>>();
    //! ```
    //! ```compile_fail
    //! fn cannot_drive(spawner: &switch_runtime::Spawner) {
    //!     spawner.poll(0);
    //! }
    //! ```
    //! ```compile_fail
    //! fn cannot_shutdown(spawner: &switch_runtime::Spawner) {
    //!     spawner.shutdown();
    //! }
    //! ```
}
