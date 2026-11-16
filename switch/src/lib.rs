//! # switch
//!
//! A caller-driven, single-threaded async runtime. Your program owns the loop: the runtime runs tasks only inside [`Runtime::poll`], never sleeps, never reads a clock and never starts a thread. It is the task runtime under `switch-net`, whose `round` receives packets, polls this runtime, then sends.
//!
//! ```rust
//! use switch::Runtime;
//!
//! #[switch::task]
//! async fn hello(name: &'static str) {
//!     println!("Hello, {name}!");
//! }
//!
//! let mut runtime = Runtime::new(100);
//! let spawner = runtime.spawner();
//! spawner.spawn(hello("switch")).unwrap();
//!
//! while runtime.poll(100).unwrap().live_tasks != 0 {}
//! ```
//!
//! A [`Runtime`] owns tasks and timers. A [`Spawner`], from [`Runtime::spawner`] and cheap to clone, starts tasks and creates [`Sleep`] futures; it cannot poll or shut the runtime down. A task is one call of a function marked `#[switch::task]`: calling the function packages its arguments into a [`SpawnToken`], and [`Spawner::spawn`] starts it. [`Spawner::spawn_join`] also returns a [`JoinHandle`] for the task's value.
//!
//! Each call to [`Runtime::poll`] passes the current tick, a `u64` that is the runtime's only clock. Ticks must never go backwards. Their unit is yours, but `switch_net::within` and other `Duration`-based waits need monotonic nanoseconds.
//!
//! Create one runtime, keep it for the life of the process, and poll it from the thread that created it. Memory for tasks is never returned, even after a runtime is dropped. A task's `Waker` may be cloned, dropped and woken on any thread; a wake from another thread takes effect at the runtime's next poll. `Runtime` and `Spawner` stay on their thread:
//!
//! ```compile_fail
//! fn require_send<T: Send>() {}
//! require_send::<switch::Runtime>();
//! ```
//!
//! Joining a task's value:
//!
//! ```rust
//! #[switch::task]
//! async fn double(value: u32) -> u32 {
//!     value * 2
//! }
//!
//! #[switch::task]
//! async fn report(handle: switch::JoinHandle<u32>) {
//!     assert_eq!(handle.await, Ok(42));
//! }
//!
//! let mut runtime = switch::Runtime::new(0);
//! let spawner = runtime.spawner();
//! let handle = spawner.spawn_join(double(21)).unwrap();
//! spawner.spawn(report(handle)).unwrap();
//! while runtime.poll(0).unwrap().live_tasks != 0 {}
//! ```
//!
//! What one poll does, in order, is documented on [`Runtime::poll`].
//!
//! Book: see the switch book, part "The switch runtime".

// `#[task]` expansions name the crate `::switch`, here too (`boxed`, tests).
extern crate self as switch;

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

/// A task whose future is heap-allocated: one allocation per spawn, and one dynamic call per
/// poll. For slow-path spawns only, where writing a task function is not practical; normal
/// code writes a `#[switch::task]` function, whose future lives in its pool's slot.
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
    //! A task future that does not fit a 64 KiB task block is a compile-time error:
    //!
    //! ```compile_fail,E0080
    //! #[switch::task]
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
    //! require_sync::<switch::Runtime>();
    //! ```
    //! ```compile_fail
    //! fn require_send<T: Send>() {}
    //! require_send::<switch::Spawner>();
    //! ```
    //! ```compile_fail
    //! fn require_sync<T: Sync>() {}
    //! require_sync::<switch::Spawner>();
    //! ```
    //! ```compile_fail
    //! fn require_send<T: Send>() {}
    //! require_send::<switch::Sleep>();
    //! ```
    //! ```compile_fail
    //! fn require_sync<T: Sync>() {}
    //! require_sync::<switch::Sleep>();
    //! ```
    //! ```compile_fail
    //! fn require_send<T: Send>() {}
    //! require_send::<switch::SpawnToken<()>>();
    //! ```
    //! ```compile_fail
    //! fn require_sync<T: Sync>() {}
    //! require_sync::<switch::SpawnToken<()>>();
    //! ```
    //! ```compile_fail
    //! fn cannot_drive(spawner: &switch::Spawner) {
    //!     spawner.poll(0);
    //! }
    //! ```
    //! ```compile_fail
    //! fn cannot_shutdown(spawner: &switch::Spawner) {
    //!     spawner.shutdown();
    //! }
    //! ```
}
