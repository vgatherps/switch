//! # switch
//!
//! A caller-driven, single-threaded async runtime.
//!
//! ## Runtime
//!
//! `Runtime` owns its tasks and timers. It is `!Send + !Sync`; cloned `Spawner`s submit
//! tasks and create `Sleep` futures for that same runtime. Nothing blocks or reads a clock
//! for you: each call to [`Runtime::poll`] supplies the current `u64` tick. Tick units are
//! caller-defined, and absolute deadlines may use the full `u64` range. A relative sleep
//! captures the latest accepted tick when it is created.
//!
//! A poll accepts an equal or later tick, advances timers, then polls one ready-task snapshot.
//! Work that wakes or spawns tasks must run before [`Runtime::poll`]; those tasks are polled by
//! that call. The runtime never waits, though future code can block. A regressed tick returns
//! [`RuntimeError::TimeRegressed`] without changing runtime state.
//! If a task panics, the original panic payload unwinds to the caller; the
//! runtime rejects new work and later turns, while [`Runtime::shutdown`] remains available
//! for cleanup.
//!
//! ## Waking
//!
//! A task's `Waker` points at its task header. Waking, cloning and dropping it do not
//! allocate, but only on the runtime's thread: each of them panics on another thread.
//! Runtime primitives that park tasks can keep a [`StoredWaker`] instead: a copyable
//! handle that wakes its task directly, without a call through the waker vtable.
//!
//! ## Example
//!
//! ```rust
//! use switch::Runtime;
//!
//! // Hand-written expansion of `#[switch::task] async fn hello() { println!("Hello!"); }`.
//! fn hello() -> switch::SpawnToken<impl switch::__private::Task> {
//!     async fn __hello_task() {
//!         println!("Hello!");
//!     }
//!     thread_local! {
//!         static POOL: switch::__private::Pool = const { switch::__private::Pool::new() };
//!     }
//!     struct Key;
//!     unsafe impl switch::__private::PoolKey for Key {
//!         fn pool() -> &'static std::thread::LocalKey<switch::__private::Pool> {
//!             &POOL
//!         }
//!     }
//!     // SAFETY: Key's pool is private to this function, which always produces one future type.
//!     unsafe { switch::__private::token::<Key, _, _>(move || __hello_task()) }
//! }
//!
//! let mut runtime = Runtime::new(100);
//! let spawner = runtime.spawner();
//! spawner.spawn(hello()).unwrap();
//!
//! while runtime.poll(100).unwrap().live_tasks != 0 {}
//! ```
//!
//! A task future that does not fit a 64 KiB task block is a compile-time error:
//! ```compile_fail
//! fn too_big() -> switch::SpawnToken<impl switch::__private::Task> {
//!     async fn __too_big() {
//!         let bulk = [0u8; 70_000];
//!         std::future::ready(()).await;
//!         std::hint::black_box(&bulk);
//!     }
//!     thread_local! {
//!         static POOL: switch::__private::Pool = const { switch::__private::Pool::new() };
//!     }
//!     struct Key;
//!     unsafe impl switch::__private::PoolKey for Key {
//!         fn pool() -> &'static std::thread::LocalKey<switch::__private::Pool> {
//!             &POOL
//!         }
//!     }
//!     unsafe { switch::__private::token::<Key, _, _>(move || __too_big()) }
//! }
//! drop(too_big());
//! ```
//!
//! ```compile_fail
//! fn require_send<T: Send>() {}
//! require_send::<switch::Runtime>();
//! ```
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

#[cfg(test)]
extern crate self as switch;

#[cfg(test)]
#[macro_use]
#[path = "../tests/support/task_macro.rs"]
mod task_macro;

mod executor;
pub(crate) mod intrusive;
mod runtime;
mod sleep;
pub(crate) mod task;
mod timer_registration;
mod timer_wheel;
mod waker;

pub use runtime::{Capacity, PollReport, Runtime, RuntimeError, SpawnError, Spawner};
pub use sleep::{Sleep, SleepError};
pub use task::SpawnToken;
pub use waker::StoredWaker;

#[doc(hidden)]
pub use task::private as __private;

#[cfg(test)]
mod extraction_smoke;
