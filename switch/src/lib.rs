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
//! ## Tasks
//!
//! A task is one call of a function marked `#[switch::task]`: an `async fn`, or a
//! `fn` returning `impl Future<Output = ()>`, with `'static` arguments bound to plain
//! identifiers and no generics. Calling it packages the arguments into a
//! [`SpawnToken`]; `Spawner::spawn` builds the future in a slot of a 64 KiB task block.
//! `#[switch::task(capacity = N)]` sets the most occupied slots of that function per
//! runtime (default: one block's slots, `1 <= N < 2^25`).
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
//! A task future that does not fit a 64 KiB task block is a compile-time error:
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
pub use switch_macros::task;
pub use task::SpawnToken;
pub use waker::StoredWaker;

#[doc(hidden)]
pub use task::private as __private;

#[cfg(test)]
mod extraction_smoke;
