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
//! Use `wake_by_ref()` or `current_task(cx).wake()` for self-waking. Cloning an ordinary
//! `Waker` panics, so futures that require `Waker::clone()` are unsupported. Runtime
//! primitives that retain a wake may use [`StoredWaker`], whose construction is unsafe.
//!
//! ## Example
//!
//! ```rust
//! use switch::Runtime;
//!
//! let mut runtime = Runtime::new(100);
//! let spawner = runtime.spawner();
//! spawner.spawn(async {
//!     println!("Hello!");
//! }).unwrap();
//!
//! while runtime.poll(100).unwrap().live_tasks != 0 {}
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
//! fn cannot_drive(spawner: &switch::Spawner) {
//!     spawner.poll(0);
//! }
//! ```
//! ```compile_fail
//! fn cannot_shutdown(spawner: &switch::Spawner) {
//!     spawner.shutdown();
//! }
//! ```

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
pub use waker::{StoredWaker, ThinWaker, current_task};

#[cfg(test)]
mod extraction_smoke;
