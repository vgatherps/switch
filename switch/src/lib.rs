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
//! A poll accepts an equal or later tick, advances timers (expired sleeps wake in deadline
//! order), then polls ready tasks in rounds:
//! each round polls a snapshot of the woken tasks, and tasks woken or spawned during a
//! round run in the next one, until none are left or 1024 tasks were polled (checked
//! between rounds). [`StoredWaker::wake_deferred`] and [`yield_now`] instead move a task
//! to the next poll, unless an ordinary wake already queued it; it runs after the tasks
//! woken or spawned between the polls. [`request_turn`] from a task ends the poll after
//! that task; the next poll first finishes the stopped round.
//! Wake queues are plain vectors, growing as tasks queue rather than reserving per task.
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
//! allocate. Cloning and dropping work on any thread; a wake from another thread
//! goes through the runtime's mailbox, as a [`RemoteWaker`]'s does.
//! Runtime primitives that park tasks can keep a [`StoredWaker`] instead: a copyable
//! handle that wakes its task directly, without a call through the waker vtable.
//! Another thread wakes a task through a [`RemoteWaker`]: `Send + Sync` and
//! copyable, it queues the task in its runtime's mailbox, which the runtime
//! empties at the start of its next `poll` (on the task's own thread it wakes at
//! once). A wake after the runtime shut down is dropped.
//!
//! ## Tasks
//!
//! A task is one call of a function marked `#[switch::task]`: an `async fn`, or a
//! `fn` returning `impl Future<Output = T>`, with arguments bound to plain
//! identifiers. Calling it packages the arguments into a [`SpawnToken`];
//! `Spawner::spawn` builds the future in a slot of a 64 KiB task block.
//! The future and its value must be `'static`. `spawn` detaches the task: its
//! value is dropped when it completes. `Spawner::spawn_join` also returns a
//! [`JoinHandle`] that resolves to the value, or to [`oneshot::Canceled`] if the
//! task ends without one (its runtime shut down or dropped, or its poll
//! panicked); it allocates one [`oneshot`] channel. Both kinds of spawn share the function's
//! pool. Each task function, and each instantiation of a generic one, has its own
//! task blocks; a pool adds a block when every slot of its blocks is occupied, so
//! the number of live tasks has no limit.
//!
//! A runtime allocates its own task blocks and keeps them for its whole life;
//! blocks are never shared between runtimes. Shutdown drops the tasks, and a
//! dropped runtime's blocks are leaked by design, so a stale waker still points
//! at valid memory and waking it does nothing. Runtimes are meant to live as long
//! as the process.
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
//! Create one runtime, keep it for the life of the process, and poll it from the thread that created it. Memory for tasks is never returned, even after a runtime is dropped. `Runtime` and `Spawner` stay on their thread:
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
