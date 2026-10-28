# switch

A caller-driven, single-threaded async runtime. Nothing blocks or reads a clock: the caller supplies a `u64` tick on every poll, in units it chooses.

Depends on [`depot`](../depot/README.md) (`DynDepot` allocates task slots) and [`switch-macros`](../switch-macros/README.md) (the `#[switch::task]` attribute). `switchnet` depends on it. It needs no DPDK or Docker.

## API

- `Runtime`: owns tasks and timers, `!Send + !Sync`. `Runtime::new(tick)` or `with_capacity(tick, Capacity { tasks, timers })`. `poll(tick)` accepts an equal or later tick, advances timers, polls one snapshot of ready tasks and returns a `PollReport` (`tasks_polled`, `tasks_completed`, `timers_expired`, `ready_tasks`, `live_tasks`). A regressed tick returns `RuntimeError::TimeRegressed` and changes nothing. After a task panics the payload unwinds to the caller, the runtime rejects new work, and `shutdown()` stays available.
- `Spawner` (from `runtime.spawner()`): `spawn(SpawnToken<T>)` (detached: the task's return value is dropped when it completes), `spawn_join(SpawnToken<T>)` (also returns a `JoinHandle`; allocates one oneshot channel), `sleep(ticks)`, `sleep_until(...)`, returning `Sleep` futures. `SpawnError` carries the reason and gives the token back; `RuntimeError::Exhausted` means the task function's capacity of occupied slots in this runtime is used (one block's slots unless the function sets `capacity = N`). A completed task that woke itself keeps its slot until its queued wake drains on the next poll.
- `#[switch::task]` (from [`switch-macros`](../switch-macros/README.md)) marks a task function (`async fn`, or `fn` returning `impl Future<Output = T>`, with a `'static` value); calling it returns a `SpawnToken`, one call's arguments packaged for `spawn`. `#[switch::task(capacity = N)]` sets the function's capacity. Task functions may be generic.
- `JoinHandle<T>`: a future of `Result<T, oneshot::Canceled>`. It resolves to the task's value, or to `Canceled` when the task ends without a value (its runtime shut down or was dropped, or its poll panicked; a panic cancels the handle at once). Dropping it detaches the task. Detached and joined spawns of one task function share its pool and capacity.
- `oneshot::channel()`: single-thread channel for one value, one allocation. `Sender::send(self, value)` gives the value back if the receiver is gone; `Receiver` is a future of `Result<T, Canceled>`, `Canceled` once the sender is dropped without sending. Accepts any waker.
- `StoredWaker::new(cx)`: copyable handle that wakes its task directly; `wake(self)`.
- `RemoteWaker::new(cx)`: `Copy + Send + Sync` handle for waking from other threads. `wake(&self)` on the task's thread is the normal wake; on another thread it queues the task in its runtime's mailbox (a mutex-guarded list, never freed), and the runtime wakes it at the start of its next `poll`. After the runtime shut down the wake is dropped. A stale handle at most gives the task now in its slot one extra poll.

## Tasks

A task future lives in a slot of a 64 KiB, 64 KiB-aligned block that holds slots of one task function, and never moves. Each instantiation of a generic task function has its own blocks and capacity. A future that does not fit a block is a compile-time error. A runtime allocates its own blocks and keeps them for its whole life; blocks are never shared between runtimes or threads. A dropped runtime's blocks are leaked by design (runtimes are meant to live as long as the process), so a stale waker still points at valid memory and waking it does nothing. Shutdown drops the remaining tasks in an unspecified order, and finishes even if a task's destructor panics. A task's `Waker` points at its header; waking, cloning and dropping it do not allocate, but panic on any thread other than the runtime's. This relies on native thread-locals (Linux glibc and macOS).

Work that wakes or spawns tasks must run before `poll`, which polls them. The timer wheel has 13 levels of 32 slots by default.

## Build and test

```sh
cargo test -p switch
cargo bench -p switch    # executor, timer_wheel, runtime (Criterion)
cargo run -p switch --example manual_loop
```

Examples are `basic` and `manual_loop`. `plans/` and `provenance/` hold the design notes and measurement records.
