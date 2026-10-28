# switch

A caller-driven, single-threaded async runtime. Nothing blocks or reads a clock: the caller supplies a `u64` tick on every poll, in units it chooses.

Depends on [`depot`](../depot/README.md) (`DynDepot` allocates task slots). `switchnet` depends on it. It needs no DPDK or Docker.

## API

- `Runtime`: owns tasks and timers, `!Send + !Sync`. `Runtime::new(tick)` or `with_capacity(tick, Capacity { tasks, timers })`. `poll(tick)` accepts an equal or later tick, advances timers, polls one snapshot of ready tasks and returns a `PollReport` (`tasks_polled`, `tasks_completed`, `timers_expired`, `ready_tasks`, `live_tasks`). A regressed tick returns `RuntimeError::TimeRegressed` and changes nothing. After a task panics the payload unwinds to the caller, the runtime rejects new work, and `shutdown()` stays available.
- `Spawner` (from `runtime.spawner()`): `spawn(SpawnToken<T>)`, `sleep(ticks)`, `sleep_until(...)`, returning `Sleep` futures. `SpawnError` carries the reason and gives the token back; `RuntimeError::Exhausted` means the task function is at its capacity of live tasks.
- `SpawnToken`: one call of a task function, packaged by the `#[switch::task]` expansion, whose support items are in the hidden `__private` module. The proc macro is not in the workspace yet; the crate doc and `tests/support/task_macro.rs` show the expansion by hand.
- `StoredWaker::new(cx)`: copyable handle that wakes its task directly; `wake(self)`.

## Tasks

A task future lives in a slot of a 64 KiB, 64 KiB-aligned block that holds slots of one task function, and never moves. A future that does not fit a block is a compile-time error. Blocks are never freed and never change thread. A task's `Waker` points at its header; waking, cloning and dropping it do not allocate, but panic on any thread other than the runtime's. This relies on native thread-locals (Linux glibc and macOS).

Work that wakes or spawns tasks must run before `poll`, which polls them. The timer wheel has 13 levels of 32 slots by default.

## Build and test

```sh
cargo test -p switch
cargo bench -p switch    # executor, timer_wheel, runtime (Criterion)
cargo run -p switch --example manual_loop
```

Examples are `basic` and `manual_loop`. `plans/` and `provenance/` hold the design notes and measurement records.
