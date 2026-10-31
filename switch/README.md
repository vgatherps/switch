# switch

A caller-driven, single-threaded async runtime. Nothing blocks or reads a clock: the caller supplies a `u64` tick on every poll, in units it chooses.

Depends on [`depot`](../depot/README.md) (`DynDepot` hands out task slots) and [`switch-macros`](../switch-macros/README.md) (the `#[switch::task]` attribute). `switchnet` depends on it. It needs no DPDK or Docker.

## API

- `Runtime`: owns tasks and timers, `!Send + !Sync`. `Runtime::new(tick)` or `with_capacity(tick, Capacity { tasks, timers })`. `poll(tick)` accepts an equal or later tick, advances timers, polls ready tasks in rounds (the first: tasks woken or spawned before the call, then tasks deferred in the last poll; tasks woken or spawned during a round run in the next one) until none are left or 1024 polls were made, checked between rounds, and returns a `PollReport` (`tasks_polled`, `live_tasks`). A regressed tick returns `RuntimeError::TimeRegressed` and changes nothing. `switch::request_turn()`, called while a task is polled (switchnet's `Net` calls it once a TX burst of frames waits), ends that `poll` after the task; the next `poll` first finishes the round. It lasts that one poll and does nothing outside a `poll`. After a task panics the payload unwinds to the caller, the runtime rejects new work, and `shutdown()` stays available.
- `Spawner` (from `runtime.spawner()`): `spawn(SpawnToken<T>)` (detached: the task's return value is dropped when it completes), `spawn_join(SpawnToken<T>)` (also returns a `JoinHandle`; allocates one oneshot channel), `sleep(ticks)`, `sleep_until(...)`, returning `Sleep` futures. `SpawnError` carries the reason (a closed or panicked runtime) and gives the token back. A task function has no slot limit: its pool adds a 64 KiB block when every slot of its blocks is occupied. A completed task that woke itself keeps its slot until its queued wake drains: later in the same poll for an ordinary wake, or in the next poll for `wake_deferred` or when the 1024-poll limit stopped the poll.
- `#[switch::task]` (from [`switch-macros`](../switch-macros/README.md)) marks a task function (`async fn`, or `fn` returning `impl Future<Output = T>`, with a `'static` value); calling it returns a `SpawnToken`, one call's arguments packaged for `spawn`. It takes no arguments. Task functions may be generic.
- `JoinHandle<T>`: a future of `Result<T, oneshot::Canceled>`. It resolves to the task's value, or to `Canceled` when the task ends without a value (its runtime shut down or was dropped, or its poll panicked; a panic cancels the handle at once). Dropping it detaches the task. Detached and joined spawns of one task function share its pool.
- `oneshot::channel()`: single-threaded channel for one value, one allocation, received by a switch task; neither half is `Send` or `Sync`. `Sender::send(self, value)` wakes the receiving task through its `StoredWaker` and gives the value back if the receiver is gone. `Receiver` is a future of `Result<T, Canceled>`, `Canceled` once the sender is dropped without sending; polling it outside a switch task panics while the sender is alive (a finished channel is `Ready` for any poller).
- `StoredWaker::new(cx)`: copyable handle that wakes its task directly; `wake(self)`, or `wake_deferred(&self)` to run the task on the next `poll` instead of a later round of this one, unless an ordinary wake already queued it. An ordinary wake of a deferred task upgrades it: it runs in this `poll`'s next round.
- `yield_now()`: the first poll calls `wake_deferred` on the current task and returns `Pending`; the second returns `Ready`. Panics outside a switch task.
- `RemoteWaker::new(cx)`: `Copy + Send + Sync` handle for waking from other threads. `wake(&self)` on the task's thread is the normal wake; on another thread it queues the task in its runtime's mailbox (a mutex-guarded list, never freed), and the runtime wakes it at the start of its next `poll`. After the runtime shut down the wake is dropped. A stale handle at most gives the task now in its slot one extra poll.

## Tasks

A task future lives in a slot of a 64 KiB, 64 KiB-aligned block that holds slots of one task function, and never moves. Each instantiation of a generic task function has its own blocks. A pool hands out its lowest free slot, so it adds a block only once every slot of its blocks is occupied, and never frees one. A future that does not fit a block is a compile-time error. A runtime allocates its own blocks and keeps them for its whole life; blocks are never shared between runtimes or threads. A dropped runtime's blocks are leaked by design (runtimes are meant to live as long as the process), so a stale waker still points at valid memory and waking it does nothing. Shutdown drops the remaining tasks in an unspecified order, and finishes even if a task's destructor panics. A task's `Waker` points at its header; waking, cloning and dropping it do not allocate, but panic on any thread other than the runtime's. This relies on native thread-locals (Linux glibc and macOS).

Work that wakes or spawns tasks must run before `poll`, which polls them. The timer wheel has 13 levels of 32 slots by default.

## Build and test

```sh
cargo test -p switch
cargo bench -p switch    # executor, timer_wheel, runtime (Criterion)
cargo run -p switch --example manual_loop
```

Examples are `basic` and `manual_loop`. `plans/` and `provenance/` hold the design notes and measurement records.
