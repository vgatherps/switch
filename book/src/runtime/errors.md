# Panics, errors and shutdown

The earlier pages of this chapter, up to [Tasks working together](cooperating.md), described the runtime when everything goes right. This page collects everything that can go wrong, what a panicking task does to the runtime, and how to shut it down. It ends with a short look at how the runtime stores tasks, which explains why a program should create one runtime and keep it.

## Error types

The runtime has four error types. Each one belongs to a particular call.

| Type | Returned by | Values and meaning |
|---|---|---|
| `RuntimeError` | `Runtime::poll` | `TimeRegressed { current, supplied }`: the tick was lower than the last accepted one; nothing happened. `Closed`: the runtime was shut down. `Panicked`: a task panicked earlier. |
| `SpawnError<T>` | `Spawner::spawn`, `Spawner::spawn_join` | `reason` is `Closed` or `Panicked`; `token` is the unspawned task, handed back. |
| `SleepError` | `Spawner::sleep`, `Spawner::sleep_until`, awaiting a `Sleep` | `Closed`, `Panicked`, `DeadlineOverflow`, `WrongRuntime`, `RegistrationExhausted`; see [Time and sleeping](time.md#errors). |
| `oneshot::Canceled` | awaiting a `JoinHandle` or a `oneshot::Receiver` | The task ended without a value, or the sender was dropped without sending. |

`TimeRegressed` is the only one a correct loop can recover from by simply carrying on: pass a valid tick next time. `Closed` and `Panicked` are permanent; once the runtime reports either, it will not run tasks again.

`Canceled` implements `Display` and `std::error::Error`. The other three implement only `Debug`, so print them with `{:?}`.

## When a task panics

A panic inside a task does not stay inside the task. It unwinds out of `Runtime::poll`, carrying the original panic payload, into the code that called `poll`. That task is ended and is never polled again. If it was spawned with `spawn_join`, its `JoinHandle` resolves to `Err(Canceled)` straight away.

From then on, the runtime is in a panicked state. It refuses new work:

- `poll` returns `Err(RuntimeError::Panicked)`,
- `spawn` and `spawn_join` return a `SpawnError` whose `reason` is `Panicked`,
- creating or awaiting a sleep returns `Err(SleepError::Panicked)`.

The other tasks are not dropped yet; they stay where they are until the runtime is shut down or dropped, both of which still work. In practice the panic unwinds through your drive loop, and the runtime is dropped along with the rest of the loop's state.

## Shutting down

`runtime.shutdown()` closes the runtime and drops every remaining task. Dropping the `Runtime` does the same. The order in which tasks are dropped is not specified, so tasks must not rely on one being dropped before another.

When a task is dropped, its future is dropped, and everything the future owns is dropped with it: sockets, frames, buffers, other handles. Afterwards:

- the `JoinHandle` of every task that had not finished resolves to `Err(Canceled)`,
- every `Spawner` reports `Closed`: `spawn` returns `SpawnError` with reason `Closed`, and `sleep` returns `Err(SleepError::Closed)`,
- every `Sleep` still held somewhere completes with `Err(SleepError::Closed)` when awaited,
- `poll` returns `Err(RuntimeError::Closed)`,
- a `RemoteWaker` wake from another thread is dropped.

A task's destructor may run code, and that code runs during shutdown. It must not drop the `Runtime` itself (for example by owning the last reference to a structure that holds it): dropping the runtime from a destructor that its own shutdown is running aborts the process.

## Shut the runtime down before closing the network

Tasks own sockets. A socket that lives inside a parked task's future stays open until that task is dropped. Closing the network, as [Shutting down cleanly](../net/shutdown.md) describes, requires every socket to be gone first, so the runtime has to be shut down before the network is closed.

The first network program does it in this order: `rt.shutdown()`, then `net.into_stack()`, then `try_close` ([Teardown](../getting-started/first-connection.md#teardown)). [Shutting down cleanly](../net/shutdown.md) gives the full order for each backend.

## How task storage works

Task storage grows as tasks are spawned, so the number of live tasks has no limit. Each task's future must fit in a 64 KiB block; a bigger one is rejected at compile time.

Memory for tasks is never returned, not even after the runtime is dropped. A program that creates and drops many runtimes grows its memory every time, so create one runtime at startup and keep it until the process ends.

This is the end of the runtime chapter. The next part puts it to work: [Sockets with switch-net](../net/index.md) gives tasks TCP and UDP sockets, and [The drive loop](../net/drive-loop.md) adds the network to the loop you have been writing.
