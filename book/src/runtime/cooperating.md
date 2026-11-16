# Tasks working together, and other threads

Tasks on one runtime share a thread, so they cooperate rather than compete: a task runs until it awaits, and then another gets the thread. [Time and sleeping](time.md) showed one thing a task can wait for. This page covers the tools tasks use with each other (yielding, oneshot channels and join handles), the one tool for waking a task from another thread, and the rules about threads that the runtime enforces.

## Yielding

`switch::yield_now().await` gives the thread back without waiting for anything. The task stays ready, but it runs again in the *next* poll of the runtime, not in a later pass of the current one.

That is deliberate. Between the current poll and the next one, your loop gets control back and can do its other work, such as moving packets. A task that yields therefore lets the network make progress, not only the other tasks. [Driving the runtime](driving.md#fairness-no-task-is-interrupted) shows a task that splits long work into slices this way.

If something else wakes the yielded task during the same poll (a oneshot send, a socket becoming ready), that wake takes priority and the task runs again in the current poll's next pass.

`yield_now` only works inside a switch task. Polled anywhere else, it panics.

## Oneshot channels

A oneshot channel carries exactly one value from one place to one task. `switch::oneshot::channel()` returns a `Sender<T>` and a `Receiver<T>`:

```rust,ignore
{{#include ../../../switch/examples/oneshot.rs:tasks}}
```

```rust,ignore
{{#include ../../../switch/examples/oneshot.rs:run}}
```

The receiver is a future that resolves to `Result<T, Canceled>`. It resolves to `Ok(value)` once the value is sent; sending wakes the receiving task. If the sender is dropped without sending, the receiver resolves to `Err(Canceled)`.

`send` takes the sender by value and returns `Result<(), T>`. If the receiver has already been dropped, nobody will ever read the value, so `send` hands it back as `Err(value)`.

The sender can be used from anywhere on the runtime's thread: from a task, or from the loop itself between polls. A common use of the second kind is a stop signal. `switch-binance-feed`'s loop holds a `Sender<()>` and sends on it when the program should stop; the feed task races its work against the receiver (see [Timeouts and racing futures](../net/timeouts.md#racing-a-receive-against-a-stop-signal)).

The receiver must be awaited by a switch task, because it parks that task while it waits. Polled anywhere else while the sender still exists, it panics. Once the value was sent or the sender dropped, the outcome is settled and any poller gets it.

A channel costs one heap allocation. Neither half is `Send` or `Sync`; a oneshot channel connects tasks on one runtime, not threads.

## Waiting on another task

A `JoinHandle`, returned by `spawner.spawn_join`, is a oneshot receiver for a task's return value. It resolves to `Ok(value)` when the task returns, or `Err(Canceled)` if the task ended without returning (the runtime shut down or the task panicked). The same rules apply: await it from a switch task, and dropping it detaches the task without stopping it. [Writing and spawning tasks](tasks.md#detached-tasks-and-joined-tasks) has an example.

## For library authors: StoredWaker

The futures in this book's libraries (sockets in switch-net, oneshot channels, join handles) park a task by keeping a `switch::StoredWaker`, not a clone of the standard `Waker`. You need it only if you write such a future yourself.

`StoredWaker::new(cx)` takes the `Context` your future's `poll` receives and returns a small `Copy` handle to the task that is polling. Later, `wake()` makes that task ready, and `wake_deferred()` makes it ready for the next poll instead, the way `yield_now` does.

`StoredWaker::new` panics if the `Context` does not come from a switch task. Like the tasks themselves, a `StoredWaker` is neither `Send` nor `Sync`.

A stored waker may outlive the task it points to. Waking it is still safe. If no task has taken the old task's place in the runtime's storage, the wake does nothing; if a newer task has ([How task storage works](errors.md#how-task-storage-works) explains the reuse), that task gets at most one extra poll.

## Waking a task from another thread

Sometimes a task waits for something another thread produces: a value computed off the network CPU, or a signal from a control thread. The only way to wake a switch task from another thread is a `switch::RemoteWaker`.

`RemoteWaker::new(cx)`, like `StoredWaker::new`, takes the `Context` of a running switch task. The resulting handle is `Copy`, `Send` and `Sync`, so it can go to any thread. To reach the `Context` from inside an `async fn`, wrap the waiting step in `std::future::poll_fn`:

```rust,ignore
{{#include ../../../switch/examples/remote_wake.rs:task}}
```

The task publishes its waker each time it is polled and parks until the shared value is set. In `main`, the first poll runs the task once, so its waker exists. Then another thread sets the value and wakes the task:

```rust,ignore
{{#include ../../../switch/examples/remote_wake.rs:thread}}
```

The loop keeps polling until the task has finished:

```rust,ignore
{{#include ../../../switch/examples/remote_wake.rs:loop}}
```

A wake from another thread does not run anything on that thread, and it does not interrupt the loop. It puts the task into the runtime's *mailbox*, and the runtime empties the mailbox at the start of its next poll:

```text
 other thread                    loop thread
 ------------                    -----------
                                 poll(t1)   task parks
 value.store(7)
 waker.wake()  --> mailbox
                                 (loop sleeping or doing other work)
                                 poll(t2)   empty mailbox -> task ready
                                            task runs, sees 7, finishes
```

So the delay before the task runs is however long your loop takes to reach its next poll. A busy-polling loop picks the wake up on its next pass; a loop that sleeps when idle picks it up after its sleep.

On the task's own thread, `RemoteWaker::wake` is an ordinary wake. After the runtime has shut down, a wake from any thread is dropped. As with `StoredWaker`, a stale handle at most gives whichever task took the old task's place one extra poll.

## The thread rules

The runtime checks the thread rules at run time where the compiler cannot.

Each task's standard `Waker`, the one in its `Context`, belongs to the runtime's thread. Waking it, cloning it or dropping it on another thread panics with "waker used on foreign thread". This matters when you combine switch tasks with code written for other runtimes: a future that sends its waker to a background thread, as many channel and I/O libraries do, will panic. Futures that keep their waker on the same thread work. Use `RemoteWaker` for anything that crosses threads.

The runtime's types themselves (`Runtime`, `Spawner`, `Sleep`, `SpawnToken`, `JoinHandle`, `StoredWaker`, and both oneshot halves) are not `Send`, so the compiler keeps them on their thread.

Over the life of a process, at most 65,535 distinct threads can spawn switch tasks; the next thread that tries panics. A program that starts short-lived threads, each spawning tasks, counts every one of them against this limit.

Choosing a CPU for the loop's thread, and keeping other work off it, is up to the program. The runtime does not pin threads. For network programs, [The pinned CPU and helper threads](../nic/cpus.md) explains the setup, and [The drive loop](../net/drive-loop.md) shows the loop that runs there.

Next: [Panics, errors and shutdown](errors.md).
