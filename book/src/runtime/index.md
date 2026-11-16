# The switch runtime

The switch runtime is the async executor in the `switch` crate. It holds your tasks and timers and runs them when you ask. If you followed [Your first task](../getting-started/first-task.md) and [Your first network program](../getting-started/first-connection.md), you have already used all of it that most programs need: a `Runtime`, a `Spawner`, tasks, ticks and polls. This chapter explains each of those in depth.

## A runtime that waits for you

Most async runtimes take over a thread. You hand them a future, they run it, and when there is nothing to do they put the thread to sleep until a socket or a timer needs attention. They own the loop.

The switch runtime turns that around. It never sleeps, never reads a clock and never starts a thread. It does work only inside `Runtime::poll(tick)`, which your code calls:

```text
loop {
    tick = your clock
    runtime.poll(tick)     // runs every ready task, then returns
    ... anything else your loop does ...
}
```

Each call to `poll` is one *poll* of the runtime. During a poll, every task that is ready runs until it next has to wait, and then `poll` returns. Between two polls, no task runs at all.

## Why the program drives

A program built on switch runs its own network stack in userspace. Packets do not arrive through the kernel; the program has to fetch them from the NIC, and send its own, by calling into the stack over and over on one CPU. Something has to make those calls, and it has to make them often and at predictable times. If the runtime owned the thread, it would decide when the network ran.

With switch, your loop owns the thread. It alternates two things: move packets, then poll the runtime so tasks can react to them. The tasks are ordinary `async fn`s that await socket operations. The [switch-net](../net/index.md) chapter adds the network half of that loop, called [the drive loop](../net/drive-loop.md).

Because the runtime has no clock, time is whatever your loop says it is. That makes the runtime easy to test (a test can pass any tick it likes) and keeps the loop in charge of when timers fire.

## The pieces

There are four things to know by name.

A `Runtime` owns tasks and timers. It has three operations of interest: `spawner()`, `poll(tick)` and `shutdown()`. Only the code that owns the `Runtime` can poll it or shut it down.

A *spawner* is a `Spawner`, obtained from `runtime.spawner()`. It is cloneable and is what you pass around: it starts tasks and creates sleeps for its runtime. A spawner cannot poll the runtime or shut it down.

A *task* is one call of a function marked `#[switch::task]`, started with `spawner.spawn(..)` or `spawner.spawn_join(..)`. A task runs until it has to wait, then *parks*: it stays alive but is not run again until something wakes it.

A *tick* is the `u64` clock value you pass to `poll`. Ticks must never go backwards. You pick the unit; once you use the network's timeouts, the unit has to be nanoseconds.

## Waking

A parked task runs again only after it is *woken*: marked ready, so that the next poll runs it. Each kind of thing a task can wait on wakes it in its own way, but all of them end in the same queue:

```text
 same thread                               other threads
 -----------                               -------------
 a std Waker (any future's wake)  --.
 a StoredWaker (switch-net sockets) -+
 a Sleep reaching its deadline  ----+--> ready tasks --> run by the next poll
 a oneshot send or drop  ----------'          ^
                                              |
 RemoteWaker::wake() on another thread --> mailbox, emptied at the
                                           start of the next poll
```

Wakes from the runtime's own thread take effect at once: the task joins the ready set and the next poll (or a later pass of the current one, as [Driving the runtime](driving.md#one-poll-step-by-step) explains) runs it. Wakes from another thread go through a `RemoteWaker` into a mailbox, which the runtime empties at the start of its next poll. [Tasks working together, and other threads](cooperating.md) covers both.

## One thread

"Single-threaded" means more here than "runs on one thread". `Runtime`, `Spawner`, `SpawnToken`, `Sleep`, `JoinHandle` and both halves of a oneshot channel are neither `Send` nor `Sync`. The compiler stops them from leaving the thread that created the runtime, and that same thread is the one that polls it.

The benefit is that tasks may freely use `Rc`, `RefCell` and `Cell` to share state with each other and with the loop. There are no locks between tasks, because there is never a second thread running one.

The runtime does not choose which CPU that thread runs on. Pinning the thread to a CPU is the program's job; for network programs, [The pinned CPU and helper threads](../nic/cpus.md) explains how.

## What this chapter covers

The pages follow the order in which you meet these questions while writing a program:

- [Writing and spawning tasks](tasks.md): which functions can be tasks, how to start them and how to get their results.
- [Driving the runtime](driving.md): exactly what one poll does, the rules for ticks, fairness between tasks, and how to write the loop.
- [Time and sleeping](time.md): sleeps, deadlines and their errors.
- [Tasks working together, and other threads](cooperating.md): yielding, oneshot channels, joining, and waking a task from another thread.
- [Panics, errors and shutdown](errors.md): every error type, what a panicking task does to the runtime, and how to shut down.

Start with [Writing and spawning tasks](tasks.md).
