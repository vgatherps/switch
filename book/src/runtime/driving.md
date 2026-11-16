# Driving the runtime

Your loop drives the runtime by calling `Runtime::poll(tick)`. The previous page, [Writing and spawning tasks](tasks.md), showed how tasks get into the runtime; this page explains what one poll does with them, what the tick must be, how tasks share the poll, and how to write the loop around it.

## A small program to follow

`switch/examples/basic.rs` spawns two tasks and polls until both finish, printing what each poll reports:

```rust,ignore
{{#include ../../../examples/basic.rs}}
```

Run it with `cargo run -p switch-examples --example basic`. It prints:

```text
=== switch runtime example ===

Task 1: immediate
Task 2: start
Polled 2 tasks; 1 live
Task 2: after yield
Polled 1 tasks; 0 live

Done.
```

The first poll runs both tasks. `immediate` finishes. `yield_once` prints, then awaits `yield_now()`, which asks to be run again in the *next* poll, so the first poll ends with one task still alive. The second poll runs `yield_once` to the end.

The `PollReport` that `poll` returns has two fields. `tasks_polled` counts how many times this poll called into a task's future. `live_tasks` counts every task that has not finished, including tasks parked on a timer or waiting for a wake.

## One poll, step by step

```text
 poll(tick)
   |
   |-- runtime shut down or panicked?           -> Err(Closed) / Err(Panicked)
   |-- tick lower than the last accepted tick?  -> Err(TimeRegressed), nothing changes
   |
   |-- deliver wakes that other threads queued in the mailbox
   |-- requeue the tasks that yielded during the last poll
   |-- move the tick forward; every sleep whose deadline <= tick wakes its task
   |
   |-- finish the pass that request_turn() cut short last time, if any
   |   pass 1: tasks woken or spawned since the last poll, then the yielded
   |           tasks, then the tasks whose sleep just expired, earliest
   |           deadline first
   |   pass 2: tasks woken or spawned during pass 1
   |   pass 3: ...
   |   (stop when no task is ready, when 1024 task polls were made,
   |    or right after a task calls request_turn())
   |
   '-- return PollReport { tasks_polled, live_tasks }
```

The tick moves forward before any task runs, so every task in this poll, including one left over from a cut-short pass, sees the new tick.

Each pass takes the set of ready tasks as it stands when the pass starts, and runs each one until it returns `Pending` or finishes. If a running task wakes or spawns another task, that task waits for the next pass of the same poll. Passes continue until no task is ready.

The 1024 limit keeps one poll from running forever when tasks keep waking each other. It is checked between passes: a pass that has started always finishes, so one poll can make somewhat more than 1024 task polls. Work left over when the limit stops the poll is not lost; it runs in the next poll.

Calling `poll` from inside a task of the same runtime panics.

## Ticks

The tick is the only clock the runtime has. You give it a starting tick in `Runtime::new(initial_tick)`, and a new one on every `poll`.

The rule for ticks is simple: they must never go backwards. A tick equal to the last one is fine; the runtime just has no new timers to fire. A tick lower than the last accepted one makes `poll` return `Err(RuntimeError::TimeRegressed { current, supplied })`, where `current` is the runtime's tick and `supplied` the one you passed. Nothing else happens: no task runs and no state changes, so the loop can pass a correct tick next time.

The unit is yours. Tests often use small numbers, as on this page. Real programs read a monotonic clock. Once a program uses [`switch_net::within`](../net/timeouts.md) or sleeps measured in `Duration`s, its ticks must be nanoseconds, because those functions turn a `Duration` into ticks by counting nanoseconds. [The drive loop](../net/drive-loop.md) shows how one clock sample gives both the runtime's tick and the network stack's time.

## Tasks woken before a poll run in it

Anything that wakes or spawns a task between two polls is picked up by the next poll. This is how the network fits in: the loop's receive step takes in packets, which wakes the tasks waiting on them, and then the loop polls, which runs those tasks.

In `switch/examples/manual_loop.rs`, `EventQueue` stands in for the network: `poll_nonblocking` spawns one task for each event waiting in it. `main` starts the runtime at tick 100 with one task that sleeps for 10 ticks (shown on [Time and sleeping](time.md)) and one event waiting, then polls at ticks 100, 100 and 110, pushing a second event just before the last poll:

```rust,ignore
{{#include ../../../examples/manual_loop.rs:tick_loop}}
```

Run it with `cargo run -p switch-examples --example manual_loop`:

```text
first event
tick 100: PollReport { tasks_polled: 2, live_tasks: 1 }
tick 100: PollReport { tasks_polled: 0, live_tasks: 1 }
event at deadline
deadline reached
tick 110: PollReport { tasks_polled: 2, live_tasks: 0 }
```

The first poll runs the task for the first event and the sleeping task, which parks. The second poll has nothing new to run. Before the third poll the loop dispatches the second event, so that poll runs its task, and also the sleeping task, whose deadline it reached. The event's task comes first, because tasks spawned before the poll go ahead of tasks whose sleep expired.

The opposite is also true. Work your loop does *after* a poll is not seen by any task until the following poll.

## Fairness: no task is interrupted

Tasks are never interrupted. Once the runtime calls into a task, that task keeps the thread until it returns `Pending` at an `.await` or finishes. The runtime only decides the order in which ready tasks run.

So a task that computes for a long time without awaiting holds up every other task and the whole loop, including the network. Nothing moves packets until it returns. A task with a long piece of work should split it into slices and call `switch::yield_now().await` between them, as this task from `switch/examples/clock_loop.rs` does:

```rust,ignore
{{#include ../../../examples/clock_loop.rs:sliced_work}}
```

`yield_now` makes the task ready again for the *next* poll, not for a later pass of the current one. So between two slices, the current poll finishes, your loop does its other work, and the next poll resumes the task. [Tasks working together](cooperating.md#yielding) has more on yielding.

## request_turn

Sometimes a task knows that the loop should get control back now, before the rest of the ready tasks run. `switch::request_turn()`, called from inside a task, ends the current poll as soon as that task returns. The poll reports what it did so far. The next poll first finishes the tasks that were left in the cut-short pass, then carries on as usual.

The request lasts for that one poll. Outside a poll, `request_turn()` does nothing.

Application code rarely calls it. switch-net calls it for you: on the local backend, once tasks have handed the network stack a full burst of frames, it ends the poll so that the round's send step pushes them out sooner. [The drive loop](../net/drive-loop.md) describes when that happens.

## Writing the loop

Here is a complete loop that runs tasks against a real clock, from `switch/examples/clock_loop.rs`:

```rust,ignore
{{#include ../../../examples/clock_loop.rs:loop}}
```

Run it with `cargo run -p switch-examples --example clock_loop`. It prints:

```text
sum = 49995000
2 ms passed
```

In this run, the sum's ten slices, one per poll, finished before the other task's 2 ms were up.

The tick is the time since `start` in nanoseconds, read once each time through the loop. After each poll the loop sleeps for 100 µs. That sleep is the loop's *idle strategy*, and it is up to you. There are two shapes in this repository.

*Busy polling* goes straight back to the top of the loop. The thread never sleeps, so a woken task runs usually the next time through the loop, and a timer fires on the first poll at or after its deadline. This is what latency-sensitive programs do, on a CPU set aside for the loop (see [The pinned CPU and helper threads](../nic/cpus.md)). The benchmark tool `switch-bench` ([Measuring](../measuring/index.md)) works this way: each time through its loop it receives, polls the runtime and sends, with no sleep.

*Sleeping when idle* checks whether the last time through the loop did anything and, if not, sleeps the thread for a short fixed time. `switch-binance-feed`, the market-data feed walked through in [Walkthrough: a market-data feed](../protocols/binance-feed.md), sleeps 1 ms after a time through its loop where nothing moved:

```rust,ignore
if !moved {
    std::thread::sleep(IDLE_SLEEP);
}
```

The runtime has no call that tells you when its next timer is due. A sleeping loop therefore cannot sleep exactly until the next deadline; a timer fires at the first poll after its deadline, which can be up to one idle sleep late. Pick the idle sleep with that in mind, or busy poll. On the local backend, `Net::poll_at()` tells you when the network stack's own timers are next due, and [The drive loop](../net/drive-loop.md) shows how to bound the sleep with it.

## Where the network comes in

A network program's loop polls the runtime in the middle of a *round* of three steps. The receive step takes in packets and wakes the tasks whose sockets became ready. The poll runs those tasks. The send step pushes out what they wrote, so a reply that a woken task queued leaves in the same round. switch-net packages the three steps as one call, `switch_net::round`, and the loop that calls it is the [drive loop](../net/drive-loop.md). Everything on this page applies to its poll step unchanged.

Next: [Time and sleeping](time.md) explains timers in detail.
