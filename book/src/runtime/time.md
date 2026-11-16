# Time and sleeping

The runtime knows only the ticks your loop passes to `poll`, as described in [Driving the runtime](driving.md#ticks). A timer in switch is therefore a deadline measured in ticks: it fires during the first poll whose tick has reached it. This page covers how to create such a timer, when it fires, how to cancel it, and what can go wrong.

## A sleep is a deadline in ticks

A task waits for time with a `Sleep`, a future created by its spawner:

```rust,ignore
{{#include ../../../switch/examples/manual_loop.rs:sleep_task}}
```

Creating the sleep returns a `Result`, and awaiting it returns another; both errors are covered [below](#errors). The `Sleep` belongs to the runtime whose spawner created it.

`switch/examples/manual_loop.rs` drives this task with hand-picked ticks:

```rust,ignore
{{#include ../../../switch/examples/manual_loop.rs:tick_loop}}
```

The runtime starts at tick 100. In the first poll the task creates `sleep(10)`, whose deadline is 110, and parks. The second poll passes 100 again; the deadline is not reached and the task stays parked. The third poll passes 110, the timer fires, and the task prints `deadline reached`.

## Relative and absolute sleeps

There are two ways to state the deadline.

`spawner.sleep(ticks)` is relative. Its deadline is the runtime's *latest accepted tick* plus `ticks`. The latest accepted tick is the tick of the poll that is running now, or of the last poll if none is running, or the initial tick if the runtime has never been polled. It is never read from a clock: a sleep created inside `poll(100)` with `sleep(10)` has deadline 110, however much real time has passed since that poll started.

`spawner.sleep_until(deadline)` is absolute. The deadline is the tick you give, anywhere in the `u64` range.

When your ticks are nanoseconds, convert a `Duration` by counting nanoseconds:

```rust,ignore
{{#include ../../../switch/examples/clock_loop.rs:sleep_duration}}
```

`cargo run -p switch --example clock_loop` runs this task next to the sliced sum from [Driving the runtime](driving.md#fairness-no-task-is-interrupted); the sleep finishes after the sum:

```text
sum = 49995000
2 ms passed
```

`switch_net::within`, described in [Timeouts and racing futures](../net/timeouts.md), does this conversion for you.

## When a sleep fires

A sleep completes during the first poll whose tick is at or past its deadline. That poll wakes the task. It runs in the same poll unless the poll ends first, after 1024 task polls or because a task called `request_turn`; then it runs in the next one. When several sleeps reach their deadlines in the same poll, their tasks are woken earliest deadline first, and run in that order after the tasks that were already ready.

A sleep whose deadline is already at or before the runtime's tick completes the first time it is awaited, without parking. For example, a task that awaits `sleep_until(110)` on a runtime already at tick 110 runs to the end in a single poll.

A timer never fires between polls, because nothing runs between polls. How late it fires depends on how often your loop polls. With [busy polling](driving.md#writing-the-loop), that is usually the next time through the loop. With a loop that sleeps when idle, it can be up to one idle sleep, since the runtime offers no way to ask when the next timer is due. If your program has timers much shorter than its idle sleep, shorten the idle sleep or busy poll.

The timers are kept in a structure that covers the whole `u64` range, so a deadline far in the future costs the same as a near one.

## Cancelling

A `Sleep` does nothing until it is awaited: creating one registers no timer. Its timer is registered on the first poll that finds the deadline still ahead.

To cancel a sleep, drop it. Dropping removes its timer, and the task is not woken for it. This is how timeouts work: when a task races some work against a sleep and the work finishes first, the sleep is dropped with the race. [Timeouts and racing futures](../net/timeouts.md) builds on this.

A `Sleep` is not tied to one task. Each time a task polls it, the timer is pointed at that task, so a sleep can be created in one task and awaited in another. If a sleep outlives the last task that polled it (it was moved into a structure the task left behind, for example), its timer can still fire. The effect is at most one extra poll of another task, which, like any future, must already cope with being polled when nothing has changed.

## Errors

Both creating a sleep and awaiting it can fail with a `SleepError`:

| Variant | When |
|---|---|
| `Closed` | The runtime was shut down or dropped. Returned when creating a sleep, and by a sleep awaited afterwards. |
| `Panicked` | A task of this runtime panicked, so the runtime accepts no more work. Returned at creation and when awaited. |
| `DeadlineOverflow` | `sleep(ticks)`: the latest accepted tick plus `ticks` does not fit in a `u64`. |
| `WrongRuntime` | The sleep was awaited, with its deadline still ahead, by something other than a task of its own runtime: a task of another runtime, or a future polled outside any switch task. |
| `RegistrationExhausted` | The runtime cannot register another timer: about four billion are already registered. |

For example, on a runtime whose latest tick is above 0, a relative sleep of `u64::MAX` ticks overflows:

```rust,ignore
assert_eq!(
    spawner.sleep(u64::MAX).err(),
    Some(SleepError::DeadlineOverflow)
);
```

`sleep_until` never returns `DeadlineOverflow`, since its deadline is given directly.

Next: [Tasks working together, and other threads](cooperating.md).
