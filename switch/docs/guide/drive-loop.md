# The drive loop

The drive loop is the loop in `main` that receives and sends packets and runs tasks, none of which happens outside it. A round is one iteration of that loop.

## A round

One call to [`round`](crate::net::round) is one round:

```rust
use switch::Runtime;
use switch::net::{Instant, Net, RoundError};

fn drive(
    net: &mut Net,
    rt: &mut Runtime,
    mut sample: impl FnMut() -> (Instant, u64),
) -> Result<(), RoundError> {
    loop {
        let (now, tick) = sample();
        switch::net::round(net, rt, now, tick)?;
    }
}
```

```text
        read the clock once
                |
                v
   +------------------------+
   | receive                |  take packets from the card, answer what TCP
   |   net.receive(now)     |  must answer, wake the tasks whose sockets
   +------------------------+  have something for them
                |
                v
   +------------------------+
   | run tasks              |  each woken task runs until it waits again;
   |   rt.poll(tick)        |  what it sends is queued on its socket
   +------------------------+
                |
                v
   +------------------------+
   | send                   |  queued data goes to the card
   |   net.send()           |
   +------------------------+
                |
                v
        back to the top
```

Because tasks run between the receive and send steps, a task woken by a packet can run in the same round, and what it sends in answer can leave at the end of that round.

`try_send` on a socket queues data and returns; the data leaves in a send step. A task waiting in `recv` runs again only after a receive step has found data, a FIN or an RST for it.

`round` is three calls, [`Net::receive`](crate::net::Net::receive), [`Runtime::poll`](crate::Runtime::poll) and [`Net::send`](crate::net::Net::send), which a loop with work of its own to do between them can make itself.

## The clock

`now` is the network's time, an [`Instant`](crate::net::Instant) counted in microseconds, which TCP uses for its timers. `tick` is the runtime's tick from [Your first task](crate::guide::first_task).

Read the clock once per round and derive both from that reading, so tasks and the network agree on the time. Count ticks in nanoseconds, because the [timeout helpers](crate::guide::timeouts) convert a `Duration` to ticks on that assumption. The `Clock` in [Your first network program](crate::guide::first_connection) does both in its `sample`.

Both times must be monotonic, which `std::time::Instant` guarantees. Pass the same clock to `open`, and start the runtime at a tick no later than the first reading. `Runtime::new(0)` will do when ticks count from the program's start.

## Going round without pause, or sleeping

The loop above busy-polls, which is how a latency-sensitive program runs on an isolated CPU. `docs/operations/cpus.md` in the repository explains how to isolate one.

A program that shares its CPU can sleep when a round did nothing. `round` returns `false` when no packet was received or sent, none is waiting, and no task ran:

```rust
use switch::Runtime;
use switch::net::{Instant, Net, RoundError};
use std::time::Duration;

const IDLE_SLEEP: Duration = Duration::from_millis(1);

fn drive(
    net: &mut Net,
    rt: &mut Runtime,
    mut sample: impl FnMut() -> (Instant, u64),
) -> Result<(), RoundError> {
    loop {
        let (now, tick) = sample();
        if !switch::net::round(net, rt, now, tick)? {
            std::thread::sleep(IDLE_SLEEP);
        }
    }
}
```

While the loop sleeps, nothing is received, no TCP timer runs and no task's sleep ends, so everything can be late by the length of the sleep. The examples sleep one millisecond. [`net.poll_at()`](crate::net::Net::poll_at) gives the time of the network's next timer, for a loop that wants to sleep until then; `None` means it has none. The runtime has no such call for tasks' sleeps, so a loop whose tasks sleep keeps its own sleeps short.

## When a round fails

`round` returns a [`RoundError`](crate::net::RoundError), which says which step failed:

| Error | Meaning |
|---|---|
| `Turn(TurnError::Stack(_))` | The network stack or the card reported a failure. This includes `TimeWentBackwards`, when `now` is earlier than in the previous round. |
| `Turn(TurnError::HostLost(_))` | The connection to the daemon is gone. Only when [sharing a card](crate::guide::sharing). |
| `Runtime(RuntimeError::TimeRegressed { .. })` | `tick` is lower than in the previous round. |
| `Runtime(RuntimeError::Closed)` | The runtime was shut down. |
| `Runtime(RuntimeError::Panicked)` | A task panicked in an earlier round. |

`round` stops at the first failing step. The programs in this guide treat any of these as fatal, leave the loop and [shut down](crate::guide::shutdown). `RoundError` implements only `Debug`, so print it with `{:?}`.

Next: [Opening sockets](crate::guide::sockets).
