# The drive loop

Packets are received and sent, and tasks run, only when the loop in `main` goes round. That loop is called the drive loop, and one trip through it is a round.

## A round

[`round`](crate::net::round) does one:

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
   | send                   |  everything queued goes to the card
   |   net.send()           |
   +------------------------+
                |
                v
        back to the top
```

The order is what makes a reply fast: a task woken by a packet runs in the same round, and what it sends in answer leaves at the end of that round.

It also sets what a task may assume. `try_send` on a socket queues data and returns; the data leaves in the send step. A task waiting in `recv` runs again only after a receive step has found data for it. So a task that keeps the thread for a long time delays every packet in both directions, which is why long work is cut up with [`yield_now`](crate::guide::tasks).

`round` is three calls, [`Net::receive`](crate::net::Net::receive), [`Runtime::poll`](crate::Runtime::poll) and [`Net::send`](crate::net::Net::send). A loop with work of its own to do between them can call them itself.

## One clock, two units

A round takes the time twice. `now` is the network's time, an [`Instant`](crate::net::Instant) counted in microseconds, which TCP uses for its timers. `tick` is the runtime's tick from [Your first task](crate::guide::first_task).

Read the clock once per round and derive both from that reading, so tasks and the network agree on the time. Count ticks in nanoseconds: the [timeout helpers](crate::guide::timeouts) convert a `Duration` to ticks on that assumption. The `Clock` of the first network program does both:

```rust
use std::time::Instant as WallInstant;
use switch::net::Instant;

struct Clock {
    start: WallInstant,
}

impl Clock {
    fn sample(&self) -> (Instant, u64) {
        let nanos = u64::try_from(self.start.elapsed().as_nanos()).expect("fits u64 nanoseconds");
        let micros = i64::try_from(nanos / 1_000).expect("fits i64 microseconds");
        (Instant::from_micros(micros), nanos)
    }
}
```

`std::time::Instant` never goes backwards, which both units require. Use the same clock for the `Runtime::new` tick and for the clock passed to `open`.

## Going round without pause, or sleeping

The loop above calls `round` again at once, whether or not anything happened. That is how a latency-sensitive program runs: it has a CPU to itself, keeps it at 100%, and sees a packet as soon as the card has it. `docs/operations/cpus.md` in the repository explains how to keep other work off that CPU.

A program that shares its CPU can sleep when a round did nothing. `round` returns `false` when no packet was received or sent, none is waiting, and no task ran:

```rust
# use switch::Runtime;
# use switch::net::{Instant, Net, RoundError};
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

While the loop sleeps, nothing is received, no TCP timer runs and no task's sleep ends, so everything is late by up to the length of the sleep. One millisecond is a reasonable bound for most uses. To sleep longer, ask [`net.poll_at()`](crate::net::Net::poll_at) for the time of the network's next timer and wake by then; `None` means it has none. Tasks' own sleeps are not included in that answer.

## When a round fails

`round` returns a [`RoundError`](crate::net::RoundError), which says which step failed:

| Error | Meaning |
|---|---|
| `Turn(TurnError::Stack(_))` | The network stack or the card reported a failure. This includes `TimeWentBackwards`, when `now` is earlier than in the previous round. |
| `Turn(TurnError::HostLost(_))` | Only when [sharing a card](crate::guide::sharing): the connection to the daemon is gone. |
| `Runtime(RuntimeError::TimeRegressed { .. })` | `tick` is lower than in the previous round. |
| `Runtime(RuntimeError::Closed)` | The runtime was shut down. |
| `Runtime(RuntimeError::Panicked)` | A task panicked in an earlier round. |

`round` stops at the first failing step. The programs in this guide treat any of these as the end: they leave the loop and [shut down](crate::guide::shutdown). `RoundError` implements only `Debug`, so print it with `{:?}`.

Next: [Opening sockets](crate::guide::sockets).
