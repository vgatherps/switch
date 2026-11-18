# The drive loop

The drive loop is the loop in `main` that receives and sends packets and runs tasks. A round is one iteration of that loop.

## A round

The loop calls [`round`](crate::net::round):

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

If your loop needs to do work between steps, call [`net.receive(now)`](crate::net::Net::receive), [`rt.poll(tick)`](crate::Runtime::poll) and [`net.send()`](crate::net::Net::send) in that order instead of `round`.

## The clock

`now` is the network's time, an [`Instant`](crate::net::Instant) counted in microseconds, which TCP uses for its timers. `tick` is the runtime's tick from [Your first task](crate::guide::first_task).

Read the clock once per round and derive both from that reading. Count ticks in nanoseconds, because the [timeout helpers](crate::guide::timeouts) convert a `Duration` to ticks on that assumption. The `Clock` in [Your first network program](crate::guide::first_connection) does both in its `sample`.

Both times must be monotonic. Pass the same clock to `open`, and start the runtime at a tick no later than the first reading.

## Going round without pause, or sleeping

The loop above busy-polls. `docs/operations/cpus.md` in the repository explains how to isolate a CPU for it.

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

While the loop sleeps, nothing is received and no timer fires, so everything can be late by the length of the sleep. [`net.poll_at()`](crate::net::Net::poll_at) gives the time of the network's next timer; the runtime has no such call for tasks' sleeps.

## When a round fails

`round` returns a [`RoundError`](crate::net::RoundError), which is `Turn` for a receive or send error and `Runtime` for a poll error. The guide's examples return it from `main`, and `tcp_echo_server` [shuts down](crate::guide::shutdown) first. `RoundError` implements only `Debug`, so print it with `{:?}`.

Next: [Opening sockets](crate::guide::sockets).
