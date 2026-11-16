# The drive loop

A switch-net program has one loop that does all the work: it moves packets and runs tasks, over and over, on the pinned CPU. [Sockets with switch-net](index.md) introduced the `Net` this loop holds. This page explains what each part of that loop does, how time flows into it, and when the loop may rest. [Driving the runtime](../runtime/driving.md) covers the runtime half on its own; here we add the network.

## What a turn does

A **turn** is one pass of network work, done by `Net::turn(now)`. It is the only thing in switch-net that moves packets. A task that calls `try_send` only queues a frame; the frame leaves in a later turn. A task never sees new network state while it runs: data that arrives is noticed during a turn, and the turn wakes the task so the next runtime poll runs it.

On the local backend a turn is one `Stack::turn` on the Stack inside your process. It takes in up to `NetConfig::ingress_budget` received packets, runs TCP's timers, sends what sockets have queued, and then wakes every task waiting on a socket that became ready, plus every task waiting for an empty frame if one is free. Only sockets that reported a change since the last turn are checked, so an idle turn costs the same with one parked task or a thousand. The repository uses an ingress budget of 32 in its tap examples and tests, and 128 in switch-bench and the market-data feed (`switch-binance-feed`).

On the junction backend a turn polls the session with the daemon: it sends pending control messages, applies the daemon's events (a connection is established, the peer sent FIN, a socket closed), collects the packets the daemon delivered into the receive ring (the session's shared memory that the daemon writes received packets into), and wakes the tasks those changes concern. The daemon runs the Stack itself, on its own CPU.

`turn` returns a `Turn` with counters for what moved: packets received and processed, packets the NIC accepted, and packets still staged or pending. On the junction backend `rx_staged`, `tx_pending` and `rx_rejected` are always 0.

## A round

You rarely call `turn` alone. `switch_net::round` does one turn and then one runtime poll, both at the same moment:

```rust,ignore
pub fn round(net: &mut Net, rt: &mut Runtime, now: Instant, tick: u64) -> Result<bool, RoundError>
```

It returns `true` if anything moved: the turn received, processed or sent something, packets are still staged or pending in the Stack, or the runtime polled at least one task. `round` never sleeps. A drive loop is a loop around it:

```rust,ignore
{{#include ../../../switch-net/examples/tcp_client.rs:drive}}
```

```text
   +--> clock.sample() ---> Net::turn(now) ---> Runtime::poll(tick) --+
   |                                                                  |
   |                    a task's send reaches the TX burst:           |
   |                    the poll ends early, so the turn comes sooner |
   |                                                                  |
   +---- busy polling: go again at once <---- moved? ----------------+
         sleeping: if nothing moved, sleep (at most until poll_at)
```

## Time: one clock, two units

`Net::turn` takes the Stack's time, a `switch_net::Instant` in microseconds. `Runtime::poll` takes a tick, a `u64`. The two must come from one clock reading so the network and the tasks agree on what time it is. The repository's programs take one `std::time::Instant` at startup and, every round, read the elapsed time once: the Stack gets it in microseconds and the runtime gets it in nanoseconds.

```rust,ignore
{{#include ../../../switch-net/examples/tcp_client.rs:clock}}
```

Ticks in nanoseconds are required as soon as any task uses `switch_net::within` or a `Duration`-based sleep, because those convert durations to nanoseconds; see [Ticks](../runtime/driving.md#ticks) and [Timeouts and racing futures](timeouts.md). Use this one clock for everything, including the clock you pass to `switch_open::open`. Do not use `Instant::now()` from smoltcp, the TCP/IP library the Stack is built on: it reads the wall clock, which can jump.

## Busy polling or sleeping

The runtime cannot tell you when its next timer is due, so a loop has two reasonable shapes.

**Busy polling** calls `round` again at once, whether or not anything moved. This gives the lowest latency and is the intended way to run on a pinned CPU that nothing else uses. switch-bench's loop is a busy-polling round. [The pinned CPU and helper threads](../nic/cpus.md) explains how to give the loop a CPU to itself.

**Sleeping** pauses briefly when `round` returns `false`. The market-data feed and the first network program sleep 1 ms after an idle round, because there the CPU is shared with other work. On the local backend `Net::poll_at()` tells you when the Stack's next TCP timer is due, so a sleeping loop must not sleep past it. On the junction backend `poll_at()` always returns `None`: TCP timers run in the daemon. Either way, a runtime sleep can fire up to one idle sleep late.

## When tasks send many frames

On the local backend, frames that tasks hand to the Stack wait in the socket until the next turn sends them. If a task sends a lot in one runtime poll, those frames would sit there until every other ready task had run too. switch-net shortens that wait on its own: once the frames handed to the Stack since the last turn reach `Stack::tx_burst()`, the number of frames the NIC port takes in one burst, every further `try_send` or UDP `send` asks the runtime to end the current poll after that task ([`request_turn`](../runtime/driving.md#request_turn)). `round` then returns, and your loop turns the network sooner. There is nothing to configure. The junction backend has no equivalent; its sends go at once into the send ring, the shared memory that carries sends to the daemon.

## Turn errors

`Net::turn` returns `TurnError`, and `round` wraps it as `RoundError::Turn`:

- `TurnError::Stack` on the local backend: the Stack failed, for example `TimeWentBackwards` when `now` is earlier than the previous turn's time, or a `Port` error from the NIC port. [Stack errors and panics](../stack/errors.md) lists them.
- `TurnError::HostLost` on the junction backend: the session with the daemon ended. See [When the daemon goes away](errors.md#when-the-daemon-goes-away).

A turn that returns an error still wakes the tasks it found ready, because an error while sending can come after data was received. Tasks still waiting stay waiting for the next turn. `RoundError` also carries `RuntimeError` from the poll. It implements only `Debug`, so print it with `{:?}`.

If a panic unwinds out of `turn`, the Net is unusable: drop it. Dropping the `Net` while sockets are still alive leaves those sockets with no one to turn them, and any task waiting on them is never woken; [Shutting down cleanly](shutdown.md) gives the right order.

Next: [Opening sockets](opening.md). For the same loop without switch-net, see [The Stack's drive loop](../stack/drive-loop.md).
