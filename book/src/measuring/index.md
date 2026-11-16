# Measuring latency under load

Run `switch-bench` to find out how much load an echo server takes before it slows down. It sends small messages to the server at a fixed rate, measures how long each echo takes and how many never come back, and raises the rate step by step. To follow this part you need [tap devices](../getting-started/first-connection.md) and [network config files](../nic/config.md), [pinned CPUs](../nic/cpus.md), and the [switch-net chapter](../net/index.md); for the junction backend also [Sharing a NIC with the junction daemon](../junction/index.md). This page explains what the benchmark measures and the words its output uses.

## What is measured

The benchmark is an echo test. A generator sends messages to a receiver, the receiver sends each message straight back, and the generator records how long each one took. The receiver is the thing being measured. It can be:

- `switch-bench serve` on the [local backend](../net/backends.md): an echo server whose process owns the NIC,
- `switch-bench serve` on the [junction backend](../junction/index.md): the same server, connected to `switch-junctiond`, which owns the NIC,
- `switch-bench kernel-serve`: an echo server on ordinary Linux kernel sockets, for comparison.

Each generator is a switch process on its own pinned CPU. On the local backend it owns a NIC port; on the junction backend it sends through the daemon that owns the NIC.

Every message starts with a 24-byte header, followed by padding up to the message size. The header holds four little-endian fields: the step number, the sender number, the sequence number and the scheduled send time:

```rust,ignore
{{#include ../../../switch-bench/src/wire.rs:header}}
```

The scheduled send time in the header is what latency is measured from. The receiver returns the bytes unchanged, so the generator learns everything it needs from the echo itself. UDP messages are datagrams, TCP messages are fixed-size pieces cut out of each connection's byte stream, and WebSocket messages are binary messages. The protocols are UDP, TCP and plain WebSocket (`ws://`).

On the command line, `--backend conduit` selects the local backend and `--backend junction` the junction backend.

## Open loop

Each generator has a number of senders (UDP sockets or TCP connections), and they share one fixed timetable. At a rate of R messages per second over N senders, message k (counting all senders together) is due at `t0 + k / R` and belongs to sender `k mod N`, so each sender sends every N / R seconds and the senders take turns. A message is due at its time whatever happened to earlier messages. This is called an open loop: a sender does not wait for an echo before sending the next message.

Because of this, a slow receiver shows up in the results: when the receiver slows down, the senders keep sending on time, so the slowdown shows up as higher latency and as messages that never come back. A closed-loop test (send, wait for the reply, send again) would instead slow its senders down and hide exactly the delays you want to see.

Latency is the time the echo arrived minus the time the message was scheduled, not the time it was actually sent. A message that went out late because the generator fell behind still counts its full delay.

## Steps and sweeps

A step holds one offered rate for a fixed time. A sweep is a list of steps at rising rates. One step looks like this:

```text
 command sent    step start
     |  50 ms lead  |
     v              v
 ----+--------------+------------+---------------------------+-----------+---->
                    |  warmup    |   measurement window      |   drain   |
                    |  (sent,    |   (sent and measured)     | (no sends,|
                    | not timed) |                           |  echoes   |
                    |            |                           |  counted) |
                    |<----------- senders follow the timetable ->|        |
                                                  sending stops here ^
```

Messages scheduled during the warmup are sent but not measured. Messages scheduled in the measurement window are measured. When the window ends, sending stops, even for messages that are still owed: a step past the receiver's limit ends on time instead of running on. Echoes keep counting until the drain ends.

Two numbers come out of each step. The latency percentiles (p50, p99, p99.9 and the maximum) cover every measured message that was echoed. The drop fraction is the share of measured messages that were not echoed by the end of the drain, and a message that was never sent counts as lost.

By default a sweep's rates are `--rate-min` × (√2)^k for k = 0, 1, 2, ... up to `--rate-max`: each rate is about 1.41 times the last; `--rates` gives an explicit list instead.

## Workers

One generator process may not be able to offer the highest rates on its own, so a sweep uses several. `switch-bench sweep` is the coordinator. It does not start DPDK or open a NIC. It starts G worker processes (`switch-bench gen`), splits each step's rate and the senders evenly among them, and talks to each over its standard input and output, one JSON line per message.

```text
                         switch-bench sweep (coordinator)
                     stdin/stdout JSON lines   |   one JSON line per step on stdout
          +------------------+-----------------+
          |                  |                  |
   switch-bench gen 0  switch-bench gen 1  ... gen G-1      each: own pinned CPU; own NIC port
                                                           (local) or the daemon's (junction)
          |                  |                  |
          +--------- messages over the network -+----->  receiver: switch-bench serve
          <--------------------- echoes ----------------         or kernel-serve
```

All workers start a step at a common wall-clock time, worker g offset by g / rate seconds so that their timetables interleave rather than send in bursts together. Before each step the coordinator checks every worker's clock; a worker whose clock is off by more than 1 ms plus half the time the check took, or one that receives a step after its start time has passed, fails the sweep. The coordinator merges the workers' results by summing counts and merging their latency histograms. Percentiles are computed from the merged histogram, never averaged across workers.

## When the generator is the limit

A benchmark has to be sure the generator kept up; otherwise it measures the generator. After each step every generator reports whether it fell behind: whether it still had messages due that it had not sent (not counting those a blocked connection was holding), or whether the 99th percentile of its senders' send lag (actual send time minus scheduled time) was above 20 µs. If any generator fell behind, the step is marked sender-late (the `generator_bound` field in the output).

A sender-late step ends the sweep. The sweep exits with a nonzero status, and the report marks the step `SENDER-LATE`. The flag does not say whether the generator or receiver backpressure was the limit.

How pushback shows up differs between the backends. On the local backend, a TCP connection whose send buffer is full blocks only its own sender; the messages it owes go out in order later, and their delay shows as latency and loss, not as sender lateness. On the junction backend, a full [send ring](../junction/index.md) (the shared memory the daemon reads sends from) stops every sender until the generator's next runtime poll, and the unsent messages count as the generator falling behind. Junction saturation can therefore show up as `SENDER-LATE`.

## Budgeted TCP and WebSocket sweeps

With TCP and WebSocket, an open-loop sender can build up an unbounded backlog of messages inside one connection once the receiver slows down. The `--budget-us W` option caps it. The first step runs without a cap, and its median round trip (echo time minus actual send time) becomes the baseline. In every later step each connection may have at most its share of the rate × (baseline + W) messages sent and not yet echoed. A message due beyond that waits until an echo frees a place; its latency is still measured from its scheduled time. A step in which any measured message had to wait is marked budget-bound.

Comparing a budgeted sweep with an uncapped one shows whether the latency at high rates comes from the receiver or from backlog inside the connections.

## Where to go next

[Running a sweep on one machine](local.md) runs all of this in Docker over tap devices, shows what the smoke run starts, and runs against the junction daemon. [Reading a benchmark report](reports.md) explains the table the sweep produces.
