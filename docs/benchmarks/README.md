# The latency benchmark

`switch-bench` measures how much load an echo server takes before it slows down. Generators send small messages to the server at a fixed rate, the server sends each one back, and the generators record how long each echo took and how many never came back. The rate rises step by step until the server, or the generators, cannot keep up.

The server under test is one of:

- `switch-bench serve`, a switch program that owns its network card;
- `switch-bench serve --backend junction`, the same program connected to `switch-junctiond`, which owns the card;
- `switch-bench kernel-serve`, an echo server on Linux kernel sockets, for comparison.

The generators are always switch programs. The messages are UDP datagrams, fixed-size pieces of a TCP byte stream, or binary WebSocket messages over plain `ws://`. Every message starts with a 24-byte header (step number, sender number, sequence number, scheduled send time), padded to the message size. The server returns the bytes unchanged.

Where to run it:

- [Running the benchmark on one machine](local.md): Docker and tap devices, to learn the tools and check they work.
- [Benchmarking on AWS](aws.md): real network cards on EC2, for numbers.
- [Reading a benchmark report](reports.md): what each column and flag in the report means.
- [Stack-to-stack throughput](memif.md): a separate tool that times two network stacks joined by shared memory, with no network card.

## Open loop

Each generator has a number of senders: UDP sockets, TCP connections or WebSocket connections. At a rate of R messages per second over N senders, message k is due at `k / R` seconds after the step's start and belongs to sender `k mod N`. A message goes out when it is due, whatever happened to earlier messages. A sender does not wait for an echo before it sends again.

Latency is the time the echo arrived minus the time the message was scheduled, not the time it actually left. When the server slows down, the senders keep to the timetable, and the slowdown shows up as higher latency and as messages that never come back. A closed-loop test (send, wait, send again) would slow its own senders down and hide the delay you want to see.

## Steps and sweeps

A step holds one rate for a fixed time:

```text
            warmup              measurement window             drain
      |-----------------|------------------------------|-----------------|
       sent, not timed      sent and measured             nothing sent,
                                                          echoes still count
```

Messages scheduled during the warmup are sent but not measured. Messages scheduled in the window are measured. When the window ends, sending stops, even for messages still owed, so a step past the server's limit ends on time. Echoes keep counting until the drain ends.

Each step gives latency percentiles (p50, p99, p99.9 and the maximum) over every measured message that came back, and a loss figure: the share of measured messages not echoed by the end of the drain. A message that was due but never sent counts as lost.

A sweep is a list of steps at rising rates. By default the rates are `--rate-min` × √2^k for k = 0, 1, 2, ..., up to `--rate-max`; `--rates` gives an explicit list instead.

`switch-bench sweep` runs a sweep. It starts several generator processes (`switch-bench gen`), splits each step's rate and the senders evenly among them, and merges their results. Percentiles come from the merged latency histogram; they are never averages of per-generator percentiles. It prints one JSON line per step and a summary line at the end.

## When the generator is the limit

If a generator falls behind its timetable, the benchmark is measuring the generator. So every step checks the send lag of each generator: how late messages left compared with their scheduled time. When the 99th percentile of a generator's send lag is above 20 µs, the step is marked sender-late, the sweep stops after it, and `switch-bench sweep` exits with a nonzero status.

Some messages wait for reasons that are the server's or the network's doing, and these do not count toward send lag. A TCP or WebSocket connection whose send buffer is full holds its own messages until it has room; their wait shows up as latency and loss. A send refused for every sender at once (no free packet buffer, a UDP send the stack could not queue, or a full send ring on the junction backend) ends that batch of sends and is counted as a stop. Neither ends the sweep.

## The in-flight budget

With TCP and WebSocket, an open-loop sender can pile up an unbounded backlog inside one connection once the server slows down. `--budget-us W` caps it. The first step runs uncapped, and its median round trip (echo time minus actual send time) becomes the baseline. In every later step, each connection may have at most its share of rate × (baseline + W) messages sent and not yet echoed. A message due beyond that waits for an echo to free a place, and its latency is still counted from its scheduled time. A step in which any measured message had to wait is marked budget-bound.

Run a sweep with a budget and one without: if the high-rate latency drops with the budget, it came from backlog inside the connections, not from the server.
