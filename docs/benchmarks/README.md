# The latency benchmark

`switch-bench` measures how much load an echo server takes before it slows down. Generators send small messages to the server at a fixed rate, open loop, and the rate rises through a list of steps. Each step reports the latency of the echoes, measured from each message's scheduled send time, and the share of messages that never came back.

The server under test is one of:

- `switch-bench serve`, a switch program that owns its network card (`--backend conduit`, the default);
- `switch-bench serve --backend junction`, the same program sharing a card through the daemon, `switch-junctiond`;
- `switch-bench kernel-serve`, an echo server on Linux kernel sockets, for comparison.

The generators are always switch programs.

Where to run it:

- [Running the benchmark on one machine](local.md): Docker and tap devices, to learn the tools and check they work.
- [Benchmarking on AWS](aws.md): real network cards on EC2, for numbers.
- [Reading a benchmark report](reports.md): what each column and flag in the report means.
- [Stack-to-stack throughput](memif.md): a separate tool that times two network stacks joined by shared memory, with no network card.

## Steps and sweeps

A step holds one rate for a fixed time:

```text
            warmup              measurement window             drain
      |-----------------|------------------------------|-----------------|
       sent, latency      sent and measured              one last batch,
       not measured                                      echoes still count
```

Messages scheduled in the window are measured, and their echoes count until the drain ends. Each step gives latency percentiles (p50, p99, p99.9 and the maximum) and the share of measured messages not echoed. A sweep is a list of steps at rising rates. `switch-bench sweep` runs one across several generator processes and prints one JSON line per step and a summary line at the end.

A step is marked sender-late when the 99th percentile of a generator's send lag, which is how late messages left compared with their scheduled time, is above 20 µs. The generator, not the server, was then the limit, so the sweep stops after that step and `switch-bench sweep` exits with a nonzero status.

A sweep is set by:

- `--proto`: UDP datagrams, fixed-size pieces of a TCP byte stream, or binary WebSocket messages over plain `ws://`;
- `--rate-min` and `--rate-max`, or `--rates`: the rates of the steps;
- `--senders`: the UDP sockets, TCP connections or WebSocket connections the load is spread over;
- `--budget-us`: for TCP and WebSocket, the in-flight budget below.

The [command reference](local.md#command-reference) has every flag.

## The in-flight budget

With TCP and WebSocket, an open-loop sender can pile up an unbounded backlog inside one connection once the server slows down. `--budget-us W` caps the messages that are sent and not yet echoed at the number the step's rate sends in the first step's median round trip plus W microseconds, divided among the connections. The first step runs uncapped to measure that round trip. A message over the cap waits, its latency still counted from its scheduled time, and a step in which any measured message waited is marked budget-bound.
