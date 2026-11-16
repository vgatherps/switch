# Running a sweep on one machine

This page runs `switch-bench` on one machine: first the ready-made smoke run in Docker, then the same sweep by hand so you can see each process, then against the junction daemon. It ends with a reference of every command and flag. [Measuring latency under load](index.md) explains the terms used here.

Everything on this page runs over [tap devices](../getting-started/first-connection.md), so the Linux kernel sits in the middle of the network path. That makes it a good way to check that the tools work and to learn them; the numbers it produces are not measurements of the NIC path.

## The smoke run

From the repository root, with the [Docker image](../getting-started/environment.md#the-docker-image) built:

```sh
make -C conduit docker-bench-smoke
```

The target builds a release `switch-bench` in the image and starts one container that holds the whole setup. Each switch process gets its own tap device on its own subnet, with the kernel side of the tap as its gateway, and the kernel routes between the subnets:

```text
  CPU first     CPU first+1                CPU first+2 ...
  sweep         serve (receiver)           gen 0                 gen 1
  coordinator   10.10.0.2                  10.20.0.2             10.21.0.2
                   |                          |                     |
               tap cddut                  tap cdgen0            tap cdgen1
                   |                          |                     |
                10.10.0.1 ------ Linux kernel, IP forwarding ---- 10.20.0.1, 10.21.0.1
```

For each protocol (UDP, TCP, WebSocket) it starts the receiver and runs one sweep against it; the TCP and WebSocket sweeps use `--budget-us 50`. A last check runs a TCP sweep against a kernel listener that accepts connections and never reads: that step must still end within seconds, report its loss and finish with a complete summary.

The results land in `switch-bench/out/smoke/`: one `.jsonl` file per sweep, a `.log` file with every process's stderr, and the [report](reports.md) (`report.md`, `results.json` and one plot per sweep). The make target fails if the report is incomplete. A sweep that stopped because a sender fell behind is accepted here, since over tap devices that is expected.

The smoke run's settings are make variables:

| Variable | Default | Meaning |
|---|---|---|
| `BENCH_CPUSET` | `9-12` | The container's CPUs, a range of at least 2 + `BENCH_GENS`: the coordinator, the receiver, one per generator |
| `BENCH_GENS` | `2` | Generator workers |
| `BENCH_SENDERS` | `4` | Senders across all workers |
| `BENCH_PROTOS` | `udp tcp ws` | Protocols to sweep |
| `BENCH_SMOKE_ARGS` | `--rate-min 1000 --rate-max 256000 --step-ms 300 --warmup-ms 100 --drain-ms 100` | Extra `sweep` flags |

The default CPU range needs a Docker VM with 13 CPUs. On a smaller VM pick another range, for example `make -C conduit docker-bench-smoke BENCH_CPUSET=3-6`. The target refuses to start if another running container shares those CPUs, or runs with no CPU limit, because timed runs need their CPUs to themselves.

## The same sweep by hand

The smoke run is a shell script, `switch-bench/local/smoke.sh`, and reading it is the quickest way to learn how the pieces connect. It runs inside a container started with `--sysctl net.ipv4.ip_forward=1`, the tap capabilities from [Setting up a build environment](../getting-started/environment.md#running-a-container-for-the-examples) and a range of CPUs instead of one.

Each process needs a [network config file](../nic/config.md) for its own tap device, plus a small script that gives the kernel side of the tap its address once the switch process has created the device:

```sh
{{#include ../../../switch-bench/local/smoke.sh:node}}
```

The coordinator starts the generators through a command template. The script writes one, `gen.sh`, that pins generator `I` to its own CPU and runs `switch-bench gen` with that generator's config file:

```sh
{{#include ../../../switch-bench/local/smoke.sh:gen}}
```

Then a sweep is two commands. Start the receiver pinned to its CPU and wait for its ready line, `switch-bench serve: udp echo on port 7000`, on stderr. Then run the coordinator on a CPU of its own:

```sh
taskset -c 4 switch-bench serve --config dut.json --proto udp --port 7000 &
sh up-dut.sh

taskset -c 3 switch-bench sweep --proto udp --gens 2 --senders 4 \
    --gen-cmd "sh gen.sh {i} udp 10.10.0.2:7000 {senders} 64" \
    --rate-min 1000 --rate-max 256000 > udp.jsonl
```

The coordinator replaces `{i}` in the command with the worker number (0 to G-1) and `{senders}` with each worker's share of `--senders`, and runs it with `sh -c`. It prints progress on stderr (`processes up`, `processes synced`, `sweep start`, `sweep end`) and one JSON line per step on stdout, then a summary line. `python3 switch-bench/report.py DIR` turns a directory of `.jsonl` files into a report.

The receiver runs until it gets SIGINT or SIGTERM. Each generator runs until its standard input closes, which the coordinator does at the end of the sweep.

## Against the junction daemon

With `--backend junction`, `serve` and `gen` do not open a NIC. They connect to a running [`switch-junctiond`](../junction/running.md) through its control socket instead, so they take `--junction-socket` in place of `--config`. Start the daemon on its own CPU first, then point the receiver at the daemon's directory:

```sh
taskset -c 2 switch-junctiond --config nic.json --dir /run/switch-junctiond --control-cpus 0
taskset -c 3 switch-bench serve --backend junction \
    --junction-socket /run/switch-junctiond/control.sock --proto udp --port 9000
```

The generators can use either backend, independently of the receiver. On the junction backend a process never starts DPDK itself; it sizes its packets from what the daemon reports when it connects.

`switch-bench/local/junction-smoke.sh` checks this path end to end inside the DPDK container. Build the binaries first (`cargo build --locked -p switch-junctiond -p switch-bench --bins`), then run it in a container with two CPUs and the tap capabilities. It starts the daemon on a tap device, echoes UDP and TCP messages from kernel clients through `serve --backend junction`, runs a short UDP step with `gen --backend junction`, and requires every process to exit cleanly. The daemon's CPU and the application's CPU are the environment variables `JUNCTION_CPU` (default 0) and `APP_CPU` (default 2).

The two backends report pushback differently, as [Measuring latency under load](index.md#when-the-generator-is-the-limit) explains: on the local backend a full TCP connection shows up as latency and loss, on the junction backend a full send ring shows up as a sender-late step.

## Command reference

Every flag is written `--name value`. A flag given twice, an unknown flag or a flag without a value is an error; the command then prints the error and the usage text and exits nonzero.

### serve

```text
switch-bench serve [--backend conduit --config F | --backend junction --junction-socket PATH]
                   --proto udp|tcp|ws --port P
```

The echo receiver. UDP echoes each datagram to its sender. TCP and WebSocket keep 64 accepts armed and serve up to 256 connections at once, sending every byte or message back. The WebSocket side is a `ws://` server built on [switch-ws](../protocols/ws-server.md). While serving, its loop makes no system calls. It prints `switch-bench serve: <proto> echo on port <P>` on stderr once it is ready; a TCP or WebSocket listener that fails to start fails before that line. It runs until SIGINT or SIGTERM.

### gen

```text
switch-bench gen [--backend conduit --config F | --backend junction --junction-socket PATH]
                 --proto udp|tcp|ws --target IP:PORT --senders N --size S [--local-port 40000]
```

One generator worker. The coordinator starts it; you normally do not run it by hand. It opens its NIC (or connects to the daemon), binds or connects its N senders, reports ready, and then runs the steps it receives on standard input. UDP uses N sockets, TCP N connections, WebSocket N `ws://` connections. Sender i uses local port `--local-port` + i.

`--size` is the message size in bytes and must be at least 24, the header. WebSocket messages may be at most 1400 bytes, so each fits in one TCP segment.

### sweep

```text
switch-bench sweep --proto udp|tcp|ws --gens G --gen-cmd CMD --senders N
                   (--rate-min PPS --rate-max PPS | --rates PPS,PPS,...)
                   [--step-ms 2000] [--warmup-ms 500] [--drain-ms 200] [--budget-us W]
```

| Flag | Meaning |
|---|---|
| `--gens G` | Generator workers to start |
| `--gen-cmd CMD` | The `sh -c` command that starts worker `{i}` with `{senders}` senders |
| `--senders N` | Senders across all workers; must be a positive multiple of `--gens` |
| `--rate-min`, `--rate-max` | Rates `rate-min` × √2^k up to `rate-max`, in messages per second; `0 < rate-min <= rate-max` |
| `--rates` | An explicit list instead; positive and strictly increasing; cannot be combined with `--rate-min`/`--rate-max` |
| `--step-ms` | Measurement window of each step (default 2000) |
| `--warmup-ms` | Warmup before the window (default 500) |
| `--drain-ms` | Time after the window during which echoes still count (default 200) |
| `--budget-us W` | TCP and WebSocket only: the in-flight budget described on [Measuring latency under load](index.md#budgeted-tcp-and-websocket-sweeps) |

The sweep exits 0 only when every step ran and no sender fell behind. A worker that does not answer within a step's time plus 10 seconds, that exits, or that reports a step it started late fails the sweep; the coordinator then kills every worker's process group. Steps already printed stay valid, and the summary line says `"complete": false` with the error.

### kernel-serve

```text
switch-bench kernel-serve --proto udp|tcp|ws --bind IP --port P
```

An echo receiver on Linux kernel sockets, for comparison with `serve`. It exists only on Linux, binds to `IP:P` and runs until SIGINT or SIGTERM. The generators stay switch processes.

## Where to go next

[Reading a benchmark report](reports.md) explains the report the smoke run writes.
