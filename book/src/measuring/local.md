# Running a sweep on one machine

Start with one command, `make -C conduit docker-bench-smoke`, which runs a whole benchmark in Docker on one machine. This page explains what that run starts and what it writes, how to run the benchmark against the junction daemon, and every command and flag of `switch-bench`. [Measuring latency under load](index.md) defines the terms.

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

The results land in `switch-bench/out/smoke/`: one `.jsonl` file per sweep (`udp-switch.jsonl`, `tcp-switch.jsonl`, `ws-switch.jsonl`, and `overload-check.jsonl` from the last check), and the stderr of each process (`dut.log` for the receiver, `sweep.log` for the coordinator, `gen<i>.log` for each generator). When every step of the script succeeds, the make target then runs `report.py`, which writes the [report](reports.md) (`report.md`, `results.json` and one plot per sweep) into the same directory and fails the target if the report is incomplete.

Over tap devices the generators often fall behind, and the script treats that differently for the sweeps and for the last check. A protocol sweep that ends complete but sender-late is accepted, and the script goes on. The bounded-overload check is not: the coordinator exits nonzero for its sender-late step, the script prints `smoke: bounded overload: the sweep against a sink failed: ...` and exits 1, and make reports the target failed (exit 2) before `report.py` has run. The `.jsonl` files are all there by then, so make the report yourself and read it:

```sh
python3 switch-bench/report.py switch-bench/out/smoke --title 'Local smoke (tooling validation over net_tap; not a measurement)'
```

`report.py` lists each sender-late sweep as a reason the results are incomplete and exits 1; [Reading a benchmark report](reports.md) shows a report from such a run.

The smoke run's settings are make variables:

| Variable | Default | Meaning |
|---|---|---|
| `BENCH_CPUSET` | `9-12` | The container's CPUs, a range of at least 2 + `BENCH_GENS`: the coordinator, the receiver, one per generator |
| `BENCH_GENS` | `2` | Generator workers |
| `BENCH_SENDERS` | `4` | Senders across all workers |
| `BENCH_PROTOS` | `udp tcp ws` | Protocols to sweep |
| `BENCH_SMOKE_ARGS` | `--rate-min 1000 --rate-max 256000 --step-ms 300 --warmup-ms 100 --drain-ms 100` | Extra `sweep` flags |

The default CPU range needs a Docker VM with 13 CPUs. On a smaller VM pick another range, for example `make -C conduit docker-bench-smoke BENCH_CPUSET=3-6`. The target refuses to start if another running container shares those CPUs, or runs with no CPU limit, because timed runs need their CPUs to themselves.

## What the smoke run starts

The smoke run is a shell script, `switch-bench/local/smoke.sh`. The make target runs it in a container started like this, with the image's tap capabilities, IP forwarding on, and here the four CPUs 3 to 6 that `BENCH_CPUSET=3-6` gives:

```sh
docker run --rm --init --cpuset-cpus=3-6 --sysctl net.ipv4.ip_forward=1 \
    --cap-add NET_ADMIN --device /dev/net/tun \
    -v "$PWD:/work" -w /work/conduit \
    -v switch-cargo-registry:/usr/local/cargo/registry \
    -v switch-linux-target:/work/target -v switch-linux-target:/work/conduit/target \
    switch-dpdk-test:local sh ../switch-bench/local/smoke.sh ../switch-bench/out/smoke <sweep flags>
```

The script starts by working out its settings:

```sh
{{#include ../../../switch-bench/local/smoke.sh:vars}}
```

`$out` is the output directory, its first argument; the rest of its arguments are passed on to every sweep. `$bench` is the `switch-bench` binary. `$first` is the first CPU of the container's range: the coordinator runs on `$first`, the receiver on `$first + 1`, and generator `i` on `$first + 2 + i`, which is why the range needs at least 2 + `GENS` CPUs.

Each process needs a [network config file](../nic/config.md) for its own tap device, plus a small script that gives the kernel side of the tap its address once the switch process has created the device. The `node` function writes both; the script calls it once for the receiver (`node dut 10`) and once per generator (`node gen0 20`, `node gen1 21`, ...):

```sh
{{#include ../../../switch-bench/local/smoke.sh:node}}
```

The coordinator starts the generators through a command template. The script writes one, `$out/gen.sh`, that pins generator `I` to its own CPU and runs `switch-bench gen` with that generator's config file:

```sh
{{#include ../../../switch-bench/local/smoke.sh:gen}}
```

For each protocol, the script starts the receiver on `$first + 1`, gives its tap device the kernel address, and waits three seconds:

```sh
sh "$out/up-dut.sh" &
taskset -c $((first + 1)) "$bench" serve --config "$out/dut.json" --proto "$proto" --port 7000 \
    2>>"$out/dut.log" &
```

The receiver prints `switch-bench serve: <proto> echo on port 7000` to stderr once it is ready. Then the script runs the coordinator on `$first`:

```sh
taskset -c "$first" "$bench" sweep --proto "$proto" --gens "$gens" --senders "$senders" \
    --gen-cmd "sh $out/gen.sh {i} $proto 10.10.0.2:7000 {senders} 64" "$@" \
    >"$out/$proto-switch.jsonl" 2>>"$out/sweep.log"
```

(TCP and WebSocket sweeps also get `--budget-us 50`.) The coordinator replaces `{i}` in the command with the worker number (0 to G-1) and `{senders}` with each worker's share of `--senders`, and runs it with `sh -c`. It prints progress on stderr (`processes up`, `processes synced`, `sweep start`, `sweep end`) and one JSON line per step on stdout, then a summary line.

The receiver runs until it gets SIGINT or SIGTERM; the script stops it after each sweep. Each generator runs until its standard input closes, which the coordinator does at the end of the sweep.

## Against the junction daemon

With `--backend junction`, `serve` and `gen` do not open a NIC. They connect to a running [`switch-junctiond`](../junction/running.md) through its control socket instead, so they take `--junction-socket` in place of `--config`. The commands below need a container that may use CPUs 0, 2 and 3 (`--cpuset-cpus=0,2,3` instead of `--cpuset-cpus=3` in the [example container](../getting-started/environment.md#running-a-container-for-the-examples)). `jn0.json` is a tap network config file, the one saved on [Running switch-junctiond](../junction/running.md). Start the daemon with its network thread on CPU 2 and its control thread and DPDK's helper threads on CPU 0 (`--control-cpus`, see [CPUs](../junction/running.md#cpus)), then point the receiver at the daemon's directory:

```sh
taskset -c 2 switch-junctiond --config jn0.json --dir /run/switch-junctiond --control-cpus 0 &
taskset -c 3 switch-bench serve --backend junction \
    --junction-socket /run/switch-junctiond/control.sock --proto udp --port 9000
```

The generators can use either backend, independently of the receiver. On the junction backend a process does not start DPDK or open a NIC; it sizes its packets from what the daemon reports when it connects.

`switch-bench/local/junction-smoke.sh` checks this path end to end inside the DPDK container. Build the binaries first (`cargo build --locked -p switch-junctiond -p switch-bench --bins`), then run it in a container that may use two CPUs, with the tap capabilities. It writes its own tap config, starts the daemon, echoes UDP and TCP messages from kernel clients through `serve --backend junction`, runs a short UDP step with `gen --backend junction`, and requires every process to exit cleanly. The daemon's network CPU and the application's CPU are the environment variables `JUNCTION_CPU` (default 0) and `APP_CPU` (default 2); the script puts the daemon's control thread on `APP_CPU` too.

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

`--size` is the message size in bytes and must be at least 24, the header. WebSocket messages may be at most 1400 bytes.

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
| `--rate-min`, `--rate-max` | Rates `rate-min` × (√2)^k up to `rate-max` (each rate about 1.41 times the last), in messages per second; `0 < rate-min <= rate-max` |
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
