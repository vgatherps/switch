# Running the benchmark on one machine

The smoke run starts a whole benchmark in one Docker container: a switch echo server and two generators, each on its own tap device, plus the coordinator that starts and drives the generators. The packets go through the Linux kernel, so use this run to check the tools and learn them, and use the [AWS run](aws.md) for numbers.

## The smoke run

1. Build the Docker image once, from the repository root:

   ```sh
   make -C conduit docker-image
   ```

2. Start the run:

   ```sh
   make -C conduit docker-bench-smoke
   ```

   The default CPU range is 9-12, so Docker's VM needs 13 CPUs. On a smaller VM give it another range of at least four CPUs, written `first-last`:

   ```sh
   make -C conduit docker-bench-smoke BENCH_CPUSET=3-6
   ```

   The target refuses to start if a running container shares those CPUs, or runs without a CPU limit, and prints `docker-bench-smoke needs BENCH_CPUSET (3-6) free of other containers.`

3. Watch the output. The container builds `switch-bench` in release mode, then sweeps UDP, TCP and WebSocket in turn. After each sweep it prints `smoke:`, the protocol and that sweep's summary line, for example:

   ```text
   smoke: tcp: {"kind":"summary","steps":17,"complete":true,"generator_bound":false,"behind_workers":[],"error":null}
   ```

   Then it runs an overload check: a TCP sweep against a kernel listener that accepts connections and never reads. That step must end on time with every message lost and a complete summary:

   ```text
   smoke: bounded overload: <seconds>s, drop 1.0, sent <n> of 64000 pps, summary complete=True
   ```

   Last, it writes the report and prints `wrote ../switch-bench/out/smoke/report.md`.

4. Open `switch-bench/out/smoke/report.md`. [Reading a benchmark report](reports.md) explains it.

Over tap devices a generator often falls behind schedule at the higher rates. The smoke run accepts a sweep that stopped that way and goes on with the next protocol, but the report counts the sweep as incomplete, prints a line such as

```text
incomplete: udp-switch: sender behind schedule: worker(s) [0, 1]
```

and the make target fails. Read the report to see where each sweep stopped. Any other failure stops the run at once with a `smoke: <proto> FAILED: ...` line, or `smoke: bounded overload: the sweep against a sink failed: ...`.

The run writes everything into `switch-bench/out/smoke/`:

| File | What |
|---|---|
| `udp-switch.jsonl`, `tcp-switch.jsonl`, `ws-switch.jsonl` | Each sweep's step lines and summary line |
| `overload-check.jsonl` | The overload check's sweep |
| `report.md`, `results.json`, `<sweep>.svg` | The [report](reports.md) |
| `dut.log`, `sweep.log`, `gen<i>.log` | The stderr of the echo server, the coordinator and each generator |

These make variables change the run:

| Variable | Default | Meaning |
|---|---|---|
| `BENCH_CPUSET` | `9-12` | The container's CPUs, a range of at least 2 + `BENCH_GENS` |
| `BENCH_GENS` | `2` | Generator processes |
| `BENCH_SENDERS` | `4` | Senders across all generators, a multiple of `BENCH_GENS` |
| `BENCH_PROTOS` | `udp tcp ws` | Protocols to sweep |
| `BENCH_SMOKE_ARGS` | `--rate-min 1000 --rate-max 256000 --step-ms 300 --warmup-ms 100 --drain-ms 100` | Flags passed to every `sweep` |

The TCP and WebSocket sweeps also get `--budget-us 50`.

## What the smoke run starts

The script behind the target is `switch-bench/local/smoke.sh`. It gives each switch process a tap device on its own subnet, with the kernel side of the tap as its gateway, and lets the kernel route between them:

```text
  CPU first     CPU first+1          CPU first+2          CPU first+3
  coordinator   echo server          generator 0          generator 1
                10.10.0.2            10.20.0.2            10.21.0.2
                    |                    |                    |
                tap cddut            tap cdgen0           tap cdgen1
                    |                    |                    |
                10.10.0.1 ------- Linux kernel, routing ------ 10.20.0.1, 10.21.0.1
```

For each protocol it starts the echo server on the second CPU of the range:

```sh
taskset -c $((first + 1)) switch-bench serve --config dut.json --proto tcp --port 7000
```

The server prints `switch-bench serve: tcp echo on port 7000` on stderr when it is ready. Then the coordinator runs on the first CPU:

```sh
taskset -c $first switch-bench sweep --proto tcp --gens 2 --senders 4 \
    --gen-cmd "sh gen.sh {i} tcp 10.10.0.2:7000 {senders} 64" \
    --rate-min 1000 --rate-max 256000 --step-ms 300 --warmup-ms 100 --drain-ms 100 \
    --budget-us 50 > tcp-switch.jsonl
```

The coordinator runs the `--gen-cmd` command with `sh -c` once per generator, with `{i}` replaced by the generator's number (0 to G-1) and `{senders}` by its share of the senders. The script's `gen.sh` pins generator `i` to CPU `first + 2 + i` and starts it:

```sh
taskset -c $((first + 2 + i)) switch-bench gen --config gen$i.json --proto tcp \
    --target 10.10.0.2:7000 --senders 2 --size 64 --local-port 40000
```

The coordinator reports its progress on stderr (`processes up`, `processes synced`, `sweep start`, `sweep end`). Each process reads its own [network config file](../operations/config.md), which the script writes for its tap device.

## Sharing a card through the daemon

With `--backend junction`, `serve` and `gen` share a card through a running [`switch-junctiond`](../operations/junctiond.md) and take `--junction-socket` in place of `--config`. Start the daemon with its network thread on one CPU and its control thread elsewhere, then the echo server on a third CPU:

```sh
taskset -c 2 switch-junctiond --config jn0.json --dir /run/switch-junctiond --control-cpus 0 &
taskset -c 3 switch-bench serve --backend junction \
    --junction-socket /run/switch-junctiond/control.sock --proto udp --port 9000
```

The generators can own their cards or share one, whatever the server does.

`switch-bench/local/junction-smoke.sh` checks this path end to end. It needs a container that may use two CPUs, 0 and 2 by default (set `JUNCTION_CPU` and `APP_CPU` to change them). From the repository root:

```sh
docker run --rm --init --cpuset-cpus=0,2 --cap-add NET_ADMIN --device /dev/net/tun \
    -v "$PWD:/work" -w /work -v switch-cargo-registry:/usr/local/cargo/registry \
    -v switch-linux-target:/work/target switch-dpdk-test:local \
    sh -c 'cargo build --locked -p switch-junctiond -p switch-bench --bins && sh switch-bench/local/junction-smoke.sh'
```

It starts the daemon on a tap device and checks `serve` and `gen` with `--backend junction`, printing one line per check, such as `serve --backend junction udp: 16/16 echoes`.

## Command reference

Every flag is written `--name value`. An invalid argument, or an error the command returns while running, prints `switch-bench: <error>` followed by the usage text and exits with status 1. A panic exits with status 101; a generator panics, for example, when the server refuses one of its TCP connections or closes it while connecting, or when a WebSocket upgrade fails.

```text
switch-bench serve [--backend conduit --config F | --backend junction --junction-socket PATH]
                   --proto udp|tcp|ws --port P
```

The echo server. `--backend conduit`, the default, owns the network card named in `--config`; `--backend junction` shares one through the daemon. For TCP and WebSocket it has room for 320 sockets. It runs until SIGINT or SIGTERM.

```text
switch-bench gen [--backend conduit --config F | --backend junction --junction-socket PATH]
                 --proto udp|tcp|ws --target IP:PORT --senders N --size S [--local-port 40000]
```

One generator. The coordinator starts it and drives it over its standard input and output; you do not run it by hand. `--target` is an IPv4 address and port. `--size` is the message size in bytes, at least 24; WebSocket messages are at most 1400. A TCP or UDP size larger than one packet buffer is not rejected, and the generator panics when it sends. Sender i uses local port `--local-port` + i.

```text
switch-bench sweep --proto udp|tcp|ws --gens G --gen-cmd CMD --senders N
                   (--rate-min PPS --rate-max PPS | --rates PPS,PPS,...)
                   [--step-ms 2000] [--warmup-ms 500] [--drain-ms 200] [--budget-us W]
```

The coordinator. It does not open a network card.

| Flag | Meaning |
|---|---|
| `--gens G` | Generator processes to start |
| `--gen-cmd CMD` | The command that starts generator `{i}` with `{senders}` senders |
| `--senders N` | Senders across all generators; a positive multiple of `--gens` |
| `--rate-min`, `--rate-max` | Rates from `rate-min` up to `rate-max`, each √2 times the last, in messages per second |
| `--rates` | An explicit, strictly increasing list instead |
| `--step-ms` | The measurement window of each step |
| `--warmup-ms` | The warmup before the window |
| `--drain-ms` | The time after the window during which echoes still count |
| `--budget-us W` | TCP and WebSocket only: the [in-flight budget](README.md#the-in-flight-budget) |

It exits 0 only when every step ran and no generator fell behind schedule. The sweep fails when a generator exits, when its clock is more than about 1 ms off the coordinator's, or when it does not reply in time. The summary line then says `"complete": false` with the error, and the steps already printed stay valid.

```text
switch-bench kernel-serve --proto udp|tcp|ws --bind IP --port P
```

An echo server on Linux kernel sockets, for comparison with `serve`. It exists only on Linux and runs until SIGINT or SIGTERM. It prints `switch-bench kernel-serve: tcp echo on IP:P` when ready.
