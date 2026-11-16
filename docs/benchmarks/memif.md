# Stack-to-stack throughput

`switch-memif-bench` times the network stack alone. It runs two stacks in two processes and joins them with DPDK's memif device, a virtual network card whose two ends exchange packets through shared memory. There is no physical card and no kernel in the path, so the numbers are the cost of the stack and the loop that drives it, plus memif's copy of every packet into and out of shared memory on both sides.

One process is the peer, which echoes or discards what it receives. The other is the client, which sends the traffic and measures it. Each runs on its own CPU, and DPDK's helper threads of both processes share a third.

## Running the matrix

The matrix runs a fixed list of configurations several times and prints a table for each.

1. Build the Docker image once, from the repository root:

   ```sh
   make -C conduit docker-image
   ```

2. Run the matrix in a container on CPUs 3 to 5:

   ```sh
   make -C conduit docker-switch-memif-bench
   ```

   It uses CPU 3 for the helper threads, 4 for the peer and 5 for the client (`MEMIF_CONTROL_CPU`, `MEMIF_PEER_CPU`, `MEMIF_CLIENT_CPU`; the container's CPUs are `MEMIF_CPUSET`, default `3-5`). It refuses to start if another running container shares those CPUs.

   On Linux with DPDK 25.11.3 installed, `make -C conduit switch-memif-bench` runs the same matrix without Docker.

3. Read the output. The matrix logs `switch-memif-bench: matrix rep <n> <configuration>` on stderr before each run. When every run is valid it prints `results: <path>`, the file holding every run's reports, and then one table per configuration.

The configurations are:

| Configuration | Traffic |
|---|---|
| `pingpong-m64` | 1 TCP connection, 64-byte messages echoed, 100,000 rounds after 10,000 warmup rounds |
| `pingpong-m1448` | the same with 1448-byte messages |
| `parallel-k4` | 4 connections, 64-byte messages, 20,000 rounds after 2,000 warmup rounds |
| `parallel-k16` | the same with 16 connections |
| `parallel-k64` | the same with 64 connections |
| `bulk-1g` | 1 connection writing 1 GiB, after a 64 MiB warmup, into a peer that discards it |

`--reps R` (default 3) sets how many times the list runs, alternately forward and in reverse. The matrix stops at the first run that is not valid, after recording it in the results file.

The results file is `target/switch-memif-bench/matrix-<unix seconds>-<pid>.jsonl` under the directory the command runs in, which is `conduit/`. Under `docker-switch-memif-bench` that directory is in the Docker volume `switch-linux-target`, not on your disk; the tables on standard output are the summary.

## Reading the table

Each table gives the minimum, median and maximum over the configuration's runs:

| Row | Meaning |
|---|---|
| `ns/round trip` | Ping-pong: measured time divided by round trips |
| `rtt p50 ns`, `rtt p99 ns`, `rtt max ns` | Ping-pong: single round-trip times |
| `turns/round trip` | Ping-pong: times round the client's loop per round trip |
| `ns/frame` | Time per frame moved |
| `frames/s` | Frames moved per second |
| `client idle turns` | The share of the client's times round its loop that moved nothing, in the measured window |
| `peer idle turns (whole run)` | The same for the peer, over its whole run |

`bulk-1g` has no round-trip rows.

## A single run

`run` starts both processes, waits for them and prints both reports, one JSON line each (`{"role":"client","report":{...}}`, then `{"role":"peer","report":{...}}`). All three CPUs must be in the set the command may run on, so use a container started with `--cpuset-cpus=3-5`, or a Linux machine with DPDK. From `conduit/`:

```sh
cargo build --locked --release -p switch-memif-bench
../target/release/switch-memif-bench run --shape pingpong --conns 1 --message 64 \
    --rounds 100000 --warmup 10000 --peer-cpu 4 --client-cpu 5 --control-cpu 3
```

The shapes are:

| Shape | Flags | Traffic |
|---|---|---|
| `pingpong` | `--conns K --message M --rounds N --warmup W` | K TCP connections (1 to 25536), each writing an M-byte message (8 bytes to 512 KiB) and waiting for all of it to come back; W untimed rounds, then N timed ones |
| `bulk` | `--bytes B --warmup W` | One TCP connection writing W untimed bytes, then B measured bytes, into a peer that discards them |
| `udp` | `--size N --count N --warmup N` | One-way datagrams of 8 to 1472 bytes into a peer that discards them: the warmup, then the measured count. Loss is datagrams sent minus datagrams delivered; lost datagrams are not resent |

`--run-timeout-s S` bounds each process's run (1 to 3600 seconds, default 120). `run` exits 0 only if both reports are valid and both processes exited 0.
