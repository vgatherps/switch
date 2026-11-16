# Stack-to-stack throughput with switch-memif-bench

The earlier measuring pages, from [Measuring latency under load](index.md) to [Benchmarking on AWS](aws.md), time a whole system: generators, a NIC or tap device, the kernel or the daemon, and the program under test. `switch-memif-bench` measures something narrower. It connects two Stacks to each other with no NIC and no kernel in between and times how fast they exchange TCP and UDP traffic. The numbers describe the cost of the Stack itself and of the [drive loop](../stack/drive-loop.md) around it.

## What it measures

Each Stack runs in its own process, with its own EAL, on its own CPU. The link between them is DPDK's memif device (`net_memif`): a virtual NIC whose two ends exchange packets through shared memory and find each other through a Unix socket. One process is the memif server, the peer; the other is the memif client, which drives the traffic and measures it.

```text
            switch-memif-bench run
              (launcher; reads stdout, writes stdin)
              |                               |
   taskset -c X,P                    taskset -c X,C
              |                               |
 +------------v-------------+   +-------------v------------+
 | peer process             |   | client process           |
 |  EAL main thread on P    |   |  EAL main thread on C    |
 |  Stack  echo | sink      |   |  Stack  measures         |
 |  memif server  <---------+---+-> memif client           |
 +--------------------------+   +--------------------------+
   DPDK helper threads of both processes run on the control CPU X
```

The memif device copies every packet into and out of shared memory on both sides. Frame counts in the reports include that copy.

## A single run

`run` launches both processes, waits for them, and prints both reports. It needs three CPUs, all in the launcher's allowed set: one for the peer (`--peer-cpu`), one for the client (`--client-cpu`), and one for both processes' DPDK helper threads (`--control-cpu`). In a container, that means a cpuset that includes all three.

In the DPDK environment from [Setting up a build environment](../getting-started/environment.md), build the binary from the repository root, then start a ping-pong run with one connection and 64-byte messages:

```text
cargo build --locked --release -p switch-memif-bench
target/release/switch-memif-bench run --shape pingpong --conns 1 --message 64 \
    --rounds 100000 --warmup 10000 --peer-cpu 4 --client-cpu 5 --control-cpu 3
```

The shape says what traffic to send:

| Shape | Flags | What happens |
|---|---|---|
| `pingpong` | `--conns K --message M --rounds N --warmup W` | K TCP connections. Each writes an M-byte message and waits for all of it to come back from the peer (in echo mode). W untimed rounds, then N timed ones. K is 1 to 25536; M is 8 bytes to 512 KiB. |
| `bulk` | `--bytes B --warmup W` | One TCP connection writes W untimed bytes, then B measured bytes, into a peer that discards them (sink mode). |
| `udp` | `--size N --count N --warmup N` | One-way datagrams of N bytes (8 to 1472) into a UDP sink: the warmup, then the measured count. |

The UDP shape counts loss as datagrams sent minus datagrams delivered. Lost datagrams are never retried. The sink keeps receiving until the client has finished sending and no datagram has arrived for 50 ms.

A bulk run's elapsed time can include one delayed ACK of the last segment, up to 10 ms.

`--run-timeout-s S` bounds each process's run (1 to 3600 seconds, default 120). `--socket NAME` names the memif Unix socket; `run` picks `switch-memif-bench-<pid>` if you leave it out. A name must be at most 106 bytes, must not contain `,` or `=`, and must never be reused within one process.

`run` prints each report on its own line as `{"role":"client","report":{...}}` and `{"role":"peer","report":{...}}`. Its exit status is 0 only if both reports are valid and both ports closed.

### Starting the two sides yourself

You can start the two sides by hand with `peer --mode echo|sink|udp-sink ... --cpu C --socket NAME` and `client SHAPE --cpu C --socket NAME`, but `run` orders their start and shutdown for you. If you do, speak the launcher's protocol: lines on stdin and stdout. The peer prints `ready` once its memif server is listening; only then start the client. Each side prints `report <json>` when it has finished and stops polling. For UDP, write `sent` to the peer after the client's report, so the peer knows the client has stopped sending. Each side closes its NIC port only after reading the line `close`; send it to the client first, wait for the client to exit, and then send it to the peer.

## The matrix

`matrix` runs a fixed set of configurations through `run`, several times:

| Configuration | Shape |
|---|---|
| `pingpong-m64` | ping-pong, 1 connection, 64-byte messages, 100 000 rounds after 10 000 warmup |
| `pingpong-m1448` | ping-pong, 1 connection, 1448-byte messages, 100 000 rounds after 10 000 warmup |
| `parallel-k4` | ping-pong, 4 connections, 64-byte messages, 20 000 rounds after 2 000 warmup |
| `parallel-k16` | as above with 16 connections |
| `parallel-k64` | as above with 64 connections |
| `bulk-1g` | bulk, 1 GiB after 64 MiB warmup |

`--reps R` (default 3) sets how many repetitions over the list; they alternate forward and reverse order. `--dry-run` prints the run order and stops. Every run's outcome and both its reports go to a new file, `target/switch-memif-bench/matrix-<unix seconds>-<pid>.jsonl` under the directory you run it from, one JSON object per line. The matrix stops at the first run that is not valid, after recording it.

The make targets live in `conduit/Makefile`. They run the matrix with CPUs 3 (control), 4 (peer) and 5 (client), set by `MEMIF_CONTROL_CPU`, `MEMIF_PEER_CPU` and `MEMIF_CLIENT_CPU`, and write the results under `conduit/target/switch-memif-bench/`:

```text
make -C conduit switch-memif-bench          # release build, then the matrix, on Linux with DPDK 25.11.3
make -C conduit docker-switch-memif-bench   # the same in a container pinned to MEMIF_CPUSET (default 3-5)
make -C conduit test-memif                  # the crate's tests
```

`docker-switch-memif-bench` refuses to start if another running container shares those CPUs.

## Reading the table

After all runs are valid, `matrix` prints one table per configuration with the minimum, median and maximum over its runs:

| Row | Meaning |
|---|---|
| `ns/round trip` | ping-pong: measured time divided by round trips |
| `rtt p50 ns`, `rtt p99 ns`, `rtt max ns` | ping-pong: distribution of single round-trip times |
| `turns/round trip` | ping-pong: client drive-loop turns per round trip |
| `ns/frame` | time per frame moved |
| `frames/s` | frames moved per second |
| `client idle turns` | fraction of the client's turns that moved nothing, in the measured window |
| `peer idle turns (whole run)` | the same for the peer, over its whole run |

Rows that do not apply to a shape (round-trip rows for `bulk-1g`) are left out. Frame counts include memif's copy on both sides.

This is the last page on measuring. Next: [running the test suite](../testing/suite.md).
