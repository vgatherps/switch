# memif-bench

Throughput and latency benchmarks between two zhida `Stack`s connected by a DPDK net_memif link, each in its own process with its own EAL and CPU.

Depends on [`dpdk-smoltcp`](../dpdk-smoltcp/README.md), `zhida-buffer` and `zhida-dpdk-sys`. Nothing depends on it.

## Contents

- Library: `memif` (`Memif`, `MemifVdev`, `Role`, `MemifError`: a Port on a net_memif vdev), `workload` (`Shape`, `PeerMode`, `Client`, `Peer`, `ClientReport`, `PeerReport`, `port_config`), `matrix` (`CONFIGS`, the six benchmark configurations, plus `plan`, `execute`, `table`).
- Binary `memif-bench` with four subcommands:
  - `peer --mode echo|sink|udp-sink ...`: the memif server.
  - `client SHAPE ...`: the measuring memif client.
  - `run SHAPE --peer-cpu P --client-cpu C --control-cpu X`: launches both and orders their shutdown.
  - `matrix [--reps R] [--dry-run] ...`: runs every configuration through `run`, forward and reverse alternately, writes both reports of each run to `target/memif-bench/matrix-<unix seconds>-<pid>.jsonl`, and prints min, median and max per configuration.
  - `SHAPE` is `--shape pingpong`, `bulk` or `udp`. UDP is command-line only, and lost datagrams are counted as sent minus delivered, never retried. Run `memif-bench` with bad arguments for the full usage.

The process-to-process protocol is lines on stdout and stdin (`ready`, `report <json>`, `close`, `sent`); see the doc comment at the top of `src/main.rs`.

## Build and run

Needs DPDK 25.11.3 (it builds net_memif), so use the Docker image on macOS. The three CPUs must be in the process's allowed set.

```sh
make test-memif           # cargo test -p memif-bench, plus the cpuset-overlap self-test
make memif-bench          # release build, then `memif-bench matrix` (MEMIF_PEER_CPU, MEMIF_CLIENT_CPU, MEMIF_CONTROL_CPU)
make docker-memif-bench   # same in a container pinned to MEMIF_CPUSET (default 3-5)
```

`docker-memif-bench` refuses to start if another running container shares those CPUs. `docker/README.md` lists the net_memif workarounds zhida applies for DPDK 25.11.3.
