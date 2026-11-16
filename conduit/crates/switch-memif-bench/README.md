# switch-memif-bench

A benchmark of the network stack alone: two Stacks in two processes, joined by DPDK's shared-memory memif device, exchanging TCP and UDP traffic. It is a contributor tool; applications do not use it.

How to run it and read its output is in [docs/benchmarks/memif.md](../../../docs/benchmarks/memif.md).

```sh
make -C conduit test-memif                 # tests, plus the cpuset-overlap self-test; needs DPDK
make -C conduit docker-switch-memif-bench  # the matrix in a container on MEMIF_CPUSET (default 3-5)
```
