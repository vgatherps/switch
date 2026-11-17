# CPUs

A program that owns a card runs on one CPU, and a busy-polling loop uses that CPU at 100%.

## Pin the process to one CPU

Opening the card fails unless the calling thread's CPU affinity mask contains exactly one CPU:

```text
CPU affinity: the calling thread may run on 4 CPUs, not exactly one; restrict it to the CPU for EAL (taskset -c N, docker --cpuset-cpus=N)
```

Set the affinity before the process starts; every thread it creates inherits it:

```sh
taskset -c 3 ./my-program /etc/switch/dut.json
```

In a container started with one CPU (`docker run --cpuset-cpus=3 ...`), every process already has that affinity, and `taskset` is not needed. On a real machine, choose a CPU on the card's NUMA node (the config's `numa_node`), and not CPU 0.

`switch-junctiond` has the same rule for its network thread: start it under `taskset -c N` ([switch-junctiond](junctiond.md#cpus)).

## Isolate it

Pinning does not keep other processes, interrupts and kernel housekeeping off the CPU. Isolate it with kernel boot options ([Preparing a Linux machine](host-setup.md#3-isolate-a-cpu) gives them), leaving at least one housekeeping CPU. On a machine with two hardware threads per core, isolate the pinned CPU's sibling too.

`cat /sys/devices/system/cpu/isolated` lists the isolated CPUs.

## Where DPDK's helper threads run

When the card opens, DPDK starts a few mostly idle threads of its own for interrupts and control work, which should not run on the pinned CPU.

If the pinned CPU is isolated, the helpers go to the housekeeping CPUs that the process may use. If it is not isolated, or inside a container limited to one CPU, they share the pinned CPU.

To keep the helpers off the pinned CPU without isolating it, the program names other CPUs for them when it opens the card (the `helper_cpus` argument of `switch::nic::open`, in the API reference, `make docs`). `switch-junctiond` does this with its `--control-cpus` flag. The named CPUs must be available to the process, for example inside the container's `--cpuset-cpus`; otherwise opening fails with `EAL helper CPU affinity: EAL helper CPUs are outside the effective cpuset`.

## On EC2

`deploy/hft-host.sh` isolates two physical cores on the data ENI's NUMA node: `APP_CPU` for the program and `JUNCTION_CPU` for `switch-junctiond`. Every other CPU is housekeeping. [Running on EC2](ec2.md#which-cpu-to-use) shows how to read them.
