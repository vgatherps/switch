# CPUs

A program that owns a card runs its loop on one CPU and keeps that CPU busy all the time. It never waits for the kernel to report a packet; it asks the card again and again. That gives the shortest delay between a packet arriving and the program seeing it, and costs one CPU at 100% whether traffic flows or not.

## Pin the process to one CPU

Opening the card fails unless the calling thread is allowed on exactly one CPU:

```text
CPU affinity: the calling thread may run on 4 CPUs, not exactly one; restrict it to the CPU for EAL (taskset -c N, docker --cpuset-cpus=N)
```

Restrict the process before it starts; every thread it creates inherits the restriction:

```sh
taskset -c 3 ./my-program dut.json
```

or give a container one CPU with `docker run --cpuset-cpus=3 ...`. On a real machine, choose a CPU on the card's NUMA node (the config's `numa_node`), and not CPU 0, where Linux does much of its own work.

`switch-junctiond` has the same rule for its network thread: start it under `taskset -c N` ([switch-junctiond](junctiond.md#cpus)).

## Isolate it

Pinning keeps the program on its CPU. It does not keep other work off that CPU: other processes, interrupts and kernel housekeeping each pause the program's loop while they run. To keep the CPU to the program, isolate it with kernel boot options ([Preparing a Linux machine](host-setup.md#3-isolate-a-cpu) gives them). An isolated CPU is one the kernel keeps other work off; the rest are housekeeping CPUs, which run Linux, its interrupts and everything else. A machine set up this way needs at least one of each. On a machine with two hardware threads per core, isolate the pinned CPU's sibling too, so nothing shares the physical core.

`cat /sys/devices/system/cpu/isolated` lists the isolated CPUs.

## Where DPDK's helper threads run

When the card opens, DPDK starts a few threads of its own for interrupts and control work. They are mostly idle, but they wake now and then, and they should not run on the pinned CPU.

If the pinned CPU is isolated, the helpers go to the housekeeping CPUs that the process may use. If it is not isolated, they share the pinned CPU. Inside a container limited to one CPU they share it too, which is fine for tests and trying things.

To keep the helpers off the pinned CPU without isolating it, the program names other CPUs for them when it opens the card (the `helper_cpus` argument of `switch::nic::open`, in the API reference, `make docs`). `switch-junctiond` does this with its `--control-cpus` flag. The named CPUs must be available to the process, for example inside the container's `--cpuset-cpus`; otherwise opening fails with `EAL helper CPU affinity: EAL helper CPUs are outside the effective cpuset`.

## On EC2

`deploy/hft-host.sh` isolates two physical cores on the data ENI's NUMA node: `APP_CPU` for the program and `JUNCTION_CPU` for `switch-junctiond`. Every other CPU is housekeeping. [Running on EC2](ec2.md#which-cpu-to-use) shows how to read them.
