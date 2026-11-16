# The pinned CPU and helper threads

A program that owns a NIC runs its drive loop on one CPU and keeps that CPU busy all the time. [Opening the NIC from code](open.md) and [Packet-buffer pools](pools.md) covered what `open` needs as arguments; this page covers what it needs from the machine: the rule `open` enforces about CPUs, why [Preparing a Linux machine](host-setup.md) isolated a CPU, and where DPDK's own threads end up.

## Why one CPU

The drive loop never waits for the kernel to say a packet has arrived. It asks the card again and again, a style called busy polling. That gives the lowest delay between a packet arriving and your code seeing it, at the price of one CPU running at 100% whether or not traffic flows. A thread that polls like this should never be moved to another CPU by the scheduler, and nothing else should be scheduled on its CPU.

`switch_open::open` makes the first half of that a requirement. Before it does anything else, it asks Linux which CPUs the calling thread may run on. If the answer is not exactly one CPU, it fails with `NotPinned`:

```text
CPU affinity: the calling thread may run on 8 CPUs, not exactly one; restrict it to the CPU for EAL (taskset -c N, docker --cpuset-cpus=N)
```

If it is one CPU, that CPU becomes the pinned CPU for the life of the Stack. `open` tells DPDK to put its main thread there (`--lcores=0@<cpu>`; DPDK calls a thread bound to a CPU an lcore). The thread that called `open` is the drive-loop thread: it owns the Stack and must stay on that CPU.

## Pinning a process

Restrict the process before it starts, and every thread it creates inherits the restriction:

```sh
taskset -c 3 ./my-program dut.json
```

In Docker, give the container one CPU:

```sh
docker run --cpuset-cpus=3 ... my-image ./my-program dut.json
```

Pick the CPU with care on a real machine. It should be on the same NUMA node as the data NIC (the `numa_node` key in the [config file](config.md) tells you which), and it should not be CPU 0, where Linux does much of its own work.

## Isolated and housekeeping CPUs

Pinning stops your thread from moving. It does not stop Linux from putting other work on the same CPU: other processes, interrupt handlers, kernel housekeeping. Each such interruption is a pause in your drive loop. To keep the pinned CPU to yourself, isolate it at boot with kernel options; [Preparing a Linux machine](host-setup.md#3-choosing-and-isolating-a-cpu) gives the boot line.

An isolated CPU is one the kernel keeps other work off. Housekeeping CPUs are the rest: they run the operating system, take interrupts and host helper threads. A machine set up this way needs at least one of each. On a machine with two hardware threads per core, isolate the pinned CPU's sibling thread as well, so nothing shares the physical core; the EC2 script does.

The repository's EC2 script ([Running on EC2](ec2.md)) chooses like this: the application CPU is the highest-numbered CPU on the data NIC's NUMA node; the CPU for the [junction daemon](../junction/index.md) is the highest CPU on a different physical core of that node; both, with their sibling threads, are isolated; every other CPU is housekeeping, and the script refuses to continue if none would be left. [Preparing a Linux machine](host-setup.md) does the same by hand.

## Where DPDK's helper threads go

When DPDK starts, it creates a few helper threads of its own for interrupts and control work. They are not busy, but they wake up now and then, and you want them off the pinned CPU. DPDK places them on the CPUs the starting thread is allowed to use at that moment, avoiding the CPU of its main thread when others are available. Since your thread is allowed only on the pinned CPU, `open` briefly widens the thread's allowed set while DPDK starts, then narrows it again:

```text
 thread affinity during open()

 before open      {3}                    one CPU: the pinned CPU
 EAL starting     {3} + helper CPUs      helpers start on the extra CPUs;
                                          DPDK's main thread is placed on 3
 after open       {3}                    restored, also if EAL failed
```

What "helper CPUs" means depends on the `helper_cpus` argument.

With `None`, `open` looks at `/sys/devices/system/cpu/isolated`. If the pinned CPU is listed there, it adds every online CPU that is not isolated (Linux then trims that to what the container's cpuset allows). The helpers land on housekeeping CPUs. If the pinned CPU is not isolated, or the file does not exist, DPDK starts with the thread as it is, and the helpers share the pinned CPU. They also share it when no housekeeping CPU is available, for example in a container limited to one CPU.

With `Some(&[cpus])`, Linux only, you name the helper CPUs yourself. This is for machines where the pinned CPU is not isolated at boot but you still want the helpers elsewhere, as in Docker with a cpuset of several CPUs. The list must not be empty. `open` widens the thread to the pinned CPU plus that list, then reads the result back: if the container's cpuset dropped any of the CPUs you named, it restores the single CPU and fails with `HelperAffinity` (`EAL helper CPUs are outside the effective cpuset`). A list that contains only the pinned CPU behaves like `None`. On any other operating system, `Some` fails with `HelperAffinity` (`explicit EAL helper CPUs require Linux`).

The [junction daemon](../junction/running.md) uses `Some`: its `--control-cpus` flag is passed as the helper list.

In every case the thread's affinity is set back to the single pinned CPU before `open` continues, including when DPDK's setup fails. If DPDK fails and restoring the affinity fails too, `open` reports both errors as `EalAffinityRestore`.

## In containers

The repository's test containers run on one CPU. Inside such a container `open` finds exactly one allowed CPU and uses it, and DPDK's helpers share it. That is fine for tests and for trying things.

To keep the helpers off the pinned CPU in a container, give the container the pinned CPU plus one or more housekeeping CPUs, start the program under `taskset -c` on the pinned CPU, and pass the other CPUs as `helper_cpus`. The junction daemon works this way: it passes the CPUs from its `--control-cpus` flag to `open` as `Some(&helper_cpus)`.

Next: [Offloads and hardware timestamps](offloads.md).
