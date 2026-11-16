# Preparing a Linux machine

The earlier pages in this part, up to [Offloads and hardware timestamps](offloads.md), described the config file and what `open` does with it. A tap device needs no machine preparation; a real NIC does. This page takes a Linux machine that has a spare NIC to the point where `switch-nic info` opens that NIC and reports on it. It works for a lab machine with an Intel NIC (E810, X710, 82599 and their relatives); [Running on EC2](ec2.md) covers what differs on Amazon's ENA NICs and the script that does all of it there.

## The steps at a glance

```text
 bare Linux machine with a spare NIC
   |
   |  1. keep a second interface for SSH (management NIC)        root, once
   |  2. reserve hugepages on the NIC's NUMA node                 root, each boot
   |  3. choose a CPU and keep the kernel off it                  root, boot options
   |  4. IOMMU on, or plan for --noiommu                          root, boot options
   v
 machine ready
   |
   |  5. build switch-netconf and switch-nic                      no root
   |  6. switch-netconf discover, check                           no root, no DPDK
   |     switch-netconf bind                                      root
   v
 NIC on vfio-pci, config file written
   |
   |  7. switch-nic --config dut.json info                        DPDK
   v
 ready to run
   |
   |  8. switch-netconf unbind (when you are done)                root
```

## 1. Two interfaces

When your program owns a NIC, Linux no longer sees it: the interface disappears from `ip link` and nothing the kernel does (SSH, DNS, package updates) can use it. The machine therefore needs two interfaces. The [management NIC](index.md) stays with Linux and carries SSH and the default route. The [data NIC](index.md) is the one you hand to switch.

Pick the data NIC now and note its interface name, for example `ens5f0`. If the two ports are on one machine and you want them to talk to each other for a test, put the peer port in its own network namespace (`ip netns add peer; ip link set <if> netns peer`); otherwise the kernel routes the traffic locally and it never reaches the wire.

## 2. Hugepages on the NIC's NUMA node

[Hugepages](index.md) are large memory pages the machine sets aside for DPDK's packet buffers. They have to be reserved by the host before a DPDK program starts; the program cannot create them. On a machine with several NUMA nodes (groups of CPUs with their own memory), reserve them on the node the NIC is attached to. The node number is in `/sys/class/net/<if>/device/numa_node`, and `switch-netconf discover` records it in the config file too.

Reserve 1 GiB of 2 MiB pages on node `N` and mount the hugepage filesystem:

```sh
echo 512 > /sys/devices/system/node/node<N>/hugepages/hugepages-2048kB/nr_hugepages
mkdir -p /dev/hugepages && { mountpoint -q /dev/hugepages || mount -t hugetlbfs nodev /dev/hugepages; }
```

The `echo` lasts until the next reboot. To make it permanent, put the reservation on the kernel command line instead, as the EC2 script does: `hugepagesz=2M hugepages=<N>:512`.

Tests and tap devices do not use hugepages. Their configs pass `--no-huge` to DPDK, which then uses ordinary memory, and that is why the container on [Setting up a build environment](../getting-started/environment.md) needs no hugepages.

## 3. Choosing and isolating a CPU

The Stack busy-polls its [pinned CPU](cpus.md) at 100%. Pick one CPU on the NIC's NUMA node, and not CPU 0, where the kernel does much of its own housekeeping work.

For steady latency, keep everything else off that CPU with kernel boot options. These make it an [isolated CPU](cpus.md); the rest become [housekeeping CPUs](cpus.md). The EC2 setup script writes this set, with `<iso>` the isolated CPU list and `<housekeeping>` the rest:

```text
isolcpus=managed_irq,domain,<iso> nohz_full=<iso> rcu_nocbs=<iso> irqaffinity=<housekeeping>
```

`isolcpus` keeps the scheduler from placing other tasks there, `nohz_full` and `rcu_nocbs` stop most periodic kernel work on it, and `irqaffinity` sends interrupts to the housekeeping CPUs. Add them through your distribution's boot loader tool (on Fedora-family systems, `grubby --update-kernel=ALL --args="..."`) and reboot. After the reboot, `/sys/devices/system/cpu/isolated` lists the isolated CPUs. `switch_open::open` reads that file to decide where DPDK's own [helper threads](cpus.md) go.

The EC2 script also turns off `irqbalance` and moves every movable interrupt to the housekeeping CPUs at each boot; on a lab machine you do the same with `systemctl disable --now irqbalance` and the `/proc/irq/*/smp_affinity_list` files.

## 4. IOMMU or no-IOMMU

To hand a PCI NIC to a userspace program, Linux detaches it from its kernel driver and attaches it to `vfio-pci`, a kernel driver that lets a process own a device. An IOMMU (Intel VT-d, AMD-Vi) confines what the device can reach in memory and makes that safe. Turn it on in the firmware and, on Intel, with `intel_iommu=on` on the kernel command line.

Machines without an IOMMU, such as most virtual machines, can still bind the NIC in vfio-pci's no-IOMMU mode. `switch-netconf bind --noiommu` does that. It taints the kernel, and `bind` refuses to proceed on such a machine unless you pass the flag.

## 5. Build the tools

`switch-netconf` writes and checks the [network config file](config.md) and moves the NIC between drivers. It needs no DPDK and builds directly on the host:

```sh
cargo build --locked --release -p switch-netconf
```

The binary is `target/release/switch-netconf`; the commands below assume it is on your `PATH`.

`switch-nic` opens the NIC and needs DPDK. Build it natively (see [A Linux machine without Docker](../getting-started/environment.md#a-linux-machine-without-docker)) with `cargo build --locked --release -p switch-nic`, or in the Docker image. For a real NIC the container needs more than the tap-only container on the setup page: the hugepage mount, the VFIO devices and the host's network namespace. This shell function, run from `conduit/`, builds `switch-nic` in the image and runs it pinned to one CPU:

```sh
nic() {
  docker run --rm -it --privileged --network host \
    --cpuset-cpus="${SWITCH_CPU:-2}" \
    -v /dev/hugepages:/dev/hugepages -v /dev/vfio:/dev/vfio -v /lib/firmware:/lib/firmware:ro \
    -v "$PWD/..:/work" -w /work/conduit \
    -v switch-cargo-registry:/usr/local/cargo/registry -v switch-nic-target:/work/target \
    switch-dpdk-test:local sh -c 'cargo build --locked --release -p switch-nic &&
      exec taskset -c "$0" ../target/release/switch-nic "$@"' "${SWITCH_CPU:-2}" "$@"
}
```

Set `SWITCH_CPU` to the CPU you chose in step 3. `/lib/firmware` is mounted because Intel E810 NICs load a firmware package (`ice.pkg`) at startup.

## 6. Discover, check and bind

`switch-netconf discover` reads the interface's address, MAC, MTU, PCI address and NUMA node from the kernel and writes a config file. Run it while the NIC is still on its kernel driver:

```sh
switch-netconf discover --interface ens5f0 -o dut.json
switch-netconf check dut.json
```

If the interface has a default route in the main routing table, `discover` takes the gateway from it. Otherwise set `gateway` by hand, or to `null` if every peer is on the same subnet. Edit offloads, `rx_timestamps` and the EAL options now if you need to ([The network config file](config.md) and [Offloads and hardware timestamps](offloads.md) describe them); `check` must exit 0 afterwards.

Then bind, first as a dry run:

```sh
sudo switch-netconf bind dut.json --dry-run    # prints every write, changes nothing
sudo switch-netconf bind dut.json              # with an IOMMU
sudo switch-netconf bind dut.json --noiommu    # without one
```

Before it writes anything, `bind` checks that the device still matches the file and that taking it from Linux cuts nothing else off. In particular it refuses when the interface carries any route besides its own connected subnet, so a default route or a management route is never removed by accident. [switch-netconf](netconf-tool.md) lists every check and what `bind` prints.

## 7. Check with switch-nic info

`switch-nic info` opens the NIC with the config file, prints what the driver offers and closes it again:

```sh
nic --config dut.json info 2>&1 | tee info.log
```

DPDK writes its own messages to stderr and `switch-nic` writes its result lines, each starting with `switch-nic:`, to stdout, hence `2>&1`. A good run exits 0 and prints the driver name, its offload capabilities, the link state `up`, the hugepages the process took and `port_close: ok` at the end. [switch-nic](switch-nic.md) explains each line; [When the NIC won't open](troubleshooting.md) maps each error to its cause.

At this point the machine is ready: any program that opens the NIC with this config file, pinned to the CPU from step 3, will work the same way.

## 8. Undo

When you are done, give the NIC back to Linux:

```sh
sudo switch-netconf unbind dut.json
```

It puts the device back on its kernel driver and reports whether the IPv4 address came back on the interface. It does not restore addresses or routes itself; your network manager does that. Hugepages reserved with `echo` disappear at the next reboot; remove the boot options from step 3 to undo the CPU isolation.

## Where to go next

[Running on EC2](ec2.md) does all of this on Amazon's ENA NICs, by hand or with one script. To run load against a prepared NIC, see [switch-nic](switch-nic.md).
