# Troubleshooting

A program that opens a card gets these messages as the text of the error `switch::nic::open` returns; `switch-nic` prints them after `switch-nic: error: `, and `switch-junctiond` after `switch-junctiond: open: `. Where `switch-nic` uses its own wording, the table shows both.

When something fails inside DPDK, its driver usually writes the details to stderr, so keep stderr when you run a program that opens a card. For `switch-junctiond`'s own startup errors, see [switch-junctiond](junctiond.md#when-it-fails).

## Opening the card

| Message | Cause and what to do |
|---|---|
| `CPU affinity: the calling thread may run on N CPUs, not exactly one; restrict it to the CPU for EAL (taskset -c N, docker --cpuset-cpus=N)` | Start the program with `taskset -c <cpu>`, or give the container one CPU ([CPUs](cpus.md)). |
| `EAL initialization: rte_eal_init returned <code> with rte_errno <errno>` | DPDK could not start; stderr has the reason. A real card needs hugepages reserved ([Preparing a Linux machine](host-setup.md#2-reserve-hugepages)) and root. A tap device without hugepages needs `"--no-huge", "-m", "256"` in `eal.extra`. DPDK starts once per process, so exit rather than open again. |
| `EAL helper CPU affinity: EAL helper CPUs are outside the effective cpuset` | The CPUs named for DPDK's threads (`--control-cpus` for the daemon) are not available to the process. Name CPUs inside the cpuset. |
| `EAL helper CPU affinity: explicit EAL helper CPUs require Linux`, `EAL helper CPU affinity: EAL helper CPUs must be nonempty and below CPU_SETSIZE` | The program passed helper CPUs on another operating system, or an empty or out-of-range list. |
| `EAL initialization: ...; restoring polling CPU affinity: ...` | DPDK failed, and the thread could not be put back on its one CPU. Fix the first error; the process should exit. |
| `device lookup: "0000:00:06.0" was not found` | DPDK has no device by that name. A PCI card must be bound to `vfio-pci` (`switch-netconf bind`), and `pci` must match. For a `vdev`, the name is the part of the string before the first comma. |
| stderr: `Failed to load the DDP package,Use safe-mode-support=1 to enter Safe Mode` | An E810 without its DDP package `ice.pkg`. Install it under `/lib/firmware/intel/ice/ddp/` ([Preparing a Linux machine](host-setup.md)); in Docker, mount the host's `/lib/firmware`. |
| `device information: ...` | DPDK could not read the device's details; stderr has the reason. |
| `device MAC: config 0a-12-34-56-78-9a differs from device 0a-12-34-56-78-9b` | The card at `pci` is not the one the config was written for. Fix `pci`, or rewrite the config with `switch-netconf discover -o` before binding. |
| `descriptor adjustment: ...` | The driver refused the ring sizes. Change `descriptors`. |
| `port configuration: TX pool count exceeds u32`, `port configuration: frame pool count exceeds u32` | The program's pool sizes are too large. See `switch::nic::Sizing` in the API reference (`make docs`). |
| `port configuration: pool data room exceeds u16` | `mtu` makes a packet buffer larger than DPDK allows. Use a smaller `mtu`. |
| `port open: port setup failed: InvalidSettings("PMD lacks the requested TX TCP checksum offload")`; switch-nic: `port_open_error: InvalidSettings("PMD lacks the requested TX TCP checksum offload")` | An offload requested explicitly that the driver does not offer; the message names it (`RX IPv4/TCP/UDP checksum`, `TX TCP checksum`, `TX UDP checksum`). Set it to `false`, or use `"auto"` ([Offloads](offloads.md)). |
| `port open: port setup failed: InvalidSettings("PMD lacks the requested RX timestamp offload")`; switch-nic: `port_open_error: ...` | `rx_timestamps` is on and the driver cannot stamp packets. Set it to `false`, or on EC2 use an instance that supports it ([Running on EC2](ec2.md#before-you-log-in)). |
| `port open: port setup failed: ...`; switch-nic: `port_open_error: ...`, other reasons | The card refused another setting, for example an `mtu` outside the `min_mtu` and `max_mtu` that `switch-nic info` prints. |
| `link query: ...` | DPDK could not read the link state; stderr has the reason. |
| `link wait: link down after 10 seconds`; switch-nic: `error: link down after 10 seconds` | The link did not come up. Check that the cable, switch port or cloud network interface is attached and up. |
| `stack random seed: ...`; switch-nic: `error: OS random seed: ...` | Reading `/dev/urandom` failed. Check that it exists in the container. |
| `Stack::new: TxPoolTooSmall { count: .., required: .. }`, `TxFramePoolTooSmall`, `RxPoolTooSmall` | A pool is smaller than the network stack needs. Enlarge it to at least `required`; see `switch::nic::Sizing` in the API reference. |
| `Stack::new: InvalidConfig("max_sockets must be positive")`, and the same for `neighbor_capacity`, `rx_staging`, `tx_pending` | A field of the program's `Sizing` is 0. |
| `gateway 10.0.1.1 unresolved after 3.000s` | The gateway did not answer ARP within three seconds. Check `gateway`: it must be a router on the card's network. On EC2 it is the subnet's first address after the network address, which `switch-netconf discover --aws` writes. |
| `gateway receive: ...` | The network stack failed while waiting for the gateway's answer. |

## Closing the card

| Message | Cause and what to do |
|---|---|
| `OutstandingBuffers { rx: .., tx: .., frame: .. }`; switch-nic: `port_close: buffers still in use: rx=.. tx=.. frame=..`, exit status 2 | The program still held packet buffers when it closed the card. Drop every received frame and send buffer, and shut the runtime down, before closing; the guide chapter Shutting down in the `switch` crate documentation (`make docs`) gives the order. |

## Running switch-nic sockperf-server

| Message | Cause and what to do |
|---|---|
| `switch-nic: error: nonzero RX timestamp <n> with rx_timestamps disabled` | The driver stamped a packet although the config asked for no timestamps. Turn `rx_timestamps` on, or report the driver's behaviour. |

## Writing the config

| Message | Cause and what to do |
|---|---|
| `switch-netconf: /etc/switch/dut.json: <key>: <reason>` | The file breaks a rule; see [The network config file](config.md#checking-a-file). |
| `interface eth1 does not exist` | Check the name with `ip link`. A card already bound to `vfio-pci` has no interface; `unbind` it first. |
| `eth1 is not a PCI device` | The interface is virtual (a bridge, a veth, a tap). Write a `vdev` config by hand. |
| `eth1 (0000:00:06.0) has no kernel driver bound` | Load the card's Linux driver, or write the config by hand. |
| `eth1 has no IPv4 address` | Give the interface an address, or on EC2 use `--aws`. |
| `eth1 has several IPv4 addresses (...); choose one with --ipv4` | Add `--ipv4 ADDR`. |
| `10.0.1.99 is not an IPv4 address of eth1` | `--ipv4` names an address the interface does not have. |
| `eth1: /proc/net/route: several default gateways (...)` | Remove one of the interface's default routes and discover again. |
| `eth0 is the primary ENI (device-number 0); it carries IMDS and SSH` | `--aws` on the instance's primary interface. Attach a second ENI and use that one. |
| `IMDS token request to 169.254.169.254:80: ...; from a container without host networking, the IMDSv2 PUT response hop limit (1 by default, 2 on AL2023) drops the reply` | The metadata service did not answer. In Docker, run with `--network host`. |
| `IMDS item <item> is missing (404)` | The metadata service knows nothing about this interface's MAC address under that item. Check that the interface is an ENI of this instance. |
| `10.0.1.23 is not in IMDS local-ipv4s (...)` | The address on the interface, or given with `--ipv4`, is not one of the ENI's private addresses. |
| `kernel IPv4 ... prefix differs from IMDS subnet-ipv4-cidr-block ...`, `eth1 kernel default gateway ... differs from IMDS subnet gateway ...` | Linux's view of the interface disagrees with the ENI's subnet. Fix the interface's address or routes, or flush them and discover again. |
| `switch-netconf: warning: eth1 has no public IPv4 (IMDS public-ipv4s); internet destinations need an Elastic IP on this ENI or a NAT gateway route` | A warning. Attach an Elastic IP or route through a NAT gateway if the program talks to the internet. |

## Binding

`bind` and `unbind` refuse before changing anything.

| Message | Cause and what to do |
|---|---|
| `bind/unbind needs euid 0` | Run with `sudo`. |
| `bind needs a PCI device` | The config is for a `vdev`, which needs no binding. |
| `0000:00:06.0 PCI ID 8086:1592, config 1d0f:ec20` | The card at that address is a different model from the config's. Fix `pci`, or discover again. |
| `0000:00:06.0 MAC 0a:12:34:56:78:9b, config 0a-12-34-56-78-9a` | The card's address differs from `mac`. Discover again for this interface. |
| `0000:00:06.0 is bound to <driver>` | The card is on a driver that is neither its Linux driver nor `vfio-pci`. Unbind it from that driver by hand first. |
| `eth1 has route 0.0.0.0/0 via 10.0.1.1` | The interface carries a route other than its own subnet's. Take it away from the network manager and flush its routes ([step 6](host-setup.md#6-discover-check-and-bind)). |
| `IOMMU peer 0000:00:06.1 is bound to <driver>` | Another device in the card's IOMMU group is on a Linux driver. `vfio-pci` takes the whole group: unbind that device or bind it to `vfio-pci` too. |
| `no-IOMMU mode taints the kernel and needs --noiommu` | The machine has no IOMMU for this card. Add `--noiommu` ([switch-netconf](switch-netconf.md#bind)). |
| `vfio-pci driver missing after modprobe (...)` | The `vfio-pci` module could not be loaded. Install it for the running kernel. |
| `...; rebind ok; current driver ena` | A step failed after the card left its Linux driver, and `bind` tried to give it back; the message ends with the driver the card is on now. Fix the first error in the message and bind again. |
