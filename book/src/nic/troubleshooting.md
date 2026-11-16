# When the NIC won't open

This page lists the messages you can meet while preparing a machine and opening a card, with what each one means and what to do. The messages are quoted as the code prints them. In [`switch-nic`](switch-nic.md) output, errors from `open` appear after `switch-nic: error:`; in your own program they are the `Display` text of `OpenError`.

When something fails inside DPDK, its NIC driver usually writes the details to stderr. Always keep stderr when you run a program that opens a card.

## Opening the card

| Message | Cause | What to do |
|---|---|---|
| `CPU affinity: the calling thread may run on N CPUs, not exactly one; restrict it to the CPU for EAL (taskset -c N, docker --cpuset-cpus=N)` | `NotPinned`: the process may run on more than one CPU. | Start it with `taskset -c <cpu>`, or give the container one CPU. See [The pinned CPU](cpus.md). |
| `EAL initialization: rte_eal_init returned <code> with rte_errno <errno>` | DPDK's process setup failed; DPDK writes the reason to stderr. | Fix what stderr names. A tap device without reserved hugepages needs `"--no-huge", "-m", "256"` in `eal.extra`, as in the [tap config](config.md#a-complete-example). EAL starts only once per process, so exit rather than call `open` again. |
| `EAL helper CPU affinity: EAL helper CPUs are outside the effective cpuset` | `helper_cpus` names CPUs the process or container may not use. | Pass CPUs inside the cpuset, or `None`. |
| `EAL helper CPU affinity: explicit EAL helper CPUs require Linux` | `helper_cpus` was `Some` on another operating system. | Pass `None`. |
| `EAL initialization: ...; restoring polling CPU affinity: ...` | DPDK failed, and putting the thread back on its one CPU failed too. | Fix the first error; the process should exit. |
| `device lookup: "0000:00:06.0" was not found` | DPDK has no device by that PCI address or virtual device name. For a PCI card, DPDK only takes cards bound to `vfio-pci`. | Check `pci` in the config; run `switch-netconf bind`. For a tap or other virtual device, the name is the part of the `vdev` string before the first comma. |
| `device MAC: config 0a-12-34-56-78-9a differs from device 0a-12-34-56-78-9b` | `MacMismatch`: the card at `pci` is not the card the config was written for. | Run `switch-netconf discover -o` against the right interface before binding, or fix `pci`. |
| `descriptor adjustment: ...` | The NIC driver refused to adjust the ring sizes. | Change `descriptors`. |
| `port configuration: ...` pool count or data room messages | `Sizing` or `mtu` produced a pool larger than DPDK accepts. | Reduce `frame_share`, `tx_pending` or `descriptors`. See [Packet-buffer pools](pools.md). |
| `port open: port setup failed: InvalidSettings("PMD lacks the requested RX IPv4/TCP/UDP checksum offload")` (or `TX TCP checksum`, `TX UDP checksum`) | An explicit offload the NIC driver does not offer. | Set that offload to `false`, or use `"auto"`. See [Offloads](offloads.md). |
| `port open: port setup failed: InvalidSettings("PMD lacks the requested RX timestamp offload")` | `rx_timestamps: true` on a driver without receive timestamps. | Set `rx_timestamps` to `false`. |
| `port open: port setup failed: InvalidSettings(...)` other reasons | The card refused another setting, for example an MTU outside its range (the report's `min_mtu` and `max_mtu` lines show the range). | Fix the setting the reason names. |
| `link query: ...` | DPDK could not read the link state. | Read stderr. |
| `link wait: link down after 10 seconds` | The card did not report its link up within ten seconds. | Check that the cable, the switch port or the cloud network interface is attached and up. `switch-nic` prints `eth_stats` here too. |
| `stack random seed: ...` | Reading `/dev/urandom` failed. | Check that `/dev/urandom` exists in the container. |
| `Stack::new: TxPoolTooSmall { count: .., required: .. }` (also `TxFramePoolTooSmall`, `RxPoolTooSmall`) | A pool is below the minimum the Stack needs for its rings. | Enlarge the named pool to at least `required`. See [Packet-buffer pools](pools.md). |
| `gateway 10.0.1.1 unresolved after 3.000s` | No ARP answer from the gateway within three seconds. | Check `gateway` in the config: it must be a router on the same network segment as the card. On EC2 it is the subnet's base address plus one. |
| `gateway receive: ...` | A Stack error while waiting for the gateway's ARP answer. | Read the error; it comes from the Stack ([Stack errors](../stack/errors.md)). |

## Closing the card

| Message | Cause | What to do |
|---|---|---|
| `OutstandingBuffers { rx: .., tx: .., frame: .. }` from `try_close`; `switch-nic: port_close: buffers still in use: rx=.. tx=.. frame=..` and exit 2 | Your code still holds packet buffers: received frames, datagrams or frames you got for sending. `rx` counts received buffers, `frame` buffers handed out for sending, `tx` buffers the Stack built itself. | Drop them on the drive-loop thread and call `try_close` again. With switch-net, shut the runtime down and take the Stack out of the `Net` first ([Shutting down cleanly](../net/shutdown.md)). |

## Writing and checking the config

| Message | Cause | What to do |
|---|---|---|
| `switch-netconf: dut.json: <key>: <reason>` from `check` | The file breaks a rule. | See the [validation table](config.md#validation-rules). |
| `interface eth1 does not exist` | `discover --interface` names no Linux interface. | Check the name with `ip link`. If the card is already bound to `vfio-pci`, Linux no longer has an interface for it; `unbind` it first. |
| `eth1 is not a PCI device` | The interface is virtual (a bridge, a veth, a tap). | `discover` is for PCI cards; write a `vdev` config by hand. |
| `eth1 (0000:00:06.0) has no kernel driver bound` | The card has no driver. | Load its kernel driver, or write the config by hand. |
| `eth1 has no IPv4 address` | Linux has no address on the interface. | Configure one, or with `--aws` let the metadata service supply it. |
| `eth1 has several IPv4 addresses (...); choose one with --ipv4` | More than one address. | Add `--ipv4 ADDR`. |
| `eth1: /proc/net/route: several default gateways (...)` | Two default routes through this interface. | Remove one, then discover again. |
| `eth0 is the primary ENI (device-number 0); it carries IMDS and SSH` | `--aws` on the instance's primary interface. | Attach a second network interface and use that one. |
| `switch-netconf: warning: eth1 has no public IPv4 (IMDS public-ipv4s); ...` | `--aws` found no public address on the interface. | Attach an Elastic IP to it, or route through a NAT gateway, if you need internet destinations. |

## Binding

Each of these refusals happens before anything is written.

| Message | Cause | What to do |
|---|---|---|
| `bind/unbind needs euid 0` | Not root. | Run with `sudo`. |
| `bind needs a PCI device` | The config is for a tap or other virtual device. | Nothing to do: virtual devices need no binding. |
| `0000:00:06.0 PCI ID 8086:1592, config 1d0f:ec20` | The card at that address is a different model from the config's. | Fix `pci`, or discover again. |
| `0000:00:06.0 MAC 0a:12:..., config ...` | The card's MAC differs from `mac`. | Discover again for this interface. |
| `0000:00:06.0 is bound to <driver>` | The card is on a driver that is neither its kernel driver nor `vfio-pci`. | Unbind it from that driver by hand first. |
| `eth1 has route 0.0.0.0/0 via 10.0.1.1` | The interface carries a route other than its own subnet. Binding would cut it. | Move that route to the management interface, or remove it, then bind. |
| `IOMMU peer 0000:00:06.1 is bound to <driver>` | Another device in the same IOMMU group is still on a kernel driver. | Unbind that device or move it to `vfio-pci` as well; `vfio-pci` hands out a whole IOMMU group at once. |
| `no-IOMMU mode taints the kernel and needs --noiommu` | The machine has no IOMMU for this card. | Add `--noiommu` if you accept that (see [bind](netconf-tool.md#bind)). |
| `vfio-pci driver missing after modprobe (...)` | The `vfio-pci` module could not be loaded. | Install or enable it for the running kernel. |
| `... ; rebind ok; current driver ena` | A write or the final check failed after the kernel driver was detached, and `bind` put the card back. | Fix the first error in the message and bind again. |

Back to: [Owning a NIC](index.md). Next part: [Sharing a NIC with the junction daemon](../junction/index.md).
