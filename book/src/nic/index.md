# Owning a NIC

[Your first network program](../getting-started/first-connection.md) ran over a tap device: a pretend network card that Linux creates on the fly, with the kernel sitting on the other end of the wire. The program did not care. It loaded a config file, called `switch_open::open`, got a Stack back and drove it. The [market-data feed](../protocols/binance-feed.md) at the end of the previous part ran over a tap device as well. This part of the book is about doing the same thing with a real network card, and about everything that has to be true of the machine before that call succeeds.

## What changes when your process owns the NIC

A normal Linux program never touches the network card. It calls `connect` and `send`, the kernel's TCP/IP stack builds packets, and the kernel's driver hands them to the card. Here, your process takes the card away from the kernel. DPDK, the userspace NIC driver framework this project runs on, lets a program drive the card's receive and transmit queues directly from its own memory. The TCP/IP stack runs inside your process as well. In this book that stack is called the Stack: the type `Stack` from the `dpdk-smoltcp` crate, running over one NIC port on one thread. A NIC port is one receive queue and one transmit queue of the card, set up for DPDK together with the pools of buffers they use (the type `Port`). "Port" on its own always means a TCP or UDP port number.

Three consequences follow, and the rest of this part deals with each of them.

The kernel loses the card. Once your program owns a NIC, Linux no longer sees it as a network interface: no SSH, no DNS, no `ping` from the shell goes through it. A machine that runs a program like this needs two network cards. The data NIC is the one your program owns. The management NIC stays with Linux and carries SSH, DNS and everything else the machine does.

The card needs a userspace driver. DPDK has its own driver for each family of cards (`net_ena` for Amazon's ENA, `net_ice` for Intel E810, `net_tap` for tap devices, and so on). This book calls such a driver a NIC driver; DPDK's documentation calls it a PMD, for poll-mode driver. For a real PCI card, Linux must first let go of the device: you detach it from its kernel driver and attach it to `vfio-pci`, a kernel driver whose only job is to let one userspace process control a PCI device safely. This book calls that step binding the NIC. A tap device needs no binding, because DPDK creates it itself.

One thread does all the work, on one CPU. The Stack never sleeps and never blocks; your drive loop calls it over and over. That loop runs on exactly one CPU, the pinned CPU, and the process must be allowed to run on that CPU only. Opening the NIC refuses to start otherwise.

DPDK also has a one-time setup step per process, the EAL (Environment Abstraction Layer). It reserves memory, finds or creates the devices and starts a few helper threads of its own. You do not call it directly when you use `switch_open::open`; it is the first thing `open` does. Real cards need the EAL's packet memory to come from hugepages, large memory pages the machine reserves ahead of time. Tap devices run without them.

The picture of a machine set up this way:

```text
            ┌──────────────────────── one Linux machine ────────────────────────┐
            │                                                                   │
            │  housekeeping CPUs                    pinned CPU (isolated)       │
            │  ┌──────────────────────────┐         ┌───────────────────────┐   │
            │  │ Linux, interrupts, sshd  │         │ your drive loop:      │   │
            │  │ DPDK helper threads      │         │ Stack + your tasks    │   │
            │  └────────────┬─────────────┘         └───────────┬───────────┘   │
            │               │ kernel driver                     │ DPDK NIC      │
            │               │ (ena, ice, ...)                   │ driver via    │
            │               │                                   │ vfio-pci      │
            └───────────────┼───────────────────────────────────┼───────────────┘
                            │                                   │
                    management NIC                          data NIC
                  (SSH, DNS, updates)              (your program's traffic only)
```

## What you need

For a tap device, which is how you try things on a laptop or in the Docker image, you need very little: Linux with DPDK 25.11.3 (or the Docker image from [Setting up a build environment](../getting-started/environment.md)), the capability to create network devices (`NET_ADMIN` and `/dev/net/tun`), a [network config file](config.md) that names a tap device, and a process restricted to one CPU.

For a real card you need, in addition:

- a second network interface that stays with Linux, so you can still reach the machine;
- hugepages reserved on the card's NUMA node (the memory bank closest to the card);
- one CPU for the drive loop, ideally isolated from other work by kernel boot options, on the same NUMA node as the card;
- the card bound to `vfio-pci`, which needs root once, and on machines without an IOMMU (EC2 instances other than metal ones) the kernel's unsafe no-IOMMU mode.

## From a bare machine to a running Stack

```text
  prepare the machine          describe the NIC            hand it to vfio-pci
  hugepages, isolated CPU  ──► switch-netconf discover ──► switch-netconf bind
  (root, once per boot)        switch-netconf check        (root; PCI cards only)
                                     │
                                     ▼
                               dut.json (network config file)
                                     │
                    taskset -c N  your program
                                     │
                                     ▼
                     switch_open::open(&config, &sizing, helper_cpus, &mut clock)
                     EAL start → find device → open NIC port → wait for link
                     → build Stack → resolve gateway by ARP
                                     │
                                     ▼
                     Opened { stack, eal, report }  →  Net::new(stack, ...)
```

The pages of this part follow that path:

- [The network config file](config.md) describes every field of `dut.json`, the JSON file that names the device and its addresses.
- [switch-netconf](netconf-tool.md) is the command that writes that file from what Linux knows about an interface, checks it, and binds and unbinds the card.
- [Opening the NIC from code](open.md) explains `switch_open::open`: what you pass, what it does, what you get back and how it fails.
- [Packet-buffer pools](pools.md) explains the `Sizing` you pass to `open`.
- [The pinned CPU and helper threads](cpus.md) explains the one-CPU rule and where DPDK's own threads go.
- [Offloads and hardware timestamps](offloads.md) covers checksum work the card can take over and receive timestamps.
- [Preparing a Linux machine](host-setup.md) and [Running on EC2](ec2.md) take a real machine through the steps above.
- [switch-nic](switch-nic.md) is a ready-made program that opens a card and reports on it, and can serve load.
- [When the NIC won't open](troubleshooting.md) maps each error message to its cause.

If you only ever want to share one card between several programs, read this part up to the config file and the CPU page, then go to [Sharing a NIC with the junction daemon](../junction/index.md): the daemon opens the card the same way and your program connects to it.

Next: [The network config file](config.md).
