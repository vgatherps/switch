# Starting DPDK and opening a NIC port by hand

`switch_open::open` turns a network config file into a running Stack in one call, as in the client on the [previous page](index.md). Underneath, it pins DPDK to your CPU, starts DPDK, finds the device, configures the NIC port and its pools, and builds the Stack. This page does those steps yourself. You need this when your program creates its own test device, chooses pool sizes `open` does not offer, or opens a device that is not described by a config file.

The example throughout is the tap echo harness in `switch-tap-echo` (`src/driver.rs`), which runs a TCP echo server on a [tap device](../getting-started/first-connection.md) for the repository's tests.

## 1. Pin the thread and start EAL

DPDK starts with one call per process, `Eal::init`. EAL (DPDK's environment abstraction layer) is its one-time setup: memory, devices and helper threads. The thread that calls `Eal::init` becomes DPDK's main thread, and from then on that thread owns the NIC port, the Stack and every packet buffer.

`Eal::init` adds no options of its own and does not pin anything. You restrict the thread to one CPU before starting (with `taskset -c N`, or a container's `--cpuset-cpus=N`), then tell DPDK which CPU that is. `switch_dpdk_sys::only_allowed_cpu()` returns the one CPU the calling thread may run on, or `AffinityError::NotOne { allowed }` if it may run on more or fewer:

```rust,ignore
{{#include ../../../conduit/crates/switch-tap-echo/src/driver.rs:only_allowed_cpu}}
```

The arguments are an ordinary command line, starting with a program name. The harness gives DPDK's thread number 7 that CPU with `--lcores=7@<cpu>` and makes thread 7 the main thread with `--main-lcore=7`. Any lcore number works; `open` uses 0 (`--lcores=0@<cpu>`). A tap device needs no PCI devices and no hugepages, so it passes `--no-pci` and `--no-huge` with a memory size (`-m`) large enough for its pools:

```rust,ignore
{{#include ../../../conduit/crates/switch-tap-echo/src/driver.rs:eal_init}}
```

`Eal::init` can succeed only once in a process. A second call fails with `EalError::Initialization` (its `errno` is `EALREADY`). A failed call does not promise that a retry will work, and some DPDK argument errors, as well as `--help`, exit the process. Dropping the returned `Eal` does nothing: EAL is never torn down. Keep the `Eal` alive until the process exits, because the pools depend on it.

## 2. Find or create the device

`Port::open` takes a DPDK port id, the number DPDK gives each NIC it knows about. For a real NIC, pass its PCI address to EAL with `-a <address>`; EAL takes the device at start-up and `port_by_name(&eal, "<address>")` returns its id. For a test device, create it after EAL starts. The harness creates a tap device with `rte_vdev_init` from `switch-dpdk-sys` and then looks it up by name:

```rust,ignore
{{#include ../../../conduit/crates/switch-tap-echo/src/driver.rs:vdev}}
```

`device_info(&eal, port_id)` returns what the driver reports about the device: its driver name, the offloads it supports, its MTU range and its MAC address. It is useful for checking a device before you configure it.

## 3. Configure the port

A `PortConfig` describes the one receive queue and one transmit queue the Stack uses, and the three [packet-buffer pools](../nic/pools.md):

| Field | Meaning |
|---|---|
| `port_id` | The DPDK port id from step 2. |
| `mtu` | The IP MTU. Must be inside the device's MTU range, and at least 88 for `Stack::new` to accept the port. |
| `rx_descriptors`, `tx_descriptors` | Slots in the NIC's receive and transmit rings. Must be positive. |
| `rx_burst`, `tx_burst` | Most packets taken from or offered to the driver per call. Must be positive. |
| `queue_socket` | NUMA node for the queues; -1 for any. |
| `rx_pool`, `tx_pool`, `frame_pool` | One `switch_buffer::PoolConfig` each. |
| `offloads` | Which `Offloads` to request from the driver. The default requests none. |

A `PoolConfig` names the pool (`name`, a C string unique in the process), gives its `count` of packet buffers, the per-CPU `cache_size`, the `data_room_size` of each buffer in bytes including DPDK's headroom (`RTE_PKTMBUF_HEADROOM`), and the NUMA `socket_id` (-1 for any).

The data room must hold a whole packet in one buffer, because the Port never chains buffers together. `Port::open` checks:

- RX pool: at least headroom + (MTU + 26) rounded up to a multiple of 1024.
- TX pool and frame pool: at least headroom + MTU + 14.

At MTU 1500 the harness uses headroom + 2048 for all three. It sizes each pool as its reserve plus a share:

```rust,ignore
{{#include ../../../conduit/crates/switch-tap-echo/src/driver.rs:port_config}}
```

Offloads are requested exactly. If you ask for one the driver does not support, `Port::open` fails with `PortError::InvalidSettings` naming it, for example "PMD lacks the requested RX timestamp offload". There is no silent fallback. [Offloads and hardware timestamps](../nic/offloads.md) explains each one.

## 4. Open it

`Port::open` configures the device, creates the pools, sets up both queues and starts the device. It does not wait for the link to come up (`switch_open::open` waits up to 10 seconds; by hand, that is your choice).

```rust,ignore
{{#include ../../../conduit/crates/switch-tap-echo/src/driver.rs:port_open}}
```

`Port::open` is `unsafe` because the compiler cannot check what it relies on. In plain words, by calling it you promise:

- the device was just created or probed, has never been configured or started, and nothing else uses it;
- every EAL call, every packet operation and the cleanup happen on the thread that started EAL;
- a test device you created stays alive until the port has closed, and packets queued on it return before the pools are destroyed;
- the NIC driver honours the requested settings and gives back all its packet buffers when the port closes;
- the `Eal` stays alive until all of that is done.

A failure returns `PortOpenError`, with three fields. `cause` is the `PortError` that stopped the open. Errors found before the pools exist leave the device as it was. After that point `Port::open` owns the device and undoes its setup on failure; if undoing fails too, `rollback_error` says why and `retained` holds the half-open `Port`. A retained `Port` still owns packet buffers: keep the `Eal` and any test device alive and clean it up with `try_close` like any other port.

## 5. Configure the Stack

`Stack::new(port, config, now)` takes the open `Port`, a `StackConfig` and the start time. The `StackConfig` fields are:

| Field | Meaning |
|---|---|
| `ethernet` | The MAC address the Stack sends from. Must be unicast and nonzero. Use the device's own, `EthernetAddress(device_info(&eal, port_id)?.mac)`, as `open` does. |
| `ipv4` | Our address and subnet, as an `Ipv4Cidr`. |
| `gateway` | The default route for destinations outside the subnet, or `None` for on-link peers only. Must be a unicast address inside the subnet other than ours, the network address or the broadcast address. |
| `paths` | Per-destination path MTUs (`PathMtu { prefix, mtu }`), longest prefix wins. Each must be at least 88. TCP connections cap their MSS at path MTU − 40. |
| `owned_ipv4` | Extra addresses the Stack answers ARP for and accepts traffic to. Routing, the subnet broadcast and the default source address stay with `ipv4`. |
| `random_seed` | Seed for the Stack's random numbers, including TCP initial sequence numbers. Use `os_random_seed()` in production. |
| `neighbor_capacity` | ARP cache entries; `DEFAULT_NEIGHBOR_CAPACITY` is 256. Must be positive. |
| `max_sockets`, `max_udp_sockets` | TCP and UDP socket slots. TCP must be positive; UDP zero disables UDP. |
| `rx_staging`, `tx_pending` | The Stack's receive staging and pending transmit queues (see [packet-buffer pools](../nic/pools.md)). Both must be positive. |

These are the same rules the [network config file](../nic/config.md) enforces; `open` fills `StackConfig` from it. The harness's values:

```rust,ignore
{{#include ../../../conduit/crates/switch-tap-echo/src/driver.rs:stack_config}}
```

The harness uses a fixed MAC address of its own.

The start time sets the origin of the Stack's clock. The harness starts the Stack at `Instant::ZERO` and takes a `std::time::Instant` just before, so every later time is the microseconds elapsed since then:

```rust,ignore
{{#include ../../../conduit/crates/switch-tap-echo/src/driver.rs:stack_new}}
```

A refused config gives back the `Port` with a `StackError`: `InvalidConfig(reason)` for a rule above (or a start time before zero), `IpMtuBelowIpv4Minimum` for an IP MTU below 68, `InvalidConfig` for an IP MTU from 68 to 87 (88 is the smallest MTU that carries a 48-byte TCP segment, the same floor the config file enforces), or one of the `*PoolTooSmall` errors from the pool checks. Close the returned port as in step 6.

From here the program adds sockets and runs its [drive loop](drive-loop.md).

## 6. Close in order

Shutdown runs the lifecycle backwards. Drop every frame and datagram your code holds. Call `stack.into_port()`, which drops the sockets and every packet still queued inside the Stack and returns the `Port`. Call `port.try_close()`. It stops and closes the device, then frees its pools once every packet buffer is back. Only when it returns `Ok` is all of that done, so only then remove a test device you created:

```rust,ignore
{{#include ../../../conduit/crates/switch-tap-echo/src/driver.rs:teardown}}
```

`PortError::OutstandingBuffers { rx, tx, frame }` means the device is closed but something still holds packet buffers from those pools. The `Port` comes back with the error, so you can drop what is held and call `try_close` again; the port does no more network work either way. If a `Port` is dropped while buffers are still out, it keeps its pool memory for the rest of the process and prints a line to stderr saying how many buffers were in use.

EAL itself is never torn down. It lives until the process exits.

Next: [the Stack's drive loop](drive-loop.md).
