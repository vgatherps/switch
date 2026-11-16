# Stack errors and panics

This page collects every error type a program meets when it uses DPDK, the NIC port and the Stack directly, with what each one means and what to do about it. The pages before it, from [Programming the Stack directly](index.md) to [Waiting on many sockets](readiness.md), explain the calls; come here when a call returned something you need to look up. If you open the NIC with `switch_open::open`, these errors reach you wrapped in its `OpenError`, and [when the NIC won't open](../nic/troubleshooting.md) lists the messages you will see.

## Starting DPDK

`switch_dpdk_sys::only_allowed_cpu()` returns `AffinityError`:

| Error | Meaning | What to do |
|---|---|---|
| `NotOne { allowed }` | The thread may run on `allowed` CPUs, not exactly one. | Start the process with `taskset -c N`, or a container with `--cpuset-cpus=N`. |
| `Query(io::Error)` | The kernel refused to report the thread's CPU set. | Read the I/O error. |

`Eal::init` returns `EalError`:

| Error | Meaning | What to do |
|---|---|---|
| `EmptyArguments` | No arguments; the first must be a program name. | Pass a program name first. |
| `ArgumentCountTooLarge { count }` | More arguments than DPDK's C interface can count. | Pass fewer. |
| `InteriorNul { argument, position }` | An argument contains a NUL byte. | Fix the argument. |
| `Initialization { return_code, errno }` | DPDK's own start-up failed. `errno` is `EALREADY` if EAL already started in this process. | Read DPDK's messages on stderr. Do not count on a retry working. |
| `TimerSubsystem { return_code }` | EAL started, but DPDK's timer subsystem did not. | Read DPDK's messages; treat as fatal. |

Some DPDK argument errors, and `--help`, exit the process inside `Eal::init` instead of returning.

## Opening and closing the NIC port

`Port::open` returns `PortOpenError`, which has three fields: `cause`, the `PortError` that stopped the open; `rollback_error`, set if undoing a half-done setup also failed; and `retained`, the half-open `Port` in that case. A retained `Port` still owns packet buffers. Keep the `Eal` and any test device alive and close it with `try_close`, as with any open port.

`PortError` appears as that `cause`, from `try_close`, and inside `StackError::Port`:

| Error | Meaning | What to do |
|---|---|---|
| `OwnerMismatch { expected, actual }` | Called on a thread other than the one that started EAL. | Keep EAL, the port and the Stack on one thread. |
| `InvalidSettings(reason)` | A `PortConfig` value the device or the rules refuse: zero descriptors or burst, MTU outside the device's range, an offload the driver lacks or did not apply, a pool data room too small for one packet. `reason` says which. | Fix the config. |
| `Pool { direction, error }` | Creating the `"RX"`, `"TX"` or `"frame"` pool failed; `error` is a `PoolCreateError` (below). | See below. |
| `Dpdk { operation, code }` | A DPDK call failed; `operation` names it. | Read DPDK's messages. |
| `OutstandingBuffers { rx, tx, frame }` | `try_close` found packet buffers still in use: that many from each pool. | Drop received frames and datagrams you hold, then call `try_close` again with the returned `Port`. |
| `NotReady` | The port's setup did not finish, its cleanup has begun, or a reset recovery failed. | Close the port. |

`switch_buffer::PoolCreateError`, inside `PortError::Pool`:

| Error | Meaning |
|---|---|
| `CacheDisabled` | `cache_size` is 0; a nonzero pool cache is required. |
| `DataRoomTooSmall { provided, minimum }` | `data_room_size` is smaller than DPDK's headroom. |
| `Dpdk { errno }` | DPDK could not create the pool: a name already used in this process, or not enough memory. |
| `OwnerCacheUnavailable { lcore_id }` | The pool has no cache for the EAL thread. |

## Building and driving the Stack

`Stack::new` gives back the `Port` with the error, so you can close it. The drive calls (`turn`, `receive`, `send`, `flush`) return the error alone.

| `StackError` | From | Meaning | What to do |
|---|---|---|---|
| `InvalidConfig(reason)` | `new` | A `StackConfig` rule failed, or the start time is below zero. | Fix the config. |
| `IpMtuBelowIpv4Minimum { mtu }` | `new` | The port's IP MTU is below 68. | Use a larger MTU. |
| `RxPoolTooSmall { count, required }` | `new` | The RX pool is below its reserve. | Make the pool at least `required`. |
| `TxPoolTooSmall { count, required }` | `new` | The TX pool is below its reserve. | Make the pool at least `required`. |
| `TxFramePoolTooSmall { count, required }` | `new` | The frame pool is below its reserve. | Make the pool at least `required`. |
| `TimeWentBackwards { last, now }` | `turn`, `receive` | `now` is earlier than the last time. Nothing was done. | Use one clock that never goes back. |
| `Port(PortError)` | drive calls | The NIC driver failed, or the port could not be recovered after a reset. The Stack is unusable. | Close it: `into_port`, then `try_close`. |

[Packet-buffer pools](../nic/pools.md) explains the reserves.

## Sockets

| Error | From | Meaning |
|---|---|---|
| `AddTcpError::SocketsFull(config)` | `add_tcp` | Every TCP slot is taken (sockets still closing after their handle was dropped count too). Your config comes back. |
| `AddUdpError::InvalidConfig(reason)` | `add_udp` | Port or `rx_queue` is zero. |
| `AddUdpError::Unaddressable` | `add_udp` | `local_addr` is not an address the Stack owns. |
| `AddUdpError::PortInUse` | `add_udp` | That address (or all addresses) and port are already bound. |
| `AddUdpError::Full` | `add_udp` | Every UDP slot is taken. |
| `tcp::ConnectError::InvalidState` | `connect` | The socket is not closed. |
| `tcp::ConnectError::Unaddressable` | `connect` | A zero port, an unspecified remote, a local address the Stack does not own, or no source address. |
| `tcp::ConnectError::AddressInUse` | `connect` | Another live socket, including one in TIME-WAIT, holds the same addresses and ports. |
| `tcp::ListenError::Unaddressable` | `listen` | Port 0, or an address the Stack does not own. |
| `tcp::ListenError::InvalidState` | `listen` | The socket is open and not already listening on that endpoint. |
| `tcp::RecvError::Finished` | `recv` | The peer sent FIN and every byte before it was read. |
| `tcp::RecvError::InvalidState` | `recv` | The receive side is not open: reset, or not established. |
| `tcp::SendError::InvalidState` | `send_slice` | The transmit side is not open. A full queue is `Ok(0)`, not an error. |
| `tcp::SendFrameError::{Foreign, InvalidState, Empty, Full}` | `tcp_send_frame` | Another Stack's frame; may not send; zero length; longer than the free transmit bytes. The frame comes back. |
| `UdpConnectError::Invalid` | `UdpSocket::connect` | The peer is not unicast IPv4, or its port is 0. |
| `UdpSendError` | `udp_send`, `udp_send_frame` | See the table on [UDP sockets on the Stack](udp.md#send-errors). |
| `UdpSendFrameError::Foreign` | `udp_send_frame` | The frame came from another Stack's pool. The frame comes back. |

None of the socket errors changes anything: the socket, the slot and the frame are as they were before the call.

## Panics

These are documented panics. Each one points to a bug in the calling code, not to something that happens on the network.

- Using a `TcpHandle` or `UdpHandle` on a Stack other than the one that made it: "TCP handle belongs to another Stack" (or UDP).
- `add_tcp` with `rx_capacity` over 1 GiB, or `mss_cap: Some(0)`.
- `tcp::tx_mbuf_budget` with an IP MTU below 68, or `mss_cap: Some(0)`.
- `set_initial_window(0)`; `set_rto` with zero or more than 60 s; `set_ack_delay` with more than 60 s; `set_hop_limit(Some(0))`.
- A readiness key used after its socket was removed, when its slot no longer exists (release builds; debug builds check every key).
- `turn`, `receive` and `send`, when the NIC driver's cleanup of sent packets fails with an error other than "not supported".
- `turn`, `receive`, `send` and `flush`, with TX checksum offload on, when the driver refuses to prepare a queued packet.

A panic that unwinds out of `turn`, `receive`, `send` or `flush` leaves the Stack unusable: drop it or call `into_port`, nothing else.

This is the end of the part on using the Stack directly. The next part, [under the hood](../blocks/index.md), describes the pieces under the junction backend.
