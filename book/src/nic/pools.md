# Packet-buffer pools

Every byte the Stack receives or sends sits in a packet buffer: a fixed-size block of memory that the NIC can read from and write to directly. DPDK calls these buffers mbufs. They are not allocated one at a time while the program runs. They come from pools, fixed sets of packet buffers made once when the NIC port opens. This page explains the three pools a Stack uses, what each one is for, how [`switch_open::open`](open.md) sizes them from your `Sizing`, and what your program sees when one of them runs out.

If you opened the NIC with `open` in [your first network program](../getting-started/first-connection.md), you already filled in a `Sizing`. This page tells you what those numbers mean.

## Three pools and who draws on each

A NIC port has three pools:

```text
                      arriving packets
 NIC --------------> RX pool -------> TcpFrame / UdpDatagram / Datagram
                     (fixed size)      held by your code until dropped

                     packets the Stack builds itself
 NIC <-------------- TX pool <------- ACKs, ARP, resets,
                     (exactly its        TCP segments sent by copy
                      reserve with open)

                     data your code sends
 NIC <-------------- frame pool <---- send_slice, tx_frame, try_send
                     (reserve +          TCP keeps each frame until
                      frame_share)       the peer acknowledges it
```

The RX pool holds arriving packets. The NIC driver takes an empty buffer from it for each packet it receives. When the Stack hands you received data, as a `TcpFrame` or `UdpDatagram` on the Stack or a `TcpFrame` or `Datagram` in switch-net, that value still owns its packet buffer. The buffer goes back to the RX pool when you drop the value (or the last clone of the frame you made from it).

The TX pool holds packets the Stack builds on its own (acknowledgements, ARP requests and replies, resets) and TCP segments it sends by copy. A TCP segment goes out as a copy when it cannot be sent as one whole frame: a frame longer than the segment, a shared or sliced frame, a segment that spans frames, or part of a frame being retransmitted. [TCP sockets on the Stack](../stack/tcp.md#handing-over-a-frame-tcp_send_frame) says more about when that happens.

The frame pool holds the data your program sends. On the Stack, `send_slice` copies bytes into frames from this pool, and `Stack::tx_frame` hands you an empty one to fill. In switch-net, `NetHandle::tx_frame` does the same on the local backend. A TCP socket keeps each frame in its send queue until the peer acknowledges the frame's last byte; a small frame you hand over may instead be copied into the frame already at the end of the queue and freed at once. A UDP frame returns to the pool once the NIC has sent it.

## Reserve and share

Each pool is sized in two parts. The reserve is what the Stack and the NIC driver need for themselves, whatever your sockets do. The share is what all your sockets and your own code may hold at once on top of that.

```text
 RX pool    [ reserve: driver ring + low mark + staging + cache | share: held received data ]
 TX pool    [ reserve: TX descriptors + tx_pending                                          ]
 frame pool [ reserve: TX descriptors + tx_pending + 1 | share: queued + unacknowledged    ]
```

Nothing is reserved per socket. Every socket draws on the same shares. A socket added after the Stack starts is never refused because a pool is too small; `add_tcp` only fails when every socket slot is taken. A pool that is too small for many busy sockets shows up later, as the backpressure described in [Running dry](#running-dry) below.

`Stack::new` checks each pool against a reserve it computes from the descriptor counts and queue sizes, and refuses to start if one is below it. The error names the pool, its size and the size it needs: `RxPoolTooSmall { count, required }`, `TxPoolTooSmall { count, required }` or `TxFramePoolTooSmall { count, required }`. Through `switch_open::open` these arrive inside `OpenError::Stack`. Enlarge the named pool to at least `required`. When you open the port by hand you compute the reserves yourself; [Sizing pools by hand](#sizing-pools-by-hand) gives the formulas.

## Filling `Sizing`

`switch_open::open` builds all three pools from the network config file and one `Sizing` value. It fixes some numbers itself and takes the rest from you:

| Pool | Size with `open` |
|---|---|
| RX pool | 32767 packet buffers, always |
| TX pool | `tx_reserve(TX descriptors, tx_pending)` |
| frame pool | `frame_reserve(TX descriptors, tx_pending)` + `frame_share` |

The RX pool's fixed 32767 is large enough to stay above its low mark in practice; at MTU 1500 it takes about 82 MiB of hugepages, at MTU 9001 about 306 MiB. `open` also fixes the receive burst at 128 packets, the transmit burst at 32 and the pool cache at 8. The descriptor counts come from the config file.

The fields of `Sizing` are:

| Field | What it controls |
|---|---|
| `frame_share` | Frames all TCP send queues and your code may hold at once, beyond the frame pool's reserve. |
| `max_sockets` | TCP socket slots. Must be positive. |
| `max_udp_sockets` | UDP socket slots. Zero disables UDP. |
| `rx_staging` | Received packets the Stack may hold before processing them. Counts towards the RX reserve. |
| `tx_pending` | Packets built but not yet accepted by the NIC driver. Counts towards both the TX and the frame reserve. |
| `neighbor_capacity` | ARP cache entries. `dpdk_smoltcp::DEFAULT_NEIGHBOR_CAPACITY` is 256. |

The number that needs thought is `frame_share`. A TCP socket can hold up to its `tx_capacity` bytes in frames: queued data plus data sent but not yet acknowledged. `tcp::tx_mbuf_budget(&config, ip_mtu)` estimates how many frames that is when every frame is a full segment: `tx_capacity` divided by the segment payload (IP MTU − 40, or `mss_cap` if smaller), rounded up, and never less than 6. Smaller writes use more frames for the same bytes. The function panics for an IP MTU below 68 or `mss_cap == Some(0)`.

The Stack client from [Programming the Stack directly](../stack/index.md#a-complete-client), like the first network program, has one socket, so its share is one socket's budget:

```rust,ignore
{{#include ../../../conduit/crates/switch-open/examples/tcp_client.rs:sizing}}
```

A program with many connections adds the budgets of the sockets that may be sending at the same time, plus room for frames it holds itself.

### The Stack's own queues: `rx_staging`, `tx_pending` and the ingress budget

Two `Sizing` fields size queues inside the Stack rather than pools, and both appear in the reserves.

`rx_staging` is how many received packets the Stack may hold between taking them from the NIC and processing them. Each receive step processes at most `ingress_budget` packets (an argument to `Stack::receive` and `Stack::turn`, a field of switch-net's `NetConfig`). Packets beyond that wait in staging for the next receive step, which reports them in `Turn::rx_staged`. See [the Stack's drive loop](../stack/drive-loop.md).

`tx_pending` is how many built packets may wait for the NIC driver to accept them. When it is full, the send step can only flush and UDP sends return `WouldBlock`. Receiving goes on: a reply built while receiving (an ACK, a reset, an ARP reply) that finds no room is dropped.

## Running dry

When a pool has no free buffer, nothing fails loudly. Each pool produces a different kind of backpressure:

| Pool runs dry | What happens | What frees it |
|---|---|---|
| RX pool | The NIC driver has no buffer for arrivals and drops them. TCP peers retransmit; UDP senders are not told. | Read queued data and drop received frames you hold. |
| TX pool | The Stack cannot build packets. Sockets that want to send wait for a later send step; replies built while receiving are dropped. Receiving itself goes on. | The driver finishing earlier sends. |
| frame pool | `send_slice` queues what fits and may return `Ok(0)`; `Stack::tx_frame` returns `None`; `udp_send` returns `WouldBlock`; in switch-net `tx_frame().await` waits. | Peers acknowledging TCP data; dropping frames your code holds; the driver finishing UDP sends. |

On the Stack nothing tells you when frames come back. A program that got `Ok(0)` from `send_slice` or `None` from `tx_frame` tries again on a later time through its loop; `Stack::free_frames()` says how many frames it could take now.

The RX pool also has a low mark: one full refill of the receive ring plus one receive burst. After each receive step that took in packets, and after every receive step while the pool is below the mark, the Stack checks the pool. While it is below, the TCP sockets holding the most out-of-order frames (data that arrived after a gap) release them, starting with the data farthest past the gap, until the pool is back at the mark. In-order data is never released this way. The peer retransmits what was released. This keeps the driver supplied with buffers. Separately, one socket never holds more than 1024 out-of-order frames: it refuses further out-of-order segments and counts them in `rx_refused_segments()`.

Two consequences follow for TCP. `tcp::Config::rx_capacity` is the receive window the socket advertises, not a cap on received data: a socket accepts contiguous data even past its advertised edge, and frames you hold after reading them still come from the shared RX pool. And `tx_capacity` together with the frame pool bounds unacknowledged data, because a queued frame is held until the peer acknowledges its last byte.

## Values used in this repository

There is no single right size. These are the values the programs in this repository use, as a starting point:

| Program | `frame_share` | `rx_staging` | `tx_pending` |
|---|---|---|---|
| Stack client (`switch-open` example `tcp_client`) and the first network program | one 64 KiB socket's `tx_mbuf_budget` | 32 | 32 |
| `switch-bench serve`, TCP and WebSocket | sockets × `tx_mbuf_budget` + 64 | 128 | 64 |
| `switch-bench serve`, UDP | 64 | 128 | 64 |
| `switch-nic` | one socket's `tx_mbuf_budget`, shared by all | 32 (128 in `feed` mode) | 32, or the TCP connection count if larger |
| `switch-junctiond` | 4096 | 128 | 64 |

On the junction backend your program does not size these pools; the daemon does. The application-side frame count there is `RemoteConfig::tx_frames`, which `switch-bench` sets to the same `frame_share` it would use locally. See [connecting an application through switch-net](../junction/apps.md).

## Sizing pools by hand

If you open the port yourself instead of through `open`, you fill a `PortConfig` with one `PoolConfig` per pool and choose every count. The [by-hand page](../stack/by-hand.md) walks through it. The rule is the same: each count is its reserve plus a share. `dpdk-smoltcp` exports the reserve formulas as functions:

| Function | Value |
|---|---|
| `rx_reserve(rx_descriptors, rx_burst, rx_staging, rx_cache_size)` | max(rx_descriptors, 32) + max(rx_descriptors, 32) + rx_burst + rx_staging + floor(3 × rx_cache_size / 2) |
| `tx_reserve(tx_descriptors, tx_pending)` | tx_descriptors + tx_pending |
| `frame_reserve(tx_descriptors, tx_pending)` | tx_descriptors + tx_pending + 1 |

The RX reserve covers the buffers the driver keeps posted in its receive ring, one more ring refill plus one receive burst (the low mark, described in [Running dry](#running-dry)), the Stack's staging queue and one CPU's pool cache. The TX and frame reserves cover packets the NIC driver may still hold after sending and packets waiting in the Stack's pending queue. Descriptor counts here are the ones the driver actually agreed to (`Port::descriptor_counts()`), which can differ from the ones you asked for.

The tap echo harness gives its RX pool one window of full segments as share and its frame pool one socket's budget:

```rust,ignore
{{#include ../../../conduit/crates/switch-tap-echo/src/driver.rs:port_config}}
```

Next: [the pinned CPU and helper threads](cpus.md). If a pool error stops your program from starting, [when the NIC won't open](troubleshooting.md) lists the messages.
