# Offloads and hardware timestamps

An offload is work the network card does so the CPU does not have to. The previous page kept the [pinned CPU](cpus.md) free of other threads; offloads take some per-packet work off it as well. The Stack can hand three kinds of checksum work to the card, and it can ask the card to stamp every received packet with the time it arrived. Both are set in the [network config file](config.md), resolved when the card opens, and printed in the report `open` returns.

## Checksum offloads

Every IPv4, TCP and UDP packet carries checksums. Without offloads, the Stack computes them on the drive-loop CPU for everything it sends and checks them for everything it receives. With offloads, the card does that work:

| Setting | What the card does |
|---|---|
| `rx_checksum` | checks IPv4, TCP and UDP checksums of received packets |
| `tx_tcp_checksum` | fills in the TCP checksum of sent packets |
| `tx_udp_checksum` | fills in the UDP checksum of sent packets |

The `offloads` key takes either an explicit object or the string `"auto"`.

An explicit object is requested as written:

```json
"offloads": {"rx_checksum": true, "tx_tcp_checksum": true, "tx_udp_checksum": false}
```

If the NIC driver does not offer a requested offload, opening the NIC port fails, and `open` returns `PortOpen` with a message naming the missing offload, for example:

```text
port open: port setup failed: InvalidSettings("PMD lacks the requested TX TCP checksum offload")
```

Turn that setting off and open again. A refused offload never silently falls back to the CPU.

## What auto chooses, driver by driver

`"auto"` looks at the NIC driver's name and the offloads it advertises, and picks a subset known to work with that driver:

| NIC driver | Requests | Reason printed in the report |
|---|---|---|
| `net_ena` (Amazon ENA) | each of the three that the driver advertises | `ena: requested because advertised` / `ena: not advertised` |
| `net_ice` (Intel E810) | each of the three that the driver advertises | `ice: requested because advertised` / `ice: not advertised` |
| `net_i40e` (Intel X710 family) | `rx_checksum` if advertised; no transmit offloads | `i40e: requested because advertised` / `i40e: not advertised`; `i40e: vector TX allows no checksum offloads` |
| `net_ixgbe` (Intel 82599, X5xx) | nothing | `ixgbe: off until verified on an Intel host`; `ixgbe: vector TX needs zero TX offloads` |
| any other, including `net_tap` | nothing | `unknown driver <name>: none requested; set offloads explicitly` |

The i40e and ixgbe drivers have a fast transmit path that only works when no transmit offloads are on, which is why `auto` leaves those off. For drivers not in the table, set the offloads explicitly if you want any.

The report prints one line per checksum offload with the choice and its reason:

```text
rx_checksum: requested (ena: requested because advertised)
tx_tcp_checksum: requested (ena: requested because advertised)
tx_udp_checksum: requested (ena: requested because advertised)
```

With an explicit object the reason is `explicit`.

## Hardware receive timestamps

With `"rx_timestamps": true`, the card records when each packet arrived, by its own clock, and the Stack carries that value with the packet to your code. A received TCP frame or UDP datagram returns it from `rx_timestamp()`, and [switch-ws messages](../protocols/messages.md) pass it on. Read it before you turn the received data into a plain frame with `into_frame()`, which does not keep it (see [Frames](../net/frames.md)).

The value is the raw value from the NIC driver, 0 = none; units are the driver's (ENA: nanoseconds of the NIC clock). A packet the driver did not stamp, or any packet when `rx_timestamps` is off, reports 0.

`rx_timestamps` is never turned on by `"auto"`; it is requested exactly as written. If the NIC driver does not offer it, opening the port fails with `InvalidSettings("PMD lacks the requested RX timestamp offload")`. The `rx_offload_capa` line of the report shows the driver's receive capabilities; timestamps are DPDK's receive offload bit 14 (`0x4000`).

Two drivers add a note to the report when timestamps are on:

```text
ice: RX timestamp forces scalar RX
ena: stamps are device ns on the Time Sync clock
```

On ice, turning timestamps on makes the driver use its scalar receive path instead of its vector one.

## Reading the NIC clock

To compare a hardware timestamp with something else, you need the card's clock as well. `dpdk_smoltcp::read_clock(port_id)` reads it:

```rust,ignore
let phc = dpdk_smoltcp::read_clock(port_id);
```

It returns `Ok(value)`, the card's clock (ENA: nanoseconds of the NIC clock the timestamps use), or `Err` with a negative error number; `-ENOTSUP` means the driver cannot read its clock. Unlike everything else on the Stack, `read_clock` may be called from any thread. The port id is in the `port_id` line of the report and the `report.port_id` field. `switch-nic feed` uses it at startup to log how far the card's clock is from the system clock (see [switch-nic](switch-nic.md)).

## Device resets

Some cards can ask to be reset when they detect a fault; ENA's health watchdog does this. The Stack checks for such a request once a second, during a `turn` or `receive` call. When there is one, it resets the NIC port, configures and starts it again, and carries on. Your sockets stay as they were. Packets the card was holding go back to their pools, and TCP resends whatever was lost. If recovery fails, the drive call returns `StackError::Port`, and every later call fails as well; close the Stack.

`dpdk_smoltcp::port_resets()` counts the resets recovered so far in this process. The card's clock may not continue smoothly across a reset, so a program that compares hardware timestamps over a long run should check this counter: `switch-nic feed` reads it before and after its run and marks its latency figures invalid if it changed.

Next: [Preparing a Linux machine](host-setup.md).
