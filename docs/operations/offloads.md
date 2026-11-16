# Offloads and hardware timestamps

Both are set in the [network config file](config.md), decided when the card opens, and printed in the report that `switch-nic info` and your program print after opening.

## Checksum offloads

Without offloads, the program computes the checksums of every packet it sends and checks those of every packet it receives, on its own CPU. With them, the card does that work:

| Setting | What the card does |
|---|---|
| `rx_checksum` | checks the IPv4, TCP and UDP checksums of received packets |
| `tx_tcp_checksum` | fills in the TCP checksum of sent packets |
| `tx_udp_checksum` | fills in the UDP checksum of sent packets |

`offloads` is either an explicit object, requested as written:

```json
"offloads": {"rx_checksum": true, "tx_tcp_checksum": true, "tx_udp_checksum": false}
```

or `"auto"`. If the driver does not offer an offload you requested explicitly, opening fails and names it:

```text
port open: port setup failed: InvalidSettings("PMD lacks the requested TX TCP checksum offload")
```

Turn that setting off and open again. A refused offload never falls back to the CPU silently.

## What auto requests

`"auto"` picks, per driver, a set of offloads that works with it:

| Driver | Requests | Reason printed |
|---|---|---|
| `net_ena` (Amazon ENA) | each of the three that the driver offers | `ena: requested because advertised` or `ena: not advertised` |
| `net_ice` (Intel E810) | each of the three that the driver offers | `ice: ...`, as for ENA |
| `net_i40e` (Intel X710 family) | `rx_checksum` if offered; no send offloads | `i40e: ...` for receive; `i40e: vector TX allows no checksum offloads` |
| `net_ixgbe` (Intel 82599, X5xx) | nothing | `ixgbe: off until verified on an Intel host`; `ixgbe: vector TX needs zero TX offloads` |
| any other, including `net_tap` | nothing | `unknown driver <name>: none requested; set offloads explicitly` |

The i40e and ixgbe drivers have a fast send path that works only with no send offloads, which is why `auto` leaves those off. For a driver not in the table, set the offloads explicitly if you want any.

The report prints one line per offload, with the reason; with an explicit object the reason is `explicit`:

```text
rx_checksum: requested (ena: requested because advertised)
tx_tcp_checksum: requested (ena: requested because advertised)
tx_udp_checksum: requested (ena: requested because advertised)
```

## Hardware receive timestamps

With `"rx_timestamps": true`, the card records when each packet arrived, by its own clock, and the program reads that value with `rx_timestamp()` on a received `switch::net::TcpFrame`, `switch::net::Datagram` or WebSocket message (API reference, `make docs`). It is the driver's raw value; on ENA it is nanoseconds of the card's clock. A packet without a stamp, or any packet with `rx_timestamps` off, reports 0.

`"auto"` never turns timestamps on. If the driver cannot stamp received packets, opening fails with:

```text
port open: port setup failed: InvalidSettings("PMD lacks the requested RX timestamp offload")
```

Which drivers offer receive timestamps, in the DPDK the repository builds:

| Driver | Timestamps |
|---|---|
| `net_ena` | when the instance's ENA supports them: nanoseconds of the card's clock. On EC2 that needs the instance family and placement group described in [Running on EC2](ec2.md#before-you-log-in). |
| `net_ice` | yes, from the card's clock |
| `net_i40e`, `net_ixgbe`, `net_tap` | no |
| `net_af_packet` | yes, but they are the Linux kernel's receive time in nanoseconds, taken in software, not by the card |

`switch-nic info` shows whether a card offers them: bit 14 (`0x4000`) of `rx_offload_capa` is set.

Two drivers add a line to the report when timestamps are on:

```text
ice: RX timestamp forces scalar RX
ena: stamps are device ns on the Time Sync clock
```

On ice, timestamps make the driver use its scalar receive path instead of its vector one.
