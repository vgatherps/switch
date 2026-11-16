# switch-nic: checking a NIC and serving load

`switch-nic` is a ready-made program built on `switch_open::open`. Point it at a [network config file](config.md) and it opens the card, prints what it found, and then does one of three things: nothing more (`info`), answer sockperf load-test clients (`sockperf-server`), or read a live market-data feed over TLS (`feed`). [Preparing a Linux machine](host-setup.md) ends by running `switch-nic info`, and [Running on EC2](ec2.md) uses `info` and `feed` on a prepared host; this page documents all three modes. Use it to check that a machine is set up correctly before you run your own program on it, and as a reference for what a complete owning-the-NIC program looks like.

```text
switch-nic --config FILE <MODE> [MODE OPTIONS]

  info
  sockperf-server [--udp-ports P[,P..]] [--tcp-port P] [--tcp-connections N]
                  [--seconds S] [--report-seconds R]
  feed [--host H] [--port P] [--path P] [--print N] [--seconds S]
```

`--config` is required and comes before the mode. Every option may appear at most once, and the mode's own options come after the mode name. Like any program that opens a card, it must run on exactly one CPU:

```sh
cargo build --locked --release -p switch-nic
taskset -c 3 target/release/switch-nic --config dut.json info 2>&1 | tee info.log
```

Results go to stdout, one per line, in the form `switch-nic: <key>: <value>`. DPDK and its NIC drivers write their own messages to stderr, and those often explain a failure, so capture both streams as above.

## info

`info` opens the card, prints the report, and closes it again. It is the check that a machine is ready: if `info` exits 0, your own program can open the same config.

```text
switch-nic: driver: net_ena
switch-nic: mac: 0a:12:34:56:78:9a
switch-nic: cpu: 3
switch-nic: port_id: 0
switch-nic: rx_offload_capa: 0x...
switch-nic: tx_offload_capa: 0x...
switch-nic: max_rx_pktlen: ...
switch-nic: min_mtu: ...
switch-nic: max_mtu: ...
switch-nic: descriptors: rx=1024 tx=1024
switch-nic: rx_pool_count: 32767
switch-nic: tx_pool_count: ...
switch-nic: rx_checksum: requested (ena: requested because advertised)
switch-nic: tx_tcp_checksum: requested (ena: requested because advertised)
switch-nic: tx_udp_checksum: requested (ena: requested because advertised)
switch-nic: link_speed_mbps: ...
switch-nic: link_duplex: full
switch-nic: link_status: up
switch-nic: gateway: 10.0.1.1 at ... after 1 ms
switch-nic: hugepages: hugepages-2048kB used=... free=...
switch-nic: eth_stats: ipackets=... opackets=... imissed=... ierrors=... oerrors=... rx_nombuf=...
switch-nic: port_close: ok
```

The first block is the report from [`open`](open.md#what-you-get-back). The `hugepages` lines, one per hugepage size, show how many pages this process took while opening (`used`) and how many are still free. `eth_stats` are the card's own counters: packets in and out, packets it dropped because no receive buffer was free (`imissed`, `rx_nombuf`), and errors. `port_close: ok` means every packet buffer came back.

A useful habit: run `info` once with `"offloads": "auto"` and once with a copy of the config where you set explicit offloads and `rx_timestamps` the way you want them. If the driver refuses something, the second run says which (see [Offloads and hardware timestamps](offloads.md)).

## sockperf-server

`sockperf-server` answers clients of the latency test tool [sockperf](https://github.com/Mellanox/sockperf) (version 3.10) running on another machine. It replies the way `sockperf server` does, so you can measure the card and the Stack with an unmodified sockperf client.

| Option | Default | Meaning |
|---|---|---|
| `--udp-ports P[,P..]` | `11111` | UDP ports to answer on, one socket each; no duplicates |
| `--tcp-port P` | none | TCP port to accept connections on |
| `--tcp-connections N` | 8 | how many TCP connections may be open at once |
| `--seconds S` | 0 | stop after S seconds; 0 runs until SIGINT or SIGTERM |
| `--report-seconds R` | 5 | print counters every R seconds |

```sh
taskset -c 3 switch-nic --config dut.json sockperf-server --udp-ports 11111 --tcp-port 11112 \
    --tcp-connections 16 2>&1 | tee server.log
```

On the client machine:

```sh
sockperf ping-pong -i 10.0.1.23 -p 11111 -t 10 --msg-size 64
sockperf ping-pong -i 10.0.1.23 -p 11112 --tcp -t 10 --msg-size 64
```

When it is listening, the server prints `udp_listen` and `tcp_listen` lines and then `switch-nic: server: ready`. Each report starts with `report: periodic` (or `report: final` at the end) and has the message counts per protocol (`received`, `replies`, `malformed`, and for TCP `accepted` and `closed`), one `udp_socket` line per UDP port, one `tcp_socket` line per connection slot with its state and retransmission counters, `stack_totals` and `eth_stats`. With `rx_timestamps` on, it also prints how many received packets carried a hardware timestamp.

Replies keep their order per socket, not across sockets, so give each sockperf client its own UDP port and allow at least as many TCP connections as clients. A UDP message larger than the path MTU minus 28 bytes would need IP fragmentation, which the Stack does not do; keep `--msg-size` below that (1472 at MTU 1500).

On SIGINT or SIGTERM, or when `--seconds` runs out, the server closes its TCP connections with FIN, waiting at most five seconds, prints the final report and closes the card. If the connections do not close in time, or a UDP reply is still waiting to be sent, it exits 3.

The wire format is small: every sockperf message starts with a 14-byte header whose bytes 10 to 13 hold the message length (big-endian) and whose flags say whether the client wants a reply. A UDP datagram must be exactly one message. On TCP, messages are cut out of the byte stream by their length field; a length below 14 or above 65507 marks the stream malformed and the server resets that connection. The crate `switch-sockperf-wire` implements this framing.

## feed

`feed` connects to Binance's public market-data WebSocket over TLS, through the card, and prints the first few messages. It is the same client as the [market-data feed walkthrough](../protocols/binance-feed.md), run on the Stack this program opened, and it is how you check that a real card reaches the internet.

| Option | Default | Meaning |
|---|---|---|
| `--host H` | `stream.binance.com` | server name, for DNS, TLS and the HTTP `Host` header |
| `--port P` | `9443` | TCP port |
| `--path P` | `/ws/btcusdt@bookTicker` | WebSocket path |
| `--print N` | 10 | how many messages to print |
| `--seconds S` | 30 | how long to read, counted from when the WebSocket opens |

`feed` needs a `gateway` in the config; without one it stops with `feed requires a gateway in the config`. It resolves the host name before it opens the card, and prints `resolved: <host> -> <address>` and `feed_mss_cap: <n>`, the TCP segment limit it took from the path MTU toward that address. After the card opens it prints `phc_check`: the difference between the card's clock and the system clock, read once, or `unsupported` if the driver cannot read its clock. It is logged, not checked.

The feed runs as a switch task on a `Net` over the Stack, and the drive loop busy-polls. SIGINT or SIGTERM tells the feed task to stop early. It fails (exit 3) if no message arrived. The lines it prints about messages and latency are described in [the walkthrough](../protocols/binance-feed.md). If the card was reset during the run, the latency figures are reported as invalid, because the card's clock may have jumped ([Device resets](offloads.md#device-resets)).

## Exit codes

| Code | Meaning |
|---|---|
| 0 | success |
| 2 | packet buffers were still held when the card was closed: `port_close: buffers still in use: rx=.. tx=.. frame=..`. This code replaces 0 or 3 from the run itself. |
| 3 | an argument, setup or run error; the `switch-nic: error:` line (or `port_open_error`) says which |
| 101 | a panic |

An argument error prints `switch-nic: error: <problem>` followed by the usage text. When `open` fails after the card's port exists, `switch-nic` still closes the port and prints `port_close`; [Opening the NIC from code](open.md#when-it-fails) shows that code, and [When the NIC won't open](troubleshooting.md) lists the messages.

Next: [When the NIC won't open](troubleshooting.md).
