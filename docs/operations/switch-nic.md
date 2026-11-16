# switch-nic

`switch-nic` opens a card from a [network config file](config.md), prints what it found, and then either stops (`info`), answers sockperf latency-test clients (`sockperf-server`), or reads a live market-data feed over TLS (`feed`). Use `info` to check that a machine is ready before running your own program.

```text
switch-nic --config FILE <MODE> [MODE OPTIONS]

  info
  sockperf-server [--udp-ports P[,P..]] [--tcp-port P] [--tcp-connections N]
                  [--seconds S] [--report-seconds R]
  feed [--host H] [--port P] [--path P] [--print N] [--seconds S]
```

`--config` comes before the mode, and each option may appear once. Build it from the repository root on a machine with DPDK:

```sh
cargo build --locked --release -p switch-nic
```

Like every program that opens a card, it must be allowed on exactly one CPU ([CPUs](cpus.md)). Run it as root for a real card:

```sh
sudo taskset -c 3 target/release/switch-nic --config dut.json info 2>&1 | tee info.log
```

Results go to stdout, one per line, as `switch-nic: <key>: <value>`. DPDK writes its own messages to stderr, and they often explain a failure, so keep both streams.

| Exit status | Meaning |
|---|---|
| 0 | success |
| 2 | the run succeeded, but packet buffers were still in use when the card closed: `port_close: buffers still in use: rx=.. tx=.. frame=..` |
| 3 | an argument, setup or run error; the `switch-nic: error:` or `switch-nic: port_open_error:` line says which |
| 101 | a panic |

An argument error prints `switch-nic: error: <problem>` followed by the usage text.

## info

`info` opens the card, prints the report, and closes it. If `info` exits 0, your program can open the card with the same config. This is a run on the tap device from [the config page](config.md#two-examples):

```text
switch-nic: driver: net_tap
switch-nic: mac: c6:95:8a:be:84:01
switch-nic: cpu: 3
switch-nic: port_id: 0
switch-nic: rx_offload_capa: 0x000000000000200e
switch-nic: tx_offload_capa: 0x000000000000802e
switch-nic: max_rx_pktlen: 16128
switch-nic: min_mtu: 46
switch-nic: max_mtu: 65535
switch-nic: descriptors: rx=1024 tx=1024
switch-nic: rx_pool_count: 32767
switch-nic: tx_pool_count: 1056
switch-nic: rx_checksum: not requested (explicit)
switch-nic: tx_tcp_checksum: not requested (explicit)
switch-nic: tx_udp_checksum: not requested (explicit)
switch-nic: link_speed_mbps: 10000
switch-nic: link_duplex: full
switch-nic: link_status: up
switch-nic: gateway: none
switch-nic: hugepages: hugepages-1048576kB used=0 free=0
switch-nic: hugepages: hugepages-2048kB used=0 free=0
switch-nic: hugepages: hugepages-32768kB used=0 free=0
switch-nic: hugepages: hugepages-64kB used=0 free=0
switch-nic: eth_stats: ipackets=0 opackets=0 imissed=0 ierrors=0 oerrors=0 rx_nombuf=0
switch-nic: port_close: ok
```

| Line | Meaning |
|---|---|
| `driver` | DPDK's driver for the card, such as `net_ena`, `net_ice` or `net_tap` |
| `cpu` | the CPU the program is pinned to |
| `port_id` | DPDK's number for the card |
| `rx_offload_capa`, `tx_offload_capa` | the offloads the driver offers, as DPDK bit masks |
| `max_rx_pktlen`, `min_mtu`, `max_mtu` | the packet sizes the card accepts; `mtu` in the config must lie between `min_mtu` and `max_mtu` |
| `descriptors` | the ring sizes the driver chose |
| `rx_pool_count`, `tx_pool_count` | packet buffers in the receive and send pools |
| `rx_checksum`, `tx_tcp_checksum`, `tx_udp_checksum` | each offload, whether it was requested and why ([Offloads](offloads.md)) |
| `link_speed_mbps`, `link_duplex`, `link_status` | the link as the card reports it |
| `gateway` | `none`, or the gateway's address, Ethernet address and how long it took to answer ARP: `gateway: 10.0.1.1 at 0a-12-34-56-78-01 after 1 ms` |
| `hugepages` | per hugepage size, the pages this process took (`used`) and those still free |
| `eth_stats` | the card's counters: packets in and out, packets dropped because no buffer was free (`imissed`, `rx_nombuf`), and errors |
| `port_close` | `ok` when every packet buffer came back |

To find out which offloads a card accepts, run `info` once with `"offloads": "auto"`, then with a copy of the config that sets them explicitly. If the driver refuses one, the second run fails and says which ([Offloads](offloads.md)).

## sockperf-server

`sockperf-server` answers clients of [sockperf](https://github.com/Mellanox/sockperf) 3.10 running on another machine, the way `sockperf server` does, so an unmodified sockperf client measures the card and the network stack.

| Option | Default | Meaning |
|---|---|---|
| `--udp-ports P[,P..]` | `11111` | UDP ports to answer on, one socket each |
| `--tcp-port P` | none | TCP port to accept connections on |
| `--tcp-connections N` | 8 | TCP connections open at once; only with `--tcp-port` |
| `--seconds S` | 0 | stop after S seconds; 0 runs until SIGINT or SIGTERM |
| `--report-seconds R` | 5 | print counters every R seconds |

```sh
sudo taskset -c 3 switch-nic --config dut.json sockperf-server --udp-ports 11111 --tcp-port 11112 \
    --tcp-connections 16 2>&1 | tee server.log
```

On the client machine:

```sh
sockperf ping-pong -i 10.0.1.23 -p 11111 -t 10 --msg-size 64
sockperf ping-pong -i 10.0.1.23 -p 11112 --tcp -t 10 --msg-size 64
```

Once listening, the server prints `udp_listen` and `tcp_listen` lines and `switch-nic: server: ready`. Each report starts with `report: periodic`, or `report: final` at the end, followed by message counts per protocol (`udp:` and `tcp:` lines with `received`, `replies` and `malformed`, plus `accepted` and `closed` for TCP), one `udp_socket` line per port, one `tcp_socket` line per connection slot with its state and retransmissions, `stack_totals` and `eth_stats`. With `rx_timestamps` on, `udp_timestamps` and `tcp_timestamps` lines count the packets that carried a hardware timestamp.

Replies keep their order per socket, not across sockets, so give each sockperf client its own UDP port, and allow at least as many TCP connections as clients. Keep `--msg-size` at or below the path MTU minus 28 (1472 at MTU 1500): larger UDP messages need IP fragmentation, which switch does not do. A malformed TCP message resets that connection.

On SIGINT, SIGTERM or the end of `--seconds`, the server closes its TCP connections, waiting up to five seconds, prints the final report and closes the card. It exits 3 if the connections did not close in time or a UDP reply was still unsent.

## feed

`feed` connects to Binance's public market-data WebSocket over TLS 1.3 through the card and prints the first messages. It checks that a card reaches the internet through its gateway.

| Option | Default | Meaning |
|---|---|---|
| `--host H` | `stream.binance.com` | server name, for DNS, TLS and the HTTP `Host` header |
| `--port P` | `9443` | TCP port |
| `--path P` | `/ws/btcusdt@bookTicker` | WebSocket path |
| `--print N` | 10 | messages to print |
| `--seconds S` | 30 | how long to read, counted from when the WebSocket opens |

The config needs a `gateway`; without one `feed` stops with `feed requires a gateway in the config`. Before opening the card it resolves the host name through Linux and prints `resolved: <host> -> <address>` and `feed_mss_cap: <n>`, the largest TCP segment it will send toward that address. After opening it prints `phc_check`: how far the card's clock is from the system clock (`phc_minus_realtime_ns=<n>`), or `unsupported` when the driver cannot read its clock.

Then it prints `feed_connected`, one `feed_message` line for each of the first N messages, `feed_count`, `feed_latency`, `feed_update_ids` and `feed_close`. `feed_latency` is a histogram of the system clock at each message minus the card's receive timestamp, in nanoseconds with power-of-two buckets, plus `no_stamp`, the count of messages without a timestamp; it is only meaningful with `rx_timestamps` on and the system clock synchronised to the card's ([Running on EC2](ec2.md#running-the-market-data-feed)). If the card was reset during the run, the line reads `latency invalid: NIC clock jumped`. SIGINT or SIGTERM ends the feed early. It exits 3 if no message arrived.
