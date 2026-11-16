# switch-binance-feed

Client for Binance's bookTicker stream: TLS 1.3 and WebSocket on one switch TCP socket, run as a switch task on a switch-net `Net`. WebSocket and TLS are switch-ws's wss client (`switch_ws::switch_net::connect`) over switch-tls.

Depends on `switch-ws` (feature `switch-net`), `switch-net`, `switch`, [`switch-tap-echo`](../switch-tap-echo/README.md) (its `driver`), rustls (ring provider) and webpki-roots. `switch-nic` depends on it for the `feed` subcommand.

## API

- `feed(handle, spawner, target, socket, tls, local_port, name, print, read_for, stop, out, clock_reset)`: the whole feed as one `#[switch::task]`, which both binaries spawn: connect (within `CONNECT_LIMIT`), read updates until the oneshot `stop` finishes, `read_for` passed since the connect, or a read error, then always the close handshake (within `CLOSE_LIMIT`). It prints `{name}: {key}: {value}` lines (`feed_connected`, `feed_message` for the first `print` updates, `feed_count`, `feed_latency`, `feed_update_ids`, `feed_close` or `feed_close_error`, or `feed_stopped` if `stop` came before the connect) and puts the `Summary` (count, update-id range, `Latencies`) and the result (the read error before a close error) into `out` (`Outcome`). Each binary spawns it on its own Runtime and runs its own turn loop (`switch_net::round`): switch-binance-feed sleeps 1 ms after a round that moved nothing, `switch-nic feed` busy-polls. Runtime ticks are nanoseconds (`Clock::ns`).
- `feed_latency` is `no_stamp=N` and a log2 ns histogram of CLOCK_REALTIME at message hand-over minus the message's NIC RX stamp (`<=0` for a stamp at or after now); the host's chrony disciplines CLOCK_REALTIME to the NIC's PHC. `clock_reset` is a one-shot callback checked immediately before it: a reset prints `latency invalid: NIC clock jumped`, not the histogram. The NIC runner compares the synchronous port reset count against its pre-feed value; the local tap passes `|| false`, retaining `no_stamp` counts without hardware timestamps. No reset check runs per packet or turn.
- `resolve(host, port, path)`, `random_local_port()` (`FIRST_LOCAL_PORT`..65535), `CONNECT_LIMIT`, `CLOSE_LIMIT` and `DEFAULT_{HOST,PORT,PATH,PRINT}`: the setup both binaries share.
- `connect(handle, spawner, target, socket, tls, local_port, limit)`: opens TCP, TLS and WebSocket within one time limit and returns switch-ws's `Wss`; on an error or timeout the socket is dropped.
  TCP opening errors are `switch_net::Error` codes carried by `FeedError::Open`. A refused connect
  remains `FeedError::Closed { reset_by_peer: true, .. }`; a connect closed without reset uses
  `reset_by_peer: false`. Socket allocation and connect failures are delivered by awaiting the
  TCP connect future, within the same overall connection deadline.
- `Target { addr, host, path }`, `BookTicker<'a>` (`update_id`, `symbol`, bid and ask price and quantity as Binance's decimal strings, borrowing the received message; a JSON-escaped field is unescaped into an owned copy), `Clock`, `FeedError`.
- `Clock::sample()` returns the Stack's microsecond time and the Runtime's nanosecond tick from one monotonic clock sample; `Clock::ns()` supplies Runtime initialization. Relative sleeps count from the most recent tick supplied to the Runtime and run once a later poll reaches their deadline.
- `socket(mss_cap)`: the `tcp::Config` (256 KiB RX and TX buffers). `client_config(roots)` (TLS 1.3 only) and `webpki_client_config()` build the rustls config.

## Binary

`switch-binance-feed [--host HOST] [--port PORT] [--path PATH] [--print N] -- <tester> [args...]` runs on the switch-tap-echo driver and netdev `cdtap_echo`. It resolves HOST with the kernel resolver before DPDK starts, connects from switch (192.0.2.2/24, gateway 192.0.2.1) through the tester's NAT, prints the first N updates, and reads until the tester exits. Run it with no valid arguments for the full usage. The default host is `stream.binance.com`: `data-stream.binance.vision` speaks TLS 1.2 only, so switch-tls cannot reach it, and `stream.binance.com` may answer HTTP 451 from restricted locations. Exit statuses are switch-tap-echo's.

## Build and test

```sh
make test-binance                     # cargo test -p switch-binance-feed -- --test-threads=1
make docker-binance BINANCE_SECONDS=30   # live run; needs internet
```

`test-binance` needs DPDK 25.11.3, `CAP_NET_ADMIN` and `/dev/net/tun` (Docker image on macOS). The tests run the feed against a local TLS server with a certificate from `rcgen`. `docker-binance` uses a separate image with nftables (`BINANCE_IMAGE`) and is not part of `make test`.
