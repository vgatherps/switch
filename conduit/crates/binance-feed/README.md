# binance-feed

Client for Binance's bookTicker stream: TLS 1.3 and WebSocket on one zhida TCP socket, run as a switch task on a switchnet `Net`. WebSocket and TLS are relay's wss client (`relay::switchnet::connect`) over sheath.

Depends on `relay` (feature `switchnet`), `switchnet`, `switch`, [`tap-echo`](../tap-echo/README.md) (its `driver`), rustls (ring provider) and webpki-roots. `nic-runner` depends on it for the `feed` subcommand.

## API

- `run(net, clock, should_stop, client)`: runs `client(handle, spawner, stop)` as a switch task on a new Runtime, turning the Net until it finishes; `stop` finishes once `should_stop()` returns true. Runtime ticks are milliseconds (`Clock::ms`).
- `connect(handle, spawner, target, socket, tls, local_port, limit)`: opens TCP, TLS and WebSocket within one time limit. Returns a `Feed`; on an error or timeout the socket is dropped.
- `Feed::next(stop)`: the next `BookTicker`, or `None` once `stop` finishes (updates already received come first). Server pings are answered inside. `Feed::close(spawner, limit)` sends the WebSocket close and reads until the server's close echo; then close_notify and FIN go out, and it returns once the server acked them.
- `Target { addr, host, path }`, `BookTicker` (`update_id`, `symbol`, bid and ask price and quantity as Binance's decimal strings), `Clock`, `FeedError`.
- `socket(mss_cap)`: the `tcp::Config` (256 KiB RX and TX buffers). `client_config(roots)` (TLS 1.3 only) and `webpki_client_config()` build the rustls config.

## Binary

`binance-feed [--host HOST] [--port PORT] [--path PATH] [--print N] -- <tester> [args...]` runs on the tap-echo driver and netdev `cdtap_echo`. It resolves HOST with the kernel resolver before DPDK starts, connects from zhida (192.0.2.2/24, gateway 192.0.2.1) through the tester's NAT, prints the first N updates, and reads until the tester exits. Run it with no valid arguments for the full usage. The default host is `stream.binance.com`: `data-stream.binance.vision` speaks TLS 1.2 only, so sheath cannot reach it, and `stream.binance.com` may answer HTTP 451 from restricted locations. Exit statuses are tap-echo's.

## Build and test

```sh
make test-binance                     # cargo test -p binance-feed -- --test-threads=1
make docker-binance BINANCE_SECONDS=30   # live run; needs internet
```

`test-binance` needs DPDK 25.11.3, `CAP_NET_ADMIN` and `/dev/net/tun` (Docker image on macOS). The tests run the feed against a local TLS server with a certificate from `rcgen`. `docker-binance` uses a separate image with nftables (`BINANCE_IMAGE`) and is not part of `make test`.
