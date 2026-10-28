# binance-feed

Client for Binance's bookTicker stream over TCP, TLS and WebSocket on one zhida TCP socket. It is the workspace's example of a full protocol stack running on a `Stack`.

Depends on [`dpdk-smoltcp`](../dpdk-smoltcp/README.md), [`stack-stream`](../stack-stream/README.md) and [`tap-echo`](../tap-echo/README.md) (its `driver`), plus tungstenite, rustls (ring provider) and webpki-roots. `nic-runner` depends on it for the `feed` subcommand.

## API

- `connect(stack, clock, target, socket, tls, local_port, ...)`: opens TCP, TLS and WebSocket within one time limit, turning the Stack between retries. Returns a `Feed`.
- `Feed::next(clock, stop)`: reads one `BookTicker`; `Feed::tls_summary()` names the TLS version and cipher suite.
- `Target { addr, host, path }`, `BookTicker` (`update_id`, `symbol`, bid and ask price and quantity as Binance's decimal strings), `Clock`, `FeedError`.
- `socket(mss_cap)`: the `tcp::Config` (256 KiB RX and TX buffers). `client_config(roots)` and `webpki_client_config()` build the rustls config.

## Binary

`binance-feed [--host HOST] [--port PORT] [--path PATH] [--print N] -- <tester> [args...]` runs on the tap-echo driver and netdev `cdtap_echo`. It resolves HOST with the kernel resolver before DPDK starts, connects from zhida (192.0.2.2/24, gateway 192.0.2.1) through the tester's NAT, prints the first N updates, and reads until the tester exits. Run it with no valid arguments for the full usage. The default host is `data-stream.binance.vision`, since `stream.binance.com` answers HTTP 451 from restricted locations. Exit statuses are tap-echo's.

## Build and test

```sh
make test-binance                     # cargo test -p binance-feed -- --test-threads=1
make docker-binance BINANCE_SECONDS=30   # live run; needs internet
```

`test-binance` needs DPDK 25.11.3, `CAP_NET_ADMIN` and `/dev/net/tun` (Docker image on macOS). The tests run the feed against a local TLS server with a certificate from `rcgen`. `docker-binance` uses a separate image with nftables (`BINANCE_IMAGE`) and is not part of `make test`.
