# switch-binance-feed

A client of Binance's bookTicker stream over `wss://`, run as a switch task. `switch-nic feed` runs it on a real NIC, and the `switch-binance-feed` binary runs it on a tap device.

Its API reference is the crate's rustdoc (`make docs` at the repository root); the feed's output is described in [docs/operations/switch-nic.md](../../../docs/operations/switch-nic.md#feed).

```sh
make -C conduit test-binance                       # offline tests; DPDK, CAP_NET_ADMIN, /dev/net/tun
make -C conduit docker-binance BINANCE_SECONDS=30  # live run against Binance; needs internet
```
