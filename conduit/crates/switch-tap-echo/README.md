# switch-tap-echo

A TCP echo server on a tap device, the target of the packetdrill scripts and the Scapy TCP fuzzer, and the tap driver that `switch-ws-autobahn` and `switch-binance-feed` share. It is test infrastructure; applications do not use it.

Its API reference is the crate's rustdoc (`make docs` at the repository root); the suites that use it are in [docs/contributing/testing.md](../../../docs/contributing/testing.md).

```sh
make -C conduit test-echo         # needs DPDK, CAP_NET_ADMIN and /dev/net/tun
make -C conduit test-packetdrill  # see docs/contributing/testing.md for its CPUs
make -C conduit docker-fuzz       # the Scapy fuzzer
```

The tester must exit on SIGTERM and be run directly or through `exec`: only the tester process itself is killed.
