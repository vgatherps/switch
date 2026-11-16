# switch

switch is a Rust library for network programs that need low, predictable latency, such as market-data feeds and order gateways. It is a userspace network stack: your program sends and receives packets through the network card directly, without going through the Linux kernel.

You write the program as async tasks. A task opens TCP or UDP sockets and awaits them. TLS 1.3 and WebSocket run on top of TCP.

## Using it

Programs depend on one crate, `switch`, from a clone of this repository:

```toml
[dependencies]
switch = { path = "<clone>/switch" }
```

That gives tasks and timers. The features `net`, `tls` and `ws` add sockets, TLS and WebSocket.

## Documentation

```sh
make docs
```

builds the documentation of the `switch` crate and opens it in a browser. Its front page leads to the guide, which goes from a first task to a complete WebSocket client, and to the API reference.

## Try it

This runs on any machine with cargo:

```sh
cargo run -p switch-examples --example first_task
```

The first chapter of the guide explains what it prints. The network examples need Linux with DPDK 25.11.3; the second chapter sets that up, in a container or on a Linux machine.

## Other documents

- [`docs/operations/`](docs/operations/README.md): preparing a machine and a network card, the config file, and the tools.
- [`docs/benchmarks/`](docs/benchmarks/README.md): what is measured and how to run it.
- [`docs/contributing/`](docs/contributing/): tests, conformance, and the map of the crates.

## License

GPL-3.0-only.
