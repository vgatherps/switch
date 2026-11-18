# switch

switch is a userspace network stack and async runtime for Rust, built on DPDK. It provides TCP and UDP sockets, a TLS 1.3 client, and WebSocket clients and servers.

## Using it

Programs depend on one crate, `switch`, from a clone of this repository:

```toml
[dependencies]
switch = { path = "<clone>/switch" }
```

With no features this is the runtime alone, tasks and timers; the features `net`, `tls` and `ws` add sockets, TLS and WebSocket.

## Documentation

The documentation is built in the repository's Docker image, because most of the code needs DPDK to compile. With Docker installed, build the image once, then the documentation:

```sh
make -C conduit docker-image
make docs
```

`make docs` opens the documentation of the `switch` crate in a browser. Its front page leads to the guide, which goes from a first task to a complete WebSocket client, and to the API reference.

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

GPL-3.0-only, see [LICENSE](LICENSE).
