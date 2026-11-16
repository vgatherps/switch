# switch

switch is a set of Rust crates for network programs that need low, predictable latency. Your program runs its own TCP/IP stack in userspace, talks to the NIC directly through [DPDK](https://www.dpdk.org/), and drives the network and its async tasks from one loop on one CPU. Tasks are ordinary async Rust: they open TCP and UDP sockets, and TLS 1.3 and WebSocket connections on top of them.

A program gets its NIC in one of two ways. It can own the NIC itself, with the network stack inside its process. Or it can share the NIC with other programs through `switch-junctiond`, a daemon that owns the NIC and serves sockets over shared memory. The socket code is the same either way.

## The book

The documentation is an mdBook in [`book/`](book/src/SUMMARY.md) plus each crate's API reference (rustdoc). Start with the [Introduction](book/src/introduction.md), then [How the pieces fit together](book/src/overview/architecture.md). To read both rendered in a browser:

```sh
make docs        # builds the book and the API reference, and opens both
make book        # the book only (needs mdbook: cargo install mdbook)
make api-docs    # the API reference only
```

The API reference builds in the Docker image described under [Building](#building), because most of the crates need DPDK; build the image once with `make -C conduit docker-image`. The [crate map](book/src/reference/crates.md) lists every package and the chapter that covers it.

## Using the crates

The crates are used from a clone of this repository. Clone it, then depend on the crates by path from your own `Cargo.toml`:

```toml
[dependencies]
switch = { path = "<clone>/switch" }
switch-net = { path = "<clone>/switch-net" }
```

`<clone>` is where you cloned the repository. The [crate map](book/src/reference/crates.md) says which other crates an application may need.

## Try it

The runtime builds on macOS or Linux with only a Rust toolchain. From the clone:

```sh
cargo run -p switch --example first_task
```

This runs three short examples on the switch runtime with no network, and is explained in [Your first task](book/src/getting-started/first-task.md).

Anything that touches the NIC needs Linux with DPDK 25.11.3. The repository ships a Docker image with everything installed, which also works on macOS:

```sh
make -C conduit docker-image
docker run --rm -it --init \
    --cap-add NET_ADMIN --device /dev/net/tun --cpuset-cpus=3 \
    -v "$PWD:/work" -w /work \
    -v switch-cargo-registry:/usr/local/cargo/registry \
    -v switch-linux-target:/work/target \
    switch-dpdk-test:local bash
```

Inside the container, [Your first network program](book/src/getting-started/first-connection.md) builds a TCP client that runs over a tap device, a virtual NIC with the Linux kernel as the peer, so no spare network card is needed. [Setting up a build environment](book/src/getting-started/environment.md) explains the container and what builds where.

## Tests

`make test` at the repository root runs the tests that need no DPDK, on macOS or Linux. `make docker-test` runs the full suite in the Docker image. [Running the test suite](book/src/testing/suite.md) has the details.

## License

GPL-3.0-only.
