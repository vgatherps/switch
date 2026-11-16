# Setting up for network programs

A program that uses the network enables the `net` feature of `switch`:

```toml
[dependencies]
switch = { path = "<clone>/switch", features = ["net"] }
```

That feature builds only on Linux with DPDK 25.11.3 installed. DPDK is the library that drives the network card from userspace, and switch is built against exactly that version. The repository has a Docker image with everything installed, which is the quickest way to start on any machine, a Mac included. A Linux machine can also be set up directly.

## In a container

From the clone, build the image once. It compiles DPDK from source, so the first build takes a while:

```sh
make -C conduit docker-image
```

This leaves an image tagged `switch-dpdk-test:local`. Start a shell in it:

```sh
docker run --rm -it --init \
    --cap-add NET_ADMIN --device /dev/net/tun \
    --cpuset-cpus=0 \
    -v "$PWD:/work" -w /work \
    -v switch-cargo-registry:/usr/local/cargo/registry \
    -v switch-linux-target:/work/target \
    switch-dpdk-test:local bash
```

The clone is mounted at `/work`, so you edit files on your machine and build in the container. The two named volumes keep cargo's downloads and the Linux build output between containers.

`--cap-add NET_ADMIN --device /dev/net/tun` lets a program in the container create a virtual network card, which the examples in this guide use instead of a real one.

`--cpuset-cpus=0` allows the container CPU 0 and no other. A switch program that opens a network card must be allowed on exactly one CPU, and this is the easiest way to arrange that. Any CPU the Docker host has will do.

In the container's shell, build the example the next chapter runs:

```sh
cargo build -p switch-examples --features net --example tcp_client_local
```

The first build compiles the DPDK bindings and takes a few minutes. It ends with cargo's usual `Finished` line, and the program is at `target/debug/examples/tcp_client_local`.

## On a Linux machine

The image is Ubuntu 24.04, and its recipe, `conduit/docker/Dockerfile`, is the reference for a native setup. Install the same packages:

```sh
sudo apt-get install build-essential make pkg-config clang-18 libclang-18-dev lld-18 \
    meson ninja-build python3-pyelftools libnuma-dev libpcap-dev curl ca-certificates iproute2 ethtool
sudo update-alternatives --install /usr/bin/clang clang /usr/bin/clang-18 100
sudo update-alternatives --install /usr/bin/ld.lld ld.lld /usr/bin/ld.lld-18 100
```

Install Rust with `rustup`; the repository's `rust-toolchain.toml` selects the compiler version. Then build DPDK 25.11.3 into `/opt/dpdk` by running the Dockerfile's DPDK step in a shell: it downloads the release, applies `conduit/docker/dpdk-ena-read-clock.patch`, and runs `meson` and `ninja` with the options switch needs.

Point the build at that DPDK and check the result:

```sh
export PKG_CONFIG_PATH=/opt/dpdk/lib/pkgconfig
export LD_LIBRARY_PATH=/opt/dpdk/lib
export RUSTFLAGS='-C linker=clang -C link-arg=-fuse-ld=lld'
make -C conduit doctor
```

`doctor` prints the versions of the kernel, cargo, rustc, clang, pkg-config and DPDK, the last of which must read `25.11.3`. If something is missing it prints one line naming it, such as `Missing libdpdk development package` or `Expected DPDK 25.11.3; see docker/README.md.`

The examples create a virtual network card, which needs root, or the `CAP_NET_ADMIN` capability and access to `/dev/net/tun`. Where the container gets its one CPU from `--cpuset-cpus`, a native run starts each program under `taskset -c N`.

To use a real network card, the machine needs more preparation: `docs/operations/host-setup.md` in the repository covers a Linux machine with a spare card, and `docs/operations/ec2.md` an EC2 instance.

Next: [Your first network program](crate::guide::first_connection).
