# Setting up for network programs

A program that uses the network enables the `net` feature of `switch`:

```toml
[dependencies]
switch = { path = "<clone>/switch", features = ["net"] }
```

The feature builds only on Linux with DPDK 25.11.3 installed. The repository's Docker image has both and runs on any Docker host, macOS included.

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

The clone is mounted at `/work`, and the two named volumes keep cargo's downloads and the build output between containers. `--cap-add NET_ADMIN --device /dev/net/tun` lets a program create a tap device, which the examples in this guide use instead of a real card.

`--cpuset-cpus=0` gives the container one CPU, because opening a network card requires a CPU affinity mask of exactly one CPU. Any CPU the Docker host has will do.

## On a Linux machine

The image is Ubuntu 24.04, and its recipe, `conduit/docker/Dockerfile`, is the reference for a native setup. Install the same packages:

```sh
sudo apt-get install build-essential make pkg-config clang-18 libclang-18-dev lld-18 \
    meson ninja-build python3-pyelftools libnuma-dev libpcap-dev curl ca-certificates iproute2 ethtool
sudo update-alternatives --install /usr/bin/clang clang /usr/bin/clang-18 100
sudo update-alternatives --install /usr/bin/ld.lld ld.lld /usr/bin/ld.lld-18 100
```

Install Rust with `rustup`; the repository's `rust-toolchain.toml` selects the compiler version. Then build DPDK 25.11.3 into `/opt/dpdk` the way the image does. From the clone:

```sh
curl --fail --location --retry 2 -o /tmp/dpdk.tar.xz https://fast.dpdk.org/rel/dpdk-25.11.3.tar.xz
echo '3719acc586b310c4f60ba230683bf4f1e12c6f2f5bee11f7c01b1bbd0ded7490  /tmp/dpdk.tar.xz' | sha256sum --check
mkdir /tmp/dpdk && tar -xJf /tmp/dpdk.tar.xz -C /tmp/dpdk --strip-components=1
patch -d /tmp/dpdk -p1 < conduit/docker/dpdk-ena-read-clock.patch
CC=clang meson setup /tmp/dpdk/build /tmp/dpdk \
    --buildtype=release --prefix=/opt/dpdk --libdir=lib \
    -Dplatform=generic -Denable_stdatomic=false -Dmbuf_refcnt_atomic=false \
    -Denable_iova_as_pa=true -Dtests=false -Ddisable_apps='*' \
    -Denable_drivers=net/ring,net/pcap,net/tap,net/memif,net/af_packet,net/ena,net/intel/ixgbe,net/intel/i40e,net/intel/ice
ninja -C /tmp/dpdk/build
sudo meson install -C /tmp/dpdk/build
```

Point the build at that DPDK and check the result:

```sh
export PKG_CONFIG_PATH=/opt/dpdk/lib/pkgconfig
export LD_LIBRARY_PATH=/opt/dpdk/lib
export RUSTFLAGS='-C linker=clang -C link-arg=-fuse-ld=lld'
make -C conduit doctor
```

`doctor` prints the tool versions; the last line is DPDK's and must read `25.11.3`. If something is missing it prints one line naming it, for example `Missing libdpdk development package`.

The examples create a tap device, which needs root, or the `CAP_NET_ADMIN` capability and access to `/dev/net/tun`. Start each program under `taskset -c N`, as the commands in this guide do, so that it has an affinity mask of one CPU.

A real network card needs more preparation of the machine, which `docs/operations/host-setup.md` in the repository covers for a Linux machine with a spare card and `docs/operations/ec2.md` for an EC2 instance.

## Check the setup

In the container's shell, or on the Linux machine, build the example the next chapter runs:

```sh
cargo build -p switch-examples --features net --example tcp_client_local
```

The first build compiles the DPDK bindings and takes a few minutes. The program is at `target/debug/examples/tcp_client_local`.

Next: [Your first network program](crate::guide::first_connection).
