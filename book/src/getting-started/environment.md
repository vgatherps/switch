# Setting up a build environment

[Your first task](first-task.md) ran on the switch runtime alone and needed nothing but cargo. The next program opens a NIC, and that needs DPDK, which builds only on Linux. This page shows which crates build where, then gets you a Linux environment with the right DPDK: a Docker container (the usual choice on macOS, and the quickest on Linux) or a Linux machine set up by hand. It ends with a running container, which is where [your first network program](first-connection.md) runs.

## What builds where

The split follows DPDK's C libraries. A crate that links them, directly or through another crate, needs DPDK installed to build; the others do not.

| Crate | Platforms | Needs DPDK |
|---|---|---|
| `switch`, `switch-macros`, `switch-depot` (the runtime) | macOS, Linux | no |
| `switch-tls`, `switch-ws` without their `switch-net` support | macOS, Linux | no |
| `switch-frame`, `slice_ringbuffer` (packet-buffer handles, the receive ring) | macOS, Linux | no |
| `shared-memory`, `ceph-control`, `ceph-proto`, `ceph-fifo` | macOS, Linux | no |
| `ceph-rseq`, `ceph-ring` | macOS, Linux (glibc, x86_64 or aarch64) | no |
| `switch-junction` (the client side of the junction daemon) | Linux | no |
| `switch-netconf` (the config file tool and library) | macOS, Linux; `discover` and `bind` read Linux's `/sys` and only work on Linux | no |
| `switch-sockperf-wire`, `switch-test-support`, `switch-test-support-macros` | macOS, Linux | no |
| `switch-dpdk-sys`, `switch-buffer` (the DPDK bindings and packet buffers) | Linux | yes |
| `dpdk-smoltcp` ([the Stack](../stack/index.md)), `switch-open`, `switch-net`, `switch-junctiond`, `switch-nic`, `switch-bench`, `switch-test-kit` and the other tools | Linux | yes |

`switch-net` depends on `dpdk-smoltcp` unconditionally. A program that only ever uses the [junction backend](../net/backends.md), and so never opens a NIC itself, still needs `libdpdk` installed to build. The [crate map](../reference/crates.md) lists every package.

DPDK must be exactly version 25.11.3. The Rust bindings read DPDK's packet-buffer fields directly, and other versions lay them out differently; the `doctor` targets described below refuse any other version.

A plain `cargo build` or `cargo test` at the workspace root builds every member, the DPDK crates included. On a machine without DPDK, name the packages you want with `-p`, or use the root `make test` (see [Tests that run without DPDK](#tests-that-run-without-dpdk)).

## The Docker image

The repository ships a Docker image with everything the DPDK crates need: Ubuntu 24.04, Rust 1.99.1, clang and lld 18, and DPDK 25.11.3 built from source under `/opt/dpdk`. It builds for the host's native architecture, so it runs natively on both x86-64 and arm64 machines, Apple silicon included.

The Docker targets live in `conduit/Makefile`. From the repository root:

```sh
make -C conduit docker-image     # build the image, tagged switch-dpdk-test:local
make -C conduit docker-doctor    # print the toolchain and DPDK versions inside it
make -C conduit docker-build     # build the whole workspace inside it
```

`docker-build` mounts the repository root at `/work` in the container and keeps cargo's registry and the Linux build output in two named Docker volumes, so later runs reuse them. The build output volume hides your host's `target/` directory from the container, so macOS and Linux artifacts never mix.

The example container below uses CPU 3, so Docker's virtual machine (on macOS, Docker Desktop's VM) needs at least 4 CPUs. The full test suite's default CPUs need 8, `docker-soak` 9, and the benchmark smoke run 13 (or `BENCH_CPUSET=3-6`); [Running the test suite](../testing/suite.md#cpus-and-timeouts) lists them. In Docker Desktop the setting is under Settings, Resources, CPU limit.

## Running a container for the examples

The examples in this book open a [tap device](first-connection.md): a virtual NIC that DPDK creates and that Linux sees as an ordinary network interface, so programs on the Linux side can be the peer. Creating one needs the `NET_ADMIN` capability and the `/dev/net/tun` device. Containers in this project are not privileged; they add exactly those two things and nothing else.

From the repository root, start a shell in the image:

```sh
docker run --rm -it --init \
    --cap-add NET_ADMIN --device /dev/net/tun \
    --cpuset-cpus=3 \
    -v "$PWD:/work" -w /work \
    -v switch-cargo-registry:/usr/local/cargo/registry \
    -v switch-linux-target:/work/target \
    switch-dpdk-test:local bash
```

These are the same capabilities and cache volumes the Makefile's own targets use, so a build in this shell shares its cache with `make -C conduit docker-build`. `--init` makes a small init process the container's first process, so Ctrl-C and `docker kill` reach the program you run.

`--cpuset-cpus=3` limits the whole container to CPU 3. The reason is a rule that runs through all of switch: a thread that drives the network runs on exactly one CPU, its [pinned CPU](../nic/cpus.md). `switch_open::open` checks that the calling thread may run on exactly one CPU and otherwise fails with `NotPinned`, naming how many it may use; it then starts DPDK with its main thread on that CPU. Inside this container every process is allowed only CPU 3, so the examples start without further setup, and `taskset -c 3` in their commands is correct too. Any free CPU works; change the number in both places if you pick another.

Inside the container, build as usual:

```text
root@container:/work# cargo build -p switch-examples --features net --example tcp_client_local
```

The first build compiles DPDK's bindings and the workspace and takes a few minutes; the volumes keep the result for the next container.

## A Linux machine without Docker

On Linux you can also install the toolchain natively, the way the image (`conduit/docker/Dockerfile`) does it. On Ubuntu 24.04:

```sh
sudo apt-get install build-essential make pkg-config clang-18 libclang-18-dev lld-18 \
    meson ninja-build python3-pyelftools libnuma-dev libpcap-dev curl ca-certificates iproute2 ethtool
sudo update-alternatives --install /usr/bin/clang clang /usr/bin/clang-18 100
sudo update-alternatives --install /usr/bin/ld.lld ld.lld /usr/bin/ld.lld-18 100
```

Rust comes from `rustup`; `rust-toolchain.toml` at the repository root pins 1.99.1 for cargo commands in this repository.

Then build and install DPDK into `/opt/dpdk` the way the image does. These are the image's build steps. On a native machine, copy `conduit/docker/dpdk-ena-read-clock.patch` to `/tmp`, then run the command after `RUN` in a shell, with `sudo` for `meson install`:

```dockerfile
{{#include ../../../conduit/docker/Dockerfile:dpdk}}
```

Point the build at that DPDK and check it:

```sh
export PKG_CONFIG_PATH=/opt/dpdk/lib/pkgconfig
export LD_LIBRARY_PATH=/opt/dpdk/lib
export RUSTFLAGS='-C linker=clang -C link-arg=-fuse-ld=lld'
make -C conduit doctor
```

`make doctor` checks that it runs on Linux, finds cargo, clang and pkg-config, finds `libdpdk`, and fails with `Expected DPDK 25.11.3` for any other version. The full test suite needs a few more tools on a native machine (packetdrill, Scapy, sockperf); `conduit/docker/README.md`, section "Linux VM", lists them.

Running the examples natively also needs what the container grants: root, or `CAP_NET_ADMIN` and `/dev/net/tun`, for tap devices. Run each DPDK program under `taskset -c N` so it is allowed on one CPU.

## Tests that run without DPDK

On any machine, including macOS without Docker:

```sh
make test
```

[Running the test suite](../testing/suite.md#tests-that-run-anywhere) says what it covers and how to run the full suite in Docker.

## Where to go next

With a container running, go on to [Your first network program](first-connection.md), which opens a tap device inside it.
