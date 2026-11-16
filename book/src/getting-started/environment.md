# Setting up a build environment

Some of switch builds on any machine with a Rust toolchain. The rest talks to the NIC through DPDK, and DPDK builds only on Linux. [How the pieces fit together](../overview/architecture.md) showed which crate does what; this page tells you which crates build where, and then gets you a Linux environment with the right DPDK: either a Docker container (the usual choice on macOS, and the quickest on Linux) or a Linux machine set up by hand. It ends with a running container, which is where the [first network program](first-connection.md) runs.

## What builds where

The split follows one dependency. Every crate that links `dpdk-smoltcp`, the crate that holds [the Stack](../nic/index.md), needs DPDK's C libraries to build. Crates that do not link it build anywhere.

| Crate | Builds on | Needs DPDK |
|---|---|---|
| `switch`, `switch-macros`, `switch-depot` (the runtime) | any OS | no |
| `switch-tls`, `switch-ws` without their `switch-net` support | any OS | no |
| `ceph-rseq`, `ceph-ring`, `ceph-fifo`, `ceph-proto`, `ceph-control` | any OS | no |
| `switch-frame`, `shared-memory`, `slice_ringbuffer` (packet-buffer handles, shared-memory files and the receive ring; see [Frames, shared memory and rings](../blocks/index.md)) | any OS | no |
| `switch-netconf` (the config file tool and library) | any OS; `discover` and `bind` read Linux's `/sys` and only work on Linux | no |
| `switch-test-support`, `switch-test-support-macros` | any OS | no |
| `switch-net` | Linux | yes, for either backend |
| `switch-open`, `switch-nic`, `switch-junctiond`, `switch-bench` and everything else that links `dpdk-smoltcp` | Linux | yes |

`switch-net` depends on `dpdk-smoltcp` unconditionally. A program that only ever uses the [junction backend](../net/backends.md), and so never opens a NIC itself, still needs `libdpdk` installed to build.

DPDK must be exactly version 25.11.3. The Rust bindings read DPDK's packet-buffer fields directly, and other versions lay them out differently; the `doctor` targets described below refuse any other version.

A plain `cargo build` or `cargo test` at the workspace root builds every member, the DPDK crates included, so on a machine without DPDK use the root `make test` (see [Tests that run without DPDK](#tests-that-run-without-dpdk)) or name the packages you want with `-p`.

## The Docker image

The repository ships a Docker image with everything the DPDK crates need: Ubuntu 24.04, Rust 1.93.0, clang and lld 18, and DPDK 25.11.3 built from source under `/opt/dpdk`. It builds for the host's native architecture, so it runs natively on both x86-64 and arm64 machines, Apple silicon included.

The Docker targets live in `conduit/Makefile`. Run them from the `conduit/` directory:

```sh
cd conduit
make docker-image     # build the image, tagged switch-dpdk-test:local
make docker-doctor    # print the toolchain and DPDK versions inside it
make docker-build     # build the whole workspace inside it
```

`docker-build` mounts the repository root at `/work` in the container and keeps cargo's registry and the Linux build output in two named Docker volumes, so later runs reuse them. The build output volume hides your host's `target/` directory from the container, so macOS and Linux artifacts never mix.

Docker's virtual machine (on macOS, Docker Desktop's VM) needs at least 8 CPUs. The Makefile's test and benchmark targets put different jobs on fixed CPU numbers, up to CPU 7 by default; [Running the test suite](../testing/suite.md) lists them. In Docker Desktop the setting is under Settings, Resources, CPU limit.

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

These are the same capabilities, mounts and volumes the Makefile's own targets use, so a build in this shell shares its cache with `make docker-build`. `--init` makes a small init process the container's first process, so Ctrl-C and `docker kill` reach the program you run.

`--cpuset-cpus=3` limits the whole container to CPU 3. The reason is a rule that runs through all of switch: a thread that drives the network runs on exactly one CPU, its [pinned CPU](../nic/cpus.md). When DPDK starts in a process, it pins its main thread to the one CPU the process is allowed to use, and it refuses to start, naming the count, if the process may run on more than one. Inside this container every process is allowed only CPU 3, so DPDK programs start without further setup, and `taskset -c 3` in the examples is correct too. Any free CPU works; change the number in both places if you pick another.

Inside the container, build as usual:

```text
root@container:/work# cargo build -p switch-net --example tcp_client
```

The first build compiles DPDK's bindings and the workspace and takes a few minutes; the volumes keep the result for the next container.

## A Linux machine without Docker

On Linux you can also install the toolchain natively. The machine needs what the Dockerfile (`conduit/docker/Dockerfile`) installs:

- Rust 1.93.0. `rust-toolchain.toml` at the repository root pins it for cargo commands in this repository.
- clang 18 and lld 18 on `PATH`, plus pkg-config, meson, ninja, python3-pyelftools, libnuma-dev, libpcap-dev, iproute2 and ethtool (Ubuntu 24.04 package names).
- DPDK 25.11.3 built from source into `/opt/dpdk` with the Dockerfile's meson options, after applying `conduit/docker/dpdk-ena-read-clock.patch`.

Then point the build at that DPDK and check it:

```sh
export PKG_CONFIG_PATH=/opt/dpdk/lib/pkgconfig
export LD_LIBRARY_PATH=/opt/dpdk/lib
export RUSTFLAGS='-C linker=clang -C link-arg=-fuse-ld=lld'
make -C conduit doctor
```

`make doctor` checks that it runs on Linux, finds cargo, clang and pkg-config, finds `libdpdk`, and fails with `Expected DPDK 25.11.3` for any other version. The full list of what the test suite needs on a native machine (packetdrill, Scapy, sockperf) is in `conduit/docker/README.md`, section "Linux VM".

Running the examples natively also needs what the container grants: root, or `CAP_NET_ADMIN` and `/dev/net/tun`, for tap devices. Run each DPDK program under `taskset -c N` so it is allowed on one CPU.

## Tests that run without DPDK

On any machine, including macOS without Docker, one root Makefile target runs most of the tests that need no DPDK:

```sh
make test
```

It runs `cargo test` for the runtime, the TLS and WebSocket protocol cores and the `ceph-*` crates, then the vendored smoltcp tests and the tests of the EC2 host-setup script. The frame and shared-memory crates also test natively, by name: `cargo test -p switch-frame -p shared-memory -p slice_ringbuffer`. [Running the test suite](../testing/suite.md) covers the full suite, which runs in Docker.

## Where to go next

With a container running, you have everything the next two pages need. [Your first task](first-task.md) runs the switch runtime alone, with no network, and works on your host machine too. [Your first network program](first-connection.md) then opens a tap device inside the container.
