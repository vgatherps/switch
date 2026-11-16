# Running the test suite

The tests split the same way the crates do on [Setting up a build environment](../getting-started/environment.md): the crates that do not need DPDK test on any machine, and everything else tests in the Docker image. This page shows both, how to run one suite at a time, and the CPU, timeout and concurrency rules the suite follows. It starts the testing part of the book, after the measuring part ended with [Stack-to-stack throughput with switch-memif-bench](../measuring/memif.md).

## Tests that run anywhere

From the repository root, on macOS or Linux, with no DPDK installed:

```sh
make test
```

This runs `cargo test` for `switch`, `switch-macros`, `switch-depot`, `switch-ws`, `switch-tls` and the `ceph-*` crates, then the vendored smoltcp tests, then `sh deploy/test-hft-host.sh`, which runs the EC2 setup script from [Running on EC2](../nic/ec2.md) against a fake `/proc`, `/sys` and `/etc` with no root. `switch-ws` and `switch-tls` are tested here without their `switch-net` support, which needs DPDK.

Two more crates test without DPDK: `make -C conduit test-support` runs the tests of `switch-test-support` and its macro crate, and `cargo test -p switch-sockperf-wire` tests the sockperf framing library.

`make miri` runs tests under Miri, Rust's checker for undefined behavior: the runtime's library tests and its oneshot tests under both of Miri's aliasing models (Stacked Borrows and Tree Borrows), and the `switch-ws` library tests under the default model. It needs a nightly toolchain with Miri installed.

## The full suite in Docker

Everything else needs DPDK and Linux. With the [Docker image](../getting-started/environment.md#the-docker-image) built (`make -C conduit docker-image`), run from the repository root:

```sh
make docker-test
```

This starts one container that runs `make test` from `conduit/Makefile`, the full DPDK suite, and then runs the Autobahn WebSocket conformance suite in both client and server roles, even if `make test` failed. It ends with one line:

```text
docker-test: PASS: make test exit 0, docker-autobahn exit 0
```

or `docker-test: FAIL: ...` with both exit codes. [Protocol conformance](conformance.md) covers Autobahn and the TLS-Anvil suite, which is not part of `docker-test`.

The in-container `make test` runs these targets, in this order:

| Target | Tests |
|---|---|
| `test-support` | `switch-test-support` and its macros |
| `test-sys` | `switch-dpdk-sys`, the DPDK bindings |
| `test-buffer`, `test-props` | `switch-buffer`, then its property tests |
| `test-stack`, `test-stack-release` | `dpdk-smoltcp` and `switch-test-kit`, then `dpdk-smoltcp` again in release mode |
| `test-echo` | `switch-tap-echo`: two Stacks, and the Linux kernel as a TCP peer over a tap device |
| `test-switch-nic` | `switch-nic` against kernel `sockperf` clients |
| `test-bench` | `switch-bench`, including an end-to-end test over a tap device |
| `test-netconf` | `switch-netconf` and `switch-open` |
| `test-packetdrill` | TCP behavior scripts run by packetdrill against `switch-tap-echo` |
| `test-switch-net` | `switch-net`, and a build of its benchmarks |
| `test-wss` | `switch-ws` and `switch-tls` with their `switch-net` support, against kernel servers over a tap device |
| `test-binance` | `switch-binance-feed`, offline |
| `test-memif` | `switch-memif-bench` |
| `test-autobahn-check`, `test-scapy` | Self-tests of the Autobahn and Scapy tooling |
| `test-smoltcp` | The vendored smoltcp |

`make docker-test` does not run the `switch-junction` and `switch-junctiond` tests. Run them in the container on their own:

```sh
cargo test --locked -p switch-junctiond -- --test-threads=1
cargo test --locked -p switch-junction
```

The AWS runner's `test` command ([Benchmarking on AWS](../measuring/aws.md#other-commands)) runs them natively on an EC2 host, together with the `ceph-*` ring crates and `switch-net`/`switch-tls` on both backends.

## One suite at a time

Every row in the table is a make target of its own. Inside a container (see [Running a container for the examples](../getting-started/environment.md#running-a-container-for-the-examples)), from `conduit/`:

```sh
make test-switch-net
```

On a native Linux machine with DPDK, run the same targets directly, after `make doctor`. Or run one crate's tests with cargo, keeping the one-thread rule from the next section:

```sh
cargo test --locked -p switch-net -- --test-threads=1
```

`make help` in `conduit/` lists every target.

## CPUs and timeouts

Every process that starts DPDK must be allowed on exactly one CPU (the [pinned CPU](../nic/cpus.md) rule), so the Docker targets put each run on CPUs of its own:

| CPU | Default use | Variable |
|---|---|---|
| 0 | `make test`, and every Docker target not listed below | `DOCKER_CPUSET` |
| 1 | the Autobahn peer | `AUTOBAHN_CPUSET` |
| 2 | packetdrill | `PACKETDRILL_CPUSET` |
| 3-6 | benchmarks (`docker-switch-memif-bench`, `docker-bench-stack`) | `MEMIF_CPUSET`, `STACK_BENCH_CPU` |
| 7 | the `switch-tap-echo` that packetdrill tests | `PACKETDRILL_SWITCH_CPUSET` |
| 8 | `make docker-soak` | `SOAK_CPUSET` |

With these defaults Docker's VM needs at least 8 CPUs (9 for `docker-soak`). Override any of them on the command line, for example `make DOCKER_CPUSET=1 AUTOBAHN_CPUSET=2 PACKETDRILL_CPUSET=3 PACKETDRILL_SWITCH_CPUSET=8 docker-test`. `AUTOBAHN_CPUSET` must differ from `DOCKER_CPUSET`, and `DOCKER_CPUSET`, `PACKETDRILL_CPUSET` and `PACKETDRILL_SWITCH_CPUSET` must all differ.

On a native machine `make test` needs the same three CPUs and must itself run under `taskset` on the first:

```sh
taskset -c 3 make -C conduit test DOCKER_CPUSET=3 PACKETDRILL_CPUSET=4 PACKETDRILL_SWITCH_CPUSET=5
```

Every cargo test step runs with `--test-threads=1` and under `timeout`, by default 600 seconds (`TEST_TIMEOUT`); a hung step fails with exit 124, or 137 if it ignored the first signal. The whole in-container `make test` has its own limit of 3600 seconds (`MAKE_TEST_TIMEOUT`). Inside a test binary, every DPDK test body has a 180-second limit of its own; [Writing tests that use DPDK](writing-tests.md) explains it.

Some tests create tap devices and veth pairs. Under `make docker-test` they live in the container's network namespace. Run natively, they are created in the namespace of whoever runs them, normally the host's, and need root or `CAP_NET_ADMIN` and `CAP_NET_RAW`, and `/dev/net/tun`.

## Running more than one at a time

The Makefiles do not support parallel make; run separate `make` invocations one after another too.

The Docker build output lives in a named volume shared by every run from the same checkout. To run the suite from two worktrees at once, give each its own volume:

```sh
make DOCKER_TARGET_VOLUME=switch-target-mybranch docker-test
```

The packetdrill tests check timing, so one lock (a shared Docker volume) lets only one packetdrill run at a time across all worktrees, and they assume nothing else saturates the Docker VM.

The image can also be built for x86-64 on an arm64 host with `DOCKER_PLATFORM=linux/amd64`. That image runs under emulation, which is slow enough to break the suite's wall-clock limits; run the suite on a native x86-64 machine instead.

## Where to go next

[Writing tests that use DPDK](writing-tests.md) explains the pinned-test attribute and the fixtures these suites are built on.
