# Running the test suite

Run `make test` at the repository root for the tests that need no DPDK, and `make docker-test` for everything else. The split is the one on [Setting up a build environment](../getting-started/environment.md): crates that do not link DPDK test on macOS or Linux directly, and the rest test in the Docker image. This page shows both, how to run one suite at a time, the AWS test command, and the CPU, timeout and concurrency rules the suite follows.

## Tests that run anywhere

From the repository root, on macOS or Linux, with no DPDK installed:

```sh
make test
```

This runs `cargo test` for `switch`, `switch-macros`, `switch-depot`, `switch-ws`, `switch-tls` and the `ceph-*` crates, then the vendored smoltcp tests, then `sh deploy/test-hft-host.sh`, which runs the EC2 setup script from [Running on EC2](../nic/ec2.md) against a fake `/proc`, `/sys` and `/etc` with no root. `switch-ws` and `switch-tls` are tested here without their `switch-net` support, which needs DPDK.

More crates test without DPDK when you name them:

```sh
cargo test --locked -p switch-frame -p shared-memory -p slice_ringbuffer -p switch-sockperf-wire
cargo test --locked -p switch-test-support -p switch-test-support-macros
```

The second line is what `make -C conduit test-support` runs. That target wraps it in GNU `timeout`, which macOS lacks; on macOS run the cargo line, or clear the wrapper with `make -C conduit test-support TIMEOUT=`. `switch-junction` needs no DPDK either, but its tests need Linux: run `cargo test --locked -p switch-junction` on a Linux machine or in the container.

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
| `test-switch-nic` | `switch-nic` against clients of sockperf, a kernel-socket latency tool |
| `test-bench` | `switch-bench`, including an end-to-end test over a tap device |
| `test-netconf` | `switch-netconf` and `switch-open` |
| `test-packetdrill` | packetdrill scripts against `switch-tap-echo`: scripted TCP conversations with exact timing |
| `test-switch-net` | `switch-net`, and a build of its benchmarks |
| `test-wss` | `switch-ws` and `switch-tls` with their `switch-net` support, against kernel servers over a tap device |
| `test-binance` | `switch-binance-feed`, offline |
| `test-memif` | `switch-memif-bench` |
| `test-autobahn-check`, `test-scapy` | Self-tests of the Autobahn tooling and of the TCP fuzzer built on Scapy, a Python packet library |
| `test-smoltcp` | The vendored smoltcp |

`make docker-test` does not run the `switch-junctiond` tests. Run them in the container on their own, pinned to the container's CPU:

```sh
taskset -c 3 cargo test --locked -p switch-junctiond -- --test-threads=1
```

## One suite at a time

Every row in the table is a make target of its own. In the [example container](../getting-started/environment.md#running-a-container-for-the-examples), which may use only CPU 3, run them from `conduit/` and tell the Makefile which CPU that is:

```sh
make test-switch-net
make test-switch-nic DOCKER_CPUSET=3
```

`test-switch-nic` pins its processes to `DOCKER_CPUSET`, which defaults to CPU 0, so name the container's CPU. `test-packetdrill` needs more than one CPU: it puts packetdrill and its `switch-tap-echo` on `PACKETDRILL_CPUSET` (default 2) and `PACKETDRILL_SWITCH_CPUSET` (default 7). Start a container that may use three CPUs, for example `--cpuset-cpus=3-5`, run make under `taskset` on the first, and name all three:

```sh
taskset -c 3 make test-packetdrill DOCKER_CPUSET=3 PACKETDRILL_CPUSET=4 PACKETDRILL_SWITCH_CPUSET=5
```

On a native Linux machine with DPDK, run the same targets directly, after `make doctor`. Or run one crate's tests with cargo, on one CPU and one test thread:

```sh
taskset -c 3 cargo test --locked -p switch-net -- --test-threads=1
```

Without `taskset` the DPDK tests panic at startup, because the process may run on more than one CPU. `make help` in `conduit/` lists every target.

## On an EC2 host

The AWS runner's `test` command ([Benchmarking on AWS](../measuring/aws.md#other-commands)) prepares one EC2 instance and runs three suites natively, as root, on its DPDK and Rust:

```sh
# rings
cargo test --locked --no-fail-fast -p ceph-rseq -p ceph-ring -p ceph-fifo -p switch-junction -p slice_ringbuffer
# switch-junctiond and switch-net-switch-tls run on the first housekeeping CPU
. /etc/switch/host.env
test_cpu=${HOUSEKEEPING%%,*}; test_cpu=${test_cpu%%-*}
taskset -c "$test_cpu" cargo test --locked --no-fail-fast -p switch-junctiond -- --test-threads=1
taskset -c "$test_cpu" cargo test --locked --no-fail-fast -p switch-net -p switch-tls \
    --features switch-net/testkit,switch-tls/e2e -- --test-threads=1
```

The rings suite runs unpinned, because its tests of the per-CPU rings need at least two CPUs. On x86-64 it runs the rings' x86 assembly on real hardware. The other two suites are built unpinned first (`--no-run`), then run pinned.

## CPUs and timeouts

Every process that starts DPDK must be allowed on exactly one CPU (the [pinned CPU](../nic/cpus.md) rule), so the Docker targets put each run on CPUs of its own:

| CPU | Default use | Variable |
|---|---|---|
| 0 | `make test`, and every Docker target not listed below | `DOCKER_CPUSET` |
| 1 | the Autobahn peer | `AUTOBAHN_CPUSET` |
| 1-3 | TLS-Anvil | `TLS_ANVIL_CPUSET` |
| 2 | packetdrill | `PACKETDRILL_CPUSET` |
| 3-6 | benchmarks (`docker-switch-memif-bench`, `docker-bench-stack`) | `MEMIF_CPUSET`, `STACK_BENCH_CPU` |
| 7 | the `switch-tap-echo` that packetdrill tests | `PACKETDRILL_SWITCH_CPUSET` |
| 8 | `make docker-soak` | `SOAK_CPUSET` |
| 9-12 | `make -C conduit docker-bench-smoke` | `BENCH_CPUSET` |

With these defaults Docker's VM needs 8 CPUs for `docker-test`, 9 for `docker-soak` and 13 for `docker-bench-smoke`. Override any of them on the command line, for example `make DOCKER_CPUSET=1 AUTOBAHN_CPUSET=2 PACKETDRILL_CPUSET=3 PACKETDRILL_SWITCH_CPUSET=8 docker-test`. `AUTOBAHN_CPUSET` must differ from `DOCKER_CPUSET`, and `DOCKER_CPUSET`, `PACKETDRILL_CPUSET` and `PACKETDRILL_SWITCH_CPUSET` must all differ.

On a native machine the full `make test` needs the same three CPUs and must itself run under `taskset` on the first:

```sh
taskset -c 3 make -C conduit test DOCKER_CPUSET=3 PACKETDRILL_CPUSET=4 PACKETDRILL_SWITCH_CPUSET=5
```

Every DPDK test step runs with `--test-threads=1`, and every cargo test step runs under `timeout`, by default 600 seconds (`TEST_TIMEOUT`); a hung step fails with exit 124, or 137 if it ignored the first signal. The whole in-container `make test` has its own limit of 3600 seconds (`MAKE_TEST_TIMEOUT`). Inside a test binary, every DPDK test body has a 180-second limit of its own; [Writing tests that use DPDK](writing-tests.md) explains it.

Some tests create tap devices and veth pairs. Under `make docker-test` they live in the container's network namespace. Run natively, they are created in the namespace of whoever runs them, normally the host's, and need root or `CAP_NET_ADMIN` and `CAP_NET_RAW`, and `/dev/net/tun`.

## Running more than one at a time

Run one suite at a time per checkout: the Makefiles do not support parallel make, so run separate `make` invocations one after another too. Separate checkouts (worktrees) can run at once, each with its own build volume:

```sh
make DOCKER_TARGET_VOLUME=switch-target-mybranch docker-test
```

The packetdrill tests check timing, so one lock (a shared Docker volume) lets only one packetdrill run at a time across all checkouts, and they assume nothing else saturates the Docker VM.

The image can also be built for x86-64 on an arm64 host with `DOCKER_PLATFORM=linux/amd64`. That image runs under emulation, which is slow enough to break the suite's wall-clock limits; run the suite on a native x86-64 machine instead.

## Where to go next

[Writing tests that use DPDK](writing-tests.md) explains the pinned-test attribute and the fixtures these suites are built on.
