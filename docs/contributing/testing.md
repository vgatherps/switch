# Running the tests

Most of the workspace links DPDK and builds only on Linux with DPDK 25.11.3; the rest builds anywhere. Each suite runs under one of these commands:

| Command | Where | Runs |
|---|---|---|
| `make test` (repository root) | macOS or Linux | The tests of the packages that need no DPDK, the vendored smoltcp's tests, and the EC2 host setup script's tests |
| `make docker-test` | the Docker image | The DPDK suites (the targets of `conduit/Makefile`'s `make test`), then the Autobahn WebSocket suite |
| `make miri` | macOS or Linux, nightly toolchain | The runtime's and `switch-ws`'s unsafe code under Miri |

`make docker-test` does not run the tests of `switch-runtime`, `switch`, `switch-examples`, `switch-macros`, `switch-depot`, `switch-frame`, `shared-memory`, `slice_ringbuffer` or the `ceph-*` crates, nor the host setup script's tests; only the root `make test` does. Run `make test` while you work and both before you push.

## Tests that need no DPDK

From the repository root, on macOS or Linux:

```sh
make test
```

This runs `cargo test` for `switch-runtime`, `switch`, `switch-examples`, `switch-macros`, `switch-depot`, `switch-frame`, `shared-memory`, `slice_ringbuffer`, `switch-ws`, `switch-tls` and the five `ceph-*` crates. `switch`, `switch-examples`, `switch-ws` and `switch-tls` are built without their network features here, because those need DPDK. Then it runs the vendored smoltcp's tests, and last `sh deploy/test-hft-host.sh`, which tests the EC2 host setup script against a fake `/proc`, `/sys` and `/etc` without root. That script ends with `test-hft-host: all passed`.

`switch-test-support`, its macros and `switch-sockperf-wire` need no DPDK either. `make docker-test` runs their tests (targets `test-support` and `test-switch-nic`); on macOS, run them by name:

```sh
cargo test --locked -p switch-sockperf-wire
cargo test --locked -p switch-test-support -p switch-test-support-macros
```

`make miri` runs the runtime's library tests and its `oneshot_join` test under Miri, Rust's checker for undefined behavior, with both of its aliasing models, and the `switch-ws` library tests with the default model. It needs a nightly toolchain with Miri installed:

```sh
cargo +nightly miri test -p switch-runtime --lib --test oneshot_join
MIRIFLAGS=-Zmiri-tree-borrows cargo +nightly miri test -p switch-runtime --lib --test oneshot_join
cargo +nightly miri test -p switch-ws --lib
```

## The full suite in Docker

1. Build the image once:

   ```sh
   make -C conduit docker-image
   ```

2. Give Docker's VM at least 8 CPUs (in Docker Desktop: Settings, Resources), then run from the repository root:

   ```sh
   make docker-test
   ```

   This runs the full DPDK suite in one container, then the [Autobahn WebSocket suite](conformance.md#autobahn) in both roles, even if the first part failed.

3. It ends with one line:

   ```text
   docker-test: PASS: make test exit 0, docker-autobahn exit 0
   ```

   or `docker-test: FAIL: ...` with both exit codes.

Inside the container, `make test` from `conduit/Makefile` runs these targets in order. Each is a make target of its own:

| Target | Tests |
|---|---|
| `test-support` | `switch-test-support` and its macros |
| `test-sys` | `switch-dpdk-sys`, the DPDK bindings |
| `test-buffer`, `test-props` | `switch-buffer`, then its property tests |
| `test-stack`, `test-stack-release` | `dpdk-smoltcp` and `switch-test-kit`, then `dpdk-smoltcp` again in release mode |
| `test-echo` | `switch-tap-echo`: two stacks, and the Linux kernel as a TCP peer over a tap device |
| `test-switch-nic` | `switch-nic` against clients of sockperf, a kernel-socket latency tool |
| `test-bench` | `switch-bench`, including an end-to-end test over a tap device |
| `test-netconf` | `switch-netconf` and `switch-open` |
| `test-packetdrill` | packetdrill scripts, scripted TCP conversations with exact timing, against `switch-tap-echo` |
| `test-switch-net` | `switch-net`, a build of its benchmarks, and a build of the `switch-examples` programs that need only feature `net` |
| `test-junction` | `switch-junction`, then `switch-junctiond` |
| `test-wss` | `switch-ws` and `switch-tls` with their network support, against kernel servers over a tap device; then the guide's code blocks (doctests of `switch` with feature `ws`) and a build of every `switch-examples` program |
| `test-binance` | `switch-binance-feed`, offline |
| `test-memif` | `switch-memif-bench` |
| `test-autobahn-check`, `test-scapy` | Self-tests of the Autobahn scripts and of the Scapy TCP fuzzer |
| `test-smoltcp` | The vendored smoltcp |

[Protocol conformance](conformance.md) covers Autobahn and TLS-Anvil. TLS-Anvil is not part of `docker-test`.

## One target at a time

A DPDK test process must be allowed on exactly one CPU; otherwise it panics at startup. Start a container on one CPU, here CPU 3, from the repository root:

```sh
docker run --rm -it --init --cpuset-cpus=3 --cap-add NET_ADMIN --device /dev/net/tun \
    -v "$PWD:/work" -w /work/conduit -v switch-cargo-registry:/usr/local/cargo/registry \
    -v switch-linux-target:/work/target -v switch-linux-target:/work/conduit/target \
    switch-dpdk-test:local bash
```

Then run any target from the table, naming the container's CPU for the targets that pin processes themselves:

```sh
make test-switch-net
make test-junction DOCKER_CPUSET=3
make test-switch-nic DOCKER_CPUSET=3
```

Or one package with cargo, with one test thread:

```sh
cargo test --locked -p switch-net -- --test-threads=1
```

`test-packetdrill` needs three CPUs: one for make, one for packetdrill and one for the `switch-tap-echo` it tests. Start the container with `--cpuset-cpus=3-5` and run:

```sh
taskset -c 3 make test-packetdrill DOCKER_CPUSET=3 PACKETDRILL_CPUSET=4 PACKETDRILL_SWITCH_CPUSET=5
```

`make help` lists every target.

On a Linux machine with DPDK installed, run the same targets without Docker after `make -C conduit doctor`, which checks the toolchain and the DPDK version. Put each command under `taskset -c` on one CPU. The full suite needs three CPUs and runs make on the first:

```sh
taskset -c 3 make -C conduit test DOCKER_CPUSET=3 PACKETDRILL_CPUSET=4 PACKETDRILL_SWITCH_CPUSET=5
```

Natively, the tests that create tap devices and veth pairs need root or `CAP_NET_ADMIN` and `CAP_NET_RAW`, and `/dev/net/tun`; they create the devices in your network namespace.

## Tests that need two CPUs

The tests of where DPDK's helper threads run need more than the one CPU every other DPDK test requires. Inside `make test` they print `SKIP: ...` and pass. Run them by hand when you change how `switch-open` or `switch-junctiond` place helper threads. Start the container above with `--cpuset-cpus=0,2` instead of `--cpuset-cpus=3`, and run each test on its own with `--exact`, because the tests around it need exactly one CPU:

```sh
cargo test --locked -p switch-open --lib eal_affinity::tests::eal_helpers_avoid_polling_cpu -- --exact
cargo test --locked -p switch-junctiond --test process \
    explicit_control_cpu_keeps_every_helper_off_the_network_cpu -- --exact --test-threads=1
```

The first starts DPDK in a child process with one CPU treated as isolated and checks that the helper threads stay off it; it needs two allowed CPUs, one of them not isolated by the kernel. The second starts the daemon with `--control-cpus 2` on CPU 0 and checks that its main thread may run only on CPU 0 and every other thread only on CPU 2; it needs CPUs 0 and 2 among the allowed CPUs.

## CPUs, timeouts and parallel runs

Each Docker target runs on CPUs of its own:

| CPU | Default use | Variable |
|---|---|---|
| 0 | `make test` and the other Docker targets not listed below | `DOCKER_CPUSET` |
| 1 | the Autobahn peer | `AUTOBAHN_CPUSET` |
| 1-3 | TLS-Anvil | `TLS_ANVIL_CPUSET` |
| 2 | packetdrill | `PACKETDRILL_CPUSET` |
| 3-5 | `docker-switch-memif-bench` | `MEMIF_CPUSET` |
| 6 | `docker-bench-stack` | `STACK_BENCH_CPU` |
| 7 | the `switch-tap-echo` that packetdrill tests | `PACKETDRILL_SWITCH_CPUSET` |
| 8 | `docker-soak` | `SOAK_CPUSET` |
| 9-12 | `docker-bench-smoke` | `BENCH_CPUSET` |

Override them on the command line, for example `make DOCKER_CPUSET=1 AUTOBAHN_CPUSET=2 PACKETDRILL_CPUSET=3 PACKETDRILL_SWITCH_CPUSET=8 docker-test`. `AUTOBAHN_CPUSET` must differ from `DOCKER_CPUSET`, and `DOCKER_CPUSET`, `PACKETDRILL_CPUSET` and `PACKETDRILL_SWITCH_CPUSET` must all differ.

Every cargo test step runs under `timeout`, 600 seconds by default (`TEST_TIMEOUT`). A hung step fails with exit status 124, or 137 if it ignored the first signal. The whole in-container `make test` has a limit of 3600 seconds (`MAKE_TEST_TIMEOUT`). Each DPDK test body also has its own limit, described in [Writing tests](writing-tests.md#one-thread-for-every-dpdk-test). GNU `timeout` is not on macOS, so a conduit target run there needs `TIMEOUT=` on the command line, for example `make -C conduit test-support TIMEOUT=`.

The Makefiles do not support parallel make, and a checkout runs one suite at a time. Separate worktrees can run at once, each with its own build volume:

```sh
make DOCKER_TARGET_VOLUME=switch-target-mybranch docker-test
```

The packetdrill tests check timing, so a lock shared by every checkout lets one packetdrill run go at a time, and they assume nothing else is saturating Docker's VM.

`DOCKER_PLATFORM=linux/amd64` builds an x86-64 image on an arm64 machine. It runs under emulation, too slow for the suite's time limits; run the suite on an x86-64 machine instead.

## Longer runs

These are not part of `docker-test`:

| Command | What |
|---|---|
| `cargo bench -p switch-runtime` | The runtime's Criterion benchmarks (executor, timer wheel, whole runtime). No DPDK; runs on macOS. Run them before and after a change to the runtime |
| `make -C conduit docker-soak` | A two-stack soak test with an impaired link for `SOAK_SECONDS` (default 600), seeded by `SOAK_SEED` |
| `make -C conduit docker-fuzz` | The Scapy mutation fuzzer against `switch-tap-echo` (`FUZZ_SEED`, `FUZZ_CONNECTIONS`) |
| `make -C conduit docker-bench-stack` | The stack's Criterion benchmarks; `BENCH_FILTER` picks benchmarks by name |
| `make -C conduit docker-tls-anvil` | [TLS-Anvil](conformance.md#tls-anvil) |

## On an EC2 host

`python3 switch-bench/aws/switch_aws_bench.py test --region R` runs three suites natively on one EC2 instance (c6in.2xlarge, or c7gn.xlarge with `--shape arm`) and deletes it afterwards. It needs the same account setup as the [AWS benchmark](../benchmarks/aws.md#what-you-need). On the host, as root:

```sh
# rings: unpinned, because the per-CPU ring tests need at least two CPUs
cargo test --locked --no-fail-fast -p ceph-rseq -p ceph-ring -p ceph-fifo -p switch-junction -p slice_ringbuffer
# the other two run pinned to the first housekeeping CPU
cargo test --locked --no-fail-fast -p switch-junctiond -- --test-threads=1
cargo test --locked --no-fail-fast -p switch-net -p switch-tls \
    --features switch-net/testkit,switch-tls/e2e -- --test-threads=1
```

It writes a `.log` and `.exit` file per suite and `tests.json`, and exits 0 only when every suite passed, every result was fetched and teardown verified that nothing is left.

## API documentation

```sh
make docs
```

builds the API reference of every workspace crate, with all of `switch`'s features, in the Docker image (build it first with `make -C conduit docker-image`), writes it to `target/api-docs`, and opens `target/api-docs/switch/index.html`.
