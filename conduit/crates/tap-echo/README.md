# tap-echo

TCP echo server on a `Stack` over a net_tap netdev, plus the driver that relay's `relay-autobahn` and `binance-feed` reuse. It is the zhida target for the packetdrill scripts and the Scapy fuzzer.

Depends on [`dpdk-smoltcp`](../dpdk-smoltcp/README.md), `zhida-buffer` and `zhida-dpdk-sys`. relay (feature `e2e`) and `binance-feed` depend on it for `driver`.

## Contents

- Binary `tap-echo`: `tap-echo [--listeners N] [--buffer BYTES] [--close-after BYTES] -- <tester> [args...]`. It spawns the tester before DPDK starts, serves echo on port 8080 until the tester exits, and returns an exit status (0 tester exited 0, 1 tester failed, 2 packet buffers still in use at port close, 3 bad arguments or a setup, serve or teardown error). `tap-echo` with no valid arguments prints the full usage, including the fixed addressing: Zhida 192.0.2.2/24, tester 192.0.2.1, netdev `cdtap_echo`.
- `EchoServer`, `EchoError`, `serve(stack, echo, start, stop)` in `lib.rs`: the echo application and its drive loop. `with_close_after` makes it close after echoing a set number of bytes.
- `driver` module: `run(...)` (spawn tester, build the Port and Stack on the tap vdev, run an app until the tester exits, tear down, compose the exit status; the app takes the Stack by value and hands it back with its result, so it can own it in a switchnet `Net`), `tester_env()`, `report()`, and the constants for addresses, pool sizes and exit codes.
- `driver::run_tester(...)`: the same tester environment, stop callback and exit-status composition without EAL, a tap device or a Stack. `relay-autobahn --backend junction --junction-socket PATH` uses this while a separate `junctiond` owns the network; the app retires its session before returning.

## Build and test

Needs DPDK 25.11.3, `CAP_NET_ADMIN` and `/dev/net/tun`; use the Docker image on macOS.

```sh
make test-echo         # cargo test -p tap-echo -- --test-threads=1 --show-output
make test-packetdrill  # packetdrill scripts against tap-echo
make fuzz              # Scapy TCP fuzzer against tap-echo
```

Tests are `tests/echo.rs`, `driver.rs`, `serve.rs` and `kernel_interop.rs`.

## Constraints

- The process must be restricted to one CPU: EAL's main lcore is pinned to it. packetdrill runs put tap-echo and packetdrill on separate CPUs (see [fuzz/packetdrill/README.md](../../fuzz/packetdrill/README.md)).
- The tester must exit on SIGTERM and be run directly or via `exec`, since only the tester process is killed.
