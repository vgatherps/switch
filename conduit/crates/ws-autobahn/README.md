# ws-autobahn

tungstenite WebSocket server and client sessions over a zhida `Stack`, run against the Autobahn test suite for conformance.

Depends on [`stack-stream`](../stack-stream/README.md), [`dpdk-smoltcp`](../dpdk-smoltcp/README.md) and [`tap-echo`](../tap-echo/README.md) (its `driver`). Nothing in the workspace depends on it.

## Contents

- Library: `Server`, `ServerError`, `Client`, `ClientError`, WebSocket sessions over a `StackStream`.
- Binary `ws-autobahn`, on the tap-echo driver and netdev `cdtap_echo`:
  - `ws-autobahn server --expect-connections N -- <tester> [args...]` runs an echo server at ws://192.0.2.2:8080, one session at a time, Text and Binary echoed. It fails if the accepted connection count is not N.
  - `ws-autobahn client --expect-cases N -- <tester> [args...]` waits for `autobahn/run/client/server-ready`, then runs Autobahn's fuzzingserver cases 1..=N from zhida (ws://192.0.2.1:9001), echoing Text and Binary. It writes `autobahn/run/client/client-done` when the sequence completes.
  - Exit statuses are tap-echo's. `ws-autobahn` with bad arguments prints the full usage.

The Autobahn report is compared with tungstenite's expected results by `autobahn/check.py` ([autobahn/README.md](../../autobahn/README.md)).

## Build and test

```sh
make test-ws               # cargo test -p ws-autobahn -- --test-threads=1
make test-autobahn-check   # check.py and peer script self-tests; plain Python and sh
make docker-autobahn       # full suite against the Autobahn peer container
```

`test-ws` needs DPDK 25.11.3, `CAP_NET_ADMIN` and `/dev/net/tun` (Docker image on macOS). `docker-autobahn` needs two CPUs: `AUTOBAHN_CPUSET` must differ from `DOCKER_CPUSET`. `make docker-test` runs it last.

tungstenite is pinned to `=0.30.0`.
