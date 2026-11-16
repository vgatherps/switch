# conduit

The part of the switch workspace that owns a network card: the TCP/UDP stack on one DPDK port (`dpdk-smoltcp`), its DPDK bindings and packet buffers, the network config file and its tool, the tools that run on a real card, and the DPDK test fixtures. Applications reach it through the `switch` crate (`switch::net`, `switch::nic`).

The crates are listed in [docs/contributing/crates.md](../docs/contributing/crates.md), running a card in [docs/operations/](../docs/operations/README.md), and the tests in [docs/contributing/testing.md](../docs/contributing/testing.md). The API reference is the `switch` crate's rustdoc (`make docs` at the repository root).

```sh
make docker-image    # Ubuntu 24.04, Rust, DPDK 25.11.3 built from source
make help            # the per-suite test targets, which run natively on Linux or in the image
```

## License

Copyright (C) 2026 vgatherps. GPL-3.0-only, see [LICENSE](LICENSE).

Imported files keep their own licenses:

- `vendor/smoltcp`: 0BSD (`vendor/smoltcp/LICENSE-0BSD.txt`).
- packetdrill scripts from freebsd-net/tcp-testsuite: BSD-2-Clause, notice in each file.
- packetdrill scripts from google/packetdrill, and `docker/packetdrill-relearn-isn.patch`:
  GPL-2.0-or-later.
- `docker/dpdk-ena-read-clock.patch` (to DPDK's ENA driver): BSD-3-Clause.
- `autobahn/expected-results-switch-ws.json`: derived from tungstenite's expected results, MIT OR
  Apache-2.0 (`autobahn/LICENSE-MIT-tungstenite`).

Sources and commits: [fuzz/packetdrill/README.md](fuzz/packetdrill/README.md#upstream-suites),
[autobahn/README.md](autobahn/README.md).
