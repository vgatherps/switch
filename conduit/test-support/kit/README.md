# zhida-test-kit

DPDK fixtures shared by the workspace's integration tests and benches. Not published.

Depends on `dpdk-smoltcp`, `zhida-buffer` and `zhida-dpdk-sys`. Dev-dependency of `dpdk-smoltcp`, `stack-stream`, `tap-echo`, `binance-feed`, `memif-bench` and `switchnet`. Its own tests use `zhida-test-support`.

## Modules

| module | provides |
|---|---|
| (root) | `eal_args()`, `with_eal(body)`: EAL with no PCI, no hugepages, 1 GiB, lcore 7 on the one allowed CPU. `eal_args` panics unless exactly one CPU is allowed. |
| `ring_port` | `RingPort`: a net_ring `Port` where the test is the peer, producing RX frames and taking TX frames. Helpers `resize`, `arp`, `frame`, `send`. |
| `pair` | `Pair`: two `Stack`s on net_ring ports joined by copying links on one counter clock. `new_pair`, `establish`, `config(Side)`, `tcp_frame`, `segment`, `pattern`. |
| `many` | Many established TCP connections over a `Pair` (`open`, `assert_quiet`, `WAVE`, `SERVER_BASE`, `CLIENT_BASE`). |
| `peers` | On-link peers for neighbor-cache tests and benches (`peer_ip`, `peer_mac`, `respond`, `connect_all`). |
| `wire` | `Link`: one direction of a lossy copying wire, with `Verdict` per frame; pool lookups `pool_by_name`, `in_use`, `pool_exists`, `assert_returned_exactly_once`. |
| `tap` | `TapVdev`: a net_tap vdev with a kernel netdev. Needs `CAP_NET_ADMIN` and `/dev/net/tun`. |
| `pcap` | `PcapVdev` (net_pcap vdev) and `write_pcap`. |

Fixtures that own vdevs or rings must be declared before the `Port` they back so the `Port` drops first. Dropped without `close`, they clean up and, outside a panic, fail the test.

## Test

```sh
make test-stack    # cargo test -p dpdk-smoltcp -p zhida-test-kit -- --test-threads=1
```

Needs DPDK 25.11.3 and a one-CPU process; use the Docker image on macOS (`make docker-test`). The tap tests need the container flags in the Makefile (`--cap-add NET_ADMIN --device /dev/net/tun`).
