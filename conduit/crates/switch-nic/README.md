# switch-nic

The `switch-nic` command opens one NIC port from a network config file and checks it (`info`), answers sockperf clients (`sockperf-server`), or reads Binance's market-data feed (`feed`). It is how you check that a machine is ready before running your own program.

Its usage and output are in [docs/operations/switch-nic.md](../../../docs/operations/switch-nic.md); preparing the machine is in [docs/operations/](../../../docs/operations/README.md).

```sh
cargo build --locked --release -p switch-nic          # needs DPDK 25.11.3
make -C conduit test-switch-nic DOCKER_CPUSET=3       # in the Docker image: unit tests, sockperf_merge.py
                                                      # self-test, then switch-nic against kernel sockperf clients
```

`sockperf_merge.py` combines the `--full-log` files of several `sockperf under-load --full-rtt` clients into one latency summary; `python3 sockperf_merge.py --self-test` checks it.
