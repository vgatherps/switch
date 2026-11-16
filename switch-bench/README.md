# switch-bench

An open-loop latency benchmark: switch generators send messages at fixed rates to an echo server and record each echo's latency and the share lost, rate by rate. It is a contributor tool; applications do not use it.

How it works is in [docs/benchmarks/README.md](../docs/benchmarks/README.md); running it locally in [local.md](../docs/benchmarks/local.md), on EC2 in [aws.md](../docs/benchmarks/aws.md), and reading the report in [reports.md](../docs/benchmarks/reports.md).

```sh
make -C conduit test-bench           # cargo tests with the e2e tap test; DPDK, NET_ADMIN, /dev/net/tun
make -C conduit docker-bench-smoke   # the local smoke run
python3 switch-bench/test_report.py  # report.py's tests
python3 -m unittest discover -s switch-bench/aws    # the AWS runner's tests; no AWS account
python3 -m unittest discover -s switch-bench/local  # the junction smoke's echo reader
```
