# zhida-bench

Latency/load benchmark of the Zhida stack: open-loop generators (Zhida apps), a Zhida echo receiver, a fixed
rate sweep, and a report. Design: `plans/bench/design.md`.

## Pieces

- `zhida-bench sweep`: the coordinator (no DPDK). Starts G `gen` workers and runs every rate
  `--rate-min` x sqrt(2)^k (k = 0, 1, .. while at most `--rate-max`; 10k..5M is 18 steps) on all of them, rate
  and senders split evenly. Worker g starts each step g / rate s after the common start, so the workers'
  schedules interleave instead of sending in phase. Results are merged (counts summed, histograms merged:
  percentiles are never averaged). Prints one `step` JSON line per rate (offered, achieved `sent_pps` /
  `received_pps`, latency p50/p99/p99.9/max, `drop_fraction`, `unsent`, `budget`, `budget_bound`,
  `generator_bound`) and always one `summary` line (`complete`, `generator_bound`, `error`, `steps`,
  `baseline_rtt_p50_ns`). No knee is computed: the table and plot show it. A step where any generator fell
  behind (a backlog at the end of its send window, or send-lag p99 over `--lag-limit-us`) is `generator_bound`:
  it measured the generator, so the sweep stops there with `generator_bound: true` and exits non-zero. Exit 0
  iff `complete` and not `generator_bound`. When a worker fails, the steps already printed stand and the
  summary says `complete: false`. A worker that does not answer within the step's time plus 10 s fails the
  sweep, and every worker's process group is killed. Before each step every worker's wall clock is probed: one
  more than 1 ms off the coordinator's fails the sweep, and so does a worker that gets a step after its start
  time (`--lead-ms` ahead). `--budget-us W` (TCP, ws): the first step runs without a budget and its median
  service round trip (echo time - actual send time) is the baseline; from then on each connection may have its
  rate share x (baseline + W) messages sent and not yet echoed. A message due beyond that waits, its latency
  still charged from its scheduled time; a step where any measured message waited is `budget_bound`.
- `zhida-bench gen`: one generator worker, a Zhida Stack on its own Port and core, driven over stdin/stdout by
  the coordinator. N senders send on a fixed schedule: message k is due at `t0 + k*interval/N`, regardless of
  replies. UDP: N sockets. TCP: N connections carrying S-byte messages (S at most the MSS), the echoed byte
  stream cut back into messages. ws: N ws:// connections (a bench-only client over the same TCP halves:
  fixed-key upgrade, one masked binary frame per message, S at most 1400 B so it is one TCP segment; the echo
  stream is cut into equal-size frames, and any other frame fails the worker). At most `--send-budget` tries
  per poll, then the loop turns the Net; a message that waits keeps its scheduled time. A full TCP connection
  (or one at its budget) blocks only its own sender: its messages are owed and go out in order later, and
  their wait is the receiver's doing, not send lag. Latency = receive time - scheduled time (no coordinated
  omission); drops = measured messages not echoed by the end of the drain. Sending stops at the end of the
  window: what is still owed or behind is `unsent` (part of the loss), so a step past the knee ends on time.
- `zhida-bench serve`: the Zhida echo receiver (UDP, or TCP/ws with `--accepts` pre-armed accepts; ws through
  relay's `accept_ws` and `Ws`). Its loop makes no syscall while serving; it runs until SIGINT/SIGTERM.
- `report.py DIR`: `report.md` (a sweeps table and per-sweep step tables), `results.json` and one SVG per sweep
  (latency quantiles and drop % against offered rate; a step with no echo stays on the plot). Exit 1 when
  incomplete: an expected sweep missing, a sweep without summary or with `complete: false`, or any sweep
  generator-bound. Python stdlib only; tests: `python3 bench/test_report.py`.

Protocols: UDP, TCP, ws:// (no TLS). The binary's global allocator is mimalloc;
`MIMALLOC_ALLOW_LARGE_OS_PAGES=1` puts its heap on large pages. Message: step u32, sender u32, seq u64,
scheduled ns u64 (24 B header), then padding.

## Local smoke

```sh
make -C conduit docker-bench-smoke     # BENCH_CPUSET (2 + BENCH_GENS CPUs, default 9-12), BENCH_GENS,
                                       # BENCH_SENDERS, BENCH_PROTOS, BENCH_SMOKE_ARGS
```

In the Docker image: the Zhida receiver and each generator on its own net_tap port, subnet and CPU, the kernel
routing between them (its side of each tap is the gateway). Runs, per protocol, a sweep against the receiver
(TCP/ws with `--budget-us 50`), then the bounded-overload check (a TCP sweep against a listener that never
reads must end within seconds with 100% loss reported and a complete summary), then writes
`bench/out/smoke/report.md`; the make target fails if the report is incomplete. Every process's stderr is
kept in `bench/out/smoke/*.log`. It validates the tooling; net_tap numbers are not measurements (the
generators hit the per-packet tap syscall early, so sweeps often end generator-bound, which the smoke
accepts). A bridge does not work here: net_tap gives the kernel side of a tap the Zhida MAC, which a bridge
takes as its own and keeps the frames.

## Tests

`cargo test -p zhida-bench` (Docker image: the crate links DPDK) covers the schedule, the pacer (per-poll budget
under overload, a blocked sender not holding back the others, flow control kept apart from backlog, unsent
counts at the cutoff), receive accounting (latency, warmup, loss, ignored duplicates/stale/malformed echoes,
in-window receives), the histogram, the rate list, the worker start offsets, merging of worker results
(achieved rates, budget, generator-bound), the outstanding-budget rule, the coordinator's reply and exit
timeouts (silent workers and their process groups killed), its clock and late-start checks, its summary after
a worker dies, after total loss and after a generator-bound step (fake workers), the TCP stream cutter and the
ws client framing. With `--features e2e` (`make -C conduit test-bench`, in `make test`; needs NET_ADMIN and
/dev/net/tun) it also runs the TCP echo receiver on net_tap against a kernel peer whose small segments conduit
appends into one received frame larger than a TX frame: every byte must come back.

## AWS

`--region` defaults to the AWS profile's region (`aws configure`, or `AWS_REGION`); without either the runner stops before touching AWS. AWS project accounts (the "new AWS experience") are locked to one region, so leave it at the profile's. Binance's servers are in Tokyo: from another region the `binance` command's NIC-to-receive latency is still valid, only the wire path to Binance is longer.

`python3 bench/aws/zhida_aws_bench.py run [--region R] [--ttl 2h] [--out DIR]` runs the whole thing. Preflight first, before anything is created: clean committed tree containing `deploy/hft-host.sh` (the hosts build `git archive HEAD`), AWS credentials, on-demand Standard vCPU quota L-1216C47A with 48 free. Then it creates, for this run only and tagged with the run id: a VPC, subnet, internet gateway, route table, security group (SSH from your public IP), cluster placement group and key pair; six c7gn.2xlarge on-demand instances (1 receiver, 5 senders) in one run_instances, one DPDK data ENI each. Every ID is recorded as its create succeeds (in memory and `RUN_DIR/created.json`). Each host runs `deploy/hft-host.sh apply` (builds DPDK and the toolchain, reboots), then `verify`, then `bench/aws/bootstrap.sh` (one `cargo build --release -p zhida-bench` with hft-host.sh's build env). Then the fixed matrix (udp uncapped, tcp and ws `--budget-us 50`; 20 senders, 64 B, 10k..5M msg/s in 18 x sqrt(2) steps) with the coordinator on sender0 driving one worker per sender host over SSH; a failed sweep is logged and the next one runs; every sweep's files are fetched. Teardown runs on normal exit, errors, Ctrl-C and SIGTERM; once it starts, Ctrl-C/SIGTERM are ignored, and every ssh/scp before it has a timeout. It deletes everything the run created by recorded ID and by tag (instances, data ENIs, key pair, placement group, security group, subnet, route table, internet gateway, VPC, in that dependency order), checks nothing is left, and exits non-zero if anything remains. Then `bench/report.py RUN_DIR`. Each instance also powers itself off (terminates) at the TTL as a backstop. Exit 0 only if report.py passed, every fetch succeeded and nothing remains.

`plan` prints the hosts, sweeps and what gets created, with no AWS calls; `preflight` runs only the checks; `cleanup --run-id ID [--region R] [--out DIR]` deletes a leftover run using `DIR/ID/created.json` plus a tag lookup (tag lookup only if the file is gone). Needs python3 with boto3, ssh, scp, git; the IAM permissions are listed in the runner's `-h`. Tests (no AWS): `python3 -m unittest discover -s bench/aws`, `sh deploy/test-hft-host.sh`.

Before the first paid run:
- [ ] quota: `preflight` passes (L-1216C47A >= 48 free in the region)
- [ ] credentials: `preflight` shows the intended account/ARN
- [ ] Ctrl-C trial: start `run`, press Ctrl-C after the instances launch, and check that teardown logs `nothing left` and `cleanup --run-id ID` also reports `nothing left`
- [ ] leftovers from the old shared-resource version (VPC/SG/key pair named `zhida-bench`, S3 bucket `zhida-bench-cache-*`, IAM role/profile `zhida-bench-host`, `~/.zhida-bench/*.pem`) are not deleted by this tool anymore; delete them once by hand if they exist.

## Binance listener on AWS

`python3 bench/aws/zhida_aws_bench.py binance [--region R] [--seconds 600] [--out DIR] [--ttl 2h]` runs `deploy/run-binance.sh` on one EC2 host and tears it down. Needs the same as `run` (python3 with boto3, ssh, scp, git, a clean committed tree, credentials; IAM list in `-h`, plus Elastic IP allocate/associate/disassociate/release/describe), and on-demand Standard vCPU quota L-1216C47A with 4 free. Preflight also checks that the region offers m7g.xlarge, before anything is created.

The host is one m7g.xlarge (4 vCPU Graviton3; M7g is on deploy/README.md's list of families with ENA hardware RX timestamps and the PHC). Created for the run, tagged with the run id and recorded in `RUN_DIR/created.json`: VPC, subnet (in a zone offering the type), internet gateway, route table, security group (SSH from your public IP), a `precision-time` placement group, key pair, the instance (powers itself off at the TTL), its data ENI and an Elastic IP on the data ENI (the feed's route to the internet). The host runs `hft-host.sh apply`, `verify`, then `run-binance.sh --seconds N`. Into `RUN_DIR`: `verify.txt` (apply and verify output), `binance.log` and `feed.out` (the feed's output). It prints the message count and the latency counts with the histogram buckets holding p50/p99/p99.9/max, or why there are none. Teardown deletes everything (the Elastic IP disassociated, then released) on exit, errors, Ctrl-C and SIGTERM, and checks nothing is left; `cleanup --run-id ID` deletes a leftover run. Exit 0 only when the feed exited 0, both files were fetched and nothing is left.
