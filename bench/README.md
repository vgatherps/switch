# zhida-bench

Latency/load benchmark of the Zhida stack: open-loop generators (Zhida apps), a Zhida echo receiver, a fixed
rate sweep, and a report. Design: `plans/bench/design.md`.

## Pieces

- `zhida-bench sweep`: the coordinator (no DPDK). Starts G `gen` workers and runs every rate
  `--rate-min` x sqrt(2)^k (k = 0, 1, .. while at most `--rate-max`; 10k..5M is 18 steps) on all of them, rate
  and senders split evenly. Worker g starts each step g / rate s after the common start, so the workers'
  schedules interleave instead of sending in phase. Results are merged (counts summed, histograms merged:
  percentiles are never averaged). Prints one `step` JSON line per rate (`offered_pps`, `sent_pps` /
  `received_pps`, `drop_fraction`, `latency_ns` p50/p99/p99.9/max, `budget_bound`, `generator_bound`).
  `sent_pps` counts messages accepted by the generator stack, not NIC transmissions; `received_pps` counts
  echoes arriving within the window, including warmup echoes. Always prints one
  `summary` line (`steps`, `complete`, `generator_bound`, `error`). No knee is computed: the table
  and plot show it. A step where any generator fell behind (a backlog at the end of its send window, or
  send-lag p99 over 20 µs) is `generator_bound`:
  it measured the generator, so the sweep stops there with `generator_bound: true` and exits non-zero. Exit 0
  iff `complete` and not `generator_bound`. When a worker fails, the steps already printed stand and the
  summary says `complete: false`. A worker that does not answer within the step's time plus 10 s fails the
  sweep, and every worker's process group is killed. A shutdown failure also kills all workers still awaiting
  shutdown, including their child processes, before returning the incomplete summary.
  Before each step every worker's wall clock is probed: one
  more than 1 ms off the coordinator's fails the sweep, and so does a worker that gets a step after its start
  time (50 ms ahead). `--budget-us W` (TCP, ws): the first step runs without a budget and its median
  service round trip (echo time - actual send time) is the baseline; from then on each connection may have its
  rate share x (baseline + W) messages sent and not yet echoed. A message due beyond that waits, its latency
  still charged from its scheduled time; a step where any measured message waited is `budget_bound`.
- `zhida-bench gen`: one generator worker, a Zhida Stack on its own Port and core, driven over stdin/stdout by
  the coordinator. N senders send on a fixed schedule: message k is due at `t0 + k*interval/N`, regardless of
  replies. UDP: N sockets. TCP: N connections carrying S-byte messages (S at most the MSS), the echoed byte
  stream cut back into messages. ws: N ws:// connections (a bench-only client over the same TCP halves:
  fixed-key upgrade, one masked binary frame per message, S at most 1400 B so it is one TCP segment; the echo
  stream is cut into equal-size frames, and any other frame fails the worker). At most 32 tries
  per poll, then the loop turns the Net; a message that waits keeps its scheduled time. A full TCP connection
  (or one at its budget) blocks only its own sender: its messages are owed and go out in order later, and
  their wait is the receiver's doing, not send lag. Latency = receive time - scheduled time (no coordinated
  omission); drops = measured messages not echoed by the end of the drain. Sending stops at the end of the
  window: what is still owed or behind is never sent (part of the loss), so a step past the knee ends on time.
- `zhida-bench serve`: the Zhida echo receiver (UDP, or TCP/ws with 64 pre-armed accepts; ws through
  relay's `accept_ws` and `Ws`). Its loop makes no syscall while serving; it runs until SIGINT/SIGTERM.
- `zhida-bench kernel-serve --proto udp|tcp|ws --bind IP --port P`: Linux-only kernel-socket echo
  receiver. The generator remains Zhida; TCP uses the same fixed-size byte stream and WS is plain
  `ws://`, not TLS.
- `report.py DIR`: `report.md` (one step table per sweep), `results.json` and one SVG per sweep
  (latency quantiles and drop % against offered rate; a step with no echo stays on the plot). Exit 1 when
  incomplete: an expected sweep missing, a sweep without summary or with `complete: false`, or any sweep
  generator-bound. Python stdlib only; tests: `python3 bench/test_report.py`.

Protocols: UDP, TCP, ws:// (no TLS). The binary's global allocator is mimalloc. AWS receiver and sender
launches explicitly set `MIMALLOC_ALLOW_LARGE_OS_PAGES=0`, overriding an inherited value so mimalloc
does not take DPDK's reserved HugeTLB pages. Mimalloc's transparent hugepage support stays at its default;
THP backing is possible, not guaranteed. Message: step u32, sender u32, seq u64, scheduled ns u64 (24 B header),
then padding.

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
under overload, a blocked sender not holding back the others, flow control kept apart from backlog, the
backlog at the cutoff), receive accounting (latency, warmup, loss, ignored duplicates/stale/malformed echoes,
in-window receives), the histogram, the rate list, the worker start offsets, merging of worker results
(achieved rates, loss, budget, generator-bound), the outstanding-budget rule, the coordinator's reply and exit
timeouts (silent workers and their process groups killed), cleanup of remaining process groups after a worker
fails shutdown, its clock and late-start checks, its summary after a worker dies, after total loss and after
a generator-bound step (fake workers), the TCP stream cutter and the
ws client framing. With `--features e2e` (`make -C conduit test-bench`, in `make test`; needs NET_ADMIN and
/dev/net/tun) it also runs the TCP echo receiver on net_tap against a kernel peer whose small segments conduit
appends into one received frame larger than a TX frame: every byte must come back.

`python3 -m unittest discover -s bench/aws` needs no AWS account. Its SSH authentication regression
starts a temporary localhost `sshd` and separate `ssh-agent` with unrelated keys; it never changes
the user's agent. It checks real SSH/SCP authentication and sender SSH configuration with the run
key. Missing OpenSSH executables skip that test; an installed but unusable server fails visibly.

## AWS

`--region` defaults to the AWS profile's region (`aws configure`, or `AWS_REGION`); without either the runner stops before touching AWS. AWS project accounts (the "new AWS experience") are locked to one region, so leave it at the profile's. Binance's servers are in Tokyo: from another region the `binance` command's NIC-to-receive latency is still valid, only the wire path to Binance is longer.

`python3 bench/aws/zhida_aws_bench.py run [--region R] [--ttl 2h] [--out DIR]` runs the whole thing. Preflight first, before anything is created: clean committed tree containing `deploy/hft-host.sh` (the hosts build `git archive HEAD`), AWS credentials, on-demand Standard vCPU quota L-1216C47A with 16 free. Then it creates, for this run only and tagged with the run id: a VPC, subnet, internet gateway, route table, security group (SSH from your public IP), cluster placement group and key pair; four c7gn.xlarge on-demand instances (1 receiver, 3 senders; 4 vCPUs each: hft-host.sh isolates CPU 3 for the app, CPUs 0-2 housekeep and run the coordinator on sender0) in one run_instances, one DPDK data ENI each. The shape is two constants in `zhida_aws_bench.py`, `SENDERS` and `HOST_TYPE`; 1 + 5 x c7gn.2xlarge needs 48 vCPUs of quota. Every ID is recorded as its create succeeds (in memory and `RUN_DIR/created.json`). Each host runs `deploy/hft-host.sh apply` (builds DPDK and the toolchain, reboots), then `verify`, then `bench/aws/bootstrap.sh` (one `cargo build --release -p zhida-bench` with hft-host.sh's build env). Then the fixed matrix (udp uncapped, tcp and ws `--budget-us 50`; 12 connections (4 per sender host), 64 B, 10k..5M msg/s in 18 x sqrt(2) steps) with the coordinator on sender0 driving one worker per sender host over SSH; a failed sweep is logged and the next one runs; every sweep's files are fetched. Teardown runs on normal exit, errors, Ctrl-C and SIGTERM; once it starts, Ctrl-C/SIGTERM are ignored, and every ssh/scp before it has a timeout. It deletes everything the run created by recorded ID and by tag (instances, data ENIs, key pair, placement group, security group, subnet, route table, internet gateway, VPC, in that dependency order), checks nothing is left, and exits non-zero if anything remains. Then `bench/report.py RUN_DIR`. Each instance also powers itself off (terminates) at the TTL as a backstop. Exit 0 only if report.py passed, every fetch succeeded and nothing remains.

For the Linux-kernel receiver comparison, run one command:

```sh
python3 bench/aws/zhida_aws_bench.py kernel --region R
```

Each command provisions fresh hosts. The kernel comparison uses the same host types, three
Zhida/DPDK sender workers, coordinator, 64-byte payloads, rate steps, warmup/measurement/drain
schedule, TCP/WS budget, reports and teardown; it does not introduce a kernel generator. The
receiver's data ENI stays on Linux `ena` throughout host setup and reboot, while the senders'
data ENIs use DPDK. The receiver uses the same CPU isolation and initial IRQ policy as the
senders. Its data interface is brought up with the MTU in `/etc/zhida/dut.json`, and the run fails
if the actual MTU differs. New discovery caps MTU at 1500 without raising a smaller NIC MTU;
an explicitly configured jumbo MTU is preserved. Its data IPv4/subnet is assigned without adding a
main-table subnet route. After bringing up the data interface, the runner waits for six consecutive
unchanged main-route comparisons at
0.5-second intervals before installing policy routes. If the routes do not settle within 20 checks,
the run fails. Source routing uses policy table 7000, priority 10000; occupied slots are refused.
Only the data interface gets loose reverse-path filtering. The main table must remain exactly
unchanged across policy installation, and each route to a sender from the receiver data IP must
resolve through the data interface and policy table or the run fails.

`kernel-serve` is pinned to the receiver's `APP_CPU`. Every data-ENI MSI-X IRQ must have effective
affinity exclusively on that CPU; data RX queues must have RPS disabled. Unsupported/managed IRQ
affinity is an error, not a skipped check. Each sweep saves `TAG.kernel-cpu.json` with before/after
data-IRQ counters and host-wide per-CPU `NET_RX` counters. Data IRQs running outside `APP_CPU`,
counter resets, or affinity/RPS drift fail the run. `NET_RX` also includes management traffic:
its housekeeping deltas are reported, not falsely attributed to the data ENI. Compare this run's
report with `run --region R` for the Zhida receiver; no performance numbers are implied here.

The laptop shows run start, hosts up (every host reachable over SSH), and code built (every host's
setup verification and benchmark build succeeded). For each protocol, live coordinator stderr reports
processes up only after every worker replies ready, then processes synced only after every initial clock
probe passes, then sweep start (dispatching the first rate) and sweep end with the completion status.
These milestones appear once per sweep, not once per rate. Stderr is also kept in each `.coord.log`;
JSON results remain in `.jsonl`. A failed startup or clock check never claims synchronization.

SSH/SCP and sender-to-sender SSH use `IdentitiesOnly` with the run key so unrelated ssh-agent keys
do not exhaust the server's authentication attempts.
The initial SSH reachability wait allows 600 s; if that deadline expires, its error includes the
last SSH exit code and stderr (up to 2,000 characters).
Host setup retries verification through the reboot and transient SSH failures for up to 900 s.
An observed failed `zhida-host.service` stops polling early. Before teardown, an apply failure,
failed service or verification timeout includes the last verify output (when available), the service
status and current-boot journal, and loaded/on-disk `vfio_pci_core` source versions in the setup
error. If SSH prevents collecting those diagnostics, the error reports that failure too.

The AWS runner measures each rate for 5 seconds, after 250 ms warmup and a 50 ms lead, then waits
200 ms for replies. The 18 rates take 90 seconds of measurement and 99 seconds of scheduled time
per protocol. Setup, worker startup, clock probes, result collection and teardown take additional
time. Manual `zhida-bench sweep` defaults remain 2 seconds measurement / 500 ms warmup /
200 ms drain.

`plan` prints the hosts, sweeps and what gets created, with no AWS calls; `preflight` runs only the checks; `cleanup --run-id ID [--region R] [--out DIR]` deletes a leftover run using `DIR/ID/created.json` plus a tag lookup (tag lookup only if the file is gone). Needs python3 with boto3, ssh, scp, git; the IAM permissions are listed in the runner's `-h`. Tests (no AWS): `python3 -m unittest discover -s bench/aws`, `sh deploy/test-hft-host.sh`.

Teardown and standalone cleanup keep the recorded IDs and append describe/delete-acceptance witnesses
to the same `created.json`. Keep that file for repeated cleanup, including after a partially failed
teardown. An accepted delete is not proof that an asynchronous deletion has finished: every cleanup
still describes recorded IDs as well as tags. A newly created ID that no describe has shown and whose
delete only answers NotFound remains unverified, even on a later cleanup attempt.
Older journals without witnesses cannot distinguish an already deleted resource from one not yet
visible; NotFound alone remains unverified for those IDs.

Before the first paid run:
- [ ] quota: `preflight` passes (L-1216C47A >= 16 free in the region)
- [ ] credentials: `preflight` shows the intended account/ARN
- [ ] Ctrl-C trial: start `run`, press Ctrl-C after the instances launch, and check that teardown logs `nothing left` and `cleanup --run-id ID` also reports `nothing left`
- [ ] leftovers from the old shared-resource version (VPC/SG/key pair named `zhida-bench`, S3 bucket `zhida-bench-cache-*`, IAM role/profile `zhida-bench-host`, `~/.zhida-bench/*.pem`) are not deleted by this tool anymore; delete them once by hand if they exist.

## Binance listener on AWS

`python3 bench/aws/zhida_aws_bench.py binance [--region R] [--seconds 600] [--out DIR] [--ttl 2h]` runs `deploy/run-binance.sh` on one EC2 host and tears it down. Needs the same as `run` (python3 with boto3, ssh, scp, git, a clean committed tree, credentials; IAM list in `-h`, plus Elastic IP allocate/associate/disassociate/release/describe), and on-demand Standard vCPU quota L-1216C47A with 4 free. Preflight also checks that the region offers m7g.xlarge, before anything is created.

The host is one m7g.xlarge (4 vCPU Graviton3; M7g is on deploy/README.md's list of families with ENA hardware RX timestamps and the PHC). Created for the run, tagged with the run id and recorded in `RUN_DIR/created.json`: VPC, subnet (in a zone offering the type), internet gateway, route table, security group (SSH from your public IP), a `precision-time` placement group, key pair, the instance (powers itself off at the TTL), its data ENI and an Elastic IP on the data ENI (the feed's route to the internet). The host runs `hft-host.sh apply`, `verify`, then `run-binance.sh --seconds N`. Into `RUN_DIR`: `verify.txt` (apply and verify output), `binance.log` and `feed.out` (the feed's output). It prints the message count and the latency counts with the histogram buckets holding p50/p99/p99.9/max, or why there are none. Teardown deletes everything (the Elastic IP disassociated, then released) on exit, errors, Ctrl-C and SIGTERM, and checks nothing is left; `cleanup --run-id ID` deletes a leftover run. Exit 0 only when the feed exited 0, both files were fetched and nothing is left.

Progress reports SSH readiness, setup start and verified completion, then the remote feed request
and its exit status; it does not infer completion of hidden build substages. Host setup enables
`ena.phc_enable=1` through its existing reboot path. The feed explicitly uses `--phc /dev/ptp_ena`;
an absent PHC refuses startup rather than falling back to wall time.
`max_mapping_bound_ns` describes mapper sampling uncertainty under its assumptions, not a measured
bound on cross-interface or AWS clock error. AWS identifies the packet timestamp reference as the
[Amazon Time Sync PTP hardware clock](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/set-time.html#hardware-packet-timestamping);
this run does not independently measure skew between the data and management ENIs.
