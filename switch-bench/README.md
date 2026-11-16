# switch-bench

See [Measuring latency under load](../book/src/measuring/index.md) in the switch book.

Latency/load benchmark of the switch stack: open-loop generators (switch apps), a switch echo receiver, a fixed
rate sweep, and a report.

Socket opening runs in switch tasks: UDP binding finishes while the host runs rounds (receive, poll, send) before the
generator reports ready or the UDP server starts serving. TCP connect and accept futures report
opening failures when polled. Send backpressure uses `switch_net::Error::Full`; UDP also pauses for
`NeighborPending`.
Conduit TCP/WS `Full` means one connection is flow-controlled: that sender's messages are owed.
Junction TCP/WS `Full` means the shared send ring is full, so the generator stops that poll's send
batch and counts a stop (`stops`), like a missing TX frame or a UDP `Full`/`NeighborPending`; the
messages due then are not fresh sends and do not count toward `behind_schedule`.

`gen` and `serve` accept `--backend conduit|junction` (default `conduit`). Conduit still requires
`--config F` and owns the NIC in the app process. Junction requires `--junction-socket PATH` instead:
start `switch-junctiond --config F --dir DIR` on its own isolated CPU, then point the app at `DIR/control.sock`
on another isolated CPU. Both receiver and generator support either backend, for every protocol.
Junction uses the session's advertised frame capacity and maximum record size for device-dependent
sizing; it never opens EAL or reads a NIC config. With `SWITCH_BENCH_XSTATS=1`, junction labels port
xstats absent rather than reporting synthetic counters. Conduit's ENA allowance/reset counters do not
establish whether junction was shaped or reset; that requires counters from junction's own run.
UDP `serve` requests one junction session slot; TCP and WebSocket still request 320.
WebSocket upgrade requests retain their frame and retry a full send ring between rounds,
within the existing 10-second connection deadline.

```sh
taskset -c 2 switch-junctiond --config nic.json --dir /run/switch-junctiond --control-cpus 0
taskset -c 3 switch-bench serve --backend junction --junction-socket /run/switch-junctiond/control.sock \
    --proto udp --port 9000
```

## Pieces

- `switch-bench sweep`: the coordinator (no DPDK). Starts G `gen` workers and runs every rate
  `--rate-min` x sqrt(2)^k (k = 0, 1, .. while at most `--rate-max`; 10k..5M is 18 steps) on all of them, rate
  and senders split evenly. Worker g starts each step g / rate s after the common start, so the workers'
  schedules interleave instead of sending in phase. Results are merged (counts summed, histograms merged:
  percentiles are never averaged). Prints one `step` JSON line per rate (`offered_pps`, `sent_pps` /
  `received_pps`, `drop_fraction`, `latency_ns` p50/p99/p99.9/max, `budget_bound`, `generator_bound`,
  `service_rtt_ns` (echo time - actual send time) and `send_lag_ns` (actual - scheduled send time of
  fresh sends) with the same quantiles, `sent` / `echoed` (messages sent, and of those echoed by the end
  of the drain, over the whole step, warmup included), `gen_tx_packets` / `gen_rx_packets` (packets the
  generators' own NICs sent and received over the same span: read when a worker gets the step command and
  after its drain, never inside the window, because on ENA each read is an admin command that sleeps at
  least 100 µs; each worker owns its port and the counts are summed; generator-side, not the receiver's),
  and `workers`: per worker `behind_schedule`, `backlog_at_end`, `stops`, and `last_send_ns` /
  `last_echo_ns` after its step start). The report shows `gen_tx_packets / sent` and
  `gen_rx_packets / echoed`. Junction has no generator-local port counters: these JSON fields are
  `null`, and the report renders their ratios as `-`, never using conduit counters for junction.
  `sent_pps` counts messages accepted by the generator stack, not NIC transmissions; `received_pps` counts
  echoes arriving within the window, including warmup echoes. Always prints one
  `summary` line (`steps`, `complete`, `generator_bound`, `behind_workers`, `error`). No knee is computed:
  the table and plot show it. A worker is `behind_schedule` when its fresh-send lag p99 is over 20 µs; fresh
  sends exclude messages a full connection (or one at its budget) owed and messages already due when a
  pool stop ended a poll, so receiver backpressure and pool stops show in `stops` / `backlog_at_end` and
  never end the sweep. A step with any worker behind schedule is `generator_bound`: the sweep stops, names
  the workers and exits non-zero. Exit 0 iff `complete` and not
  `generator_bound`. When a worker fails, the steps already printed stand and the
  summary says `complete: false`. A worker that does not answer within the step's time plus 10 s fails the
  sweep, and every worker's process group is killed. A shutdown failure also kills all workers still awaiting
  shutdown, including their child processes, before returning the incomplete summary.
  Before each step every worker's wall clock is probed: one
  more than 1 ms plus half the probe's round trip off the coordinator's fails the sweep, and so does a worker that gets a step after its start
  time (50 ms ahead). `--budget-us W` (TCP, ws): the first step runs without a budget and its median
  service round trip (echo time - actual send time) is the baseline; from then on each connection may have its
  rate share x (baseline + W) messages sent and not yet echoed. A message due beyond that waits, its latency
  still charged from its scheduled time; a step where any measured message waited is `budget_bound`.
- `switch-bench gen`: one generator worker, a switch Stack on its own Port and core, driven over stdin/stdout by
  the coordinator. N senders send on a fixed schedule: message k is due at `t0 + k*interval/N`, regardless of
  replies. UDP: N sockets. TCP: N connections carrying S-byte messages, the echoed byte
  stream cut back into messages. S must be at least 24 B. ws: N ws:// connections (a bench-only client over the same TCP halves:
  fixed-key upgrade, one masked binary frame per message, S at most 1400 B; the echo
  stream is cut into equal-size frames, and any other frame fails the worker). At most 32 tries
  per poll, then the loop runs the rest of the round (send, then the next receive); a message that waits keeps its scheduled time. A full TCP connection
  (or one at its budget) blocks only its own sender: its messages are owed and go out in order later, and
  their wait is the receiver's doing, not send lag. Latency = receive time - scheduled time (no coordinated
  omission); not echoed = measured messages not echoed by the end of the drain. Sending stops at the end of the
  window: what is still owed or behind is never sent, so a step past the knee ends on time.
- `switch-bench serve`: the switch echo receiver (UDP, or TCP/ws with 64 persistent accept tasks; ws through
  switch-ws's `accept_ws` and `Ws`). Accepts retry after socket exhaustion so listening resumes as slots free.
  Its loop makes no syscall while serving; it runs until SIGINT/SIGTERM.
- `switch-bench kernel-serve --proto udp|tcp|ws --bind IP --port P`: Linux-only kernel-socket echo
  receiver. The generator remains switch; TCP uses the same fixed-size byte stream and WS is plain
  `ws://`, not TLS.
- `report.py DIR`: `report.md` (one step table per sweep), `results.json` and one SVG per sweep
  (latency quantiles and not echoed % against offered rate; a step with no echo stays on the plot). Exit 1 when
  incomplete: an expected sweep missing, a sweep without summary or with `complete: false`, a sweep
  stopped because a worker was behind schedule (`<sweep>: sender behind schedule: worker(s) [..]`), or
  placement errors. Each table also shows service RTT and send-lag p50/p99 and the generator packet ratios. Python stdlib only; tests: `python3 switch-bench/test_report.py`.

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

In the Docker image: the switch receiver and each generator on its own net_tap port, subnet and CPU, the kernel
routing between them (its side of each tap is the gateway). Runs, per protocol, a sweep against the receiver
(TCP/ws with `--budget-us 50`), then the bounded-overload check (a TCP sweep against a listener that never
reads must end within seconds with 100% loss reported and a complete summary), then writes
`switch-bench/out/smoke/report.md`; the make target fails if the report is incomplete. Every process's stderr is
kept in `switch-bench/out/smoke/*.log`. It validates the tooling; net_tap numbers are not measurements.
smoke.sh accepts a sweep that stops because a worker fell behind schedule and runs the remaining checks;
report.py then marks that sweep incomplete and exits 1, so the make target fails. A bridge does not work here: net_tap gives
the kernel side of a tap the switch MAC, which a bridge
takes as its own and keeps the frames.

For junction (build `cargo build --locked -p switch-junctiond -p switch-bench --bins` first), run
`sh switch-bench/local/junction-smoke.sh` inside the DPDK container with two CPUs and NET_ADMIN plus
`/dev/net/tun`. `JUNCTION_CPU=0 APP_CPU=2` are defaults. It runs the daemon on a tap vdev, checks
kernel-client UDP and TCP echoes through `serve --backend junction`, then runs a short UDP
`gen --backend junction` against a kernel echo peer. A peer closing before a complete TCP echo fails
immediately rather than spinning on EOF. All processes must retire cleanly.
The echo reader's regression tests need only Python: `python3 -m unittest discover -s switch-bench/local`.

## Tests

`cargo test -p switch-bench` (Docker image: the crate links DPDK) covers the schedule, the pacer (per-poll budget
under overload, a blocked sender not holding back the others, flow control kept apart from backlog, the
backlog at the cutoff), receive accounting (latency, warmup, loss, ignored duplicates/stale/malformed echoes,
in-window receives), the histogram, the rate list, the worker start offsets, merging of worker results
(achieved rates, loss, budget, sender delays), the outstanding-budget rule, the coordinator's reply and exit
timeouts (silent workers and their process groups killed), cleanup of remaining process groups after a worker
fails shutdown, its clock and late-start checks, its summary after a worker dies, after total loss and after
a sender-late step (fake workers), the TCP stream cutter and the
ws client framing. With `--features e2e` (`make -C conduit test-bench`, in `make test`; needs NET_ADMIN and
/dev/net/tun) it also runs the TCP echo receiver on net_tap against a kernel peer whose small segments conduit
appends into one received frame larger than a TX frame: every byte must come back.

`python3 -m unittest discover -s switch-bench/aws` needs no AWS account. Its SSH authentication regression
starts a temporary localhost `sshd` and separate `ssh-agent` with unrelated keys; it never changes
the user's agent. It checks real SSH/SCP authentication and sender SSH configuration with the run
key. Missing OpenSSH executables skip that test; an installed but unusable server fails visibly.

## AWS

`--region` defaults to the AWS profile's region (`aws configure`, or `AWS_REGION`); without either the runner stops before touching AWS. AWS project accounts (the "new AWS experience") are locked to one region, so leave it at the profile's. Binance's servers are in Tokyo: from another region the `binance` command's NIC-to-receive latency is still valid, only the wire path to Binance is longer.

`python3 switch-bench/aws/switch_aws_bench.py run [--shape arm|intel] [--region R] [--ttl 2h] [--out DIR]` runs the whole thing. The default `arm` shape uses four c7gn.xlarge instances (one receiver, three senders; 16 vCPUs total) and an arm64 Amazon Linux 2023 image. `--shape intel` uses one c6in.8xlarge receiver and three c6in.2xlarge senders (56 vCPUs total) and an x86_64 Amazon Linux 2023 image. For the Intel run in Tokyo, use `--shape intel --region ap-northeast-1`. The same shape option works with `plan`, `preflight` and `kernel`.

`--backend conduit junction` runs the same five sweeps on each backend; the default is `conduit`.
For each protocol/budget pair, backends run back to back in the selected order before the next pair.
Junction's uncapped TCP/WS steps after the first loss or SENDER-LATE step are labeled CARRYOVER:
unbounded daemon queues can retain earlier work, so those later measurements are not comparable.
Budgeted sweeps remain unchanged.
Every sweep artifact is prefixed with its backend (`conduit-…` or `junction-…`).
Junction starts a separate `switch-junctiond` on every receiver and generator host before each sweep, pinned
to `JUNCTION_CPU` with its control thread on `HOUSEKEEPING`; benchmark apps stay on `APP_CPU`.
The runner waits for the control socket before launching the app and stops the daemon afterward.
Host setup reserves both physical cores and their SMT siblings on the data ENI's NUMA node;
conduit continues to use only `APP_CPU`. In the `kernel` comparison, the receiver stays on the
kernel and `--backend` selects the generator backend.
Daemon startup failures fail that sweep without skipping the remaining matrix. Each attempted daemon
gets an invocation-scoped `TAG.ROLE.switch-junctiond.log`, ending with its exit status; nonzero or unavailable
statuses are also logged as warnings.

Before the coordinator starts, each host launches a housekeeping-pinned placement waiter. It waits
up to 60 seconds for that host's expected processes and their main threads' final single-CPU pinning,
so conduit EAL's temporary startup mask is not mistaken for measurement affinity.
`TAG.ROLE.placement.txt` records every userspace thread's TID, name, allowed CPU list and current/last
CPU, the configured assignments, the kernel's isolated CPU list, and `/proc/interrupts`.
After the penultimate rate result is flushed, all hosts capture `TAG.ROLE.placement.end.txt`
concurrently. Its timestamp must precede a conservative final-measurement deadline derived from
that result's file mtime. If the coordinator already stopped early, its summary supplies the failure
reason; shutdown-time or missing end snapshots do not add placement failures.
Network/app main threads must be pinned to their assigned single CPU; other
daemon/app/EAL threads and coordinator descendants must stay off the backend's used isolated CPUs.
Only the junction app's `junction-ctl` reader may share `APP_CPU`.
Both snapshots are checked after a successful sweep. Unrelated userspace tasks whose masks include
used isolated CPUs also invalidate it: conduit uses `APP_CPU`, junction additionally uses
`JUNCTION_CPU` where its daemon runs. Activity on reserved but unused isolated CPUs is reported only.
Interrupt deltas on all isolated CPUs are reported per host. Device IRQs firing on used CPUs invalidate
the sweep; local interrupts such as LOC/RES and arm64's `arch_timer` action do not. An IRQ absent at
the start has a zero baseline, not an automatic failure.
The kernel receiver comparison exempts only its already-verified data-ENI IRQs on `APP_CPU`;
every other device IRQ on a used isolated CPU still invalidates the sweep.
A violation, readiness timeout, or missing snapshot marks the sweep INVALID without skipping later
sweeps. Per-backend report tables
show each host's CPU IDs and process core count: conduit uses one isolated app core, junction uses
two (the kernel receiver comparison still uses one receiver core).

Before creating anything, preflight checks for a clean committed tree containing `deploy/hft-host.sh` (the hosts build `git archive HEAD`), AWS credentials, enough free on-demand Standard vCPU quota L-1216C47A, and an availability zone offering every selected instance type. The subnet and cluster placement group use that common zone. An offering does not guarantee capacity; either instance launch can still fail.
Use `--zone Z` with `run`, `kernel` or `preflight` to select a common availability zone explicitly
when the default zone lacks capacity.
Each benchmark run records the exact commit and SHA-256 of its source archive in `source.json`, `report.md`, and `results.json`.

The run creates and tags a VPC, subnet, internet gateway, route table, security group (SSH from your public IP), cluster placement group and key pair; each on-demand instance has one DPDK data ENI. The ARM hosts launch in one request; Intel receiver and senders launch in separate requests. Every returned instance ID is recorded before waiting or making the next launch, in memory and `RUN_DIR/created.json`. Each host runs `deploy/hft-host.sh apply` (builds DPDK and the toolchain, reboots), then `verify`, then `switch-bench/aws/bootstrap.sh` (`cargo build --release --locked -p switch-bench -p switch-junctiond` with hft-host.sh's build env).

The fixed matrix has five sweeps: UDP uncapped, TCP with `--budget-us 50` and uncapped, and WS with `--budget-us 50` and uncapped. Comparing the capped and uncapped sweeps shows whether the backlog cap shifts the knee. Each sweep uses 12 connections (4 per sender host), 64 B messages, and rates of 10k, 100k, 400k, 800k, 900k, 1M, 1.1M, 1.2M, 1.4M, 1.8M and 2.5M msg/s. The coordinator on sender0 drives one worker per sender host over SSH; a failed sweep is logged and the next one runs; every sweep's files are fetched. Teardown runs on normal exit, errors, Ctrl-C and SIGTERM; once it starts, Ctrl-C/SIGTERM are ignored, and every ssh/scp before it has a timeout. It deletes everything the run created by recorded ID and by tag (instances, data ENIs, key pair, placement group, security group, subnet, route table, internet gateway, VPC, in that dependency order), checks nothing is left, and exits non-zero if anything remains. Then `switch-bench/report.py RUN_DIR`. Each instance also powers itself off (terminates) at the TTL as a backstop. Exit 0 only if report.py passed, every fetch succeeded and nothing remains.

For the Linux-kernel receiver comparison, run one command:

```sh
python3 switch-bench/aws/switch_aws_bench.py kernel --region R
```

Each command provisions fresh hosts. The kernel comparison uses the same host types, three
Switch/DPDK sender workers, coordinator, 64-byte payloads, rate steps, warmup/measurement/drain
schedule, TCP/WS budget, reports and teardown; it does not introduce a kernel generator. The
receiver's data ENI stays on Linux `ena` throughout host setup and reboot, while the senders'
data ENIs use DPDK. The receiver uses the same CPU isolation and initial IRQ policy as the
senders. Its data interface is brought up with the MTU in `/etc/switch/dut.json`, and the run fails
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
report with `run --region R` for the switch receiver; no performance numbers are implied here.

If IRQ placement is deferred, a sender sends a brief UDP burst to the receiver before measurement;
the runner then waits up to five seconds for actual IRQ affinities to converge without rewriting them. On failure,
`kernel-irq-affinity-error.json` saves each data-ENI IRQ's name, requested and effective CPU lists,
any write error, and available kernel IRQ debug state. A failed post-traffic check also retains the
original placement attempt under `initial`.

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
An observed failed `switch-host.service` stops polling early. Before teardown, an apply failure,
failed service or verification timeout includes the last verify output (when available), the service
status and current-boot journal, and loaded/on-disk `vfio_pci_core` source versions in the setup
error. If SSH prevents collecting those diagnostics, the error reports that failure too.

The AWS runner measures each rate for 10 seconds, after 250 ms warmup and a 50 ms lead, then waits
200 ms for replies. All 11 rates take 110 seconds of measurement and 115.5 seconds of scheduled time
per sweep. The coordinator's 15-minute timeout is a backstop, not a bound on every possible
sequence of per-worker timeouts. Setup, worker startup, clock probes, result collection and
teardown take additional time. Manual `switch-bench sweep` defaults remain 2 seconds measurement / 500 ms warmup /
200 ms drain.

`plan` prints the hosts, sweeps and what gets created, with no AWS calls; `preflight` runs only the checks; `cleanup --run-id ID [--region R] [--out DIR]` deletes a leftover run using `DIR/ID/created.json` plus a tag lookup (tag lookup only if the file is gone). Needs python3 with boto3, ssh, scp, git; the IAM permissions are listed in the runner's `-h`. Tests (no AWS): `python3 -m unittest discover -s switch-bench/aws`, `sh deploy/test-hft-host.sh`.

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

## Native tests on AWS

```sh
python3 switch-bench/aws/switch_aws_bench.py test --region R [--zone Z] [--ttl 2h] [--out DIR]
```

This provisions one `c6in.2xlarge` with the x86_64 AL2023 image (8 free Standard vCPUs required),
installs and verifies the host, then runs native tests as root in `/opt/switch-src` with bootstrap's
DPDK/clang/Rust environment. It executes the x86 rseq assembly, not just a cross-compile.
`--shape arm` instead selects one `c7gn.xlarge` with arm64 AL2023 (4 free vCPUs).
Source is the pinned committed HEAD; `source.json` records its revision and archive SHA-256.

The three independent suites are:

```sh
cargo test --locked --no-fail-fast -p ceph-rseq -p ceph-ring -p ceph-fifo -p switch-junction -p slice_ringbuffer
. /etc/switch/host.env
test_cpu=${HOUSEKEEPING%%,*}; test_cpu=${test_cpu%%-*}
cargo test --locked --no-fail-fast -p switch-junctiond --no-run
taskset -c "$test_cpu" cargo test --locked --no-fail-fast -p switch-junctiond -- --test-threads=1
cargo test --locked --no-fail-fast -p switch-net -p switch-tls --features switch-net/testkit,switch-tls/e2e --no-run
taskset -c "$test_cpu" cargo test --locked --no-fail-fast -p switch-net -p switch-tls --features switch-net/testkit,switch-tls/e2e -- --test-threads=1
```

DPDK-linked suites build unpinned, then execute on one housekeeping CPU with one test thread.
The rings suite stays unpinned because its rseq tests need at least two CPUs. `--no-fail-fast`
lets every test binary in a suite run even when an earlier binary fails.

Each suite has a one-hour SSH timeout and its own `.log` and `.exit` file (`rings`, `switch-junctiond`,
`switch-net-switch-tls`). A failure does not skip the later suites. Outputs are fetched before teardown,
including on test errors; `tests.json` records each exit code and any transport/fetch errors.
The command exits zero only when all suites and fetches succeeded and teardown verified every
resource absent. The TTL includes provisioning/build/test time; raise it if needed.

## Binance listener on AWS

`python3 switch-bench/aws/switch_aws_bench.py binance [--region R] [--seconds 600] [--out DIR] [--ttl 2h]` runs `deploy/run-binance.sh` on one EC2 host and tears it down. Needs the same as `run` (python3 with boto3, ssh, scp, git, a clean committed tree, credentials; IAM list in `-h`, plus Elastic IP allocate/associate/disassociate/release/describe), and on-demand Standard vCPU quota L-1216C47A with 4 free. Preflight also checks that the region offers m7g.xlarge, before anything is created.

The host is one m7g.xlarge (4 vCPU Graviton3; M7g is on deploy/README.md's list of families with ENA hardware RX timestamps and the PHC). Created for the run, tagged with the run id and recorded in `RUN_DIR/created.json`: VPC, subnet (in a zone offering the type), internet gateway, route table, security group (SSH from your public IP), a `precision-time` placement group, key pair, the instance (powers itself off at the TTL), its data ENI and an Elastic IP on the data ENI (the feed's route to the internet). The host runs `hft-host.sh apply`, `verify`, then `run-binance.sh --seconds N`. Into `RUN_DIR`: `verify.txt` (apply and verify output), `binance.log` and `feed.out` (the feed's output). It prints the message count and the latency counts with the histogram buckets holding p50/p99/p99.9/max, or why there are none. Teardown deletes everything (the Elastic IP disassociated, then released) on exit, errors, Ctrl-C and SIGTERM, and checks nothing is left; `cleanup --run-id ID` deletes a leftover run. Exit 0 only when the feed exited 0, both files were fetched and nothing is left.

Progress reports SSH readiness, setup start and verified completion, then the remote feed request
and its exit status; it does not infer completion of hidden build substages. Host setup makes chrony
discipline CLOCK_REALTIME to the Time Sync PHC; the feed's latency is CLOCK_REALTIME minus each
message's ENA hardware RX stamp, and its startup `phc_check` line gives the data ENI's PHC minus
CLOCK_REALTIME once.
AWS identifies the packet timestamp reference as the
[Amazon Time Sync PTP hardware clock](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/set-time.html#hardware-packet-timestamping).
