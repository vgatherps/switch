# zhida-bench

Latency/load benchmark of the Zhida stack: open-loop generators (Zhida apps), a Zhida or kernel echo DUT, a rate
sweep to the knee, and a report. Design: `plans/bench/design.md`.

## Pieces

- `zhida-bench sweep`: the coordinator (no DPDK). Starts G `gen` workers, runs each rate step on all of them at a
  common start time (rate and senders split evenly), merges their results (counts summed, histograms merged:
  percentiles are never averaged), judges the step (bad: p99 above `--p99-factor` x the first step's p99, no echo
  at all, or drop above `--max-drop`), doubles the rate then bisects to the knee, then runs the last good and
  first bad rates twice more each and two sustained points past the knee (2x and 4x the first bad rate, capped
  at `--rate-max`). The knee is the highest rate run below the lowest rate with any bad run; a sweep that
  reaches `--rate-max` without a bad step reports it as a lower bound (`knee_lower_bound`). Prints one JSON line
  per step (merged numbers, achieved `sent_pps`/`received_pps`, `unsent`, plus `per_gen` with each worker's send
  lag, backlog, ethdev and `Turn` counters) and always a summary line (`knee_pps`, `stop`: knee, rate_max,
  no_good_step, generator_bound or failed, `complete`, `error`): when a worker fails, the steps already printed
  stand and the summary says `complete: false`. A step where any generator fell behind (a backlog at the end of
  its send window, or send-lag p99 over `--lag-limit-us`) is `generator_bound`: it measured the generator, and
  the sweep stops. A worker that does not answer within the step's time plus 10 s fails the sweep, and every
  worker's process group is killed. `--budget-us W` (TCP, ws): the first step runs without a budget and its
  median service round trip (echo time - actual send time) is the baseline; from then on each connection may
  have its rate share x (baseline + W) messages sent and not yet echoed. A message due beyond that waits, its
  latency still charged from its scheduled time; a step where any measured message waited is `budget_bound`,
  with the service RTT and in-flight occupancy to tell receiver queueing from the budget.
- `zhida-bench gen`: one generator worker, a Zhida Stack on its own Port and core, driven over stdin/stdout by
  the coordinator. N senders (UDP sockets) send on a fixed schedule: message k is due at `t0 + k*interval/N`,
  regardless of replies. UDP: N sockets. TCP: N connections carrying S-byte messages (S at most the MSS),
  the echoed byte stream cut back into messages. ws: N ws:// connections (a bench-only client over the same TCP
  halves: fixed-key upgrade, one masked binary frame per message, S at most 1400 B so it is one TCP segment; the
  echo stream is cut into equal-size frames, and any other frame fails the worker). At most `--send-budget`
  tries per poll, then the loop turns the Net; a message that waits keeps its scheduled time. A full TCP
  connection (or one at its outstanding budget) blocks only its own sender: its messages are owed and go out in
  order later (`owed_at_end`, `owed_wait_ns`: the receiver side, not generator backlog). Latency = receive time
  - scheduled time (no coordinated omission); drops = measured messages not echoed by the end of the drain.
  Sending stops at the end of the window: what is still owed or behind is `unsent` (part of the loss), so a step
  past the knee ends on time. TCP/ws connections then get up to 1 s for their echoes to come back
  (`in_flight_after`; the next step's `in_flight_at_start`).
- `zhida-bench pingpong`: closed-loop TCP on one Zhida connection, the round-trip baseline.
- `zhida-bench serve`: the Zhida echo DUT (UDP, or TCP/ws with `--accepts` pre-armed accepts; ws through relay's
  `accept_ws` and `Ws`). During a run its loop makes no syscall: counters go into a
  preallocated ring every 100 ms and are written to `--counter-log` on SIGINT/SIGTERM, with ethdev stats from
  start and end only. SIGUSR1 prints each connection's state to stderr (bytes received and echoed, and what
  its task waits for: recv, a TX frame, or send-buffer room).
- `zhida-bench kernel-serve`: the kernel echo DUT, UDP, TCP or ws (tungstenite) (baseline, and the
  generator-headroom check). SIGUSR1 prints each TCP/ws connection's bytes in and out and whether it is
  reading or writing.
- `report.py DIR`: `report.md` (knees, per-sweep tables, ping-pong, the DUT counter-capture check),
  `results.json` and one SVG per sweep. Python stdlib only; tests: `python3 bench/test_report.py`.

Protocols: UDP, TCP, ws:// (no TLS). Each sweep table gives the slowest generator worker's cost per round (µs per
Net turn + Runtime poll, task polls per round). The binary's global allocator is mimalloc; `MIMALLOC_ALLOW_LARGE_OS_PAGES=1`
puts its heap on large pages. Message: step u32, sender u32, seq u64, scheduled ns u64 (24 B header), then padding.

## Local smoke

```sh
make -C conduit docker-bench-smoke     # BENCH_CPUSET (4 CPUs, default 9-12), BENCH_GENS, BENCH_SENDERS,
                                       # BENCH_PROTOS, BENCH_SMOKE_ARGS
```

In the Docker image: each Zhida process (DUT, generators, ping-pong) on its own net_tap port, subnet and CPU,
the kernel routing between them (its side of each tap is the gateway; the kernel echo listens there). Runs, per
protocol, a sweep against each DUT (TCP/ws against the Zhida DUT with `--budget-us 50`; ping-pong for TCP), the
counter-capture check, the bounded-overload check (a TCP sweep against a listener that never reads must end
within seconds with 100% loss reported and a complete summary), then writes `bench/out/smoke/report.md`; the
make target fails if the report is incomplete. Every process's stderr is kept in `bench/out/smoke/*.log`; a
failed sweep sends SIGUSR1 to the DUT and kernel echo processes, saves `ss -tnoi` as `ss.txt`, and stops the smoke.
It validates the tooling; net_tap numbers are not measurements (the generators
hit the per-packet tap syscall early, so sweeps often end generator-bound). A bridge does not work here: net_tap
gives the kernel side of a tap the Zhida MAC, which a bridge takes as its own and keeps the frames.

## Tests

`cargo test -p zhida-bench` (Docker image: the crate links DPDK) covers the schedule, the pacer (per-poll budget
under overload, a blocked sender not holding back the others, flow control kept apart from backlog, unsent
counts at the cutoff), receive accounting (latency, warmup, loss, duplicates, stale, in-window receives), the
histogram, the sweep state machine (repeats, past-knee points, lower bound, total loss), merging of worker
results (achieved rates, budget), the outstanding-budget rule, the coordinator's reply and exit timeouts (silent
workers and their process groups killed) and its summary after a worker dies (fake workers), the TCP stream
cutter, the ws client framing and the counter ring. With
`--features e2e` (`make -C conduit test-bench`, in `make test`; needs NET_ADMIN and /dev/net/tun) it also
runs the TCP echo DUT on net_tap against a kernel peer whose small segments conduit appends into one
received frame larger than a TX frame: every byte must come back.

## AWS

`bench/aws/zhida_aws_bench.py run|plan|cleanup` provisions spot hosts in one region, builds natively, applies
`deploy/hft-host.sh`, runs the matrix (Zhida and kernel DUTs, kernel sockperf sender), writes the report and
tears down; everything billable expires at `--ttl` without the laptop (a scheduled janitor Lambda plus an
instance power-off timer). Needs python3 with boto3, ssh, git and AWS credentials. Tests (no AWS):
`python3 bench/aws/test_janitor.py`, `python3 bench/aws/test_runner.py`, `sh deploy/test-hft-host.sh`.
