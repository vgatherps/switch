# Benchmarking on AWS

`switch-bench/aws/switch_aws_bench.py` runs a fixed benchmark matrix on EC2 in one command. It creates its own network and instances, prepares every host with the script from [Running on EC2](../nic/ec2.md), builds the code on the hosts, runs the sweeps, fetches the results, deletes everything it created and writes the [report](reports.md). This page covers what it needs, each of its commands, the checks that can mark a sweep invalid, and where its output goes.

The runner creates paid resources in your AWS account. Every resource is tagged with the run's id and an expiry time, every instance powers itself off when the run's time-to-live (TTL) runs out, and teardown runs however the script ends.

## Prerequisites

The runner itself runs on your laptop and needs only:

- Python 3 with `boto3`, plus `ssh`, `scp` and `git`,
- AWS credentials with the permissions listed in `switch_aws_bench.py -h` (EC2 networking, instances, key pairs and tags, `ssm:GetParameter` for the Amazon Linux image, `sts:GetCallerIdentity`, `servicequotas:GetServiceQuota`),
- a region: `--region R`, or the region of your AWS profile (`aws configure`, or `AWS_REGION`); without one the runner stops before touching AWS,
- enough free on-demand Standard vCPU quota (AWS quota code `L-1216C47A`) for the chosen shape,
- a clean working tree with everything committed: the hosts build `git archive HEAD`, so uncommitted changes would not be measured.

Nothing builds on the laptop. The hosts build DPDK and `switch-bench` themselves.

## plan and preflight

```sh
python3 switch-bench/aws/switch_aws_bench.py plan [--shape arm|intel] [--backend conduit junction]
python3 switch-bench/aws/switch_aws_bench.py preflight --region R [--shape ...] [--zone Z]
```

`plan` makes no AWS calls. It prints the hosts, the sweeps and every resource a run would create. `preflight` runs the checks a run starts with and creates nothing: the clean committed tree, the credentials (it shows the account it will use), an availability zone that offers every instance type of the shape, and the vCPU quota. Use `--zone Z` to choose the zone yourself when the default one lacks capacity. An offering in a zone does not guarantee capacity; a launch can still fail.

## run

```sh
python3 switch-bench/aws/switch_aws_bench.py run --region R [--shape arm|intel] \
    [--backend conduit junction] [--zone Z] [--ttl 2h] [--out DIR]
```

There are two shapes, each with one receiver host and three sender hosts:

| Shape | Receiver | Senders | vCPUs |
|---|---|---|---|
| `arm` (default) | c7gn.xlarge | 3 × c7gn.xlarge | 16 |
| `intel` | c6in.8xlarge | 3 × c6in.2xlarge | 56 |

`--backend` chooses the receiver's and generators' backend: `conduit` (the local backend, the default), `junction`, or both, which runs each sweep once per backend, back to back.

The matrix is fixed. It has five sweeps: UDP without a budget, TCP with `--budget-us 50` and without, WebSocket with `--budget-us 50` and without. Every sweep uses 12 connections (4 per sender host), 64-byte messages and the same 11 rates: 10k, 100k, 400k, 800k, 900k, 1M, 1.1M, 1.2M, 1.4M, 1.8M and 2.5M messages per second. Each step measures for 10 seconds after a 250 ms warmup, then drains for 200 ms. Sweep files are named `BACKEND-PROTO-n12-s64-uncapped` or `...-b50`.

```text
 laptop: switch_aws_bench.py run
   |
   |  preflight checks
   |  create VPC, subnet, internet gateway, route table, security group, placement group, key pair
   |  launch receiver + 3 senders (one data ENI each)        -> "run <id> start", "hosts up"
   |  every host: hft-host.sh apply (reboots), verify, build  -> "code built"
   |
   |  for each sweep:
   |     junction backend: start switch-junctiond on every host, wait for its control socket
   |     start the receiver; start a placement watcher on every host
   |     coordinator on sender0 drives one worker per sender host over SSH
   |     fetch results, check placement, stop receiver and daemons
   |
   |  teardown: delete everything the run created, verify it is gone
   |  report.py                                                -> "run <id> end: success|failed"
```

On each host the receiver and each generator run on `APP_CPU`. On the junction backend each host also runs its own daemon, with its network thread on `JUNCTION_CPU` and its control thread on the housekeeping CPUs; [Running on EC2](../nic/ec2.md#which-cpu-to-use) explains these CPUs. The laptop shows a few milestones (`run <id> start`, `hosts up`, `code built`), then for each sweep the coordinator's `processes up`, `processes synced`, `sweep start` and `sweep end`. A failed sweep is logged, and the next one still runs.

The command exits 0 only when `report.py` exited 0, every fetch succeeded, no sweep failed and teardown verified that nothing is left.

## What makes a sweep INVALID

A latency number from a host where something else ran on the measured CPU is not worth much, so the runner records where every thread actually ran. Before the coordinator starts, each host launches a watcher on a housekeeping CPU. It waits up to 60 seconds for the expected processes to appear and for their main threads to settle on their final CPU, then snapshots every thread's allowed CPUs and current CPU, the isolated CPU list and the interrupt counters. A second snapshot is taken near the end of the sweep.

The sweep is marked INVALID in the report when:

- a network or application main thread is not pinned to exactly its assigned CPU,
- any other thread of the daemon, the application, DPDK or the coordinator may run on an isolated CPU the backend uses (`APP_CPU`, and `JUNCTION_CPU` on the junction backend); only the application's `junction-ctl` thread (the session's helper thread, see [Connecting an application through switch-net](../junction/apps.md)) may share `APP_CPU`,
- an unrelated process may run on a used isolated CPU,
- a device interrupt fired on a used isolated CPU (local timer and rescheduling interrupts do not count),
- the watcher timed out or a snapshot is missing.

An INVALID sweep keeps its measurements in the report and plot, and the remaining sweeps still run.

## Outputs

Everything goes to `DIR/<run-id>/`, where `DIR` is `--out` (default `switch-bench/out/aws`, which git ignores):

| File | What |
|---|---|
| `report.md`, `results.json`, `<sweep>.svg` | The [report](reports.md) |
| `<sweep>.jsonl` | The coordinator's step and summary lines |
| `<sweep>.coord.log`, `<sweep>.receiver.log` | The coordinator's progress and the receiver's stderr |
| `<sweep>.<role>.placement.txt`, `...placement.end.txt`, `<sweep>.placement.json` | The placement snapshots and the check's result |
| `<sweep>.<role>.junctiond.log` | Each daemon's output and exit status (junction backend) |
| `source.json` | The commit and the SHA-256 of the source archive the hosts built |
| `host-setup.txt` | Each host's `hft-host.sh verify` output |
| `created.json` | Every resource the run created, by id |

## Cleanup

Teardown runs when the run ends normally, on an error, on Ctrl-C and on SIGTERM. Once it starts it ignores further Ctrl-C and SIGTERM. It deletes everything recorded in `created.json` and everything carrying the run's tag, then describes the resources again and logs `run <id>: nothing left`, or `NOT VERIFIED GONE` with what remains.

If a run was interrupted harder than that, delete what it left with:

```sh
python3 switch-bench/aws/switch_aws_bench.py cleanup --run-id ID [--region R] [--out DIR]
```

It uses `DIR/ID/created.json` plus a lookup by tag (only the tag lookup if the file is gone). Keep `created.json` until cleanup reports nothing left. Independently of all this, each instance powers itself off at the TTL (`--ttl`, default `2h`).

## Other commands

`kernel` runs the same matrix with the Linux kernel as the receiver: the receiver host is prepared with `hft-host.sh apply --kernel-receiver`, keeps its data ENI on the `ena` driver, and runs `switch-bench kernel-serve` pinned to `APP_CPU`, with every data-ENI interrupt pinned to that CPU too. The senders are the same switch generators as in `run`. Compare its report with a `run` report on the same shape.

```sh
python3 switch-bench/aws/switch_aws_bench.py kernel --region R
```

`test` runs native test suites on one EC2 host instead of benchmarks: an x86-64 c6in.2xlarge by default, or an arm64 c7gn.xlarge with `--shape arm`. It runs three suites, `rings`, `switch-junctiond` and `switch-net-switch-tls`, writes a `.log` and `.exit` file per suite and `tests.json`, and exits 0 only when every suite passed, every fetch succeeded and teardown verified. [Running the test suite](../testing/suite.md) lists the commands it runs.

`binance` runs the market-data feed (`deploy/run-binance.sh`) for `--seconds` (default 600) on one m7g.xlarge in a `precision-time` placement group with an Elastic IP, fetches its log and output, and tears down. [Walkthrough: a market-data feed](../protocols/binance-feed.md) explains the feed.

The runner's own tests need no AWS account: `python3 -m unittest discover -s switch-bench/aws`.

## Where to go next

The next page, [Stack-to-stack throughput with switch-memif-bench](memif.md), measures two Stacks connected with no NIC at all.
