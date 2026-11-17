# Benchmarking on AWS

`switch-bench/aws/switch_aws_bench.py` runs a fixed set of sweeps on EC2 instances with real network cards (ENA) in one command. It creates its own network and instances, prepares each host with `deploy/hft-host.sh` (see [Running on EC2](../operations/ec2.md)), builds the code on the hosts, runs the sweeps, fetches the results, deletes everything it created and writes the [report](reports.md).

The runner creates resources you pay for. Every resource is tagged with the run's id and an expiry time, every instance powers itself off when the run's time to live (`--ttl`, default `2h`) runs out, and teardown runs however the script ends.

## What you need

The runner runs on your own machine and needs:

- Python 3 with `boto3`, and `ssh`, `scp` and `git`;
- AWS credentials allowed to manage EC2 networking, instances, key pairs and tags, and to call `ssm:GetParameter`, `sts:GetCallerIdentity` and `servicequotas:GetServiceQuota` (`python3 switch-bench/aws/switch_aws_bench.py -h` lists every permission);
- a region, given with `--region` or configured in the AWS profile boto3 uses (`aws configure`);
- free on-demand Standard vCPU quota (quota code `L-1216C47A`): 16 vCPUs for the `arm` shape, 56 for `intel`;
- a clean working tree with everything committed. The hosts build `git archive HEAD`, so uncommitted changes would not be measured.

Nothing is built on your machine.

## Running it

1. See what a run would create. This makes no AWS calls:

   ```sh
   python3 switch-bench/aws/switch_aws_bench.py plan
   ```

   It prints the hosts, the CPU each process runs on, the sweeps, and the quota the run needs.

2. Check the account without creating anything:

   ```sh
   python3 switch-bench/aws/switch_aws_bench.py preflight --region eu-west-1
   ```

   This checks the working tree, shows the AWS account it will use, finds an availability zone that offers every instance type of the shape, and checks the quota. Pass `--zone Z` to pick the zone yourself when the default one lacks capacity.

3. Run:

   ```sh
   python3 switch-bench/aws/switch_aws_bench.py run --region eu-west-1
   ```

   The runner logs each stage on a line starting `switch-aws-bench:`. Expect `run <id> start: provisioning hosts`, `hosts up: ...`, and, once every host is prepared and has built `switch-bench`, `code built: ...`. Preparing a host includes a reboot. Then each sweep runs in turn, with the coordinator's progress lines. A sweep whose coordinator exits nonzero, or whose placement check fails, is recorded as failed and the next sweep runs. Any other error ends the run there and goes straight to teardown. At the end the runner tears down, writes the report, and logs `run <id>: nothing left` and `run <id> end: success`, or `failed`.

The command exits 0 only when the report is complete, every fetch succeeded, no sweep failed and teardown verified that nothing is left.

## What it measures

A run has one receiver host and three sender hosts:

| `--shape` | Receiver | Senders | vCPUs |
|---|---|---|---|
| `arm` (default) | c7gn.xlarge | 3 × c7gn.xlarge | 16 |
| `intel` | c6in.8xlarge | 3 × c6in.2xlarge | 56 |

Each host has a second network interface as its data card. The receiver runs `switch-bench serve`, each sender host runs one generator, and the coordinator runs on the first sender host. The echo server and the generators each run on the host's isolated application CPU, `APP_CPU`.

`--backend` chooses how the echo server and the generators reach their cards: `conduit` (the default) makes each process own its card; `junction` makes each host run `switch-junctiond` on its `JUNCTION_CPU` and the processes share the card through it; `--backend conduit junction` runs every sweep both ways.

The sweeps are fixed: UDP, TCP with `--budget-us 50` and without, and WebSocket with `--budget-us 50` and without. Every sweep uses 12 connections (4 per sender host), 64-byte messages and the same 11 rates: 10k, 100k, 400k, 800k, 900k, 1M, 1.1M, 1.2M, 1.4M, 1.8M and 2.5M messages per second. Each step measures for 10 seconds after a 250 ms warmup, then drains for 200 ms. Sweep files are named `BACKEND-PROTO-n12-s64-uncapped` or `BACKEND-PROTO-n12-s64-b50`, where `BACKEND` is the `--backend` value, for example `conduit-tcp-n12-s64-b50`.

When sharing the card, the runner stops each daemon after its sweep and writes its exit status at the end of `<sweep>.<role>.switch-junctiond.log`. A nonzero status is logged but does not fail the sweep, so check those lines.

When a process owns its card, the echo server and the generators log the card's ENA allowance counters (`pps_allowance_exceeded`, `bw_in_allowance_exceeded`, `bw_out_allowance_exceeded`) and its reset count as `ena_xstats` JSON lines at start and at close, with an `invalid` list naming any reset or counter that went backwards. The report does not read these lines, so check the logs before trusting a sweep's numbers.

## INVALID sweeps

The runner checks CPU placement at the start of each sweep and near its end, and counts interrupts in between. The report marks a sweep INVALID when:

- the echo server, a generator or the daemon is not pinned to exactly its CPU;
- any other thread of those processes, or any other process, may run on a CPU the benchmark uses (the application's `junction-ctl` thread may share `APP_CPU`);
- a device interrupt fired on one of those CPUs during the sweep;
- a snapshot is missing or the watcher timed out.

An INVALID sweep keeps its measurements in the report, the report counts as incomplete, and the remaining sweeps still run.

## Outputs

Everything goes to `DIR/<run-id>/`, where `DIR` is `--out` (default `switch-bench/out/aws`, which git ignores):

| File | What |
|---|---|
| `report.md`, `results.json`, `<sweep>.svg` | The [report](reports.md) |
| `<sweep>.jsonl` | The coordinator's step and summary lines |
| `<sweep>.coord.log`, `<sweep>.receiver.log`, `sender<i>/gen-<sweep>.log` | The coordinator's progress and the stderr of the echo server and each generator |
| `<sweep>.placement.json`, `<sweep>.<role>.placement.txt`, `<sweep>.<role>.placement.end.txt` | The placement check's result and its snapshots |
| `<sweep>.<role>.switch-junctiond.log` | Each daemon's output and exit status, when sharing the card |
| `source.json` | The commit and the SHA-256 of the source archive the hosts built |
| `host-setup.txt` | Each host's `hft-host.sh verify` output and CPU assignment |
| `expected.json` | The sweeps the run meant to produce |
| `created.json` | Every resource the run created |
| `hosts.json`, `key.pem`, `known_hosts` | The hosts, and the run's SSH key and host keys |
| `fetch-errors.txt` | Results that could not be fetched, if any |

## Cleaning up

Teardown runs when the run ends, on an error, on Ctrl-C and on SIGTERM; once started it ignores further Ctrl-C and SIGTERM. It deletes everything in `created.json` and everything carrying the run's tag, checks again, and logs `run <id>: nothing left`. If something remains it logs ``run <id>: NOT VERIFIED GONE; rerun `cleanup --run-id <id>`: `` followed by what is left.

If a run was stopped harder than that, delete what it left with:

```sh
python3 switch-bench/aws/switch_aws_bench.py cleanup --run-id ID --region R
```

It uses `created.json` from the run's directory (pass `--out DIR` if it is not the default) and a lookup by tag. Keep `created.json` until cleanup reports nothing left.

## The kernel as receiver

`kernel` runs the same sweeps with `switch-bench kernel-serve` on the receiver host, with its data card on Linux's `ena` driver and every interrupt of that card pinned to `APP_CPU`. The senders are the same switch generators. Compare its report with a `run` report of the same shape:

```sh
python3 switch-bench/aws/switch_aws_bench.py kernel --region R
```

Its sweep files are named as in a `run`, after how the generators reach their cards; the report's title says the receiver was the Linux kernel.
