# Kioto

Local runtime benchmark for framed TCP stream echo under load.

The first benchmark is intentionally narrow:

```text
5 independent sender processes
1 TCP connection per sender process
fixed Tokio sender for all receiver comparisons
deterministic sender seed shared across receiver variants
victim parses framed stream and echoes frames
sender records RTT percentiles
```

The sender is fixed on purpose. It should be boring load generation, not part of
the runtime comparison. Receiver variants are the experimental surface.

## Binaries

```text
bench-victim-mio       Mio receiver
bench-victim-tokio     Tokio current-thread receiver
bench-sender-tokio     canonical sender/load generator
bench-sender-mio       development/control sender, not canonical comparison sender
```

Tokio binaries use `Builder::new_current_thread()` and the workspace disables
Tokio default features, so `rt-multi-thread` is not enabled.

## Local Sweep

Run the receiver-only localhost sweep:

```bash
uv run python scripts/run_local_sweep.py \
  --sender-variant tokio \
  --duration-secs 30 \
  --sender-procs 5 \
  --connections-per-sender 1 \
  --max-pending-bytes 67108864 \
  --seed 1 \
  --rates 400000,600000,800000,1000000,1250000,1500000,1750000,2000000
```

Artifacts:

```text
results/local-sweep-*/summary.csv
results/local-sweep-*/summary.json
results/local-sweep-*/report.html
results/local-sweep-*/raw/<victim>/<rate>/*
```

`summary.csv` includes `expected_sent`, `offered_ratio`, `write_bytes`,
`expected_write_bytes`, `write_byte_ratio`, and `probe_connections`.
A point falls over if the sender under-offers the requested load, so a saturated
sender cannot make a receiver look clean. Achieved throughput uses the active
send duration, not sender drain/report time.
Sender JSON also reports `write_bytes`; `sent` means frames queued by the sender,
not proof every byte reached the socket before a later connection failure.
`write_byte_ratio` is the stricter proof that bytes actually reached the socket.
Sender binaries cap pending unsent frame bytes, so a failed receiver becomes a
failed point instead of unbounded sender-side buffering. The same
`max_pending_bytes` knob caps sender in-flight bytes and receiver echo backlog.
Summary/report artifacts are rewritten after each completed point, so a later
failed point or interrupted sweep does not lose prior data.

## Current Localhost Finding

Latest 30s localhost sweep, fixed Tokio sender, 5 sender processes, 256-byte
payloads, no core pinning, no host tuning:

```text
rate      Mio p99.9   Tokio p99.9
400k      0.240ms     0.257ms
600k      0.281ms     0.288ms
800k      0.336ms     0.350ms
1.0M      0.374ms     0.413ms
1.25M     0.347ms     0.385ms
1.5M      0.313ms     1.677ms
1.75M     0.322ms     1.728ms
2.0M      0.753ms     6.056ms
```

No loss, errors, or timeouts through 2M msg/s for either receiver.

Interpretation:

```text
Tokio receiver overhead is not the first-order problem in this synthetic localhost benchmark.
The Tokio-vs-Mio receiver gap only starts to show clearly around 1.5M-2M msg/s.
Do not aggressively patch/replace Tokio before AWS, NIC, host tuning, and protocol realism show Tokio is the bottleneck.
```

Caveats:

```text
localhost only
no core pinning
no IRQ/NIC/network path
no TLS/WebSocket/application parser
tail percentiles are max-of-sender summaries, not merged raw histograms
achieved throughput excludes sender drain/report time
victim connection counters include one listen-probe connection per point
```

This benchmark is useful as a guardrail: if a future optimization cannot beat
plain current-thread Tokio here or on AWS, it is probably not worth complexity.

## Basic AWS Sweep

First network automation path:

```bash
uv run python scripts/run_aws_basic_sweep.py \
  --region us-east-1 \
  --instance-type c7i.2xlarge \
  --duration-secs 30 \
  --sender-procs 5 \
  --connections-per-sender 1 \
  --max-pending-bytes 67108864 \
  --seed 1 \
  --rust-toolchain nightly-2025-11-06 \
  --rates 100000,200000,400000,600000,800000,1000000
```

Cheap smoke:

```bash
uv run python scripts/run_aws_basic_sweep.py \
  --region us-east-1 \
  --instance-type c7i.2xlarge \
  --variants tokio \
  --rates 10000 \
  --duration-secs 5 \
  --rust-toolchain nightly-2025-11-06 \
  --max-runtime-minutes 30
```

Shape:

```text
Terraform creates one victim EC2 instance and one sender EC2 instance.
Both land in one AZ and one cluster placement group.
SSM runs setup/build/benchmark commands; no SSH key is required.
Source and raw result files move through a run-scoped S3 bucket.
Local Terraform and AWS CLI use the same configured AWS credentials.
The sender remains fixed Tokio; only receiver variant changes.
Raw point artifacts are kept under `raw/<variant>/<rate>/`.
Final summary/report files publish only after the sweep and cleanup both succeed.
```

Cleanup is part of the runner:

```text
normal completion -> terraform destroy
Python exception/Ctrl-C/SIGTERM -> terraform destroy in finally
Terraform destroy failure -> direct EC2 terminate by KiotoRunId tag
post-cleanup verification -> fail if tagged live/stopped instances remain
```

Each run writes `cleanup_command.txt` with manual fallback commands. The only
intentional escape hatch is `--keep-on-failure`, which leaves resources up for
debugging and should be used rarely.
Setup, control-plane, collection, upload, fetch, aggregate, and cleanup failures
write the failed stage to `run_error.txt`/`aws_run.json` and abort. Remote sender
command failures kill any leftover sender processes before the normal abort path.
