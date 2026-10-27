# Local Benchmark Scripts

Run full local sweep and report:

```bash
uv run python scripts/run_local_sweep.py
```

Useful focused run:

```bash
uv run python scripts/run_local_sweep.py \
  --duration-secs 10 \
  --sender-variant tokio \
  --sender-procs 5 \
  --connections-per-sender 1 \
  --max-pending-bytes 67108864 \
  --seed 1 \
  --rates 10000,20000,40000,80000,120000,160000,200000
```

Receiver-only high-rate run:

```bash
uv run python scripts/run_local_sweep.py \
  --sender-variant tokio \
  --duration-secs 30 \
  --sender-procs 5 \
  --connections-per-sender 1 \
  --seed 1 \
  --rates 400000,600000,800000,1000000,1250000,1500000,1750000,2000000
```

Artifacts:

```text
results/local-sweep-*/sweep_config.json
results/local-sweep-*/summary.csv
results/local-sweep-*/summary.json
results/local-sweep-*/report.html
results/local-sweep-*/raw/<variant>/<rate>/*
```

`summary.csv` includes `expected_sent`, `offered_ratio`, `write_bytes`,
`expected_write_bytes`, `write_byte_ratio`, and `probe_connections`.
Fallover includes sender under-offer, receiver errors, loss, and tail latency.
Achieved throughput uses the active send duration, not sender drain/report time.
Artifacts are rewritten after each completed point. Sender JSON includes
`write_bytes`; `sent` is sender-queued frames. `write_byte_ratio` is the stricter
proof that bytes reached the socket. Sender binaries cap pending unsent frame
bytes, so receiver failure cannot turn into unbounded sender-side buffering. The
same `max_pending_bytes` knob caps sender in-flight bytes and receiver echo
backlog.

Regenerate report from existing raw summary:

```bash
uv run python scripts/report_local_sweep.py results/local-sweep-YYYYMMDD-HHMMSS
```

## Basic AWS Sweep

Requires local `terraform`, local `aws` CLI, and AWS credentials visible to both
(`AWS_PROFILE`, env vars, or shared config). Uses SSM, not SSH.
Set `--region` or configure `AWS_REGION`/`AWS_DEFAULT_REGION` before running.

Smoke:

```bash
uv run python scripts/run_aws_basic_sweep.py \
  --region us-east-1 \
  --variants tokio \
  --rates 10000 \
  --duration-secs 5 \
  --rust-toolchain nightly-2025-11-06 \
  --max-runtime-minutes 30
```

Network baseline:

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

Artifacts:

```text
results/aws-basic-*/aws_run.json
results/aws-basic-*/cleanup_command.txt
results/aws-basic-*/run_error.txt              # failed runs only
results/aws-basic-*/cleanup_failed.txt         # cleanup failures only
results/aws-basic-*/summary.csv                # full green runs only
results/aws-basic-*/summary.json               # full green runs only
results/aws-basic-*/report.html                # full green runs only
results/aws-basic-*/raw/<variant>/<rate>/*
```

Default cleanup always destroys Terraform resources and then verifies no
`KiotoRunId=<run>` EC2 instances remain pending/running/stopping/stopped. If
Terraform fails to destroy instances, the runner directly terminates them by tag.
Final summary/report files are published only after the sweep and cleanup both
succeed. Setup, control-plane, collection, upload, fetch, aggregate, and cleanup
failures write the failed stage to `run_error.txt`/`aws_run.json` and abort.
Remote sender command failures kill leftover sender processes before the normal
abort path.
