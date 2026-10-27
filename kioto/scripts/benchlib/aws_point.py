from __future__ import annotations

from pathlib import Path

from .aws_commands import (
    AwsCli,
    s3_uri,
    send_ssm_command,
    wait_ssm_command,
)
from .aws_models import RUN_ROOT, AwsBasicConfig, TerraformOutputs
from .models import PointResult, ProcessOutcome, SenderArtifact, SweepConfig
from .process import (
    SEED_INDEX_MULTIPLIER,
    SEED_MODULUS,
    SEED_RATE_MULTIPLIER,
    aggregate_point,
    apply_victim_health,
    clear_point_dir,
    read_int,
    read_json,
)


def prepare_remote_point(config: AwsBasicConfig, variant: str, rate: int) -> tuple[str, Path]:
    remote_point = f"raw/{variant}/{rate}"
    local_point = config.output_dir / remote_point
    local_point.mkdir(parents=True, exist_ok=True)
    clear_point_dir(local_point)
    return remote_point, local_point


def fetch_remote_point_artifacts(
    config: AwsBasicConfig,
    aws: AwsCli,
    outputs: TerraformOutputs,
    remote_point: str,
    local_point: Path,
) -> None:
    aws.run(
        [
            "s3",
            "cp",
            s3_uri(outputs.bucket, config.s3_prefix, remote_point) + "/",
            str(local_point) + "/",
            "--recursive",
        ]
    )


def upload_remote_point_artifacts(
    config: AwsBasicConfig,
    aws: AwsCli,
    outputs: TerraformOutputs,
    remote_point: str,
    deadline: float,
) -> None:
    upload_remote_host_artifacts(config, aws, outputs.victim_instance_id, outputs.bucket, remote_point, deadline)
    upload_remote_host_artifacts(config, aws, outputs.sender_instance_id, outputs.bucket, remote_point, deadline)


def upload_remote_host_artifacts(
    config: AwsBasicConfig,
    aws: AwsCli,
    instance_id: str,
    bucket: str,
    remote_point: str,
    deadline: float,
) -> None:
    point_dir = RUN_ROOT / "results" / remote_point
    script = f"""set -euo pipefail
if [ -d {point_dir} ]; then
  aws s3 cp {point_dir}/ {s3_uri(bucket, config.s3_prefix, remote_point)}/ --recursive
fi
"""
    command_id = send_ssm_command(aws, instance_id, script, timeout_seconds=120)
    wait_ssm_command(aws, command_id, instance_id, deadline)


def start_victim(
    config: AwsBasicConfig,
    aws: AwsCli,
    outputs: TerraformOutputs,
    variant: str,
    rate: int,
    port: int,
    remote_point: str,
    deadline: float,
) -> None:
    victim_duration = config.duration_secs + config.victim_extra_secs
    point_dir = RUN_ROOT / "results" / remote_point
    script = f"""set -euo pipefail
rm -rf {point_dir}
mkdir -p {point_dir}
cd {RUN_ROOT}/source
nohup target/release/bench-victim-{variant} --listen 0.0.0.0:{port} --duration-secs {victim_duration} --max-pending-bytes {config.max_pending_bytes} > {point_dir}/victim.json 2> {point_dir}/victim.err &
echo $! > {point_dir}/victim.pid
if ! python3 - {port} {config.listen_grace_secs} <<'PY'; then
import socket
import sys
import time

port = int(sys.argv[1])
deadline = time.monotonic() + float(sys.argv[2])
last_error = None
while True:
    try:
        sock = socket.create_connection(("127.0.0.1", port), timeout=0.05)
        sock.close()
        sys.exit(0)
    except OSError as err:
        last_error = err
    if time.monotonic() >= deadline:
        print("victim did not listen on 127.0.0.1:%s: %s" % (port, last_error), file=sys.stderr)
        sys.exit(1)
    time.sleep(0.01)
PY
  cat {point_dir}/victim.err >&2 || true
  exit 1
fi
if ! kill -0 "$(cat {point_dir}/victim.pid)" 2>/dev/null; then
  cat {point_dir}/victim.err >&2 || true
  exit 1
fi
"""
    command_id = send_ssm_command(aws, outputs.victim_instance_id, script, timeout_seconds=30)
    wait_ssm_command(aws, command_id, outputs.victim_instance_id, deadline)


def run_senders(
    config: AwsBasicConfig,
    aws: AwsCli,
    outputs: TerraformOutputs,
    variant: str,
    rate: int,
    port: int,
    remote_point: str,
    deadline: float,
) -> None:
    point_dir = RUN_ROOT / "results" / remote_point
    per_sender_rate = rate / config.sender_procs
    sender_timeout = config.duration_secs + config.sender_timeout_extra_secs
    seed_expr = (
        f"(({config.seed} + {rate} * {SEED_RATE_MULTIPLIER} + "
        f"i * {SEED_INDEX_MULTIPLIER}) % {SEED_MODULUS})"
    )
    script = f"""set -uo pipefail
mkdir -p {point_dir}
rm -f {point_dir}/sender-*
cd {RUN_ROOT}/source
i=0
while [ "$i" -lt {config.sender_procs} ]; do
  seed=$(( {seed_expr} ))
  if [ "$seed" -eq 0 ]; then seed=1; fi
  setsid sh -c '
    i="$1"
    seed="$2"
    timeout {sender_timeout}s target/release/bench-sender-{config.sender_variant} --target {outputs.victim_private_ip}:{port} --connections {config.connections_per_sender} --rate-total {per_sender_rate:.6f} --duration-secs {config.duration_secs} --payload-bytes {config.payload_bytes} --max-pending-bytes {config.max_pending_bytes} --seed "$seed" > {point_dir}/sender-${{i}}.json 2> {point_dir}/sender-${{i}}.err
    rc=$?
    echo "$rc" > {point_dir}/sender-${{i}}.rc
    if [ "$rc" -eq 124 ]; then echo 1 > {point_dir}/sender-${{i}}.timeout; else echo 0 > {point_dir}/sender-${{i}}.timeout; fi
  ' sh "$i" "$seed" &
  echo $! > {point_dir}/sender-${{i}}.pid
  i=$((i + 1))
done
i=0
while [ "$i" -lt {config.sender_procs} ]; do
  pid=$(cat {point_dir}/sender-${{i}}.pid)
  wait "$pid" || true
  if [ ! -f {point_dir}/sender-${{i}}.rc ]; then
    echo 127 > {point_dir}/sender-${{i}}.rc
    echo 0 > {point_dir}/sender-${{i}}.timeout
  fi
  i=$((i + 1))
done
exit 0
"""
    command_id = send_ssm_command(aws, outputs.sender_instance_id, script, timeout_seconds=sender_timeout + 120)
    wait_ssm_command(aws, command_id, outputs.sender_instance_id, deadline)


def stop_remote_senders(
    config: AwsBasicConfig,
    aws: AwsCli,
    outputs: TerraformOutputs,
    remote_point: str,
    deadline: float,
) -> None:
    point_dir = RUN_ROOT / "results" / remote_point
    script = f"""set -uo pipefail
if [ -d {point_dir} ]; then
  for pid_file in {point_dir}/sender-*.pid; do
    [ -e "$pid_file" ] || continue
    pid=$(cat "$pid_file" 2>/dev/null || true)
    if [ -n "$pid" ]; then
      kill -TERM -- "-$pid" 2>/dev/null || kill "$pid" 2>/dev/null || true
    fi
  done
  sleep 1
  for pid_file in {point_dir}/sender-*.pid; do
    [ -e "$pid_file" ] || continue
    pid=$(cat "$pid_file" 2>/dev/null || true)
    if [ -n "$pid" ]; then
      kill -KILL -- "-$pid" 2>/dev/null || kill -9 "$pid" 2>/dev/null || true
    fi
  done
fi
exit 0
"""
    command_id = send_ssm_command(aws, outputs.sender_instance_id, script, timeout_seconds=30)
    wait_ssm_command(aws, command_id, outputs.sender_instance_id, deadline)


def collect_victim(
    config: AwsBasicConfig,
    aws: AwsCli,
    outputs: TerraformOutputs,
    variant: str,
    rate: int,
    remote_point: str,
    deadline: float,
) -> None:
    point_dir = RUN_ROOT / "results" / remote_point
    collect_timeout = config.victim_extra_secs + 10
    script = f"""set -uo pipefail
mkdir -p {point_dir}
pid=$(cat {point_dir}/victim.pid 2>/dev/null || true)
deadline=$((SECONDS + {collect_timeout}))
if [ -n "$pid" ]; then
  while kill -0 "$pid" 2>/dev/null && [ "$SECONDS" -lt "$deadline" ]; do
    sleep 1
  done
  if kill -0 "$pid" 2>/dev/null; then
    echo 1 > {point_dir}/victim.timeout
    kill "$pid" 2>/dev/null || true
    sleep 1
    kill -9 "$pid" 2>/dev/null || true
  else
    echo 0 > {point_dir}/victim.timeout
  fi
else
  echo 1 > {point_dir}/victim.missing_pid
fi
exit 0
"""
    command_id = send_ssm_command(aws, outputs.victim_instance_id, script, timeout_seconds=collect_timeout + 60)
    wait_ssm_command(aws, command_id, outputs.victim_instance_id, deadline)


def aggregate_remote_point(
    config: SweepConfig,
    variant: str,
    rate: int,
    port: int,
    point_dir: Path,
) -> PointResult:
    senders: list[SenderArtifact] = []
    for index in range(config.sender_procs):
        stdout_path = point_dir / f"sender-{index}.json"
        stderr_path = point_dir / f"sender-{index}.err"
        returncode = read_int(point_dir / f"sender-{index}.rc", default=127)
        timed_out = read_int(point_dir / f"sender-{index}.timeout", default=0) == 1
        senders.append(
            SenderArtifact(
                index=index,
                outcome=ProcessOutcome(
                    returncode=returncode,
                    timed_out=timed_out,
                    stdout_path=stdout_path,
                    stderr_path=stderr_path,
                ),
                summary=read_json(stdout_path),
            )
        )

    point = aggregate_point(
        config=config,
        variant=variant,
        rate=rate,
        port=port,
        victim_summary_path=point_dir / "victim.json",
        victim_stderr_path=point_dir / "victim.err",
        senders=senders,
    )
    apply_victim_health(point, point_dir)
    return point


def write_control_error(
    local_point: Path,
    phase: str,
    err: Exception,
    cleanup_errors: list[Exception],
) -> None:
    lines = [format_point_error(phase, err)]
    for cleanup_err in cleanup_errors:
        lines.append(format_point_error("cleanup after control failure", cleanup_err))
    (local_point / "control_error.txt").write_text("\n".join(lines) + "\n")


def format_point_error(phase: str, err: Exception) -> str:
    first_line = str(err).splitlines()[0] if str(err).splitlines() else repr(err)
    text = f"aws control error during {phase}: {type(err).__name__}: {first_line}"
    return text[:240]
