from __future__ import annotations

from pathlib import Path

from benchlib import aws_basic, aws_lifecycle
from benchlib.models import PointResult


def point(variant: str = "mio", rate: int = 100) -> PointResult:
    path = Path("/tmp/unused")
    return PointResult(
        variant=variant,
        rate_total=rate,
        port=19_000,
        sender_procs=1,
        connections_per_sender=1,
        ok_procs=1,
        probe_connections=1,
        sent=100,
        expected_sent=100.0,
        offered_ratio=1.0,
        write_bytes=28_800,
        expected_write_bytes=28_800.0,
        write_byte_ratio=1.0,
        received=100,
        lost=0,
        achieved=100.0,
        p99_ms=1.0,
        p999_ms=2.0,
        p9999_ms=3.0,
        max_ms=4.0,
        errors=0,
        timeouts=0,
        pending_limit_errors=0,
        bad_procs=0,
        fallover=False,
        fallover_reason="",
        victim_summary_path=path,
        victim_stderr_path=path,
        sender_summary_paths=[path],
        sender_stderr_paths=[path],
    )


def aws_config(tmp: Path, sender_procs: int = 2) -> aws_basic.AwsBasicConfig:
    return aws_basic.AwsBasicConfig(
        repo_root=tmp,
        terraform_dir=tmp,
        output_dir=tmp / "out",
        run_id="abc123",
        region="us-east-1",
        availability_zone="",
        owner="test",
        instance_type="c7i.2xlarge",
        variants=("mio",),
        sender_variant="tokio",
        rates=(100,),
        sender_procs=sender_procs,
        connections_per_sender=1,
        duration_secs=1,
        payload_bytes=256,
        max_pending_bytes=64 * 1024 * 1024,
        seed=1,
        base_port=19_000,
        fallover_p999_ms=20.0,
        fallover_min_receive_ratio=0.98,
        fallover_min_offered_ratio=0.98,
        stop_on_fallover=False,
        keep_on_failure=False,
        max_runtime_minutes=10,
        rust_toolchain="nightly-2025-11-06",
    )


def aws_outputs() -> aws_basic.TerraformOutputs:
    return aws_basic.TerraformOutputs(
        region="us-east-1",
        availability_zone="us-east-1a",
        bucket="bucket",
        placement_group="pg",
        victim_instance_id="i-victim",
        victim_private_ip="10.0.0.10",
        sender_instance_id="i-sender",
        sender_private_ip="10.0.0.11",
    )


def aws_pipeline(config: aws_basic.AwsBasicConfig, outputs: aws_basic.TerraformOutputs | None = None) -> aws_lifecycle.Pipeline:
    pipeline = aws_lifecycle.Pipeline(config, 10**12)
    pipeline.outputs = outputs
    return pipeline


class FakeAwsCli:
    def __init__(self, region: str) -> None:
        self.region = region

    def run(self, *_args: object, **_kwargs: object) -> None:
        return None


class FakeProc:
    def __init__(self, name: str) -> None:
        self.name = name
        self.returncode = None
