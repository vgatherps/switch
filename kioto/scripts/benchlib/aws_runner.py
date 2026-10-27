from __future__ import annotations

import signal
import time

from .aws_cleanup import cleanup, write_cleanup_command
from .aws_commands import AwsCli, s3_uri, wait_for_ssm
from .aws_infra import create_source_bundle, setup_remote_host, terraform_apply, terraform_outputs
from .aws_lifecycle import Pipeline
from .aws_models import AwsBasicConfig, TerraformOutputs, validate_config
from .aws_point_runner import run_sweep_points
from .models import PointResult
from .report import clear_artifacts, write_artifacts


def install_signal_cleanup_handler() -> None:
    def raise_interrupt(signum: int, _frame: object) -> None:
        raise KeyboardInterrupt(f"received signal {signum}")

    signal.signal(signal.SIGTERM, raise_interrupt)
    if hasattr(signal, "SIGHUP"):
        signal.signal(signal.SIGHUP, raise_interrupt)


def run_aws_basic_sweep(config: AwsBasicConfig) -> list[PointResult]:
    validate_config(config)
    install_signal_cleanup_handler()
    config.output_dir.mkdir(parents=True, exist_ok=True)

    pipeline = Pipeline(config, time.monotonic() + config.max_runtime_minutes * 60)
    points: list[PointResult] = []

    clear_artifacts(config.output_dir)
    write_cleanup_command(config, None)
    try:
        pipeline.step("terraform apply", lambda: terraform_apply(config, pipeline.deadline))
        outputs = pipeline.step("terraform outputs", lambda: terraform_outputs(config))
        pipeline.outputs = outputs
        pipeline.step("write cleanup command", lambda: write_cleanup_command(config, outputs))
        pipeline.step("write infra metadata", lambda: pipeline.write_metadata("infra-ready"))

        aws = AwsCli(outputs.region)
        pipeline.step("wait SSM victim", lambda: wait_for_ssm(aws, outputs.victim_instance_id, pipeline.deadline))
        pipeline.step("wait SSM sender", lambda: wait_for_ssm(aws, outputs.sender_instance_id, pipeline.deadline))
        pipeline.step("upload source bundle", lambda: upload_source_bundle(config, outputs, aws))
        pipeline.step(
            "setup victim",
            lambda: setup_remote_host(config, aws, outputs.victim_instance_id, outputs.bucket, "victim", pipeline.deadline),
        )
        pipeline.step(
            "setup sender",
            lambda: setup_remote_host(config, aws, outputs.sender_instance_id, outputs.bucket, "sender", pipeline.deadline),
        )
        pipeline.step("write running metadata", lambda: pipeline.write_metadata("running", stage="benchmark sweep"))

        points = run_sweep_points(pipeline, aws, outputs)
    except BaseException as err:
        pipeline.fail_if_needed(err)
        raise
    finally:
        cleanup_after_run(pipeline)

    pipeline.step("final report", lambda: publish_final_artifacts(pipeline, points))
    return points


def upload_source_bundle(config: AwsBasicConfig, outputs: TerraformOutputs, aws: AwsCli) -> None:
    bundle = create_source_bundle(config.repo_root, config.run_id)
    try:
        aws.run(["s3", "cp", str(bundle), s3_uri(outputs.bucket, config.s3_prefix, "source.tar.gz")])
    finally:
        bundle.unlink(missing_ok=True)


def cleanup_after_run(pipeline: Pipeline) -> None:
    if pipeline.failed and pipeline.config.keep_on_failure:
        print("keeping AWS resources because --keep-on-failure was set", flush=True)
        return

    try:
        pipeline.enter("cleanup")
        cleanup(pipeline.config, pipeline.outputs, pipeline.deadline)
    except BaseException as cleanup_error:
        pipeline.record_cleanup_failure(cleanup_error)
        if pipeline.failed:
            print(f"cleanup failed after original error: {cleanup_error}", flush=True)
            if pipeline.original_error is not None:
                print(f"original error: {pipeline.original_error}", flush=True)
            return
        pipeline.fail("cleanup", cleanup_error)
        raise


def publish_final_artifacts(pipeline: Pipeline, points: list[PointResult]) -> None:
    write_artifacts(pipeline.config.sweep_config(), points)
    pipeline.write_metadata("complete", stage="final report", points=points)
