from __future__ import annotations

from pathlib import Path

from .aws_commands import AwsCli, DeadlineExceeded, RemoteCommandError, check_deadline
from .aws_lifecycle import Pipeline
from .aws_models import AwsBasicConfig, TerraformOutputs
from .aws_point import (
    aggregate_remote_point,
    collect_victim,
    fetch_remote_point_artifacts,
    prepare_remote_point,
    run_senders,
    start_victim,
    stop_remote_senders,
    upload_remote_point_artifacts,
    write_control_error,
)
from .models import PointResult, SweepConfig
from .process import print_point


def run_sweep_points(pipeline: Pipeline, aws: AwsCli, outputs: TerraformOutputs) -> list[PointResult]:
    config = pipeline.config
    sweep_config = config.sweep_config()
    points: list[PointResult] = []
    stopped_variants: set[str] = set()

    for rate_index, rate in enumerate(config.rates):
        for variant_index, variant in enumerate(config.variants):
            if variant in stopped_variants:
                continue
            label = point_label(variant, rate)
            pipeline.step(f"{label}: check deadline", lambda: check_deadline(pipeline.deadline))
            port = config.base_port + variant_index * 1_000 + rate_index
            pipeline.enter(label)
            point = run_remote_point_steps(
                pipeline=pipeline,
                sweep_config=sweep_config,
                aws=aws,
                outputs=outputs,
                variant=variant,
                rate=rate,
                port=port,
            )
            points.append(point)
            print_point(point)
            if config.stop_on_fallover and point.fallover:
                stopped_variants.add(variant)

    return points


def run_remote_point_steps(
    pipeline: Pipeline,
    sweep_config: SweepConfig,
    aws: AwsCli,
    outputs: TerraformOutputs,
    variant: str,
    rate: int,
    port: int,
) -> PointResult:
    config = pipeline.config
    label = point_label(variant, rate)
    remote_point, local_point = pipeline.step(
        f"{label}: prepare local artifacts",
        lambda: prepare_remote_point(config, variant, rate),
    )

    try:
        pipeline.step(
            f"{label}: start victim",
            lambda: start_victim(config, aws, outputs, variant, rate, port, remote_point, pipeline.deadline),
        )
        pipeline.step(
            f"{label}: run senders",
            lambda: run_senders(config, aws, outputs, variant, rate, port, remote_point, pipeline.deadline),
        )
    except DeadlineExceeded:
        raise
    except RemoteCommandError as err:
        handle_load_phase_control_failure(config, aws, outputs, remote_point, local_point, pipeline, err)
        raise

    pipeline.step(
        f"{label}: collect victim",
        lambda: collect_victim(config, aws, outputs, variant, rate, remote_point, pipeline.deadline),
    )
    pipeline.step(
        f"{label}: upload artifacts",
        lambda: upload_remote_point_artifacts(config, aws, outputs, remote_point, pipeline.deadline),
    )
    pipeline.step(
        f"{label}: fetch artifacts",
        lambda: fetch_remote_point_artifacts(config, aws, outputs, remote_point, local_point),
    )
    return pipeline.step(
        f"{label}: aggregate",
        lambda: aggregate_remote_point(sweep_config, variant, rate, port, local_point),
    )


def handle_load_phase_control_failure(
    config: AwsBasicConfig,
    aws: AwsCli,
    outputs: TerraformOutputs,
    remote_point: str,
    local_point: Path,
    pipeline: Pipeline,
    err: RemoteCommandError,
) -> None:
    failed_stage = pipeline.stage
    cleanup_errors = stop_remote_senders_recording_errors(config, aws, outputs, remote_point, pipeline.deadline)
    try:
        write_control_error(local_point, failed_stage, err, cleanup_errors)
    except Exception as write_error:
        print(f"failed to write point control error artifact: {write_error}", flush=True)


def stop_remote_senders_recording_errors(
    config: AwsBasicConfig,
    aws: AwsCli,
    outputs: TerraformOutputs,
    remote_point: str,
    deadline: float,
) -> list[Exception]:
    try:
        stop_remote_senders(config, aws, outputs, remote_point, deadline)
    except Exception as err:
        return [err]
    return []


def point_label(variant: str, rate: int) -> str:
    return f"point {variant}@{rate}"
