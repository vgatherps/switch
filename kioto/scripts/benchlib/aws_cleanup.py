from __future__ import annotations

import shlex
import time

from .aws_commands import AwsCli, DeadlineExceeded
from .aws_infra import terraform_destroy, terraform_state_args, terraform_vars
from .aws_models import AwsBasicConfig, TerraformOutputs


def cleanup(config: AwsBasicConfig, outputs: TerraformOutputs | None, deadline: float) -> None:
    print("cleaning up AWS resources", flush=True)
    destroy_error: BaseException | None = None
    try:
        terraform_destroy(config)
    except BaseException as err:
        destroy_error = err

    region = outputs.region if outputs else config.region
    aws = AwsCli(region)
    cleanup_deadline = max(deadline, time.monotonic() + 600)
    terminate_tagged_instances(aws, config.run_id, cleanup_deadline)

    if destroy_error is not None:
        raise destroy_error


def terminate_tagged_instances(aws: AwsCli, run_id: str, deadline: float) -> None:
    ids = live_instance_ids(aws, run_id)
    if ids:
        print(f"terraform left live instances, terminating: {', '.join(ids)}", flush=True)
        aws.run(["ec2", "terminate-instances", "--instance-ids", *ids])

    end = min(deadline, time.monotonic() + 600)
    while True:
        ids = live_instance_ids(aws, run_id)
        if not ids:
            return
        if time.monotonic() >= end:
            raise DeadlineExceeded(f"instances still not terminated for run {run_id}: {ids}")
        time.sleep(10)


def live_instance_ids(aws: AwsCli, run_id: str) -> list[str]:
    response = aws.json(
        [
            "ec2",
            "describe-instances",
            "--filters",
            f"Name=tag:KiotoRunId,Values={run_id}",
            "Name=instance-state-name,Values=pending,running,stopping,stopped",
        ]
    )
    ids: list[str] = []
    for reservation in response.get("Reservations", []):
        for instance in reservation.get("Instances", []):
            ids.append(str(instance["InstanceId"]))
    return ids


def write_cleanup_command(config: AwsBasicConfig, outputs: TerraformOutputs | None) -> None:
    region_arg = config.region or "<region>"
    describe_tagged = shlex.join(
        [
            "aws",
            "--region",
            region_arg,
            "ec2",
            "describe-instances",
            "--filters",
            f"Name=tag:KiotoRunId,Values={config.run_id}",
            "Name=instance-state-name,Values=pending,running,stopping,stopped",
            "--query",
            "Reservations[].Instances[].InstanceId",
            "--output",
            "text",
        ]
    )
    terminate_tagged = shlex.join(
        [
            "aws",
            "--region",
            region_arg,
            "ec2",
            "terminate-instances",
            "--instance-ids",
        ]
    )
    lines = [
        "# Manual cleanup fallback for this run.",
        shlex.join(
            [
                "terraform",
                f"-chdir={config.terraform_dir}",
                "destroy",
                "-auto-approve",
                *terraform_state_args(config),
                *terraform_vars(config),
            ]
        ),
        "",
        "# Tag-based fallback for instances if Terraform state is incomplete:",
        f"IDS=$({describe_tagged})",
        f'if [ -n "$IDS" ]; then {terminate_tagged} $IDS; fi',
    ]
    if outputs is not None:
        lines.extend(
            [
                "",
                "# If Terraform fails, terminate benchmark instances directly:",
                shlex.join(
                    [
                        "aws",
                        "--region",
                        outputs.region,
                        "ec2",
                        "terminate-instances",
                        "--instance-ids",
                        outputs.victim_instance_id,
                        outputs.sender_instance_id,
                    ]
                ),
            ]
        )
    (config.output_dir / "cleanup_command.txt").write_text("\n".join(lines) + "\n")


def write_cleanup_failure(
    config: AwsBasicConfig,
    cleanup_error: BaseException,
    original_error: BaseException | None,
) -> None:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    lines = [
        "cleanup failed",
        f"cleanup_error={cleanup_error}",
    ]
    if original_error is not None:
        lines.append(f"original_error={original_error}")
    lines.append("")
    lines.append((config.output_dir / "cleanup_command.txt").read_text() if (config.output_dir / "cleanup_command.txt").exists() else "")
    (config.output_dir / "cleanup_failed.txt").write_text("\n".join(lines))
