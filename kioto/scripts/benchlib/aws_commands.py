from __future__ import annotations

import getpass
import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any


TERMINAL_SSM_STATUSES = {"Success", "Cancelled", "TimedOut", "Failed", "Cancelling"}
DEFAULT_COMMAND_TIMEOUT_SECS = 300


class CommandError(RuntimeError):
    pass


class RemoteCommandError(CommandError):
    pass


class DeadlineExceeded(RuntimeError):
    pass


class AwsCli:
    def __init__(self, region: str) -> None:
        self.region = region

    def args(self, service_args: list[str]) -> list[str]:
        args = ["aws"]
        if self.region:
            args.extend(["--region", self.region])
        args.extend(service_args)
        return args

    def json(
        self,
        service_args: list[str],
        cwd: Path | None = None,
        timeout_secs: int = DEFAULT_COMMAND_TIMEOUT_SECS,
    ) -> dict[str, Any]:
        text = run_capture(
            self.args(service_args + ["--output", "json"]),
            cwd=cwd,
            timeout_secs=timeout_secs,
        )
        if not text.strip():
            return {}
        return json.loads(text)

    def run(
        self,
        service_args: list[str],
        cwd: Path | None = None,
        timeout_secs: int = DEFAULT_COMMAND_TIMEOUT_SECS,
    ) -> None:
        run_stream(self.args(service_args), cwd=cwd, timeout_secs=timeout_secs)


def send_ssm_command(
    aws: AwsCli,
    instance_id: str,
    script: str,
    timeout_seconds: int,
) -> str:
    response = aws.json(
        [
            "ssm",
            "send-command",
            "--document-name",
            "AWS-RunShellScript",
            "--instance-ids",
            instance_id,
            "--parameters",
            json.dumps({"commands": [script]}),
            "--timeout-seconds",
            str(timeout_seconds),
        ]
    )
    return str(response["Command"]["CommandId"])


def wait_ssm_command(
    aws: AwsCli,
    command_id: str,
    instance_id: str,
    deadline: float,
) -> dict[str, Any]:
    while True:
        check_deadline(deadline)
        try:
            invocation = aws.json(
                [
                    "ssm",
                    "get-command-invocation",
                    "--command-id",
                    command_id,
                    "--instance-id",
                    instance_id,
                ]
            )
        except CommandError as err:
            if "InvocationDoesNotExist" not in str(err):
                raise
            time.sleep(2)
            continue

        status = str(invocation.get("Status", ""))
        if status in TERMINAL_SSM_STATUSES:
            if status != "Success":
                stdout = invocation.get("StandardOutputContent", "")
                stderr = invocation.get("StandardErrorContent", "")
                raise RemoteCommandError(f"SSM command {command_id} ended {status}\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}")
            return invocation
        time.sleep(2)


def wait_for_ssm(aws: AwsCli, instance_id: str, deadline: float) -> None:
    while True:
        check_deadline(deadline)
        response = aws.json(
            [
                "ssm",
                "describe-instance-information",
                "--filters",
                f"Key=InstanceIds,Values={instance_id}",
            ]
        )
        items = response.get("InstanceInformationList", [])
        if items and items[0].get("PingStatus") == "Online":
            return
        time.sleep(5)


def s3_uri(bucket: str, prefix: str, key: str) -> str:
    return f"s3://{bucket}/{prefix}/{key}".rstrip("/")


def check_deadline(deadline: float) -> None:
    if time.monotonic() >= deadline:
        raise DeadlineExceeded("AWS benchmark max runtime exceeded")


def remaining_timeout(deadline: float) -> int:
    return max(1, int(deadline - time.monotonic()))


def run_capture(
    cmd: list[str],
    cwd: Path | None = None,
    timeout_secs: int | None = DEFAULT_COMMAND_TIMEOUT_SECS,
) -> str:
    try:
        proc = subprocess.run(cmd, cwd=cwd, text=True, capture_output=True, timeout=timeout_secs)
    except FileNotFoundError as err:
        raise CommandError(f"missing command: {cmd[0]}") from err
    except subprocess.TimeoutExpired as err:
        raise CommandError(f"command timed out after {timeout_secs}s: {' '.join(cmd)}") from err
    if proc.returncode != 0:
        raise CommandError(
            f"command failed ({proc.returncode}): {' '.join(cmd)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )
    return proc.stdout


def run_stream(
    cmd: list[str],
    cwd: Path | None = None,
    check: bool = True,
    timeout_secs: int | None = DEFAULT_COMMAND_TIMEOUT_SECS,
) -> int:
    try:
        proc = subprocess.run(cmd, cwd=cwd, timeout=timeout_secs)
    except FileNotFoundError as err:
        raise CommandError(f"missing command: {cmd[0]}") from err
    except subprocess.TimeoutExpired as err:
        raise CommandError(f"command timed out after {timeout_secs}s: {' '.join(cmd)}") from err
    if check and proc.returncode != 0:
        raise CommandError(f"command failed ({proc.returncode}): {' '.join(cmd)}")
    return proc.returncode


def default_owner() -> str:
    return os.environ.get("USER") or getpass.getuser() or "unknown"


def resolve_region(region: str) -> str:
    if region.strip():
        return region.strip()
    for env_name in ("AWS_REGION", "AWS_DEFAULT_REGION"):
        value = os.environ.get(env_name, "").strip()
        if value:
            return value
    try:
        configured = run_capture(["aws", "configure", "get", "region"], timeout_secs=30).strip()
    except CommandError:
        configured = ""
    if configured:
        return configured
    raise CommandError("missing AWS region: pass --region or configure AWS_REGION/AWS_DEFAULT_REGION")


def require_tool(name: str) -> None:
    if shutil.which(name) is None:
        raise CommandError(f"missing required tool: {name}")
