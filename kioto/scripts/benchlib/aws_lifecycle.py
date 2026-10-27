from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable, TypeVar

from .aws_cleanup import write_cleanup_failure
from .aws_commands import run_capture
from .aws_models import AwsBasicConfig, TerraformOutputs
from .models import PointResult


T = TypeVar("T")


class Pipeline:
    def __init__(self, config: AwsBasicConfig, deadline: float) -> None:
        self.config = config
        self.deadline = deadline
        self.outputs: TerraformOutputs | None = None
        self.stage = "init"
        self.failed = False
        self.original_error: BaseException | None = None

    def enter(self, stage: str) -> None:
        self.stage = stage

    def step(self, stage: str, fn: Callable[[], T]) -> T:
        self.enter(stage)
        try:
            return fn()
        except BaseException as err:
            self.fail(stage, err)
            raise

    def fail_if_needed(self, err: BaseException) -> None:
        if not self.failed:
            self.fail(self.stage, err)

    def fail(self, stage: str, err: BaseException) -> None:
        if self.failed:
            return
        self.failed = True
        self.stage = stage
        self.original_error = err
        self.record_run_error(stage, err)

    def record_run_error(self, stage: str, err: BaseException) -> None:
        try:
            write_run_error(self.config, self.outputs, stage, err)
        except BaseException as write_error:
            print(f"failed to write AWS run error artifact: {write_error}", flush=True)

    def record_cleanup_failure(self, cleanup_error: BaseException) -> None:
        try:
            write_cleanup_failure(self.config, cleanup_error, self.original_error)
        except BaseException as write_error:
            print(f"failed to write AWS cleanup failure artifact: {write_error}", flush=True)

    def write_metadata(
        self,
        phase: str,
        stage: str | None = None,
        points: list[PointResult] | None = None,
    ) -> None:
        write_aws_metadata(self.config, self.outputs, phase=phase, stage=stage or self.stage, points=points)


def write_run_error(
    config: AwsBasicConfig,
    outputs: TerraformOutputs | None,
    stage: str,
    err: BaseException,
) -> None:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    error_text = str(err) if str(err) else repr(err)
    lines = [
        "phase: failed",
        f"stage: {stage}",
        f"error_type: {type(err).__name__}",
        "error:",
        error_text,
    ]
    (config.output_dir / "run_error.txt").write_text("\n".join(lines) + "\n")
    write_aws_metadata(config, outputs, phase="failed", stage=stage, error=err)


def write_aws_metadata(
    config: AwsBasicConfig,
    outputs: TerraformOutputs | None,
    phase: str,
    stage: str | None = None,
    error: BaseException | None = None,
    points: list[PointResult] | None = None,
) -> None:
    config_data = asdict(config)
    config_data["repo_root"] = str(config.repo_root)
    config_data["terraform_dir"] = str(config.terraform_dir)
    config_data["output_dir"] = str(config.output_dir)
    data: dict[str, object] = {
        "phase": phase,
        "written_at": datetime.now(UTC).isoformat(),
        "config": config_data,
        "terraform": asdict(outputs) if outputs is not None else None,
    }
    if stage is not None:
        data["stage"] = stage
    if error is not None:
        data["error_type"] = type(error).__name__
        data["error"] = error_summary(error)
    data["git_commit"] = git_commit(config.repo_root)
    data["git_dirty"] = git_dirty(config.repo_root)
    data["git_status_short"] = git_status_short(config.repo_root)
    if points is not None:
        data["points"] = [point.to_json_dict() for point in points]
    (config.output_dir / "aws_run.json").write_text(json.dumps(data, indent=2) + "\n")


def error_summary(err: BaseException) -> str:
    lines = str(err).splitlines()
    summary = lines[0] if lines else repr(err)
    return summary[:1_000]


def git_commit(repo_root: Path) -> str:
    try:
        return run_capture(["git", "rev-parse", "HEAD"], cwd=repo_root, timeout_secs=30).strip()
    except Exception:
        return ""


def git_status_short(repo_root: Path) -> str:
    try:
        return run_capture(["git", "status", "--short"], cwd=repo_root, timeout_secs=30)
    except Exception:
        return ""


def git_dirty(repo_root: Path) -> bool:
    return bool(git_status_short(repo_root).strip())
