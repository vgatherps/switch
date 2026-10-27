#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import fields
from pathlib import Path

from benchlib.models import PointResult, SweepConfig
from benchlib.report import write_report, write_summary_csv


def main() -> None:
    args = parse_args()
    config = load_config(args.run_dir / "sweep_config.json")
    points = load_points(args.run_dir / "summary.json")
    write_summary_csv(args.run_dir / "summary.csv", points)
    write_report(args.run_dir / "report.html", config, points)
    print(f"report: {args.run_dir / 'report.html'}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Regenerate local sweep report artifacts.")
    parser.add_argument("run_dir", type=Path)
    return parser.parse_args()


def load_config(path: Path) -> SweepConfig:
    data = json.loads(path.read_text())
    data["repo_root"] = Path(data["repo_root"])
    data["output_dir"] = Path(data["output_dir"])
    data["variants"] = tuple(data["variants"])
    data["rates"] = tuple(data["rates"])
    return SweepConfig(**data)


def load_points(path: Path) -> list[PointResult]:
    raw_points = json.loads(path.read_text())
    allowed = {field.name for field in fields(PointResult)}
    points = []
    for raw in raw_points:
        data = {key: value for key, value in raw.items() if key in allowed}
        data.setdefault("probe_connections", 0)
        data.setdefault("expected_sent", float(data.get("sent", 0)))
        expected_sent = float(data["expected_sent"])
        data.setdefault(
            "offered_ratio",
            float(data["sent"]) / expected_sent if expected_sent > 0 else 0.0,
        )
        data.setdefault("write_bytes", 0)
        data.setdefault("expected_write_bytes", 0.0)
        expected_write_bytes = float(data["expected_write_bytes"])
        data.setdefault(
            "write_byte_ratio",
            float(data["write_bytes"]) / expected_write_bytes if expected_write_bytes > 0 else 0.0,
        )
        data["victim_summary_path"] = Path(data["victim_summary_path"])
        data["victim_stderr_path"] = Path(data["victim_stderr_path"])
        data["sender_summary_paths"] = [Path(item) for item in data["sender_summary_paths"]]
        data["sender_stderr_paths"] = [Path(item) for item in data["sender_stderr_paths"]]
        points.append(PointResult(**data))
    return points


if __name__ == "__main__":
    main()
