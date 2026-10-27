#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from benchlib.models import DEFAULT_RATES, DEFAULT_VARIANTS, SweepConfig
from benchlib.process import run_sweep
from benchlib.report import write_artifacts


def main() -> None:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    output_dir = args.output_dir or repo_root / "results" / default_run_name()

    config = SweepConfig(
        repo_root=repo_root,
        output_dir=output_dir,
        variants=parse_csv(args.variants),
        sender_variant=args.sender_variant,
        rates=tuple(int(item) for item in parse_csv(args.rates)),
        sender_procs=args.sender_procs,
        connections_per_sender=args.connections_per_sender,
        duration_secs=args.duration_secs,
        payload_bytes=args.payload_bytes,
        max_pending_bytes=args.max_pending_bytes,
        seed=args.seed,
        base_port=args.base_port,
        release=not args.debug,
        build=not args.no_build,
        fallover_p999_ms=args.fallover_p999_ms,
        fallover_min_receive_ratio=args.fallover_min_receive_ratio,
        fallover_min_offered_ratio=args.fallover_min_offered_ratio,
        stop_on_fallover=args.stop_on_fallover,
    )

    points = run_sweep(config, after_point=lambda points: write_artifacts(config, points))
    write_artifacts(config, points)
    print(f"\nsummary: {config.output_dir / 'summary.csv'}")
    print(f"report:  {config.output_dir / 'report.html'}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run local Mio/Tokio benchmark sweep and generate report."
    )
    parser.add_argument("--variants", default=",".join(DEFAULT_VARIANTS))
    parser.add_argument(
        "--sender-variant",
        default="tokio",
        help="Fixed sender implementation used for all victim variants.",
    )
    parser.add_argument("--rates", default=",".join(str(rate) for rate in DEFAULT_RATES))
    parser.add_argument("--sender-procs", type=int, default=5)
    parser.add_argument("--connections-per-sender", type=int, default=1)
    parser.add_argument("--duration-secs", type=int, default=10)
    parser.add_argument("--payload-bytes", type=int, default=256)
    parser.add_argument("--max-pending-bytes", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--seed", type=int, default=1, help="Deterministic sender seed shared across victim variants.")
    parser.add_argument("--base-port", type=int, default=19_000)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--fallover-p999-ms", type=float, default=20.0)
    parser.add_argument("--fallover-min-receive-ratio", type=float, default=0.98)
    parser.add_argument("--fallover-min-offered-ratio", type=float, default=0.98)
    parser.add_argument("--stop-on-fallover", action="store_true")
    return parser.parse_args()


def parse_csv(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


def default_run_name() -> str:
    return "local-sweep-" + datetime.now().strftime("%Y%m%d-%H%M%S")


if __name__ == "__main__":
    main()
