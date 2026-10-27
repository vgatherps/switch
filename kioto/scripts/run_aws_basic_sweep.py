#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from benchlib.aws_basic import (
    AwsBasicConfig,
    default_owner,
    require_tool,
    resolve_region,
    run_aws_basic_sweep,
)
from benchlib.models import DEFAULT_VARIANTS


DEFAULT_AWS_RATES = (100_000, 200_000, 400_000, 600_000, 800_000, 1_000_000)


def main() -> None:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    run_id = args.run_id or new_run_id()
    output_dir = (args.output_dir or repo_root / "results" / f"aws-basic-{run_id}").resolve()
    terraform_dir = (args.terraform_dir or repo_root / "infra" / "aws" / "basic").resolve()

    require_tool("terraform")
    require_tool("aws")

    region = resolve_region(args.region)

    config = AwsBasicConfig(
        repo_root=repo_root,
        terraform_dir=terraform_dir,
        output_dir=output_dir,
        run_id=run_id,
        region=region,
        availability_zone=args.availability_zone,
        owner=args.owner,
        instance_type=args.instance_type,
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
        fallover_p999_ms=args.fallover_p999_ms,
        fallover_min_receive_ratio=args.fallover_min_receive_ratio,
        fallover_min_offered_ratio=args.fallover_min_offered_ratio,
        stop_on_fallover=args.stop_on_fallover,
        keep_on_failure=args.keep_on_failure,
        max_runtime_minutes=args.max_runtime_minutes,
        rust_toolchain=args.rust_toolchain,
    )

    points = run_aws_basic_sweep(config)
    print(f"\nsummary: {config.output_dir / 'summary.csv'}")
    print(f"report:  {config.output_dir / 'report.html'}")
    print(f"points:  {len(points)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Spin up two AWS instances, run basic network benchmark sweep, collect results, and tear down."
    )
    parser.add_argument("--region", default="", help="AWS region. Empty uses AWS_REGION/AWS_DEFAULT_REGION/aws config.")
    parser.add_argument("--availability-zone", default="", help="AZ for both instances. Empty uses first available AZ.")
    parser.add_argument("--instance-type", default="c7i.2xlarge")
    parser.add_argument("--owner", default=default_owner())
    parser.add_argument("--run-id", default="")
    parser.add_argument("--terraform-dir", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--variants", default=",".join(DEFAULT_VARIANTS))
    parser.add_argument("--sender-variant", default="tokio")
    parser.add_argument("--rates", default=",".join(str(rate) for rate in DEFAULT_AWS_RATES))
    parser.add_argument("--sender-procs", type=int, default=5)
    parser.add_argument("--connections-per-sender", type=int, default=1)
    parser.add_argument("--duration-secs", type=int, default=30)
    parser.add_argument("--payload-bytes", type=int, default=256)
    parser.add_argument("--max-pending-bytes", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--seed", type=int, default=1, help="Deterministic sender seed shared across victim variants.")
    parser.add_argument("--base-port", type=int, default=19_000)
    parser.add_argument("--fallover-p999-ms", type=float, default=20.0)
    parser.add_argument("--fallover-min-receive-ratio", type=float, default=0.98)
    parser.add_argument("--fallover-min-offered-ratio", type=float, default=0.98)
    parser.add_argument("--stop-on-fallover", action="store_true")
    parser.add_argument("--max-runtime-minutes", type=int, default=90)
    parser.add_argument("--rust-toolchain", default="nightly-2025-11-06")
    parser.add_argument(
        "--keep-on-failure",
        action="store_true",
        help="Dangerous: leave AWS resources up after failure for debugging. Default always destroys.",
    )
    return parser.parse_args()


def parse_csv(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


def new_run_id() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S") + "-" + uuid4().hex[:10]


if __name__ == "__main__":
    main()
