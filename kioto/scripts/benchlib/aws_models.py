from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .models import KNOWN_VARIANTS, SweepConfig
from .process import SEED_MODULUS


RUN_ROOT = Path("/opt/kioto-run")
RUN_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{2,46}$")
INSTANCE_SIZE_VCPUS = {
    "large": 2,
    "xlarge": 4,
}


@dataclass(frozen=True)
class AwsBasicConfig:
    repo_root: Path
    terraform_dir: Path
    output_dir: Path
    run_id: str
    region: str
    availability_zone: str
    owner: str
    instance_type: str
    variants: tuple[str, ...]
    sender_variant: str
    rates: tuple[int, ...]
    sender_procs: int
    connections_per_sender: int
    duration_secs: int
    payload_bytes: int
    max_pending_bytes: int
    seed: int
    base_port: int
    fallover_p999_ms: float
    fallover_min_receive_ratio: float
    fallover_min_offered_ratio: float
    stop_on_fallover: bool
    keep_on_failure: bool
    max_runtime_minutes: int
    rust_toolchain: str
    listen_grace_secs: float = 0.5
    victim_extra_secs: int = 5
    sender_timeout_extra_secs: int = 8

    @property
    def s3_prefix(self) -> str:
        return f"runs/{self.run_id}"

    def sweep_config(self) -> SweepConfig:
        return SweepConfig(
            repo_root=self.repo_root,
            output_dir=self.output_dir,
            variants=self.variants,
            sender_variant=self.sender_variant,
            rates=self.rates,
            sender_procs=self.sender_procs,
            connections_per_sender=self.connections_per_sender,
            duration_secs=self.duration_secs,
            payload_bytes=self.payload_bytes,
            max_pending_bytes=self.max_pending_bytes,
            seed=self.seed,
            base_port=self.base_port,
            release=True,
            build=False,
            listen_grace_secs=self.listen_grace_secs,
            victim_extra_secs=self.victim_extra_secs,
            sender_timeout_extra_secs=self.sender_timeout_extra_secs,
            fallover_p999_ms=self.fallover_p999_ms,
            fallover_min_receive_ratio=self.fallover_min_receive_ratio,
            fallover_min_offered_ratio=self.fallover_min_offered_ratio,
            stop_on_fallover=self.stop_on_fallover,
            report_title="AWS Basic Runtime Sweep Report",
        )


@dataclass(frozen=True)
class TerraformOutputs:
    region: str
    availability_zone: str
    bucket: str
    placement_group: str
    victim_instance_id: str
    victim_private_ip: str
    sender_instance_id: str
    sender_private_ip: str

    @classmethod
    def from_terraform_json(cls, data: dict[str, Any]) -> TerraformOutputs:
        def value(name: str) -> str:
            return str(data[name]["value"])

        return cls(
            region=value("region"),
            availability_zone=value("availability_zone"),
            bucket=value("bucket"),
            placement_group=value("placement_group"),
            victim_instance_id=value("victim_instance_id"),
            victim_private_ip=value("victim_private_ip"),
            sender_instance_id=value("sender_instance_id"),
            sender_private_ip=value("sender_private_ip"),
        )


def validate_config(config: AwsBasicConfig) -> None:
    if RUN_ID_RE.fullmatch(config.run_id) is None:
        raise ValueError("run_id must be 3-47 chars: lowercase letters, digits, and hyphen")
    unknown_variants = set(config.variants) - KNOWN_VARIANTS
    if unknown_variants:
        raise ValueError(f"unknown victim variants: {sorted(unknown_variants)}")
    if config.sender_variant not in KNOWN_VARIANTS:
        raise ValueError(f"unknown sender variant: {config.sender_variant}")
    if not config.rates or any(rate <= 0 for rate in config.rates):
        raise ValueError("rates must be positive")
    if config.sender_procs <= 0:
        raise ValueError("sender_procs must be positive")
    if config.connections_per_sender <= 0:
        raise ValueError("connections_per_sender must be positive")
    if config.duration_secs <= 0:
        raise ValueError("duration_secs must be positive")
    if config.max_pending_bytes <= 0:
        raise ValueError("max_pending_bytes must be positive")
    if not (0 < config.seed < SEED_MODULUS):
        raise ValueError(f"seed must be in [1, {SEED_MODULUS})")
    if config.max_runtime_minutes <= 0:
        raise ValueError("max_runtime_minutes must be positive")
    if not (0.0 < config.fallover_min_offered_ratio <= 1.0):
        raise ValueError("fallover_min_offered_ratio must be in (0, 1]")
    if not (0.0 < config.fallover_min_receive_ratio <= 1.0):
        raise ValueError("fallover_min_receive_ratio must be in (0, 1]")
    if not re.fullmatch(r"[A-Za-z0-9._+-]+", config.rust_toolchain):
        raise ValueError("rust_toolchain contains unsupported characters")
    vcpus = instance_vcpus(config.instance_type)
    min_sender_vcpus = config.sender_procs + 1
    if vcpus is not None and vcpus < min_sender_vcpus:
        raise ValueError(
            f"{config.instance_type} has {vcpus} vCPU; need at least {min_sender_vcpus} for {config.sender_procs} sender processes"
        )
    last_port = config.base_port + len(config.variants) * 1_000 + len(config.rates) + 100
    if config.base_port <= 0 or last_port > 65_535:
        raise ValueError("base_port/rates/variants produce invalid TCP port range")


def instance_vcpus(instance_type: str) -> int | None:
    size = instance_type.split(".")[-1]
    if size in INSTANCE_SIZE_VCPUS:
        return INSTANCE_SIZE_VCPUS[size]
    match = re.fullmatch(r"(\d+)xlarge", size)
    if match:
        return int(match.group(1)) * 4
    return None
