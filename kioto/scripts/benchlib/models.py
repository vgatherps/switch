from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


DEFAULT_RATES = (10_000, 20_000, 40_000, 80_000, 120_000, 160_000, 200_000)
DEFAULT_VARIANTS = ("mio", "tokio")
DEFAULT_MAX_PENDING_BYTES = 64 * 1024 * 1024
KNOWN_VARIANTS = frozenset(DEFAULT_VARIANTS)
# Kioto's packages inside the repository-wide cargo workspace (Cargo.toml at the parent of repo_root).
CARGO_PACKAGES = (
    "bench-core",
    "bench-sender-mio",
    "bench-sender-tokio",
    "bench-victim-mio",
    "bench-victim-tokio",
)
CARGO_PACKAGE_ARGS = tuple(arg for package in CARGO_PACKAGES for arg in ("-p", package))


@dataclass(frozen=True)
class SweepConfig:
    repo_root: Path
    output_dir: Path
    variants: tuple[str, ...] = DEFAULT_VARIANTS
    sender_variant: str = "tokio"
    rates: tuple[int, ...] = DEFAULT_RATES
    sender_procs: int = 5
    connections_per_sender: int = 1
    duration_secs: int = 10
    payload_bytes: int = 256
    max_pending_bytes: int = DEFAULT_MAX_PENDING_BYTES
    seed: int = 1
    base_port: int = 19_000
    release: bool = True
    build: bool = True
    listen_grace_secs: float = 0.20
    victim_extra_secs: int = 4
    sender_timeout_extra_secs: int = 6
    fallover_p999_ms: float = 20.0
    fallover_min_receive_ratio: float = 0.98
    fallover_min_offered_ratio: float = 0.98
    stop_on_fallover: bool = False
    report_title: str = "Local Runtime Sweep Report"

    def to_json_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["repo_root"] = str(self.repo_root)
        data["output_dir"] = str(self.output_dir)
        return data

    @property
    def profile_dir(self) -> str:
        return "release" if self.release else "debug"

    @property
    def workspace_root(self) -> Path:
        return self.repo_root.parent

    def victim_bin(self, variant: str) -> Path:
        return self.workspace_root / "target" / self.profile_dir / f"bench-victim-{variant}"

    def sender_bin(self, variant: str) -> Path:
        return self.workspace_root / "target" / self.profile_dir / f"bench-sender-{variant}"


@dataclass(frozen=True)
class ProcessOutcome:
    returncode: int | None
    timed_out: bool
    stdout_path: Path
    stderr_path: Path

    @property
    def ok(self) -> bool:
        return self.returncode == 0 and not self.timed_out


@dataclass
class SenderArtifact:
    index: int
    outcome: ProcessOutcome
    summary: dict[str, Any] | None

    @property
    def ok(self) -> bool:
        return self.outcome.ok and self.summary is not None


@dataclass
class PointResult:
    variant: str
    rate_total: int
    port: int
    sender_procs: int
    connections_per_sender: int
    ok_procs: int
    probe_connections: int
    sent: int
    expected_sent: float
    offered_ratio: float
    write_bytes: int
    expected_write_bytes: float
    write_byte_ratio: float
    received: int
    lost: int
    achieved: float
    p99_ms: float | None
    p999_ms: float | None
    p9999_ms: float | None
    max_ms: float | None
    errors: int
    timeouts: int
    pending_limit_errors: int
    bad_procs: int
    fallover: bool
    fallover_reason: str
    victim_summary_path: Path
    victim_stderr_path: Path
    sender_summary_paths: list[Path]
    sender_stderr_paths: list[Path]

    def to_json_dict(self) -> dict[str, Any]:
        data = asdict(self)
        for key in (
            "victim_summary_path",
            "victim_stderr_path",
            "sender_summary_paths",
            "sender_stderr_paths",
        ):
            value = data[key]
            if isinstance(value, list):
                data[key] = [str(item) for item in value]
            else:
                data[key] = str(value)
        return data

    def csv_row(self) -> list[Any]:
        return [
            self.variant,
            self.rate_total,
            self.sender_procs,
            self.connections_per_sender,
            self.ok_procs,
            self.probe_connections,
            self.sent,
            f"{self.expected_sent:.0f}",
            f"{self.offered_ratio:.3f}",
            self.write_bytes,
            f"{self.expected_write_bytes:.0f}",
            f"{self.write_byte_ratio:.3f}",
            self.received,
            self.lost,
            f"{self.achieved:.0f}",
            fmt_float(self.p99_ms),
            fmt_float(self.p999_ms),
            fmt_float(self.p9999_ms),
            fmt_float(self.max_ms),
            self.errors,
            self.timeouts,
            self.pending_limit_errors,
            self.bad_procs,
            int(self.fallover),
            self.fallover_reason,
        ]


CSV_HEADER = [
    "victim_variant",
    "rate_total",
    "sender_procs",
    "connections_per_sender",
    "ok_procs",
    "probe_connections",
    "sent",
    "expected_sent",
    "offered_ratio",
    "write_bytes",
    "expected_write_bytes",
    "write_byte_ratio",
    "received",
    "lost",
    "achieved",
    "p99_ms",
    "p999_ms",
    "p9999_ms",
    "max_ms",
    "errors",
    "timeouts",
    "pending_limit_errors",
    "bad_procs",
    "fallover",
    "fallover_reason",
]


def fmt_float(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.3f}"
