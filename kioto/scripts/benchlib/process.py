from __future__ import annotations

import json
import shutil
import socket
import subprocess
import time
from pathlib import Path
from typing import Any, Callable

from .models import KNOWN_VARIANTS, PointResult, ProcessOutcome, SenderArtifact, SweepConfig


PROBE_CONNECTIONS_PER_POINT = 1
FRAME_HEADER_BYTES = 32
SEED_MODULUS = 9_223_372_036_854_775_807
SEED_RATE_MULTIPLIER = 1_000_003
SEED_INDEX_MULTIPLIER = 97_003


def build_binaries(config: SweepConfig) -> None:
    if not config.build:
        return

    cmd = ["cargo", "build", "--workspace"]
    if config.release:
        cmd.append("--release")
    subprocess.run(cmd, cwd=config.repo_root, check=True)


def run_sweep(
    config: SweepConfig,
    after_point: Callable[[list[PointResult]], None] | None = None,
) -> list[PointResult]:
    validate_sweep_config(config)
    build_binaries(config)
    config.output_dir.mkdir(parents=True, exist_ok=True)

    points: list[PointResult] = []
    stopped_variants: set[str] = set()
    for rate_index, rate in enumerate(config.rates):
        for variant_index, variant in enumerate(config.variants):
            if variant in stopped_variants:
                continue
            port = config.base_port + variant_index * 1_000 + rate_index
            point = run_point(config, variant, rate, port)
            points.append(point)
            print_point(point)
            if after_point is not None:
                after_point(points)
            if config.stop_on_fallover and point.fallover:
                stopped_variants.add(variant)

    return points


def run_point(config: SweepConfig, variant: str, rate: int, port: int) -> PointResult:
    validate_variant(variant)
    if rate <= 0:
        raise ValueError("rate must be positive")
    point_dir = config.output_dir / "raw" / variant / str(rate)
    point_dir.mkdir(parents=True, exist_ok=True)
    clear_point_dir(point_dir)

    victim_stdout = point_dir / "victim.json"
    victim_stderr = point_dir / "victim.err"
    victim_duration = config.duration_secs + config.victim_extra_secs

    victim: subprocess.Popen[Any] | None = None
    senders: list[tuple[int, subprocess.Popen[Any], Path, Path]] = []
    try:
        with victim_stdout.open("w") as out, victim_stderr.open("w") as err:
            victim = subprocess.Popen(
                [
                    str(config.victim_bin(variant)),
                    "--listen",
                    f"127.0.0.1:{port}",
                    "--duration-secs",
                    str(victim_duration),
                    "--max-pending-bytes",
                    str(config.max_pending_bytes),
                ],
                cwd=config.repo_root,
                stdout=out,
                stderr=err,
            )

        wait_for_listen("127.0.0.1", port, config.listen_grace_secs)

        sender_bin = config.sender_bin(config.sender_variant)
        per_sender_rate = rate / config.sender_procs
        for index in range(config.sender_procs):
            stdout_path = point_dir / f"sender-{index}.json"
            stderr_path = point_dir / f"sender-{index}.err"
            out = stdout_path.open("w")
            err = stderr_path.open("w")
            try:
                proc = subprocess.Popen(
                    [
                        str(sender_bin),
                        "--target",
                        f"127.0.0.1:{port}",
                        "--connections",
                        str(config.connections_per_sender),
                        "--rate-total",
                        str(per_sender_rate),
                        "--duration-secs",
                        str(config.duration_secs),
                        "--payload-bytes",
                        str(config.payload_bytes),
                        "--max-pending-bytes",
                        str(config.max_pending_bytes),
                        "--seed",
                        str(sender_seed(config.seed, rate, index)),
                    ],
                    cwd=config.repo_root,
                    stdout=out,
                    stderr=err,
                )
            finally:
                out.close()
                err.close()
            senders.append((index, proc, stdout_path, stderr_path))

        sender_deadline = time.monotonic() + config.duration_secs + config.sender_timeout_extra_secs
        sender_artifacts = [
            wait_sender(index, proc, stdout_path, stderr_path, sender_deadline)
            for index, proc, stdout_path, stderr_path in senders
        ]

        wait_or_stop(victim, timeout=config.victim_extra_secs + 2.0)

        point = aggregate_point(
            config=config,
            variant=variant,
            rate=rate,
            port=port,
            victim_summary_path=victim_stdout,
            victim_stderr_path=victim_stderr,
            senders=sender_artifacts,
        )
        apply_victim_health(point, point_dir)
        return point
    except BaseException:
        stop_senders(senders)
        if victim is not None:
            stop_process_quietly(victim)
        raise


def wait_sender(
    index: int,
    proc: subprocess.Popen[Any],
    stdout_path: Path,
    stderr_path: Path,
    deadline: float,
) -> SenderArtifact:
    timed_out = False
    timeout = max(0.0, deadline - time.monotonic())
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        wait_or_stop(proc, timeout=0.0)

    summary = read_json(stdout_path)
    return SenderArtifact(
        index=index,
        outcome=ProcessOutcome(
            returncode=proc.returncode,
            timed_out=timed_out,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        ),
        summary=summary,
    )


def clear_point_dir(point_dir: Path) -> None:
    for path in point_dir.iterdir():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()


def wait_for_listen(host: str, port: int, timeout_secs: float) -> None:
    deadline = time.monotonic() + timeout_secs
    last_error: OSError | None = None
    while True:
        try:
            with socket.create_connection((host, port), timeout=0.05):
                return
        except OSError as err:
            last_error = err
        if time.monotonic() >= deadline:
            raise RuntimeError(f"victim did not listen on {host}:{port}: {last_error}")
        time.sleep(0.01)


def wait_or_stop(proc: subprocess.Popen[Any], timeout: float) -> None:
    try:
        proc.wait(timeout=timeout)
        return
    except subprocess.TimeoutExpired:
        proc.terminate()

    try:
        proc.wait(timeout=1)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=1)


def stop_senders(senders: list[tuple[int, subprocess.Popen[Any], Path, Path]]) -> None:
    for _index, proc, _stdout_path, _stderr_path in senders:
        stop_process_quietly(proc)


def stop_process_quietly(proc: subprocess.Popen[Any]) -> None:
    try:
        wait_or_stop(proc, timeout=0.0)
    except BaseException:
        pass


def aggregate_point(
    config: SweepConfig,
    variant: str,
    rate: int,
    port: int,
    victim_summary_path: Path,
    victim_stderr_path: Path,
    senders: list[SenderArtifact],
) -> PointResult:
    summaries = [sender.summary for sender in senders if sender.summary is not None]
    sent = sum(int(summary.get("sent", 0)) for summary in summaries)
    expected_sent = float(rate * config.duration_secs)
    offered_ratio = sent / expected_sent if expected_sent > 0 else 0.0
    write_bytes = sum(int(summary.get("write_bytes", 0)) for summary in summaries)
    expected_write_bytes = expected_sent * (FRAME_HEADER_BYTES + config.payload_bytes)
    write_byte_ratio = write_bytes / expected_write_bytes if expected_write_bytes > 0 else 0.0
    received = sum(int(summary.get("received", 0)) for summary in summaries)
    reported_lost = sum(int(summary.get("lost", 0)) for summary in summaries)
    lost = max(sent - received, reported_lost, 0)
    elapsed = max(
        (
            float(summary.get("active_elapsed_secs", summary.get("elapsed_secs", 0.0)))
            for summary in summaries
        ),
        default=0.0,
    )
    achieved = received / elapsed if elapsed > 0 else 0.0
    connection_errors = sum(int(summary.get("connection_errors", 0)) for summary in summaries)
    validation_errors = sum(int(summary.get("validation_errors", 0)) for summary in summaries)
    pending_limit_errors = sum(int(summary.get("pending_limit_errors", 0)) for summary in summaries)
    bad_procs = sum(1 for sender in senders if not sender.ok)
    timeouts = sum(1 for sender in senders if sender.outcome.timed_out)
    errors = connection_errors + validation_errors + pending_limit_errors + bad_procs

    p99_ms = max_tail_ms(summaries, "rtt_p99_ns")
    p999_ms = max_tail_ms(summaries, "rtt_p999_ns")
    p9999_ms = max_tail_ms(summaries, "rtt_p9999_ns")
    max_ms = max_tail_ms(summaries, "rtt_max_ns")

    fallover, reason = classify_fallover(
        config=config,
        sent=sent,
        received=received,
        lost=lost,
        errors=errors,
        timeouts=timeouts,
        pending_limit_errors=pending_limit_errors,
        offered_ratio=offered_ratio,
        write_byte_ratio=write_byte_ratio,
        p999_ms=p999_ms,
    )

    return PointResult(
        variant=variant,
        rate_total=rate,
        port=port,
        sender_procs=config.sender_procs,
        connections_per_sender=config.connections_per_sender,
        ok_procs=sum(1 for sender in senders if sender.ok),
        probe_connections=PROBE_CONNECTIONS_PER_POINT,
        sent=sent,
        expected_sent=expected_sent,
        offered_ratio=offered_ratio,
        write_bytes=write_bytes,
        expected_write_bytes=expected_write_bytes,
        write_byte_ratio=write_byte_ratio,
        received=received,
        lost=lost,
        achieved=achieved,
        p99_ms=p99_ms,
        p999_ms=p999_ms,
        p9999_ms=p9999_ms,
        max_ms=max_ms,
        errors=errors,
        timeouts=timeouts,
        pending_limit_errors=pending_limit_errors,
        bad_procs=bad_procs,
        fallover=fallover,
        fallover_reason=reason,
        victim_summary_path=victim_summary_path,
        victim_stderr_path=victim_stderr_path,
        sender_summary_paths=[sender.outcome.stdout_path for sender in senders],
        sender_stderr_paths=[sender.outcome.stderr_path for sender in senders],
    )


def classify_fallover(
    config: SweepConfig,
    sent: int,
    received: int,
    lost: int,
    errors: int,
    timeouts: int,
    pending_limit_errors: int,
    offered_ratio: float,
    write_byte_ratio: float,
    p999_ms: float | None,
) -> tuple[bool, str]:
    if timeouts:
        return True, "sender timeout"
    if pending_limit_errors:
        return True, "sender pending limit hit"
    if errors:
        return True, "process or validation error"
    if offered_ratio < config.fallover_min_offered_ratio:
        return True, f"offered ratio below {config.fallover_min_offered_ratio:g}"
    if write_byte_ratio < config.fallover_min_offered_ratio:
        return True, f"write byte ratio below {config.fallover_min_offered_ratio:g}"
    if sent > 0 and received / sent < config.fallover_min_receive_ratio:
        return True, "receive ratio below threshold"
    if lost:
        return True, "lost echoes"
    if p999_ms is not None and p999_ms >= config.fallover_p999_ms:
        return True, f"p99.9 >= {config.fallover_p999_ms:g}ms"
    return False, ""


def validate_sweep_config(config: SweepConfig) -> None:
    for variant in config.variants:
        validate_variant(variant)
    validate_variant(config.sender_variant)
    if not config.rates or any(rate <= 0 for rate in config.rates):
        raise ValueError("rates must be positive")
    if config.sender_procs <= 0:
        raise ValueError("sender_procs must be positive")
    if config.connections_per_sender <= 0:
        raise ValueError("connections_per_sender must be positive")
    if config.duration_secs <= 0:
        raise ValueError("duration_secs must be positive")
    if config.payload_bytes <= 0:
        raise ValueError("payload_bytes must be positive")
    if config.max_pending_bytes <= 0:
        raise ValueError("max_pending_bytes must be positive")
    if not (0 < config.seed < SEED_MODULUS):
        raise ValueError(f"seed must be in [1, {SEED_MODULUS})")
    if not (0.0 < config.fallover_min_offered_ratio <= 1.0):
        raise ValueError("fallover_min_offered_ratio must be in (0, 1]")
    if not (0.0 < config.fallover_min_receive_ratio <= 1.0):
        raise ValueError("fallover_min_receive_ratio must be in (0, 1]")
    last_port = config.base_port + len(config.variants) * 1_000 + len(config.rates) + 100
    if config.base_port <= 0 or last_port > 65_535:
        raise ValueError("base_port/rates/variants produce invalid TCP port range")


def validate_variant(variant: str) -> None:
    if variant not in KNOWN_VARIANTS:
        raise ValueError(f"unknown variant: {variant}")


def sender_seed(run_seed: int, rate: int, index: int) -> int:
    value = (
        run_seed
        + rate * SEED_RATE_MULTIPLIER
        + index * SEED_INDEX_MULTIPLIER
    ) % SEED_MODULUS
    return value or 1


def max_tail_ms(summaries: list[dict[str, Any]], key: str) -> float | None:
    values = [summary.get(key) for summary in summaries if summary.get(key) is not None]
    if not values:
        return None
    return max(float(value) for value in values) / 1_000_000.0

def apply_victim_health(point: PointResult, point_dir: Path) -> None:
    reason = ""
    victim_summary = read_json(point_dir / "victim.json")
    if read_int(point_dir / "victim.timeout", default=0) == 1:
        point.timeouts += 1
        reason = "victim timeout"
    elif (point_dir / "victim.missing_pid").exists():
        reason = "victim pid missing"
    elif victim_summary is None:
        reason = "victim summary missing"
    elif int(victim_summary.get("parse_errors", 0)) > 0:
        reason = "victim parse error"
    elif int(victim_summary.get("pending_limit_errors", 0)) > 0:
        point.pending_limit_errors += int(victim_summary.get("pending_limit_errors", 0))
        reason = "victim pending limit hit"
    elif int(victim_summary.get("connection_errors", 0)) > 0:
        reason = "victim connection error"

    if reason:
        point.errors += 1
        point.fallover = True
        point.fallover_reason = reason


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        text = path.read_text().strip()
    except OSError:
        return None
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def read_int(path: Path, default: int) -> int:
    try:
        return int(path.read_text().strip())
    except (OSError, ValueError):
        return default


def print_point(point: PointResult) -> None:
    p999 = "" if point.p999_ms is None else f"{point.p999_ms:.3f}ms"
    print(
        f"{point.variant:5} rate={point.rate_total:>7} "
        f"ok={point.ok_procs}/{point.sender_procs} "
        f"offer={point.offered_ratio:>5.3f} "
        f"write={point.write_byte_ratio:>5.3f} "
        f"recv={point.received:>8}/{point.sent:<8} "
        f"p99.9={p999:>10} "
        f"errors={point.errors} timeouts={point.timeouts} "
        f"pending={point.pending_limit_errors} "
        f"{point.fallover_reason}",
        flush=True,
    )
