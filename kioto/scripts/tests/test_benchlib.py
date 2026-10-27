from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from benchlib import process, report
from benchlib.models import ProcessOutcome, SenderArtifact, SweepConfig
from helpers import FakeProc, point


class BenchlibTests(unittest.TestCase):
    def test_sender_seed_is_deterministic_and_variant_independent(self) -> None:
        self.assertEqual(process.sender_seed(1, 400_000, 3), process.sender_seed(1, 400_000, 3))
        self.assertNotEqual(process.sender_seed(1, 400_000, 3), process.sender_seed(1, 400_000, 4))

    def test_run_sweep_calls_after_point_incrementally(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = SweepConfig(
                repo_root=Path(tmp),
                output_dir=Path(tmp) / "out",
                variants=("mio", "tokio"),
                rates=(100,),
                build=False,
            )
            written_lengths: list[int] = []

            with (
                patch.object(process, "run_point", side_effect=[point("mio"), point("tokio")]),
                patch.object(process, "print_point"),
            ):
                points = process.run_sweep(config, after_point=lambda rows: written_lengths.append(len(rows)))

            self.assertEqual(len(points), 2)
            self.assertEqual(written_lengths, [1, 2])

    def test_run_point_stops_started_processes_on_sender_spawn_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = SweepConfig(
                repo_root=Path(tmp),
                output_dir=Path(tmp) / "out",
                variants=("mio",),
                rates=(100,),
                sender_procs=2,
                build=False,
            )
            created: list[FakeProc] = []
            stopped: list[FakeProc] = []

            def fake_popen(*_args: object, **_kwargs: object) -> FakeProc:
                if len(created) == 2:
                    raise OSError("spawn failed")
                proc = FakeProc(f"proc-{len(created)}")
                created.append(proc)
                return proc

            with (
                patch.object(process.subprocess, "Popen", side_effect=fake_popen),
                patch.object(process, "wait_for_listen"),
                patch.object(process, "wait_or_stop", side_effect=lambda proc, timeout: stopped.append(proc)),
            ):
                with self.assertRaises(OSError):
                    process.run_point(config, "mio", 100, 19_000)

            self.assertEqual([proc.name for proc in stopped], ["proc-1", "proc-0"])

    def test_written_bytes_are_fallover_input(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            point_dir = Path(tmp)
            sender_json = point_dir / "sender-0.json"
            sender_err = point_dir / "sender-0.err"
            config = SweepConfig(
                repo_root=Path(tmp),
                output_dir=point_dir,
                rates=(100,),
                duration_secs=1,
                payload_bytes=256,
                build=False,
            )
            artifact = SenderArtifact(
                index=0,
                outcome=ProcessOutcome(
                    returncode=0,
                    timed_out=False,
                    stdout_path=sender_json,
                    stderr_path=sender_err,
                ),
                summary={
                    "sent": 100,
                    "received": 100,
                    "lost": 0,
                    "active_elapsed_secs": 1.0,
                    "write_bytes": 1_000,
                    "connection_errors": 0,
                    "validation_errors": 0,
                    "pending_limit_errors": 0,
                    "rtt_p999_ns": 1_000_000,
                },
            )

            result = process.aggregate_point(
                config=config,
                variant="mio",
                rate=100,
                port=19_000,
                victim_summary_path=point_dir / "victim.json",
                victim_stderr_path=point_dir / "victim.err",
                senders=[artifact],
            )

            self.assertTrue(result.fallover)
            self.assertEqual(result.fallover_reason, "write byte ratio below 0.98")
            self.assertEqual(result.write_bytes, 1_000)
            self.assertEqual(result.expected_write_bytes, 28_800.0)

    def test_pending_limit_error_gets_specific_reason(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            point_dir = Path(tmp)
            sender_json = point_dir / "sender-0.json"
            sender_err = point_dir / "sender-0.err"
            config = SweepConfig(repo_root=Path(tmp), output_dir=point_dir, build=False)
            artifact = SenderArtifact(
                index=0,
                outcome=ProcessOutcome(
                    returncode=0,
                    timed_out=False,
                    stdout_path=sender_json,
                    stderr_path=sender_err,
                ),
                summary={
                    "sent": 10,
                    "received": 10,
                    "lost": 0,
                    "active_elapsed_secs": 1.0,
                    "write_bytes": 2_880,
                    "connection_errors": 0,
                    "validation_errors": 0,
                    "pending_limit_errors": 1,
                },
            )

            result = process.aggregate_point(
                config=config,
                variant="mio",
                rate=10,
                port=19_000,
                victim_summary_path=point_dir / "victim.json",
                victim_stderr_path=point_dir / "victim.err",
                senders=[artifact],
            )

            self.assertTrue(result.fallover)
            self.assertEqual(result.fallover_reason, "sender pending limit hit")
            self.assertEqual(result.pending_limit_errors, 1)

    def test_report_uses_actual_sender_shape(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = SweepConfig(
                repo_root=Path(tmp),
                output_dir=Path(tmp) / "out",
                sender_procs=7,
                connections_per_sender=3,
                build=False,
            )

            html = report.render_report(config, [point()])

            self.assertIn("Sender processes: <b>7</b>", html)
            self.assertIn("Connections per sender: <b>3</b>", html)
            self.assertIn("Max pending bytes: <b>67108864</b>", html)

    def test_report_artifacts_publish_only_after_all_files_render(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = SweepConfig(repo_root=Path(tmp), output_dir=Path(tmp) / "out", build=False)

            with patch.object(report, "write_report", side_effect=RuntimeError("render failed")):
                with self.assertRaises(RuntimeError):
                    report.write_artifacts(config, [point()])

            self.assertFalse((config.output_dir / "sweep_config.json").exists())
            self.assertFalse((config.output_dir / "summary.json").exists())
            self.assertFalse((config.output_dir / "summary.csv").exists())
            self.assertFalse((config.output_dir / "report.html").exists())


if __name__ == "__main__":
    unittest.main()
