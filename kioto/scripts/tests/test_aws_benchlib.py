from __future__ import annotations

import json
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from benchlib import aws_basic, aws_cleanup, aws_models, aws_point, aws_point_runner, aws_runner
from benchlib.models import DEFAULT_VARIANTS, KNOWN_VARIANTS
from helpers import FakeAwsCli, aws_config, aws_outputs, aws_pipeline, point


class AwsBenchlibTests(unittest.TestCase):
    def test_variant_source_is_shared(self) -> None:
        self.assertEqual(KNOWN_VARIANTS, frozenset(DEFAULT_VARIANTS))
        self.assertEqual(aws_basic.KNOWN_VARIANTS, KNOWN_VARIANTS)

    def test_aws_basic_facade_keeps_public_imports(self) -> None:
        self.assertIs(aws_basic.AwsBasicConfig, aws_models.AwsBasicConfig)
        self.assertIs(aws_basic.run_aws_basic_sweep, aws_runner.run_aws_basic_sweep)

    def test_aws_remote_command_error_aborts_and_records_control_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = aws_config(Path(tmp))
            outputs = aws_outputs()
            pipeline = aws_pipeline(config, outputs)

            with (
                patch.object(aws_point_runner, "start_victim"),
                patch.object(aws_point_runner, "run_senders", side_effect=aws_basic.RemoteCommandError("sender host failed")),
                patch.object(aws_point_runner, "stop_remote_senders") as stop_senders,
                patch.object(aws_point_runner, "collect_victim"),
                patch.object(aws_point_runner, "upload_remote_point_artifacts"),
                patch.object(aws_point_runner, "fetch_remote_point_artifacts"),
                patch.object(aws_point_runner, "aggregate_remote_point") as aggregate_point,
            ):
                with self.assertRaises(aws_basic.RemoteCommandError):
                    aws_point_runner.run_remote_point_steps(
                        pipeline=pipeline,
                        sweep_config=config.sweep_config(),
                        aws=object(),
                        outputs=outputs,
                        variant="mio",
                        rate=100,
                        port=19_000,
                    )

            stop_senders.assert_called_once()
            control_error = config.output_dir / "raw" / "mio" / "100" / "control_error.txt"
            self.assertTrue(control_error.exists())
            self.assertIn("aws control error during point mio@100: run senders", control_error.read_text())
            metadata = json.loads((config.output_dir / "aws_run.json").read_text())
            self.assertEqual(metadata["stage"], "point mio@100: run senders")
            aggregate_point.assert_not_called()

    def test_sender_cleanup_error_does_not_replace_original_control_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = aws_config(Path(tmp))
            outputs = aws_outputs()
            pipeline = aws_pipeline(config, outputs)
            original = aws_basic.RemoteCommandError("sender host failed")

            with (
                patch.object(aws_point_runner, "start_victim"),
                patch.object(aws_point_runner, "run_senders", side_effect=original),
                patch.object(
                    aws_point_runner,
                    "stop_remote_senders",
                    side_effect=aws_basic.DeadlineExceeded("cleanup timed out"),
                ),
            ):
                with self.assertRaises(aws_basic.RemoteCommandError) as raised:
                    aws_point_runner.run_remote_point_steps(
                        pipeline=pipeline,
                        sweep_config=config.sweep_config(),
                        aws=object(),
                        outputs=outputs,
                        variant="mio",
                        rate=100,
                        port=19_000,
                    )

            self.assertIs(raised.exception, original)
            control_error = (config.output_dir / "raw" / "mio" / "100" / "control_error.txt").read_text()
            self.assertIn("sender host failed", control_error)
            self.assertIn("cleanup timed out", control_error)

    def test_aws_cleanup_destroys_then_terminates_tagged_instances(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = aws_config(Path(tmp))
            outputs = aws_outputs()
            calls: list[str] = []

            with (
                patch.object(aws_cleanup, "terraform_destroy", side_effect=lambda _config: calls.append("destroy")),
                patch.object(
                    aws_cleanup,
                    "terminate_tagged_instances",
                    side_effect=lambda _aws, _run_id, _deadline: calls.append("terminate"),
                ),
            ):
                aws_cleanup.cleanup(config, outputs, 10**12)

            self.assertEqual(calls, ["destroy", "terminate"])

    def test_aws_runner_cleans_up_on_setup_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = aws_config(Path(tmp))
            cleaned: list[bool] = []

            with (
                patch.object(aws_runner, "write_cleanup_command"),
                patch.object(aws_runner, "terraform_apply", side_effect=aws_basic.CommandError("apply failed")),
                patch.object(aws_runner, "cleanup", side_effect=lambda _config, _outputs, _deadline: cleaned.append(True)),
            ):
                with self.assertRaises(aws_basic.CommandError):
                    aws_runner.run_aws_basic_sweep(config)

            self.assertEqual(cleaned, [True])
            run_error = (config.output_dir / "run_error.txt").read_text()
            self.assertIn("stage: terraform apply", run_error)
            metadata = json.loads((config.output_dir / "aws_run.json").read_text())
            self.assertEqual(metadata["phase"], "failed")
            self.assertEqual(metadata["stage"], "terraform apply")
            self.assertIsNone(metadata["terraform"])

    def test_aws_runner_writes_final_report_after_cleanup_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = aws_config(Path(tmp))
            outputs = aws_outputs()
            bundle = Path(tmp) / "source.tar.gz"
            bundle.write_text("bundle")
            events: list[str] = []

            with (
                patch.object(aws_runner, "install_signal_cleanup_handler"),
                patch.object(aws_runner, "terraform_apply"),
                patch.object(aws_runner, "terraform_outputs", return_value=outputs),
                patch.object(aws_runner, "AwsCli", FakeAwsCli),
                patch.object(aws_runner, "wait_for_ssm"),
                patch.object(aws_runner, "create_source_bundle", return_value=bundle),
                patch.object(aws_runner, "setup_remote_host"),
                patch.object(aws_runner, "run_sweep_points", return_value=[point("mio", 100)]),
                patch.object(aws_runner, "cleanup", side_effect=lambda _config, _outputs, _deadline: events.append("cleanup")),
                patch.object(
                    aws_runner,
                    "write_artifacts",
                    side_effect=lambda _config, rows: events.append(f"report:{len(rows)}"),
                ),
            ):
                points = aws_runner.run_aws_basic_sweep(config)

            self.assertEqual(len(points), 1)
            self.assertEqual(events, ["cleanup", "report:1"])
            self.assertFalse((config.output_dir / "run_error.txt").exists())
            metadata = json.loads((config.output_dir / "aws_run.json").read_text())
            self.assertEqual(metadata["phase"], "complete")
            self.assertEqual(metadata["stage"], "final report")
            self.assertEqual(len(metadata["points"]), 1)

    def test_aws_runner_failure_does_not_write_partial_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = replace(aws_config(Path(tmp)), rates=(100, 200))
            outputs = aws_outputs()
            bundle = Path(tmp) / "source.tar.gz"
            bundle.write_text("bundle")
            config.output_dir.mkdir(parents=True)
            (config.output_dir / "summary.csv").write_text("stale\n")
            (config.output_dir / "report.html").write_text("stale\n")
            cleaned: list[bool] = []

            def fail_point(pipeline: object, *_args: object, **_kwargs: object) -> list[object]:
                pipeline.enter("point mio@200")
                raise aws_basic.CommandError("remote failed")

            with (
                patch.object(aws_runner, "install_signal_cleanup_handler"),
                patch.object(aws_runner, "terraform_apply"),
                patch.object(aws_runner, "terraform_outputs", return_value=outputs),
                patch.object(aws_runner, "AwsCli", FakeAwsCli),
                patch.object(aws_runner, "wait_for_ssm"),
                patch.object(aws_runner, "create_source_bundle", return_value=bundle),
                patch.object(aws_runner, "setup_remote_host"),
                patch.object(aws_runner, "run_sweep_points", side_effect=fail_point),
                patch.object(aws_runner, "cleanup", side_effect=lambda _config, _outputs, _deadline: cleaned.append(True)),
                patch.object(aws_runner, "write_artifacts") as write_artifacts,
            ):
                with self.assertRaises(aws_basic.CommandError):
                    aws_runner.run_aws_basic_sweep(config)

            write_artifacts.assert_not_called()
            self.assertFalse((config.output_dir / "summary.csv").exists())
            self.assertFalse((config.output_dir / "report.html").exists())
            self.assertEqual(cleaned, [True])
            run_error = (config.output_dir / "run_error.txt").read_text()
            self.assertIn("stage: point mio@200", run_error)
            metadata = json.loads((config.output_dir / "aws_run.json").read_text())
            self.assertEqual(metadata["phase"], "failed")
            self.assertEqual(metadata["stage"], "point mio@200")
            self.assertNotIn("points", metadata)

    def test_aws_runner_cleanup_failure_blocks_complete_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = aws_config(Path(tmp))
            outputs = aws_outputs()
            bundle = Path(tmp) / "source.tar.gz"
            bundle.write_text("bundle")

            with (
                patch.object(aws_runner, "install_signal_cleanup_handler"),
                patch.object(aws_runner, "terraform_apply"),
                patch.object(aws_runner, "terraform_outputs", return_value=outputs),
                patch.object(aws_runner, "AwsCli", FakeAwsCli),
                patch.object(aws_runner, "wait_for_ssm"),
                patch.object(aws_runner, "create_source_bundle", return_value=bundle),
                patch.object(aws_runner, "setup_remote_host"),
                patch.object(aws_runner, "run_sweep_points", return_value=[point("mio", 100)]),
                patch.object(aws_runner, "cleanup", side_effect=aws_basic.CommandError("destroy failed")),
                patch.object(aws_runner, "write_artifacts") as write_artifacts,
            ):
                with self.assertRaises(aws_basic.CommandError):
                    aws_runner.run_aws_basic_sweep(config)

            write_artifacts.assert_not_called()
            self.assertTrue((config.output_dir / "cleanup_failed.txt").exists())
            run_error = (config.output_dir / "run_error.txt").read_text()
            self.assertIn("stage: cleanup", run_error)
            metadata = json.loads((config.output_dir / "aws_run.json").read_text())
            self.assertEqual(metadata["phase"], "failed")
            self.assertEqual(metadata["stage"], "cleanup")

    def test_aws_control_error_aborts_instead_of_becoming_point(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = aws_config(Path(tmp))
            outputs = aws_outputs()
            pipeline = aws_pipeline(config, outputs)

            with (
                patch.object(aws_point_runner, "start_victim", side_effect=aws_basic.CommandError("aws cli failed")),
                patch.object(aws_point_runner, "collect_victim"),
                patch.object(aws_point_runner, "upload_remote_point_artifacts"),
                patch.object(aws_point_runner, "fetch_remote_point_artifacts"),
            ):
                with self.assertRaises(aws_basic.CommandError):
                    aws_point_runner.run_remote_point_steps(
                        pipeline=pipeline,
                        sweep_config=config.sweep_config(),
                        aws=object(),
                        outputs=outputs,
                        variant="mio",
                        rate=100,
                        port=19_000,
                    )
            metadata = json.loads((config.output_dir / "aws_run.json").read_text())
            self.assertEqual(metadata["stage"], "point mio@100: start victim")

    def test_aws_artifact_fetch_error_aborts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = aws_config(Path(tmp))
            outputs = aws_outputs()
            pipeline = aws_pipeline(config, outputs)

            with (
                patch.object(aws_point_runner, "start_victim"),
                patch.object(aws_point_runner, "run_senders"),
                patch.object(aws_point_runner, "collect_victim"),
                patch.object(aws_point_runner, "upload_remote_point_artifacts"),
                patch.object(aws_point_runner, "fetch_remote_point_artifacts", side_effect=aws_basic.CommandError("s3 failed")),
                patch.object(aws_point_runner, "aggregate_remote_point") as aggregate_point,
            ):
                with self.assertRaises(aws_basic.CommandError):
                    aws_point_runner.run_remote_point_steps(
                        pipeline=pipeline,
                        sweep_config=config.sweep_config(),
                        aws=object(),
                        outputs=outputs,
                        variant="mio",
                        rate=100,
                        port=19_000,
                    )
            aggregate_point.assert_not_called()
            metadata = json.loads((config.output_dir / "aws_run.json").read_text())
            self.assertEqual(metadata["stage"], "point mio@100: fetch artifacts")

    def test_aws_artifact_upload_error_aborts_not_point_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = aws_config(Path(tmp))
            outputs = aws_outputs()
            pipeline = aws_pipeline(config, outputs)

            with (
                patch.object(aws_point_runner, "start_victim"),
                patch.object(aws_point_runner, "run_senders"),
                patch.object(aws_point_runner, "collect_victim"),
                patch.object(
                    aws_point_runner,
                    "upload_remote_point_artifacts",
                    side_effect=aws_basic.RemoteCommandError("s3 upload failed"),
                ),
                patch.object(aws_point_runner, "fetch_remote_point_artifacts") as fetch_artifacts,
            ):
                with self.assertRaises(aws_basic.RemoteCommandError):
                    aws_point_runner.run_remote_point_steps(
                        pipeline=pipeline,
                        sweep_config=config.sweep_config(),
                        aws=object(),
                        outputs=outputs,
                        variant="mio",
                        rate=100,
                        port=19_000,
                    )

            fetch_artifacts.assert_not_called()
            metadata = json.loads((config.output_dir / "aws_run.json").read_text())
            self.assertEqual(metadata["stage"], "point mio@100: upload artifacts")

    def test_aws_remote_point_clears_stale_local_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = aws_config(Path(tmp), sender_procs=1)
            outputs = aws_outputs()
            pipeline = aws_pipeline(config, outputs)
            point_dir = config.output_dir / "raw" / "mio" / "100"
            point_dir.mkdir(parents=True)
            (point_dir / "sender-0.rc").write_text("0\n")
            (point_dir / "sender-0.timeout").write_text("0\n")
            (point_dir / "sender-0.json").write_text(
                '{"sent":999,"received":999,"write_bytes":287712,"active_elapsed_secs":1.0}\n'
            )

            with (
                patch.object(aws_point_runner, "start_victim"),
                patch.object(aws_point_runner, "run_senders"),
                patch.object(aws_point_runner, "collect_victim"),
                patch.object(aws_point_runner, "upload_remote_point_artifacts"),
                patch.object(aws_point_runner, "fetch_remote_point_artifacts"),
            ):
                result = aws_point_runner.run_remote_point_steps(
                    pipeline=pipeline,
                    sweep_config=config.sweep_config(),
                    aws=object(),
                    outputs=outputs,
                    variant="mio",
                    rate=100,
                    port=19_000,
                )

            self.assertEqual(result.sent, 0)
            self.assertEqual(result.write_bytes, 0)
            self.assertEqual(result.bad_procs, 1)
            self.assertTrue(result.fallover)

    def test_remote_sender_scripts_use_process_groups(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = aws_config(Path(tmp))
            outputs = aws_outputs()
            scripts: list[str] = []

            def fake_send(_aws: object, _instance_id: str, script: str, timeout_seconds: int) -> str:
                scripts.append(script)
                return f"cmd-{len(scripts)}"

            with (
                patch.object(aws_point, "send_ssm_command", side_effect=fake_send),
                patch.object(aws_point, "wait_ssm_command"),
            ):
                aws_point.run_senders(config, object(), outputs, "mio", 100, 19_000, "raw/mio/100", 10**12)
                aws_point.stop_remote_senders(config, object(), outputs, "raw/mio/100", 10**12)

            self.assertIn("setsid sh -c", scripts[0])
            self.assertIn("echo $! >", scripts[0])
            self.assertIn('kill -TERM -- "-$pid"', scripts[1])
            self.assertIn('kill -KILL -- "-$pid"', scripts[1])


if __name__ == "__main__":
    unittest.main()
