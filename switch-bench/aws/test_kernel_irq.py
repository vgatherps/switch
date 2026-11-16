import contextlib
import io
import json
import os
import pathlib
import shlex
import sys
import tempfile
import types
import unittest
from unittest import mock

import switch_aws_bench as bench
import kernel_irq

from kernel_irq import wait_for_affinity


class AffinityConvergenceTest(unittest.TestCase):
    def test_waits_until_every_irq_moves(self):
        now = [0.0]
        def read(irq):
            return '31' if irq == '44' or now[0] >= 0.15 else '25'
        def sleep(seconds):
            now[0] += seconds

        actual = wait_for_affinity(['44', '45'], '31', read, timeout=1,
                                   clock=lambda: now[0], sleep=sleep)
        self.assertEqual(actual, {'44': '31', '45': '31'})
        self.assertGreaterEqual(now[0], 0.15)
        self.assertLess(now[0], 1)

    def test_timeout_reports_final_effective_assignments(self):
        now = [0.0]
        def read(irq):
            return '31' if irq == '44' else '25'
        def sleep(seconds):
            now[0] += seconds

        actual = wait_for_affinity(['44', '45'], '31', read, timeout=0.2,
                                   clock=lambda: now[0], sleep=sleep)
        self.assertEqual(actual, {'44': '31', '45': '25'})
        self.assertEqual(now[0], 0.2)

class AffinitySetupTest(unittest.TestCase):
    def test_traffic_allows_setup_after_affinity_moves(self):
        pending = {"affinity_error": {"target_cpu": "31", "irqs": {
            "45": {"name": "ens6-Tx-Rx-0", "requested": "31",
                   "effective": "25", "write_error": None}}}}
        moved = {"effective_affinity": {"45": "31"}}
        with tempfile.TemporaryDirectory() as run_dir:
            responses = [types.SimpleNamespace(stdout=json.dumps(value)) for value in (pending, moved)]
            receiver = types.SimpleNamespace(run_dir=run_dir, root=mock.Mock(side_effect=responses))
            activate = mock.Mock()
            result = bench.kernel_snapshot(receiver, {"DATA_PCI": "pci", "DATA_IF": "ens6", "APP_CPU": "31"},
                                           configure=True, activate=activate)
            self.assertEqual(result, moved)
            activate.assert_called_once()

    def test_traffic_must_make_requested_affinity_effective(self):
        error = {"target_cpu": "31", "irqs": {
            "45": {"name": "ens6-Tx-Rx-0", "requested": "31",
                   "effective": "25", "write_error": None}}}
        response = types.SimpleNamespace(stdout=json.dumps({"affinity_error": error}))
        with tempfile.TemporaryDirectory() as run_dir:
            receiver = types.SimpleNamespace(run_dir=run_dir, root=mock.Mock(side_effect=[response, response]))
            activate = mock.Mock()
            with self.assertRaisesRegex(RuntimeError, "affinity differs"):
                bench.kernel_snapshot(receiver, {"DATA_PCI": "pci", "DATA_IF": "ens6", "APP_CPU": "31"},
                                      configure=True, activate=activate)
            activate.assert_called_once()
            with open(os.path.join(run_dir, "kernel-irq-affinity-error.json")) as f:
                saved = json.load(f)
            self.assertEqual(saved["irqs"], error["irqs"])
            self.assertEqual(saved["initial"], error)

class RemoteAffinityTest(unittest.TestCase):
    def exercise(self, moves):
        state = {"active": False, "requested": "0", "reads": 0, "writes": 0, "rps": "0"}

        class Path:
            def __init__(self, *parts):
                self.path = "/".join(str(part).strip("/") for part in parts)
                self.name = self.path.rsplit("/", 1)[-1]

            def __truediv__(self, other):
                return Path(self.path, other)

            def iterdir(self):
                return [Path(self.path, "45")]

            def glob(self, _pattern):
                return [Path(self.path, "rx-0/rps_cpus")]

            def exists(self):
                return False

            def read_text(self):
                if self.name == "smp_affinity_list":
                    return state["requested"]
                if self.name == "effective_affinity_list":
                    if state["active"]:
                        state["reads"] += 1
                        if moves and state["reads"] >= 3:
                            return "31"
                    return "25"
                if self.name == "rps_cpus":
                    return state["rps"]
                if self.name == "interrupts":
                    return "CPU0 CPU31\n45: 0 2 PCI-MSIX ens6-Tx-Rx-0\n"
                if self.name == "softirqs":
                    return "CPU0 CPU31\n NET_RX: 0 2\n"
                raise AssertionError(self.path)

            def write_text(self, value):
                if self.name == "smp_affinity_list":
                    state["writes"] += 1
                    state["requested"] = value
                elif self.name == "rps_cpus":
                    state["rps"] = value
                else:
                    raise AssertionError(self.path)

        def wait(irqs, cpu, read):
            now = [0.0]
            def sleep(seconds):
                now[0] += seconds
            return wait_for_affinity(irqs, cpu, read, timeout=0.1, clock=lambda: now[0], sleep=sleep)

        def root(command):
            parts = shlex.split(command)
            stdout = io.StringIO()
            with (mock.patch.object(pathlib, "Path", Path), mock.patch.object(sys, "argv", ["-c", *parts[5:]]),
                  mock.patch.object(kernel_irq, "wait_for_affinity", wait), contextlib.redirect_stdout(stdout)):
                try:
                    exec(parts[4], {})
                except SystemExit as exc:
                    self.assertEqual(exc.code, 0)
            return types.SimpleNamespace(stdout=stdout.getvalue())

        def activate():
            state["active"] = True

        with tempfile.TemporaryDirectory() as run_dir:
            receiver = types.SimpleNamespace(run_dir=run_dir, root=root)
            facts = {"DATA_PCI": "pci", "DATA_IF": "ens6", "APP_CPU": "31"}
            if moves:
                result = bench.kernel_snapshot(receiver, facts, configure=True, activate=activate)
                self.assertEqual(result["effective_affinity"], {"45": "31"})
            else:
                with self.assertRaisesRegex(RuntimeError, "affinity differs"):
                    bench.kernel_snapshot(receiver, facts, configure=True, activate=activate)
                with open(os.path.join(run_dir, "kernel-irq-affinity-error.json")) as f:
                    error = json.load(f)
                self.assertEqual(error["irqs"]["45"]["effective"], "25")
                self.assertEqual(error["initial"]["irqs"]["45"]["before"]["requested"], "0")
                self.assertEqual(error["irqs"]["45"]["before"]["requested"], "31")
            self.assertEqual(state["writes"], 1)

    def test_waits_after_packets_without_rewriting(self):
        self.exercise(moves=True)

    def test_timeout_keeps_last_affinity_without_rewriting(self):
        self.exercise(moves=False)


if __name__ == '__main__':
    unittest.main()
