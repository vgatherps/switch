import json
import os
import tempfile
import types
import unittest
from unittest import mock

import zhida_aws_bench as bench

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
                self.assertEqual(json.load(f), error)


if __name__ == '__main__':
    unittest.main()
