#!/usr/bin/env python3
"""Tests of report.py: python3 bench/test_report.py"""
import json
import os
import tempfile
import unittest
import xml.etree.ElementTree as ET

import report


def step(pps, p99, drop=0.0, good=True, bound=False, window=(0, 0), lag=1000):
    return {"kind": "step", "proto": "udp", "offered_pps": pps, "drop_fraction": drop, "good": good,
            "generator_bound": bound, "window_unix_ns": list(window),
            "latency_ns": {"p50": p99 // 2, "p90": p99, "p99": p99, "p999": p99 * 2, "max": p99 * 3, "mean": 1.0},
            "per_gen": [{"send_lag_ns": {"p99": lag}, "owed_at_end": 3, "ns_per_round": 900, "tasks_per_round": 2.0},
                        {"send_lag_ns": {"p99": lag * 2}, "ns_per_round": 1250, "tasks_per_round": 9.5}]}


def sample(t, echoed, dropped):
    return {"unix_ns": t, "echoed": echoed, "rx_dropped_full": dropped, "send_errors": 0,
            "turns": {"rx_received": echoed}}


class ReportTest(unittest.TestCase):
    def test_dut_delta_brackets_the_window(self):
        samples = [sample(100, 0, 0), sample(200, 10, 1), sample(300, 50, 4), sample(400, 60, 4)]
        self.assertEqual(report.dut_delta(samples, (250, 350))["echoed"], 50)
        self.assertEqual(report.dut_delta(samples, (200, 300))["rx_dropped_full"], 3)
        self.assertIsNone(report.dut_delta(samples, (50, 150)))
        self.assertIsNone(report.dut_delta(samples, (350, 450)))

    def test_round_cost_is_the_slowest_worker_and_absent_without_data(self):
        self.assertEqual(report.round_cost(step(1000, 10_000)), "1.25 (9.5)")
        self.assertEqual(report.round_cost({"per_gen": [{"send_lag_ns": None}]}), "-")

    def test_svg_is_well_formed_and_marks_the_knee(self):
        svg = report.svg_plot([step(1000, 10_000), step(2000, 12_000), step(4000, 90_000, drop=0.01)], 2000, "u<d>p")
        root = ET.fromstring(svg)
        self.assertEqual(len(root.findall("{http://www.w3.org/2000/svg}polyline")), 5)
        self.assertIn("knee 2000 pps", svg)
        ET.fromstring(report.svg_plot([], None, "empty"))
        ET.fromstring(report.svg_plot([step(5000, 10_000)], None, "one point"))

    def test_build_writes_report_results_and_plots(self):
        with tempfile.TemporaryDirectory() as d:
            lines = [step(1000, 10_000, window=(150, 250)), step(2000, 90_000, good=False, bound=True),
                     {"kind": "summary", "proto": "udp", "gens": 2, "senders": 4, "size": 64, "knee_pps": 1000,
                      "stop": "knee", "last_good_pps": 1000, "steps": 2}]
            with open(os.path.join(d, "udp-zhida.jsonl"), "w") as f:
                f.write("\n".join(json.dumps(l) for l in lines))
            with open(os.path.join(d, "dut-counters.jsonl"), "w") as f:
                f.write("\n".join(json.dumps({"kind": "sample", "sample": s}) for s in [sample(100, 0, 0), sample(300, 99, 2)]))
            for mode in ("on", "off"):
                with open(os.path.join(d, f"counters-{mode}.jsonl"), "w") as f:
                    f.write(json.dumps(step(5000, 20_000)))
            with open(os.path.join(d, "pingpong-zhida.jsonl"), "w") as f:
                f.write(json.dumps({"kind": "pingpong", "proto": "tcp", "size": 64, "count": 10,
                                    "rtt_ns": {"p50": 20_000, "p99": 30_000, "p999": 40_000, "max": 50_000}}))
            with open(os.path.join(d, "kernel-sender-udp-s64.jsonl"), "w") as f:
                f.write(json.dumps({"kind": "kernel_sender", "proto": "udp", "size": 64, "rate_pps": 10000,
                                    "p50_us": 20.0, "p99_us": 30.5, "p99.9_us": 40.0, "max_us": 50.0, "dropped": 0}))
            md = "\n".join(report.build(d, "T"))
            self.assertIn("| udp | 64 | 10000 | 20.0 | 30.5 | 40.0 | 50.0 | 0 |", md)
            self.assertIn("| zhida | 64 | 10 | 20.0 | 30.0 | 40.0 | 50.0 |", md)
            self.assertIn("| udp-zhida | udp | 2 | 4 | 64 | 1000 | knee | 1000 | 2 |", md)
            self.assertIn("| 1000 | 5.0 | 10.0 | 20.0 | 30.0 | 0.000 | yes | no | 2.0 | 3 | 99 | 2 | 1.25 (9.5) |", md)
            self.assertIn("YES", md)
            self.assertIn("## DUT counter capture check", md)
            self.assertTrue(os.path.exists(os.path.join(d, "udp-zhida.svg")))
            with open(os.path.join(d, "results.json")) as f:
                results = json.load(f)
            self.assertEqual(results["sweeps"][0]["summary"]["knee_pps"], 1000)


if __name__ == "__main__":
    unittest.main()
