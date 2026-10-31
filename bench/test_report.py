#!/usr/bin/env python3
"""Tests of report.py: python3 bench/test_report.py"""
import json
import os
import tempfile
import unittest
import xml.etree.ElementTree as ET

import report

SVG = "{http://www.w3.org/2000/svg}"


def step(pps, p99, drop=0.0, good=True, bound=False, window=(0, 0), lag=1000, n=0):
    return {"kind": "step", "proto": "udp", "step": n, "offered_pps": pps, "sent_pps": pps * 0.99,
            "received_pps": pps * 0.98, "drop_fraction": drop, "unsent": 0, "good": good, "generator_bound": bound,
            "budget_bound": False, "window_unix_ns": list(window),
            "latency_ns": None if p99 is None else
            {"p50": p99 // 2, "p90": p99, "p99": p99, "p999": p99 * 2, "max": p99 * 3, "mean": 1.0},
            "per_gen": [{"send_lag_ns": {"p99": lag}, "owed_at_end": 3, "ns_per_round": 900, "tasks_per_round": 2.0},
                        {"send_lag_ns": {"p99": lag * 2}, "ns_per_round": 1250, "tasks_per_round": 9.5}]}


def summary(knee, stop="knee", steps=2, complete=True, lower_bound=False, error=None):
    return {"kind": "summary", "proto": "udp", "gens": 2, "senders": 4, "size": 64, "knee_pps": knee,
            "knee_lower_bound": lower_bound, "stop": stop, "last_good_pps": knee, "steps": steps,
            "baseline_rtt_ns": None, "budget_window_us": None,
            "complete": complete, "error": error}


def sample(t, echoed, dropped):
    return {"unix_ns": t, "echoed": echoed, "rx_dropped_full": dropped, "send_errors": 0,
            "turns": {"rx_received": echoed}}


def write_jsonl(path, lines):
    with open(path, "w") as f:
        f.write("\n".join(json.dumps(l) for l in lines) + "\n")


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
        steps = [step(1000, 10_000), step(2000, 12_000), step(4000, 90_000, drop=0.01)]
        svg = report.svg_plot(steps, summary(2000), "u<d>p")
        root = ET.fromstring(svg)
        self.assertEqual(len(root.findall(f"{SVG}polyline")), 5)
        self.assertIn("knee 2000 pps", svg)
        ET.fromstring(report.svg_plot([], None, "empty"))
        ET.fromstring(report.svg_plot([step(5000, 10_000)], None, "one point"))

    def test_a_step_with_total_loss_stays_on_the_plot(self):
        """Regression: a step with no echo at all (100% loss) used to vanish from the plot."""
        steps = [step(1000, 10_000, n=0), step(2000, None, drop=1.0, good=False, n=1)]
        root = ET.fromstring(report.svg_plot(steps, summary(1000), "loss"))
        self.assertEqual(len(root.findall(f"{SVG}path[@class='no-echo']")), 1)
        drop_line = root.findall(f"{SVG}polyline")[-1].get("points").split()
        self.assertEqual(len(drop_line), 2, "the drop line goes through both steps")
        only_loss = report.svg_plot([step(2000, None, drop=1.0, good=False)], summary(None, "no_good_step", 1), "x")
        self.assertIn("no-echo", only_loss)
        self.assertIn("NO ECHO", report.table(steps, []))

    def test_a_knee_at_rate_max_is_a_lower_bound(self):
        s = summary(5_000_000, stop="rate_max", lower_bound=True)
        self.assertEqual(report.knee_text(s), ">= 5000000 (rate max)")
        self.assertIn("knee &gt;= 5e+06 pps (rate max)", report.svg_plot([step(5_000_000, 10_000)], s, "max"))
        self.assertEqual(report.knee_text(summary(1000)), "1000")

    def test_missing_summary_or_expected_sweep_makes_the_report_incomplete(self):
        """Regression: a sweep without its summary (it died) and a sweep never produced used to be
        reported as if the run were whole."""
        with tempfile.TemporaryDirectory() as d:
            write_jsonl(os.path.join(d, "udp-a.jsonl"), [step(1000, 10_000), summary(1000, steps=1)])
            write_jsonl(os.path.join(d, "tcp-b.jsonl"), [step(1000, 10_000)])
            write_jsonl(os.path.join(d, "ws-c.jsonl"), [step(1000, 10_000),
                                                        summary(None, "failed", 1, complete=False, error="generator 3: exited")])
            with open(os.path.join(d, "expected.json"), "w") as f:
                json.dump({"sweeps": ["udp-a", "tcp-b", "ws-c", "udp-d"]}, f)
            self.assertEqual(report.main(["report.py", d]), 1)
            with open(os.path.join(d, "report.md")) as f:
                md = f.read()
            self.assertTrue(md.startswith("# INCOMPLETE: Zhida bench"))
            for line in ("- udp-d: missing", "- tcp-b: no summary (the sweep did not finish)",
                         "- ws-c: incomplete after 1 steps: generator 3: exited"):
                self.assertIn(line, md)
            self.assertNotIn("udp-a:", md.split("## Knees")[0])
            with open(os.path.join(d, "results.json")) as f:
                self.assertEqual(len(json.load(f)["incomplete"]), 3)

    def test_an_empty_results_directory_is_incomplete(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(report.main(["report.py", d]), 1)

    def test_build_writes_report_results_and_plots(self):
        with tempfile.TemporaryDirectory() as d:
            lines = [step(1000, 10_000, window=(150, 250)), step(2000, 90_000, good=False, bound=True, n=1),
                     summary(1000)]
            write_jsonl(os.path.join(d, "udp-zhida.jsonl"), lines)
            write_jsonl(os.path.join(d, "dut-counters-udp-zhida.jsonl"),
                        [{"kind": "sample", "sample": s} for s in [sample(100, 0, 0), sample(300, 99, 2)]])
            md, issues = report.build(d, "T")
            md = "\n".join(md)
            self.assertEqual(issues, [])
            self.assertTrue(md.startswith("# T"))
            self.assertIn("| udp-zhida | udp | 2 | 4 | 64 | 1000 | knee | 1000 | 2 | - | - | yes |", md)
            self.assertIn("| 0 | 1000 | 990 | 980 | 5.0 | 10.0 | 20.0 | 30.0 | 0.000 | 0 | yes | - | - | - | - | 3 |"
                          " 2.0 | 99 | 2 | 1.25 (9.5) |", md)
            self.assertIn("GEN-BOUND", md)
            self.assertTrue(os.path.exists(os.path.join(d, "udp-zhida.svg")))
            with open(os.path.join(d, "results.json")) as f:
                results = json.load(f)
            self.assertEqual(results["sweeps"][0]["summary"]["knee_pps"], 1000)
            self.assertEqual(len(results["sweeps"][0]["dut_samples"]), 2)


if __name__ == "__main__":
    unittest.main()
