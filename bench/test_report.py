#!/usr/bin/env python3
"""Tests of report.py: python3 bench/test_report.py"""
import json
import os
import tempfile
import unittest
import xml.etree.ElementTree as ET

import report

SVG = "{http://www.w3.org/2000/svg}"


def step(pps, p99, drop=0.0, bound=False, n=0):
    return {"kind": "step", "proto": "udp", "step": n, "offered_pps": pps, "sent_pps": pps * 0.99,
            "received_pps": pps * 0.98, "drop_fraction": drop, "unsent": 0, "generator_bound": bound,
            "budget_bound": False, "budget": None,
            "latency_ns": None if p99 is None else {"p50": p99 // 2, "p99": p99, "p999": p99 * 2, "max": p99 * 3}}


def summary(steps=2, complete=True, error=None):
    return {"kind": "summary", "proto": "udp", "gens": 2, "senders": 4, "size": 64, "steps": steps,
            "baseline_rtt_p50_ns": None, "budget_window_us": None, "complete": complete, "error": error}


def write_jsonl(path, lines):
    with open(path, "w") as f:
        f.write("\n".join(json.dumps(l) for l in lines) + "\n")


class ReportTest(unittest.TestCase):
    def test_svg_is_well_formed(self):
        steps = [step(1000, 10_000), step(2000, 12_000), step(4000, 90_000, drop=0.01)]
        root = ET.fromstring(report.svg_plot(steps, "u<d>p"))
        self.assertEqual(len(root.findall(f"{SVG}polyline")), 5)
        ET.fromstring(report.svg_plot([], "empty"))
        ET.fromstring(report.svg_plot([step(5000, 10_000)], "one point"))

    def test_a_step_with_total_loss_stays_on_the_plot(self):
        """Regression: a step with no echo at all (100% loss) used to vanish from the plot."""
        steps = [step(1000, 10_000, n=0), step(2000, None, drop=1.0, n=1)]
        root = ET.fromstring(report.svg_plot(steps, "loss"))
        self.assertEqual(len(root.findall(f"{SVG}path[@class='no-echo']")), 1)
        drop_line = root.findall(f"{SVG}polyline")[-1].get("points").split()
        self.assertEqual(len(drop_line), 2, "the drop line goes through both steps")
        only_loss = report.svg_plot([step(2000, None, drop=1.0)], "x")
        self.assertIn("no-echo", only_loss)
        self.assertIn("NO ECHO", report.table(steps))

    def test_missing_summary_or_expected_sweep_makes_the_report_incomplete(self):
        """Regression: a sweep without its summary (it died) and a sweep never produced used to be
        reported as if the run were whole."""
        with tempfile.TemporaryDirectory() as d:
            write_jsonl(os.path.join(d, "udp-a.jsonl"), [step(1000, 10_000), summary(steps=1)])
            write_jsonl(os.path.join(d, "tcp-b.jsonl"), [step(1000, 10_000)])
            write_jsonl(os.path.join(d, "ws-c.jsonl"), [step(1000, 10_000),
                                                        summary(1, complete=False, error="generator 3: exited")])
            with open(os.path.join(d, "expected.json"), "w") as f:
                json.dump({"sweeps": ["udp-a", "tcp-b", "ws-c", "udp-d"]}, f)
            self.assertEqual(report.main(["report.py", d]), 1)
            with open(os.path.join(d, "report.md")) as f:
                md = f.read()
            self.assertTrue(md.startswith("# INCOMPLETE: Zhida bench"))
            for line in ("- udp-d: missing", "- tcp-b: no summary (the sweep did not finish)",
                         "- ws-c: incomplete after 1 steps: generator 3: exited"):
                self.assertIn(line, md)
            self.assertNotIn("udp-a:", md.split("## Sweeps")[0])
            with open(os.path.join(d, "results.json")) as f:
                self.assertEqual(len(json.load(f)["incomplete"]), 3)

    def test_an_empty_results_directory_is_incomplete(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(report.main(["report.py", d]), 1)

    def test_build_writes_report_results_and_plots(self):
        with tempfile.TemporaryDirectory() as d:
            lines = [step(1000, 10_000), step(2000, 90_000, bound=True, n=1), summary()]
            write_jsonl(os.path.join(d, "udp-zhida.jsonl"), lines)
            md, issues = report.build(d, "T")
            md = "\n".join(md)
            self.assertEqual(issues, [])
            self.assertTrue(md.startswith("# T"))
            self.assertIn("| udp-zhida | udp | 2 | 4 | 64 | 2 | - | - | yes |", md)
            self.assertIn("| 0 | 1000 | 990 | 980 | 5.0 | 10.0 | 20.0 | 30.0 | 0.000 | 0 | - | - |", md)
            self.assertIn("GEN-BOUND", md)
            self.assertTrue(os.path.exists(os.path.join(d, "udp-zhida.svg")))
            with open(os.path.join(d, "results.json")) as f:
                results = json.load(f)
            self.assertEqual(results["sweeps"][0]["summary"]["steps"], 2)


if __name__ == "__main__":
    unittest.main()
