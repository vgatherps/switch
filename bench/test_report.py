#!/usr/bin/env python3
"""Tests of report.py: python3 bench/test_report.py"""
import json
import os
import tempfile
import unittest
import xml.etree.ElementTree as ET

import report

SVG = "{http://www.w3.org/2000/svg}"


def step(pps, p99, drop=0.0, bound=False, rtt_p99=None, lag_p99=None):
    def q(p):
        return None if p is None else {"p50": p // 2, "p99": p, "p999": p * 2, "max": p * 3}
    return {"kind": "step", "offered_pps": pps, "sent_pps": pps * 0.99, "received_pps": pps * 0.98,
            "drop_fraction": drop, "budget_bound": False, "generator_bound": bound,
            "latency_ns": q(p99), "service_rtt_ns": q(rtt_p99), "send_lag_ns": q(lag_p99)}


def summary(steps=2, complete=True, error=None, bound=False):
    return {"kind": "summary", "steps": steps, "complete": complete, "generator_bound": bound, "error": error}


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
        steps = [step(1000, 10_000), step(2000, None, drop=1.0)]
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
            self.assertNotIn("udp-a:", md.split("\n## ")[0])
            with open(os.path.join(d, "results.json")) as f:
                self.assertEqual(len(json.load(f)["incomplete"]), 3)

    def test_any_generator_bound_sweep_is_incomplete(self):
        """Regression: one generator-bound sweep next to good ones passed."""
        with tempfile.TemporaryDirectory() as d:
            write_jsonl(os.path.join(d, "tcp-b.jsonl"), [step(1000, 10_000), summary(1)])
            self.assertEqual(report.main(["report.py", d]), 0)
            write_jsonl(os.path.join(d, "udp-a.jsonl"), [step(1000, 10_000, bound=True), summary(1, bound=True)])
            self.assertEqual(report.main(["report.py", d]), 1)

    def test_an_empty_results_directory_is_incomplete(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(report.main(["report.py", d]), 1)

    def test_build_writes_report_results_and_plots(self):
        with tempfile.TemporaryDirectory() as d:
            lines = [step(1000, 10_000), step(2000, 90_000, bound=True), summary()]
            write_jsonl(os.path.join(d, "udp-zhida.jsonl"), lines)
            source = {"commit": "a" * 40, "archive_sha256": "b" * 64}
            with open(os.path.join(d, "source.json"), "w") as f:
                json.dump(source, f)
            md, issues = report.build(d, "T")
            md = "\n".join(md)
            self.assertEqual(issues, [])
            self.assertTrue(md.startswith("# T"))
            self.assertIn("| not echoed % |", md)
            self.assertIn("| 1000 | 990 | 980 | 0.000 | 5.0 | 10.0 | 20.0 | 30.0 | - | - | - | - | - |", md)
            self.assertIn(f"Commit: `{source['commit']}`", md)
            self.assertIn(f"Archive SHA-256: `{source['archive_sha256']}`", md)
            self.assertTrue(os.path.exists(os.path.join(d, "udp-zhida.svg")))
            with open(os.path.join(d, "results.json")) as f:
                results = json.load(f)
                self.assertEqual(results["source"], source)
            self.assertEqual(results["sweeps"][0]["summary"]["steps"], 2)

    def test_the_step_table_shows_service_rtt_and_send_lag(self):
        t = report.table([step(1000, 10_000, rtt_p99=8_000, lag_p99=600)])
        self.assertIn("| service RTT p50 µs | service RTT p99 µs | send lag p50 µs | send lag p99 µs |", t)
        self.assertIn("| 30.0 | 4.0 | 8.0 | 0.3 | 0.6 | - |", t)


if __name__ == "__main__":
    unittest.main()
