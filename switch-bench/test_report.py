#!/usr/bin/env python3
"""Tests of report.py: python3 switch-bench/test_report.py"""
import json
import os
import tempfile
import unittest
import xml.etree.ElementTree as ET

import report

SVG = "{http://www.w3.org/2000/svg}"


def step(pps, p99, drop=0.0, bound=False):
    return {"kind": "step", "offered_pps": pps, "sent_pps": pps * 0.99, "received_pps": pps * 0.98,
            "drop_fraction": drop, "budget_bound": False, "generator_bound": bound,
            "latency_ns": None if p99 is None else {"p50": p99 // 2, "p99": p99, "p999": p99 * 2, "max": p99 * 3}}


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
            self.assertTrue(md.startswith("# INCOMPLETE: switch bench"))
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
            write_jsonl(os.path.join(d, "udp-switch.jsonl"), lines)
            source = {"commit": "a" * 40, "archive_sha256": "b" * 64}
            with open(os.path.join(d, "source.json"), "w") as f:
                json.dump(source, f)
            md, issues = report.build(d, "T")
            md = "\n".join(md)
            self.assertEqual(issues, [])
            self.assertTrue(md.startswith("# T"))
            self.assertIn("| not echoed % |", md)
            self.assertIn("| 1000 | 990 | 980 | 0.000 | 5.0 | 10.0 | 20.0 | 30.0 | - |", md)
            self.assertIn(f"Commit: `{source['commit']}`", md)
            self.assertIn(f"Archive SHA-256: `{source['archive_sha256']}`", md)
            self.assertTrue(os.path.exists(os.path.join(d, "udp-switch.svg")))
            with open(os.path.join(d, "results.json")) as f:
                results = json.load(f)
                self.assertEqual(results["source"], source)
            self.assertEqual(results["sweeps"][0]["summary"]["steps"], 2)
            self.assertNotIn("Thread placement", md)
            self.assertNotIn("INVALID", md)
            self.assertNotIn("placement", results["sweeps"][0])

    def test_placement_tables_show_backend_host_cpu_ids_and_core_counts(self):
        with tempfile.TemporaryDirectory() as d:
            placements = {
                "udp-conduit": {
                    "backend": "conduit",
                    "hosts": {
                        "receiver": {"app_cpu": "7", "junction_cpu": None, "housekeeping": "0-5", "isolated": "6-7"},
                        "sender0": {"app_cpu": "7", "junction_cpu": None, "housekeeping": "0-5", "isolated": "6-7"},
                    },
                    "errors": [],
                },
                "udp-junction": {
                    "backend": "junction",
                    "hosts": {
                        "receiver": {"app_cpu": "7", "junction_cpu": "6", "housekeeping": "0-5", "isolated": "6-7"},
                        "sender0": {"app_cpu": "9", "junction_cpu": "8", "housekeeping": "0-7", "isolated": "8-9"},
                    },
                    "errors": [],
                },
                "udp-junction-kernel": {
                    "backend": "junction",
                    "hosts": {
                        "receiver": {"app_cpu": "7", "junction_cpu": None, "housekeeping": "0-5", "isolated": "6-7"},
                        "sender0": {"app_cpu": "7", "junction_cpu": "6", "housekeeping": "0-5", "isolated": "6-7"},
                    },
                    "errors": [],
                },
            }
            for stem, placement in placements.items():
                write_jsonl(os.path.join(d, stem + ".jsonl"), [step(1000, 10_000), summary(1)])
                with open(os.path.join(d, stem + ".placement.json"), "w") as f:
                    json.dump(placement, f)
            md, issues = report.build(d, "T")
            md = "\n".join(md)
            self.assertEqual(issues, [])
            self.assertNotIn("INVALID", md)
            self.assertEqual(md.count("### Thread placement"), 3)
            self.assertIn("| conduit | receiver | 7 | - | 7 | 1 | 0-5 | 6-7 |", md)
            self.assertIn("| conduit | sender0 | 7 | - | 7 | 1 | 0-5 | 6-7 |", md)
            self.assertIn("| junction | receiver | 7 | 6 | 7, 6 | 2 | 0-5 | 6-7 |", md)
            self.assertIn("| junction | sender0 | 9 | 8 | 9, 8 | 2 | 0-7 | 8-9 |", md)
            self.assertIn("| junction | receiver | 7 | - | 7 | 1 | 0-5 | 6-7 |", md)
            self.assertIn("| junction | sender0 | 7 | 6 | 7, 6 | 2 | 0-5 | 6-7 |", md)
            for stem, placement in placements.items():
                for role in placement["hosts"]:
                    self.assertIn(f"[raw]({stem}.{role}.placement.txt)", md)
            with open(os.path.join(d, "results.json")) as f:
                results = json.load(f)
            self.assertEqual({s["name"]: s["placement"] for s in results["sweeps"]}, placements)
            self.assertNotIn("Interrupt deltas", md)
            self.assertNotIn("[end]", md)

    def test_placement_errors_invalidate_complete_sweep_without_hiding_data(self):
        with tempfile.TemporaryDirectory() as d:
            measured = step(1000, 10_000)
            complete = summary(1)
            write_jsonl(os.path.join(d, "udp-junction.jsonl"), [measured, complete])
            write_jsonl(os.path.join(d, "udp-conduit.jsonl"), [measured, complete])
            placement = {
                "backend": "junction",
                "hosts": {
                    "receiver": {"app_cpu": "7", "junction_cpu": "6", "housekeeping": "0-5", "isolated": "6-7"},
                },
                "errors": ["receiver: app thread allowed on CPU 0", "receiver: daemon missing on CPU 6"],
            }
            with open(os.path.join(d, "udp-junction.placement.json"), "w") as f:
                json.dump(placement, f)
            self.assertEqual(report.main(["report.py", d]), 1)
            with open(os.path.join(d, "report.md")) as f:
                md = f.read()
            self.assertTrue(md.startswith("# INCOMPLETE:"))
            self.assertIn("## udp-junction — INVALID", md)
            self.assertIn("## udp-conduit\n", md)
            self.assertNotIn("## udp-conduit — INVALID", md)
            for error in placement["errors"]:
                self.assertIn(f"- INVALID placement: {error}", md)
            self.assertIn("![udp-junction](udp-junction.svg)", md)
            self.assertIn("| 1000 | 990 | 980 |", md)
            with open(os.path.join(d, "udp-junction.svg")) as f:
                root = ET.fromstring(f.read())
            self.assertEqual(len(root.findall(f"{SVG}polyline")), 5)
            with open(os.path.join(d, "results.json")) as f:
                results = json.load(f)
            invalid = next(s for s in results["sweeps"] if s["name"] == "udp-junction")
            self.assertEqual(invalid, {"name": "udp-junction", "summary": complete,
                                       "steps": [measured], "placement": placement})
            self.assertEqual(results["incomplete"],
                             [f"udp-junction: placement: {error}" for error in placement["errors"]])

    def test_carryover_starts_after_loss_or_sender_late_only_for_junction_uncapped_streams(self):
        for trigger in ("loss", "sender-late", "clean"):
            with self.subTest(trigger=trigger), tempfile.TemporaryDirectory() as d:
                measured = [
                    step(1000, 10_000),
                    step(2000, 10_000, drop=0.01 if trigger == "loss" else 0,
                         bound=trigger == "sender-late"),
                    step(3000, 10_000),
                    step(4000, None if trigger != "clean" else 10_000,
                         drop=1.0 if trigger != "clean" else 0),
                    step(5000, 10_000, bound=trigger != "clean"),
                ]
                complete = summary(5, bound=trigger != "clean")
                names = []
                affected = set()
                for backend in ("junction", "conduit"):
                    for proto in ("tcp", "ws", "udp"):
                        for budget in ("uncapped", "b100"):
                            stem = f"{backend}-{proto}-n1-s64-{budget}"
                            names.append(stem)
                            if backend == "junction" and proto in ("tcp", "ws") and budget == "uncapped":
                                affected.add(stem)
                            write_jsonl(os.path.join(d, stem + ".jsonl"), [*measured, complete])
                md, _ = report.build(d, "T")
                md = "\n".join(md)
                for stem in names:
                    with self.subTest(stem=stem):
                        section = md.split(f"## {stem}\n", 1)[1].split("\n## ", 1)[0]
                        rows = [line for line in section.splitlines()
                                if line.startswith("| ") and line.split("|")[1].strip().isdigit()]
                        self.assertEqual(len(rows), len(measured))
                        expected = stem in affected and trigger != "clean"
                        for index, row in enumerate(rows):
                            self.assertEqual("CARRYOVER" in row, expected and index > 1)
                        self.assertEqual("not comparable" in section, expected)
                        if trigger != "clean":
                            self.assertIn("NO ECHO", rows[3])
                            self.assertIn("SENDER-LATE", rows[4])
                        if trigger == "sender-late":
                            self.assertIn("SENDER-LATE", rows[1])
                        with open(os.path.join(d, stem + ".svg")) as f:
                            root = ET.fromstring(f.read())
                        drop_line = root.findall(f"{SVG}polyline")[-1].get("points").split()
                        self.assertEqual(len(drop_line), len(measured))
                with open(os.path.join(d, "results.json")) as f:
                    results = json.load(f)
                self.assertEqual(len(results["sweeps"]), len(names))
                for sweep in results["sweeps"]:
                    self.assertEqual(sweep["steps"], measured)
                    self.assertEqual(sweep["summary"], complete)

    def test_first_problematic_step_without_later_steps_is_not_carryover(self):
        for problematic in (step(1000, None, drop=1), step(1000, 10_000, bound=True)):
            with self.subTest(problematic=problematic):
                text = report.table([problematic], carryover=True)
                self.assertNotIn("CARRYOVER", text)
                self.assertNotIn("not comparable", text)
                later = report.table([problematic, step(2000, 10_000)], carryover=True)
                data_rows = [line for line in later.splitlines() if line.startswith("| 1000")
                             or line.startswith("| 2000")]
                self.assertNotIn("CARRYOVER", data_rows[0])
                self.assertIn("CARRYOVER", data_rows[1])

    def test_numeric_arch_timer_zero_counts_remain_visible(self):
        host = {"app_cpu": "7", "junction_cpu": None, "housekeeping": "0-5", "isolated": "6-7",
                "interrupt_deltas": [
                    {"irq": "10", "name": "GICv3 27 Level arch_timer", "cpus": {"6": 0, "7": 0}},
                    {"irq": "11", "name": "GICv3 28 Level ena-arch_timer", "cpus": {"6": 0, "7": 0}}]}
        rendered = report.placement_table("tag", {"backend": "conduit", "hosts": {"receiver": host}})
        self.assertIn("| 10 | GICv3 27 Level arch_timer | 0 | 0 |", rendered)
        self.assertNotIn("| 11 |", rendered)

    def test_early_stop_reports_the_coordinator_reason_without_end_snapshot_noise(self):
        with tempfile.TemporaryDirectory() as directory:
            stem = "conduit-tcp-n12-s64-uncapped"
            write_jsonl(os.path.join(directory, stem + ".jsonl"),
                        [step(1000, 10_000, bound=True), summary(1, bound=True)])
            with open(os.path.join(directory, stem + ".placement.json"), "w") as file:
                json.dump({"backend": "conduit", "hosts": {}, "errors": [],
                           "end_snapshot_skipped": "coordinator stopped early; see sweep summary"}, file)
            markdown, issues = report.build(directory, "early stop")
            self.assertEqual(issues, [stem + ": sender fell behind schedule; receiver capacity is not isolated"])
            self.assertNotIn("INVALID", "\n".join(markdown))
            self.assertNotIn("end snapshot", "\n".join(markdown))

    def test_kernel_receiver_data_irq_exemption_is_explicit(self):
        host = {"app_cpu": "7", "junction_cpu": None, "housekeeping": "0-5", "isolated": "6-7",
                "allowed_device_irqs": {"45": [7]},
                "interrupt_deltas": [{"irq": "45", "name": "ena", "cpus": {"6": 0, "7": 5}}]}
        rendered = report.placement_table("tag", {"backend": "conduit", "hosts": {"receiver": host}})
        self.assertIn("| 45 | ena | 0 | 5 |", rendered)
        self.assertIn("Kernel receiver measured data-ENI IRQs exempt from device-IRQ invalidation: 45 on CPU 7.", rendered)

    def test_interrupt_deltas_show_each_host_cpu_and_both_snapshots_without_hiding_invalid_data(self):
        with tempfile.TemporaryDirectory() as d:
            stem = "junction-tcp-n1-s64-uncapped"
            measured = step(1000, 10_000)
            write_jsonl(os.path.join(d, stem + ".jsonl"), [measured, summary(1)])
            placement = {"backend": "junction", "hosts": {}, "errors": [
                "receiver: IRQ 24 increased on isolated CPU 7 by 3"]}
            for role, app, daemon in (("receiver", "7", "6"), ("sender0", "11", "10")):
                placement["hosts"][role] = {
                    "app_cpu": app, "junction_cpu": daemon,
                    "housekeeping": "0-5" if role == "receiver" else "0-9",
                    "isolated": f"{daemon}-{app}",
                    "snapshots": [f"{stem}.{role}.placement.txt", f"{stem}.{role}.placement.end.txt"],
                    "interrupt_deltas": [
                        {"irq": "24", "name": "ena-Tx-Rx-0", "cpus": {app: 3, daemon: 0}},
                        {"irq": "25", "name": "ena-Tx-Rx-1", "cpus": {app: 0, daemon: 0}},
                        {"irq": "LOC", "name": "Local timer interrupts", "cpus": {app: 0, daemon: 0}},
                        {"irq": "RES", "name": "Rescheduling interrupts", "cpus": {app: 0, daemon: 2}},
                        {"irq": "NMI", "name": "Non-maskable interrupts", "cpus": {app: 0, daemon: 0}},
                    ],
                }
            with open(os.path.join(d, stem + ".placement.json"), "w") as f:
                json.dump(placement, f)
            md, issues = report.build(d, "T")
            md = "\n".join(md)
            self.assertEqual(issues, [f"{stem}: placement: {placement['errors'][0]}"])
            self.assertIn(f"## {stem} — INVALID", md)
            self.assertIn("| 1000 | 990 | 980 |", md)
            for role, cpus in (("receiver", ("6", "7")), ("sender0", ("10", "11"))):
                self.assertIn(f"[raw]({stem}.{role}.placement.txt)", md)
                self.assertIn(f"[end]({stem}.{role}.placement.end.txt)", md)
                section = md.split(f"### Interrupt deltas — {role}\n", 1)[1].split("\n### ", 1)[0]
                self.assertIn(f"| IRQ | name | CPU {cpus[0]} | CPU {cpus[1]} |", section)
                self.assertIn("| 24 | ena-Tx-Rx-0 | 0 | 3 |", section)
                self.assertNotIn("| 25 |", section)
                self.assertIn("| LOC | Local timer interrupts | 0 | 0 |", section)
                self.assertIn("| RES | Rescheduling interrupts | 2 | 0 |", section)
                self.assertIn("| NMI | Non-maskable interrupts | 0 | 0 |", section)
            self.assertIn("| junction | receiver | 7 | 6 | 7, 6 | 2 |", md)
            self.assertIn("| junction | sender0 | 11 | 10 | 11, 10 | 2 |", md)
            with open(os.path.join(d, "results.json")) as f:
                sweep = json.load(f)["sweeps"][0]
            self.assertEqual(sweep["placement"], placement)
            self.assertEqual(sweep["steps"], [measured])


if __name__ == "__main__":
    unittest.main()
