#!/usr/bin/env python3
"""Formats bench results: report.md, results.json and one SVG plot per sweep.

Usage: report.py RESULTS_DIR [--title T]

RESULTS_DIR holds `<sweep>.jsonl` sweep outputs (`zhida-bench sweep` lines), and optionally
`dut-counters-<sweep>.jsonl` (the `zhida-bench serve` counter log of that sweep's DUT),
`expected.json` ({"sweeps": [<sweep>, ...]}: every sweep the run meant to produce),
`host-setup.txt` (the host setup verification), `pingpong-*.jsonl` and `counters-on.jsonl` /
`counters-off.jsonl` (the counter-capture check: the same single rate with DUT counter capture on
and off). Python stdlib only.

Exit status 1 when the results are incomplete: an expected sweep is missing, a sweep has no
summary, or a sweep did not complete. The report is still written, headed INCOMPLETE.
"""
import json
import math
import os
import sys
from xml.sax.saxutils import escape


def load_jsonl(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def sweep_files(directory):
    for name in sorted(os.listdir(directory)):
        if name.endswith(".jsonl") and not name.startswith(("dut-counters-", "counters-", "pingpong-")):
            yield name, os.path.join(directory, name)


def split_sweep(lines):
    steps = [l for l in lines if l.get("kind") == "step"]
    summary = next((l for l in lines if l.get("kind") == "summary"), None)
    return steps, summary


def problems(directory, sweeps):
    """Why the results are incomplete, one line each; empty if complete. `sweeps` maps a sweep's
    name to its summary (None if it has none)."""
    out = []
    expected_path = os.path.join(directory, "expected.json")
    if os.path.exists(expected_path):
        with open(expected_path) as f:
            expected = json.load(f)["sweeps"]
        out += [f"{name}: missing" for name in expected if name not in sweeps]
    elif not sweeps:
        out.append("no sweep results")
    for name, summary in sweeps.items():
        if summary is None:
            out.append(f"{name}: no summary (the sweep did not finish)")
        elif not summary.get("complete"):
            out.append(f"{name}: incomplete after {summary.get('steps')} steps: {summary.get('error')}")
    return out


def dut_samples(directory, sweep):
    """The `sample`s of the sweep's DUT counter log, oldest first."""
    path = os.path.join(directory, f"dut-counters-{sweep}.jsonl")
    if not os.path.exists(path):
        return []
    samples = [l["sample"] for l in load_jsonl(path) if l.get("kind") == "sample"]
    return sorted(samples, key=lambda s: s["unix_ns"])


def dut_delta(samples, window):
    """DUT counter deltas over [start, end] from the last sample at or before `start` to the first at
    or after `end`; None if the samples do not bracket the window."""
    start, end = window
    before = [s for s in samples if s["unix_ns"] <= start]
    after = [s for s in samples if s["unix_ns"] >= end]
    if not before or not after:
        return None
    a, b = before[-1], after[0]
    return {
        "echoed": b["echoed"] - a["echoed"],
        "rx_dropped_full": b["rx_dropped_full"] - a["rx_dropped_full"],
        "rx_received": b["turns"]["rx_received"] - a["turns"]["rx_received"],
        "send_errors": b["send_errors"] - a["send_errors"],
    }


def us(ns):
    return "-" if ns is None else f"{ns / 1000:.1f}"


def lat(step, key):
    q = step.get("latency_ns")
    return None if q is None else q[key]


def max_gen_lag_p99(step):
    lags = [g["send_lag_ns"]["p99"] for g in step.get("per_gen", []) if g.get("send_lag_ns")]
    return max(lags) if lags else None


def round_cost(step):
    """The slowest generator worker's loop: µs per round and Runtime task polls per round."""
    gens = [g for g in step.get("per_gen", []) if g.get("ns_per_round")]
    if not gens:
        return "-"
    g = max(gens, key=lambda g: g["ns_per_round"])
    return f"{g['ns_per_round'] / 1000:.2f} ({g['tasks_per_round']:.1f})"


def owed(step):
    return sum(g.get("owed_at_end", 0) for g in step.get("per_gen", []))


def flags(step):
    out = []
    if step.get("generator_bound"):
        out.append("GEN-BOUND")
    if step.get("budget_bound"):
        out.append("BUDGET-BOUND")
    if step.get("latency_ns") is None:
        out.append("NO ECHO")
    return " ".join(out) or "-"


def in_flight(step):
    mean = step.get("in_flight_mean")
    return "-" if mean is None else f"{mean:.1f} / {step.get('in_flight_max', 0)}"


def table(steps, samples):
    rows = [
        "| step | offered pps | sent pps | received pps | p50 µs | p99 µs | p99.9 µs | max µs | drop % | unsent |"
        " good | flags | service RTT p50 µs | budget | in flight mean / max | owed | max gen lag p99 µs |"
        " DUT echoed | DUT full-queue drops | gen µs/round (tasks/round) |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:-:|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for s in sorted(steps, key=lambda s: (s["offered_pps"], s["step"])):
        d = dut_delta(samples, s["window_unix_ns"]) if samples else None
        rtt = s.get("service_rtt_ns")
        rows.append(
            f"| {s['step']} | {s['offered_pps']:.0f} | {s.get('sent_pps', 0):.0f} | {s.get('received_pps', 0):.0f} |"
            f" {us(lat(s, 'p50'))} | {us(lat(s, 'p99'))} | {us(lat(s, 'p999'))} | {us(lat(s, 'max'))} |"
            f" {100 * s['drop_fraction']:.3f} | {s.get('unsent', 0)} | {'yes' if s['good'] else 'no'} | {flags(s)} |"
            f" {us(rtt['p50'] if rtt else None)} | {s.get('budget') or '-'} | {in_flight(s)} | {owed(s)} |"
            f" {us(max_gen_lag_p99(s))} | {'-' if d is None else d['echoed']} |"
            f" {'-' if d is None else d['rx_dropped_full']} | {round_cost(s)} |"
        )
    return "\n".join(rows)


def knee_text(summary):
    knee = summary.get("knee_pps")
    if knee is None:
        return "none"
    return f">= {knee} (rate max)" if summary.get("knee_lower_bound") else str(knee)


def svg_plot(steps, summary, title):
    """p50/p99/p99.9/max latency (log y, left) and drop % (linear, right) vs offered pps (log x).
    A step where nothing came back has no latency: it is drawn as an x at the top of the latency
    axis, and its 100% drop is on the drop line like every other step's."""
    W, H, L, R, T, B = 760, 420, 70, 70, 40, 50
    steps = sorted(steps, key=lambda s: (s["offered_pps"], s["step"]))
    pts = [s for s in steps if s.get("latency_ns")]
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" font-family="sans-serif" font-size="11">',
           f'<rect width="{W}" height="{H}" fill="white"/>',
           f'<text x="{W / 2}" y="20" text-anchor="middle" font-size="14">{escape(title)}</text>']
    if not steps:
        out.append(f'<text x="{W / 2}" y="{H / 2}" text-anchor="middle">no data</text></svg>')
        return "\n".join(out)
    xs = [s["offered_pps"] for s in steps]
    ys = [s["latency_ns"][k] / 1000 for s in pts for k in ("p50", "p99", "p999", "max")] or [1.0]
    x0, x1 = math.log10(min(xs)), math.log10(max(xs))
    if x1 - x0 < 1e-9:
        x0, x1 = x0 - 0.5, x1 + 0.5
    y0, y1 = math.floor(math.log10(max(min(ys), 0.1))), math.ceil(math.log10(max(ys)))
    if y1 <= y0:
        y1 = y0 + 1
    dmax = max(1.0, max(100 * s["drop_fraction"] for s in steps))

    def px(x):
        return L + (math.log10(x) - x0) / (x1 - x0) * (W - L - R)

    def py(y):
        return H - B - (math.log10(max(y, 10 ** y0)) - y0) / (y1 - y0) * (H - T - B)

    def pd(d):
        return H - B - d / dmax * (H - T - B)

    out.append(f'<rect x="{L}" y="{T}" width="{W - L - R}" height="{H - T - B}" fill="none" stroke="#888"/>')
    for e in range(y0, y1 + 1):
        y = py(10 ** e)
        out.append(f'<line x1="{L}" y1="{y:.1f}" x2="{W - R}" y2="{y:.1f}" stroke="#eee"/>')
        out.append(f'<text x="{L - 6}" y="{y + 4:.1f}" text-anchor="end">{10 ** e:g} µs</text>')
    for e in range(math.floor(x0), math.ceil(x1) + 1):
        if x0 <= e <= x1:
            x = px(10 ** e)
            out.append(f'<line x1="{x:.1f}" y1="{T}" x2="{x:.1f}" y2="{H - B}" stroke="#eee"/>')
            out.append(f'<text x="{x:.1f}" y="{H - B + 16}" text-anchor="middle">{10 ** e:g}</text>')
    out.append(f'<text x="{(L + W - R) / 2}" y="{H - 10}" text-anchor="middle">offered pps</text>')
    out.append(f'<text x="{W - R + 4}" y="{T + 4}">{dmax:.3g}%</text><text x="{W - R + 4}" y="{T + 16}">drop</text>')
    out.append(f'<text x="{W - R + 4}" y="{H - B}">0%</text>')
    colors = {"p50": "#1f77b4", "p99": "#ff7f0e", "p999": "#2ca02c", "max": "#d62728"}
    for i, (key, color) in enumerate(colors.items()):
        if pts:
            path = " ".join(f"{px(s['offered_pps']):.1f},{py(s['latency_ns'][key] / 1000):.1f}" for s in pts)
            out.append(f'<polyline points="{path}" fill="none" stroke="{color}" stroke-width="1.5"/>')
        out.append(f'<text x="{L + 8 + 70 * i}" y="{T + 14}" fill="{color}">{key}</text>')
    path = " ".join(f"{px(s['offered_pps']):.1f},{pd(100 * s['drop_fraction']):.1f}" for s in steps)
    out.append(f'<polyline points="{path}" fill="none" stroke="#9467bd" stroke-dasharray="4 3"/>')
    out.append(f'<text x="{L + 8 + 280}" y="{T + 14}" fill="#9467bd">drop %</text>')
    for s in steps:
        if not s.get("latency_ns"):
            x = px(s["offered_pps"])
            out.append(f'<path class="no-echo" d="M{x - 4:.1f},{T + 2} l8,8 m0,-8 l-8,8" stroke="black"/>')
    if any(not s.get("latency_ns") for s in steps):
        out.append(f'<text x="{L + 8 + 350}" y="{T + 14}">x: no echo (100% loss)</text>')
    knee = summary.get("knee_pps") if summary else None
    if knee:
        x = px(knee)
        label = f"knee >= {knee:g} pps (rate max)" if summary.get("knee_lower_bound") else f"knee {knee:g} pps"
        out.append(f'<line x1="{x:.1f}" y1="{T}" x2="{x:.1f}" y2="{H - B}" stroke="black" stroke-dasharray="2 2"/>')
        out.append(f'<text x="{x + 4:.1f}" y="{T + 30}">{escape(label)}</text>')
    out.append("</svg>")
    return "\n".join(out)


def pingpong(directory):
    rows = ["| ping-pong (TCP, closed loop) | size | count | p50 µs | p99 µs | p99.9 µs | max µs |",
            "|---|---:|---:|---:|---:|---:|---:|"]
    for name in sorted(os.listdir(directory)):
        if name.startswith("pingpong-") and name.endswith(".jsonl"):
            for r in load_jsonl(os.path.join(directory, name)):
                q = r.get("rtt_ns") or {}
                rows.append(f"| {name[len('pingpong-'):-len('.jsonl')]} | {r['size']} | {r['count']} |"
                            f" {us(q.get('p50'))} | {us(q.get('p99'))} | {us(q.get('p999'))} | {us(q.get('max'))} |")
    return "\n".join(rows) if len(rows) > 2 else None


def counters_check(directory):
    paths = [os.path.join(directory, f"counters-{m}.jsonl") for m in ("on", "off")]
    if not all(os.path.exists(p) for p in paths):
        return None
    rows = ["| DUT counter capture | offered pps | p50 µs | p99 µs | max µs | drop % |", "|---|---:|---:|---:|---:|---:|"]
    for mode, path in zip(("on", "off"), paths):
        for s in split_sweep(load_jsonl(path))[0]:
            rows.append(f"| {mode} | {s['offered_pps']:.0f} | {us(lat(s, 'p50'))} | {us(lat(s, 'p99'))} |"
                        f" {us(lat(s, 'max'))} | {100 * s['drop_fraction']:.3f} |")
    return "\n".join(rows)


def build(directory, title):
    """Writes report.md, results.json and the plots; returns (markdown lines, problems)."""
    md = []
    setup = os.path.join(directory, "host-setup.txt")
    if os.path.exists(setup):
        with open(setup) as f:
            md += ["## Host setup", "", "```", f.read().rstrip(), "```", ""]
    results = {"title": title, "sweeps": []}
    knees = ["| sweep | proto | gens | senders | size | knee pps | stopped by | last good pps | steps |"
             " baseline RTT p50 / p99 µs | budget window µs | first bad step budget-bound | complete |",
             "|---|---|---:|---:|---:|---:|---|---:|---:|---:|---:|:-:|:-:|"]
    sections = []
    summaries = {}
    for name, path in sweep_files(directory):
        steps, summary = split_sweep(load_jsonl(path))
        stem = name[: -len(".jsonl")]
        summaries[stem] = summary
        samples = dut_samples(directory, stem)
        results["sweeps"].append({"name": stem, "summary": summary, "steps": steps, "dut_samples": samples})
        if summary:
            base = summary.get("baseline_rtt_ns")
            knees.append(f"| {stem} | {summary['proto']} | {summary['gens']} | {summary['senders']} | {summary['size']} |"
                         f" {knee_text(summary)} | {summary.get('stop', '-')} | {summary.get('last_good_pps') or '-'} |"
                         f" {summary['steps']} | {us(base['p50']) + ' / ' + us(base['p99']) if base else '-'} |"
                         f" {summary.get('budget_window_us') or '-'} |"
                         f" {'YES' if summary.get('first_bad_budget_bound') else 'no'} |"
                         f" {'yes' if summary.get('complete') else 'NO'} |")
        else:
            knees.append(f"| {stem} | - | - | - | - | - | no summary | - | {len(steps)} | - | - | - | NO |")
        with open(os.path.join(directory, stem + ".svg"), "w") as f:
            f.write(svg_plot(steps, summary, stem))
        sections += [f"## {stem}", "", f"![{stem}]({stem}.svg)", "", table(steps, samples), ""]
        if samples:
            sections += ["DUT columns: the Zhida DUT's counters between its samples (every 100 ms) bracketing the"
                         " step's measured window, warmup included.", ""]
        if any(s["generator_bound"] for s in steps):
            sections += ["GEN-BOUND: a generator fell behind its schedule; that step measured the generator, not the"
                         " DUT, and the sweep stopped there.", ""]
        if any(s.get("budget_bound") for s in steps):
            sections += ["BUDGET-BOUND: some messages waited because their connection already had its budget in"
                         " flight (rate share x (baseline RTT + window)). Where service RTT stays near the baseline,"
                         " the latency there is the budget's, not receiver saturation.", ""]
    issues = problems(directory, summaries)
    head = [f"# {title}", ""]
    if issues:
        head = [f"# INCOMPLETE: {title}", "", "These results are incomplete; what ran is below.", "",
                *[f"- {i}" for i in issues], ""]
    md = head + md + ["## Knees", "", "\n".join(knees), "", *sections]
    pp = pingpong(directory)
    if pp:
        md += ["## Ping-pong", "", "Round-trip time of one message at a time on one connection.", "", pp, ""]
    check = counters_check(directory)
    if check:
        md += ["## DUT counter capture check", "",
               "The same rate with the DUT's in-loop counter capture on and off: p99 and max should agree within noise.",
               "", check, ""]
    md += ["Notes: latency is receive time minus scheduled send time (open loop, no coordinated omission); drop % is"
           " measured messages not echoed by the end of the drain, unsent ones included (sending stops at the end"
           " of the window); sent / received pps are what actually left the generators and came back within the"
           " window; service RTT is echo time minus actual send time (TCP/ws); owed = messages a TCP connection"
           " still owed at the end of the window (flow control or budget: the receiver side, not the generator);"
           " DUT echoed counts datagrams (UDP) or bytes (TCP); no TLS (ws:// only)."]
    results["incomplete"] = issues
    with open(os.path.join(directory, "report.md"), "w") as f:
        f.write("\n".join(md) + "\n")
    with open(os.path.join(directory, "results.json"), "w") as f:
        json.dump(results, f, indent=1)
    return md, issues


def main(argv):
    if len(argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    title = argv[argv.index("--title") + 1] if "--title" in argv else "Zhida bench"
    _, issues = build(argv[1], title)
    print(f"wrote {os.path.join(argv[1], 'report.md')}")
    for i in issues:
        print(f"incomplete: {i}", file=sys.stderr)
    return 1 if issues else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
