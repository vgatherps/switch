#!/usr/bin/env python3
"""Formats bench results: report.md, results.json and one SVG plot per sweep.

Usage: report.py RESULTS_DIR [--title T]

RESULTS_DIR holds `<sweep>.jsonl` sweep outputs (`zhida-bench sweep` lines), and optionally
`expected.json` ({"sweeps": [<sweep>, ...]}: every sweep the run meant to produce) and
`host-setup.txt` (the host setup verification). Python stdlib only.

Exit status 1 when the results are incomplete: an expected sweep is missing, a sweep has no
summary, a sweep did not complete, or a sweep stopped at a generator-bound step (the generators,
not the receiver, were the limit). The report is still written, headed INCOMPLETE.
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
        if name.endswith(".jsonl"):
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
        elif summary.get("generator_bound"):
            out.append(f"{name}: stopped at a generator-bound step: not a receiver measurement")
    return out


def us(ns):
    return "-" if ns is None else f"{ns / 1000:.1f}"


def lat(step, key):
    q = step.get("latency_ns")
    return None if q is None else q[key]


def flags(step):
    out = []
    if step.get("generator_bound"):
        out.append("GEN-BOUND")
    if step.get("budget_bound"):
        out.append("BUDGET-BOUND")
    if step.get("latency_ns") is None:
        out.append("NO ECHO")
    return " ".join(out) or "-"


def table(steps):
    rows = [
        "| offered pps | sent pps | received pps | not echoed % | p50 µs | p99 µs | p99.9 µs | max µs | flags |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for s in sorted(steps, key=lambda s: s["offered_pps"]):
        rows.append(
            f"| {s['offered_pps']:.0f} | {s['sent_pps']:.0f} | {s['received_pps']:.0f} | {100 * s['drop_fraction']:.3f} |"
            f" {us(lat(s, 'p50'))} | {us(lat(s, 'p99'))} | {us(lat(s, 'p999'))} | {us(lat(s, 'max'))} | {flags(s)} |"
        )
    return "\n".join(rows)


def svg_plot(steps, title):
    """p50/p99/p99.9/max latency (log y, left) and not echoed % (linear, right) vs offered pps (log x).
    A step where nothing came back has no latency: it is drawn as an x at the top of the latency
    axis, and its 100% not echoed is on the loss line like every other step's."""
    W, H, L, R, T, B = 760, 420, 70, 70, 40, 50
    steps = sorted(steps, key=lambda s: s["offered_pps"])
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
    out.append(f'<text x="{L + 8 + 280}" y="{T + 14}" fill="#9467bd">not echoed %</text>')
    for s in steps:
        if not s.get("latency_ns"):
            x = px(s["offered_pps"])
            out.append(f'<path class="no-echo" d="M{x - 4:.1f},{T + 2} l8,8 m0,-8 l-8,8" stroke="black"/>')
    if any(not s.get("latency_ns") for s in steps):
        out.append(f'<text x="{L + 8 + 350}" y="{T + 14}">x: no echo (100% loss)</text>')
    out.append("</svg>")
    return "\n".join(out)


def build(directory, title):
    """Writes report.md, results.json and the plots; returns (markdown lines, problems)."""
    md = []
    source_path = os.path.join(directory, "source.json")
    source = None
    if os.path.exists(source_path):
        with open(source_path) as f:
            source = json.load(f)
        md += ["## Source", "", f"Commit: `{source['commit']}`  ",
               f"Archive SHA-256: `{source['archive_sha256']}`", ""]
    setup = os.path.join(directory, "host-setup.txt")
    if os.path.exists(setup):
        with open(setup) as f:
            md += ["## Host setup", "", "```", f.read().rstrip(), "```", ""]
    results = {"title": title, "sweeps": []}
    if source is not None:
        results["source"] = source
    sections = []
    summaries = {}
    for name, path in sweep_files(directory):
        steps, summary = split_sweep(load_jsonl(path))
        stem = name[: -len(".jsonl")]
        summaries[stem] = summary
        results["sweeps"].append({"name": stem, "summary": summary, "steps": steps})
        with open(os.path.join(directory, stem + ".svg"), "w") as f:
            f.write(svg_plot(steps, stem))
        sections += [f"## {stem}", "", f"![{stem}]({stem}.svg)", "", table(steps), ""]
        if any(s["generator_bound"] for s in steps):
            sections += ["GEN-BOUND: a generator fell behind its schedule; that step measured the generator, not the"
                         " DUT, and the sweep stopped there.", ""]
        if any(s.get("budget_bound") for s in steps):
            sections += ["BUDGET-BOUND: some messages waited because their connection already had its budget in"
                         " flight (rate share x (baseline RTT + window)): the latency there may be the budget's, not"
                         " receiver saturation.", ""]
    issues = problems(directory, summaries)
    head = [f"# {title}", ""]
    if issues:
        head = [f"# INCOMPLETE: {title}", "", "These results are incomplete; what ran is below.", "",
                *[f"- {i}" for i in issues], ""]
    md = head + md + sections
    md += ["Notes: latency is receive time minus scheduled send time (open loop, no coordinated omission); not echoed % is"
           " measured messages not echoed by the end of the drain, never-sent ones included (sending stops at the end"
           " of the window); sent pps counts messages accepted by the generator stack, not NIC transmissions;"
           " received pps counts echoes arriving within the window, including warmup echoes; baseline RTT is the"
           " first step's median echo time minus actual send time (TCP/ws); no TLS (ws:// only)."]
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
