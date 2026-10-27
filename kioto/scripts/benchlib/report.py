from __future__ import annotations

import csv
import html
import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Iterable

from .models import CSV_HEADER, PointResult, SweepConfig


COLORS = {
    "mio": "#1f77b4",
    "tokio": "#d62728",
}
FINAL_ARTIFACT_NAMES = ("sweep_config.json", "summary.json", "summary.csv", "report.html")


def clear_artifacts(output_dir: Path) -> None:
    for name in FINAL_ARTIFACT_NAMES:
        (output_dir / name).unlink(missing_ok=True)


def write_artifacts(config: SweepConfig, points: list[PointResult]) -> None:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    suffix = f".tmp-{os.getpid()}"
    paths = [
        (config.output_dir / f".sweep_config.json{suffix}", config.output_dir / "sweep_config.json"),
        (config.output_dir / f".summary.json{suffix}", config.output_dir / "summary.json"),
        (config.output_dir / f".summary.csv{suffix}", config.output_dir / "summary.csv"),
        (config.output_dir / f".report.html{suffix}", config.output_dir / "report.html"),
    ]
    temp_paths = [temp_path for temp_path, _target_path in paths]
    try:
        write_config_to_path(paths[0][0], config)
        write_summary_json(paths[1][0], points)
        write_summary_csv(paths[2][0], points)
        write_report(paths[3][0], config, points)
        for temp_path, target_path in paths:
            temp_path.replace(target_path)
    finally:
        for temp_path in temp_paths:
            temp_path.unlink(missing_ok=True)


def write_config(config: SweepConfig) -> None:
    write_config_to_path(config.output_dir / "sweep_config.json", config)


def write_config_to_path(path: Path, config: SweepConfig) -> None:
    path.write_text(json.dumps(config.to_json_dict(), indent=2) + "\n")


def write_summary_json(path: Path, points: list[PointResult]) -> None:
    path.write_text(json.dumps([point.to_json_dict() for point in points], indent=2) + "\n")


def write_summary_csv(path: Path, points: list[PointResult]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADER)
        for point in points:
            writer.writerow(point.csv_row())


def load_points(path: Path) -> list[dict[str, object]]:
    return json.loads(path.read_text())


def write_report(path: Path, config: SweepConfig, points: list[PointResult]) -> None:
    html_text = render_report(config, points)
    path.write_text(html_text)


def render_report(config: SweepConfig, points: list[PointResult]) -> str:
    first_fallovers = first_fallover_by_variant(points)
    rows = "\n".join(render_row(point) for point in points)

    p999_chart = line_chart(
        title="p99.9 RTT vs Offered Load",
        y_label="p99.9 RTT (ms, log scale)",
        series=series_by_variant(points, "p999_ms"),
        log_y=True,
        threshold_y=config.fallover_p999_ms,
    )
    p99_chart = line_chart(
        title="p99 RTT vs Offered Load",
        y_label="p99 RTT (ms, log scale)",
        series=series_by_variant(points, "p99_ms"),
        log_y=True,
    )
    achieved_chart = line_chart(
        title="Achieved Throughput vs Offered Load",
        y_label="received msg/s",
        series=series_by_variant(points, "achieved"),
        log_y=False,
    )
    offered_chart = line_chart(
        title="Sender Offered Ratio vs Offered Load",
        y_label="sent / expected",
        series=series_by_variant(points, "offered_ratio"),
        log_y=False,
        threshold_y=config.fallover_min_offered_ratio,
        threshold_label=f"min {config.fallover_min_offered_ratio:g}",
    )
    write_chart = line_chart(
        title="Sender Written Byte Ratio vs Offered Load",
        y_label="written bytes / expected bytes",
        series=series_by_variant(points, "write_byte_ratio"),
        log_y=False,
        threshold_y=config.fallover_min_offered_ratio,
        threshold_label=f"min {config.fallover_min_offered_ratio:g}",
    )
    loss_chart = line_chart(
        title="Lost Echoes vs Offered Load",
        y_label="lost echoes",
        series=series_by_variant(points, "lost"),
        log_y=False,
    )

    fallover_items = "\n".join(
        f"<li><b>{html.escape(variant)}</b>: {html.escape(text)}</li>"
        for variant, text in first_fallovers.items()
    )
    if not fallover_items:
        fallover_items = "<li>No fallover by configured threshold.</li>"

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{html.escape(config.report_title)}</title>
  <style>
    :root {{
      color-scheme: light;
      --ink: #17202a;
      --muted: #667085;
      --grid: #d0d5dd;
      --panel: #f7f8fa;
      --border: #d9dee7;
    }}
    body {{
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--ink);
      margin: 32px;
      line-height: 1.35;
    }}
    h1, h2 {{
      margin: 0 0 12px;
    }}
    section {{
      margin: 28px 0;
    }}
    .meta, .note {{
      color: var(--muted);
      max-width: 920px;
    }}
    .charts {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(520px, 1fr));
      gap: 20px;
      align-items: start;
    }}
    .chart {{
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 14px;
      background: #fff;
    }}
    table {{
      border-collapse: collapse;
      font-size: 13px;
      width: 100%;
    }}
    th, td {{
      border-bottom: 1px solid var(--border);
      padding: 7px 8px;
      text-align: right;
      white-space: nowrap;
    }}
    th:first-child, td:first-child, td:last-child {{
      text-align: left;
    }}
    th {{
      color: #344054;
      background: var(--panel);
      position: sticky;
      top: 0;
    }}
    .fallover {{
      background: #fff4ed;
    }}
    .legend {{
      font-size: 12px;
      color: var(--muted);
    }}
  </style>
</head>
<body>
  <h1>{html.escape(config.report_title)}</h1>
  <p class="meta">
    Sender variant: <b>{html.escape(config.sender_variant)}</b>.
    Sender processes: <b>{config.sender_procs}</b>.
    Connections per sender: <b>{config.connections_per_sender}</b>.
    Max pending bytes: <b>{config.max_pending_bytes}</b>.
    Tokio uses current-thread runtimes.
    Sender seed: <b>{config.seed}</b>.
    Tail percentiles shown here are worst per-sender summaries, not merged raw HDR histograms.
  </p>

  <section>
    <h2>Fallover</h2>
    <ul>{fallover_items}</ul>
  </section>

  <section class="charts">
    <div class="chart">{p999_chart}</div>
    <div class="chart">{p99_chart}</div>
    <div class="chart">{achieved_chart}</div>
    <div class="chart">{offered_chart}</div>
    <div class="chart">{write_chart}</div>
    <div class="chart">{loss_chart}</div>
  </section>

  <section>
    <h2>Summary</h2>
    <table>
      <thead>
        <tr>
          <th>victim</th><th>rate</th><th>ok</th><th>probe conns</th><th>sent</th><th>expected</th>
          <th>offered</th><th>written bytes</th><th>expected bytes</th><th>write ratio</th>
          <th>received</th><th>lost</th><th>achieved</th><th>p99 ms</th><th>p99.9 ms</th>
          <th>p99.99 ms</th><th>max ms</th><th>errors</th><th>pending limit</th><th>fallover</th>
          <th>reason</th>
        </tr>
      </thead>
      <tbody>{rows}</tbody>
    </table>
  </section>
</body>
</html>
"""


def render_row(point: PointResult) -> str:
    css = " class=\"fallover\"" if point.fallover else ""
    fallover = "yes" if point.fallover else ""
    return (
        f"<tr{css}>"
        f"<td>{html.escape(point.variant)}</td>"
        f"<td>{point.rate_total}</td>"
        f"<td>{point.ok_procs}/{point.sender_procs}</td>"
        f"<td>{point.probe_connections}</td>"
        f"<td>{point.sent}</td>"
        f"<td>{point.expected_sent:.0f}</td>"
        f"<td>{point.offered_ratio:.3f}</td>"
        f"<td>{point.write_bytes}</td>"
        f"<td>{point.expected_write_bytes:.0f}</td>"
        f"<td>{point.write_byte_ratio:.3f}</td>"
        f"<td>{point.received}</td>"
        f"<td>{point.lost}</td>"
        f"<td>{point.achieved:.0f}</td>"
        f"<td>{fmt(point.p99_ms)}</td>"
        f"<td>{fmt(point.p999_ms)}</td>"
        f"<td>{fmt(point.p9999_ms)}</td>"
        f"<td>{fmt(point.max_ms)}</td>"
        f"<td>{point.errors}</td>"
        f"<td>{point.pending_limit_errors}</td>"
        f"<td>{fallover}</td>"
        f"<td>{html.escape(point.fallover_reason)}</td>"
        "</tr>"
    )


def first_fallover_by_variant(points: Iterable[PointResult]) -> dict[str, str]:
    found: dict[str, str] = {}
    for point in sorted(points, key=lambda p: (p.variant, p.rate_total)):
        if point.variant not in found and point.fallover:
            found[point.variant] = f"{point.rate_total:,} msg/s: {point.fallover_reason}"
    return found


def series_by_variant(points: list[PointResult], field: str) -> dict[str, list[tuple[float, float]]]:
    series: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for point in points:
        value = getattr(point, field)
        if value is None:
            continue
        series[point.variant].append((float(point.rate_total), float(value)))
    return {variant: sorted(values) for variant, values in series.items()}


def line_chart(
    title: str,
    y_label: str,
    series: dict[str, list[tuple[float, float]]],
    log_y: bool,
    threshold_y: float | None = None,
    threshold_label: str | None = None,
) -> str:
    width = 680
    height = 360
    left = 72
    right = 24
    top = 42
    bottom = 54
    plot_w = width - left - right
    plot_h = height - top - bottom

    all_points = [point for values in series.values() for point in values]
    if not all_points:
        return f"<h3>{html.escape(title)}</h3><p class=\"note\">No data.</p>"

    xs = [x for x, _y in all_points]
    ys = [y for _x, y in all_points]
    x_min, x_max = min(xs), max(xs)
    if x_min == x_max:
        x_min *= 0.95
        x_max *= 1.05

    if threshold_y is not None:
        ys.append(threshold_y)

    if log_y:
        positive = [max(y, 0.001) for y in ys if y > 0]
        y_min = min(positive) if positive else 0.001
        y_max = max(positive) if positive else 1.0
        y_min = 10 ** floor_log10(y_min)
        y_max = 10 ** ceil_log10(y_max)
        if y_min == y_max:
            y_min /= 10.0
            y_max *= 10.0
    else:
        y_min = 0.0
        y_max = max(ys) if ys else 1.0
        if y_max <= 0:
            y_max = 1.0
        y_max *= 1.10

    def sx(x: float) -> float:
        return left + ((x - x_min) / (x_max - x_min)) * plot_w

    def sy(y: float) -> float:
        if log_y:
            y = max(y, 0.001)
            ly = (log10(y) - log10(y_min)) / (log10(y_max) - log10(y_min))
        else:
            ly = (y - y_min) / (y_max - y_min)
        return top + (1.0 - ly) * plot_h

    y_ticks = log_ticks(y_min, y_max) if log_y else linear_ticks(y_min, y_max, 5)
    x_ticks = linear_ticks(x_min, x_max, 5)

    parts = [
        f"<h3>{html.escape(title)}</h3>",
        f"<svg viewBox=\"0 0 {width} {height}\" width=\"100%\" role=\"img\" aria-label=\"{html.escape(title)}\">",
        f"<rect x=\"0\" y=\"0\" width=\"{width}\" height=\"{height}\" fill=\"white\" />",
    ]

    for tick in y_ticks:
        y = sy(tick)
        parts.append(f"<line x1=\"{left}\" y1=\"{y:.1f}\" x2=\"{width-right}\" y2=\"{y:.1f}\" stroke=\"#d0d5dd\" stroke-width=\"1\" />")
        parts.append(f"<text x=\"{left-8}\" y=\"{y+4:.1f}\" text-anchor=\"end\" font-size=\"11\" fill=\"#667085\">{fmt_tick(tick)}</text>")

    for tick in x_ticks:
        x = sx(tick)
        parts.append(f"<line x1=\"{x:.1f}\" y1=\"{top}\" x2=\"{x:.1f}\" y2=\"{height-bottom}\" stroke=\"#eef1f5\" stroke-width=\"1\" />")
        parts.append(f"<text x=\"{x:.1f}\" y=\"{height-24}\" text-anchor=\"middle\" font-size=\"11\" fill=\"#667085\">{fmt_rate(tick)}</text>")

    parts.append(f"<line x1=\"{left}\" y1=\"{height-bottom}\" x2=\"{width-right}\" y2=\"{height-bottom}\" stroke=\"#344054\" />")
    parts.append(f"<line x1=\"{left}\" y1=\"{top}\" x2=\"{left}\" y2=\"{height-bottom}\" stroke=\"#344054\" />")
    parts.append(f"<text x=\"{left + plot_w/2:.1f}\" y=\"{height-4}\" text-anchor=\"middle\" font-size=\"12\" fill=\"#344054\">offered msg/s</text>")
    parts.append(f"<text transform=\"translate(16 {top + plot_h/2:.1f}) rotate(-90)\" text-anchor=\"middle\" font-size=\"12\" fill=\"#344054\">{html.escape(y_label)}</text>")

    if threshold_y is not None and y_min <= threshold_y <= y_max:
        y = sy(threshold_y)
        parts.append(f"<line x1=\"{left}\" y1=\"{y:.1f}\" x2=\"{width-right}\" y2=\"{y:.1f}\" stroke=\"#b42318\" stroke-dasharray=\"5 4\" />")
        label = threshold_label or f"fallover {threshold_y:g}ms"
        parts.append(f"<text x=\"{width-right-4}\" y=\"{y-6:.1f}\" text-anchor=\"end\" font-size=\"11\" fill=\"#b42318\">{html.escape(label)}</text>")

    legend_x = left
    legend_y = 20
    for variant, points in series.items():
        color = COLORS.get(variant, "#344054")
        path = " ".join(
            ("M" if idx == 0 else "L") + f"{sx(x):.1f},{sy(y):.1f}"
            for idx, (x, y) in enumerate(points)
        )
        parts.append(f"<path d=\"{path}\" fill=\"none\" stroke=\"{color}\" stroke-width=\"2.4\" />")
        for x, y in points:
            parts.append(f"<circle cx=\"{sx(x):.1f}\" cy=\"{sy(y):.1f}\" r=\"3\" fill=\"{color}\" />")
        parts.append(f"<circle cx=\"{legend_x}\" cy=\"{legend_y-4}\" r=\"4\" fill=\"{color}\" />")
        parts.append(f"<text x=\"{legend_x+10}\" y=\"{legend_y}\" font-size=\"12\" fill=\"#344054\">{html.escape(variant)}</text>")
        legend_x += 78

    parts.append("</svg>")
    return "\n".join(parts)


def fmt(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.3f}"


def fmt_rate(value: float) -> str:
    if value >= 1000:
        return f"{value/1000:.0f}k"
    return f"{value:.0f}"


def fmt_tick(value: float) -> str:
    if value >= 1000:
        return f"{value/1000:.0f}k"
    if value >= 10:
        return f"{value:.0f}"
    if value >= 1:
        return f"{value:.1f}".rstrip("0").rstrip(".")
    return f"{value:.3f}".rstrip("0").rstrip(".")


def linear_ticks(min_value: float, max_value: float, count: int) -> list[float]:
    if count <= 1 or min_value == max_value:
        return [min_value]
    step = (max_value - min_value) / (count - 1)
    return [min_value + idx * step for idx in range(count)]


def log_ticks(min_value: float, max_value: float) -> list[float]:
    start = floor_log10(min_value)
    end = ceil_log10(max_value)
    return [10**exp for exp in range(start, end + 1)]


def log10(value: float) -> float:
    import math

    return math.log10(value)


def floor_log10(value: float) -> int:
    import math

    return math.floor(math.log10(value))


def ceil_log10(value: float) -> int:
    import math

    return math.ceil(math.log10(value))
