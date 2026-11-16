# Reading a benchmark report

A sweep writes one JSON line per step, as on [Running a sweep on one machine](local.md). `switch-bench/report.py` turns a directory of sweeps into a report you can read: a table and a plot per sweep, with flags that say which rows to trust. This page runs it, walks through one row column by column, and explains the flags and what makes a report incomplete.

## Running report.py

```sh
python3 switch-bench/report.py RESULTS_DIR [--title T]
```

It needs only Python's standard library. Every `*.jsonl` file in `RESULTS_DIR` is one sweep, named after the file. The script writes three things into the same directory:

- `report.md`: one section per sweep, with its plot and step table, then notes defining each column,
- `results.json`: the same data in machine-readable form,
- `<sweep>.svg`: latency percentiles and the not-echoed percentage against the offered rate.

It prints `wrote RESULTS_DIR/report.md` and exits 0 when the results are complete, 1 when they are not (see [Incomplete reports](#incomplete-reports)).

A few optional files in the directory add to the report. The AWS runner writes them; for a local run they are absent and the report works without them. `expected.json` lists every sweep the run meant to produce, `source.json` records the commit, `host-setup.txt` holds the host verification output, and `<sweep>.placement.json` records which CPU every thread ran on.

## A row, column by column

Each sweep's table has one row per step, sorted by offered rate:

```text
| offered pps | sent pps | received pps | not echoed % | p50 µs | p99 µs | p99.9 µs | max µs | flags        |
       |           |             |              |           \______ latency percentiles ______/      |
       |           |             |              |                                                     |
  rate asked   messages the    echoes that   measured messages not echoed            which rows to trust
  for          generators'     arrived in    by the end of the drain,
               stacks accepted the window    never-sent ones included
```

Offered pps is the rate the step asked for, summed over the workers. Sent pps is the number of messages the generators' network stacks accepted during the measurement window, divided by the window. It counts what the generator handed to its stack, not what the NIC transmitted. Received pps counts echoes that arrived within the window, including echoes of warmup messages.

Not echoed % is the share of measured messages that had not come back when the drain ended. Messages that were due but never sent count as not echoed.

The latency columns are percentiles of receive time minus scheduled send time, in microseconds, over every echoed measured message. They come from one histogram merged across all workers; percentiles are never averaged. A step with no echo at all shows `-` in these columns and `NO ECHO` in the flags, and the plot marks it with an x.

A healthy receiver shows sent and received close to offered, nothing lost, and latencies that stay flat as the rate rises. Saturation looks like latency rising sharply, then the not-echoed share rising, and on a sweep where the generator could not keep up, a `SENDER-LATE` row after which the sweep stops.

## Flags

| Flag | Meaning |
|---|---|
| `-` | Nothing to note. |
| `SENDER-LATE` | A sender fell behind its timetable in this step: it still owed messages at the end of its window, or its send lag had a 99th percentile above 20 µs. The sweep stopped after this step. The flag does not say why; pushback from the receiver can also delay sends. |
| `BUDGET-BOUND` | Some measured message waited because its connection already had its budget in flight (`--budget-us`). The latency in this row may be the budget's, not the receiver's. |
| `NO ECHO` | No measured message came back. |
| `CARRYOVER` | Only on junction-backend TCP and WebSocket sweeps without a budget, on steps after the first step with loss or a sender-late step. The daemon may still hold queued work from earlier steps, so these rows are not comparable with others. Budgeted sweeps are never marked. |

A sweep whose CPU placement check failed has `— INVALID` in its section heading and a list of the placement errors. Only the AWS runner records placement; [Benchmarking on AWS](aws.md#what-makes-a-sweep-invalid) explains the checks. Its measurements stay in the table and plot.

## Incomplete reports

When the results are incomplete, `report.md` starts with `# INCOMPLETE:` and a list of reasons, still followed by everything that ran, and `report.py` exits 1 and prints each reason on stderr prefixed `incomplete:`. The reasons are:

- a sweep listed in `expected.json` has no file (`<sweep>: missing`), or the directory has no sweeps at all,
- a sweep has no summary line, so it did not finish,
- a sweep's summary says it did not complete, with the coordinator's error,
- a sweep stopped because a sender fell behind,
- the placement check invalidated a sweep.

The sweep's own exit status follows the same rule for one sweep: `switch-bench sweep` exits 0 only when it completed and no sender fell behind.

## Where to go next

[Benchmarking on AWS](aws.md) produces reports like this on real ENA hardware, with the placement checks filled in.
