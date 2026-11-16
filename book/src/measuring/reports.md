# Reading a benchmark report

Run `report.py` on the directory the sweeps wrote. It turns each sweep's JSON lines into a table and a plot, and flags the rows not to trust. This page runs it, reads a real report row by row, and explains the flags and what makes a report incomplete.

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

## A real report

This is the TCP section of the report from one `make -C conduit docker-bench-smoke` run, made with `python3 switch-bench/report.py switch-bench/out/smoke --title 'Local smoke (tooling validation over net_tap; not a measurement)'` after the run stopped at its overload check, as [Running a sweep on one machine](local.md#the-smoke-run) describes. It ran over tap devices in a Docker VM, so it is tooling validation, not a measurement. The plot and table are exactly as `report.py` wrote them:

![tcp-switch](tcp-switch.svg)

{{#include smoke-report.md:24:40}}

Each row is one step, sorted by offered rate. Take the first row, offered 1000.

Offered pps is the rate the step asked for, summed over the workers: 1000 messages per second. Sent pps is the number of messages the generators' network stacks accepted during the measurement window, divided by the window. It counts what the generator handed to its stack, not what the NIC transmitted. Received pps counts echoes that arrived within the window, including echoes of warmup messages. Here all three are 1000.

Not echoed % is the share of measured messages that had not come back when the drain ended; messages that were due but never sent count as not echoed. Here it is 0.

The four latency columns are percentiles of receive time minus scheduled send time, in microseconds, over every echoed measured message: half the messages came back within 15.3 µs, 99% within 19.1 µs. They come from one histogram merged across all workers; percentiles are never averaged. A step with no echo at all shows `-` in these columns and `NO ECHO` in the flags, and the plot marks it with an x.

Down the table, sent and received follow offered and the median stays near 15 µs, while two rows are `BUDGET-BOUND`: some messages there waited for their connection's budget. The last row, at 128000 messages per second, is where a generator fell behind its timetable: the median jumps to 81.4 µs, a few messages are not echoed, and the row is `SENDER-LATE`, so the sweep stopped there. A saturating receiver looks similar, with latency rising sharply and then the not-echoed share rising; the `SENDER-LATE` flag says that this time the generator, or backpressure on it, set the limit.

## Flags

| Flag | Meaning |
|---|---|
| `-` | Nothing to note. |
| `SENDER-LATE` | A generator fell behind in this step: it still had messages due that it had not sent (not counting those a blocked connection was holding), or the 99th percentile of its senders' send lag was above 20 µs. The sweep stopped after this step. The flag does not say whether the generator or receiver backpressure was the limit. |
| `BUDGET-BOUND` | Some measured message waited because its connection already had its budget in flight (`--budget-us`). The latency in this row may be the budget's, not the receiver's. |
| `NO ECHO` | No measured message came back. |
| `CARRYOVER` | Only on junction-backend TCP and WebSocket sweeps without a budget, on steps after the first step with loss or a sender-late step. The daemon may still hold queued work from earlier steps, so these rows are not comparable with others. Budgeted sweeps are never marked. |

A sweep whose CPU placement check failed has `— INVALID` in its section heading and a list of the placement errors. Only the AWS runner records placement; [Benchmarking on AWS](aws.md#what-makes-a-sweep-invalid) explains the checks. Its measurements stay in the table and plot.

## Incomplete reports

When the results are incomplete, `report.md` starts with `# INCOMPLETE:` and a list of reasons, still followed by everything that ran, and `report.py` exits 1 and prints each reason on stderr prefixed `incomplete:`. The run above printed four:

```text
incomplete: overload-check: sender fell behind schedule; receiver capacity is not isolated
incomplete: tcp-switch: sender fell behind schedule; receiver capacity is not isolated
incomplete: udp-switch: sender fell behind schedule; receiver capacity is not isolated
incomplete: ws-switch: sender fell behind schedule; receiver capacity is not isolated
```

The reasons are:

- a sweep listed in `expected.json` has no file (`<sweep>: missing`), or the directory has no sweeps at all,
- a sweep has no summary line, so it did not finish,
- a sweep's summary says it did not complete, with the coordinator's error,
- a sweep stopped because a sender fell behind,
- the placement check invalidated a sweep.

The sweep's own exit status follows the same rule for one sweep: `switch-bench sweep` exits 0 only when it completed and no sender fell behind.

## Where to go next

[Benchmarking on AWS](aws.md) produces reports like this on real ENA hardware, with the placement checks filled in.
