# Reading a benchmark report

`report.py` turns the JSON lines that `switch-bench sweep` prints into a table and a plot per sweep. The smoke run and the AWS runner call it for you. To run it on a directory yourself:

```sh
python3 switch-bench/report.py RESULTS_DIR [--title T]
```

It needs only Python's standard library. Every `*.jsonl` file in the directory is one sweep, named after the file. It writes into the same directory:

- `report.md`: one section per sweep, with its plot and step table;
- `results.json`: the same data, machine-readable;
- `<sweep>.svg`: latency percentiles and the share not echoed, against the offered rate.

It prints `wrote RESULTS_DIR/report.md` and exits 0 when the results are complete, 1 when they are not.

## A step table

These rows come from the TCP sweep of the [smoke report](smoke-report/smoke-report.md) of one smoke run over tap devices ([Running the benchmark on one machine](local.md)). The report has more columns: p99.9 and max latency, service RTT p99 and send lag p50 are left out here.

| offered pps | sent pps | received pps | not echoed % | p50 µs | p99 µs | service RTT p50 µs | send lag p99 µs | gen tx packets per sent message | gen rx packets per echo | flags |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1000 | 1000 | 1000 | 0.000 | 19.5 | 54.0 | 19.2 | 4.4 | 1.01 | 1.01 | - |
| 128000 | 128000 | 127997 | 0.000 | 47.4 | 78.8 | 46.6 | 13.8 | 1.00 | 0.78 | BUDGET-BOUND |

Each row is one step, sorted by offered rate.

Offered pps is the rate the step scheduled, summed over the generators. Sent pps counts the messages the generators handed to their network stacks during the measurement window, not what left the card. Received pps counts echoes that arrived within the window.

Not echoed % is the share of measured messages that had not come back when the drain ended, including messages that were due but never sent.

The latency columns (p50, p99, p99.9, max) are receive time minus scheduled send time, over every echoed measured message of all generators. A step with no echo at all shows `-` in these columns.

Service RTT is the round trip measured from the actual send time instead of the scheduled one, for TCP and WebSocket; UDP shows `-`. When it is close to the latency, as in both rows above, messages left on time and the delay was in the network and the server.

Send lag is how late messages left compared with their schedule, merged across the generators. The step is sender-late when any one generator's own send lag p99 is above 20 µs, which the merged column can hide.

The two packet columns are the packets the generators' own ports sent per message and received per echo, ACKs and ARP included. A ratio below 1, as in the second row, means a packet carried more than one echo. Generators that share a card through the daemon show `-`.

## Flags

| Flag | Meaning |
|---|---|
| `-` | Nothing to note |
| `SENDER-LATE` | A generator was behind schedule: its own send lag p99 was above 20 µs. The generator, not the server, set the limit, and the sweep stopped after this step |
| `BUDGET-BOUND` | Some measured message waited because its connection already had its budget in flight (`--budget-us`), so part of the latency is the budget's |
| `NO ECHO` | No measured message came back |
| `CARRYOVER` | Sweeps through the daemon (files named `junction-...`), TCP or WebSocket without a budget: a step after the first step with loss or the first sender-late step. The daemon may still hold queued work from earlier steps, so the row is not comparable with others |

A sweep whose CPU placement check failed has `— INVALID` after its section heading, followed by the placement errors. Only the AWS runner records placement; [Benchmarking on AWS](aws.md#invalid-sweeps) explains it. Its measurements stay in the table and plot.

## Incomplete reports

When the results are incomplete, `report.md` starts with `# INCOMPLETE:` and the reasons, followed by everything that ran, and `report.py` prints each reason on stderr after `incomplete:` and exits 1. The reasons are:

| Reason | Meaning |
|---|---|
| `<sweep>: missing` | The run's `expected.json` lists a sweep that has no file |
| `no sweep results` | The directory has no `*.jsonl` file and no `expected.json` |
| `<sweep>: no summary (the sweep did not finish)` | The file has no summary line |
| `<sweep>: incomplete after N steps: <error>` | The coordinator failed; the error says why |
| `<sweep>: sender behind schedule: worker(s) [...]` | The sweep stopped at a sender-late step |
| an INVALID placement | The placement check failed for that sweep |

## The JSON lines

`results.json` keeps every step line whole, and the `.jsonl` files hold them as `switch-bench sweep` printed them. A step line has these fields:

| Field | Meaning |
|---|---|
| `offered_pps`, `sent_pps`, `received_pps`, `drop_fraction` | The rates and loss of the table's first four columns |
| `latency_ns` | `p50`, `p99`, `p999` and `max` of receive time minus scheduled send time; `null` when nothing was echoed |
| `service_rtt_ns` | The same percentiles of echo time minus actual send time |
| `send_lag_ns` | The same percentiles of actual minus scheduled send time |
| `sent`, `echoed` | Messages sent over the whole step, warmup included, and how many of them were echoed by the end of the drain |
| `gen_tx_packets`, `gen_rx_packets` | Packets the generators' ports sent and received during the step; `null` when a generator shares a card through the daemon |
| `budget_bound`, `generator_bound` | The `BUDGET-BOUND` and `SENDER-LATE` flags |
| `baseline_rtt_ns`, `budget_per_connection` | With `--budget-us`: the first step's median service round trip, and each generator's budget per connection |
| `workers` | Per generator: `behind_schedule`, and the diagnostic counters `backlog_at_end`, `stops`, `last_send_ns` and `last_echo_ns` |

The last line is the summary: `steps`, `complete`, `generator_bound`, `behind_workers` (the generators behind schedule at the step that stopped the sweep) and `error`.
