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

Offered pps is the rate the step asked for, summed over the generators. Sent pps is the number of messages the generators' network stacks accepted during the measurement window, divided by the window; it counts what the generator handed to its stack, not what left the card. Received pps counts echoes that arrived within the window.

Not echoed % is the share of measured messages that had not come back when the drain ended. Messages that were due but never sent count as not echoed.

The latency columns (p50, p99, p99.9, max) are receive time minus scheduled send time, over every echoed measured message, from one histogram merged across all generators. A step with no echo at all shows `-` in these columns.

Service RTT is the same round trip measured from the actual send time instead of the scheduled one, for TCP and WebSocket; UDP shows `-`. When it is close to the latency, as in both rows above, messages left on time and the delay was in the network and the server. Send lag is how late messages left compared with their schedule; above 20 µs at the 99th percentile the step is sender-late.

The two packet columns divide the packets the generators' own ports sent by the messages they sent, and the packets received by the echoes. They count every packet on those ports, ACKs and ARP included, and say nothing about the server's card. In the second row each received packet carried more than one echo, so the ratio fell below 1. On the junction backend the generators have no port of their own and these columns show `-`.

The second row is budget-bound: some messages waited because their connection already had its budget in flight, so part of that latency is the budget's.

## Flags

| Flag | Meaning |
|---|---|
| `-` | Nothing to note |
| `SENDER-LATE` | A generator was behind schedule: its send lag p99 was above 20 µs. The generator, not the server, set the limit, and the sweep stopped after this step |
| `BUDGET-BOUND` | Some measured message waited because its connection already had its budget in flight (`--budget-us`) |
| `NO ECHO` | No measured message came back |
| `CARRYOVER` | Junction backend, TCP or WebSocket without a budget: a step after the first step with loss or the first sender-late step. The daemon may still hold queued work from earlier steps, so the row is not comparable with others |

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
| `gen_tx_packets`, `gen_rx_packets` | Packets the generators' ports sent and received during the step; `null` on the junction backend |
| `budget_bound`, `generator_bound` | The `BUDGET-BOUND` and `SENDER-LATE` flags |
| `baseline_rtt_ns`, `budget_per_connection` | With `--budget-us`: the first step's median service round trip, and each generator's budget per connection |
| `workers` | Per generator: `behind_schedule`; `backlog_at_end`, messages due and not sent when sending stopped; `stops`, send batches ended by a refusal for every sender; `last_send_ns` and `last_echo_ns`, its last send and last echo after its step start |

The last line is the summary: `steps`, `complete`, `generator_bound`, `behind_workers` (the generators behind schedule at the step that stopped the sweep) and `error`.
