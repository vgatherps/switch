//! The sweep coordinator: spawns G generator workers (`zhida-bench gen`, each its own Stack, Port
//! and core), runs every rate step on all of them at a common start time, merges their results,
//! judges the step and picks the next rate. No DPDK here.
//!
//! Output: one JSON line per step, then always one `summary` line, also when a worker fails
//! (`complete: false` with the error; the steps already printed stand).
//!
//! TCP/ws outstanding budget (`budget_window_us`): the first step (at `rate_min`) runs without
//! one and its median service round trip (echo time - actual send time) is the baseline; every
//! later step lets each connection have `budget_per_connection` messages in flight, so the
//! normal round trip is allowed and only backlog beyond it waits. A step in which any measured
//! message waited on the budget is `budget_bound`.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::process::CommandExt;
use std::process::{Child, ChildStdin, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::hist::Histogram;
use crate::host::{EthStats, TurnSums};
use crate::load::{self, StepResult};
use crate::sweep::{Limits, Next, Outcome, Sweep};

#[derive(Debug, Clone)]
pub struct Args {
    pub proto: &'static str,
    pub gens: u32,
    /// `sh -c` command of worker `{i}` with `{senders}` senders.
    pub gen_cmd: String,
    pub senders: u32,
    pub size: usize,
    pub limits: Limits,
    pub step_ms: u64,
    pub warmup_ms: u64,
    pub drain_ms: u64,
    pub lag_limit_us: u64,
    /// How far ahead of sending a step's command its start time lies.
    pub lead_ms: u64,
    /// TCP/ws: the outstanding budget's window beyond the baseline round trip; `None`: no budget.
    pub budget_window_us: Option<u64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Quantiles {
    pub p50: u64,
    pub p90: u64,
    pub p99: u64,
    pub p999: u64,
    pub max: u64,
    pub mean: f64,
}

impl Quantiles {
    pub fn of(h: &Histogram) -> Option<Quantiles> {
        Some(Quantiles {
            p50: h.quantile(0.5)?,
            p90: h.quantile(0.9)?,
            p99: h.quantile(0.99)?,
            p999: h.quantile(0.999)?,
            max: h.max(),
            mean: h.mean()?,
        })
    }
}

/// Messages a connection may have in flight: its share of the rate over the baseline round trip
/// plus the window, at least one.
pub fn budget_per_connection(
    rate_pps: u64,
    conns: u32,
    baseline_rtt_ns: u64,
    window_ns: u64,
) -> u32 {
    let per_conn = rate_pps as f64 / f64::from(conns);
    ((per_conn * (baseline_rtt_ns + window_ns) as f64 / 1e9).ceil() as u32).max(1)
}

#[derive(Debug, Clone, Serialize)]
pub struct GenReport {
    pub generator: u32,
    pub offered_pps: f64,
    pub sent_pps: f64,
    pub received_pps: f64,
    pub received: u64,
    pub measured: u64,
    pub unsent: u64,
    pub send_lag_ns: Option<Quantiles>,
    pub backlog_at_end: u64,
    /// Messages owed by connections the receiver kept full (flow control) or at their budget,
    /// and how long owed messages waited: the receiver's doing, not the generator's.
    pub owed_at_end: u64,
    pub owed_wait_ns: Option<Quantiles>,
    pub service_rtt_ns: Option<Quantiles>,
    pub budget: Option<u32>,
    pub budget_waits: u64,
    pub in_flight_mean: Option<f64>,
    pub in_flight_max: u64,
    pub in_flight_at_start: u64,
    pub in_flight_after: u64,
    pub generator_bound: bool,
    /// The worker's loop: wall time per round, and Runtime task polls per round.
    pub ns_per_round: u64,
    pub tasks_per_round: f64,
    pub turns: TurnSums,
    pub eth: EthStats,
    pub rx_dropped_full: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StepReport {
    pub kind: &'static str,
    pub proto: &'static str,
    pub step: u32,
    pub gens: u32,
    pub senders: u32,
    pub size: usize,
    pub rate_pps: u64,
    pub offered_pps: f64,
    /// What actually left the generators, and what came back, within the window.
    pub sent_pps: f64,
    pub received_pps: f64,
    pub window_unix_ns: [u64; 2],
    pub measured: u64,
    pub received: u64,
    /// measured - received; includes `unsent`.
    pub lost: u64,
    /// Measured messages still owed or behind when the window ended: never sent.
    pub unsent: u64,
    pub drop_fraction: f64,
    pub duplicates: u64,
    pub stale: u64,
    pub malformed: u64,
    pub latency_ns: Option<Quantiles>,
    pub send_lag_ns: Option<Quantiles>,
    /// TCP/ws: echo time - actual send time.
    pub service_rtt_ns: Option<Quantiles>,
    /// TCP/ws: messages in flight per connection allowed this step (largest over workers).
    pub budget: Option<u32>,
    /// Measured messages that waited on the budget.
    pub budget_waits: u64,
    /// Some message waited on the budget: a knee here may be the budget's, not the
    /// receiver's (compare `service_rtt_ns` with the baseline).
    pub budget_bound: bool,
    /// A connection's in-flight messages right after each measured send.
    pub in_flight_mean: Option<f64>,
    pub in_flight_max: u64,
    /// Over all connections: left from the step before, and left after this step's settle.
    pub in_flight_at_start: u64,
    pub in_flight_after: u64,
    /// Any generator fell behind its schedule: the step measured the generator, not the DUT.
    pub generator_bound: bool,
    pub good: bool,
    pub per_gen: Vec<GenReport>,
}

/// The last line of every sweep.
#[derive(Debug, Clone, Serialize)]
pub struct Summary {
    pub kind: &'static str,
    pub proto: &'static str,
    pub gens: u32,
    pub senders: u32,
    pub size: usize,
    pub rate_max: u64,
    pub budget_window_us: Option<u64>,
    /// The first step's service round trip (TCP/ws), the budget's baseline (its p50).
    pub baseline_rtt_ns: Option<Quantiles>,
    pub knee_pps: Option<u64>,
    /// The sweep reached `rate_max` without a bad step: the knee is at least `knee_pps`.
    pub knee_lower_bound: bool,
    /// A run at the first rate above the knee waited on the budget: the knee may be the
    /// budget's, not the receiver's (compare its `service_rtt_ns` with the baseline).
    pub first_bad_budget_bound: bool,
    pub last_good_pps: Option<u64>,
    /// "knee", "rate_max", "no_good_step", "generator_bound" or "failed".
    pub stop: &'static str,
    pub steps: u32,
    /// Every step ran and the sweep ended on its own; false: see `error`.
    pub complete: bool,
    pub error: Option<String>,
}

/// Merges the workers' results of one step (`results[g]` from worker g). `good` is left false
/// for the caller's judgement.
pub fn merge(
    args: &Args,
    step: u32,
    rate_pps: u64,
    results: &[StepResult],
) -> Result<StepReport, String> {
    let mut latency = Histogram::new();
    let mut lag = Histogram::new();
    let mut rtt = Histogram::new();
    let mut per_gen = Vec::with_capacity(results.len());
    let window_s = args.step_ms as f64 / 1e3;
    for (g, r) in results.iter().enumerate() {
        if r.step != step {
            return Err(format!(
                "generator {g} answered step {} for step {step}",
                r.step
            ));
        }
        latency.merge(&Histogram::from_sparse(&r.latency)?);
        let gen_lag = Histogram::from_sparse(&r.send_lag)?;
        lag.merge(&gen_lag);
        let gen_rtt = Histogram::from_sparse(&r.service_rtt)?;
        rtt.merge(&gen_rtt);
        let send_lag_ns = Quantiles::of(&gen_lag);
        let lag_bound = send_lag_ns
            .as_ref()
            .is_some_and(|q| q.p99 > args.lag_limit_us * 1000);
        per_gen.push(GenReport {
            generator: g as u32,
            offered_pps: r.offered_pps,
            sent_pps: r.sent_in_window as f64 / window_s,
            received_pps: r.received_in_window as f64 / window_s,
            received: r.received,
            measured: r.measured,
            unsent: r.unsent,
            send_lag_ns,
            backlog_at_end: r.backlog_at_end,
            owed_at_end: r.owed_at_end,
            owed_wait_ns: Quantiles::of(&Histogram::from_sparse(&r.owed_wait)?),
            service_rtt_ns: Quantiles::of(&gen_rtt),
            budget: r.budget,
            budget_waits: r.budget_waits,
            in_flight_mean: (r.in_flight_samples > 0)
                .then(|| r.in_flight_sum as f64 / r.in_flight_samples as f64),
            in_flight_max: r.in_flight_max,
            in_flight_at_start: r.in_flight_at_start,
            in_flight_after: r.in_flight_after,
            ns_per_round: r.ns_per_round,
            tasks_per_round: r.turns.tasks_polled as f64 / r.turns.turns.max(1) as f64,
            generator_bound: r.backlog_at_end > 0 || lag_bound,
            turns: r.turns,
            eth: r.eth,
            rx_dropped_full: r.rx_dropped_full,
        });
    }
    let sum = |f: fn(&StepResult) -> u64| results.iter().map(f).sum::<u64>();
    let (measured, received) = (sum(|r| r.measured), sum(|r| r.received));
    let lost = measured - received;
    let in_flight_samples = sum(|r| r.in_flight_samples);
    let budget_waits = sum(|r| r.budget_waits);
    Ok(StepReport {
        kind: "step",
        proto: args.proto,
        step,
        gens: results.len() as u32,
        senders: args.senders,
        size: args.size,
        rate_pps,
        offered_pps: results.iter().map(|r| r.offered_pps).sum(),
        sent_pps: sum(|r| r.sent_in_window) as f64 / window_s,
        received_pps: sum(|r| r.received_in_window) as f64 / window_s,
        window_unix_ns: [
            results
                .iter()
                .map(|r| r.window_unix_ns[0])
                .min()
                .unwrap_or(0),
            results
                .iter()
                .map(|r| r.window_unix_ns[1])
                .max()
                .unwrap_or(0),
        ],
        measured,
        received,
        lost,
        unsent: sum(|r| r.unsent),
        drop_fraction: if measured == 0 {
            0.0
        } else {
            lost as f64 / measured as f64
        },
        duplicates: sum(|r| r.duplicates),
        stale: sum(|r| r.stale),
        malformed: sum(|r| r.malformed),
        latency_ns: Quantiles::of(&latency),
        send_lag_ns: Quantiles::of(&lag),
        service_rtt_ns: Quantiles::of(&rtt),
        budget: results.iter().filter_map(|r| r.budget).max(),
        budget_waits,
        budget_bound: budget_waits > 0,
        in_flight_mean: (in_flight_samples > 0)
            .then(|| sum(|r| r.in_flight_sum) as f64 / in_flight_samples as f64),
        in_flight_max: results.iter().map(|r| r.in_flight_max).max().unwrap_or(0),
        in_flight_at_start: sum(|r| r.in_flight_at_start),
        in_flight_after: sum(|r| r.in_flight_after),
        generator_bound: per_gen.iter().any(|g| g.generator_bound),
        good: false,
        per_gen,
    })
}

/// Rate `rate` split over `gens` workers: as even as integers allow, worker 0 takes the rest.
pub fn split_rate(rate: u64, gens: u32) -> Vec<u64> {
    let g = u64::from(gens);
    (0..g)
        .map(|i| rate / g + if i == 0 { rate % g } else { 0 })
        .collect()
}

struct Worker {
    child: Child,
    stdin: ChildStdin,
    /// Its stdout's lines, read by a thread so that waiting for one can time out.
    lines: Receiver<std::io::Result<String>>,
}

/// How long a worker may take to open its Port and connect.
const READY_LIMIT: Duration = Duration::from_secs(60);
/// A step's reply may come this long after the step's drain (the worker's settle and its own
/// cutoff check come first).
const REPLY_SLACK: Duration = Duration::from_secs(10);
/// How long a worker may take to exit once its input closes.
const EXIT_LIMIT: Duration = Duration::from_secs(10);

impl Worker {
    fn spawn(cmd: &str) -> Result<Worker, String> {
        // its own process group, so that a kill reaches the worker under the shell wrappers
        let mut child = Command::new("sh")
            .arg("-c")
            .arg(cmd)
            .process_group(0)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|e| format!("spawn {cmd:?}: {e}"))?;
        let stdin = child.stdin.take().expect("piped");
        let stdout = BufReader::new(child.stdout.take().expect("piped"));
        let (send, lines) = mpsc::channel();
        thread::spawn(move || {
            for line in stdout.lines() {
                if send.send(line).is_err() {
                    break;
                }
            }
        });
        Ok(Worker {
            child,
            stdin,
            lines,
        })
    }

    /// Its next line within `limit`.
    fn line(&mut self, limit: Duration) -> Result<String, String> {
        match self.lines.recv_timeout(limit) {
            Ok(Ok(line)) => Ok(line),
            Ok(Err(e)) => Err(format!("generator output: {e}")),
            Err(RecvTimeoutError::Disconnected) => Err("generator exited".into()),
            Err(RecvTimeoutError::Timeout) => Err(format!("generator silent for {limit:?}")),
        }
    }

    /// Waits up to `limit` for it to exit; kills it after.
    fn exit(mut self, limit: Duration) -> Result<ExitStatus, String> {
        drop(self.stdin);
        let deadline = Instant::now() + limit;
        loop {
            if let Some(status) = self.child.try_wait().map_err(|e| e.to_string())? {
                return Ok(status);
            }
            if Instant::now() > deadline {
                kill_group(&mut self.child);
                return Err(format!("did not exit within {limit:?}; killed"));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

/// The running workers; any still running when this drops (a failed sweep) are killed.
struct Workers(Vec<Worker>);

impl Drop for Workers {
    fn drop(&mut self) {
        for w in &mut self.0 {
            if !matches!(w.child.try_wait(), Ok(Some(_))) {
                kill_group(&mut w.child);
            }
        }
    }
}

/// SIGKILL to a worker's whole process group (it leads one: `process_group(0)`), so the worker
/// under the shell wrappers dies too; then reaps the shell.
fn kill_group(child: &mut Child) {
    // SAFETY: kill(2) with a pid we own; no memory involved.
    unsafe { libc::kill(-(child.id() as libc::pid_t), libc::SIGKILL) };
    let _ = child.wait();
}

/// Every worker's reply line, in order; on any failure every worker is killed.
fn replies(workers: &mut [Worker], limit: Duration) -> Result<Vec<String>, String> {
    let mut lines = Vec::with_capacity(workers.len());
    for (i, w) in workers.iter_mut().enumerate() {
        match w.line(limit) {
            Ok(line) => lines.push(line),
            Err(e) => {
                for w in workers.iter_mut() {
                    kill_group(&mut w.child);
                }
                return Err(format!("generator {i}: {e}"));
            }
        }
    }
    Ok(lines)
}

fn unix_now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after 1970")
        .as_nanos() as u64
}

/// Runs the sweep and prints its steps, then its summary whatever happened.
pub fn run(args: &Args, out: &mut dyn FnMut(&str)) -> Result<(), String> {
    if args.gens == 0 || !args.senders.is_multiple_of(args.gens) {
        return Err("--senders must be a positive multiple of --gens".into());
    }
    let mut summary = Summary {
        kind: "summary",
        proto: args.proto,
        gens: args.gens,
        senders: args.senders,
        size: args.size,
        rate_max: args.limits.rate_max,
        budget_window_us: args.budget_window_us,
        baseline_rtt_ns: None,
        knee_pps: None,
        knee_lower_bound: false,
        first_bad_budget_bound: false,
        last_good_pps: None,
        stop: "failed",
        steps: 0,
        complete: false,
        error: None,
    };
    let result = sweep(args, &mut summary, out);
    summary.complete = result.is_ok();
    summary.error = result.as_ref().err().cloned();
    out(&serde_json::to_string(&summary).expect("summary serializes"));
    result
}

fn sweep(args: &Args, summary: &mut Summary, out: &mut dyn FnMut(&str)) -> Result<(), String> {
    let per = args.senders / args.gens;
    let mut workers = Workers(Vec::new());
    for i in 0..args.gens {
        let cmd = args
            .gen_cmd
            .replace("{i}", &i.to_string())
            .replace("{senders}", &per.to_string());
        workers.0.push(Worker::spawn(&cmd)?);
    }
    for (i, line) in replies(&mut workers.0, READY_LIMIT)?.iter().enumerate() {
        if line.trim() != r#"{"kind":"ready"}"# {
            return Err(format!("generator {i}: expected ready, got {line:?}"));
        }
    }
    let step_limit =
        Duration::from_millis(args.lead_ms + args.warmup_ms + args.step_ms + args.drain_ms)
            + REPLY_SLACK;
    let window_ns = args.budget_window_us.map(|us| us * 1000);
    let mut sweep = Sweep::new(args.limits);
    let (mut rate, mut step) = (sweep.first(), 0u32);
    // (rate, budget_bound) of every judged step
    let mut judged: Vec<(u64, bool)> = Vec::new();
    loop {
        let start_unix_ns = unix_now_ns() + args.lead_ms * 1_000_000;
        let baseline = summary.baseline_rtt_ns.as_ref().map(|q| q.p50);
        for (w, share) in workers.0.iter_mut().zip(split_rate(rate, args.gens)) {
            let command = load::Command {
                step,
                rate_pps: share,
                start_unix_ns,
                warmup_ms: args.warmup_ms,
                step_ms: args.step_ms,
                drain_ms: args.drain_ms,
                budget: window_ns
                    .zip(baseline)
                    .map(|(w, b)| budget_per_connection(share, per, b, w)),
            };
            writeln!(
                w.stdin,
                "{}",
                serde_json::to_string(&command).expect("command serializes")
            )
            .and_then(|()| w.stdin.flush())
            .map_err(|e| format!("generator input: {e}"))?;
        }
        let mut results = Vec::with_capacity(workers.0.len());
        for (i, line) in replies(&mut workers.0, step_limit)?.iter().enumerate() {
            results.push(
                serde_json::from_str::<StepResult>(line)
                    .map_err(|e| format!("generator {i}: {e}: {line:?}"))?,
            );
        }
        let mut report = merge(args, step, rate, &results)?;
        summary.steps = step + 1;
        if step == 0 && window_ns.is_some() {
            summary.baseline_rtt_ns = report.service_rtt_ns.clone();
        }
        if report.generator_bound {
            out(&serde_json::to_string(&report).expect("report serializes"));
            summary.stop = "generator_bound";
            summary.knee_pps = sweep.knee();
            break;
        }
        let (good, next) = sweep.next(
            rate,
            Outcome {
                p99_ns: report.latency_ns.as_ref().map(|q| q.p99),
                drop_fraction: report.drop_fraction,
            },
        );
        report.good = good;
        out(&serde_json::to_string(&report).expect("report serializes"));
        judged.push((rate, report.budget_bound));
        if good {
            summary.last_good_pps = Some(summary.last_good_pps.map_or(rate, |g| g.max(rate)));
        }
        match next {
            Next::Run(r) => rate = r,
            Next::Done(knee) => {
                summary.knee_pps = knee.map(|k| k.pps);
                summary.knee_lower_bound = knee.is_some_and(|k| k.lower_bound);
                summary.stop = match knee {
                    None => "no_good_step",
                    Some(k) if k.lower_bound => "rate_max",
                    Some(_) => "knee",
                };
                break;
            }
        }
        step += 1;
    }
    summary.first_bad_budget_bound = summary.knee_pps.is_some_and(|knee| {
        let first_bad = judged.iter().map(|j| j.0).filter(|&r| r > knee).min();
        judged
            .iter()
            .any(|&(r, bound)| Some(r) == first_bad && bound)
    });
    for (i, w) in std::mem::take(&mut workers.0).into_iter().enumerate() {
        let status = w
            .exit(EXIT_LIMIT)
            .map_err(|e| format!("generator {i}: {e}"))?;
        if !status.success() {
            return Err(format!("generator {i} exited with {status}"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hist::Histogram;

    fn args() -> Args {
        Args {
            proto: "udp",
            gens: 2,
            gen_cmd: String::new(),
            senders: 4,
            size: 64,
            limits: Limits {
                rate_min: 1,
                rate_max: 2,
                bisect_steps: 0,
                p99_factor: 3.0,
                max_drop: 0.001,
            },
            step_ms: 1,
            warmup_ms: 0,
            drain_ms: 0,
            lag_limit_us: 20,
            lead_ms: 0,
            budget_window_us: None,
        }
    }

    fn result(
        step: u32,
        latencies: &[u64],
        lags: &[u64],
        measured: u64,
        backlog: u64,
    ) -> StepResult {
        let (mut lat, mut lag) = (Histogram::new(), Histogram::new());
        latencies.iter().for_each(|v| lat.record(*v));
        lags.iter().for_each(|v| lag.record(*v));
        StepResult {
            step,
            offered_pps: 500.0,
            window_unix_ns: [100 + u64::from(step), 200],
            measured,
            received: latencies.len() as u64,
            unsent: 0,
            sent_in_window: measured,
            received_in_window: latencies.len() as u64,
            duplicates: 1,
            stale: 0,
            malformed: 0,
            warmup_received: 0,
            latency: lat.to_sparse(),
            send_lag: lag.to_sparse(),
            owed_wait: Histogram::new().to_sparse(),
            backlog_at_end: backlog,
            owed_at_end: 0,
            service_rtt: Histogram::new().to_sparse(),
            budget: None,
            budget_waits: 0,
            in_flight_sum: 0,
            in_flight_samples: 0,
            in_flight_max: 0,
            in_flight_at_start: 0,
            in_flight_after: 0,
            ns_per_round: 0,
            turns: TurnSums::default(),
            eth: EthStats::default(),
            rx_dropped_full: 0,
        }
    }

    /// The budget lets each connection have its rate share over the baseline round trip plus
    /// the window in flight, rounded up, and never less than one message.
    #[test]
    fn budget_covers_the_baseline_round_trip_plus_the_window() {
        // 1M pps over 4 connections = 250k pps each; 30 µs + 50 µs = 80 µs -> 20 messages
        assert_eq!(budget_per_connection(1_000_000, 4, 30_000, 50_000), 20);
        assert_eq!(budget_per_connection(1_000_001, 4, 30_000, 50_000), 21);
        assert_eq!(budget_per_connection(1000, 4, 30_000, 50_000), 1);
    }

    /// Achieved rates come from what was sent and received within the window, not the
    /// schedule; unsent messages are part of the loss; any budget wait marks the step.
    #[test]
    fn merge_reports_achieved_rates_unsent_and_budget_waits() {
        let mut a = result(1, &[1000; 4], &[100], 10, 0);
        a.sent_in_window = 6;
        a.unsent = 4;
        a.budget = Some(3);
        a.budget_waits = 2;
        (a.in_flight_sum, a.in_flight_samples, a.in_flight_max) = (12, 6, 3);
        let b = result(1, &[1000; 10], &[100], 10, 0);
        let mut args = args();
        args.step_ms = 2000;
        let m = merge(&args, 1, 10, &[a, b]).unwrap();
        assert_eq!((m.measured, m.received, m.lost, m.unsent), (20, 14, 6, 4));
        assert_eq!((m.sent_pps, m.received_pps), (8.0, 7.0));
        assert_eq!(
            (m.budget, m.budget_waits, m.budget_bound),
            (Some(3), 2, true)
        );
        assert_eq!((m.in_flight_mean, m.in_flight_max), (Some(2.0), 3));
        assert_eq!(m.per_gen[0].sent_pps, 3.0);
        assert_eq!(m.per_gen[1].budget_waits, 0);
    }

    /// A fake worker: ready, then for each command the canned `result` line with the command's
    /// step number; it exits after `steps` commands.
    fn fake_worker(name: &str, result: &StepResult, steps: u32) -> String {
        let path =
            std::env::temp_dir().join(format!("zhida-bench-{name}-{}.json", std::process::id()));
        std::fs::write(&path, serde_json::to_string(result).unwrap() + "\n").unwrap();
        format!(
            r#"echo '{{"kind":"ready"}}'; n=0; while read line; do [ $n -ge {steps} ] && exit 1; sed "s/\"step\":0,/\"step\":$n,/" {}; n=$((n+1)); done"#,
            path.display()
        )
    }

    fn run_lines(args: &Args) -> (Result<(), String>, Vec<serde_json::Value>) {
        let mut lines = vec![];
        let r = run(args, &mut |l| lines.push(serde_json::from_str(l).unwrap()));
        (r, lines)
    }

    /// A worker dying mid-sweep fails the sweep, but the steps already run are printed and the
    /// last line is still a summary that says the sweep is incomplete and why.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn a_failed_worker_keeps_the_steps_and_ends_with_an_incomplete_summary() {
        let mut args = args();
        args.limits.rate_max = 1000;
        args.gen_cmd = fake_worker("dies", &result(0, &[1000], &[100], 1, 0), 2);
        let (r, lines) = run_lines(&args);
        assert!(r.is_err());
        let kinds: Vec<&str> = lines.iter().map(|l| l["kind"].as_str().unwrap()).collect();
        assert_eq!(kinds, ["step", "step", "summary"]);
        let summary = &lines[2];
        assert_eq!(summary["complete"], false);
        assert_eq!(summary["stop"], "failed");
        assert_eq!(summary["steps"], 2);
        assert!(summary["error"].as_str().unwrap().contains("generator 0"));
    }

    /// A step where nothing comes back is reported (no latency, 100% loss) and judged bad.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn a_step_with_total_loss_is_reported_and_bad() {
        let mut args = args();
        args.gen_cmd = fake_worker("silent", &result(0, &[], &[100], 5, 0), 10);
        let (r, lines) = run_lines(&args);
        assert_eq!(r, Ok(()));
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["drop_fraction"], 1.0);
        assert!(lines[0]["latency_ns"].is_null());
        assert_eq!(lines[0]["good"], false);
        assert_eq!(
            (lines[1]["complete"].clone(), lines[1]["stop"].clone()),
            (true.into(), "no_good_step".into())
        );
    }

    /// Counts add up across generators, latency quantiles are those of all echoes together,
    /// and loss is measured minus received over both.
    #[test]
    fn merge_sums_counts_and_pools_latency() {
        let a = result(3, &[1000, 2000], &[100, 200], 3, 0);
        let b = result(3, &[3000, 4000, 5000], &[100], 3, 0);
        let m = merge(&args(), 3, 1000, &[a, b]).unwrap();
        assert_eq!((m.measured, m.received, m.lost, m.duplicates), (6, 5, 1, 2));
        assert!((m.drop_fraction - 1.0 / 6.0).abs() < 1e-12);
        let q = m.latency_ns.unwrap();
        assert!(q.p50 >= 3000 && q.p50 <= 3000 + 3000 / 128, "p50 {}", q.p50);
        assert_eq!(q.max, 5000);
        assert_eq!(m.offered_pps, 1000.0);
        assert!(!m.generator_bound);
        assert_eq!(m.per_gen.len(), 2);
    }

    /// One generator falling behind (a backlog at the end, or send-lag p99 over the limit)
    /// flags the merged step, and shows which generator it was.
    #[test]
    fn one_overloaded_generator_flags_the_step() {
        let fine = result(0, &[1000], &[1000], 1, 0);
        let backlogged = result(0, &[1000], &[1000], 1, 7);
        let m = merge(&args(), 0, 1000, &[fine.clone(), backlogged]).unwrap();
        assert!(m.generator_bound);
        assert_eq!(
            m.per_gen
                .iter()
                .map(|g| g.generator_bound)
                .collect::<Vec<_>>(),
            [false, true]
        );
        let lagging = result(0, &[1000], &[25_000; 100], 1, 0);
        let m = merge(&args(), 0, 1000, &[lagging, fine]).unwrap();
        assert_eq!(
            m.per_gen
                .iter()
                .map(|g| g.generator_bound)
                .collect::<Vec<_>>(),
            [true, false]
        );
    }

    #[test]
    fn merge_refuses_a_result_of_another_step() {
        let m = merge(
            &args(),
            2,
            1000,
            &[result(2, &[], &[], 0, 0), result(1, &[], &[], 0, 0)],
        );
        assert!(m.is_err());
    }

    #[test]
    fn rate_split_adds_up() {
        assert_eq!(split_rate(1003, 4), [253, 250, 250, 250]);
        assert_eq!(split_rate(1003, 4).iter().sum::<u64>(), 1003);
    }

    /// A worker that answers once and then goes silent fails the wait within the limit, and every
    /// worker is killed.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn a_silent_worker_fails_the_wait_and_all_are_killed() {
        let mut workers = vec![
            Worker::spawn("echo one; echo two; sleep 30").unwrap(),
            Worker::spawn(
                "sh -c 'echo $$ > /tmp/zhida-bench-coord-test.pid; echo one; exec sleep 30'",
            )
            .unwrap(),
        ];
        assert_eq!(
            replies(&mut workers, Duration::from_secs(5)).unwrap(),
            ["one", "one"]
        );
        let started = Instant::now();
        let e = replies(&mut workers, Duration::from_millis(300)).unwrap_err();
        assert_eq!(e, "generator 1: generator silent for 300ms");
        assert!(started.elapsed() < Duration::from_secs(5));
        for w in &mut workers {
            assert!(w.child.try_wait().unwrap().is_some(), "worker left running");
        }
        // the worker under the shell wrapper is gone too (SIGKILL lands asynchronously; a
        // reparented zombie counts as gone)
        let pid = std::fs::read_to_string("/tmp/zhida-bench-coord-test.pid").unwrap();
        let gone = || {
            let stat =
                std::fs::read_to_string(format!("/proc/{}/stat", pid.trim())).unwrap_or_default();
            stat.is_empty() || stat.rsplit(") ").next().is_some_and(|r| r.starts_with('Z'))
        };
        let deadline = Instant::now() + Duration::from_secs(2);
        while !gone() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(gone(), "grandchild {} survived", pid.trim());
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn a_worker_that_does_not_exit_is_killed() {
        let w = Worker::spawn("exec sleep 30").unwrap();
        let started = Instant::now();
        assert_eq!(
            w.exit(Duration::from_millis(200)).unwrap_err(),
            "did not exit within 200ms; killed"
        );
        assert!(started.elapsed() < Duration::from_secs(5));
        let w = Worker::spawn("read line; exit 3").unwrap();
        assert_eq!(w.exit(Duration::from_secs(5)).unwrap().code(), Some(3));
    }
}
