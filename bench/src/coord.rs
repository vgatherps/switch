//! The sweep coordinator: spawns G generator workers (`zhida-bench gen`, each its own Stack, Port
//! and core), runs every rate of a fixed log-spaced list (`rates`) on all of them at a common
//! start time and merges their results. No DPDK here.
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
use crate::load::{self, Reply, Request, StepResult};

/// The sweep's rates: `rate_min` x sqrt(2)^k for k = 0, 1, .. while at most `rate_max`.
pub fn rates(rate_min: u64, rate_max: u64) -> Vec<u64> {
    (0..)
        .map(|k| (rate_min as f64 * 2f64.powf(f64::from(k) / 2.0)).round() as u64)
        .take_while(|&r| r <= rate_max)
        .collect()
}

#[derive(Debug, Clone)]
pub struct Args {
    pub gens: u32,
    /// `sh -c` command of worker `{i}` with `{senders}` senders.
    pub gen_cmd: String,
    pub senders: u32,
    pub rate_min: u64,
    pub rate_max: u64,
    pub step_ms: u64,
    pub warmup_ms: u64,
    pub drain_ms: u64,
    /// TCP/ws: the outstanding budget's window beyond the baseline round trip; `None`: no budget.
    pub budget_window_us: Option<u64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Quantiles {
    pub p50: u64,
    pub p99: u64,
    pub p999: u64,
    pub max: u64,
}

impl Quantiles {
    pub fn of(h: &Histogram) -> Option<Quantiles> {
        Some(Quantiles {
            p50: h.quantile(0.5)?,
            p99: h.quantile(0.99)?,
            p999: h.quantile(0.999)?,
            max: h.max(),
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
pub struct StepReport {
    pub kind: &'static str,
    pub offered_pps: f64,
    /// What actually left the generators, and what came back, within the window.
    pub sent_pps: f64,
    pub received_pps: f64,
    /// Measured messages not echoed by the end of the drain, over measured messages (those never
    /// sent included).
    pub drop_fraction: f64,
    pub latency_ns: Option<Quantiles>,
    /// Some measured message waited on the budget: a knee here may be the budget's, not the
    /// receiver's.
    pub budget_bound: bool,
    /// Any generator fell behind its schedule: the step measured the generator, not the DUT.
    pub generator_bound: bool,
    /// TCP/ws: echo time - actual send time, median; the first step's is the budget's baseline.
    #[serde(skip)]
    pub service_rtt_p50_ns: Option<u64>,
}

/// The last line of every sweep.
#[derive(Debug, Clone, Serialize)]
pub struct Summary {
    pub kind: &'static str,
    pub steps: u32,
    /// Every step ran or the sweep stopped at a generator-bound step; false: see `error`.
    pub complete: bool,
    /// The sweep stopped at a generator-bound step: it measured the generators, not the
    /// receiver, so it is not a success (`run` fails) even when complete.
    pub generator_bound: bool,
    pub error: Option<String>,
}

/// A generator whose send lag (actual - scheduled send time) p99 is above this fell behind.
const LAG_LIMIT_NS: u64 = 20_000;

/// Merges the workers' results of one step (`results[g]` from worker g).
pub fn merge(args: &Args, step: u32, results: &[StepResult]) -> Result<StepReport, String> {
    let mut latency = Histogram::new();
    let mut rtt = Histogram::new();
    let mut generator_bound = false;
    for (g, r) in results.iter().enumerate() {
        if r.step != step {
            return Err(format!(
                "generator {g} answered step {} for step {step}",
                r.step
            ));
        }
        latency.merge(&Histogram::from_sparse(&r.latency)?);
        rtt.merge(&Histogram::from_sparse(&r.service_rtt)?);
        let lag_p99 = Histogram::from_sparse(&r.send_lag)?.quantile(0.99);
        generator_bound |= r.backlog_at_end > 0 || lag_p99.is_some_and(|p| p > LAG_LIMIT_NS);
    }
    let sum = |f: fn(&StepResult) -> u64| results.iter().map(f).sum::<u64>();
    let (measured, received) = (sum(|r| r.measured), sum(|r| r.received));
    let window_s = args.step_ms as f64 / 1e3;
    Ok(StepReport {
        kind: "step",
        offered_pps: results.iter().map(|r| r.offered_pps).sum(),
        sent_pps: sum(|r| r.sent_in_window) as f64 / window_s,
        received_pps: sum(|r| r.received_in_window) as f64 / window_s,
        drop_fraction: if measured == 0 {
            0.0
        } else {
            (measured - received) as f64 / measured as f64
        },
        latency_ns: Quantiles::of(&latency),
        budget_bound: sum(|r| r.budget_waits) > 0,
        generator_bound,
        service_rtt_p50_ns: rtt.quantile(0.5),
    })
}

/// Worker `g`'s step start: `g / rate` s after the common start `start_unix_ns`, so the workers'
/// schedules (each `rate / G`) interleave evenly instead of sending in phase.
pub fn gen_start(start_unix_ns: u64, g: u32, rate: u64) -> u64 {
    start_unix_ns + (u128::from(g) * 1_000_000_000 / u128::from(rate)) as u64
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
/// A step's reply may come this long after the step's drain.
const REPLY_SLACK: Duration = Duration::from_secs(10);
/// How far ahead of sending a step's command its start time lies.
const LEAD_MS: u64 = 50;
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
        steps: 0,
        complete: false,
        generator_bound: false,
        error: None,
    };
    let result = sweep(args, &mut summary, out);
    summary.complete = result.is_ok();
    summary.error = result.as_ref().err().cloned();
    out(&serde_json::to_string(&summary).expect("summary serializes"));
    result?;
    if summary.generator_bound {
        return Err(format!(
            "stopped at a generator-bound step after {} steps: not a receiver measurement",
            summary.steps
        ));
    }
    Ok(())
}

/// How far a worker's clock may be from the coordinator's, beyond the probe's half round trip:
/// workers start each step at a common wall-clock time.
const CLOCK_LIMIT_NS: u64 = 1_000_000;
/// How long a worker may take to answer a clock probe.
const PROBE_LIMIT: Duration = Duration::from_secs(5);

fn send(w: &mut Worker, request: &Request) -> Result<(), String> {
    writeln!(
        w.stdin,
        "{}",
        serde_json::to_string(request).expect("request serializes")
    )
    .and_then(|()| w.stdin.flush())
    .map_err(|e| format!("generator input: {e}"))
}

fn parse(i: usize, line: &str) -> Result<Reply, String> {
    serde_json::from_str(line).map_err(|e| format!("generator {i}: {e}: {line:?}"))
}

/// Probes each worker's clock; fails if one is off this host's by more than `CLOCK_LIMIT_NS`
/// beyond its probe's half round trip.
fn check_clocks(workers: &mut [Worker]) -> Result<(), String> {
    for (i, w) in workers.iter_mut().enumerate() {
        let sent = unix_now_ns();
        send(w, &Request::Clock)?;
        let line = w
            .line(PROBE_LIMIT)
            .map_err(|e| format!("generator {i}: {e}"))?;
        let answered = unix_now_ns();
        let Reply::Clock { unix_ns } = parse(i, &line)? else {
            return Err(format!("generator {i}: expected its clock, got {line:?}"));
        };
        let offset = unix_ns as i64 - sent.midpoint(answered) as i64;
        let slack = (answered - sent) / 2;
        if offset.unsigned_abs() > CLOCK_LIMIT_NS + slack {
            return Err(format!(
                "generator {i}: its clock is {offset} ns off the coordinator's (probe +-{slack} ns, \
                 limit {CLOCK_LIMIT_NS} ns)"
            ));
        }
    }
    Ok(())
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
        if !matches!(parse(i, line), Ok(Reply::Ready)) {
            return Err(format!("generator {i}: expected ready, got {line:?}"));
        }
    }
    let step_limit = Duration::from_millis(LEAD_MS + args.warmup_ms + args.step_ms + args.drain_ms)
        + REPLY_SLACK;
    let window_ns = args.budget_window_us.map(|us| us * 1000);
    // the first step's median service round trip (TCP/ws): the budget's baseline
    let mut baseline_rtt_ns = None;
    for (id, rate) in (0u32..).zip(rates(args.rate_min, args.rate_max)) {
        check_clocks(&mut workers.0)?;
        let start_unix_ns = unix_now_ns() + LEAD_MS * 1_000_000;
        for (g, (w, share)) in (0u32..).zip(workers.0.iter_mut().zip(split_rate(rate, args.gens))) {
            let budget = window_ns
                .zip(baseline_rtt_ns)
                .map(|(w, b)| budget_per_connection(share, per, b, w));
            send(
                w,
                &Request::Step(load::Command {
                    step: id,
                    rate_pps: share,
                    start_unix_ns: gen_start(start_unix_ns, g, rate),
                    warmup_ms: args.warmup_ms,
                    step_ms: args.step_ms,
                    drain_ms: args.drain_ms,
                    budget,
                }),
            )?;
        }
        let mut results = Vec::with_capacity(workers.0.len());
        for (i, line) in replies(&mut workers.0, step_limit)?.iter().enumerate() {
            match parse(i, line)? {
                Reply::Step(r) => results.push(*r),
                Reply::Late { late_ns, .. } => {
                    return Err(format!(
                        "generator {i} got the step at {rate} pps {late_ns} ns after its start"
                    ));
                }
                _ => {
                    return Err(format!(
                        "generator {i}: expected a step result, got {line:?}"
                    ));
                }
            }
        }
        let report = merge(args, id, &results)?;
        summary.steps = id + 1;
        out(&serde_json::to_string(&report).expect("report serializes"));
        if id == 0 && window_ns.is_some() {
            baseline_rtt_ns = report.service_rtt_p50_ns;
            if baseline_rtt_ns.is_none() {
                return Err(
                    "no echo in the first step: no baseline round trip for the budget".into(),
                );
            }
        }
        if report.generator_bound {
            summary.generator_bound = true;
            break;
        }
    }
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
            gens: 2,
            gen_cmd: String::new(),
            senders: 4,
            rate_min: 1000,
            rate_max: 1000,
            step_ms: 1,
            warmup_ms: 0,
            drain_ms: 0,
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
            measured,
            received: latencies.len() as u64,
            sent_in_window: measured,
            received_in_window: latencies.len() as u64,
            latency: lat.to_sparse(),
            send_lag: lag.to_sparse(),
            backlog_at_end: backlog,
            service_rtt: Histogram::new().to_sparse(),
            budget_waits: 0,
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
    /// schedule; loss is over all measured messages; any budget wait marks the step.
    #[test]
    fn merge_reports_achieved_rates_loss_and_budget_waits() {
        let mut a = result(1, &[1000; 4], &[100], 10, 0);
        a.sent_in_window = 6;
        a.budget_waits = 2;
        let b = result(1, &[1000; 10], &[100], 10, 0);
        let mut args = args();
        args.step_ms = 2000;
        let m = merge(&args, 1, &[a.clone(), b.clone()]).unwrap();
        assert!((m.drop_fraction - 0.3).abs() < 1e-12);
        assert_eq!((m.sent_pps, m.received_pps), (8.0, 7.0));
        assert!(m.budget_bound);
        a.budget_waits = 0;
        assert!(!merge(&args, 1, &[a, b]).unwrap().budget_bound);
    }

    /// A fake worker script: ready; a clock probe answers its wall clock plus `skew_ns`; the
    /// first `late` step commands answer late; the others answer the canned `result` with the
    /// command's step id; it exits on its step command number `steps`.
    fn fake(name: &str, result: &StepResult, steps: u32, skew_ns: i64, late: u32) -> String {
        let path =
            std::env::temp_dir().join(format!("zhida-bench-{name}-{}.json", std::process::id()));
        let line = serde_json::to_string(&Reply::Step(Box::new(result.clone()))).unwrap();
        std::fs::write(&path, line + "\n").unwrap();
        r#"echo '{"kind":"ready"}'; n=0
        while read line; do
            case "$line" in *'"kind":"clock"'*)
                echo "{\"kind\":\"clock\",\"unix_ns\":$(($(date +%s%N) + SKEW))}"; continue;; esac
            [ $n -ge STEPS ] && exit 1
            if [ $n -lt LATE ]; then echo "{\"kind\":\"late\",\"step\":$n,\"late_ns\":1000}"
            else sed "s/\"step\":0,/\"step\":$n,/" FILE; fi
            n=$((n+1))
        done"#
            .replace("SKEW", &skew_ns.to_string())
            .replace("STEPS", &steps.to_string())
            .replace("LATE", &late.to_string())
            .replace("FILE", &path.display().to_string())
    }

    fn fake_worker(name: &str, result: &StepResult, steps: u32) -> String {
        fake(name, result, steps, 0, 0)
    }

    /// A worker that got a step after its start time fails the sweep: its step would not have
    /// overlapped the others'. The summary says why.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn a_late_start_fails_the_sweep() {
        let mut args = args();
        args.gens = 1;
        args.gen_cmd = fake("late", &result(0, &[1000], &[100], 1, 0), 10, 0, 1);
        let (r, lines) = run_lines(&args);
        assert!(r.unwrap_err().contains("ns after its start"));
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0]["complete"], false);
    }

    /// A worker whose clock is 50 ms off fails the sweep before the step runs; one on time runs.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn a_worker_clock_off_fails_the_sweep_before_the_step() {
        let mut args = args();
        args.gens = 1;
        args.gen_cmd = fake("skew", &result(0, &[1000], &[100], 1, 0), 10, 50_000_000, 0);
        let (r, lines) = run_lines(&args);
        assert!(r.unwrap_err().contains("generator 0: its clock is"));
        assert_eq!(lines.len(), 1, "no step ran");
        args.gen_cmd = fake("noskew", &result(0, &[1000], &[100], 1, 0), 10, 0, 0);
        assert_eq!(run_lines(&args).0, Ok(()));
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
        args.rate_max = 2000;
        args.gen_cmd = fake_worker("dies", &result(0, &[1000], &[100], 1, 0), 2);
        let (r, lines) = run_lines(&args);
        assert!(r.is_err());
        let kinds: Vec<&str> = lines.iter().map(|l| l["kind"].as_str().unwrap()).collect();
        assert_eq!(kinds, ["step", "step", "summary"]);
        let summary = &lines[2];
        assert_eq!(summary["complete"], false);
        assert_eq!(summary["steps"], 2);
        assert!(summary["error"].as_str().unwrap().contains("generator 0"));
    }

    /// A step where nothing comes back is reported (no latency, 100% loss) and the sweep goes on.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn a_step_with_total_loss_is_reported() {
        let mut args = args();
        args.gen_cmd = fake_worker("silent", &result(0, &[], &[100], 5, 0), 10);
        let (r, lines) = run_lines(&args);
        assert_eq!(r, Ok(()));
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["drop_fraction"], 1.0);
        assert!(lines[0]["latency_ns"].is_null());
        assert_eq!(lines[1]["complete"], true);
    }

    /// TCP/ws with a budget: no echo in the first step leaves no baseline, and the sweep fails
    /// instead of running the later steps uncapped.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn no_baseline_fails_a_budgeted_sweep() {
        let mut args = args();
        args.budget_window_us = Some(50);
        args.rate_max = 4000;
        args.gen_cmd = fake_worker("nobase", &result(0, &[], &[100], 5, 0), 10);
        let (r, lines) = run_lines(&args);
        assert!(r.unwrap_err().contains("no baseline"));
        assert_eq!(lines.len(), 2, "the first step, then the summary");
        assert_eq!(lines[1]["complete"], false);
    }

    /// A sweep that stops at a generator-bound step ends with a complete summary that says so,
    /// and is not a success.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn a_generator_bound_sweep_is_not_a_success() {
        let mut args = args();
        args.rate_max = 4000;
        args.gen_cmd = fake_worker("bound", &result(0, &[1000], &[100], 1, 7), 10);
        let (r, lines) = run_lines(&args);
        assert!(r.unwrap_err().contains("generator-bound"));
        assert_eq!(lines.len(), 2, "stopped after the first step");
        assert_eq!(lines[1]["complete"], true);
        assert_eq!(lines[1]["generator_bound"], true);
    }

    /// Latency quantiles are those of all echoes together, and loss is measured minus received
    /// over all generators.
    #[test]
    fn merge_pools_latency_and_loss() {
        let a = result(3, &[1000, 2000], &[100, 200], 3, 0);
        let b = result(3, &[3000, 4000, 5000], &[100], 3, 0);
        let m = merge(&args(), 3, &[a, b]).unwrap();
        assert!((m.drop_fraction - 1.0 / 6.0).abs() < 1e-12);
        let q = m.latency_ns.unwrap();
        assert!(q.p50 >= 3000 && q.p50 <= 3000 + 3000 / 128, "p50 {}", q.p50);
        assert_eq!(q.max, 5000);
        assert_eq!(m.offered_pps, 1000.0);
        assert!(!m.generator_bound);
    }

    /// One generator falling behind (a backlog at the end, or send-lag p99 over the limit)
    /// flags the merged step.
    #[test]
    fn one_overloaded_generator_flags_the_step() {
        let fine = result(0, &[1000], &[1000], 1, 0);
        let backlogged = result(0, &[1000], &[1000], 1, 7);
        let lagging = result(0, &[1000], &[25_000; 100], 1, 0);
        let bound = |r: &[StepResult]| merge(&args(), 0, r).unwrap().generator_bound;
        assert!(!bound(&[fine.clone(), fine.clone()]));
        assert!(bound(&[fine.clone(), backlogged]));
        assert!(bound(&[lagging, fine]));
    }

    #[test]
    fn merge_refuses_a_result_of_another_step() {
        let m = merge(
            &args(),
            2,
            &[result(2, &[], &[], 0, 0), result(1, &[], &[], 0, 0)],
        );
        assert!(m.is_err());
    }

    /// 10k..5M: 18 rates, each sqrt(2) above the last, the last at most rate_max; a single rate
    /// when rate_min = rate_max.
    #[test]
    fn rates_are_log_spaced_by_sqrt_two() {
        let r = rates(10_000, 5_000_000);
        assert_eq!(r.len(), 18);
        assert_eq!(&r[..4], [10_000, 14_142, 20_000, 28_284]);
        assert_eq!(r[17], 3_620_387);
        assert_eq!(rates(64_000, 64_000), [64_000]);
    }

    /// Five workers at 10k pps in total: worker g starts g x 100 µs late, and all their messages
    /// together are evenly 100 µs apart.
    #[test]
    fn worker_offsets_interleave_evenly() {
        let (gens, rate) = (5u32, 10_000);
        assert_eq!(gen_start(1_000, 3, rate), 301_000);
        let mut due: Vec<u64> = (0..gens)
            .flat_map(|g| {
                let s = crate::schedule::Schedule::for_rate(4, rate / 5, gen_start(0, g, rate));
                (0..40).map(move |k| s.due_at(k))
            })
            .collect();
        due.sort_unstable();
        assert!(due.windows(2).all(|w| w[1] - w[0] == 100_000), "{due:?}");
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
