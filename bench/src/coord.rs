//! The sweep coordinator: spawns G generator workers (`zhida-bench gen`, each its own Stack, Port
//! and core), runs every rate step on all of them at a common start time, merges their results,
//! judges the step and picks the next rate. No DPDK here.

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

#[derive(Debug, Clone, Serialize)]
pub struct GenReport {
    pub generator: u32,
    pub offered_pps: f64,
    pub received: u64,
    pub measured: u64,
    pub send_lag_ns: Option<Quantiles>,
    pub backlog_at_end: u64,
    /// Messages owed by connections the receiver kept full (flow control), and how long owed
    /// messages waited: the receiver's doing, not the generator's.
    pub owed_at_end: u64,
    pub owed_wait_ns: Option<Quantiles>,
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
    pub window_unix_ns: [u64; 2],
    pub measured: u64,
    pub received: u64,
    pub lost: u64,
    pub drop_fraction: f64,
    pub duplicates: u64,
    pub stale: u64,
    pub malformed: u64,
    pub latency_ns: Option<Quantiles>,
    pub send_lag_ns: Option<Quantiles>,
    /// Any generator fell behind its schedule: the step measured the generator, not the DUT.
    pub generator_bound: bool,
    pub good: bool,
    pub per_gen: Vec<GenReport>,
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
    let mut per_gen = Vec::with_capacity(results.len());
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
        let send_lag_ns = Quantiles::of(&gen_lag);
        let lag_bound = send_lag_ns
            .as_ref()
            .is_some_and(|q| q.p99 > args.lag_limit_us * 1000);
        per_gen.push(GenReport {
            generator: g as u32,
            offered_pps: r.offered_pps,
            received: r.received,
            measured: r.measured,
            send_lag_ns,
            backlog_at_end: r.backlog_at_end,
            owed_at_end: r.owed_at_end,
            owed_wait_ns: Quantiles::of(&Histogram::from_sparse(&r.owed_wait)?),
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
    Ok(StepReport {
        kind: "step",
        proto: args.proto,
        step,
        gens: results.len() as u32,
        senders: args.senders,
        size: args.size,
        rate_pps,
        offered_pps: results.iter().map(|r| r.offered_pps).sum(),
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
/// A step's reply may come this long after the step's drain (the worker's own stall limit is
/// shorter, so a worker whose loop still runs reports its state first).
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

pub fn run(args: &Args, out: &mut dyn FnMut(&str)) -> Result<(), String> {
    if args.gens == 0 || !args.senders.is_multiple_of(args.gens) {
        return Err("--senders must be a positive multiple of --gens".into());
    }
    let per = args.senders / args.gens;
    let mut workers = Vec::new();
    for i in 0..args.gens {
        let cmd = args
            .gen_cmd
            .replace("{i}", &i.to_string())
            .replace("{senders}", &per.to_string());
        workers.push(Worker::spawn(&cmd)?);
    }
    for (i, line) in replies(&mut workers, READY_LIMIT)?.iter().enumerate() {
        if line.trim() != r#"{"kind":"ready"}"# {
            return Err(format!("generator {i}: expected ready, got {line:?}"));
        }
    }
    let step_limit =
        Duration::from_millis(args.lead_ms + args.warmup_ms + args.step_ms + args.drain_ms)
            + REPLY_SLACK;
    let mut sweep = Sweep::new(args.limits);
    let (mut rate, mut step) = (sweep.first(), 0u32);
    let mut last_good = None;
    let (knee, stop) = loop {
        let start_unix_ns = unix_now_ns() + args.lead_ms * 1_000_000;
        for (w, share) in workers.iter_mut().zip(split_rate(rate, args.gens)) {
            let command = load::Command {
                step,
                rate_pps: share,
                start_unix_ns,
                warmup_ms: args.warmup_ms,
                step_ms: args.step_ms,
                drain_ms: args.drain_ms,
            };
            writeln!(
                w.stdin,
                "{}",
                serde_json::to_string(&command).expect("command serializes")
            )
            .and_then(|()| w.stdin.flush())
            .map_err(|e| format!("generator input: {e}"))?;
        }
        let mut results = Vec::with_capacity(workers.len());
        for (i, line) in replies(&mut workers, step_limit)?.iter().enumerate() {
            results.push(
                serde_json::from_str::<StepResult>(line)
                    .map_err(|e| format!("generator {i}: {e}: {line:?}"))?,
            );
        }
        let mut report = merge(args, step, rate, &results)?;
        let p99_ns = report.latency_ns.as_ref().map_or(u64::MAX, |q| q.p99);
        let (good, next) = sweep.next(
            rate,
            Outcome {
                p99_ns,
                drop_fraction: report.drop_fraction,
            },
        );
        report.good = good && !report.generator_bound;
        out(&serde_json::to_string(&report).expect("report serializes"));
        if report.good {
            last_good = Some(last_good.map_or(rate, |g: u64| g.max(rate)));
        }
        if report.generator_bound {
            break (None, "generator_bound");
        }
        match next {
            Next::Run(r) => rate = r,
            Next::Done { knee } => {
                break (
                    knee,
                    if knee == Some(args.limits.rate_max) {
                        "rate_max"
                    } else {
                        "knee"
                    },
                );
            }
        }
        step += 1;
    };
    out(&serde_json::json!({
        "kind": "summary", "proto": args.proto, "gens": args.gens, "senders": args.senders, "size": args.size,
        "knee_pps": knee, "stop": stop, "last_good_pps": last_good, "steps": step + 1,
    })
    .to_string());
    for (i, w) in workers.into_iter().enumerate() {
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
            duplicates: 1,
            stale: 0,
            malformed: 0,
            warmup_received: 0,
            latency: lat.to_sparse(),
            send_lag: lag.to_sparse(),
            owed_wait: Histogram::new().to_sparse(),
            backlog_at_end: backlog,
            owed_at_end: 0,
            ns_per_round: 0,
            turns: TurnSums::default(),
            eth: EthStats::default(),
            rx_dropped_full: 0,
        }
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
