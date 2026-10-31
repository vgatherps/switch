//! `zhida-bench`: see `USAGE`.

use std::collections::HashMap;
use std::ffi::OsString;
use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;

use zhida_bench::load::Proto;
use zhida_bench::sweep::Limits;
use zhida_bench::{coord, kernel, load, pingpong, serve};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const USAGE: &str = "\
usage:
  zhida-bench sweep --proto udp|tcp|ws --gens G --gen-cmd CMD --senders N --size S
                    --rate-min PPS --rate-max PPS [--step-ms 2000] [--warmup-ms 500] [--drain-ms 200]
                    [--bisect 4] [--p99-factor 3] [--max-drop 0.001] [--lag-limit-us 20] [--lead-ms 50]
      The sweep coordinator: starts G generator workers (`sh -c CMD` with {i} = 0..G-1 and
      {senders} = N/G), runs each rate step on all of them (rate split evenly), merges, doubles
      then bisects to the knee. Prints one JSON line per step and a summary line.
  zhida-bench gen --config F --proto udp|tcp|ws --target IP:PORT --senders N --size S
                  [--send-budget 32] [--local-port 40000]
      One generator worker (a Zhida Stack on its own Port and core); takes steps on stdin.
      udp: N sockets; tcp: N connections, S-byte messages on each (S at most the MSS);
      ws: N ws:// connections, S-byte binary messages (S at most 1400: one TCP frame).
  zhida-bench pingpong --config F --target IP:PORT --size S [--count 100000] [--warmup 1000]
                       [--local-port 40000]
      Closed-loop TCP ping-pong on one Zhida connection; prints the round-trip times.
  zhida-bench serve --config F --proto udp|tcp|ws --port P [--counters on|off] [--counter-log F]
                    [--ring 36000] [--max-conns 256] [--accepts 64]
      Zhida echo server; runs until SIGINT/SIGTERM, then writes its counter log.
  zhida-bench kernel-serve --proto udp|tcp|ws --bind IP:PORT
      Kernel echo server; runs until SIGINT/SIGTERM.";

struct Flags(HashMap<String, String>);

impl Flags {
    fn parse(args: impl Iterator<Item = OsString>) -> Result<Flags, String> {
        let mut map = HashMap::new();
        let mut args = args;
        while let Some(flag) = args.next() {
            let flag = flag
                .into_string()
                .map_err(|f| format!("not UTF-8: {f:?}"))?;
            let name = flag
                .strip_prefix("--")
                .ok_or_else(|| format!("unexpected argument {flag}"))?;
            let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?;
            let value = value
                .into_string()
                .map_err(|v| format!("{flag}: not UTF-8: {v:?}"))?;
            if map.insert(name.to_owned(), value).is_some() {
                return Err(format!("{flag} given twice"));
            }
        }
        Ok(Flags(map))
    }

    fn take<T: FromStr>(&mut self, name: &str) -> Result<Option<T>, String> {
        self.0
            .remove(name)
            .map(|v| {
                v.parse()
                    .map_err(|_| format!("--{name}: invalid value {v:?}"))
            })
            .transpose()
    }

    fn need<T: FromStr>(&mut self, name: &str) -> Result<T, String> {
        self.take(name)?
            .ok_or_else(|| format!("--{name} is required"))
    }

    fn or<T: FromStr>(&mut self, name: &str, default: T) -> Result<T, String> {
        Ok(self.take(name)?.unwrap_or(default))
    }

    fn done(self) -> Result<(), String> {
        match self.0.keys().next() {
            Some(name) => Err(format!("unknown option --{name}")),
            None => Ok(()),
        }
    }
}

fn proto(flags: &mut Flags) -> Result<Proto, String> {
    match flags.need::<String>("proto")?.as_str() {
        "udp" => Ok(Proto::Udp),
        "tcp" => Ok(Proto::Tcp),
        "ws" => Ok(Proto::Ws),
        other => Err(format!("--proto {other}: udp, tcp or ws")),
    }
}

fn main_inner() -> Result<(), String> {
    let mut args = std::env::args_os().skip(1);
    let command = args
        .next()
        .and_then(|c| c.into_string().ok())
        .ok_or("missing subcommand")?;
    let mut flags = Flags::parse(args)?;
    match command.as_str() {
        "sweep" => {
            let proto = proto(&mut flags)?;
            let args = coord::Args {
                proto: proto.name(),
                gens: flags.need("gens")?,
                gen_cmd: flags.need("gen-cmd")?,
                senders: flags.need("senders")?,
                size: flags.need("size")?,
                limits: Limits {
                    rate_min: flags.need("rate-min")?,
                    rate_max: flags.need("rate-max")?,
                    bisect_steps: flags.or("bisect", 4)?,
                    p99_factor: flags.or("p99-factor", 3.0)?,
                    max_drop: flags.or("max-drop", 0.001)?,
                },
                step_ms: flags.or("step-ms", 2000)?,
                warmup_ms: flags.or("warmup-ms", 500)?,
                drain_ms: flags.or("drain-ms", 200)?,
                lag_limit_us: flags.or("lag-limit-us", 20)?,
                lead_ms: flags.or("lead-ms", 50)?,
            };
            flags.done()?;
            if args.limits.rate_min == 0 || args.limits.rate_min > args.limits.rate_max {
                return Err("need 0 < --rate-min <= --rate-max".into());
            }
            coord::run(&args, &mut print_line)
        }
        "gen" => {
            let proto = proto(&mut flags)?;
            let args = load::Args {
                config: flags.need::<PathBuf>("config")?,
                proto,
                target: flags.need("target")?,
                senders: flags.need("senders")?,
                size: flags.need("size")?,
                send_budget: flags.or("send-budget", 32)?,
                local_port: flags.or("local-port", 40000)?,
            };
            flags.done()?;
            if args.senders == 0 || args.send_budget == 0 {
                return Err("need --senders > 0 and --send-budget > 0".into());
            }
            load::run(&args, &mut print_line)
        }
        "pingpong" => {
            let args = pingpong::Args {
                config: flags.need("config")?,
                target: flags.need("target")?,
                size: flags.need("size")?,
                warmup: flags.or("warmup", 1000)?,
                count: flags.or("count", 100_000)?,
                local_port: flags.or("local-port", 40000)?,
            };
            flags.done()?;
            pingpong::run(&args, &mut print_line)
        }
        "serve" => {
            let proto = proto(&mut flags)?;
            let args = serve::Args {
                config: flags.need("config")?,
                proto,
                port: flags.need("port")?,
                max_conns: flags.or("max-conns", 256)?,
                accepts: flags.or("accepts", 64)?,
                counters: match flags.or::<String>("counters", "on".into())?.as_str() {
                    "on" => true,
                    "off" => false,
                    other => return Err(format!("--counters {other}: on or off")),
                },
                counter_log: flags.take("counter-log")?,
                ring: flags.or("ring", 36_000)?,
            };
            flags.done()?;
            serve::run(&args)
        }
        "kernel-serve" => {
            let proto = proto(&mut flags)?;
            let bind = flags.need("bind")?;
            flags.done()?;
            match proto {
                Proto::Udp => kernel::udp(bind),
                Proto::Tcp => kernel::tcp(bind),
                Proto::Ws => kernel::ws(bind),
            }
        }
        other => Err(format!("unknown subcommand {other}")),
    }
}

fn print_line(line: &str) {
    let mut stdout = std::io::stdout().lock();
    let _ = writeln!(stdout, "{line}");
    let _ = stdout.flush();
}

fn main() -> ExitCode {
    match main_inner() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("zhida-bench: {e}\n\n{USAGE}");
            ExitCode::FAILURE
        }
    }
}
