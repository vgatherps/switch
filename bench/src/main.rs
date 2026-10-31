//! `zhida-bench`: see `USAGE`.

use std::collections::HashMap;
use std::ffi::OsString;
use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;

use zhida_bench::load::Proto;
use zhida_bench::{coord, load, serve};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const USAGE: &str = "\
usage:
  zhida-bench sweep --proto udp|tcp|ws --gens G --gen-cmd CMD --senders N
                    --rate-min PPS --rate-max PPS [--step-ms 2000] [--warmup-ms 500] [--drain-ms 200]
                    [--budget-us W]
      The sweep coordinator: starts G generator workers (`sh -c CMD` with {i} = 0..G-1 and
      {senders} = N/G) and runs each rate PPS-min x sqrt(2)^k up to PPS-max on all of them (rate
      split evenly), stopping early at a generator-bound step. Prints one JSON line per step and
      always a summary line. --budget-us (tcp, ws): after the first step, each
      connection may have its rate share x (the first step's median round trip + W) in flight.
  zhida-bench gen --config F --proto udp|tcp|ws --target IP:PORT --senders N --size S
                  [--local-port 40000]
      One generator worker (a Zhida Stack on its own Port and core); takes steps on stdin.
      udp: N sockets; tcp: N connections, S-byte messages on each (S at most the MSS);
      ws: N ws:// connections, S-byte binary messages (S at most 1400: one TCP frame).
  zhida-bench serve --config F --proto udp|tcp|ws --port P
      Zhida echo server; runs until SIGINT/SIGTERM.";

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
                gens: flags.need("gens")?,
                gen_cmd: flags.need("gen-cmd")?,
                senders: flags.need("senders")?,
                rate_min: flags.need("rate-min")?,
                rate_max: flags.need("rate-max")?,
                step_ms: flags.or("step-ms", 2000)?,
                warmup_ms: flags.or("warmup-ms", 500)?,
                drain_ms: flags.or("drain-ms", 200)?,
                budget_window_us: flags.take("budget-us")?,
            };
            if args.budget_window_us.is_some() && proto == Proto::Udp {
                return Err("--budget-us is for tcp and ws".into());
            }
            flags.done()?;
            if args.rate_min == 0 || args.rate_min > args.rate_max {
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
                local_port: flags.or("local-port", 40000)?,
            };
            flags.done()?;
            if args.senders == 0 {
                return Err("need --senders > 0".into());
            }
            load::run(&args, &mut print_line)
        }
        "serve" => {
            let proto = proto(&mut flags)?;
            let args = serve::Args {
                config: flags.need("config")?,
                proto,
                port: flags.need("port")?,
            };
            flags.done()?;
            serve::run(&args)
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
