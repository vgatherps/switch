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
                    (--rate-min PPS --rate-max PPS | --rates PPS,PPS,...)
                    [--step-ms 2000] [--warmup-ms 500] [--drain-ms 200] [--budget-us W]
      The sweep coordinator: starts G generator workers (`sh -c CMD` with {i} = 0..G-1 and
      {senders} = N/G) and runs each rate on all of them (rate split evenly), stopping early at a
      generator-bound step. Supply a strictly increasing list with --rates, or use the default
      log-spaced schedule PPS-min x sqrt(2)^k up to PPS-max. Prints one JSON line per step and
      always a summary line. --budget-us (tcp, ws): after the first step, each
      connection may have its rate share x (the first step's median round trip + W) in flight.
  zhida-bench gen --config F --proto udp|tcp|ws --target IP:PORT --senders N --size S
                  [--local-port 40000]
      One generator worker (a Zhida Stack on its own Port and core); takes steps on stdin.
      udp: N sockets; tcp: N connections, S-byte messages on each (S at most the MSS);
      ws: N ws:// connections, S-byte binary messages (S at most 1400: one TCP frame).
  zhida-bench serve --config F --proto udp|tcp|ws --port P
      Zhida echo server; runs until SIGINT/SIGTERM.
  zhida-bench kernel-serve --proto udp|tcp|ws --bind IP --port P
      Linux kernel-socket echo server; runs until SIGINT/SIGTERM.";

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

fn sweep_rates(flags: &mut Flags) -> Result<(u64, u64, Option<Vec<u64>>), String> {
    if let Some(value) = flags.take::<String>("rates")? {
        if flags.0.contains_key("rate-min") || flags.0.contains_key("rate-max") {
            return Err("--rates cannot be combined with --rate-min or --rate-max".into());
        }
        let schedule: Vec<u64> = value
            .split(',')
            .map(|rate| rate.parse().map_err(|_| format!("--rates: invalid value {rate:?}")))
            .collect::<Result<_, _>>()?;
        if schedule[0] == 0 || schedule.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err("--rates must be positive and strictly increasing".into());
        }
        Ok((schedule[0], *schedule.last().unwrap(), Some(schedule)))
    } else {
        let min = flags.need("rate-min")?;
        let max = flags.need("rate-max")?;
        if min == 0 || min > max {
            return Err("need 0 < --rate-min <= --rate-max".into());
        }
        Ok((min, max, None))
    }
}

#[cfg(target_os = "linux")]
fn kernel_args(flags: &mut Flags) -> Result<(Proto, std::net::Ipv4Addr, u16), String> {
    let proto = proto(flags)?;
    let bind = flags.need("bind")?;
    let port = flags.need("port")?;
    if port == 0 {
        return Err("need --port > 0".into());
    }
    Ok((proto, bind, port))
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
            let (rate_min, rate_max, schedule) = sweep_rates(&mut flags)?;
            let args = coord::Args {
                gens: flags.need("gens")?,
                gen_cmd: flags.need("gen-cmd")?,
                senders: flags.need("senders")?,
                rate_min,
                rate_max,
                step_ms: flags.or("step-ms", 2000)?,
                warmup_ms: flags.or("warmup-ms", 500)?,
                drain_ms: flags.or("drain-ms", 200)?,
                budget_window_us: flags.take("budget-us")?,
            };
            if args.budget_window_us.is_some() && proto == Proto::Udp {
                return Err("--budget-us is for tcp and ws".into());
            }
            flags.done()?;
            let mut progress = |stage: &str| eprintln!("zhida-bench {proto:?}: {stage}");
            match schedule {
                Some(schedule) => {
                    coord::run_with_rates(&args, &schedule, &mut print_line, &mut progress)
                }
                None => coord::run(&args, &mut print_line, &mut progress),
            }
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
        #[cfg(target_os = "linux")]
        "kernel-serve" => {
            let (proto, bind, port) = kernel_args(&mut flags)?;
            flags.done()?;
            zhida_bench::kernel_serve::run(proto, bind, port)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_rates(args: &[&str]) -> Result<(u64, u64, Option<Vec<u64>>), String> {
        let mut flags = Flags::parse(args.iter().map(|arg| OsString::from(*arg)))?;
        let rates = sweep_rates(&mut flags)?;
        flags.done()?;
        Ok(rates)
    }

    #[test]
    fn explicit_rates_preserve_the_list_and_range_still_uses_log_schedule() {
        let rates = [
            10_000, 100_000, 400_000, 800_000, 900_000, 1_000_000, 1_100_000, 1_200_000,
            1_400_000, 1_800_000, 2_500_000,
        ];
        let parsed = parse_rates(&[
            "--rates",
            "10000,100000,400000,800000,900000,1000000,1100000,1200000,1400000,1800000,2500000",
        ]).unwrap();
        assert_eq!(parsed, (10_000, 2_500_000, Some(rates.to_vec())));
        assert_eq!(parse_rates(&["--rates", "7"]).unwrap(), (7, 7, Some(vec![7])));
        assert_eq!(
            parse_rates(&["--rate-min", "10000", "--rate-max", "5000000"]).unwrap(),
            (10_000, 5_000_000, None)
        );
    }

    #[test]
    fn rates_reject_malformed_zero_unordered_and_conflicting_flags() {
        for value in ["", ",", "1,", ",1", "1,,2", "abc", "1,2x", "-1", "18446744073709551616", "0", "1,0", "2,1", "1,1"] {
            assert!(parse_rates(&["--rates", value]).is_err(), "{value:?}");
        }
        for flags in [
            vec!["--rates", "1,2", "--rate-min", "1"],
            vec!["--rates", "1,2", "--rate-max", "2"],
            vec!["--rate-min", "1", "--rate-max", "2", "--rates", "1,2"],
            vec!["--rate-min", "0", "--rate-max", "2"],
            vec!["--rate-min", "2", "--rate-max", "1"],
            vec!["--rate-min", "1"],
            vec!["--rate-max", "2"],
            vec![],
        ] {
            assert!(parse_rates(&flags).is_err(), "{flags:?}");
        }
    }

    #[cfg(target_os = "linux")]
    fn parse_kernel(args: &[&str]) -> Result<(Proto, std::net::Ipv4Addr, u16), String> {
        let mut flags = Flags::parse(args.iter().map(|arg| OsString::from(*arg)))?;
        let args = kernel_args(&mut flags)?;
        flags.done()?;
        Ok(args)
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn kernel_requires_explicit_ipv4_protocol_and_nonzero_port() {
        for proto in ["udp", "tcp", "ws"] {
            let args = parse_kernel(&["--proto", proto, "--bind", "10.80.1.23", "--port", "7000"])
                .unwrap();
            assert_eq!(args.1, std::net::Ipv4Addr::new(10, 80, 1, 23));
            assert_eq!(args.2, 7000);
        }
        for args in [
            vec!["--proto", "udp", "--port", "7000"],
            vec!["--proto", "tls", "--bind", "10.80.1.23", "--port", "7000"],
            vec!["--proto", "udp", "--bind", "::1", "--port", "7000"],
            vec!["--proto", "udp", "--bind", "10.80.1.23", "--port", "0"],
            vec!["--proto", "udp", "--bind", "10.80.1.23", "--port", "65536"],
            vec!["--proto", "udp", "--bind", "10.80.1.23", "--port", "7000", "--config", "dut.json"],
        ] {
            assert!(parse_kernel(&args).is_err(), "{args:?}");
        }
    }
}
