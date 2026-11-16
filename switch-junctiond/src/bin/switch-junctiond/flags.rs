use std::collections::HashMap;
use std::ffi::OsString;
use std::path::PathBuf;
use std::str::FromStr;

pub const USAGE: &str = "\
usage: switch-junctiond --config <netconf.json>
                [--dir /run/switch-junctiond] [--rx-size 2097152]
                [--max-sessions 64] [--max-sockets 1024]
                [--tcp-sockets 1024] [--udp-sockets 1024]
                [--control-cpus 0]
The main thread must already be pinned to exactly one CPU (taskset -c CPU).
--control-cpus accepts a Linux CPU list, e.g. 0,2-3.";

pub struct Args {
    pub config: PathBuf,
    pub dir: PathBuf,
    pub rx_size: usize,
    pub max_sessions: u32,
    pub max_sockets: u16,
    pub tcp_sockets: usize,
    pub udp_sockets: usize,
    pub control_cpus: libc::cpu_set_t,
}

struct Flags(HashMap<String, String>);

impl Flags {
    fn take<T: FromStr>(&mut self, name: &str) -> Result<Option<T>, String> {
        self.0.remove(name).map(|value| {
            value.parse().map_err(|_| format!("--{name}: invalid value {value:?}"))
        }).transpose()
    }

    fn or<T: FromStr>(&mut self, name: &str, default: T) -> Result<T, String> {
        Ok(self.take(name)?.unwrap_or(default))
    }
}

impl Args {
    pub fn parse(mut args: impl Iterator<Item = OsString>) -> Result<Self, String> {
        let mut map = HashMap::new();
        while let Some(flag) = args.next() {
            let flag = flag.into_string().map_err(|flag| format!("not UTF-8: {flag:?}"))?;
            let name = flag.strip_prefix("--").ok_or_else(|| format!("unexpected argument {flag}"))?;
            let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?
                .into_string().map_err(|value| format!("{flag}: not UTF-8: {value:?}"))?;
            if map.insert(name.to_owned(), value).is_some() {
                return Err(format!("{flag} given twice"));
            }
        }
        let mut flags = Flags(map);
        let parsed = Self {
            config: flags.take("config")?.ok_or("--config is required")?,
            dir: flags.or("dir", PathBuf::from("/run/switch-junctiond"))?,
            rx_size: flags.or("rx-size", 2 * 1024 * 1024)?,
            max_sessions: flags.or("max-sessions", 64)?,
            max_sockets: flags.or("max-sockets", 1024)?,
            tcp_sockets: flags.or("tcp-sockets", 1024)?,
            udp_sockets: flags.or("udp-sockets", 1024)?,
            control_cpus: cpu_list(&flags.or("control-cpus", String::from("0"))?)?,
        };
        if let Some(name) = flags.0.keys().next() {
            return Err(format!("unknown option --{name}"));
        }
        if parsed.max_sessions == 0 || parsed.max_sockets == 0 || parsed.tcp_sockets == 0 {
            return Err("--max-sessions, --max-sockets and --tcp-sockets must be positive".into());
        }
        if !parsed.rx_size.is_power_of_two() || parsed.rx_size > 1 << 31 {
            return Err("--rx-size must be a power of two, at most 2 GiB".into());
        }
        if parsed.tcp_sockets.checked_add(parsed.udp_sockets).is_none_or(|n| n > u32::MAX as usize) {
            return Err("total Stack socket slots must fit in u32".into());
        }
        Ok(parsed)
    }
}

fn cpu_list(text: &str) -> Result<libc::cpu_set_t, String> {
    // SAFETY: an all-zero cpu_set_t is the empty CPU set.
    let mut set = unsafe { std::mem::zeroed() };
    for item in text.split(',') {
        let invalid = || format!("--control-cpus: invalid Linux CPU list {text:?}");
        let (first, last) = item.split_once('-').unwrap_or((item, item));
        let first: usize = first.parse().map_err(|_| invalid())?;
        let last: usize = last.parse().map_err(|_| invalid())?;
        if first > last || last >= libc::CPU_SETSIZE as usize {
            return Err(invalid());
        }
        for cpu in first..=last {
            // SAFETY: cpu is within CPU_SETSIZE.
            unsafe { libc::CPU_SET(cpu, &mut set) };
        }
    }
    Ok(set)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Args, String> {
        Args::parse(args.iter().map(OsString::from))
    }

    #[test]
    fn defaults_and_explicit_flags() {
        let args = parse(&["--config", "nic.json"]).unwrap();
        assert_eq!(args.dir, PathBuf::from("/run/switch-junctiond"));
        assert_eq!((args.rx_size, args.max_sessions, args.max_sockets), (2097152, 64, 1024));
        assert_eq!((args.tcp_sockets, args.udp_sockets), (1024, 1024));
        let args = parse(&["--config", "nic.json", "--dir", "ipc", "--rx-size", "65536",
            "--max-sessions", "2", "--max-sockets", "8", "--tcp-sockets", "4",
            "--udp-sockets", "0", "--control-cpus", "0,2-3"]).unwrap();
        assert_eq!(args.config, PathBuf::from("nic.json"));
        assert_eq!(args.dir, PathBuf::from("ipc"));
        assert_eq!((args.rx_size, args.max_sessions, args.max_sockets), (65536, 2, 8));
        assert_eq!((args.tcp_sockets, args.udp_sockets), (4, 0));
        assert_eq!(unsafe { libc::CPU_COUNT(&args.control_cpus) }, 3);
    }

    #[test]
    fn rejects_invalid_duplicate_unknown_and_missing_arguments() {
        for args in [
            vec![], vec!["--config"], vec!["nic.json"],
            vec!["--config", "x", "--config", "y"],
            vec!["--config", "x", "--typo", "1"],
            vec!["--config", "x", "--max-sessions", "0"],
            vec!["--config", "x", "--max-sockets", "65536"],
            vec!["--config", "x", "--tcp-sockets", "0"],
            vec!["--config", "x", "--rx-size", "3"],
        ] {
            assert!(parse(&args).is_err(), "{args:?}");
        }
        for cpus in ["", "1,", "3-2", "a", "1024", "-1", "1-2-3"] {
            assert!(cpu_list(cpus).is_err(), "{cpus:?}");
        }
    }
}
