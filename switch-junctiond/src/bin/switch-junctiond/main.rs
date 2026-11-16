//! `switch-junctiond` owns one NIC port and serves TCP and UDP sockets to applications on the same machine, through a Unix control socket and shared-memory rings in its directory. Linux only. Start it with its main thread pinned to exactly one CPU; it runs until SIGINT or SIGTERM and prints only errors, as `switch-junctiond: <reason>`.
//!
//! ```text
//! taskset -c 2 switch-junctiond --config nic.json [--dir /run/switch-junctiond] [--rx-size 1048576]
//!     [--max-sessions 64] [--max-sockets 1024] [--tcp-sockets 1024] [--udp-sockets 1024]
//!     [--control-cpus 0]
//! ```
//!
//!
//! See the switch book, chapter "Running switch-junctiond".
#[cfg(target_os = "linux")]
mod daemon;
#[cfg(target_os = "linux")]
mod flags;

fn main() -> std::process::ExitCode {
    #[cfg(target_os = "linux")]
    let result = match flags::Args::parse(std::env::args_os().skip(1)) {
        Ok(args) => daemon::run(args),
        Err(error) => Err(format!("{error}\n\n{}", flags::USAGE)),
    };
    #[cfg(not(target_os = "linux"))]
    let result: Result<(), String> = Err("switch-junctiond requires Linux".into());
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("switch-junctiond: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
