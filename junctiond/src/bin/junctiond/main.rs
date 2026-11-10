#[cfg(target_os = "linux")]
mod daemon;
#[cfg(target_os = "linux")]
mod flags;

fn main() -> std::process::ExitCode {
    #[cfg(target_os = "linux")]
    let result = flags::Args::parse(std::env::args_os().skip(1)).and_then(daemon::run);
    #[cfg(not(target_os = "linux"))]
    let result: Result<(), String> = Err("junctiond requires Linux".into());
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("junctiond: {error}");
            #[cfg(target_os = "linux")]
            eprintln!("\n{}", flags::USAGE);
            std::process::ExitCode::FAILURE
        }
    }
}
