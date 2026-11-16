//! The `switch-junctiond` daemon. Its flags, startup and shutdown are described in
//! `docs/operations/junctiond.md` in the repository.

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
