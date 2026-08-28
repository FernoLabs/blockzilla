use std::process::ExitCode;

fn main() -> ExitCode {
    match blockzilla_dump::cli::run() {
        Ok(outcome) => ExitCode::from(outcome.exit_code),
        Err(error) => {
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}
