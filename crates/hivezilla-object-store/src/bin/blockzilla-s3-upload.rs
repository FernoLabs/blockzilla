use std::process::ExitCode;

fn main() -> ExitCode {
    match hivezilla_object_store::uploader::run_from_env() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("upload failed: {error}");
            ExitCode::from(error.exit_status())
        }
    }
}
