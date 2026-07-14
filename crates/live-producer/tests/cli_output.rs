use std::{path::PathBuf, process::Command};

#[test]
fn tracing_stays_on_stderr_and_stdout_remains_json() {
    let config =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config/ingest-primary.example.json");
    let output = Command::new(env!("CARGO_BIN_EXE_blockzilla-live-producer"))
        .env("RUST_LOG", "debug")
        .args(["validate-ingest-config", "--config"])
        .arg(config)
        .output()
        .expect("run live-producer CLI");

    assert!(
        output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("stdout must contain only JSON");
    assert_eq!(stdout["role"], "primary");

    let stderr = String::from_utf8(output.stderr).expect("tracing output is UTF-8");
    assert!(stderr.contains("live producer CLI started"));
    assert!(!String::from_utf8_lossy(&output.stdout).contains("live producer CLI started"));
}
