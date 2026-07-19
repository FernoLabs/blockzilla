use std::{path::PathBuf, process::Command};

#[test]
fn tracing_stays_on_stderr_and_stdout_remains_json() {
    let config =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config/ingest-primary.example.json");
    let output = Command::new(env!("CARGO_BIN_EXE_hivezilla"))
        .env("RUST_LOG", "debug")
        .args(["validate-ingest-config", "--config"])
        .arg(config)
        .output()
        .expect("run Hivezilla CLI");

    assert!(
        output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("stdout must contain only JSON");
    assert_eq!(stdout["role"], "primary");

    let stderr = String::from_utf8(output.stderr).expect("tracing output is UTF-8");
    assert!(stderr.contains("Hivezilla CLI started"));
    assert!(!String::from_utf8_lossy(&output.stdout).contains("Hivezilla CLI started"));
}

#[test]
fn raw_recording_requires_an_explicit_origin_node_id() {
    let output = Command::new(env!("CARGO_BIN_EXE_hivezilla"))
        .args([
            "record-grpc-raw",
            "--endpoint",
            "https://example.invalid",
            "--output-dir",
            "unused",
        ])
        .output()
        .expect("run Hivezilla CLI");

    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8(output.stderr).expect("CLI error is UTF-8");
    assert!(stderr.contains("--origin-node-id <ORIGIN_NODE_ID>"));
}
