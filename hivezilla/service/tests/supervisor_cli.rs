#[cfg(unix)]
mod unix {
    use std::{
        fs,
        os::unix::fs::PermissionsExt,
        path::PathBuf,
        process::Command,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn temp_dir(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "hivezilla-supervisor-cli-{name}-{}-{nonce}",
            std::process::id()
        ))
    }

    #[test]
    fn ready_child_runs_through_the_portable_cli_contract() {
        let root = temp_dir("ready");
        fs::create_dir_all(&root).unwrap();
        let script = root.join("ready-child.sh");
        fs::write(
            &script,
            b"#!/bin/sh\n\"$HIVEZILLA_TEST_BIN\" notify-supervisor ready\n\"$HIVEZILLA_TEST_BIN\" notify-supervisor ready\n\"$HIVEZILLA_TEST_BIN\" notify-supervisor heartbeat\nsleep 1\n",
        )
        .unwrap();
        let mut permissions = fs::metadata(&script).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&script, permissions).unwrap();

        let binary = env!("CARGO_BIN_EXE_hivezilla");
        let output = Command::new(binary)
            .env("HIVEZILLA_TEST_BIN", binary)
            .args([
                "supervise",
                "--name",
                "ready-child",
                "--state-dir",
                root.to_str().unwrap(),
                "--restart",
                "never",
                "--readiness-timeout-secs",
                "2",
                "--",
                script.to_str().unwrap(),
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let report: serde_json::Value = serde_json::Deserializer::from_slice(&output.stdout)
            .into_iter()
            .last()
            .unwrap()
            .unwrap();
        assert_eq!(report["successful"], true);
        assert_eq!(report["attempts"], 1);
        assert_eq!(report["restarts"], 0);

        let status: serde_json::Value =
            serde_json::from_slice(&fs::read(root.join("status.json")).unwrap()).unwrap();
        assert_eq!(status["state"], "exited");
        assert_eq!(status["last_event"], "child_exited_successfully");
        let notification: serde_json::Value =
            serde_json::from_slice(&fs::read(root.join("notify.json")).unwrap()).unwrap();
        assert_eq!(notification["heartbeat_sequence"], 3);
        fs::remove_dir_all(root).unwrap();
    }
}
