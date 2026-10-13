#[path = "../test-support/r2.rs"]
mod support;

use fs2::FileExt;
use hivezilla_object_store::uploader::{
    COMPACT_ARCHIVE_MARKER, COMPACT_ARCHIVE_MAX_FILE_BYTES, S3Client, canonical_json_bytes,
    upload_compact_archive,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use support::Server;

struct Fixture {
    _temporary: tempfile::TempDir,
    archive: PathBuf,
    state: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let archive = temporary.path().join("sealed");
        let state = temporary.path().join("publication");
        for directory in [&archive, &state] {
            fs::create_dir(directory).unwrap();
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
        }
        // Transport fixtures only. These bytes are not a real Compact V2
        // archive; the production marker must come from the archive verifier.
        let files = [
            ("blocks.bin", b"sealed block bytes".as_slice()),
            ("transactions.bin", b"sealed transaction bytes".as_slice()),
            ("empty.idx", b"".as_slice()),
        ];
        let mut records = Vec::new();
        for (name, bytes) in files {
            fs::write(archive.join(name), bytes).unwrap();
            records.push(json!({"path": name, "bytes": bytes.len(), "sha256": sha256(bytes)}));
        }
        let marker = json!({
            "schema": "blockzilla-live-execution-canary-v1",
            "source_profile": "validator-finalized-execution-v1",
            "state": "COMPLETE", "canonical": false, "publication_ready": false,
            "finalized": true, "archive_verified": true,
            "source_receipt_sha256": "a".repeat(64),
            "missing_capabilities": ["shred-evidence", "independent-finality-proof"],
            "files": records,
        });
        write_marker(&archive, &marker);
        Self {
            _temporary: temporary,
            archive,
            state,
        }
    }

    fn marker(&self) -> Value {
        serde_json::from_slice(&fs::read(self.archive.join(COMPACT_ARCHIVE_MARKER)).unwrap())
            .unwrap()
    }

    fn publish(&self, server: &Server) -> hivezilla_object_store::uploader::Result<Value> {
        upload_compact_archive(
            &S3Client::new(server.settings(), 0).unwrap(),
            &self.archive,
            "execution-canary/node-a/run-1",
            &self.state,
        )
    }

    fn object_key(&self, name: &str) -> String {
        let marker = fs::read(self.archive.join(COMPACT_ARCHIVE_MARKER)).unwrap();
        format!(
            "execution-canary/node-a/run-1/{}/files/{name}",
            sha256(&marker)
        )
    }
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
fn write_marker(directory: &Path, value: &Value) {
    fs::write(
        directory.join(COMPACT_ARCHIVE_MARKER),
        canonical_json_bytes(value).unwrap(),
    )
    .unwrap();
}

#[test]
fn manifest_is_last_write_and_exact_retry_rechecks_every_object() {
    let fixture = Fixture::new();
    let server = Server::start();
    let report = fixture.publish(&server).unwrap();
    assert_eq!(report["state"], "COMPLETE");
    assert_eq!(report["canonical"], false);
    assert_eq!(report["publication_ready"], false);
    assert_eq!(report["source_retirement_authorized"], false);
    assert_eq!(report["file_count"], 3);
    assert!(fixture.state.join("intent.json").is_file());
    assert!(fixture.state.join("complete.json").is_file());
    assert_eq!(server.count("PUT"), 4);
    let gets_before = server.count("GET");
    assert_eq!(fixture.publish(&server).unwrap(), report);
    assert_eq!(server.count("PUT"), 4);
    assert_eq!(server.count("GET") - gets_before, 4);
    let state = server.state.lock().unwrap();
    let puts = state
        .requests
        .iter()
        .filter(|(method, _, _, _)| method == "PUT")
        .collect::<Vec<_>>();
    assert!(
        puts.last()
            .unwrap()
            .1
            .ends_with("/EXECUTION-CANARY-MANIFEST.json")
    );
    let manifest: Value = serde_json::from_slice(&puts.last().unwrap().3).unwrap();
    assert_eq!(
        manifest["source_marker"]["missing_capabilities"][0],
        "shred-evidence"
    );
    assert!(
        puts.iter()
            .all(|(_, _, headers, _)| headers["if-none-match"] == "*")
    );
    assert!(
        !state
            .requests
            .iter()
            .any(|(method, _, _, _)| method == "DELETE")
    );
    assert!(
        state
            .objects
            .keys()
            .all(|key| !key.ends_with("/READY") && !key.ends_with("archive-v2-generation.json"))
    );
}

#[test]
fn lost_reply_leaves_intent_and_restart_finishes_without_overwrite() {
    let fixture = Fixture::new();
    let server = Server::start();
    server.state.lock().unwrap().drop_put_response = true;
    assert!(fixture.publish(&server).is_err());
    assert!(fixture.state.join("intent.json").is_file());
    assert!(!fixture.state.join("complete.json").exists());
    assert_eq!(server.count("PUT"), 1);
    fixture.publish(&server).unwrap();
    assert_eq!(server.count("PUT"), 4);
    assert!(fixture.archive.join("blocks.bin").is_file());
}

#[test]
fn existing_or_racing_wrong_bytes_block_commit_and_remain_unchanged() {
    for racing in [false, true] {
        let fixture = Fixture::new();
        let server = Server::start();
        let wrong = b"other block bytes!".to_vec();
        let key = fixture.object_key("blocks.bin");
        if racing {
            server.state.lock().unwrap().concurrent_put = Some(wrong.clone());
        } else {
            server
                .state
                .lock()
                .unwrap()
                .objects
                .insert(key.clone(), wrong.clone());
        }
        assert!(fixture.publish(&server).is_err());
        assert!(!fixture.state.join("complete.json").exists());
        let state = server.state.lock().unwrap();
        assert_eq!(state.objects[&key], wrong);
        assert!(
            !state
                .objects
                .keys()
                .any(|key| key.ends_with("EXECUTION-CANARY-MANIFEST.json"))
        );
    }
}

#[test]
fn identical_racing_create_is_verified_and_committed() {
    let fixture = Fixture::new();
    let server = Server::start();
    server.state.lock().unwrap().concurrent_put = Some(b"sealed block bytes".to_vec());
    fixture.publish(&server).unwrap();
    assert_eq!(server.state.lock().unwrap().objects.len(), 4);
}

#[test]
fn bad_source_files_and_incomplete_or_canonical_markers_fail_before_network() {
    for mutation in [
        "digest",
        "length",
        "duplicate",
        "traversal",
        "raw",
        "unverified",
        "canonical",
        "ready",
        "unfinalized",
        "oversized",
    ] {
        let fixture = Fixture::new();
        let server = Server::start();
        let mut marker = fixture.marker();
        match mutation {
            "digest" => marker["files"][0]["sha256"] = json!("0".repeat(64)),
            "length" => marker["files"][0]["bytes"] = json!(10),
            "duplicate" => {
                let first = marker["files"][0].clone();
                marker["files"].as_array_mut().unwrap().push(first);
            }
            "traversal" => marker["files"][0]["path"] = json!("../blocks.bin"),
            "raw" => marker["source_profile"] = json!("raw-format-3"),
            "unverified" => marker["archive_verified"] = json!(false),
            "canonical" => marker["canonical"] = json!(true),
            "ready" => marker["publication_ready"] = json!(true),
            "unfinalized" => marker["finalized"] = json!(false),
            "oversized" => marker["files"][0]["bytes"] = json!(COMPACT_ARCHIVE_MAX_FILE_BYTES + 1),
            _ => unreachable!(),
        }
        write_marker(&fixture.archive, &marker);
        assert!(fixture.publish(&server).is_err(), "{mutation}");
        assert!(
            server.state.lock().unwrap().requests.is_empty(),
            "{mutation}"
        );
    }
}

#[test]
fn corrupted_readback_never_commits_and_retry_recovers() {
    for mode in ["digest", "short", "encoding", "head"] {
        let fixture = Fixture::new();
        let server = Server::start();
        {
            let mut state = server.state.lock().unwrap();
            state.corrupt_get = mode == "digest";
            state.short_get = mode == "short";
            state.encoded_get = mode == "encoding";
            if mode == "head" {
                state.objects.insert(
                    fixture.object_key("blocks.bin"),
                    b"sealed block bytes".to_vec(),
                );
                state.large_head = true;
            }
        }
        assert!(fixture.publish(&server).is_err(), "{mode}");
        assert!(!fixture.state.join("complete.json").exists());
        {
            let mut state = server.state.lock().unwrap();
            state.corrupt_get = false;
            state.short_get = false;
            state.encoded_get = false;
            state.large_head = false;
        }
        fixture.publish(&server).unwrap();
    }
}

#[test]
fn durable_intent_rejects_namespace_or_source_change_before_network() {
    let fixture = Fixture::new();
    let server = Server::start();
    fixture.publish(&server).unwrap();
    let requests_before = server.state.lock().unwrap().requests.len();
    assert!(
        upload_compact_archive(
            &S3Client::new(server.settings(), 0).unwrap(),
            &fixture.archive,
            "execution-canary/other",
            &fixture.state
        )
        .is_err()
    );
    let mut marker = fixture.marker();
    marker["source_receipt_sha256"] = json!("b".repeat(64));
    write_marker(&fixture.archive, &marker);
    assert!(fixture.publish(&server).is_err());
    assert_eq!(server.state.lock().unwrap().requests.len(), requests_before);
}

#[test]
fn unsafe_paths_or_state_fail_without_remote_requests() {
    for mode in [
        "file-symlink",
        "marker-symlink",
        "state-inside",
        "state-symlink",
        "state-writable",
        "prefix",
    ] {
        let fixture = Fixture::new();
        let server = Server::start();
        let mut state_path = fixture.state.clone();
        let mut prefix = "execution-canary/node-a/run-1";
        match mode {
            "file-symlink" => {
                fs::rename(
                    fixture.archive.join("blocks.bin"),
                    fixture.archive.join("real.bin"),
                )
                .unwrap();
                symlink("real.bin", fixture.archive.join("blocks.bin")).unwrap();
            }
            "marker-symlink" => {
                fs::rename(
                    fixture.archive.join(COMPACT_ARCHIVE_MARKER),
                    fixture.archive.join("real.json"),
                )
                .unwrap();
                symlink("real.json", fixture.archive.join(COMPACT_ARCHIVE_MARKER)).unwrap();
            }
            "state-inside" => state_path = fixture.archive.clone(),
            "state-symlink" => {
                state_path = fixture._temporary.path().join("link");
                symlink(&fixture.state, &state_path).unwrap();
            }
            "state-writable" => {
                fs::set_permissions(&fixture.state, fs::Permissions::from_mode(0o777)).unwrap()
            }
            "prefix" => prefix = "canonical/archives",
            _ => unreachable!(),
        }
        assert!(
            upload_compact_archive(
                &S3Client::new(server.settings(), 0).unwrap(),
                &fixture.archive,
                prefix,
                &state_path
            )
            .is_err(),
            "{mode}"
        );
        assert!(server.state.lock().unwrap().requests.is_empty(), "{mode}");
    }
}

#[test]
fn secret_remote_error_text_is_not_reported() {
    let fixture = Fixture::new();
    let server = Server::start();
    server.state.lock().unwrap().fail_put = true;
    let error = fixture.publish(&server).unwrap_err().to_string();
    assert!(error.contains("503"));
    for secret in ["secret-response", "test-secret", server.endpoint.as_str()] {
        assert!(!error.contains(secret));
    }
}

#[test]
fn busy_state_lock_blocks_a_second_publisher_before_network() {
    let fixture = Fixture::new();
    let server = Server::start();
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(fixture.state.join("publisher.lock"))
        .unwrap();
    lock.try_lock_exclusive().unwrap();
    assert!(
        fixture
            .publish(&server)
            .unwrap_err()
            .to_string()
            .contains("busy")
    );
    assert!(server.state.lock().unwrap().requests.is_empty());
    drop(lock);
    fixture.publish(&server).unwrap();
}

#[test]
fn preexisting_intent_collision_or_symlink_is_never_replaced() {
    for linked in [false, true] {
        let fixture = Fixture::new();
        let server = Server::start();
        let target = fixture._temporary.path().join("keep.json");
        fs::write(&target, b"keep this record").unwrap();
        if linked {
            symlink(&target, fixture.state.join("intent.json")).unwrap();
        } else {
            fs::write(fixture.state.join("intent.json"), b"different intent").unwrap();
        }
        assert!(fixture.publish(&server).is_err());
        assert!(server.state.lock().unwrap().requests.is_empty());
        assert_eq!(fs::read(&target).unwrap(), b"keep this record");
        if !linked {
            assert_eq!(
                fs::read(fixture.state.join("intent.json")).unwrap(),
                b"different intent"
            );
        }
    }
}

#[test]
fn cli_lists_compact_archive_command() {
    let result = std::process::Command::new(env!("CARGO_BIN_EXE_blockzilla-s3-upload"))
        .args(["upload-compact-archive", "--help"])
        .output()
        .unwrap();
    assert!(result.status.success());
    let help = String::from_utf8(result.stdout).unwrap();
    assert!(help.contains("ARCHIVE_DIR") && help.contains("STATE_DIR"));
}
