use super::*;
use std::io::{BufRead, BufReader};

fn inventory(slots: &[u64]) -> Value {
    json!({"schema":"hivezilla-current-block-inventory-v1","blocks":slots.iter().enumerate().map(|(index,slot)|json!({"slot":slot,"bank_id":90+index,"executed_transaction_count":2,"entries_count":3,"source_sequence":index*2,"source_raw_sha256":"a".repeat(64)})).collect::<Vec<_>>()})
}
#[test]
fn range_and_coverage_match_python_oracle_with_skipped_slots() {
    let (start, end, slots) = select_block_range(&inventory(&[44, 42]), 2).unwrap();
    assert_eq!((start, end, slots.clone()), (42, 45, vec![42, 44]));
    let body = b"{\"jsonrpc\":\"2.0\",\"id\":7,\"result\":[42,44]}\n\n";
    let coverage = coverage_from_reply(
        body,
        &slots,
        44,
        &json!({"genesis_hash":"genesis","node_identity":"node"}),
        &"b".repeat(64),
    )
    .unwrap();
    assert_eq!(coverage.start_slot, 42);
    assert_eq!(coverage.end_slot_exclusive, 45);
    assert_eq!(coverage.rpc_response_sha256, digest(body));
    assert_eq!(coverage.produced_slots, vec![42, 44]);
    assert_eq!(coverage.source_receipt_sha256, "b".repeat(64));
    for (slots, count) in [
        (vec![42, 42], 2),
        (vec![431999, 432000], 2),
        (vec![42], 2),
        (vec![0], 1),
    ] {
        assert!(select_block_range(&inventory(&slots), count).is_err());
    }
    for result in [
        json!([42]),
        json!([42, 43, 44]),
        json!([44, 42]),
        json!([true, 44]),
    ] {
        assert!(
            coverage_from_reply(
                &serde_json::to_vec(&json!({"jsonrpc":"2.0","result":result})).unwrap(),
                &[42, 44],
                44,
                &json!({}),
                &"a".repeat(64)
            )
            .is_err()
        );
    }
    assert!(coverage_from_reply(body, &[42, 44], 43, &json!({}), &"a".repeat(64)).is_err());
}
#[test]
fn duplicate_keys_trailing_json_and_endpoint_changes_fail_closed() {
    for bytes in [
        br#"{"result":[],"result":[42]}"#.as_slice(),
        br#"{"nested":{"x":1,"x":2}}"#.as_slice(),
        b"{} {}",
        b"NaN",
    ] {
        assert!(strict_json(bytes).is_err());
    }
    assert_eq!(
        loopback_url("http://127.0.0.1:8899/").unwrap(),
        "http://127.0.0.1:8899"
    );
    assert_eq!(
        loopback_url("http://127.0.0.1:80/").unwrap(),
        "http://127.0.0.1:80"
    );
    for endpoint in [
        "http://localhost:8899",
        "https://127.0.0.1:8899",
        "http://user:token@127.0.0.1:8899",
        "http://127.0.0.1:8899/path",
        "http://127.0.0.1:8899?token=secret",
        "http://2130706433:8899",
        "http://0x7f000001:8899",
        "http://127.1:8899",
    ] {
        assert!(loopback_url(endpoint).is_err());
    }
}

fn fixture_server(mode: &'static str) -> (String, std::thread::JoinHandle<(Value, Vec<u8>)>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut first = String::new();
        reader.read_line(&mut first).unwrap();
        assert!(first.starts_with("POST / "));
        let mut length = 0;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line == "\r\n" {
                break;
            }
            if let Some((name, value)) = line.split_once(':') {
                if name.eq_ignore_ascii_case("content-length") {
                    length = value.trim().parse::<usize>().unwrap();
                }
            }
        }
        let mut request = vec![0u8; length];
        reader.read_exact(&mut request).unwrap();
        let request: Value = serde_json::from_slice(&request).unwrap();
        let mut reply = json!({"jsonrpc":"2.0","id":request["id"],"result":[42,44]});
        if mode == "wrong_id" {
            reply["id"] = json!(999);
        }
        if mode == "error" {
            reply.as_object_mut().unwrap().remove("result");
            reply["error"] = json!({"code":-32005,"data":{"numSlotsBehind":7}});
        }
        let body = if mode == "duplicate" {
            br#"{"jsonrpc":"2.0","id":1,"result":[],"result":[42,44]}"#.to_vec()
        } else {
            let mut b = serde_json::to_vec_pretty(&reply).unwrap();
            b.extend_from_slice(b"\n\n");
            b
        };
        let status = if mode == "redirect" {
            "302 Found"
        } else {
            "200 OK"
        };
        let extra = if mode == "redirect" {
            "Location: http://127.0.0.1:1/not-followed\r\n"
        } else if mode == "encoding" {
            "Content-Encoding: gzip\r\n"
        } else {
            ""
        };
        write!(stream,"HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{extra}Connection: close\r\n\r\n",body.len()).unwrap();
        stream.write_all(&body).unwrap();
        (request, body)
    });
    (endpoint, task)
}
#[tokio::test]
async fn real_rpc_preserves_bytes_and_rejects_redirect_error_encoding_and_wrong_id() {
    let temp = tempfile::tempdir().unwrap();
    let (endpoint, server) = fixture_server("normal");
    let rpc = Rpc::new(&endpoint, temp.path().to_path_buf()).unwrap();
    let (slots, body, path) = rpc
        .call("getBlocks", json!([42,44,{"commitment":"finalized"}]))
        .await
        .unwrap();
    let (request, expected) = server.join().unwrap();
    assert_eq!(slots, json!([42, 44]));
    assert_eq!(body, expected);
    assert_eq!(fs::read(path).unwrap(), body);
    assert_eq!(request["params"], json!([42,44,{"commitment":"finalized"}]));
    for (mode, reason) in [
        ("error", "getBlocks RPC error"),
        ("wrong_id", "RPC envelope mismatch"),
        ("duplicate", "duplicate JSON key"),
        ("redirect", "RPC status/encoding differs"),
        ("encoding", "RPC status/encoding differs"),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let (endpoint, server) = fixture_server(mode);
        let rpc = Rpc::new(&endpoint, temp.path().to_path_buf()).unwrap();
        let error = rpc
            .call("getBlocks", json!([42,44,{"commitment":"finalized"}]))
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains(reason),
            "{mode}: unexpected RPC rejection: {error:#}"
        );
        let (_, expected) = server.join().unwrap();
        assert_eq!(
            fs::read(temp.path().join("000001-getBlocks.response.json")).unwrap(),
            expected,
            "{mode}: rejected RPC response bytes must be retained exactly"
        );
    }
}
fn canary_fixture(root: &Path) -> Value {
    use blockzilla_archive_v2::{ArchiveV2HotBlockIndexRow, write_archive_v2_hot_block_index};
    let source = b"retained normalized receipt\n";
    write_new(&root.join(NORMALIZED_EXECUTION_FILE), source).unwrap();
    write_new(&root.join(ARCHIVE_V2_BLOCKS_FILE), b"123456").unwrap();
    write_new(&root.join("events.wal"), b"raw compressed source").unwrap();
    write_archive_v2_hot_block_index(
        &root.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
        6,
        1,
        0,
        &[
            ArchiveV2HotBlockIndexRow {
                block_id: 0,
                slot: 42,
                compressed_offset: 0,
                compressed_len: 3,
                uncompressed_len: 100,
                tx_count: 1,
                first_tx_ordinal: 0,
                first_signature_ordinal: 0,
                signature_count: 1,
            },
            ArchiveV2HotBlockIndexRow {
                block_id: 1,
                slot: 44,
                compressed_offset: 3,
                compressed_len: 3,
                uncompressed_len: 100,
                tx_count: 2,
                first_tx_ordinal: 1,
                first_signature_ordinal: 1,
                signature_count: 2,
            },
        ],
    )
    .unwrap();
    let files = [
        NORMALIZED_EXECUTION_FILE,
        ARCHIVE_V2_BLOCKS_FILE,
        ARCHIVE_V2_BLOCK_INDEX_FILE,
        "events.wal",
    ]
    .iter()
    .map(|name| {
        let hash = file_hash(&root.join(name)).unwrap();
        json!({"path":name,"bytes":hash.bytes,"sha256":hash.sha256})
    })
    .collect::<Vec<_>>();
    let marker = json!({"schema":"blockzilla-live-execution-canary-v1","source_profile":EXECUTION_SOURCE_PROFILE,"state":"COMPLETE","canonical":false,"publication_ready":false,"archive_verified":true,"finalized":true,"blocks":2,"start_slot":42,"end_slot_exclusive":45,"produced_blocks":[{"slot":42},{"slot":44}],"files":files,"evidence_files":["events.wal"],"source_receipt_sha256":digest(source),"transactions":3,"signatures":3,"entries":5});
    write_new(
        &root.join(EXECUTION_CANARY_FILE),
        &json_bytes(&marker).unwrap(),
    )
    .unwrap();
    marker
}
#[test]
fn marker_hashes_and_compression_totals_match_python_oracle() {
    let temp = tempfile::tempdir().unwrap();
    let marker = canary_fixture(temp.path());
    let checked = verify_canary(temp.path(), &[42, 44]).unwrap();
    let storage = &checked["storage"];
    assert_eq!(storage["hot_frames_compressed_bytes"], 6);
    assert_eq!(storage["hot_frames_uncompressed_payload_bytes"], 200);
    assert_eq!(
        storage["hot_frames_compressed_over_uncompressed_ratio"],
        0.03
    );
    assert_eq!(storage["compact_sidecars_bytes"], 140);
    assert_eq!(storage["authoritative_compressed_raw_wal_bytes"], 21);
    for (key, value) in [
        ("canonical", json!(true)),
        ("publication_ready", json!(true)),
        ("archive_verified", json!(false)),
        ("blocks", json!(3)),
    ] {
        let mut changed = marker.clone();
        changed[key] = value;
        fs::write(
            temp.path().join(EXECUTION_CANARY_FILE),
            json_bytes(&changed).unwrap(),
        )
        .unwrap();
        assert!(verify_canary(temp.path(), &[42, 44]).is_err());
    }
    fs::write(
        temp.path().join(EXECUTION_CANARY_FILE),
        json_bytes(&marker).unwrap(),
    )
    .unwrap();
    write_new(&temp.path().join("extra"), b"unlisted").unwrap();
    assert!(verify_canary(temp.path(), &[42, 44]).is_err());
    fs::remove_file(temp.path().join("extra")).unwrap();
    fs::write(temp.path().join("events.wal"), b"changed").unwrap();
    assert!(verify_canary(temp.path(), &[42, 44]).is_err());
}
#[test]
fn no_clobber_reports_and_pid_counter_guards() {
    let temp = tempfile::tempdir().unwrap();
    let target = temp.path().join("source");
    write_new(&target, b"first").unwrap();
    assert!(write_new(&target, b"replacement").is_err());
    assert_eq!(fs::read(target).unwrap(), b"first");
    save_report(temp.path(), &json!({"state":"RUNNING"})).unwrap();
    save_report(temp.path(), &json!({"state":"FAILED"})).unwrap();
    assert_eq!(
        read_json(&temp.path().join("run-report.json")).unwrap()["state"],
        "FAILED"
    );
    write_new(&temp.path().join(".run-report.tmp"), b"interrupted write").unwrap();
    assert!(save_report(temp.path(), &json!({"state":"COMPLETE"})).is_err());
    assert_eq!(
        read_json(&temp.path().join("run-report.json")).unwrap()["state"],
        "FAILED"
    );
    let before = ProcessSample {
        pid: 9,
        start_ticks: 11,
        user_cpu_seconds: 1.0,
        system_cpu_seconds: 2.0,
        rss_bytes: 3,
        threads: 4,
    };
    let mut after = before.clone();
    after.user_cpu_seconds = 4.0;
    assert_eq!(
        process_delta(&before, &after).unwrap()["user_cpu_seconds"],
        3.0
    );
    after.start_ticks = 12;
    assert!(process_delta(&before, &after).is_err());
    after.start_ticks = 11;
    after.user_cpu_seconds = 0.0;
    assert!(process_delta(&before, &after).is_err());
}
#[cfg(target_os = "linux")]
#[tokio::test]
async fn child_exit_timeout_and_future_cancellation_reap_only_owned_group() {
    let temp = tempfile::tempdir().unwrap();
    let pid = std::process::id();
    let exit = run_stage(
        "exit",
        &["/bin/sh".into(), "-c".into(), "exit 7".into()],
        temp.path(),
        Duration::from_secs(5),
        pid,
    )
    .await
    .unwrap();
    assert_eq!(exit["exit_code"], 7);
    let command = vec![
        "/bin/sh".into(),
        "-c".into(),
        "echo $$; exec /bin/sleep 20".into(),
    ];
    assert!(
        run_stage(
            "timeout",
            &command,
            temp.path(),
            Duration::from_millis(300),
            pid
        )
        .await
        .is_err()
    );
    let child: i32 = fs::read_to_string(temp.path().join("timeout.stdout"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert_eq!(unsafe { libc::kill(child, 0) }, -1);
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    );
    let future = run_stage(
        "cancel",
        &command,
        temp.path(),
        Duration::from_secs(20),
        pid,
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(300), future)
            .await
            .is_err()
    );
    let child: i32 = fs::read_to_string(temp.path().join("cancel.stdout"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert_eq!(unsafe { libc::kill(child, 0) }, -1);
    assert!(process_sample(pid).is_ok());
}
