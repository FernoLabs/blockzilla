use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

use crate::epoch_slot_audit::{
    EPOCH_BITMAP_BYTES, EpochSlotAuditConfig, LocalEpochSource, OLD_FAITHFUL_SLOTS_PER_EPOCH,
    run_epoch_slot_audit,
};

static NEXT_TEST_DIR: AtomicU64 = AtomicU64::new(0);

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(label: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("test clock after Unix epoch")
            .as_nanos();
        let serial = NEXT_TEST_DIR.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "hivezilla-epoch-audit-{label}-{}-{nonce}-{serial}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create unique test directory");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

struct MockRpc {
    url: String,
    calls: Arc<AtomicUsize>,
    request_bodies: Arc<Mutex<Vec<Vec<u8>>>>,
    task: JoinHandle<()>,
}

impl MockRpc {
    async fn serving(response_body: impl Into<Vec<u8>>) -> Self {
        Self::serving_framed(response_body.into(), true, Duration::ZERO).await
    }

    async fn serving_without_content_length(response_body: impl Into<Vec<u8>>) -> Self {
        Self::serving_framed(response_body.into(), false, Duration::ZERO).await
    }

    async fn serving_delayed(response_body: impl Into<Vec<u8>>, delay: Duration) -> Self {
        Self::serving_framed(response_body.into(), true, delay).await
    }

    async fn serving_framed(
        response_body: Vec<u8>,
        include_content_length: bool,
        delay: Duration,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock RPC listener");
        let address = listener.local_addr().expect("mock RPC address");
        let calls = Arc::new(AtomicUsize::new(0));
        let request_bodies = Arc::new(Mutex::new(Vec::new()));
        let task_calls = Arc::clone(&calls);
        let task_bodies = Arc::clone(&request_bodies);
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept mock RPC request");
            task_calls.fetch_add(1, Ordering::SeqCst);
            let request_body = read_http_request_body(&mut socket).await;
            task_bodies
                .lock()
                .expect("lock request capture")
                .push(request_body);
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            let headers = if include_content_length {
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    response_body.len()
                )
            } else {
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"
                    .to_string()
            };
            socket
                .write_all(headers.as_bytes())
                .await
                .expect("write mock RPC headers");
            socket
                .write_all(&response_body)
                .await
                .expect("write mock RPC body");
            socket.shutdown().await.expect("close mock RPC response");
        });
        Self {
            url: format!("http://{address}/rpc"),
            calls,
            request_bodies,
            task,
        }
    }

    async fn finish(self) -> (usize, Vec<Vec<u8>>) {
        tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .expect("mock RPC task timed out")
            .expect("mock RPC task panicked");
        let calls = self.calls.load(Ordering::SeqCst);
        let bodies = self
            .request_bodies
            .lock()
            .expect("lock request capture")
            .clone();
        (calls, bodies)
    }
}

async fn read_http_request_body(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut received = Vec::new();
    let header_end = loop {
        if let Some(index) = received.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
        assert!(
            received.len() < 128 * 1024,
            "mock request headers too large"
        );
        let mut chunk = [0u8; 4096];
        let read = socket
            .read(&mut chunk)
            .await
            .expect("read mock RPC request");
        assert!(read > 0, "mock RPC request ended before its headers");
        received.extend_from_slice(&chunk[..read]);
    };
    let headers = String::from_utf8_lossy(&received[..header_end]);
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length").then(|| {
                value
                    .trim()
                    .parse::<usize>()
                    .expect("numeric Content-Length")
            })
        })
        .expect("mock RPC request has Content-Length");
    while received.len() - header_end < content_length {
        let mut chunk = [0u8; 4096];
        let read = socket.read(&mut chunk).await.expect("read mock RPC body");
        assert!(read > 0, "mock RPC request body was truncated");
        received.extend_from_slice(&chunk[..read]);
    }
    received[header_end..header_end + content_length].to_vec()
}

fn write_eligibility(root: &Path, epoch: u64) -> PathBuf {
    let path = root.join("eligibility.json");
    let epoch_end = epoch
        .checked_add(1)
        .and_then(|next| next.checked_mul(OLD_FAITHFUL_SLOTS_PER_EPOCH))
        .and_then(|next_start| next_start.checked_sub(1))
        .expect("test epoch bounds");
    fs::write(
        &path,
        serde_json::to_vec_pretty(&json!({
            "schema_version": 1,
            "cluster_label": "mainnet-beta",
            "finalized_through_slot": epoch_end,
            "observed_unix_secs": 1,
            "source_label": "unit-test"
        }))
        .expect("encode eligibility receipt"),
    )
    .expect("write eligibility receipt");
    path
}

fn audit_config(root: &Path, rpc_url: String) -> EpochSlotAuditConfig {
    EpochSlotAuditConfig {
        epoch: 0,
        rpc_url,
        rpc_x_token: Some("unit-test-token-that-must-not-persist".to_string()),
        provider_label: "unit-test-provider".to_string(),
        cluster_label: "mainnet-beta".to_string(),
        provider_archival_guarantee: true,
        eligibility_receipt: write_eligibility(root, 0),
        state_dir: root.join("state"),
        local_source: None,
        refresh_rpc_snapshot: false,
        timeout: Duration::from_secs(5),
        max_rpc_response_bytes: 1024 * 1024,
    }
}

fn successful_response(slots: &[u64]) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "jsonrpc": "2.0",
        "id": 0,
        "result": slots
    }))
    .expect("encode mock getBlocks response")
}

async fn seed_cached_snapshot(temp: &TestDir, slots: &[u64]) {
    let mock = MockRpc::serving(successful_response(slots)).await;
    let report = run_epoch_slot_audit(audit_config(temp.path(), mock.url.clone()))
        .await
        .expect("seed cached RPC snapshot");
    assert!(!report.rpc_snapshot_reused);
    let (calls, _) = mock.finish().await;
    assert_eq!(calls, 1);
}

#[tokio::test]
async fn one_get_blocks_call_builds_lsb0_bitmap_ranges_and_cache_is_reused() {
    let temp = TestDir::new("one-call-cache");
    let slots = [0, 2, 7, 8, OLD_FAITHFUL_SLOTS_PER_EPOCH - 1];
    let mock = MockRpc::serving(successful_response(&slots)).await;
    let config = audit_config(temp.path(), mock.url.clone());

    let first = run_epoch_slot_audit(config.clone())
        .await
        .expect("first audit succeeds");
    assert!(!first.rpc_snapshot_reused);
    assert_eq!(first.rpc_listed_slots, slots.len() as u64);
    assert_eq!(
        first.rpc_unlisted.count,
        OLD_FAITHFUL_SLOTS_PER_EPOCH - slots.len() as u64
    );
    assert_eq!(
        first.rpc_unlisted.ranges,
        vec![[1, 1], [3, 6], [9, OLD_FAITHFUL_SLOTS_PER_EPOCH - 2]]
    );
    assert!(!first.rpc_unlisted.ranges_truncated);

    let (calls, request_bodies) = mock.finish().await;
    assert_eq!(calls, 1, "one refresh must issue exactly one HTTP request");
    assert_eq!(request_bodies.len(), 1);
    let request: Value =
        serde_json::from_slice(&request_bodies[0]).expect("decode captured request");
    assert_eq!(request["method"], "getBlocks");
    assert_eq!(request["id"], 0);
    assert_eq!(
        request["params"],
        json!([0, OLD_FAITHFUL_SLOTS_PER_EPOCH - 1, {"commitment": "finalized"}])
    );

    let snapshot: Value = serde_json::from_slice(
        &fs::read(&first.rpc_snapshot_path).expect("read cached RPC snapshot"),
    )
    .expect("decode cached RPC snapshot");
    let bitmap = BASE64_STANDARD
        .decode(
            snapshot["bitmap_base64"]
                .as_str()
                .expect("snapshot bitmap string"),
        )
        .expect("decode LSB0 bitmap");
    assert_eq!(bitmap.len(), EPOCH_BITMAP_BYTES);
    assert_eq!(bitmap[0], 0b1000_0101, "offsets 0, 2, and 7 are LSB0");
    assert_eq!(bitmap[1], 0b0000_0001, "offset 8 starts the next byte");
    assert_eq!(bitmap[EPOCH_BITMAP_BYTES - 1], 0b1000_0000);

    let mut cached_config = config;
    cached_config.rpc_url = "http://127.0.0.1:1/MUST_NOT_BE_CONTACTED".to_string();
    let second = run_epoch_slot_audit(cached_config)
        .await
        .expect("cached audit succeeds without an RPC server");
    assert!(second.rpc_snapshot_reused);
    assert_eq!(second.rpc_listed_slots, first.rpc_listed_slots);
    assert_eq!(second.rpc_unlisted.ranges, first.rpc_unlisted.ranges);

    let persisted = format!(
        "{}\n{}",
        fs::read_to_string(&second.rpc_snapshot_path).expect("read cached snapshot"),
        fs::read_to_string(&second.receipt_path).expect("read audit receipt")
    );
    assert!(!persisted.contains("unit-test-token-that-must-not-persist"));
    assert!(!persisted.contains("MUST_NOT_BE_CONTACTED"));
}

#[tokio::test]
async fn strict_json_rpc_validation_rejects_ambiguous_or_invalid_responses() {
    let cases = [
        (
            r#"{"jsonrpc":"2.0","id":0,"result":null}"#,
            "exactly one non-null result or error",
        ),
        (
            r#"{"jsonrpc":"2.0","id":0,"error":null}"#,
            "exactly one non-null result or error",
        ),
        (
            r#"{"jsonrpc":"2.0","id":0,"result":[],"extra":true}"#,
            "decode typed getBlocks response",
        ),
        (
            r#"{"jsonrpc":"2.0","id":9,"result":[]}"#,
            "JSON-RPC id mismatch",
        ),
        (
            r#"{"jsonrpc":"1.0","id":0,"result":[]}"#,
            "invalid JSON-RPC version",
        ),
        (
            r#"{"jsonrpc":"2.0","id":0,"result":[1,1]}"#,
            "duplicate or unsorted",
        ),
        (
            r#"{"jsonrpc":"2.0","id":0,"result":[2,1]}"#,
            "duplicate or unsorted",
        ),
        (
            r#"{"jsonrpc":"2.0","id":0,"result":[432000]}"#,
            "out-of-range slot 432000",
        ),
        (
            r#"{"jsonrpc":"2.0","id":0,"result":[],"error":{"code":-1,"message":"bad"}}"#,
            "exactly one non-null result or error",
        ),
        (
            r#"{"jsonrpc":"2.0","id":0,"error":{"code":-1}}"#,
            "decode typed getBlocks response",
        ),
        (
            r#"{"jsonrpc":"2.0","id":0}"#,
            "exactly one non-null result or error",
        ),
    ];

    for (index, (body, expected_error)) in cases.iter().enumerate() {
        let temp = TestDir::new(&format!("strict-json-{index}"));
        let mock = MockRpc::serving(body.as_bytes().to_vec()).await;
        let error = run_epoch_slot_audit(audit_config(temp.path(), mock.url.clone()))
            .await
            .expect_err("invalid JSON-RPC response must fail");
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains(expected_error),
            "case {index} expected {expected_error:?}, got {rendered:?}"
        );
        let (calls, _) = mock.finish().await;
        assert_eq!(calls, 1, "case {index} should make exactly one HTTP call");
    }

    let temp = TestDir::new("typed-rpc-error");
    let mock = MockRpc::serving(
        br#"{"jsonrpc":"2.0","id":0,"error":{"code":-32004,"message":"provider-internal-secret"}}"#
            .to_vec(),
    )
    .await;
    let error = run_epoch_slot_audit(audit_config(temp.path(), mock.url.clone()))
        .await
        .expect_err("JSON-RPC error response must fail");
    let rendered = format!("{error:#}");
    assert!(rendered.contains("code -32004"));
    assert!(!rendered.contains("provider-internal-secret"));
    let (calls, _) = mock.finish().await;
    assert_eq!(calls, 1);
}

#[tokio::test]
async fn connection_errors_do_not_leak_the_secret_rpc_url() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve unused local port");
    let address = listener.local_addr().expect("unused local address");
    drop(listener);

    let temp = TestDir::new("secret-url");
    let sentinel = "RPC_URL_SECRET_SENTINEL";
    let url = format!("http://{address}/{sentinel}?api-key={sentinel}");
    let error = run_epoch_slot_audit(audit_config(temp.path(), url))
        .await
        .expect_err("closed local port must reject the RPC connection");
    let rendered = format!("{error:#}");
    assert!(rendered.contains("getBlocks RPC request failed"));
    assert!(
        !rendered.contains(sentinel),
        "connection error leaked its secret URL: {rendered}"
    );
}

#[tokio::test]
async fn rpc_response_limit_applies_to_content_length_and_streamed_bodies() {
    let oversized_body = vec![b'x'; 256];

    let content_length_temp = TestDir::new("content-length-limit");
    let content_length_mock = MockRpc::serving(oversized_body.clone()).await;
    let mut content_length_config =
        audit_config(content_length_temp.path(), content_length_mock.url.clone());
    content_length_config.max_rpc_response_bytes = 64;
    let error = run_epoch_slot_audit(content_length_config)
        .await
        .expect_err("oversized Content-Length must fail before JSON parsing");
    assert!(
        format!("{error:#}").contains("Content-Length 256 exceeds configured limit"),
        "unexpected Content-Length error: {error:#}"
    );
    let (calls, _) = content_length_mock.finish().await;
    assert_eq!(calls, 1);

    let streamed_temp = TestDir::new("stream-limit");
    let streamed_mock = MockRpc::serving_without_content_length(oversized_body).await;
    let mut streamed_config = audit_config(streamed_temp.path(), streamed_mock.url.clone());
    streamed_config.max_rpc_response_bytes = 64;
    let error = run_epoch_slot_audit(streamed_config)
        .await
        .expect_err("oversized close-delimited body must fail while streaming");
    assert!(
        format!("{error:#}").contains("response body exceeds configured limit"),
        "unexpected streamed-body error: {error:#}"
    );
    let (calls, _) = streamed_mock.finish().await;
    assert_eq!(calls, 1);
}

#[tokio::test]
async fn concurrent_refreshes_are_locked_before_a_second_http_call() {
    let temp = TestDir::new("concurrent-lock");
    let mock =
        MockRpc::serving_delayed(successful_response(&[0, 2, 4]), Duration::from_millis(300)).await;
    let config = audit_config(temp.path(), mock.url.clone());
    let observed_calls = Arc::clone(&mock.calls);
    let first = tokio::spawn(run_epoch_slot_audit(config.clone()));

    tokio::time::timeout(Duration::from_secs(2), async {
        while observed_calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first audit did not reach the mock RPC");

    let second_error = run_epoch_slot_audit(config)
        .await
        .expect_err("concurrent audit must not pass the epoch lock");
    assert!(
        format!("{second_error:#}").contains("epoch audit lock is held"),
        "unexpected concurrent audit error: {second_error:#}"
    );
    let first_report = first
        .await
        .expect("first audit task panicked")
        .expect("first audit succeeds");
    assert!(!first_report.rpc_snapshot_reused);
    let (calls, _) = mock.finish().await;
    assert_eq!(calls, 1, "the epoch lock must precede the RPC request");
}

#[tokio::test]
async fn synthetic_hix1_structural_index_matches_rpc_slot_membership() {
    let temp = TestDir::new("hix1");
    let archive = temp.path().join("archive");
    fs::create_dir(&archive).expect("create synthetic archive directory");
    let slots = [10, 12, 15];
    write_hix1_archive(&archive, &slots);

    let mock = MockRpc::serving(successful_response(&slots)).await;
    let mut config = audit_config(temp.path(), mock.url.clone());
    config.local_source = Some(LocalEpochSource::ArchiveDir(archive.clone()));
    let report = run_epoch_slot_audit(config)
        .await
        .expect("synthetic HIX1 audit succeeds");
    assert_eq!(report.state, "slot_coverage_verified");
    assert_eq!(report.missing_locally.count, 0);
    assert_eq!(report.extra_locally.count, 0);
    let local = report.local.expect("HIX1 local coverage summary");
    assert!(local.kind.contains("archive_v2_hix1"));
    assert_eq!(local.source_path, archive);
    assert_eq!(local.listed_slots, slots.len() as u64);
    assert_eq!(local.source_fingerprint_sha256.len(), 64);
    let (calls, _) = mock.finish().await;
    assert_eq!(calls, 1);
}

#[tokio::test]
async fn hix1_rejects_noncontiguous_frames_and_duplicate_slots() {
    let offset_temp = TestDir::new("hix1-bad-offset");
    seed_cached_snapshot(&offset_temp, &[10, 12, 15]).await;
    let offset_archive = offset_temp.path().join("archive");
    fs::create_dir(&offset_archive).expect("create corrupt HIX1 directory");
    write_hix1_archive(&offset_archive, &[10, 12, 15]);
    let index_path = offset_archive.join("archive-v2-blocks.index");
    let mut index = fs::read(&index_path).expect("read synthetic HIX1 index");
    let second_row = 36 + 52;
    index[second_row + 12..second_row + 20].copy_from_slice(&1u64.to_le_bytes());
    fs::write(&index_path, index).expect("write corrupt HIX1 offset");
    let mut config = audit_config(
        offset_temp.path(),
        "http://127.0.0.1:1/should-not-run".to_string(),
    );
    config.local_source = Some(LocalEpochSource::ArchiveDir(offset_archive));
    let error = run_epoch_slot_audit(config)
        .await
        .expect_err("noncontiguous HIX1 frames must fail locally");
    assert!(
        format!("{error:#}").contains("compressed frames are not contiguous"),
        "unexpected corrupt-offset error: {error:#}"
    );

    let duplicate_temp = TestDir::new("hix1-duplicate-slot");
    seed_cached_snapshot(&duplicate_temp, &[10, 12, 15]).await;
    let duplicate_archive = duplicate_temp.path().join("archive");
    fs::create_dir(&duplicate_archive).expect("create duplicate HIX1 directory");
    write_hix1_archive(&duplicate_archive, &[10, 12, 15]);
    let index_path = duplicate_archive.join("archive-v2-blocks.index");
    let mut index = fs::read(&index_path).expect("read synthetic HIX1 index");
    let second_row = 36 + 52;
    index[second_row + 4..second_row + 12].copy_from_slice(&10u64.to_le_bytes());
    fs::write(&index_path, index).expect("write duplicate HIX1 slot");
    let mut config = audit_config(
        duplicate_temp.path(),
        "http://127.0.0.1:1/should-not-run".to_string(),
    );
    config.local_source = Some(LocalEpochSource::ArchiveDir(duplicate_archive));
    let error = run_epoch_slot_audit(config)
        .await
        .expect_err("duplicate HIX1 slots must fail locally");
    assert!(
        format!("{error:#}").contains("slots are duplicate or unsorted"),
        "unexpected duplicate-slot error: {error:#}"
    );
}

fn write_hix1_archive(root: &Path, slots: &[u64]) {
    let compressed_lengths = [2u32, 3, 1];
    let uncompressed_lengths = [20u32, 30, 10];
    let tx_counts = [2u32, 0, 1];
    let signature_counts = [2u32, 0, 1];
    assert_eq!(slots.len(), compressed_lengths.len());
    let blob_len: u64 = compressed_lengths.iter().map(|value| *value as u64).sum();
    let mut index = vec![0u8; 36];
    index[0..8].copy_from_slice(b"BZV2HIX1");
    index[8..10].copy_from_slice(&1u16.to_le_bytes());
    index[12..20].copy_from_slice(&(slots.len() as u64).to_le_bytes());
    index[20..28].copy_from_slice(&blob_len.to_le_bytes());

    let mut compressed_offset = 0u64;
    let mut tx_ordinal = 0u64;
    let mut signature_ordinal = 0u64;
    for (block_id, slot) in slots.iter().copied().enumerate() {
        let mut row = [0u8; 52];
        row[0..4].copy_from_slice(&(block_id as u32).to_le_bytes());
        row[4..12].copy_from_slice(&slot.to_le_bytes());
        row[12..20].copy_from_slice(&compressed_offset.to_le_bytes());
        row[20..24].copy_from_slice(&compressed_lengths[block_id].to_le_bytes());
        row[24..28].copy_from_slice(&uncompressed_lengths[block_id].to_le_bytes());
        row[28..32].copy_from_slice(&tx_counts[block_id].to_le_bytes());
        row[32..40].copy_from_slice(&tx_ordinal.to_le_bytes());
        row[40..48].copy_from_slice(&signature_ordinal.to_le_bytes());
        row[48..52].copy_from_slice(&signature_counts[block_id].to_le_bytes());
        index.extend_from_slice(&row);
        compressed_offset += compressed_lengths[block_id] as u64;
        tx_ordinal += tx_counts[block_id] as u64;
        signature_ordinal += signature_counts[block_id] as u64;
    }
    fs::write(root.join("archive-v2-blocks.index"), index).expect("write synthetic HIX1");
    fs::write(
        root.join("archive-v2-blocks.zstd"),
        vec![0xa5; blob_len as usize],
    )
    .expect("write synthetic HIX1 blob");
}

#[tokio::test]
async fn synthetic_repair_bundle_unions_live_and_rpc_only_slots() {
    let temp = TestDir::new("repair-union");
    let repair = temp.path().join("repair-bundle");
    write_repair_bundle(&repair);
    let slots = [10, 12, 15];
    let mock = MockRpc::serving(successful_response(&slots)).await;
    let mut config = audit_config(temp.path(), mock.url.clone());
    config.local_source = Some(LocalEpochSource::RepairBundle(repair.clone()));

    let report = run_epoch_slot_audit(config)
        .await
        .expect("synthetic repair union audit succeeds");
    assert_eq!(report.state, "slot_coverage_verified");
    assert_eq!(report.missing_locally.count, 0);
    assert_eq!(report.extra_locally.count, 0);
    let local = report.local.expect("repair local coverage summary");
    assert_eq!(local.kind, "epoch_repair_union_v1");
    assert_eq!(local.source_path, repair);
    assert_eq!(local.listed_slots, 3);
    assert_eq!(local.source_fingerprint_sha256.len(), 64);
    let (calls, _) = mock.finish().await;
    assert_eq!(calls, 1);
}

#[tokio::test]
async fn repair_bundle_rejects_overlap_path_escape_and_wrong_poh_gap() {
    let overlap_temp = TestDir::new("repair-overlap");
    let overlap_root = overlap_temp.path().join("repair-bundle");
    write_repair_bundle(&overlap_root);
    mutate_repair_plan(&overlap_root, |rows| {
        rows[2]["source_offset"] = json!(1);
    });
    assert_repair_local_error(
        &overlap_temp,
        overlap_root,
        "source frames overlap or move backwards",
    )
    .await;

    let escape_temp = TestDir::new("repair-path-escape");
    let escape_root = escape_temp.path().join("repair-bundle");
    write_repair_bundle(&escape_root);
    mutate_repair_plan(&escape_root, |rows| {
        rows[0]["sources"][0]["block_path"] = json!("../outside-live.bin");
    });
    assert_repair_local_error(
        &escape_temp,
        escape_root,
        "manifest contains an unsafe relative path",
    )
    .await;

    let poh_temp = TestDir::new("repair-poh-gap");
    let poh_root = poh_temp.path().join("repair-bundle");
    write_repair_bundle(&poh_root);
    mutate_repair_manifest(&poh_root, |manifest| {
        manifest["poh"]["missing_record_ids"] = json!([0]);
    });
    assert_repair_local_error(
        &poh_temp,
        poh_root,
        "RPC produced ID does not match PoH gap ID",
    )
    .await;
}

async fn assert_repair_local_error(temp: &TestDir, root: PathBuf, expected: &str) {
    seed_cached_snapshot(temp, &[10, 12, 15]).await;
    let mut config = audit_config(temp.path(), "http://127.0.0.1:1/should-not-run".to_string());
    config.local_source = Some(LocalEpochSource::RepairBundle(root));
    let error = run_epoch_slot_audit(config)
        .await
        .expect_err("corrupt repair bundle must fail before RPC");
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains(expected),
        "expected repair error {expected:?}, got {rendered:?}"
    );
}

fn mutate_repair_plan(root: &Path, mutate: impl FnOnce(&mut [Value])) {
    let path = root.join("repair/live-merge-plan.jsonl");
    let mut rows = fs::read_to_string(&path)
        .expect("read synthetic repair plan")
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("decode repair plan row"))
        .collect::<Vec<_>>();
    mutate(&mut rows);
    let mut encoded = rows
        .iter()
        .map(|row| serde_json::to_string(row).expect("encode repair plan row"))
        .collect::<Vec<_>>()
        .join("\n");
    encoded.push('\n');
    fs::write(path, encoded).expect("rewrite synthetic repair plan");
}

fn mutate_repair_manifest(root: &Path, mutate: impl FnOnce(&mut Value)) {
    let marker = root.join("REPAIR-REQUIRED.json");
    let mut manifest: Value =
        serde_json::from_slice(&fs::read(&marker).expect("read synthetic repair manifest"))
            .expect("decode synthetic repair manifest");
    mutate(&mut manifest);
    let bytes = serde_json::to_vec_pretty(&manifest).expect("encode synthetic repair manifest");
    fs::write(&marker, &bytes).expect("rewrite repair marker");
    fs::write(root.join("repair/epoch-repair-manifest.json"), bytes)
        .expect("rewrite internal repair manifest");
}

fn write_repair_bundle(root: &Path) {
    fs::create_dir_all(root.join("repair")).expect("create synthetic repair directory");
    let rpc_path = root.join("repair/rpc-get-block/epoch-0/slot-12.getBlock.json");
    fs::create_dir_all(rpc_path.parent().expect("RPC source parent"))
        .expect("create synthetic RPC source directory");
    fs::write(root.join("repair/available-poh.wincode"), b"synthetic-poh")
        .expect("write synthetic PoH sidecar");
    fs::write(
        root.join("repair/produced-blockhashes.bin"),
        vec![0x11; 3 * 32],
    )
    .expect("write synthetic produced blockhashes");
    fs::write(&rpc_path, b"r").expect("write synthetic RPC block");
    fs::write(root.join("repair/live.bin"), vec![0x42; 64])
        .expect("write synthetic normalized live source");

    let rpc_metadata = fs::metadata(&rpc_path).expect("stat synthetic RPC source");
    let rpc_modified_nanos = rpc_metadata
        .modified()
        .expect("RPC source modification time")
        .duration_since(UNIX_EPOCH)
        .expect("RPC source modification time after Unix epoch")
        .as_nanos();
    #[cfg(unix)]
    let (rpc_device, rpc_inode) = {
        use std::os::unix::fs::MetadataExt;
        (Some(rpc_metadata.dev()), Some(rpc_metadata.ino()))
    };
    #[cfg(not(unix))]
    let (rpc_device, rpc_inode) = (None::<u64>, None::<u64>);
    let rpc_sha256 = Sha256::digest(b"r")
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();

    let manifest = json!({
        "version": 1,
        "state": "rpc_fallback_missing_poh_and_shredding",
        "epoch": 0,
        "epoch_start_slot": 0,
        "epoch_end_slot": OLD_FAITHFUL_SLOTS_PER_EPOCH - 1,
        "live_blocks": 2,
        "rpc_only_blocks": 1,
        "produced_blocks": 3,
        "blockhash_records": 3,
        "duplicate_live_blocks": 0,
        "first_produced_slot": 10,
        "last_produced_slot": 15,
        "poh": {
            "path": "repair/available-poh.wincode",
            "records": 2,
            "rpc_only_records_omitted": 1,
            "produced_id_space": 3,
            "record_ids_have_explicit_rpc_gaps": true,
            "missing_record_ids": [1]
        },
        "normalized_frames": {
            "current_live_finalizer_compatible": false
        },
        "rpc_only_slots": [{
            "slot": 12,
            "parent_slot": 10,
            "source_path": "repair/rpc-get-block/epoch-0/slot-12.getBlock.json",
            "source_bytes": 1,
            "source_sha256": rpc_sha256,
            "source_modified_nanos": rpc_modified_nanos,
            "source_device": rpc_device,
            "source_inode": rpc_inode
        }],
        "publication_ready": false
    });
    let manifest_bytes = serde_json::to_vec_pretty(&manifest).expect("encode repair manifest");
    fs::write(root.join("REPAIR-REQUIRED.json"), &manifest_bytes).expect("write repair marker");
    fs::write(
        root.join("repair/epoch-repair-manifest.json"),
        &manifest_bytes,
    )
    .expect("write internal repair manifest");

    let header = json!({
        "kind": "header",
        "version": 1,
        "epoch": 0,
        "expected_live_blocks": 2,
        "expected_rpc_blocks": 1,
        "expected_produced_blocks": 3,
        "block_id_space": "produced_ordinal",
        "live_rows_have_explicit_rpc_gaps": true,
        "sources": [{"source_id": 0, "block_path": "repair/live.bin"}]
    });
    let first_live = json!({
        "kind": "block",
        "block_id": 0,
        "slot": 10,
        "parent_slot": 9,
        "source_id": 0,
        "source_block_id": 0,
        "source_offset": 0,
        "block_len": 2
    });
    let second_live = json!({
        "kind": "block",
        "block_id": 2,
        "slot": 15,
        "parent_slot": 12,
        "source_id": 0,
        "source_block_id": 1,
        "source_offset": 32,
        "block_len": 3
    });
    let plan = format!(
        "{}\n{}\n{}\n",
        serde_json::to_string(&header).expect("encode repair plan header"),
        serde_json::to_string(&first_live).expect("encode first live row"),
        serde_json::to_string(&second_live).expect("encode second live row")
    );
    fs::write(root.join("repair/live-merge-plan.jsonl"), plan)
        .expect("write synthetic repair plan");
}
