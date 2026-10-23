use super::*;
use serde_json::{Value, json};
use tempfile::TempDir;

fn source() -> SourceIdentity {
    SourceIdentity {
        genesis_hash: "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d".into(),
        node_identity: "J7qpvCEWhdBgPdYRV3s3btWPrRVXa2qnAzgvxWTvtbrQ".into(),
        agave_revision: codec::NATIVE_REVISION.into(),
        session_id: "fixture-source-session".into(),
    }
}
fn bank(id: u64) -> BankIdentity {
    BankIdentity {
        slot: 460170404,
        bank_id: id,
        parent_slot: 460170403,
        blockhash: [4; 32],
        parent_blockhash: [3; 32],
    }
}
fn block_frame(source: SourceIdentity, bank: BankIdentity) -> Vec<u8> {
    let event = v2::BankEvent {
        sequence: 7,
        source_sequence: 15,
        kind: "block".into(),
        bank: bank.clone(),
        payload_sha256: v2::digest(b"the exact old execution marker"),
        payload_frame_sha256: None,
        commitment: Some("processed".into()),
        confirmation_basis: Some("operator-trusted-agave-processed-bank-callback".into()),
        block_height: Some(438202794),
        recent_blockhash: None,
        observed_at_unix_ms: 1792700777890,
        reason: None,
    };
    let objects: Vec<_> = v2::REQUIRED
        .iter()
        .enumerate()
        .map(|(offset, name)| v2::Object {
            path: (*name).into(),
            offset,
            bytes: 1,
            sha256: v2::digest(&[42]),
        })
        .collect();
    let mut marker = serde_json::to_value(&source).unwrap();
    marker["schema"] = "hivezilla-direct-agave-processed-compact-v1".into();
    marker["state"] = "COMPLETE".into();
    marker["source_profile"] = "agave-bank-callbacks-v1".into();
    marker["canonical"] = false.into();
    marker["publication_ready"] = false.into();
    marker["finalized"] = false.into();
    marker["produced_blocks"] = json!([bank]);
    marker["files"] = json!(
        objects
            .iter()
            .map(|o| json!({"path":o.path,"bytes":o.bytes,"sha256":o.sha256}))
            .collect::<Vec<_>>()
    );
    v2::encode(
        v2::Envelope {
            schema: v2::ENVELOPE.into(),
            profile: v2::PROFILE.into(),
            evidence_included: false,
            marker,
            objects,
            event,
        },
        &vec![42; v2::REQUIRED.len()],
    )
    .unwrap()
    .wire
    .to_vec()
}
fn wire() -> Vec<u8> {
    let mut bytes = vec![1]; // VersionedBlockFooter::V1, no marker wrapper.
    bytes.extend_from_slice(&[0x55; 32]);
    bytes.extend_from_slice(&1_792_700_777_890_000_123_u64.to_le_bytes());
    bytes.push(3);
    bytes.extend_from_slice(&[0xff, 0, 0xfe]); // User agent is not UTF-8.
    bytes.extend_from_slice(&[0, 0, 0]); // No optional certificates.
    bytes
}
fn raw_footer(id: u64, footer_wire: &[u8]) -> Vec<u8> {
    serde_json::to_vec(
        &json!({"kind":"Footer","slot":460170404,"bank_id":id,"wire_footer":footer_wire}),
    )
    .unwrap()
}
fn evidence(sequence: u64, raw: &[u8]) -> Value {
    // This fixture returns a Value, which sorts object keys. Hash the exact
    // event serialization that source_file() will emit, not its input spelling.
    let event: Value = serde_json::from_slice(raw).unwrap();
    let emitted = serde_json::to_vec(&event).unwrap();
    json!({"sequence":sequence,"sha256":v2::digest(&emitted),"event":event})
}
struct Fixture {
    _root: TempDir,
    root: PathBuf,
    binding: FactBinding,
    event: PathBuf,
    payload: PathBuf,
    output: PathBuf,
    raw: Vec<u8>,
}
impl Fixture {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let root = temp.path().to_owned();
        let payload = root.join("payload.bzstrm");
        let frame = block_frame(source(), bank(2403404));
        fs::write(&payload, &frame).unwrap();
        let raw = raw_footer(2403404, &wire());
        let event = root.join("event.json");
        fs::write(&event, &raw).unwrap();
        let binding = FactBinding {
            source: source(),
            bank: bank(2403404),
            execution_payload_sha256: v2::digest(&frame),
            source_event_sequence: 3384311167,
            source_event_sha256: v2::digest(&raw),
        };
        let output = root.join("export");
        Self {
            _root: temp,
            root,
            binding,
            event,
            payload,
            output,
            raw,
        }
    }
    fn export(&self) -> Result<ExportReceipt> {
        export_event(&self.binding, &self.event, &self.payload, &self.output)
    }
    fn source_file(&self, rows: &[Value], compressed: bool) -> PathBuf {
        let bytes = serde_json::to_vec(rows).unwrap();
        let path = self.root.join(if compressed {
            "source-events.json.zst"
        } else {
            "source-events.json"
        });
        fs::write(
            &path,
            if compressed {
                zstd::stream::encode_all(bytes.as_slice(), 1).unwrap()
            } else {
                bytes
            },
        )
        .unwrap();
        path
    }
}
fn replace(path: &Path, bytes: &[u8]) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }
    fs::write(path, bytes).unwrap();
}

#[test]
fn consensus_export_binds_actual_frame_and_exact_event_then_replays_without_rewrite() {
    let f = Fixture::new();
    let receipt = f.export().unwrap();
    assert_eq!(
        receipt.execution_digest_domain,
        "exact execution frame bytes"
    );
    assert!(receipt.no_source_retirement_authority);
    assert!(!receipt.continuous_late_evidence_coverage);
    assert_eq!(receipt.verification, "captured_not_verified");
    assert_eq!(receipt.facts[0].binding, f.binding);
    assert_eq!(receipt.input.sha256, v2::digest(&f.raw));
    let path = f.output.join(&receipt.facts[0].path);
    let fact_bytes = fs::read(&path).unwrap();
    let fact = codec::decode_bound(&fact_bytes, &f.binding).unwrap();
    assert_eq!(fact.footer_wire(), wire());
    let time = fs::metadata(&path).unwrap().modified().unwrap();
    let original_receipt = fs::read(f.output.join(RECEIPT)).unwrap();
    assert_eq!(f.export().unwrap(), receipt);
    assert_eq!(fs::read(f.output.join(RECEIPT)).unwrap(), original_receipt);
    assert_eq!(fs::metadata(path).unwrap().modified().unwrap(), time);
    let inspected = inspect_export(&f.output).unwrap();
    assert!(inspected.local_hashes_consistent);
    assert!(!inspected.provenance_authenticated);
    assert_eq!(
        inspected.footers[0]["block_producer_time_nanos"],
        "1792700777890000123"
    );
    assert_eq!(
        inspected.footers[0]["block_user_agent"],
        json!([255, 0, 254])
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(f.output.join(RECEIPT))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o444
        );
    }
}

#[test]
fn consensus_export_checks_raw_evidence_sequence_and_original_event_bytes() {
    let f = Fixture::new();
    // Pretty printing the outer record must not change the inner event's SHA.
    let record = format!(
        "{{\n\"sequence\":{},\n\"sha256\":\"{}\",\n\"event\":{}\n}}",
        f.binding.source_event_sequence,
        f.binding.source_event_sha256,
        String::from_utf8(f.raw.clone()).unwrap()
    );
    fs::write(&f.event, record).unwrap();
    let receipt = f.export().unwrap();
    assert_eq!(receipt.input.kind, "raw-evidence-record");
    assert_eq!(
        receipt.facts[0].binding.source_event_sha256,
        v2::digest(&f.raw)
    );
    let bad = Fixture::new();
    fs::write(
        &bad.event,
        serde_json::to_vec(&evidence(bad.binding.source_event_sequence + 1, &bad.raw)).unwrap(),
    )
    .unwrap();
    assert!(bad.export().is_err());
    assert!(!bad.output.exists());
}

#[test]
fn consensus_export_rejects_payload_digest_source_bank_and_nonframe_before_writes() {
    for fault in ["digest", "source", "bank", "nonframe"] {
        let mut f = Fixture::new();
        match fault {
            "digest" => f.binding.execution_payload_sha256 = "00".repeat(32),
            "source" => f.binding.source.session_id = "another-session".into(),
            "bank" => f.binding.bank.bank_id += 1,
            "nonframe" => {
                fs::write(&f.payload, b"small arbitrary file").unwrap();
                f.binding.execution_payload_sha256 = v2::digest(b"small arbitrary file");
            }
            _ => unreachable!(),
        }
        assert!(f.export().is_err(), "{fault}");
        assert!(!f.output.exists(), "{fault}");
    }
}

#[test]
fn consensus_export_rejects_control_frame_even_with_matching_source_bank_and_sha() {
    let mut f = Fixture::new();
    let payload = v2::decode(fs::read(&f.payload).unwrap().into()).unwrap();
    let mut event = payload.envelope.event.clone();
    event.sequence += 1;
    event.kind = "invalidate".into();
    event.payload_frame_sha256 = Some(payload.sha256.clone());
    event.commitment = None;
    event.confirmation_basis = None;
    event.block_height = None;
    event.reason = Some("agave_bank_invalidated".into());
    let frame = v2::encode(
        v2::Envelope {
            schema: v2::ENVELOPE.into(),
            profile: v2::PROFILE.into(),
            evidence_included: false,
            marker: v2::control_marker(&f.binding.source, &event),
            objects: vec![],
            event,
        },
        &[],
    )
    .unwrap();
    f.binding.execution_payload_sha256 = frame.sha256;
    fs::write(&f.payload, &frame.wire).unwrap();
    assert!(f.export().is_err());
    assert!(!f.output.exists());
}

#[test]
fn consensus_source_export_creates_one_bound_fact_per_footer_from_json_and_zstd() {
    for compressed in [false, true] {
        let f = Fixture::new();
        let raw_status = serde_json::to_vec(&json!({"kind":"Status","slot":460170404,"bank_id":null,"status":"processed","parent":460170403,"dead_error":null})).unwrap();
        let path = f.source_file(
            &[
                evidence(100, &raw_status),
                evidence(101, &f.raw),
                evidence(102, &f.raw),
            ],
            compressed,
        );
        let receipt = export_source_events(
            &ExecutionBinding::from(&f.binding),
            &path,
            &f.payload,
            &f.output,
        )
        .unwrap();
        assert_eq!(receipt.facts.len(), 2);
        assert_eq!(receipt.facts[0].binding.source_event_sequence, 101);
        assert_eq!(receipt.facts[1].binding.source_event_sequence, 102);
        assert_eq!(
            receipt.facts[0].footer_sha256,
            receipt.facts[1].footer_sha256
        );
        assert_ne!(receipt.facts[0].sha256, receipt.facts[1].sha256);
        assert_eq!(receipt.input.sha256, v2::digest(&fs::read(path).unwrap()));
        assert_eq!(inspect_export(&f.output).unwrap().footers.len(), 2);
    }
}

#[test]
fn consensus_source_export_stops_on_conflicting_footer_bytes_for_the_same_bank() {
    let f = Fixture::new();
    let mut changed = wire();
    changed[1] ^= 1; // A different, well-formed claimed bank hash.
    let other = raw_footer(f.binding.bank.bank_id, &changed);
    let path = f.source_file(&[evidence(101, &f.raw), evidence(102, &other)], false);
    let error = export_source_events(
        &ExecutionBinding::from(&f.binding),
        &path,
        &f.payload,
        &f.output,
    )
    .unwrap_err();
    assert_eq!(error.to_string(), "conflicting_bank_footer");
    assert!(!f.output.exists());
}

#[test]
fn consensus_export_caps_aggregate_footer_bytes_before_output_or_json_inspection() {
    let f = Fixture::new();
    let mut large = vec![1];
    large.extend_from_slice(&[0x55; 32]);
    large.extend_from_slice(&1_792_700_777_890_000_123_u64.to_le_bytes());
    large.push(0); // Empty user agent.
    large.push(1); // Finalization cert with an opaque 60,000-byte bitmap.
    large.extend_from_slice(&460170400_u64.to_le_bytes());
    large.extend_from_slice(&[8; 32]);
    large.extend_from_slice(&[0; 96]);
    large.extend_from_slice(&60000_u16.to_le_bytes());
    large.extend_from_slice(&vec![0; 60000]);
    large.extend_from_slice(&[0, 0, 0]);
    codec::decode_footer(&large).unwrap();
    let raw = raw_footer(f.binding.bank.bank_id, &large);
    let count = MAX_TOTAL_FOOTER_BYTES / large.len() + 1;
    let mut input = Vec::new();
    input.push(b'[');
    for sequence in 1..=count {
        if sequence > 1 {
            input.push(b',');
        }
        input.extend_from_slice(
            format!(
                "{{\"sequence\":{sequence},\"sha256\":\"{}\",\"event\":",
                v2::digest(&raw)
            )
            .as_bytes(),
        );
        input.extend_from_slice(&raw);
        input.push(b'}');
    }
    input.push(b']');
    let path = f.root.join("large-source.json");
    fs::write(&path, input).unwrap();
    let error = export_source_events(
        &ExecutionBinding::from(&f.binding),
        &path,
        &f.payload,
        &f.output,
    )
    .unwrap_err();
    assert_eq!(error.to_string(), "footer_total_too_large");
    assert!(!f.output.exists());

    let receipt = f.export().unwrap();
    let mut forged = receipt.clone();
    forged.facts = (1..=count)
        .map(|sequence| {
            let mut row = receipt.facts[0].clone();
            row.binding.source_event_sequence = sequence as u64;
            row.path = fact_name(&row.binding);
            row.footer_bytes = large.len();
            row
        })
        .collect();
    replace(
        &f.output.join(RECEIPT),
        &serde_json::to_vec(&forged).unwrap(),
    );
    // Fail before reading the nonexistent paths or constructing footer JSON.
    assert_eq!(
        inspect_export(&f.output).unwrap_err().to_string(),
        "footer_total_too_large"
    );
}

#[test]
fn consensus_source_export_rejects_mixed_banks_bad_hash_order_and_unknown_records() {
    for fault in ["bank", "slot", "hash", "order", "duplicate", "unknown"] {
        let f = Fixture::new();
        let mut row = evidence(102, &f.raw);
        match fault {
            "bank" | "slot" | "unknown" => {
                let mut event: Value = serde_json::from_slice(&f.raw).unwrap();
                match fault {
                    "bank" => event["bank_id"] = 1.into(),
                    "slot" => event["slot"] = 1.into(),
                    _ => event["kind"] = "UnknownCallback".into(),
                }
                row = evidence(102, &serde_json::to_vec(&event).unwrap());
            }
            "hash" => row["sha256"] = "00".repeat(32).into(),
            "order" => row["sequence"] = 100.into(),
            "duplicate" => row["sequence"] = 101.into(),
            _ => unreachable!(),
        }
        let path = f.source_file(&[evidence(101, &f.raw), row], false);
        assert!(
            export_source_events(
                &ExecutionBinding::from(&f.binding),
                &path,
                &f.payload,
                &f.output
            )
            .is_err(),
            "{fault}"
        );
        assert!(!f.output.exists());
    }
}

#[test]
fn consensus_source_export_does_not_claim_absence_is_preactivation() {
    let f = Fixture::new();
    let raw = br#"{"kind":"Entry","slot":460170404,"bank_id":2403404}"#;
    let path = f.source_file(&[evidence(101, raw)], false);
    let error = export_source_events(
        &ExecutionBinding::from(&f.binding),
        &path,
        &f.payload,
        &f.output,
    )
    .unwrap_err();
    assert_eq!(error.to_string(), "source_events_have_no_footer");
    assert!(!f.output.exists());
}

#[test]
fn consensus_export_rejects_event_identity_digest_and_invalid_footer_without_receipt() {
    for fault in ["hash", "bank", "footer", "duplicate-key", "oversized"] {
        let mut f = Fixture::new();
        match fault {
            "hash" => f.binding.source_event_sha256 = "00".repeat(32),
            "bank" => {
                let raw = raw_footer(42, &wire());
                f.binding.source_event_sha256 = v2::digest(&raw);
                fs::write(&f.event, raw).unwrap();
            }
            "footer" => {
                let raw = raw_footer(f.binding.bank.bank_id, &[1, 2, 3]);
                f.binding.source_event_sha256 = v2::digest(&raw);
                fs::write(&f.event, raw).unwrap();
            }
            "duplicate-key" => {
                let raw = String::from_utf8(f.raw.clone()).unwrap().replacen(
                    "{",
                    "{\"kind\":\"Footer\",",
                    1,
                );
                f.binding.source_event_sha256 = v2::digest(raw.as_bytes());
                fs::write(&f.event, raw).unwrap();
            }
            "oversized" => {
                File::create(&f.event)
                    .unwrap()
                    .set_len(MAX_EVENT_BYTES as u64 + 1)
                    .unwrap();
            }
            _ => unreachable!(),
        }
        assert!(f.export().is_err(), "{fault}");
        assert!(!f.output.exists());
    }
}

#[test]
fn consensus_export_resumes_after_fact_publication_before_receipt_without_rewriting_fact() {
    let f = Fixture::new();
    let receipt = f.export().unwrap();
    let fact_path = f.output.join(&receipt.facts[0].path);
    let fact_time = fs::metadata(&fact_path).unwrap().modified().unwrap();
    let original = fs::read(&fact_path).unwrap();
    fs::remove_file(f.output.join(RECEIPT)).unwrap();
    assert_eq!(f.export().unwrap(), receipt);
    assert_eq!(fs::read(&fact_path).unwrap(), original);
    assert_eq!(
        fs::metadata(fact_path).unwrap().modified().unwrap(),
        fact_time
    );
    assert!(inspect_export(&f.output).is_ok());
}

#[test]
fn consensus_export_never_repairs_committed_missing_or_changed_fact_and_never_overwrites() {
    for missing in [true, false] {
        let f = Fixture::new();
        let receipt = f.export().unwrap();
        let receipt_before = fs::read(f.output.join(RECEIPT)).unwrap();
        let path = f.output.join(&receipt.facts[0].path);
        if missing {
            fs::remove_file(&path).unwrap();
        } else {
            replace(&path, b"corrupt fact bytes");
        }
        assert!(f.export().is_err());
        assert!(inspect_export(&f.output).is_err());
        assert_eq!(fs::read(f.output.join(RECEIPT)).unwrap(), receipt_before);
        if !missing {
            assert_eq!(fs::read(path).unwrap(), b"corrupt fact bytes");
        } else {
            assert!(!path.exists());
        }
    }
}

#[test]
fn consensus_export_refuses_different_input_receipt_or_unrelated_output_files() {
    let f = Fixture::new();
    f.export().unwrap();
    let receipt_before = fs::read(f.output.join(RECEIPT)).unwrap();
    fs::write(
        &f.event,
        serde_json::to_vec(&evidence(f.binding.source_event_sequence, &f.raw)).unwrap(),
    )
    .unwrap();
    assert!(f.export().is_err());
    assert_eq!(fs::read(f.output.join(RECEIPT)).unwrap(), receipt_before);
    let other = Fixture::new();
    fs::create_dir(&other.output).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&other.output, fs::Permissions::from_mode(0o700)).unwrap();
    }
    fs::write(
        other.output.join("DIRECT-AGAVE-COMPACT.v1.json"),
        b"old execution marker",
    )
    .unwrap();
    assert!(other.export().is_err());
    assert!(!other.output.join(LOCK).exists());
    assert_eq!(
        fs::read(other.output.join("DIRECT-AGAVE-COMPACT.v1.json")).unwrap(),
        b"old execution marker"
    );
}

#[test]
fn consensus_export_inspection_rejects_false_authority_and_binding_claims() {
    for field in [
        "no_source_retirement_authority",
        "continuous_late_evidence_coverage",
        "binding",
    ] {
        let f = Fixture::new();
        f.export().unwrap();
        let mut receipt: Value =
            serde_json::from_slice(&fs::read(f.output.join(RECEIPT)).unwrap()).unwrap();
        match field {
            "no_source_retirement_authority" => receipt[field] = false.into(),
            "continuous_late_evidence_coverage" => receipt[field] = true.into(),
            _ => receipt["facts"][0]["binding"]["bank"]["bank_id"] = 42.into(),
        }
        replace(
            &f.output.join(RECEIPT),
            &serde_json::to_vec(&receipt).unwrap(),
        );
        assert!(inspect_export(&f.output).is_err(), "{field}");
    }
}

#[test]
fn consensus_export_validates_untrusted_digest_and_flat_path_before_opening_fact() {
    for fault in ["digest", "path"] {
        let f = Fixture::new();
        f.export().unwrap();
        let mut receipt: Value =
            serde_json::from_slice(&fs::read(f.output.join(RECEIPT)).unwrap()).unwrap();
        match fault {
            "digest" => {
                receipt["facts"][0]["binding"]["source_event_sha256"] = "../outside".into();
                receipt["facts"][0]["path"] = "00000000003384311167-../outside.bzcnf".into();
            }
            _ => receipt["facts"][0]["path"] = "../outside.bzcnf".into(),
        }
        replace(
            &f.output.join(RECEIPT),
            &serde_json::to_vec(&receipt).unwrap(),
        );
        let error = inspect_export(&f.output).unwrap_err().to_string();
        assert!(
            matches!(
                error.as_str(),
                "invalid_consensus_fact_binding" | "invalid_export_fact_path"
            ),
            "{error}"
        );
    }
}

#[test]
fn consensus_export_relative_output_works_in_a_separate_process() {
    let f = Fixture::new();
    fs::write(
        f.root.join("binding.json"),
        serde_json::to_vec(&f.binding).unwrap(),
    )
    .unwrap();
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "consensus::tests::consensus_relative_output_child",
            "--ignored",
        ])
        .env("HIVEZILLA_CONSENSUS_TEST_INPUT", &f.root)
        .current_dir(&f.root)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(f.root.join("relative-export").join(RECEIPT).is_file());
    assert!(inspect_export(&f.root.join("relative-export")).is_ok());
}

#[test]
#[ignore = "subprocess entry point for the relative output regression"]
fn consensus_relative_output_child() {
    let root = PathBuf::from(
        std::env::var_os("HIVEZILLA_CONSENSUS_TEST_INPUT").expect("subprocess fixture"),
    );
    let binding: FactBinding =
        serde_json::from_slice(&fs::read(root.join("binding.json")).unwrap()).unwrap();
    export_event(
        &binding,
        &root.join("event.json"),
        &root.join("payload.bzstrm"),
        Path::new("relative-export"),
    )
    .unwrap();
}

#[test]
fn consensus_export_refuses_an_active_output_writer() {
    let f = Fixture::new();
    f.export().unwrap();
    let _owner = output_lock(&f.output).unwrap();
    assert!(f.export().is_err());
}

#[cfg(unix)]
#[test]
fn consensus_export_rejects_symlink_input_output_and_fact() {
    use std::os::unix::fs::symlink;
    let f = Fixture::new();
    let event_link = f.root.join("linked-event.json");
    symlink(&f.event, &event_link).unwrap();
    assert!(export_event(&f.binding, &event_link, &f.payload, &f.output).is_err());
    assert!(!f.output.exists());
    symlink(&f.root, &f.output).unwrap();
    assert!(f.export().is_err());
    fs::remove_file(&f.output).unwrap();
    let receipt = f.export().unwrap();
    let fact_path = f.output.join(&receipt.facts[0].path);
    fs::remove_file(&fact_path).unwrap();
    symlink(&f.event, fact_path).unwrap();
    assert!(inspect_export(&f.output).is_err());
}
