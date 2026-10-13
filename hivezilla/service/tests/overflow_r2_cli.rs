#[path = "../../object-store/test-support/r2.rs"]
mod support;

use hivezilla::overflow::OverflowCatalog;
use hivezilla_protocol::{
    ClusterGenesisHash, FrameV1, ProducerConfigSha256, SealedSegmentV1, SegmentHeaderV1,
    StreamHeaderV1, StreamId, StreamManifestSha256,
};
use std::{
    fs,
    path::PathBuf,
    process::{Command, Output},
};
use support::Server;

use hivezilla::{
    grpc_raw::GrpcRawHandoffRecord,
    ingest::{
        IngressRecordMeta, LogicalKey, ObservationId, ReplicationStreamId, SpoolJournalIdentity,
        SpoolOptions, SpoolWriter,
    },
    overflow::grpc::GrpcRawOverflowDescriptor,
};
use hivezilla_object_store::{r2::R2ObjectStore, uploader::S3Client};
use hivezilla_protocol::{
    DeletionAuthorizingStoreId, DurabilityPolicyId, DurabilityPolicyV1, DurabilityTargetId,
    DurabilityTargetV1, FailureDomainId, OverflowNamespaceSha256, StreamManifestV1,
    durability_target_descriptor_sha256, terminal_catalog_descriptor_sha256,
};
use prost::Message;
use sha2::{Digest, Sha256};
use std::io::Write;
use yellowstone_grpc_proto::prelude::{
    SubscribeUpdate, SubscribeUpdateBlock, subscribe_update::UpdateOneof,
};

struct Fixture {
    root: tempfile::TempDir,
    server: Server,
    input: PathBuf,
    credentials: PathBuf,
    state: PathBuf,
    bytes: Vec<u8>,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let server = Server::start();
        let input = root.path().join("input.hseg");
        let credentials = root.path().join("r2.env");
        fs::write(&credentials, format!(
            "R2_ENDPOINT={}\nR2_BUCKET=bucket\nR2_ACCESS_KEY_ID=test-access\nR2_SECRET_ACCESS_KEY=test-secret\n",
            server.endpoint,
        )).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&credentials, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let stream = StreamHeaderV1::new(
            StreamId::new([1; 16]),
            ClusterGenesisHash::new([2; 32]),
            2,
            1,
            ProducerConfigSha256::new([3; 32]),
            StreamManifestSha256::new([4; 32]),
        )
        .unwrap();
        let mut cursor = stream.initial_cursor();
        let frames = [b"first".as_slice(), b"second"]
            .into_iter()
            .map(|payload| {
                let frame = FrameV1::new(cursor, payload.to_vec()).unwrap();
                cursor = frame.validate_after(cursor).unwrap();
                frame
            })
            .collect();
        let bytes = SealedSegmentV1::new(
            SegmentHeaderV1::new(stream, stream.initial_cursor()).unwrap(),
            frames,
        )
        .unwrap()
        .encode();
        fs::write(&input, &bytes).unwrap();
        let state = root.path().join("state");
        Self {
            root,
            server,
            input,
            credentials,
            state,
            bytes,
        }
    }

    fn command(&self, name: &str, prefix: &str) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_hivezilla"));
        command
            .env_clear()
            .env("RUST_LOG", "debug")
            .arg(name)
            .arg("--state-dir")
            .arg(&self.state)
            .arg("--credentials-file")
            .arg(&self.credentials)
            .args([
                "--remote-prefix",
                prefix,
                "--reader-id",
                "hetzner",
                "--retries",
                "0",
            ]);
        command
    }

    fn upload(&self) -> Output {
        self.command("upload-overflow-segment", "reader-a")
            .arg("--segment")
            .arg(&self.input)
            .output()
            .unwrap()
    }

    fn recover(&self, prefix: &str) -> Output {
        self.command("recover-overflow-segment", prefix)
            .args([
                "--stream-id",
                &"01".repeat(16),
                "--sequence",
                "1",
                "--output",
            ])
            .arg(self.output())
            .output()
            .unwrap()
    }

    fn output(&self) -> PathBuf {
        self.root.path().join("recovered.hseg")
    }
}

fn json(output: &Output) -> serde_json::Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

struct RawFixture {
    base: Fixture,
    source: PathBuf,
    manifest: PathBuf,
    identity: ReplicationStreamId,
    spool: SpoolWriter,
    payloads: Vec<Vec<u8>>,
}

impl RawFixture {
    fn new() -> Self {
        let base = Fixture::new();
        let source = base.root.path().join("raw");
        fs::create_dir(&source).unwrap();
        let identity = ReplicationStreamId {
            cluster_id: "test-cluster".into(),
            origin_node_id: "test-origin".into(),
            source_id: "grpc-a".into(),
            journal_id: [8; 16],
        };
        fs::write(
            source.join("identity.json"),
            serde_json::to_vec(&serde_json::json!({
                "schema_version": 1, "endpoint": "https://example.invalid",
                "cluster_id": identity.cluster_id, "origin_node_id": identity.origin_node_id,
                "source_id": identity.source_id, "journal_id": identity.journal_id,
                "payload_format_version": 2,
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(source.join("raw-blocks.jsonl"), b"").unwrap();
        let spool = SpoolWriter::open(
            source.join("wal"),
            SpoolJournalIdentity {
                cluster_id: identity.cluster_id.clone(),
                origin_node_id: identity.origin_node_id.clone(),
                source_id: identity.source_id.clone(),
                journal_id: identity.journal_id,
            },
            SpoolOptions::default(),
        )
        .unwrap();
        let store = R2ObjectStore::new(
            S3Client::new(base.server.settings(), 0).unwrap(),
            "reader-a".into(),
            256 * 1024 * 1024,
        )
        .unwrap();
        let targets = [1, 2].map(|id| DurabilityTargetV1 {
            target_id: DurabilityTargetId::new([id; 16]),
            failure_domain_id: FailureDomainId::new([id; 16]),
            target_descriptor_sha256: durability_target_descriptor_sha256(&[id]),
        });
        let manifest = base.root.path().join("grpc.manifest");
        let config = StreamManifestV1::new(
            StreamId::new([1; 16]),
            ClusterGenesisHash::new([2; 32]),
            1,
            1,
            GrpcRawOverflowDescriptor::new(identity.clone(), 1024 * 1024)
                .unwrap()
                .encode()
                .unwrap(),
            None,
            Some(StreamId::new([3; 16])),
            Some(OverflowNamespaceSha256::new(store.namespace_sha256())),
            Some(DeletionAuthorizingStoreId::new([4; 16])),
            Some(
                DurabilityPolicyV1::new(
                    DurabilityPolicyId::new([5; 16]),
                    2,
                    terminal_catalog_descriptor_sha256(b"test-only"),
                    targets.to_vec(),
                )
                .unwrap(),
            ),
        )
        .unwrap();
        fs::write(&manifest, config.encode()).unwrap();
        Self {
            base,
            source,
            manifest,
            identity,
            spool,
            payloads: Vec::new(),
        }
    }

    fn append(&mut self, slot: u64) {
        let raw = SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Block(SubscribeUpdateBlock {
                slot,
                parent_slot: slot - 1,
                blockhash: "11111111111111111111111111111111".into(),
                ..Default::default()
            })),
            ..Default::default()
        }
        .encode_to_vec();
        let compressed = zstd::bulk::compress(&raw, 1).unwrap();
        let sequence = self.payloads.len() as u64;
        let metadata = IngressRecordMeta::from_payload(
            self.identity.cluster_id.clone(),
            ObservationId {
                origin_node_id: self.identity.origin_node_id.clone(),
                journal_id: self.identity.journal_id,
                sequence,
            },
            self.identity.source_id.clone(),
            LogicalKey::Block {
                slot,
                blockhash: [0; 32],
            },
            2,
            &compressed,
        );
        let location = self
            .spool
            .append_and_sync(metadata, &compressed)
            .unwrap()
            .location();
        let row = GrpcRawHandoffRecord {
            schema_version: 1,
            frame_id: sequence,
            slot,
            parent_slot: slot - 1,
            epoch: 0,
            epoch_slot_index: slot,
            segment_id: location.segment_id,
            frame_offset: location.frame_offset,
            frame_len: location.frame_len,
            compressed_len: compressed.len() as u64,
            uncompressed_len: raw.len() as u64,
            protobuf_sha256: Sha256::digest(&raw)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
            blockhash: "11111111111111111111111111111111".into(),
        };
        let mut journal = fs::OpenOptions::new()
            .append(true)
            .open(self.source.join("raw-blocks.jsonl"))
            .unwrap();
        serde_json::to_writer(&mut journal, &row).unwrap();
        journal.write_all(b"\n").unwrap();
        journal.sync_all().unwrap();
        self.payloads.push(compressed);
    }

    fn spill(&self, records: &str, segments: &str) -> Output {
        self.base
            .command("spill-grpc-raw-overflow", "reader-a")
            .arg("--manifest")
            .arg(&self.manifest)
            .arg("--source-dir")
            .arg(&self.source)
            .args([
                "--max-records-per-segment",
                records,
                "--max-segments",
                segments,
            ])
            .output()
            .unwrap()
    }
}

#[test]
fn live_journal_spill_and_recovery_preserve_payloads_across_command_restarts() {
    let mut fixture = RawFixture::new();
    fixture.append(101);
    fixture.append(102);
    let before = fs::read(fixture.source.join("raw-blocks.jsonl")).unwrap();
    let report = json(&fixture.spill("8", "4"));
    assert_eq!(report["verified_records"], 2);
    assert_eq!(report["reached_snapshot_end"], true);
    assert_eq!(fixture.base.server.count("PUT"), 2, "manifest then segment");
    assert_eq!(json(&fixture.spill("8", "4"))["verified_records"], 0);
    json(&fixture.base.recover("reader-a"));
    let segment = SealedSegmentV1::decode(&fs::read(fixture.base.output()).unwrap()).unwrap();
    assert_eq!(segment.header().stream().payload_format(), 1);
    assert_eq!(
        segment
            .frames()
            .iter()
            .map(|frame| frame.payload())
            .collect::<Vec<_>>(),
        fixture
            .payloads
            .iter()
            .map(Vec::as_slice)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        fs::read(fixture.source.join("raw-blocks.jsonl")).unwrap(),
        before
    );
}

#[test]
fn live_spill_retries_pending_segment_after_process_exit_and_new_source_records() {
    let mut fixture = RawFixture::new();
    // Publish the manifest with an empty journal so the injected failure targets the segment.
    assert_eq!(json(&fixture.spill("8", "4"))["verified_records"], 0);
    fixture.append(101);
    fixture.append(102);
    fixture.base.server.state.lock().unwrap().drop_put_response = true;
    let failed = fixture.spill("8", "4");
    assert!(!failed.status.success());
    assert!(failed.stdout.is_empty());
    let status = OverflowCatalog::inspect(&fixture.base.state).unwrap();
    assert_eq!(status[0].pending_ranges, 1);
    assert_eq!(status[0].contiguous_verified_next_sequence, 0);
    fixture.append(103);
    assert_eq!(json(&fixture.spill("1", "1"))["next_sequence"], 2);
    assert_eq!(
        fixture.base.server.count("PUT"),
        2,
        "pending segment is reused exactly"
    );
    assert_eq!(json(&fixture.spill("8", "4"))["next_sequence"], 3);
    json(&fixture.base.recover("reader-a"));
    let recovered = SealedSegmentV1::decode(&fs::read(fixture.base.output()).unwrap()).unwrap();
    assert_eq!(recovered.frames().len(), 2);
    assert_eq!(recovered.end().next_sequence(), 2);
}

#[test]
fn separate_processes_upload_then_recover_exact_bytes() {
    let fixture = Fixture::new();
    let uploaded = json(&fixture.upload());
    assert_eq!(uploaded["first_sequence"], 0);
    assert_eq!(uploaded["next_sequence"], 2);
    assert_eq!(fs::read(&fixture.input).unwrap(), fixture.bytes);
    let report = OverflowCatalog::inspect(&fixture.state).unwrap();
    assert_eq!(report[0].verified_ranges, 1);
    assert_eq!(report[0].pending_ranges, 0);
    let recovered = json(&fixture.recover("reader-a"));
    assert_eq!(uploaded, recovered);
    assert_eq!(fs::read(fixture.output()).unwrap(), fixture.bytes);
    assert_eq!(fixture.server.count("PUT"), 1);
    assert_eq!(json(&fixture.upload()), uploaded);
    assert_eq!(fixture.server.count("PUT"), 1);
}

#[test]
fn lost_upload_reply_leaves_a_pending_intent_and_restart_repairs_it() {
    let fixture = Fixture::new();
    fixture.server.state.lock().unwrap().drop_put_response = true;
    let failed = fixture.upload();
    assert!(!failed.status.success());
    assert!(failed.stdout.is_empty());
    let status = OverflowCatalog::inspect(&fixture.state).unwrap();
    assert_eq!(status[0].pending_ranges, 1);
    assert_eq!(status[0].contiguous_verified_next_sequence, 0);
    assert_eq!(fs::read(&fixture.input).unwrap(), fixture.bytes);
    assert!(!fixture.recover("reader-a").status.success());
    assert!(!fixture.output().exists());
    json(&fixture.upload());
    json(&fixture.recover("reader-a"));
    assert_eq!(fixture.server.count("PUT"), 1);
    assert_eq!(fs::read(fixture.output()).unwrap(), fixture.bytes);
}

#[test]
fn failed_upload_or_verification_preserves_source_and_pending_range() {
    for corrupt in [false, true] {
        let fixture = Fixture::new();
        {
            let mut remote = fixture.server.state.lock().unwrap();
            remote.fail_put = !corrupt;
            remote.corrupt_get = corrupt;
        }
        let failed = fixture.upload();
        assert!(!failed.status.success());
        assert_eq!(fs::read(&fixture.input).unwrap(), fixture.bytes);
        let status = OverflowCatalog::inspect(&fixture.state).unwrap();
        assert_eq!(status[0].pending_ranges, 1);
        assert_eq!(status[0].verified_ranges, 0);
        assert!(!String::from_utf8_lossy(&failed.stderr).contains("secret-response"));
        {
            let mut remote = fixture.server.state.lock().unwrap();
            remote.fail_put = false;
            remote.corrupt_get = false;
        }
        json(&fixture.upload());
    }
}

#[test]
fn corrupt_download_never_publishes_a_partial_file() {
    let fixture = Fixture::new();
    json(&fixture.upload());
    for short in [false, true] {
        {
            let mut remote = fixture.server.state.lock().unwrap();
            remote.corrupt_get = !short;
            remote.short_get = short;
        }
        assert!(!fixture.recover("reader-a").status.success());
        assert!(!fixture.output().exists());
    }
    let status = OverflowCatalog::inspect(&fixture.state).unwrap();
    assert_eq!(
        status[0].verified_ranges, 1,
        "failed recovery cannot advance or change receipts"
    );
}

#[test]
fn wrong_namespace_and_existing_outputs_fail_before_remote_requests() {
    let fixture = Fixture::new();
    json(&fixture.upload());
    let before = fixture.server.state.lock().unwrap().requests.len();
    assert!(!fixture.recover("other-reader").status.success());
    assert_eq!(fixture.server.state.lock().unwrap().requests.len(), before);
    fs::write(fixture.output(), b"keep me").unwrap();
    assert!(!fixture.recover("reader-a").status.success());
    assert_eq!(fs::read(fixture.output()).unwrap(), b"keep me");
    assert_eq!(fixture.server.state.lock().unwrap().requests.len(), before);
}

#[test]
fn missing_catalog_and_legacy_input_are_not_silently_initialized() {
    let fixture = Fixture::new();
    assert!(!fixture.recover("reader-a").status.success());
    assert!(!fixture.state.exists());
    fs::write(&fixture.input, b"legacy WAL is not a HIVESEG1 file").unwrap();
    assert!(!fixture.upload().status.success());
    assert!(!fixture.state.exists());
    assert_eq!(fixture.server.state.lock().unwrap().requests.len(), 0);
}

#[test]
fn invalid_stream_id_is_rejected_without_state_or_network_access() {
    let fixture = Fixture::new();
    let output = fixture
        .command("recover-overflow-segment", "reader-a")
        .args(["--stream-id", "../invalid", "--sequence", "0", "--output"])
        .arg(fixture.output())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(!fixture.state.exists());
    assert_eq!(fixture.server.state.lock().unwrap().requests.len(), 0);
}
