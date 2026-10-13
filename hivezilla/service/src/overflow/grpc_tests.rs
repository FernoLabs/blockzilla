mod overflow_adapter_tests {
    use super::*;
    use crate::overflow::{
        OverflowCatalog, OverflowLimits,
        grpc::{
            GrpcRawOverflowDescriptor, GrpcRawOverflowLimits, GrpcRawOverflowSource, spill_grpc_raw,
        },
    };
    use hivezilla_object_store::{
        CreateRequest, CreateResult, DeleteDisposition, ImmutableObjectStore, MemoryObjectStore,
        ObjectKey, ObjectMetadata, ObjectRead, ObjectVersion,
    };
    use hivezilla_protocol::{
        ClusterGenesisHash, DeletionAuthorizingStoreId, DurabilityPolicyId, DurabilityPolicyV1,
        DurabilityTargetId, DurabilityTargetV1, FailureDomainId, OverflowNamespaceSha256, StreamId,
        StreamManifestV1, durability_target_descriptor_sha256, overflow_manifest_key,
        terminal_catalog_descriptor_sha256,
    };
    use std::sync::atomic::{AtomicBool, Ordering};

    const NAMESPACE: OverflowNamespaceSha256 = OverflowNamespaceSha256::new([9; 32]);
    const READER: &str = "hetzner";

    struct Fixture {
        _root: tempfile::TempDir,
        source: PathBuf,
        state: PathBuf,
        identity: GrpcRawIdentityFile,
        spool: SpoolWriter,
        count: u64,
        manifest: StreamManifestV1,
    }

    fn manifest(
        descriptor: Vec<u8>,
        format: u32,
        namespace: OverflowNamespaceSha256,
    ) -> StreamManifestV1 {
        let targets = [1, 2].map(|id| DurabilityTargetV1 {
            target_id: DurabilityTargetId::new([id; 16]),
            failure_domain_id: FailureDomainId::new([id; 16]),
            target_descriptor_sha256: durability_target_descriptor_sha256(&[id]),
        });
        // Test-only policy. The adapter neither creates nor accepts custody receipts.
        StreamManifestV1::new(
            StreamId::new([1; 16]),
            ClusterGenesisHash::new([2; 32]),
            format,
            1,
            descriptor,
            None,
            Some(StreamId::new([3; 16])),
            Some(namespace),
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
        .unwrap()
    }

    impl Fixture {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let source = root.path().join("source");
            fs::create_dir(&source).unwrap();
            let config = test_config(source.clone());
            let identity = load_or_create_identity(&config).unwrap();
            let spool = SpoolWriter::open(
                source.join(WAL_ROOT_DIR),
                identity.spool_identity(),
                SpoolOptions {
                    segment_target_bytes: config.segment_target_bytes,
                    max_record_bytes: config.max_record_bytes,
                },
            )
            .unwrap();
            recover_handoff_journal(&source.join(HANDOFF_JOURNAL_FILE)).unwrap();
            let descriptor = GrpcRawOverflowDescriptor::new(
                identity.replication_stream_id(),
                config.max_record_bytes,
            )
            .unwrap()
            .encode()
            .unwrap();
            Self {
                state: root.path().join("catalog"),
                source,
                identity,
                spool,
                count: 0,
                manifest: manifest(descriptor, 1, NAMESPACE),
                _root: root,
            }
        }

        fn append(&mut self, slot: u64) -> Vec<u8> {
            let update = update(slot);
            let row = append_fixture(&mut self.spool, &self.identity, &update, self.count);
            append_handoff_record(&self.source.join(HANDOFF_JOURNAL_FILE), &row).unwrap();
            self.count += 1;
            zstd::bulk::compress(&update.encode_to_vec(), 1).unwrap()
        }

        fn catalog(&self) -> OverflowCatalog {
            OverflowCatalog::open(&self.state, NAMESPACE, OverflowLimits::default()).unwrap()
        }

        fn spill(
            &self,
            store: &dyn ImmutableObjectStore,
            records: usize,
            segments: usize,
        ) -> Result<crate::overflow::grpc::GrpcRawOverflowReport> {
            spill_grpc_raw(
                &mut self.catalog(),
                READER,
                &self.manifest,
                GrpcRawOverflowSource::Generation(self.source.clone()),
                GrpcRawOverflowLimits {
                    max_records_per_segment: records,
                    max_segments_per_pass: segments,
                },
                store,
            )
        }

        fn assert_prefix(&self, expected: u64) {
            assert_eq!(
                self.catalog()
                    .contiguous_verified_end(READER, self.manifest.stream().stream_id())
                    .unwrap()
                    .next_sequence(),
                expected
            );
        }
    }

    #[test]
    fn exact_payloads_survive_roundtrip_without_slot_filtering_or_source_changes() {
        let mut fixture = Fixture::new();
        let expected = [101, 101, 99].map(|slot| fixture.append(slot));
        let before = fs::read(fixture.source.join(HANDOFF_JOURNAL_FILE)).unwrap();
        let store = MemoryObjectStore::new(false);
        let report = fixture.spill(&store, 8, 4).unwrap();
        assert_eq!(report.verified_records, 3);
        assert!(report.reached_snapshot_end);
        fixture.assert_prefix(3);
        let segment = fixture
            .catalog()
            .read_segment(READER, fixture.manifest.stream().stream_id(), 2, &store)
            .unwrap();
        assert_eq!(segment.header().stream(), fixture.manifest.stream());
        assert_eq!(
            segment
                .frames()
                .iter()
                .map(|frame| frame.payload())
                .collect::<Vec<_>>(),
            expected.iter().map(Vec::as_slice).collect::<Vec<_>>()
        );
        let key = ObjectKey::new(overflow_manifest_key(
            fixture.manifest.stream().stream_id(),
            fixture.manifest.stream().stream_manifest_sha256(),
        ))
        .unwrap();
        let mut remote_manifest = Vec::new();
        store
            .open(&key)
            .unwrap()
            .body
            .read_to_end(&mut remote_manifest)
            .unwrap();
        assert_eq!(remote_manifest, fixture.manifest.encode());
        assert_eq!(
            fs::read(fixture.source.join(HANDOFF_JOURNAL_FILE)).unwrap(),
            before
        );
        assert_eq!(fixture.spill(&store, 1, 1).unwrap().verified_records, 0);
        assert_eq!(store.list("").unwrap().len(), 2);
        let cursor = GrpcRawCommittedCursor::open(
            &fixture.source,
            GrpcRawCommittedReadLimits {
                max_compressed_record_bytes: 1024 * 1024,
                max_uncompressed_record_bytes: 1024 * 1024,
            },
            0,
        )
        .unwrap();
        assert_eq!(
            cursor
                .read_batch(10, 1024 * 1024, 1024 * 1024)
                .unwrap()
                .records
                .len(),
            3
        );
    }

    #[test]
    fn bounded_pass_resumes_from_verified_catalog_after_reopen() {
        let mut fixture = Fixture::new();
        for slot in 101..106 {
            fixture.append(slot);
        }
        let store = MemoryObjectStore::new(false);
        let first = fixture.spill(&store, 2, 1).unwrap();
        assert_eq!((first.first_sequence, first.next_sequence), (0, 2));
        assert!(!first.reached_snapshot_end);
        let second = fixture.spill(&store, 1, 2).unwrap();
        assert_eq!((second.first_sequence, second.next_sequence), (2, 4));
        let third = fixture.spill(&store, 8, 4).unwrap();
        assert_eq!((third.first_sequence, third.next_sequence), (4, 5));
        assert!(third.reached_snapshot_end);
        fixture.assert_prefix(5);
    }

    struct LostReplyStore {
        inner: MemoryObjectStore,
        fail_segment: AtomicBool,
    }
    impl ImmutableObjectStore for LostReplyStore {
        fn create_if_absent(
            &self,
            request: &CreateRequest,
            source: &mut dyn Read,
        ) -> hivezilla_object_store::Result<CreateResult> {
            let result = self.inner.create_if_absent(request, source)?;
            if request.key.as_str().ends_with(".hseg")
                && self.fail_segment.swap(false, Ordering::SeqCst)
            {
                return Err(hivezilla_object_store::Error::Io(std::io::Error::other(
                    "lost segment upload reply",
                )));
            }
            Ok(result)
        }
        fn open(&self, key: &ObjectKey) -> hivezilla_object_store::Result<ObjectRead> {
            self.inner.open(key)
        }
        fn stat(&self, key: &ObjectKey) -> hivezilla_object_store::Result<Option<ObjectMetadata>> {
            self.inner.stat(key)
        }
        fn list(&self, prefix: &str) -> hivezilla_object_store::Result<Vec<ObjectMetadata>> {
            self.inner.list(prefix)
        }
        fn delete(
            &self,
            _: &ObjectKey,
            _: Option<&ObjectVersion>,
        ) -> hivezilla_object_store::Result<DeleteDisposition> {
            panic!("overflow must not delete source or remote objects")
        }
    }

    #[test]
    fn failed_upload_retries_original_boundary_after_source_growth_and_limit_change() {
        let mut fixture = Fixture::new();
        fixture.append(101);
        fixture.append(102);
        let store = LostReplyStore {
            inner: MemoryObjectStore::new(false),
            fail_segment: AtomicBool::new(true),
        };
        assert!(fixture.spill(&store, 8, 4).is_err());
        fixture.assert_prefix(0);
        let pending = fixture
            .catalog()
            .containing(READER, fixture.manifest.stream().stream_id(), 0)
            .unwrap()
            .unwrap();
        assert!(!pending.verified);
        assert_eq!(pending.object.end().next_sequence(), 2);
        fixture.append(103);
        let recovered = fixture.spill(&store, 1, 1).unwrap();
        assert_eq!(recovered.next_sequence, 2);
        let verified = fixture
            .catalog()
            .containing(READER, fixture.manifest.stream().stream_id(), 0)
            .unwrap()
            .unwrap();
        assert!(verified.verified);
        assert_eq!(verified.object.object_key(), pending.object.object_key());
        assert_eq!(store.list("").unwrap().len(), 2);
        assert_eq!(fixture.spill(&store, 8, 4).unwrap().next_sequence, 3);
        fixture.assert_prefix(3);
    }

    #[test]
    fn partial_handoff_waits_for_commit_and_never_exposes_uncommitted_wal() {
        let mut fixture = Fixture::new();
        fixture.append(101);
        let row = append_fixture(&mut fixture.spool, &fixture.identity, &update(102), 1);
        let journal = fixture.source.join(HANDOFF_JOURNAL_FILE);
        let before = fs::metadata(&journal).unwrap().len();
        let encoded = serde_json::to_vec(&row).unwrap();
        let split = encoded.len() / 2;
        let mut writer = fs::OpenOptions::new().append(true).open(&journal).unwrap();
        writer.write_all(&encoded[..split]).unwrap();
        writer.sync_all().unwrap();
        let store = MemoryObjectStore::new(false);
        let report = fixture.spill(&store, 8, 4).unwrap();
        assert_eq!(report.next_sequence, 1);
        assert!(!report.reached_snapshot_end);
        assert_eq!(report.incomplete_handoff_tail_bytes, split as u64);
        assert_eq!(fs::metadata(&journal).unwrap().len(), before + split as u64);
        writer.write_all(&encoded[split..]).unwrap();
        writer.write_all(b"\n").unwrap();
        writer.sync_all().unwrap();
        assert_eq!(fixture.spill(&store, 8, 4).unwrap().next_sequence, 2);
        fixture.assert_prefix(2);
    }

    #[test]
    fn corrupt_later_record_does_not_publish_partial_segment() {
        let mut fixture = Fixture::new();
        fixture.append(101);
        let mut row = append_fixture(&mut fixture.spool, &fixture.identity, &update(102), 1);
        row.protobuf_sha256 = "00".repeat(32);
        append_handoff_record(&fixture.source.join(HANDOFF_JOURNAL_FILE), &row).unwrap();
        let store = MemoryObjectStore::new(false);
        assert!(
            fixture
                .spill(&store, 8, 4)
                .unwrap_err()
                .to_string()
                .contains("protobuf checksum mismatch")
        );
        fixture.assert_prefix(0);
        assert!(
            fixture
                .catalog()
                .ranges_from(READER, fixture.manifest.stream().stream_id(), 0, 1)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store.list("").unwrap().len(),
            1,
            "only the manifest can be published"
        );
    }

    #[test]
    fn rejects_wrong_payload_format_source_schema_and_namespace_before_data_upload() {
        let mut fixture = Fixture::new();
        fixture.append(101);
        let descriptor = fixture.manifest.producer_descriptor().to_vec();
        let store = MemoryObjectStore::new(false);
        fixture.manifest = manifest(descriptor.clone(), 2, NAMESPACE);
        assert!(
            fixture
                .spill(&store, 8, 4)
                .unwrap_err()
                .to_string()
                .contains("payload format")
        );
        let mut other_source = fixture.identity.replication_stream_id();
        other_source.journal_id = [42; 16];
        fixture.manifest = manifest(
            GrpcRawOverflowDescriptor::new(other_source, 1024 * 1024)
                .unwrap()
                .encode()
                .unwrap(),
            1,
            NAMESPACE,
        );
        assert!(
            fixture
                .spill(&store, 8, 4)
                .unwrap_err()
                .to_string()
                .contains("identity differs")
        );
        let changed = String::from_utf8(descriptor.clone())
            .unwrap()
            .replace("prost-0.14", "prost-9.99");
        fixture.manifest = manifest(changed.into_bytes(), 1, NAMESPACE);
        assert!(
            fixture
                .spill(&store, 8, 4)
                .unwrap_err()
                .to_string()
                .contains("producer descriptor")
        );
        fixture.manifest = manifest(descriptor, 1, OverflowNamespaceSha256::new([7; 32]));
        assert!(
            fixture
                .spill(&store, 8, 4)
                .unwrap_err()
                .to_string()
                .contains("namespace")
        );
        assert!(store.list("").unwrap().is_empty());
    }

    #[test]
    fn rejects_unsupported_transaction_config_before_publishing_segment() {
        let mut fixture = Fixture::new();
        let row = append_fixture(
            &mut fixture.spool,
            &fixture.identity,
            &update_with_v1_config(101, true),
            0,
        );
        append_handoff_record(&fixture.source.join(HANDOFF_JOURNAL_FILE), &row).unwrap();
        let store = MemoryObjectStore::new(false);
        assert!(
            fixture
                .spill(&store, 8, 4)
                .unwrap_err()
                .to_string()
                .contains("V1 transaction config")
        );
        fixture.assert_prefix(0);
        assert_eq!(store.list("").unwrap().len(), 1);
    }

    #[test]
    fn hot_rotation_keeps_logical_sequence_and_does_not_duplicate_seed() {
        let root = tempfile::tempdir().unwrap();
        let cache = root.path().join("hot");
        let active = cache.join(HOT_ACTIVE_DIR);
        fs::create_dir_all(&active).unwrap();
        fs::create_dir(cache.join(HOT_SEALED_DIR)).unwrap();
        fs::create_dir(cache.join(HOT_RECEIPTS_DIR)).unwrap();
        let mut config = test_config(active.clone());
        config.hot_generation_root = Some(cache.clone());
        config.max_generation_bytes = minimum_generation_bytes(&config).unwrap();
        let mut identity = load_or_create_identity(&config).unwrap();
        let descriptor = GrpcRawOverflowDescriptor::new(
            identity.replication_stream_id(),
            config.max_record_bytes,
        )
        .unwrap()
        .encode()
        .unwrap();
        let manifest = manifest(descriptor, 1, NAMESPACE);
        let mut spool = SpoolWriter::open(
            active.join(WAL_ROOT_DIR),
            identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: config.segment_target_bytes,
                max_record_bytes: config.max_record_bytes,
            },
        )
        .unwrap();
        let journal = active.join(HANDOFF_JOURNAL_FILE);
        recover_handoff_journal(&journal).unwrap();
        let mut tail = None;
        for frame in 0..3 {
            let row = append_fixture(
                &mut spool,
                &identity,
                &complete_poh_update(101 + frame),
                frame,
            );
            append_handoff_record(&journal, &row).unwrap();
            tail = Some(row);
        }
        hot_rotate_generation(
            &config,
            &cache,
            &mut identity,
            &mut spool,
            tail.as_ref().unwrap(),
        )
        .unwrap();
        let row = append_fixture(&mut spool, &identity, &complete_poh_update(104), 1);
        append_handoff_record(&journal, &row).unwrap();
        let store = MemoryObjectStore::new(false);
        let mut catalog = OverflowCatalog::open(
            &root.path().join("catalog"),
            NAMESPACE,
            OverflowLimits::default(),
        )
        .unwrap();
        let report = spill_grpc_raw(
            &mut catalog,
            READER,
            &manifest,
            GrpcRawOverflowSource::HotCache(cache.clone()),
            GrpcRawOverflowLimits::default(),
            &store,
        )
        .unwrap();
        assert_eq!(
            (
                report.verified_segments,
                report.verified_records,
                report.next_sequence
            ),
            (2, 4, 4)
        );
        let second = catalog
            .read_segment(READER, manifest.stream().stream_id(), 3, &store)
            .unwrap();
        assert_eq!(second.start().next_sequence(), 3);
        assert_eq!(second.frames().len(), 1);
        assert!(
            spill_grpc_raw(
                &mut catalog,
                READER,
                &manifest,
                GrpcRawOverflowSource::HotCache(cache),
                GrpcRawOverflowLimits::default(),
                &store
            )
            .unwrap()
            .reached_snapshot_end
        );
        // A fresh catalog cannot silently start at the retained generation's nonzero base.
        let mut fresh = OverflowCatalog::open(
            &root.path().join("fresh"),
            NAMESPACE,
            OverflowLimits::default(),
        )
        .unwrap();
        assert!(
            spill_grpc_raw(
                &mut fresh,
                READER,
                &manifest,
                GrpcRawOverflowSource::Generation(active),
                GrpcRawOverflowLimits::default(),
                &store
            )
            .is_err()
        );
        assert_eq!(
            fresh
                .contiguous_verified_end(READER, manifest.stream().stream_id())
                .unwrap()
                .next_sequence(),
            0
        );
    }

    #[test]
    fn rejects_unknown_protobuf_fields_extra_zstd_frames_and_mismatched_block_identity() {
        for scenario in ["unknown-field", "extra-frame", "wrong-block"] {
            let mut fixture = Fixture::new();
            let mut raw = update(if scenario == "wrong-block" { 102 } else { 101 }).encode_to_vec();
            if scenario == "unknown-field" {
                raw.extend_from_slice(&[0xa0, 0x06, 0x01]);
            }
            let mut compressed = zstd::bulk::compress(&raw, 1).unwrap();
            if scenario == "extra-frame" {
                compressed.extend(zstd::bulk::compress(b"", 1).unwrap());
            }
            let metadata = IngressRecordMeta::from_payload(
                fixture.identity.cluster_id.clone(),
                ObservationId {
                    origin_node_id: fixture.identity.origin_node_id.clone(),
                    journal_id: fixture.identity.journal_id,
                    sequence: 0,
                },
                fixture.identity.source_id.clone(),
                LogicalKey::Block {
                    slot: 101,
                    blockhash: [0; 32],
                },
                PAYLOAD_FORMAT_ZSTD_PROTOBUF_UPDATE_V1,
                &compressed,
            );
            let durable = fixture
                .spool
                .append_and_sync(metadata, &compressed)
                .unwrap();
            let row = handoff_record_from_block(
                &block(101),
                0,
                EpochSlot::from_slot(101, OLD_FAITHFUL_SLOTS_PER_EPOCH),
                durable.location(),
                raw.len() as u64,
                compressed.len() as u64,
                sha256_hex(&raw),
            );
            append_handoff_record(&fixture.source.join(HANDOFF_JOURNAL_FILE), &row).unwrap();
            let store = MemoryObjectStore::new(false);
            let error = fixture.spill(&store, 8, 4).unwrap_err().to_string();
            let expected = match scenario {
                "unknown-field" => "canonical known-schema",
                "extra-frame" => "exactly one zstd frame",
                _ => "block identity differs",
            };
            assert!(error.contains(expected), "{scenario}: {error}");
            fixture.assert_prefix(0);
            assert_eq!(store.list("").unwrap().len(), 1);
        }
    }

    #[test]
    fn oversized_record_stops_at_the_same_sequence_without_an_upload_intent() {
        let mut fixture = Fixture::new();
        fixture.append(101);
        let mut catalog = OverflowCatalog::open(
            &fixture.state,
            NAMESPACE,
            OverflowLimits {
                max_segment_bytes: (hivezilla_protocol::MIN_SEALED_SEGMENT_V1_ENCODED_LEN
                    + hivezilla_protocol::FRAME_V1_FIXED_ENCODED_LEN
                    + 1) as u64,
                ..OverflowLimits::default()
            },
        )
        .unwrap();
        let store = MemoryObjectStore::new(false);
        assert!(
            spill_grpc_raw(
                &mut catalog,
                READER,
                &fixture.manifest,
                GrpcRawOverflowSource::Generation(fixture.source.clone()),
                GrpcRawOverflowLimits {
                    max_records_per_segment: 1,
                    max_segments_per_pass: 1
                },
                &store
            )
            .is_err()
        );
        fixture.assert_prefix(0);
        assert!(
            catalog
                .ranges_from(READER, fixture.manifest.stream().stream_id(), 0, 1)
                .unwrap()
                .is_empty()
        );
        assert_eq!(fixture.spill(&store, 8, 4).unwrap().next_sequence, 1);
    }
}
