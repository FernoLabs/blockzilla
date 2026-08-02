use super::*;

use hivezilla_protocol::{
    BlockzillaAuthorityId, ClusterGenesisHash, DurabilityPolicyId, DurabilityPolicyV1,
    DurabilityTargetDescriptorSha256, DurabilityTargetId, DurabilityTargetV1, FailureDomainId,
    MAX_CHUNK_RECORDS_V1, MAX_RECORD_PAYLOAD_BYTES, MAX_SYNC_RECORD_BYTES_V1,
    MIN_TRANSFER_CHUNK_V1_ENCODED_LEN, OverflowNamespaceSha256, PrefixHash, ProducerConfigSha256,
    StreamRegistryEntryV1, StreamRegistrySnapshotSha256, StreamRegistryStatusV1,
    TerminalCatalogDescriptorSha256,
};
use prost::Message;

fn fixture_stream() -> StreamHeaderV1 {
    StreamHeaderV1::new(
        StreamId::new(core::array::from_fn(|index| index as u8)),
        ClusterGenesisHash::new(core::array::from_fn(|index| (index + 0x10) as u8)),
        2,
        1,
        ProducerConfigSha256::new(core::array::from_fn(|index| (index + 0x30) as u8)),
        StreamManifestSha256::new(core::array::from_fn(|index| (index + 0x50) as u8)),
    )
    .unwrap()
}

fn fixture_record() -> (CursorV1, RecordV1, CursorV1) {
    let start = fixture_stream().initial_cursor();
    let record = RecordV1::new(start, b"abc".to_vec()).unwrap();
    let end = record.end_cursor();
    (start, record, end)
}

fn cursor(sequence: u64, byte: u8) -> CursorV1 {
    CursorV1::new(sequence, PrefixHash::new([byte; 32]))
}

fn open_wire(protected: Option<CursorV1>) -> sync::OpenWireV1 {
    sync::OpenWireV1 {
        stream_id: fixture_stream().stream_id().as_bytes().to_vec(),
        terminal_store_id: vec![0x70; 16],
        protected_cursor: protected
            .map(|value| value.fixed_encode().to_vec())
            .unwrap_or_default(),
    }
}

fn ack_wire() -> sync::AckWireV1 {
    sync::AckWireV1 {
        stream_id: fixture_stream().stream_id().as_bytes().to_vec(),
        terminal_store_id: vec![0x70; 16],
        stream_manifest_sha256: fixture_stream()
            .stream_manifest_sha256()
            .as_bytes()
            .to_vec(),
        policy_id: vec![0x71; 16],
        protected_cursor: fixture_stream().initial_cursor().fixed_encode().to_vec(),
    }
}

fn resume_wire() -> sync::ResumeWireV1 {
    let start = fixture_stream().initial_cursor();
    sync::ResumeWireV1 {
        stream: fixture_stream().fixed_encode().to_vec(),
        session_id: vec![0x77; 16],
        first_available: start.fixed_encode().to_vec(),
        bulk_start: start.fixed_encode().to_vec(),
        cutover: start.fixed_encode().to_vec(),
        max_record_bytes: 48,
        max_chunk_records: 1,
        max_parallel_fetches: 1,
    }
}

fn sync_record_wire() -> sync::RecordWireV1 {
    sync::RecordWireV1 {
        record: fixture_record().1.encode(),
    }
}

fn sync_error_wire(code: i32) -> sync::HiveSyncErrorWireV1 {
    sync::HiveSyncErrorWireV1 { code }
}

fn fetch_request_wire() -> sync::FetchRangeRequestV1 {
    sync::FetchRangeRequestV1 {
        session_id: vec![0x77; 16],
        cutover: cursor(2, 0x82).fixed_encode().to_vec(),
        first_sequence: 1,
        next_sequence: 2,
    }
}

fn transfer_commit_wire() -> sync::TransferChunkCommitWireV1 {
    let (start, _, end) = fixture_record();
    sync::TransferChunkCommitWireV1 {
        start: start.fixed_encode().to_vec(),
        end: end.fixed_encode().to_vec(),
        encoded_len: MIN_TRANSFER_CHUNK_V1_ENCODED_LEN as u64,
        encoded_sha256: vec![0x88; 32],
    }
}

fn empty_discovery_wire() -> public_exit::PublicStreamListWireV1 {
    let registry = StreamRegistrySnapshotV1::new(
        BlockzillaAuthorityId::new([0x90; 16]),
        0,
        StreamRegistrySnapshotSha256::new([0; 32]),
        Vec::new(),
    )
    .unwrap();
    public_exit::PublicStreamListWireV1 {
        registry_head: StreamRegistryHeadV1::from_snapshot(&registry)
            .encode()
            .to_vec(),
        registry: registry.encode(),
        public_manifests: Vec::new(),
    }
}

fn durability_target(id: u8) -> DurabilityTargetV1 {
    DurabilityTargetV1 {
        target_id: DurabilityTargetId::new([id; 16]),
        failure_domain_id: FailureDomainId::new([id; 16]),
        target_descriptor_sha256: DurabilityTargetDescriptorSha256::new([id; 32]),
    }
}

fn durability_policy() -> DurabilityPolicyV1 {
    DurabilityPolicyV1::new(
        DurabilityPolicyId::new([0x70; 16]),
        2,
        TerminalCatalogDescriptorSha256::new([0x71; 32]),
        vec![durability_target(1), durability_target(2)],
    )
    .unwrap()
}

fn capture_manifest(id: u8, payload_format: u32) -> StreamManifestV1 {
    StreamManifestV1::new(
        StreamId::new([id; 16]),
        ClusterGenesisHash::new([0x20; 32]),
        payload_format,
        1,
        vec![0x42, id],
        None,
        Some(StreamId::new([0x30; 16])),
        Some(OverflowNamespaceSha256::new([0x40; 32])),
        Some(DeletionAuthorizingStoreId::new([0x50; 16])),
        Some(durability_policy()),
    )
    .unwrap()
}

fn derived_manifest(id: u8) -> StreamManifestV1 {
    StreamManifestV1::new(
        StreamId::new([id; 16]),
        ClusterGenesisHash::new([0x20; 32]),
        6,
        1,
        vec![0x43, id],
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap()
}

fn registry_entry(
    logical_name: &[u8],
    generation: u64,
    stream_id: StreamId,
    manifest_sha256: StreamManifestSha256,
    status: StreamRegistryStatusV1,
    successor: Option<StreamId>,
) -> StreamRegistryEntryV1 {
    StreamRegistryEntryV1::new(
        logical_name.to_vec(),
        generation,
        stream_id,
        manifest_sha256,
        status,
        successor,
    )
    .unwrap()
}

fn discovery_wire(
    registry_entries: Vec<StreamRegistryEntryV1>,
    mut public_manifests: Vec<StreamManifestV1>,
) -> public_exit::PublicStreamListWireV1 {
    let registry = StreamRegistrySnapshotV1::new(
        BlockzillaAuthorityId::new([0x90; 16]),
        0,
        StreamRegistrySnapshotSha256::new([0; 32]),
        registry_entries,
    )
    .unwrap();
    public_manifests.sort_by_key(|manifest| manifest.stream().stream_id());
    public_exit::PublicStreamListWireV1 {
        registry_head: StreamRegistryHeadV1::from_snapshot(&registry)
            .encode()
            .to_vec(),
        registry: registry.encode(),
        public_manifests: public_manifests
            .into_iter()
            .map(|manifest| manifest.encode())
            .collect(),
    }
}

fn discovery_for_manifest(manifest: &StreamManifestV1) -> public_exit::PublicStreamListWireV1 {
    discovery_wire(
        vec![registry_entry(
            b"solana.mainnet/public",
            0,
            manifest.stream().stream_id(),
            manifest.stream().stream_manifest_sha256(),
            StreamRegistryStatusV1::Active,
            None,
        )],
        vec![manifest.clone()],
    )
}

fn bind_discovery(
    discovery: &StructurallyValidatedPublicStreamListV1,
) -> ContextValidatedPublicStreamListV1<'_> {
    discovery
        .validate_context(DiscoveryValidationContextV1::new(
            BlockzillaAuthorityId::new([0x90; 16]),
            None,
        ))
        .unwrap()
}

#[derive(Default)]
struct TestPublicContext {
    cursors: Vec<CursorV1>,
    ranges: Vec<CursorRangeV1>,
    current_replay: bool,
}

impl PublicHelloValidationContextV1 for TestPublicContext {
    fn exact_cursor_is_member(&self, _stream: StreamHeaderV1, cursor: CursorV1) -> bool {
        self.cursors.contains(&cursor)
    }

    fn exact_range_is_available(&self, _stream: StreamHeaderV1, range: CursorRangeV1) -> bool {
        self.ranges.contains(&range)
    }
}

impl PublicReplayValidationContextV1 for TestPublicContext {
    fn exact_cursor_is_member(&self, _stream: StreamHeaderV1, cursor: CursorV1) -> bool {
        self.cursors.contains(&cursor)
    }

    fn exact_range_is_available(&self, _stream: StreamHeaderV1, range: CursorRangeV1) -> bool {
        self.ranges.contains(&range)
    }

    fn replay_decision_is_current(
        &self,
        _stream: StreamHeaderV1,
        _replay: &StructurallyValidatedReplayUnavailableV1,
    ) -> bool {
        self.current_replay
    }
}

fn subscribe_latest_wire() -> public_exit::SubscribeRequestV1 {
    public_exit::SubscribeRequestV1 {
        stream_id: fixture_stream().stream_id().as_bytes().to_vec(),
        start: Some(public_exit::subscribe_request_v1::Start::Latest(true)),
    }
}

fn subscribe_cursor_wire() -> public_exit::SubscribeRequestV1 {
    public_exit::SubscribeRequestV1 {
        stream_id: fixture_stream().stream_id().as_bytes().to_vec(),
        start: Some(public_exit::subscribe_request_v1::Start::Cursor(
            fixture_stream().initial_cursor().fixed_encode().to_vec(),
        )),
    }
}

fn range_wire(start: CursorV1, end: CursorV1) -> public_exit::CursorRangeWireV1 {
    public_exit::CursorRangeWireV1 {
        start: start.fixed_encode().to_vec(),
        end: end.fixed_encode().to_vec(),
    }
}

fn public_hello_wire() -> public_exit::PublicHelloWireV1 {
    let (start, _, end) = fixture_record();
    public_exit::PublicHelloWireV1 {
        protocol_version: 1,
        stream: fixture_stream().fixed_encode().to_vec(),
        available: vec![range_wire(start, end)],
        live_tail: end.fixed_encode().to_vec(),
    }
}

fn public_event_wire() -> public_exit::PublicEventWireV1 {
    public_exit::PublicEventWireV1 {
        record: fixture_record().1.encode(),
    }
}

fn replay_wire(
    reason: i32,
    recovery: i32,
    successor: bool,
) -> public_exit::ReplayUnavailableWireV1 {
    public_exit::ReplayUnavailableWireV1 {
        reason,
        requested: fixture_stream().initial_cursor().fixed_encode().to_vec(),
        available: Vec::new(),
        successor_stream_id: if successor {
            vec![0xa0; 16]
        } else {
            Vec::new()
        },
        recovery,
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn assert_golden(message: &impl Message, expected_hex: &str) {
    assert_eq!(hex(&message.encode_to_vec()), expected_hex);
}

fn append_unknown_bytes(encoded: &mut Vec<u8>, length: usize) {
    // Unknown field 15, wire type 2. Prost discards it, while the raw-byte
    // decoder must still enforce the decompressed serialized size.
    encoded.push(0x7a);
    push_varint(encoded, length as u64);
    encoded.resize(encoded.len() + length, 0);
}

fn push_varint(encoded: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        encoded.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    encoded.push(value as u8);
}

fn wrap_message_field(field: u32, nested: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(nested.len() + 8);
    push_varint(&mut encoded, u64::from((field << 3) | 2));
    push_varint(&mut encoded, nested.len() as u64);
    encoded.extend_from_slice(nested);
    encoded
}

fn nested_unknown_bytes(length: usize) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(length + 8);
    append_unknown_bytes(&mut encoded, length);
    encoded
}

#[test]
fn sync_protobuf_messages_have_pinned_golden_bytes() {
    let (start, _, _) = fixture_record();
    let open_none = open_wire(None);
    let open_cursor = open_wire(Some(start));
    let ack = ack_wire();
    let resume = resume_wire();
    let record = sync_record_wire();
    let sync_error = sync_error_wire(1);
    let fetch = fetch_request_wire();
    let commit = transfer_commit_wire();

    assert_golden(
        &open_none,
        "0a10000102030405060708090a0b0c0d0e0f121070707070707070707070707070707070",
    );
    assert_golden(
        &open_cursor,
        concat!(
            "0a10000102030405060708090a0b0c0d0e0f121070707070707070707070707070707070",
            "1a280000000000000000137f7bb5fdd716883a9b6e5a7015f7156db136b1b3c2bdfe4915ba5a6ea87332"
        ),
    );
    assert_golden(
        &ack,
        concat!(
            "0a10000102030405060708090a0b0c0d0e0f121070707070707070707070707070707070",
            "1a20505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f",
            "221071717171717171717171717171717171",
            "2a280000000000000000137f7bb5fdd716883a9b6e5a7015f7156db136b1b3c2bdfe4915ba5a6ea87332"
        ),
    );
    assert_golden(
        &sync::SyncClientFrameV1 {
            frame: Some(sync::sync_client_frame_v1::Frame::Open(open_none)),
        },
        "0a240a10000102030405060708090a0b0c0d0e0f121070707070707070707070707070707070",
    );
    assert_golden(
        &sync::SyncClientFrameV1 {
            frame: Some(sync::sync_client_frame_v1::Frame::Ack(ack)),
        },
        concat!(
            "1282010a10000102030405060708090a0b0c0d0e0f121070707070707070707070707070707070",
            "1a20505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f",
            "221071717171717171717171717171717171",
            "2a280000000000000000137f7bb5fdd716883a9b6e5a7015f7156db136b1b3c2bdfe4915ba5a6ea87332"
        ),
    );
    let resume_hex = concat!(
        "0a76000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f2021222324252627",
        "28292a2b2c2d2e2f000000020001303132333435363738393a3b3c3d3e3f404142434445464748494a4b",
        "4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f121077777777",
        "7777777777777777777777771a280000000000000000137f7bb5fdd716883a9b6e5a7015f7156db136b",
        "1b3c2bdfe4915ba5a6ea8733222280000000000000000137f7bb5fdd716883a9b6e5a7015f7156db136b",
        "1b3c2bdfe4915ba5a6ea873322a280000000000000000137f7bb5fdd716883a9b6e5a7015f7156db136b",
        "1b3c2bdfe4915ba5a6ea87332303038014001"
    );
    let record_hex = concat!(
        "0a3300000000000000000000000000000003616263",
        "6917adaca6314c5baa91015944485cac8bae55c2028cd876e0072a7f6f45e583"
    );
    assert_golden(&resume, resume_hex);
    assert_golden(&record, record_hex);
    assert_golden(&sync_error, "0801");
    assert_golden(
        &sync::SyncServerFrameV1 {
            frame: Some(sync::sync_server_frame_v1::Frame::Resume(resume)),
        },
        &format!("0a8e02{resume_hex}"),
    );
    assert_golden(
        &sync::SyncServerFrameV1 {
            frame: Some(sync::sync_server_frame_v1::Frame::Record(record)),
        },
        &format!("1235{record_hex}"),
    );
    assert_golden(
        &sync::SyncServerFrameV1 {
            frame: Some(sync::sync_server_frame_v1::Frame::Error(sync_error)),
        },
        "1a020801",
    );
    assert_golden(
        &fetch,
        concat!(
            "0a1077777777777777777777777777777777",
            "122800000000000000028282828282828282828282828282828282828282828282828282828282828282",
            "18012002"
        ),
    );
    let commit_hex = concat!(
        "0a280000000000000000137f7bb5fdd716883a9b6e5a7015f7156db136b1b3c2bdfe4915ba5a6ea87332",
        "122800000000000000016917adaca6314c5baa91015944485cac8bae55c2028cd876e0072a7f6f45e583",
        "189c0122208888888888888888888888888888888888888888888888888888888888888888"
    );
    assert_golden(&commit, commit_hex);
    assert_golden(
        &sync::FetchRangePartWireV1 {
            part: Some(sync::fetch_range_part_wire_v1::Part::ChunkBytes(
                b"chunk".to_vec(),
            )),
        },
        "0a056368756e6b",
    );
    assert_golden(
        &sync::FetchRangePartWireV1 {
            part: Some(sync::fetch_range_part_wire_v1::Part::Commit(commit)),
        },
        &format!("1279{commit_hex}"),
    );
    assert_golden(
        &sync::FetchRangePartWireV1 {
            part: Some(sync::fetch_range_part_wire_v1::Part::Error(sync_error)),
        },
        "1a020801",
    );
}

#[test]
fn public_protobuf_messages_have_pinned_golden_bytes() {
    let list = public_exit::ListStreamsRequestV1 {};
    let discovery = empty_discovery_wire();
    let latest = subscribe_latest_wire();
    let cursor = subscribe_cursor_wire();
    let hello = public_hello_wire();
    let event = public_event_wire();
    let replay = replay_wire(3, 1, false);
    let public_error = public_exit::PublicErrorWireV1 { code: 1 };

    assert_golden(&list, "");
    assert_golden(
        &discovery,
        "0a280000000000000000c22825b985f95cfc234d7954dda16debbf2be90c8fd628449b1a8c9a5f35f597125c909090909090909090909090909090900000000000000000000000000000000000000000000000000000000000000000000000000000000000000000c22825b985f95cfc234d7954dda16debbf2be90c8fd628449b1a8c9a5f35f597",
    );
    assert_golden(&latest, "0a10000102030405060708090a0b0c0d0e0f1001");
    assert_golden(
        &cursor,
        concat!(
            "0a10000102030405060708090a0b0c0d0e0f",
            "1a280000000000000000137f7bb5fdd716883a9b6e5a7015f7156db136b1b3c2bdfe4915ba5a6ea87332"
        ),
    );
    let range_hex = concat!(
        "0a280000000000000000137f7bb5fdd716883a9b6e5a7015f7156db136b1b3c2bdfe4915ba5a6ea87332",
        "122800000000000000016917adaca6314c5baa91015944485cac8bae55c2028cd876e0072a7f6f45e583"
    );
    let hello_hex = concat!(
        "08011276000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20212223242526",
        "2728292a2b2c2d2e2f000000020001303132333435363738393a3b3c3d3e3f404142434445464748494a4b",
        "4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f1a54",
        "0a280000000000000000137f7bb5fdd716883a9b6e5a7015f7156db136b1b3c2bdfe4915ba5a6ea87332",
        "122800000000000000016917adaca6314c5baa91015944485cac8bae55c2028cd876e0072a7f6f45e583",
        "222800000000000000016917adaca6314c5baa91015944485cac8bae55c2028cd876e0072a7f6f45e583"
    );
    let event_hex = concat!(
        "0a3300000000000000000000000000000003616263",
        "6917adaca6314c5baa91015944485cac8bae55c2028cd876e0072a7f6f45e583"
    );
    let replay_hex = concat!(
        "080312280000000000000000137f7bb5fdd716883a9b6e5a7015f7156db136b",
        "1b3c2bdfe4915ba5a6ea873322801"
    );
    assert_golden(&hello.available[0], range_hex);
    assert_golden(&hello, hello_hex);
    assert_golden(&event, event_hex);
    assert_golden(&replay, replay_hex);
    assert_golden(&public_error, "0801");
    assert_golden(
        &public_exit::PublicServerFrameV1 {
            frame: Some(public_exit::public_server_frame_v1::Frame::Hello(hello)),
        },
        &format!("0afa01{hello_hex}"),
    );
    assert_golden(
        &public_exit::PublicServerFrameV1 {
            frame: Some(public_exit::public_server_frame_v1::Frame::Event(event)),
        },
        &format!("1235{event_hex}"),
    );
    assert_golden(
        &public_exit::PublicServerFrameV1 {
            frame: Some(public_exit::public_server_frame_v1::Frame::ReplayUnavailable(replay)),
        },
        &format!("1a2e{replay_hex}"),
    );
    assert_golden(
        &public_exit::PublicServerFrameV1 {
            frame: Some(public_exit::public_server_frame_v1::Frame::Error(
                public_error,
            )),
        },
        "22020801",
    );
}

#[test]
fn populated_discovery_has_pinned_golden_bytes() {
    let wire = discovery_for_manifest(&derived_manifest(0x12));
    assert_golden(
        &wire,
        "0a280000000000000000a465c7dba406b68585ae44ed528a9528e227e46ae4dc14454ed1466ffc4fa34a12b3019090909090909090909090909090909000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010000000000000015736f6c616e612e6d61696e6e65742f7075626c69630000000000000000121212121212121212121212121212122d99244c2ec6eadaf0288836018dad97d86f1e31f794515863bac1bd3df671090100a465c7dba406b68585ae44ed528a9528e227e46ae4dc14454ed1466ffc4fa34a1a8701000112121212121212121212121212121212202020202020202020202020202020202020202020202020202020202020202000000006000116261b57f2865dcb4b6c7cc8836a47b20acd8daed8f134ee577bfc5e1ec5802d2d99244c2ec6eadaf0288836018dad97d86f1e31f794515863bac1bd3df67109000000000000000243120000000000",
    );
}

#[test]
fn populated_replay_range_has_pinned_golden_bytes() {
    let (start, _, end) = fixture_record();
    let mut replay = replay_wire(3, 1, false);
    replay.available = vec![range_wire(start, end)];
    assert_golden(
        &replay,
        concat!(
            "080312280000000000000000137f7bb5fdd716883a9b6e5a7015f7156db136b",
            "1b3c2bdfe4915ba5a6ea873321a54",
            "0a280000000000000000137f7bb5fdd716883a9b6e5a7015f7156db136b1b3c2bdfe4915ba5a6ea87332",
            "122800000000000000016917adaca6314c5baa91015944485cac8bae55c2028cd876e0072a7f6f45e583",
            "2801"
        ),
    );
    assert!(validate_replay_unavailable(replay, PublicFeedKindV1::RawShred).is_ok());
}

#[test]
fn every_sync_message_converts_to_a_validated_type() {
    let (start, record, _) = fixture_record();

    let open_none = open_wire(None);
    assert_eq!(
        validate_open(open_none.clone()).unwrap().protected_cursor(),
        None
    );
    let open_frame = sync::SyncClientFrameV1 {
        frame: Some(sync::sync_client_frame_v1::Frame::Open(open_none)),
    };
    assert!(matches!(
        decode_sync_client_frame(&open_frame.encode_to_vec()),
        Ok(ValidatedSyncClientFrameV1::Open(_))
    ));

    let ack = ack_wire();
    assert_eq!(validate_ack(ack.clone()).unwrap().protected_cursor(), start);
    let ack_frame = sync::SyncClientFrameV1 {
        frame: Some(sync::sync_client_frame_v1::Frame::Ack(ack)),
    };
    assert!(matches!(
        decode_sync_client_frame(&ack_frame.encode_to_vec()),
        Ok(ValidatedSyncClientFrameV1::Ack(_))
    ));

    let resume = resume_wire();
    assert_eq!(
        validate_resume(resume.clone()).unwrap().stream(),
        fixture_stream()
    );
    let resume_frame = sync::SyncServerFrameV1 {
        frame: Some(sync::sync_server_frame_v1::Frame::Resume(resume)),
    };
    assert!(matches!(
        decode_sync_server_frame(&resume_frame.encode_to_vec(), start),
        Ok(ValidatedSyncServerFrameV1::Resume(_))
    ));

    let record_wire = sync_record_wire();
    assert_eq!(
        validate_sync_record(record_wire.clone(), start).unwrap(),
        record
    );
    let record_frame = sync::SyncServerFrameV1 {
        frame: Some(sync::sync_server_frame_v1::Frame::Record(record_wire)),
    };
    assert_eq!(
        decode_sync_server_frame(&record_frame.encode_to_vec(), start).unwrap(),
        ValidatedSyncServerFrameV1::Record(record)
    );

    assert_eq!(
        validate_sync_error(sync_error_wire(1)).unwrap().code(),
        HiveSyncErrorCodeV1::Unauthorized
    );
    let error_frame = sync::SyncServerFrameV1 {
        frame: Some(sync::sync_server_frame_v1::Frame::Error(sync_error_wire(9))),
    };
    assert!(matches!(
        decode_sync_server_frame(&error_frame.encode_to_vec(), start),
        Ok(ValidatedSyncServerFrameV1::Error(_))
    ));

    let fetch = fetch_request_wire();
    assert_eq!(
        decode_fetch_range_request(&fetch.encode_to_vec())
            .unwrap()
            .next_sequence(),
        2
    );
    let commit = transfer_commit_wire();
    assert_eq!(
        validate_transfer_commit(commit.clone())
            .unwrap()
            .encoded_len(),
        MIN_TRANSFER_CHUNK_V1_ENCODED_LEN as u64
    );
    for part in [
        sync::FetchRangePartWireV1 {
            part: Some(sync::fetch_range_part_wire_v1::Part::ChunkBytes(
                b"chunk".to_vec(),
            )),
        },
        sync::FetchRangePartWireV1 {
            part: Some(sync::fetch_range_part_wire_v1::Part::Commit(commit)),
        },
        sync::FetchRangePartWireV1 {
            part: Some(sync::fetch_range_part_wire_v1::Part::Error(
                sync_error_wire(1),
            )),
        },
    ] {
        assert!(decode_fetch_range_part(&part.encode_to_vec()).is_ok());
    }
}

#[test]
fn every_sync_error_enum_value_is_pinned_and_unknown_values_are_rejected() {
    let expected = [
        "0801", "0802", "0803", "0804", "0805", "0806", "0807", "0808", "0809",
    ];
    for (code, expected_hex) in (1..=9).zip(expected) {
        let wire = sync_error_wire(code);
        assert_golden(&wire, expected_hex);
        assert!(validate_sync_error(wire).is_ok());
    }
    for code in [0, 10, i32::MAX] {
        assert!(validate_sync_error(sync_error_wire(code)).is_err());
    }
}

#[test]
fn sync_rejects_absent_oneofs_wrong_lengths_and_unverified_records() {
    let (start, record, _) = fixture_record();
    let client = sync::SyncClientFrameV1 { frame: None };
    assert!(matches!(
        decode_sync_client_frame(&client.encode_to_vec()),
        Err(WireError::MissingOneof { .. })
    ));
    let server = sync::SyncServerFrameV1 { frame: None };
    assert!(matches!(
        decode_sync_server_frame(&server.encode_to_vec(), start),
        Err(WireError::MissingOneof { .. })
    ));
    let part = sync::FetchRangePartWireV1 { part: None };
    assert!(matches!(
        decode_fetch_range_part(&part.encode_to_vec()),
        Err(WireError::MissingOneof { .. })
    ));

    let mut open = open_wire(None);
    open.stream_id.pop();
    assert!(matches!(
        validate_open(open),
        Err(WireError::InvalidLength { expected: 16, .. })
    ));
    let mut open = open_wire(None);
    open.terminal_store_id.push(0);
    assert!(matches!(
        validate_open(open),
        Err(WireError::InvalidLength { expected: 16, .. })
    ));
    let mut open = open_wire(None);
    open.protected_cursor = vec![0; 39];
    assert!(validate_open(open).is_err());

    let mut ack = ack_wire();
    ack.stream_manifest_sha256.pop();
    assert!(matches!(
        validate_ack(ack),
        Err(WireError::InvalidLength { expected: 32, .. })
    ));
    let mut ack = ack_wire();
    ack.policy_id.push(0);
    assert!(matches!(
        validate_ack(ack),
        Err(WireError::InvalidLength { expected: 16, .. })
    ));
    let mut ack = ack_wire();
    ack.protected_cursor.push(0);
    assert!(validate_ack(ack).is_err());

    let mut resume = resume_wire();
    resume.stream.pop();
    assert!(validate_resume(resume).is_err());
    let mut resume = resume_wire();
    resume.session_id.pop();
    assert!(matches!(
        validate_resume(resume),
        Err(WireError::InvalidLength { expected: 16, .. })
    ));
    let mut resume = resume_wire();
    resume.cutover.pop();
    assert!(validate_resume(resume).is_err());

    let wrong_previous = cursor(0, 0xff);
    assert!(validate_sync_record(sync_record_wire(), wrong_previous).is_err());
    let mut trailing = record.encode();
    trailing.push(0);
    assert!(validate_sync_record(sync::RecordWireV1 { record: trailing }, start).is_err());
    let mut oversized_declared = Vec::with_capacity(48);
    oversized_declared.extend_from_slice(&0_u64.to_be_bytes());
    oversized_declared.extend_from_slice(&(MAX_RECORD_PAYLOAD_BYTES + 1).to_be_bytes());
    oversized_declared.extend_from_slice(&[0; 32]);
    assert!(
        validate_sync_record(
            sync::RecordWireV1 {
                record: oversized_declared,
            },
            start,
        )
        .is_err()
    );
}

#[test]
fn sync_enforces_resume_fetch_commit_and_chunk_limits() {
    for mutate in [
        |wire: &mut sync::ResumeWireV1| wire.max_record_bytes = 47,
        |wire: &mut sync::ResumeWireV1| wire.max_record_bytes = MAX_SYNC_RECORD_BYTES_V1 + 1,
        |wire: &mut sync::ResumeWireV1| wire.max_chunk_records = 0,
        |wire: &mut sync::ResumeWireV1| wire.max_chunk_records = MAX_CHUNK_RECORDS_V1 + 1,
        |wire: &mut sync::ResumeWireV1| wire.max_parallel_fetches = 0,
        |wire: &mut sync::ResumeWireV1| wire.max_parallel_fetches = 65,
        |wire: &mut sync::ResumeWireV1| wire.max_parallel_fetches = u32::MAX,
    ] {
        let mut wire = resume_wire();
        mutate(&mut wire);
        assert!(validate_resume(wire).is_err());
    }
    let mut resume = resume_wire();
    resume.first_available = cursor(2, 2).fixed_encode().to_vec();
    resume.bulk_start = cursor(1, 1).fixed_encode().to_vec();
    resume.cutover = cursor(1, 1).fixed_encode().to_vec();
    assert!(validate_resume(resume).is_err());

    let mut fetch = fetch_request_wire();
    fetch.first_sequence = 2;
    fetch.next_sequence = 2;
    assert!(validate_fetch_range_request(fetch.clone(), fetch.encoded_len()).is_err());
    fetch.next_sequence = 1;
    assert!(validate_fetch_range_request(fetch.clone(), fetch.encoded_len()).is_err());
    let mut fetch = fetch_request_wire();
    fetch.next_sequence = 3;
    assert!(validate_fetch_range_request(fetch.clone(), fetch.encoded_len()).is_err());
    let mut fetch = fetch_request_wire();
    fetch.cutover = cursor(u64::from(MAX_CHUNK_RECORDS_V1) + 1, 9)
        .fixed_encode()
        .to_vec();
    fetch.first_sequence = 0;
    fetch.next_sequence = u64::from(MAX_CHUNK_RECORDS_V1) + 1;
    assert!(validate_fetch_range_request(fetch.clone(), fetch.encoded_len()).is_err());
    let mut fetch = fetch_request_wire();
    fetch.session_id.pop();
    assert!(matches!(
        validate_fetch_range_request(fetch.clone(), fetch.encoded_len()),
        Err(WireError::InvalidLength { expected: 16, .. })
    ));
    let mut fetch = fetch_request_wire();
    fetch.cutover.push(0);
    assert!(validate_fetch_range_request(fetch.clone(), fetch.encoded_len()).is_err());

    let mut commit = transfer_commit_wire();
    commit.end = commit.start.clone();
    assert!(validate_transfer_commit(commit).is_err());
    let mut commit = transfer_commit_wire();
    core::mem::swap(&mut commit.start, &mut commit.end);
    assert!(validate_transfer_commit(commit).is_err());
    let mut commit = transfer_commit_wire();
    commit.encoded_len = (MIN_TRANSFER_CHUNK_V1_ENCODED_LEN - 1) as u64;
    assert!(validate_transfer_commit(commit).is_err());
    let mut commit = transfer_commit_wire();
    commit.encoded_sha256.pop();
    assert!(matches!(
        validate_transfer_commit(commit),
        Err(WireError::InvalidLength { expected: 32, .. })
    ));
    let mut commit = transfer_commit_wire();
    commit.start.pop();
    assert!(validate_transfer_commit(commit).is_err());

    let empty = sync::FetchRangePartWireV1 {
        part: Some(sync::fetch_range_part_wire_v1::Part::ChunkBytes(Vec::new())),
    };
    assert!(validate_fetch_range_part(empty.clone(), empty.encoded_len()).is_err());
    let too_large = sync::FetchRangePartWireV1 {
        part: Some(sync::fetch_range_part_wire_v1::Part::ChunkBytes(vec![
            0;
            FETCH_CHUNK_BYTES_MAX_BYTES + 1
        ])),
    };
    assert!(matches!(
        validate_fetch_range_part(too_large.clone(), too_large.encoded_len()),
        Err(WireError::MessageTooLarge { .. })
    ));
}

#[test]
fn sync_checks_actual_serialized_size_before_protobuf_semantics() {
    let oversized_invalid = vec![0; SYNC_CONTROL_PROTOBUF_MAX_BYTES + 1];
    assert!(matches!(
        decode_sync_client_frame(&oversized_invalid),
        Err(WireError::MessageTooLarge { .. })
    ));

    let client = sync::SyncClientFrameV1 {
        frame: Some(sync::sync_client_frame_v1::Frame::Open(open_wire(None))),
    };
    assert!(matches!(
        validate_sync_client_frame(client.clone(), client.encoded_len() - 1),
        Err(WireError::InvalidValue { .. })
    ));
    assert!(matches!(
        validate_sync_client_frame(client, SYNC_CONTROL_PROTOBUF_MAX_BYTES + 1),
        Err(WireError::MessageTooLarge { .. })
    ));

    let (start, _, _) = fixture_record();
    let control = sync::SyncServerFrameV1 {
        frame: Some(sync::sync_server_frame_v1::Frame::Error(sync_error_wire(1))),
    };
    assert!(matches!(
        validate_sync_server_frame(control, SYNC_CONTROL_PROTOBUF_MAX_BYTES + 1, start,),
        Err(WireError::MessageTooLarge { .. })
    ));
    let mut encoded_control = sync::SyncServerFrameV1 {
        frame: Some(sync::sync_server_frame_v1::Frame::Error(sync_error_wire(1))),
    }
    .encode_to_vec();
    append_unknown_bytes(&mut encoded_control, SYNC_CONTROL_PROTOBUF_MAX_BYTES);
    assert!(matches!(
        decode_sync_server_frame(&encoded_control, start),
        Err(WireError::MessageTooLarge { .. })
    ));
    let record = sync::SyncServerFrameV1 {
        frame: Some(sync::sync_server_frame_v1::Frame::Record(sync_record_wire())),
    };
    assert!(matches!(
        validate_sync_server_frame(record, SYNC_SERVER_RECORD_FRAME_MAX_BYTES + 1, start,),
        Err(WireError::MessageTooLarge { .. })
    ));

    let fetch = fetch_request_wire();
    assert!(matches!(
        validate_fetch_range_request(fetch, SYNC_CONTROL_PROTOBUF_MAX_BYTES + 1),
        Err(WireError::MessageTooLarge { .. })
    ));
    let part = sync::FetchRangePartWireV1 {
        part: Some(sync::fetch_range_part_wire_v1::Part::Error(
            sync_error_wire(1),
        )),
    };
    assert!(matches!(
        validate_fetch_range_part(part, SYNC_CONTROL_PROTOBUF_MAX_BYTES + 1),
        Err(WireError::MessageTooLarge { .. })
    ));
    let oversized_part = vec![0; FETCH_RANGE_PART_MAX_BYTES + 1];
    assert!(matches!(
        decode_fetch_range_part(&oversized_part),
        Err(WireError::MessageTooLarge { .. })
    ));
}

#[test]
fn every_public_message_converts_to_a_validated_type() {
    assert!(
        decode_list_streams_request(&public_exit::ListStreamsRequestV1 {}.encode_to_vec()).is_ok()
    );
    let empty_discovery = empty_discovery_wire();
    let decoded = decode_public_stream_list(&empty_discovery.encode_to_vec()).unwrap();
    assert!(decoded.registry().entries().is_empty());
    assert!(decoded.public_manifests().is_empty());

    let latest = decode_subscribe_request(&subscribe_latest_wire().encode_to_vec()).unwrap();
    assert_eq!(latest.start(), PublicStartV1::Latest);
    let cursor_request =
        decode_subscribe_request(&subscribe_cursor_wire().encode_to_vec()).unwrap();
    assert_eq!(
        cursor_request.start(),
        PublicStartV1::Cursor(fixture_stream().initial_cursor())
    );

    let (start, record, end) = fixture_record();
    let range = validate_cursor_range(range_wire(start, end)).unwrap();
    assert_eq!(range.start(), start);
    assert_eq!(range.end(), end);
    let hello = validate_public_hello(public_hello_wire()).unwrap();
    assert_eq!(hello.live_tail(), end);
    assert_eq!(hello.available(), &[range]);
    assert_eq!(
        validate_public_event(public_event_wire(), start).unwrap(),
        record
    );

    let replay =
        validate_replay_unavailable(replay_wire(3, 1, false), PublicFeedKindV1::RawShred).unwrap();
    assert_eq!(replay.reason(), ReplayReasonV1::HistoryPending);
    assert_eq!(replay.recovery(), ReplayRecoveryV1::Retry);
    assert_eq!(replay.requested(), start);
    assert_eq!(
        validate_public_error(public_exit::PublicErrorWireV1 { code: 1 })
            .unwrap()
            .code(),
        PublicErrorCodeV1::UnknownStream
    );

    for frame in [
        public_exit::PublicServerFrameV1 {
            frame: Some(public_exit::public_server_frame_v1::Frame::Hello(
                public_hello_wire(),
            )),
        },
        public_exit::PublicServerFrameV1 {
            frame: Some(public_exit::public_server_frame_v1::Frame::Event(
                public_event_wire(),
            )),
        },
        public_exit::PublicServerFrameV1 {
            frame: Some(
                public_exit::public_server_frame_v1::Frame::ReplayUnavailable(replay_wire(
                    3, 1, false,
                )),
            ),
        },
        public_exit::PublicServerFrameV1 {
            frame: Some(public_exit::public_server_frame_v1::Frame::Error(
                public_exit::PublicErrorWireV1 { code: 1 },
            )),
        },
    ] {
        assert!(
            decode_public_server_frame(&frame.encode_to_vec(), PublicFeedKindV1::RawShred, start,)
                .is_ok()
        );
    }
}

#[test]
fn every_public_enum_value_has_a_valid_pinned_fixture() {
    let replay_fixtures = [
        (
            PublicFeedKindV1::ShredBlockObservation,
            replay_wire(1, 2, false),
            concat!(
                "080112280000000000000000137f7bb5fdd716883a9b6e5a7015f7156db136b",
                "1b3c2bdfe4915ba5a6ea873322802"
            ),
        ),
        (
            PublicFeedKindV1::RawShred,
            replay_wire(2, 3, true),
            concat!(
                "080212280000000000000000137f7bb5fdd716883a9b6e5a7015f7156db136b",
                "1b3c2bdfe4915ba5a6ea873322210a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a02803"
            ),
        ),
        (
            PublicFeedKindV1::RawShred,
            replay_wire(3, 1, false),
            concat!(
                "080312280000000000000000137f7bb5fdd716883a9b6e5a7015f7156db136b",
                "1b3c2bdfe4915ba5a6ea873322801"
            ),
        ),
        (
            PublicFeedKindV1::RawShred,
            replay_wire(4, 3, false),
            concat!(
                "080412280000000000000000137f7bb5fdd716883a9b6e5a7015f7156db136b",
                "1b3c2bdfe4915ba5a6ea873322803"
            ),
        ),
    ];
    for (feed, wire, expected_hex) in replay_fixtures {
        assert_golden(&wire, expected_hex);
        assert!(validate_replay_unavailable(wire, feed).is_ok());
    }

    let public_error_goldens = ["0801", "0802", "0803", "0804", "0805"];
    for (code, expected_hex) in (1..=5).zip(public_error_goldens) {
        let wire = public_exit::PublicErrorWireV1 { code };
        assert_golden(&wire, expected_hex);
        assert!(validate_public_error(wire).is_ok());
    }
    for code in [0, 6, i32::MAX] {
        assert!(validate_public_error(public_exit::PublicErrorWireV1 { code }).is_err());
    }
    for reason in [0, 5, i32::MAX] {
        assert!(
            validate_replay_unavailable(replay_wire(reason, 1, false), PublicFeedKindV1::RawShred,)
                .is_err()
        );
    }
    for recovery in [0, 4, i32::MAX] {
        assert!(
            validate_replay_unavailable(
                replay_wire(3, recovery, false),
                PublicFeedKindV1::RawShred,
            )
            .is_err()
        );
    }
}

#[test]
fn replay_reason_recovery_matrix_is_exhaustive() {
    for feed in [
        PublicFeedKindV1::RawShred,
        PublicFeedKindV1::ShredBlockObservation,
    ] {
        for reason in 1..=4 {
            for recovery in 1..=3 {
                let successor = reason == 2;
                let actual =
                    validate_replay_unavailable(replay_wire(reason, recovery, successor), feed)
                        .is_ok();
                let expected = matches!(
                    (feed, reason, recovery),
                    (PublicFeedKindV1::RawShred, 3, 1)
                        | (PublicFeedKindV1::RawShred, 4, 3)
                        | (PublicFeedKindV1::RawShred, 2, 3)
                        | (PublicFeedKindV1::ShredBlockObservation, 1, 2)
                        | (PublicFeedKindV1::ShredBlockObservation, 2, 3)
                );
                assert_eq!(
                    actual, expected,
                    "feed={feed:?} reason={reason} recovery={recovery}"
                );
            }
        }
    }
    assert!(
        validate_replay_unavailable(replay_wire(2, 3, false), PublicFeedKindV1::RawShred,).is_err()
    );
    assert!(
        validate_replay_unavailable(replay_wire(3, 1, true), PublicFeedKindV1::RawShred,).is_err()
    );
    let mut wrong_successor_len = replay_wire(2, 3, true);
    wrong_successor_len.successor_stream_id.pop();
    assert!(validate_replay_unavailable(wrong_successor_len, PublicFeedKindV1::RawShred,).is_err());
}

#[test]
fn discovery_validates_head_snapshot_public_subset_and_source_selection() {
    let raw_v2 = capture_manifest(0x10, 2);
    let raw_v3 = capture_manifest(0x11, 3);
    let observation = derived_manifest(0x12);

    for (manifest, expected_feed) in [
        (&raw_v2, PublicFeedKindV1::RawShred),
        (&raw_v3, PublicFeedKindV1::RawShred),
        (&observation, PublicFeedKindV1::ShredBlockObservation),
    ] {
        let wire = discovery_for_manifest(manifest);
        let discovery = decode_public_stream_list(&wire.encode_to_vec()).unwrap();
        let request = decode_subscribe_request(
            &public_exit::SubscribeRequestV1 {
                stream_id: manifest.stream().stream_id().as_bytes().to_vec(),
                start: Some(public_exit::subscribe_request_v1::Start::Latest(true)),
            }
            .encode_to_vec(),
        )
        .unwrap();
        let discovery = bind_discovery(&discovery);
        assert_eq!(
            request.validate_discovery(discovery).unwrap().feed(),
            expected_feed
        );
    }

    let entries = vec![
        registry_entry(
            b"solana.mainnet/a",
            0,
            raw_v2.stream().stream_id(),
            raw_v2.stream().stream_manifest_sha256(),
            StreamRegistryStatusV1::Active,
            None,
        ),
        registry_entry(
            b"solana.mainnet/b",
            0,
            raw_v3.stream().stream_id(),
            raw_v3.stream().stream_manifest_sha256(),
            StreamRegistryStatusV1::Active,
            None,
        ),
    ];
    let subset = discovery_wire(entries, vec![raw_v2.clone()]);
    let subset = decode_public_stream_list(&subset.encode_to_vec()).unwrap();
    assert_eq!(subset.registry().entries().len(), 2);
    assert_eq!(subset.public_manifests().len(), 1);

    let unavailable = SubscribeV1 {
        stream_id: raw_v3.stream().stream_id(),
        start: PublicStartV1::Latest,
    };
    let bound_subset = bind_discovery(&subset);
    assert!(unavailable.validate_discovery(bound_subset).is_err());

    let exact_zero = SubscribeV1 {
        stream_id: raw_v2.stream().stream_id(),
        start: PublicStartV1::Cursor(raw_v2.stream().initial_cursor()),
    };
    assert_eq!(
        exact_zero.validate_discovery(bound_subset).unwrap().feed(),
        PublicFeedKindV1::RawShred
    );
    let wrong_zero = SubscribeV1 {
        stream_id: raw_v2.stream().stream_id(),
        start: PublicStartV1::Cursor(CursorV1::new(0, PrefixHash::new([0xff; 32]))),
    };
    assert!(wrong_zero.validate_discovery(bound_subset).is_err());
}

#[test]
fn discovery_requires_configured_authority_and_an_exact_transition_baseline() {
    let manifest = capture_manifest(0x10, 2);
    let wire = discovery_for_manifest(&manifest);
    let generation_zero = decode_public_stream_list(&wire.encode_to_vec()).unwrap();

    assert!(
        generation_zero
            .validate_context(DiscoveryValidationContextV1::new(
                BlockzillaAuthorityId::new([0x91; 16]),
                None,
            ))
            .is_err()
    );
    generation_zero
        .validate_context(DiscoveryValidationContextV1::new(
            BlockzillaAuthorityId::new([0x90; 16]),
            None,
        ))
        .unwrap();
    generation_zero
        .validate_context(DiscoveryValidationContextV1::new(
            BlockzillaAuthorityId::new([0x90; 16]),
            Some(generation_zero.registry()),
        ))
        .unwrap();

    let generation_one_registry = StreamRegistrySnapshotV1::new(
        BlockzillaAuthorityId::new([0x90; 16]),
        1,
        generation_zero.registry().snapshot_sha256(),
        generation_zero.registry().entries().to_vec(),
    )
    .unwrap();
    let generation_one = StructurallyValidatedPublicStreamListV1 {
        registry_head: StreamRegistryHeadV1::from_snapshot(&generation_one_registry),
        registry: generation_one_registry,
        public_manifests: generation_zero.public_manifests().to_vec(),
    };
    generation_one
        .validate_context(DiscoveryValidationContextV1::new(
            BlockzillaAuthorityId::new([0x90; 16]),
            Some(generation_zero.registry()),
        ))
        .unwrap();
    assert!(
        generation_one
            .validate_context(DiscoveryValidationContextV1::new(
                BlockzillaAuthorityId::new([0x90; 16]),
                None,
            ))
            .is_err()
    );

    let unrelated_prior = StreamRegistrySnapshotV1::new(
        BlockzillaAuthorityId::new([0x90; 16]),
        0,
        StreamRegistrySnapshotSha256::new([0; 32]),
        Vec::new(),
    )
    .unwrap();
    assert!(
        generation_one
            .validate_context(DiscoveryValidationContextV1::new(
                BlockzillaAuthorityId::new([0x90; 16]),
                Some(&unrelated_prior),
            ))
            .is_err()
    );
}

#[test]
fn discovery_rejects_mismatched_or_noncanonical_authority_data() {
    let first = capture_manifest(0x10, 2);
    let second = capture_manifest(0x11, 3);
    let valid_entries = vec![
        registry_entry(
            b"solana.mainnet/a",
            0,
            first.stream().stream_id(),
            first.stream().stream_manifest_sha256(),
            StreamRegistryStatusV1::Active,
            None,
        ),
        registry_entry(
            b"solana.mainnet/b",
            0,
            second.stream().stream_id(),
            second.stream().stream_manifest_sha256(),
            StreamRegistryStatusV1::Active,
            None,
        ),
    ];

    let valid = discovery_wire(valid_entries.clone(), vec![first.clone(), second.clone()]);
    assert!(validate_public_stream_list(valid.clone(), valid.encoded_len()).is_ok());

    let mut wrong_head = valid.clone();
    *wrong_head.registry_head.last_mut().unwrap() ^= 1;
    assert!(validate_public_stream_list(wrong_head.clone(), wrong_head.encoded_len()).is_err());

    let mut trailing_registry = valid.clone();
    trailing_registry.registry.push(0);
    assert!(
        validate_public_stream_list(trailing_registry.clone(), trailing_registry.encoded_len(),)
            .is_err()
    );

    let mut trailing_manifest = valid.clone();
    trailing_manifest.public_manifests[0].push(0);
    assert!(
        validate_public_stream_list(trailing_manifest.clone(), trailing_manifest.encoded_len(),)
            .is_err()
    );

    let mut unsorted = valid.clone();
    unsorted.public_manifests.swap(0, 1);
    assert!(matches!(
        validate_public_stream_list(unsorted.clone(), unsorted.encoded_len()),
        Err(WireError::NonCanonicalOrder { .. })
    ));

    let mut duplicate = valid.clone();
    duplicate.public_manifests[1] = duplicate.public_manifests[0].clone();
    assert!(matches!(
        validate_public_stream_list(duplicate.clone(), duplicate.encoded_len()),
        Err(WireError::NonCanonicalOrder { .. })
    ));

    let missing_entry = discovery_wire(Vec::new(), vec![first.clone()]);
    assert!(matches!(
        validate_public_stream_list(missing_entry.clone(), missing_entry.encoded_len()),
        Err(WireError::RegistryMismatch { .. })
    ));

    let wrong_digest = discovery_wire(
        vec![registry_entry(
            b"solana.mainnet/a",
            0,
            first.stream().stream_id(),
            StreamManifestSha256::new([0xee; 32]),
            StreamRegistryStatusV1::Active,
            None,
        )],
        vec![first],
    );
    assert!(matches!(
        validate_public_stream_list(wrong_digest.clone(), wrong_digest.encoded_len()),
        Err(WireError::RegistryMismatch { .. })
    ));

    let non_public = capture_manifest(0x13, 5);
    let non_public = discovery_for_manifest(&non_public);
    assert!(matches!(
        validate_public_stream_list(non_public.clone(), non_public.encoded_len()),
        Err(WireError::InvalidValue { .. })
    ));
}

#[test]
fn replaced_replay_successor_must_match_the_registry_chain() {
    let old_id = StreamId::new([0x10; 16]);
    let new_id = StreamId::new([0x11; 16]);
    let registry = StreamRegistrySnapshotV1::new(
        BlockzillaAuthorityId::new([0x90; 16]),
        0,
        StreamRegistrySnapshotSha256::new([0; 32]),
        vec![
            registry_entry(
                b"solana.mainnet/raw",
                0,
                old_id,
                StreamManifestSha256::new([0x40; 32]),
                StreamRegistryStatusV1::Closed,
                Some(new_id),
            ),
            registry_entry(
                b"solana.mainnet/raw",
                1,
                new_id,
                StreamManifestSha256::new([0x41; 32]),
                StreamRegistryStatusV1::Active,
                None,
            ),
        ],
    )
    .unwrap();
    let discovery = StructurallyValidatedPublicStreamListV1 {
        registry_head: StreamRegistryHeadV1::from_snapshot(&registry),
        registry,
        public_manifests: Vec::new(),
    };
    let discovery = bind_discovery(&discovery);
    let mut replaced_wire = replay_wire(2, 3, true);
    replaced_wire.successor_stream_id = new_id.as_bytes().to_vec();
    let replay = validate_replay_unavailable(replaced_wire, PublicFeedKindV1::RawShred).unwrap();
    replay
        .validate_registry_successor(old_id, discovery)
        .unwrap();
    assert!(
        replay
            .validate_registry_successor(StreamId::new([0x22; 16]), discovery)
            .is_err()
    );

    let mut wrong = replay_wire(2, 3, true);
    wrong.successor_stream_id = vec![0x22; 16];
    let wrong = validate_replay_unavailable(wrong, PublicFeedKindV1::RawShred).unwrap();
    assert!(
        wrong
            .validate_registry_successor(old_id, discovery)
            .is_err()
    );
}

#[test]
fn structural_replay_requires_authenticated_cursor_and_decision_context() {
    let manifest = capture_manifest(0x10, 2);
    let stream = manifest.stream();
    let discovery_wire = discovery_for_manifest(&manifest);
    let discovery = decode_public_stream_list(&discovery_wire.encode_to_vec()).unwrap();
    let discovery = bind_discovery(&discovery);
    let subscription = SubscribeV1 {
        stream_id: stream.stream_id(),
        start: PublicStartV1::Cursor(stream.initial_cursor()),
    }
    .validate_discovery(discovery)
    .unwrap();
    let mut replay_wire = replay_wire(3, 1, false);
    replay_wire.requested = stream.initial_cursor().fixed_encode().to_vec();
    let replay = validate_replay_unavailable(replay_wire, PublicFeedKindV1::RawShred).unwrap();

    let valid_context = TestPublicContext {
        cursors: vec![stream.initial_cursor()],
        ranges: Vec::new(),
        current_replay: true,
    };
    replay
        .validate_context(subscription, &valid_context)
        .unwrap();

    let mismatched_manifest = capture_manifest(0x10, 3);
    let forged_subscription = ContextValidatedSubscribeV1 {
        request: subscription.request(),
        discovery,
        manifest: &mismatched_manifest,
        feed: PublicFeedKindV1::RawShred,
    };
    assert!(matches!(
        replay.validate_context(forged_subscription, &valid_context),
        Err(WireError::RegistryMismatch { .. })
    ));

    let missing_cursor = TestPublicContext {
        cursors: Vec::new(),
        ranges: Vec::new(),
        current_replay: true,
    };
    assert!(
        replay
            .validate_context(subscription, &missing_cursor)
            .is_err()
    );
    let stale_decision = TestPublicContext {
        cursors: vec![stream.initial_cursor()],
        ranges: Vec::new(),
        current_replay: false,
    };
    assert!(
        replay
            .validate_context(subscription, &stale_decision)
            .is_err()
    );
}

#[test]
fn public_rejects_absent_oneofs_false_latest_and_wrong_fixed_lengths() {
    let subscribe = public_exit::SubscribeRequestV1 {
        stream_id: vec![0; 16],
        start: None,
    };
    assert!(matches!(
        decode_subscribe_request(&subscribe.encode_to_vec()),
        Err(WireError::MissingOneof { .. })
    ));
    let false_latest = public_exit::SubscribeRequestV1 {
        stream_id: vec![0; 16],
        start: Some(public_exit::subscribe_request_v1::Start::Latest(false)),
    };
    assert!(matches!(
        decode_subscribe_request(&false_latest.encode_to_vec()),
        Err(WireError::InvalidBoolean { .. })
    ));
    let server = public_exit::PublicServerFrameV1 { frame: None };
    assert!(matches!(
        decode_public_server_frame(
            &server.encode_to_vec(),
            PublicFeedKindV1::RawShred,
            fixture_stream().initial_cursor(),
        ),
        Err(WireError::MissingOneof { .. })
    ));

    let mut subscribe = subscribe_latest_wire();
    subscribe.stream_id.pop();
    assert!(matches!(
        validate_subscribe_request(subscribe.clone(), subscribe.encoded_len()),
        Err(WireError::InvalidLength { expected: 16, .. })
    ));
    let mut subscribe = subscribe_cursor_wire();
    subscribe.start = Some(public_exit::subscribe_request_v1::Start::Cursor(vec![
        0;
        39
    ]));
    assert!(validate_subscribe_request(subscribe.clone(), subscribe.encoded_len()).is_err());

    let (start, _, end) = fixture_record();
    let mut range = range_wire(start, end);
    range.start.pop();
    assert!(validate_cursor_range(range).is_err());
    let mut range = range_wire(start, end);
    range.end.push(0);
    assert!(validate_cursor_range(range).is_err());

    let mut hello = public_hello_wire();
    hello.stream.pop();
    assert!(validate_public_hello(hello).is_err());
    let mut hello = public_hello_wire();
    hello.live_tail.pop();
    assert!(validate_public_hello(hello).is_err());
    let mut hello = public_hello_wire();
    hello.protocol_version = 0;
    assert!(validate_public_hello(hello).is_err());
    let mut hello = public_hello_wire();
    hello.protocol_version = 2;
    assert!(validate_public_hello(hello).is_err());

    let wrong_previous = cursor(0, 0xff);
    assert!(validate_public_event(public_event_wire(), wrong_previous).is_err());
    let mut event = public_event_wire();
    event.record.push(0);
    assert!(validate_public_event(event, start).is_err());

    let mut replay = replay_wire(3, 1, false);
    replay.requested.pop();
    assert!(validate_replay_unavailable(replay, PublicFeedKindV1::RawShred).is_err());
}

#[test]
fn public_ranges_are_nonempty_sorted_disjoint_merged_and_tail_bound() {
    let same = cursor(1, 1);
    assert!(validate_cursor_range(range_wire(same, same)).is_err());
    assert!(validate_cursor_range(range_wire(cursor(2, 2), cursor(1, 1))).is_err());

    let mut hello = public_hello_wire();
    hello.available = vec![
        range_wire(cursor(2, 2), cursor(3, 3)),
        range_wire(cursor(1, 1), cursor(2, 2)),
    ];
    hello.live_tail = cursor(3, 3).fixed_encode().to_vec();
    assert!(matches!(
        validate_public_hello(hello),
        Err(WireError::NonCanonicalOrder { .. })
    ));

    let mut hello = public_hello_wire();
    hello.available = vec![
        range_wire(cursor(1, 1), cursor(3, 3)),
        range_wire(cursor(2, 2), cursor(4, 4)),
    ];
    hello.live_tail = cursor(4, 4).fixed_encode().to_vec();
    assert!(matches!(
        validate_public_hello(hello),
        Err(WireError::NonCanonicalOrder { .. })
    ));

    let mut hello = public_hello_wire();
    hello.available = vec![
        range_wire(cursor(1, 1), cursor(2, 2)),
        range_wire(cursor(2, 2), cursor(3, 3)),
    ];
    hello.live_tail = cursor(3, 3).fixed_encode().to_vec();
    assert!(matches!(
        validate_public_hello(hello),
        Err(WireError::NonCanonicalOrder { .. })
    ));

    let mut hello = public_hello_wire();
    hello.available = vec![range_wire(cursor(1, 1), cursor(3, 3))];
    hello.live_tail = cursor(2, 2).fixed_encode().to_vec();
    assert!(validate_public_hello(hello).is_err());

    let mut hello = public_hello_wire();
    let (_, _, tail) = fixture_record();
    hello.available[0].end = CursorV1::new(tail.next_sequence(), PrefixHash::new([0xee; 32]))
        .fixed_encode()
        .to_vec();
    assert!(validate_public_hello(hello).is_err());

    let mut hello = public_hello_wire();
    hello.available[0].start = CursorV1::new(0, PrefixHash::new([0xee; 32]))
        .fixed_encode()
        .to_vec();
    assert!(validate_public_hello(hello).is_err());

    let mut hello = public_hello_wire();
    hello.available = vec![range_wire(cursor(1, 1), cursor(2, 2)); PUBLIC_AVAILABLE_RANGES_MAX + 1];
    hello.live_tail = cursor(2, 2).fixed_encode().to_vec();
    assert!(matches!(
        validate_public_hello(hello),
        Err(WireError::TooManyItems { .. })
    ));
}

#[test]
fn hello_requires_exact_request_and_tail_boundary_hashes() {
    let manifest = capture_manifest(0x10, 2);
    let stream = manifest.stream();
    let discovery_wire = discovery_for_manifest(&manifest);
    let discovery = decode_public_stream_list(&discovery_wire.encode_to_vec()).unwrap();
    let discovery = bind_discovery(&discovery);
    let start = stream.initial_cursor();
    let end = RecordV1::new(start, b"abc".to_vec()).unwrap().end_cursor();
    let hello = StructurallyValidatedPublicHelloV1 {
        stream,
        available: vec![CursorRangeV1::new(start, end).unwrap()],
        live_tail: end,
    };
    let context = TestPublicContext {
        cursors: vec![start, end],
        ranges: hello.available().to_vec(),
        current_replay: false,
    };
    let exact = SubscribeV1 {
        stream_id: stream.stream_id(),
        start: PublicStartV1::Cursor(start),
    }
    .validate_discovery(discovery)
    .unwrap();
    hello.validate_for_request(exact, &context).unwrap();
    let latest = SubscribeV1 {
        stream_id: stream.stream_id(),
        start: PublicStartV1::Latest,
    }
    .validate_discovery(discovery)
    .unwrap();
    hello.validate_for_request(latest, &context).unwrap();

    let wrong_header = StreamHeaderV1::new(
        stream.stream_id(),
        stream.cluster_genesis_hash(),
        stream.payload_format(),
        stream.payload_format_version(),
        ProducerConfigSha256::new([0xee; 32]),
        stream.stream_manifest_sha256(),
    )
    .unwrap();
    let wrong_header_hello = StructurallyValidatedPublicHelloV1 {
        stream: wrong_header,
        available: Vec::new(),
        live_tail: wrong_header.initial_cursor(),
    };
    assert!(
        wrong_header_hello
            .validate_for_request(latest, &TestPublicContext::default())
            .is_err()
    );

    let future = SubscribeV1 {
        stream_id: stream.stream_id(),
        start: PublicStartV1::Cursor(cursor(2, 2)),
    }
    .validate_discovery(discovery)
    .unwrap();
    assert!(hello.validate_for_request(future, &context).is_err());
    let wrong_tail_hash = SubscribeV1 {
        stream_id: stream.stream_id(),
        start: PublicStartV1::Cursor(CursorV1::new(
            end.next_sequence(),
            PrefixHash::new([0xee; 32]),
        )),
    }
    .validate_discovery(discovery)
    .unwrap();
    assert!(
        hello
            .validate_for_request(wrong_tail_hash, &context)
            .is_err()
    );
    let wrong_start_hash = SubscribeV1 {
        stream_id: stream.stream_id(),
        start: PublicStartV1::Cursor(CursorV1::new(0, PrefixHash::new([0xee; 32]))),
    };
    assert!(wrong_start_hash.validate_discovery(discovery).is_err());

    let gapped = StructurallyValidatedPublicHelloV1 {
        stream,
        available: vec![CursorRangeV1::new(cursor(2, 2), cursor(3, 3)).unwrap()],
        live_tail: cursor(3, 3),
    };
    let request = SubscribeV1 {
        stream_id: stream.stream_id(),
        start: PublicStartV1::Cursor(cursor(1, 1)),
    }
    .validate_discovery(discovery)
    .unwrap();
    let gapped_context = TestPublicContext {
        cursors: vec![cursor(1, 1), cursor(2, 2), cursor(3, 3)],
        ranges: gapped.available().to_vec(),
        current_replay: false,
    };
    assert!(
        gapped
            .validate_for_request(request, &gapped_context)
            .is_err()
    );

    let interior = StructurallyValidatedPublicHelloV1 {
        stream,
        available: vec![CursorRangeV1::new(cursor(1, 1), cursor(3, 3)).unwrap()],
        live_tail: cursor(3, 3),
    };
    let interior_request = SubscribeV1 {
        stream_id: stream.stream_id(),
        start: PublicStartV1::Cursor(cursor(2, 2)),
    }
    .validate_discovery(discovery)
    .unwrap();
    let missing_membership = TestPublicContext {
        cursors: vec![cursor(1, 1), cursor(3, 3)],
        ranges: interior.available().to_vec(),
        current_replay: false,
    };
    assert!(
        interior
            .validate_for_request(interior_request, &missing_membership)
            .is_err()
    );
    let exact_membership = TestPublicContext {
        cursors: vec![cursor(1, 1), cursor(2, 2), cursor(3, 3)],
        ranges: interior.available().to_vec(),
        current_replay: false,
    };
    interior
        .validate_for_request(interior_request, &exact_membership)
        .unwrap();
}

#[test]
fn public_server_frame_enforces_selected_feed_kind() {
    let (start, _, _) = fixture_record();
    let raw_hello = public_exit::PublicServerFrameV1 {
        frame: Some(public_exit::public_server_frame_v1::Frame::Hello(
            public_hello_wire(),
        )),
    };
    assert!(matches!(
        decode_public_server_frame(
            &raw_hello.encode_to_vec(),
            PublicFeedKindV1::ShredBlockObservation,
            start,
        ),
        Err(WireError::InvalidValue { .. })
    ));

    let unsupported = StreamHeaderV1::new(
        StreamId::new([1; 16]),
        ClusterGenesisHash::new([2; 32]),
        5,
        1,
        ProducerConfigSha256::new([3; 32]),
        StreamManifestSha256::new([4; 32]),
    )
    .unwrap();
    let hello = public_exit::PublicHelloWireV1 {
        protocol_version: 1,
        stream: unsupported.fixed_encode().to_vec(),
        available: Vec::new(),
        live_tail: unsupported.initial_cursor().fixed_encode().to_vec(),
    };
    assert!(validate_public_hello(hello).is_err());
}

#[test]
fn public_checks_actual_serialized_size_before_protobuf_semantics() {
    assert!(matches!(
        decode_list_streams_request(&vec![0; PUBLIC_LIST_STREAMS_REQUEST_MAX_BYTES + 1]),
        Err(WireError::MessageTooLarge { .. })
    ));
    assert!(matches!(
        decode_subscribe_request(&vec![0; PUBLIC_SUBSCRIBE_REQUEST_MAX_BYTES + 1]),
        Err(WireError::MessageTooLarge { .. })
    ));

    let subscribe = subscribe_latest_wire();
    assert!(matches!(
        validate_subscribe_request(subscribe.clone(), subscribe.encoded_len() - 1),
        Err(WireError::InvalidValue { .. })
    ));
    assert!(matches!(
        validate_subscribe_request(subscribe, PUBLIC_SUBSCRIBE_REQUEST_MAX_BYTES + 1),
        Err(WireError::MessageTooLarge { .. })
    ));

    let discovery = empty_discovery_wire();
    assert!(matches!(
        validate_public_stream_list(discovery.clone(), discovery.encoded_len() - 1),
        Err(WireError::InvalidValue { .. })
    ));
    assert!(matches!(
        validate_public_stream_list(discovery, PUBLIC_DISCOVERY_RESPONSE_MAX_BYTES + 1),
        Err(WireError::MessageTooLarge { .. })
    ));

    let (start, _, _) = fixture_record();
    let control = public_exit::PublicServerFrameV1 {
        frame: Some(public_exit::public_server_frame_v1::Frame::Error(
            public_exit::PublicErrorWireV1 { code: 1 },
        )),
    };
    assert!(matches!(
        validate_public_server_frame(
            control,
            PUBLIC_CONTROL_FRAME_MAX_BYTES + 1,
            PublicFeedKindV1::RawShred,
            start,
        ),
        Err(WireError::MessageTooLarge { .. })
    ));
    let event = public_exit::PublicServerFrameV1 {
        frame: Some(public_exit::public_server_frame_v1::Frame::Event(
            public_event_wire(),
        )),
    };
    assert!(matches!(
        validate_public_server_frame(
            event,
            PUBLIC_SERVER_EVENT_FRAME_MAX_BYTES + 1,
            PublicFeedKindV1::RawShred,
            start,
        ),
        Err(WireError::MessageTooLarge { .. })
    ));

    let mut encoded_control = public_exit::PublicServerFrameV1 {
        frame: Some(public_exit::public_server_frame_v1::Frame::Error(
            public_exit::PublicErrorWireV1 { code: 1 },
        )),
    }
    .encode_to_vec();
    append_unknown_bytes(&mut encoded_control, PUBLIC_CONTROL_FRAME_MAX_BYTES);
    assert!(matches!(
        decode_public_server_frame(&encoded_control, PublicFeedKindV1::RawShred, start,),
        Err(WireError::MessageTooLarge { .. })
    ));
}

#[test]
fn discovery_and_replay_enforce_nested_lengths_and_range_limits() {
    let mut discovery = empty_discovery_wire();
    discovery.registry_head.pop();
    assert!(validate_public_stream_list(discovery.clone(), discovery.encoded_len()).is_err());
    let mut discovery = empty_discovery_wire();
    discovery.registry.clear();
    assert!(validate_public_stream_list(discovery.clone(), discovery.encoded_len()).is_err());

    let mut replay = replay_wire(3, 1, false);
    replay.available =
        vec![range_wire(cursor(1, 1), cursor(2, 2)); PUBLIC_AVAILABLE_RANGES_MAX + 1];
    assert!(matches!(
        validate_replay_unavailable(replay, PublicFeedKindV1::RawShred),
        Err(WireError::TooManyItems { .. })
    ));
    let mut replay = replay_wire(3, 1, false);
    replay.available = vec![
        range_wire(cursor(2, 2), cursor(3, 3)),
        range_wire(cursor(1, 1), cursor(2, 2)),
    ];
    assert!(matches!(
        validate_replay_unavailable(replay, PublicFeedKindV1::RawShred),
        Err(WireError::NonCanonicalOrder { .. })
    ));
}

#[test]
fn protobuf_preflight_bounds_repeated_occurrences_before_prost_decode() {
    let mut too_many_manifests = Vec::with_capacity((MAX_REGISTRY_ENTRIES_V1 + 1) * 2);
    for _ in 0..=MAX_REGISTRY_ENTRIES_V1 {
        too_many_manifests.extend_from_slice(&[0x1a, 0x00]);
    }
    assert!(matches!(
        decode_public_stream_list(&too_many_manifests),
        Err(WireError::TooManyItems {
            field: "PublicStreamListWireV1.public_manifests",
            actual,
            max: MAX_REGISTRY_ENTRIES_V1,
        }) if actual == MAX_REGISTRY_ENTRIES_V1 + 1
    ));

    let nested_hello = [0x1a, 0x00].repeat(PUBLIC_AVAILABLE_RANGES_MAX + 1);
    let mut hello_frame = vec![0x0a];
    push_varint(&mut hello_frame, nested_hello.len() as u64);
    hello_frame.extend_from_slice(&nested_hello);
    assert!(matches!(
        decode_public_server_frame(
            &hello_frame,
            PublicFeedKindV1::RawShred,
            fixture_stream().initial_cursor(),
        ),
        Err(WireError::TooManyItems {
            field: "available",
            actual,
            max: PUBLIC_AVAILABLE_RANGES_MAX,
        }) if actual == PUBLIC_AVAILABLE_RANGES_MAX + 1
    ));

    let nested_replay = [0x1a, 0x00].repeat(PUBLIC_AVAILABLE_RANGES_MAX + 1);
    let mut replay_frame = vec![0x1a];
    push_varint(&mut replay_frame, nested_replay.len() as u64);
    replay_frame.extend_from_slice(&nested_replay);
    assert!(matches!(
        decode_public_server_frame(
            &replay_frame,
            PublicFeedKindV1::RawShred,
            fixture_stream().initial_cursor(),
        ),
        Err(WireError::TooManyItems { .. })
    ));

    let first_half = [0x1a, 0x00].repeat(600);
    let mut duplicate_hellos = Vec::new();
    for nested in [&first_half, &first_half] {
        duplicate_hellos.push(0x0a);
        push_varint(&mut duplicate_hellos, nested.len() as u64);
        duplicate_hellos.extend_from_slice(nested);
    }
    assert!(matches!(
        decode_public_server_frame(
            &duplicate_hellos,
            PublicFeedKindV1::RawShred,
            fixture_stream().initial_cursor(),
        ),
        Err(WireError::TooManyItems { actual: 1_200, .. })
    ));

    let hello_ranges = [0x1a, 0x00].repeat(600);
    let replay_ranges = [0x1a, 0x00].repeat(600);
    let mut replaced_ranges = wrap_message_field(1, &hello_ranges);
    replaced_ranges.extend(wrap_message_field(
        4,
        &public_exit::PublicErrorWireV1 { code: 1 }.encode_to_vec(),
    ));
    replaced_ranges.extend(wrap_message_field(3, &replay_ranges));
    assert!(preflight_public_server_frame(&replaced_ranges).is_ok());

    assert!(matches!(
        decode_public_stream_list(&[0x18, 0x01]),
        Err(WireError::MalformedProtobuf { .. })
    ));
    assert!(matches!(
        decode_public_stream_list(&[0x1a, 0x80]),
        Err(WireError::MalformedProtobuf { .. })
    ));
    let mut excessive_groups = vec![0x0b; 101];
    excessive_groups.extend([0x0c; 101]);
    assert!(matches!(
        decode_public_stream_list(&excessive_groups),
        Err(WireError::MalformedProtobuf { .. })
    ));
}

#[test]
fn sync_server_preflight_caps_replaced_and_merged_control_before_decode() {
    let start = fixture_stream().initial_cursor();
    let large_record = RecordV1::new(start, vec![0x51; 70_000]).unwrap();
    let large_record_frame = sync::SyncServerFrameV1 {
        frame: Some(sync::sync_server_frame_v1::Frame::Record(
            sync::RecordWireV1 {
                record: large_record.encode(),
            },
        )),
    }
    .encode_to_vec();
    let small_record_frame = sync::SyncServerFrameV1 {
        frame: Some(sync::sync_server_frame_v1::Frame::Record(sync_record_wire())),
    }
    .encode_to_vec();
    let error_frame = sync::SyncServerFrameV1 {
        frame: Some(sync::sync_server_frame_v1::Frame::Error(sync_error_wire(1))),
    }
    .encode_to_vec();

    let mut record_then_error = large_record_frame.clone();
    record_then_error.extend_from_slice(&error_frame);
    assert!(matches!(
        decode_sync_server_frame(&record_then_error, start),
        Err(WireError::MessageTooLarge { .. })
    ));

    let mut error_then_record = error_frame;
    error_then_record.extend_from_slice(&large_record_frame);
    assert!(matches!(
        decode_sync_server_frame(&error_then_record, start),
        Ok(ValidatedSyncServerFrameV1::Record(_))
    ));

    let mut oversized_resume =
        wrap_message_field(1, &nested_unknown_bytes(SYNC_CONTROL_PROTOBUF_MAX_BYTES));
    oversized_resume.extend_from_slice(&small_record_frame);
    assert!(matches!(
        decode_sync_server_frame(&oversized_resume, start),
        Err(WireError::MessageTooLarge { .. })
    ));

    let resume_part = wrap_message_field(1, &nested_unknown_bytes(33_000));
    let mut merged_resume = resume_part.clone();
    merged_resume.extend_from_slice(&resume_part);
    merged_resume.extend_from_slice(&small_record_frame);
    assert!(matches!(
        decode_sync_server_frame(&merged_resume, start),
        Err(WireError::MessageTooLarge { .. })
    ));

    let mut replaced_controls = wrap_message_field(1, &nested_unknown_bytes(40_000));
    replaced_controls.extend(wrap_message_field(3, &nested_unknown_bytes(40_000)));
    replaced_controls.extend_from_slice(&small_record_frame);
    assert!(matches!(
        decode_sync_server_frame(&replaced_controls, start),
        Ok(ValidatedSyncServerFrameV1::Record(_))
    ));
}

#[test]
fn public_server_preflight_caps_replaced_and_merged_control_before_decode() {
    let start = fixture_stream().initial_cursor();
    let large_record = RecordV1::new(start, vec![0x52; 1_100_000]).unwrap();
    let large_event_frame = public_exit::PublicServerFrameV1 {
        frame: Some(public_exit::public_server_frame_v1::Frame::Event(
            public_exit::PublicEventWireV1 {
                record: large_record.encode(),
            },
        )),
    }
    .encode_to_vec();
    let small_event_frame = public_exit::PublicServerFrameV1 {
        frame: Some(public_exit::public_server_frame_v1::Frame::Event(
            public_event_wire(),
        )),
    }
    .encode_to_vec();
    let error_frame = public_exit::PublicServerFrameV1 {
        frame: Some(public_exit::public_server_frame_v1::Frame::Error(
            public_exit::PublicErrorWireV1 { code: 1 },
        )),
    }
    .encode_to_vec();

    let mut event_then_error = large_event_frame.clone();
    event_then_error.extend_from_slice(&error_frame);
    assert!(matches!(
        decode_public_server_frame(&event_then_error, PublicFeedKindV1::RawShred, start),
        Err(WireError::MessageTooLarge { .. })
    ));

    let mut error_then_event = error_frame;
    error_then_event.extend_from_slice(&large_event_frame);
    assert!(matches!(
        decode_public_server_frame(&error_then_event, PublicFeedKindV1::RawShred, start),
        Ok(StructurallyValidatedPublicServerFrameV1::Event(_))
    ));

    let mut oversized_hello =
        wrap_message_field(1, &nested_unknown_bytes(PUBLIC_CONTROL_FRAME_MAX_BYTES));
    oversized_hello.extend_from_slice(&small_event_frame);
    assert!(matches!(
        decode_public_server_frame(&oversized_hello, PublicFeedKindV1::RawShred, start),
        Err(WireError::MessageTooLarge { .. })
    ));

    let hello_part = wrap_message_field(1, &nested_unknown_bytes(600_000));
    let mut merged_hello = hello_part.clone();
    merged_hello.extend_from_slice(&hello_part);
    merged_hello.extend_from_slice(&small_event_frame);
    assert!(matches!(
        decode_public_server_frame(&merged_hello, PublicFeedKindV1::RawShred, start),
        Err(WireError::MessageTooLarge { .. })
    ));

    let mut replaced_controls = wrap_message_field(1, &nested_unknown_bytes(600_000));
    replaced_controls.extend(wrap_message_field(4, &nested_unknown_bytes(600_000)));
    replaced_controls.extend_from_slice(&small_event_frame);
    assert!(matches!(
        decode_public_server_frame(&replaced_controls, PublicFeedKindV1::RawShred, start),
        Ok(StructurallyValidatedPublicServerFrameV1::Event(_))
    ));
}

#[test]
fn fetch_part_preflight_caps_replaced_and_merged_control_before_decode() {
    let large_chunk = sync::FetchRangePartWireV1 {
        part: Some(sync::fetch_range_part_wire_v1::Part::ChunkBytes(vec![
            0x53;
            70_000
        ])),
    }
    .encode_to_vec();
    let small_chunk = sync::FetchRangePartWireV1 {
        part: Some(sync::fetch_range_part_wire_v1::Part::ChunkBytes(
            b"chunk".to_vec(),
        )),
    }
    .encode_to_vec();
    let error = sync::FetchRangePartWireV1 {
        part: Some(sync::fetch_range_part_wire_v1::Part::Error(
            sync_error_wire(1),
        )),
    }
    .encode_to_vec();

    let mut chunk_then_error = large_chunk.clone();
    chunk_then_error.extend_from_slice(&error);
    assert!(matches!(
        decode_fetch_range_part(&chunk_then_error),
        Err(WireError::MessageTooLarge { .. })
    ));

    let mut error_then_chunk = error;
    error_then_chunk.extend_from_slice(&large_chunk);
    assert!(matches!(
        decode_fetch_range_part(&error_then_chunk),
        Ok(ValidatedFetchRangePartV1::ChunkBytes(bytes)) if bytes.len() == 70_000
    ));

    let mut oversized_commit =
        wrap_message_field(2, &nested_unknown_bytes(SYNC_CONTROL_PROTOBUF_MAX_BYTES));
    oversized_commit.extend_from_slice(&small_chunk);
    assert!(matches!(
        decode_fetch_range_part(&oversized_commit),
        Err(WireError::MessageTooLarge { .. })
    ));

    let commit_part = wrap_message_field(2, &nested_unknown_bytes(33_000));
    let mut merged_commit = commit_part.clone();
    merged_commit.extend_from_slice(&commit_part);
    merged_commit.extend_from_slice(&small_chunk);
    assert!(matches!(
        decode_fetch_range_part(&merged_commit),
        Err(WireError::MessageTooLarge { .. })
    ));

    let mut replaced_controls = wrap_message_field(2, &nested_unknown_bytes(40_000));
    replaced_controls.extend(wrap_message_field(3, &nested_unknown_bytes(40_000)));
    replaced_controls.extend_from_slice(&small_chunk);
    assert!(matches!(
        decode_fetch_range_part(&replaced_controls),
        Ok(ValidatedFetchRangePartV1::ChunkBytes(bytes)) if bytes == b"chunk"
    ));
}

#[test]
fn duplicate_oneofs_follow_proto3_last_one_wins() {
    let open = sync::SyncClientFrameV1 {
        frame: Some(sync::sync_client_frame_v1::Frame::Open(open_wire(None))),
    };
    let ack = sync::SyncClientFrameV1 {
        frame: Some(sync::sync_client_frame_v1::Frame::Ack(ack_wire())),
    };
    let mut duplicate_sync = open.encode_to_vec();
    duplicate_sync.extend_from_slice(&ack.encode_to_vec());
    assert!(matches!(
        decode_sync_client_frame(&duplicate_sync),
        Ok(ValidatedSyncClientFrameV1::Ack(_))
    ));

    let latest = subscribe_latest_wire();
    let cursor = subscribe_cursor_wire();
    let mut duplicate_start = latest.encode_to_vec();
    duplicate_start.extend_from_slice(&cursor.encode_to_vec());
    assert!(matches!(
        decode_subscribe_request(&duplicate_start).unwrap().start(),
        PublicStartV1::Cursor(_)
    ));

    let hello = public_exit::PublicServerFrameV1 {
        frame: Some(public_exit::public_server_frame_v1::Frame::Hello(
            public_hello_wire(),
        )),
    };
    let error = public_exit::PublicServerFrameV1 {
        frame: Some(public_exit::public_server_frame_v1::Frame::Error(
            public_exit::PublicErrorWireV1 { code: 1 },
        )),
    };
    let mut duplicate_server = hello.encode_to_vec();
    duplicate_server.extend_from_slice(&error.encode_to_vec());
    assert!(matches!(
        decode_public_server_frame(
            &duplicate_server,
            PublicFeedKindV1::RawShred,
            fixture_stream().initial_cursor(),
        ),
        Ok(StructurallyValidatedPublicServerFrameV1::Error(_))
    ));
}
