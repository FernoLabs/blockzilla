use std::process::Command;

use hivezilla::overflow::{OverflowCatalog, OverflowLimits};
use hivezilla_object_store::MemoryObjectStore;
use hivezilla_protocol::{
    ClusterGenesisHash, FrameV1, OverflowNamespaceSha256, ProducerConfigSha256, SealedSegmentV1,
    SegmentHeaderV1, StreamHeaderV1, StreamId, StreamManifestSha256,
};

#[test]
fn overflow_inspection_reports_pending_and_verified_ranges_as_json() {
    let state = tempfile::tempdir().unwrap();
    let mut catalog = OverflowCatalog::open(
        state.path(),
        OverflowNamespaceSha256::new([9; 32]),
        OverflowLimits::default(),
    )
    .unwrap();
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
    let store = MemoryObjectStore::default();
    for verified in [true, false] {
        let frame = FrameV1::new(cursor, b"input".to_vec()).unwrap();
        let segment =
            SealedSegmentV1::new(SegmentHeaderV1::new(stream, cursor).unwrap(), vec![frame])
                .unwrap();
        if verified {
            catalog.upload("hetzner", &segment, &store).unwrap();
        } else {
            catalog.begin_upload("hetzner", &segment).unwrap();
        }
        cursor = segment.end();
    }
    let output = Command::new(env!("CARGO_BIN_EXE_hivezilla"))
        .env("RUST_LOG", "debug")
        .args(["inspect-overflow-index", "--state-dir"])
        .arg(state.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(status.as_array().unwrap().len(), 1);
    assert_eq!(status[0]["reader_id"], "hetzner");
    assert_eq!(status[0]["pending_ranges"], 1);
    assert_eq!(status[0]["verified_ranges"], 1);
    assert_eq!(status[0]["contiguous_verified_next_sequence"], 1);
    assert_eq!(
        catalog
            .contiguous_verified_end("hetzner", stream.stream_id())
            .unwrap()
            .next_sequence(),
        1
    );
}

#[test]
fn overflow_inspection_does_not_initialize_missing_state() {
    let root = tempfile::tempdir().unwrap();
    let state = root.path().join("absent");
    let output = Command::new(env!("CARGO_BIN_EXE_hivezilla"))
        .args(["inspect-overflow-index", "--state-dir"])
        .arg(&state)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(!state.exists());
}
