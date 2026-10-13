use super::*;
use hivezilla_object_store::{
    CreateResult, DeleteDisposition, FilesystemObjectStore, MemoryObjectStore, ObjectMetadata,
    ObjectRead, ObjectVersion,
};
use hivezilla_protocol::{
    ClusterGenesisHash, FrameV1, PrefixHash, ProducerConfigSha256, SegmentHeaderV1,
    StreamManifestSha256,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

fn stream(id: u8) -> StreamHeaderV1 {
    StreamHeaderV1::new(
        StreamId::new([id; 16]),
        ClusterGenesisHash::new([1; 32]),
        2,
        1,
        ProducerConfigSha256::new([2; 32]),
        StreamManifestSha256::new([3; 32]),
    )
    .unwrap()
}

fn segment(stream: StreamHeaderV1, start: CursorV1, payloads: &[&[u8]]) -> SealedSegmentV1 {
    let mut cursor = start;
    let frames = payloads
        .iter()
        .map(|payload| {
            let frame = FrameV1::new(cursor, payload.to_vec()).unwrap();
            cursor = frame.validate_after(cursor).unwrap();
            frame
        })
        .collect();
    SealedSegmentV1::new(SegmentHeaderV1::new(stream, start).unwrap(), frames).unwrap()
}

fn first(stream: StreamHeaderV1) -> SealedSegmentV1 {
    segment(stream, stream.initial_cursor(), &[b"first", b"second"])
}

fn open(path: &Path) -> OverflowCatalog {
    OverflowCatalog::open(
        path,
        OverflowNamespaceSha256::new([9; 32]),
        OverflowLimits::default(),
    )
    .unwrap()
}

#[test]
fn uploaded_segment_survives_reopen_and_is_read_by_sequence() {
    let state = tempfile::tempdir().unwrap();
    let remote = tempfile::tempdir().unwrap();
    let store = FilesystemObjectStore::open_root(remote.path()).unwrap();
    let source = first(stream(1));
    let mut catalog = open(state.path());
    let object = catalog.upload("hetzner", &source, &store).unwrap();
    assert_eq!(object.start(), stream(1).initial_cursor());
    drop(catalog);
    let catalog = open(state.path());
    assert_eq!(
        catalog
            .read_segment("hetzner", stream(1).stream_id(), 1, &store)
            .unwrap(),
        source
    );
    assert!(
        catalog
            .containing("hetzner", stream(1).stream_id(), 2)
            .unwrap()
            .is_none()
    );
    let report = OverflowCatalog::inspect(state.path()).unwrap();
    assert_eq!(report[0].pending_ranges, 0);
    assert_eq!(report[0].verified_ranges, 1);
    assert_eq!(report[0].contiguous_verified_next_sequence, 2);
}

struct FaultStore {
    inner: MemoryObjectStore,
    fail_after_create: AtomicBool,
    fail_verification: AtomicBool,
    corrupt_read: AtomicBool,
    reads: AtomicUsize,
}

impl FaultStore {
    fn new() -> Self {
        Self {
            inner: MemoryObjectStore::new(false),
            fail_after_create: AtomicBool::new(false),
            fail_verification: AtomicBool::new(false),
            corrupt_read: AtomicBool::new(false),
            reads: AtomicUsize::new(0),
        }
    }
}

impl ImmutableObjectStore for FaultStore {
    fn create_if_absent(
        &self,
        request: &CreateRequest,
        source: &mut dyn Read,
    ) -> hivezilla_object_store::Result<CreateResult> {
        let result = self.inner.create_if_absent(request, source)?;
        if self.fail_after_create.swap(false, Ordering::SeqCst) {
            return Err(hivezilla_object_store::Error::Io(std::io::Error::other(
                "lost upload response",
            )));
        }
        Ok(result)
    }
    fn open(&self, key: &ObjectKey) -> hivezilla_object_store::Result<ObjectRead> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let mut object = self.inner.open(key)?;
        if self.corrupt_read.load(Ordering::SeqCst) {
            let mut bytes = Vec::new();
            object.body.read_to_end(&mut bytes)?;
            bytes[0] ^= 1;
            object.body = Box::new(std::io::Cursor::new(bytes));
        }
        Ok(object)
    }
    fn stat(&self, key: &ObjectKey) -> hivezilla_object_store::Result<Option<ObjectMetadata>> {
        if self.fail_verification.load(Ordering::SeqCst) {
            return Err(hivezilla_object_store::Error::Io(std::io::Error::other(
                "verification unavailable",
            )));
        }
        self.inner.stat(key)
    }
    fn list(&self, prefix: &str) -> hivezilla_object_store::Result<Vec<ObjectMetadata>> {
        self.inner.list(prefix)
    }
    fn delete(
        &self,
        key: &ObjectKey,
        version: Option<&ObjectVersion>,
    ) -> hivezilla_object_store::Result<DeleteDisposition> {
        self.inner.delete(key, version)
    }
}

#[test]
fn lost_upload_response_is_recovered_without_duplicate_objects() {
    let state = tempfile::tempdir().unwrap();
    let store = FaultStore::new();
    store.fail_after_create.store(true, Ordering::SeqCst);
    let source = first(stream(1));
    assert!(
        open(state.path())
            .upload("reader", &source, &store)
            .is_err()
    );
    let mut catalog = open(state.path());
    assert!(
        !catalog
            .containing("reader", stream(1).stream_id(), 0)
            .unwrap()
            .unwrap()
            .verified
    );
    assert!(
        catalog
            .read_segment("reader", stream(1).stream_id(), 0, &store)
            .is_err()
    );
    catalog.upload("reader", &source, &store).unwrap();
    assert_eq!(store.inner.list("").unwrap().len(), 1);
    assert!(
        store.reads.load(Ordering::SeqCst) > 0,
        "a backend without a digest requires readback"
    );
    assert_eq!(
        catalog
            .read_segment("reader", stream(1).stream_id(), 0, &store)
            .unwrap(),
        source
    );
}

#[test]
fn failed_verification_never_promotes_an_intent() {
    let state = tempfile::tempdir().unwrap();
    let store = FaultStore::new();
    store.fail_verification.store(true, Ordering::SeqCst);
    let mut catalog = open(state.path());
    let source = first(stream(1));
    assert!(catalog.upload("reader", &source, &store).is_err());
    let pending = catalog
        .containing("reader", stream(1).stream_id(), 0)
        .unwrap()
        .unwrap();
    assert!(!pending.verified);
    assert_eq!(
        catalog
            .contiguous_verified_end("reader", stream(1).stream_id())
            .unwrap(),
        stream(1).initial_cursor()
    );
    store.fail_verification.store(false, Ordering::SeqCst);
    catalog
        .verify_upload("reader", &pending.object, &store)
        .unwrap();
    assert!(
        catalog
            .containing("reader", stream(1).stream_id(), 0)
            .unwrap()
            .unwrap()
            .verified
    );
}

#[test]
fn out_of_order_recovery_keeps_the_gap_until_verified() {
    let state = tempfile::tempdir().unwrap();
    let store = MemoryObjectStore::default();
    let mut catalog = open(state.path());
    let first = first(stream(1));
    let second = segment(stream(1), first.end(), &[b"third", b"fourth"]);
    catalog.upload("reader", &second, &store).unwrap();
    catalog.begin_upload("reader", &first).unwrap();
    assert_eq!(
        catalog
            .contiguous_verified_end("reader", stream(1).stream_id())
            .unwrap(),
        stream(1).initial_cursor()
    );
    catalog.upload("reader", &first, &store).unwrap();
    assert_eq!(
        catalog
            .contiguous_verified_end("reader", stream(1).stream_id())
            .unwrap(),
        second.end()
    );
}

#[test]
fn overlap_and_conflicting_prefixes_do_not_mutate_the_index() {
    let state = tempfile::tempdir().unwrap();
    let mut catalog = open(state.path());
    let source = first(stream(1));
    catalog.begin_upload("reader", &source).unwrap();
    let interior = source.start().advance(b"first").unwrap();
    let overlap = segment(stream(1), interior, &[b"other", b"third"]);
    assert!(catalog.begin_upload("reader", &overlap).is_err());
    let conflicting = segment(stream(1), source.start(), &[b"different", b"second"]);
    assert!(catalog.begin_upload("reader", &conflicting).is_err());
    let wrong_join = segment(
        stream(1),
        CursorV1::new(2, PrefixHash::new([8; 32])),
        &[b"third"],
    );
    assert!(catalog.begin_upload("reader", &wrong_join).is_err());
    assert_eq!(
        OverflowCatalog::inspect(state.path()).unwrap()[0].pending_ranges,
        1
    );

    let other_state = tempfile::tempdir().unwrap();
    let mut other = open(other_state.path());
    other.begin_upload("reader", &wrong_join).unwrap();
    assert!(
        other.begin_upload("reader", &source).is_err(),
        "validate the successor join too"
    );
    assert_eq!(
        OverflowCatalog::inspect(other_state.path()).unwrap()[0].pending_ranges,
        1
    );
}

#[test]
fn recovery_pages_include_pending_ranges_without_hiding_gaps() {
    let state = tempfile::tempdir().unwrap();
    let store = MemoryObjectStore::default();
    let first = first(stream(1));
    let missing = segment(stream(1), first.end(), &[b"missing"]);
    let last = segment(stream(1), missing.end(), &[b"last"]);
    let mut catalog = open(state.path());
    catalog.upload("reader", &last, &store).unwrap();
    catalog.begin_upload("reader", &first).unwrap();
    drop(catalog);
    let catalog = open(state.path());
    let page = catalog
        .ranges_from("reader", stream(1).stream_id(), 1, 1)
        .unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].object.start(), first.start());
    assert!(!page[0].verified);
    let page = catalog
        .ranges_from(
            "reader",
            stream(1).stream_id(),
            page[0].object.end().next_sequence(),
            1,
        )
        .unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].object.start(), last.start());
    assert!(page[0].verified);
    assert!(
        catalog
            .ranges_from(
                "reader",
                stream(1).stream_id(),
                last.end().next_sequence(),
                1
            )
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        catalog
            .contiguous_verified_end("reader", stream(1).stream_id())
            .unwrap(),
        first.start()
    );
    assert!(
        catalog
            .ranges_from("other-reader", stream(1).stream_id(), 0, 1)
            .is_err()
    );
    for limit in [0, 1025] {
        assert!(
            catalog
                .ranges_from("reader", stream(1).stream_id(), 0, limit)
                .is_err()
        );
    }
}

#[test]
fn readers_and_input_streams_have_independent_progress() {
    let state = tempfile::tempdir().unwrap();
    let store = MemoryObjectStore::default();
    let mut catalog = open(state.path());
    catalog
        .upload("reader-a", &first(stream(1)), &store)
        .unwrap();
    catalog.begin_upload("reader-b", &first(stream(2))).unwrap();
    catalog.begin_upload("reader-a", &first(stream(3))).unwrap();
    assert_eq!(
        catalog
            .contiguous_verified_end("reader-b", stream(2).stream_id())
            .unwrap(),
        stream(2).initial_cursor()
    );
    assert_eq!(
        catalog
            .contiguous_verified_end("reader-a", stream(3).stream_id())
            .unwrap(),
        stream(3).initial_cursor()
    );
    assert!(
        catalog
            .containing("reader-b", stream(1).stream_id(), 0)
            .is_err()
    );
    assert!(catalog.begin_upload("reader-b", &first(stream(1))).is_err());
    let altered = StreamHeaderV1::new(
        stream(1).stream_id(),
        ClusterGenesisHash::new([99; 32]),
        2,
        1,
        stream(1).producer_config_sha256(),
        stream(1).stream_manifest_sha256(),
    )
    .unwrap();
    assert!(catalog.begin_upload("reader-a", &first(altered)).is_err());
}

#[test]
fn full_unsigned_sequence_space_is_preserved() {
    let state = tempfile::tempdir().unwrap();
    let store = MemoryObjectStore::default();
    let mut catalog = open(state.path());
    let source = segment(
        stream(1),
        CursorV1::new(u64::MAX - 2, PrefixHash::new([5; 32])),
        &[b"penultimate", b"last"],
    );
    catalog.upload("reader", &source, &store).unwrap();
    assert_eq!(
        catalog
            .read_segment("reader", stream(1).stream_id(), u64::MAX - 1, &store)
            .unwrap(),
        source
    );
    assert!(
        catalog
            .containing("reader", stream(1).stream_id(), i64::MAX as u64)
            .unwrap()
            .is_none()
    );
    assert!(
        catalog
            .containing("reader", stream(1).stream_id(), u64::MAX)
            .unwrap()
            .is_none()
    );
}

#[test]
fn corruption_and_changed_object_versions_fail_closed() {
    let state = tempfile::tempdir().unwrap();
    let store = FaultStore::new();
    let mut catalog = open(state.path());
    let source = first(stream(1));
    let object = catalog.upload("reader", &source, &store).unwrap();
    store.corrupt_read.store(true, Ordering::SeqCst);
    assert!(
        catalog
            .read_segment("reader", stream(1).stream_id(), 0, &store)
            .is_err()
    );
    store.corrupt_read.store(false, Ordering::SeqCst);
    store
        .inner
        .delete(&ObjectKey::new(object.object_key()).unwrap(), None)
        .unwrap();
    store
        .inner
        .create_if_absent(
            &object_request(&object).unwrap(),
            &mut std::io::Cursor::new(source.encode()),
        )
        .unwrap();
    assert!(
        catalog
            .read_segment("reader", stream(1).stream_id(), 0, &store)
            .is_err()
    );
    assert!(catalog.verify_upload("reader", &object, &store).is_err());
}

#[test]
fn immutable_remote_collision_leaves_an_unverified_intent() {
    let state = tempfile::tempdir().unwrap();
    let store = MemoryObjectStore::default();
    let mut catalog = open(state.path());
    let source = first(stream(1));
    let intent = catalog.begin_upload("reader", &source).unwrap();
    let bytes = b"wrong bytes";
    let request = CreateRequest {
        key: ObjectKey::new(intent.object.object_key()).unwrap(),
        encoded_len: bytes.len() as u64,
        encoded_sha256: digest(bytes),
    };
    store
        .create_if_absent(&request, &mut bytes.as_slice())
        .unwrap();
    assert!(catalog.upload("reader", &source, &store).is_err());
    assert!(
        !catalog
            .containing("reader", stream(1).stream_id(), 0)
            .unwrap()
            .unwrap()
            .verified
    );
}

#[test]
fn metadata_corruption_is_reported_before_range_use() {
    let state = tempfile::tempdir().unwrap();
    let mut catalog = open(state.path());
    catalog.begin_upload("reader", &first(stream(1))).unwrap();
    catalog
        .connection
        .execute("UPDATE ranges SET object_digest=zeroblob(32)", [])
        .unwrap();
    assert!(
        catalog
            .containing("reader", stream(1).stream_id(), 0)
            .is_err()
    );
    assert!(OverflowCatalog::inspect(state.path()).is_err());
}

#[test]
fn limits_allow_exact_retries_but_refuse_new_ranges_and_streams() {
    let state = tempfile::tempdir().unwrap();
    let limits = OverflowLimits {
        max_ranges_per_stream: 1,
        max_streams: 1,
        ..OverflowLimits::default()
    };
    let mut catalog =
        OverflowCatalog::open(state.path(), OverflowNamespaceSha256::new([9; 32]), limits).unwrap();
    let source = first(stream(1));
    catalog.begin_upload("reader", &source).unwrap();
    catalog.begin_upload("reader", &source).unwrap();
    assert!(
        catalog
            .begin_upload("reader", &segment(stream(1), source.end(), &[b"more"]))
            .is_err()
    );
    assert!(catalog.begin_upload("reader-2", &first(stream(2))).is_err());
    assert_eq!(OverflowCatalog::inspect(state.path()).unwrap().len(), 1);
    catalog.limits.max_segment_bytes = 1;
    assert!(catalog.begin_upload("reader", &source).is_err());
}

#[test]
fn namespaces_are_immutable_and_absent_inspection_creates_nothing() {
    let state = tempfile::tempdir().unwrap();
    assert!(OverflowCatalog::inspect(state.path()).is_err());
    assert_eq!(fs::read_dir(state.path()).unwrap().count(), 0);
    drop(open(state.path()));
    assert!(
        OverflowCatalog::open(
            state.path(),
            OverflowNamespaceSha256::new([8; 32]),
            OverflowLimits::default()
        )
        .is_err()
    );
    drop(open(state.path()));
}

#[test]
fn concurrent_writers_cannot_insert_conflicting_ranges() {
    let state = tempfile::tempdir().unwrap();
    let mut first_writer = open(state.path());
    let mut second_writer = open(state.path());
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let other = barrier.clone();
    let worker = std::thread::spawn(move || {
        other.wait();
        first_writer.begin_upload("reader", &first(stream(1)))
    });
    barrier.wait();
    let conflicting = segment(stream(1), stream(1).initial_cursor(), &[b"different"]);
    let result = second_writer.begin_upload("reader", &conflicting);
    assert_ne!(worker.join().unwrap().is_ok(), result.is_ok());
    assert_eq!(
        OverflowCatalog::inspect(state.path()).unwrap()[0].pending_ranges,
        1
    );
}

#[cfg(unix)]
#[test]
fn symlinked_database_and_state_directory_are_rejected() {
    use std::os::unix::fs::symlink;
    let state = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let alias = state.path().join("alias");
    symlink(target.path(), &alias).unwrap();
    assert!(
        OverflowCatalog::open(
            &alias,
            OverflowNamespaceSha256::new([9; 32]),
            OverflowLimits::default()
        )
        .is_err()
    );
    fs::write(target.path().join("unrelated"), b"unrelated").unwrap();
    symlink(
        target.path().join("unrelated"),
        state.path().join(DATABASE_FILE),
    )
    .unwrap();
    assert!(
        OverflowCatalog::open(
            state.path(),
            OverflowNamespaceSha256::new([9; 32]),
            OverflowLimits::default()
        )
        .is_err()
    );
    assert_eq!(
        fs::read(target.path().join("unrelated")).unwrap(),
        b"unrelated"
    );
}

// A separate process exits without Drop after real disk writes. The parent checks WAL recovery.
#[test]
fn crash_probe() {
    let Some(root) = std::env::var_os("HIVEZILLA_OVERFLOW_TEST_CRASH_ROOT") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let store = FilesystemObjectStore::open_root(root.join("remote")).unwrap();
    let mut catalog = open(&root.join("state"));
    let source = first(stream(1));
    let intent = catalog.begin_upload("reader", &source).unwrap();
    store
        .create_if_absent(
            &object_request(&intent.object).unwrap(),
            &mut std::io::Cursor::new(source.encode()),
        )
        .unwrap();
    if std::env::var_os("HIVEZILLA_OVERFLOW_TEST_VERIFIED").is_some() {
        catalog
            .verify_upload("reader", &intent.object, &store)
            .unwrap();
        catalog
            .connection
            .execute_batch("BEGIN IMMEDIATE; UPDATE ranges SET verified=0")
            .unwrap();
    }
    std::process::exit(71);
}

#[test]
fn process_exit_recovers_pending_upload_and_rolls_back_uncommitted_changes() {
    for verified in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args(["overflow::tests::crash_probe", "--exact"])
            .env("HIVEZILLA_OVERFLOW_TEST_CRASH_ROOT", root.path())
            .env_remove("HIVEZILLA_OVERFLOW_TEST_VERIFIED");
        if verified {
            command.env("HIVEZILLA_OVERFLOW_TEST_VERIFIED", "1");
        }
        assert_eq!(command.status().unwrap().code(), Some(71));
        let mut catalog = open(&root.path().join("state"));
        let range = catalog
            .containing("reader", stream(1).stream_id(), 0)
            .unwrap()
            .unwrap();
        assert_eq!(range.verified, verified);
        let store = FilesystemObjectStore::open_root(root.path().join("remote")).unwrap();
        catalog
            .verify_upload("reader", &range.object, &store)
            .unwrap();
        assert_eq!(
            catalog
                .read_segment("reader", stream(1).stream_id(), 0, &store)
                .unwrap(),
            first(stream(1))
        );
    }
}
