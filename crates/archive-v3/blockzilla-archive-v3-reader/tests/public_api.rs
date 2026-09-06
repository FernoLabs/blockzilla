use std::{
    num::{NonZeroU32, NonZeroUsize},
    path::Path,
};

use blockzilla_archive_v3_reader::{
    ArchiveInstructionSource, ArchiveIoSnapshot, BlockView, DEFAULT_INDEXER_V3_FULL_REGISTRY_BYTES,
    IndexerV3Archive, IndexerV3CandidatePolicy, IndexerV3OpenOptions,
    IndexerV3ParallelInstructionSource, IndexerV3RegistryReadMode, IndexerV3RegistryReadPolicy,
    IndexerV3RegistryReadReceipt, IndexerV3TargetedScanReceipt, IndexerV3TransportKind,
    IndexerV3TransportReceipt, QueryResult, ScanRange, ScanRequest, SourceVerification,
};

fn ignore_block(_: BlockView<'_>) -> QueryResult<()> {
    Ok(())
}

#[test]
fn common_query_types_are_available_from_one_crate() {
    let range = ScanRange {
        first_block: 0,
        block_count: NonZeroU32::new(1).unwrap(),
    };
    let request = ScanRequest::bounded(range);
    let snapshot = ArchiveIoSnapshot::default();
    assert_eq!(request.range, Some(range));
    assert_eq!(snapshot.network_body_bytes, 0);
    let verification = SourceVerification::ObjectSetBound;
    assert!(matches!(verification, SourceVerification::ObjectSetBound));
    assert_eq!(
        SourceVerification::OperatorTrusted.to_string(),
        "operator-trusted"
    );
}

#[test]
fn short_network_entry_points_have_stable_signatures() {
    fn accepts_source<T: ArchiveInstructionSource>() {}
    accepts_source::<IndexerV3Archive>();

    fn parallel_source(
        archive: &mut IndexerV3Archive,
        workers: NonZeroUsize,
    ) -> QueryResult<IndexerV3ParallelInstructionSource<'_>> {
        archive.parallel_instruction_source(workers)
    }
    let _ = parallel_source;

    let _open = |origin: &str, epoch: u64, root: &Path| IndexerV3Archive::open(origin, epoch, root);
    let _open_selective = |origin: &str, epoch: u64, root: &Path| {
        IndexerV3Archive::open_selective(origin, epoch, root)
    };
    let _open_local = |root: &Path, epoch: u64| IndexerV3Archive::open_local(root, epoch);
    let _options = IndexerV3OpenOptions::default();
    let default_registry_policy = IndexerV3RegistryReadPolicy::with_full_registry_limit(
        DEFAULT_INDEXER_V3_FULL_REGISTRY_BYTES,
    );
    assert_eq!(
        default_registry_policy.max_full_registry_bytes(),
        1_073_741_824
    );
    let registry_receipt = IndexerV3RegistryReadReceipt {
        mode: IndexerV3RegistryReadMode::SparseChunkCache,
        prefetch_read_calls: 0,
        prefetch_read_bytes: 0,
        resolutions: 1,
        hits: 0,
        misses: 1,
        evictions: 0,
        resident_payload_bytes: 65_536,
    };
    assert_eq!(
        registry_receipt.mode,
        IndexerV3RegistryReadMode::SparseChunkCache
    );
    let _open_local_split = |ledger: &Path, retained: &Path, epoch: u64, candidate: &str| {
        IndexerV3Archive::open_local_split(ledger, retained, epoch, candidate)
    };
    let local_transport = IndexerV3TransportReceipt {
        kind: IndexerV3TransportKind::LocalSplit,
        http_and_cache: ArchiveIoSnapshot::default(),
        local_read_calls: 1,
        local_read_bytes: 2,
    };
    assert_eq!(local_transport.http_and_cache.network_body_bytes, 0);
}

#[test]
fn targeted_helpers_are_available_without_transport_types() {
    fn use_targeted_api(
        archive: &mut IndexerV3Archive,
        key: &[u8; 32],
        request: &ScanRequest,
    ) -> blockzilla_archive_v3_reader::Result<IndexerV3TargetedScanReceipt> {
        archive.scan_target_candidates(
            key,
            IndexerV3CandidatePolicy::ReachedProgram,
            request,
            &mut blockzilla_archive_v3_reader::FnBlockSink::new(ignore_block),
        )
    }

    fn use_program_helper(
        archive: &mut IndexerV3Archive,
        key: &[u8; 32],
        request: &ScanRequest,
    ) -> blockzilla_archive_v3_reader::Result<IndexerV3TargetedScanReceipt> {
        archive.for_each_reached_program_candidate_block(key, request, |_| Ok(()))
    }

    fn use_wallet_helper(
        archive: &mut IndexerV3Archive,
        key: &[u8; 32],
        request: &ScanRequest,
    ) -> blockzilla_archive_v3_reader::Result<IndexerV3TargetedScanReceipt> {
        archive
            .for_each_signer_wallet_candidate_block(key, request, |_| -> QueryResult<()> { Ok(()) })
    }

    let _ = use_targeted_api;
    let _ = use_program_helper;
    let _ = use_wallet_helper;
}
