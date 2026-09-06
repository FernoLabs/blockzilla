use blockzilla_compact_v2_reader::archive::{
    ArchiveInstructionSource, ArchiveIoSnapshot, CompactV2Archive, CompactV2LocalDescriptor,
    CompactV2OpenOptions, CompactV2ParallelScanConfig, CompactV2ParallelScanReceipt,
    CompactV2RegistryReadPolicy, CompactV2TransportKind, CompactV2TransportReceipt,
    DEFAULT_COMPACT_V2_FULL_REGISTRY_BYTES, MAX_COMPACT_V2_PARALLEL_WORKERS, ScanRange,
    ScanRequest,
};

#[test]
fn archive_implements_the_common_source_contract() {
    fn assert_source<T: ArchiveInstructionSource>() {}
    fn assert_send_sync<T: Send + Sync>() {}

    assert_source::<CompactV2Archive>();
    assert_send_sync::<blockzilla_compact_v2_reader::archive::Error>();
}

#[test]
fn common_example_types_are_reexported() {
    fn accepts(_: ScanRequest, _: Option<ScanRange>, _: ArchiveIoSnapshot) {}

    accepts(ScanRequest::all(), None, ArchiveIoSnapshot::default());
}

#[test]
fn local_descriptor_and_transport_receipt_are_public() {
    let descriptor = CompactV2LocalDescriptor::mainnet(900, "epoch-900-corrected-v2").unwrap();
    assert_eq!(descriptor.epoch, 900);
    let receipt = CompactV2TransportReceipt {
        kind: CompactV2TransportKind::LocalDirectory,
        local_read_calls: 1,
        local_read_bytes: 32,
        ..CompactV2TransportReceipt::default()
    };
    assert_eq!(receipt.kind.to_string(), "local-directory");
}

#[test]
fn network_transport_options_are_public() {
    let options = CompactV2OpenOptions {
        allow_insecure_http: true,
    };
    assert!(options.allow_insecure_http);
}

#[test]
fn parallel_scan_contract_is_public_and_defaults_to_available_cpus() {
    fn accepts(_: CompactV2ParallelScanConfig, _: Option<CompactV2ParallelScanReceipt>) {}

    let config = CompactV2ParallelScanConfig::default();
    assert!(config.workers >= 1);
    assert!(config.workers <= MAX_COMPACT_V2_PARALLEL_WORKERS);
    accepts(config, None);
}

#[test]
fn sequential_registry_policy_is_public_and_uses_the_one_gibibyte_default() {
    fn accepts(_: CompactV2RegistryReadPolicy) {}

    let policy = CompactV2RegistryReadPolicy::with_full_registry_limit(
        DEFAULT_COMPACT_V2_FULL_REGISTRY_BYTES,
    );
    assert_eq!(
        policy.max_full_registry_bytes(),
        DEFAULT_COMPACT_V2_FULL_REGISTRY_BYTES
    );
    assert_eq!(
        CompactV2RegistryReadPolicy::sparse_only().max_full_registry_bytes(),
        0
    );
    accepts(policy);
}
