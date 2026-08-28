use std::{num::NonZeroU32, path::Path};

use blockzilla_archive_sdk::{
    ArchiveFormat, ArchiveInstructionSource, ArchiveIoSnapshot, ArchiveSource, NetworkEpoch,
    WORKER_FORMATS,
};

fn assert_source<T: ArchiveInstructionSource>() {}

#[test]
fn facade_exports_one_runtime_source_type() {
    assert_source::<ArchiveSource>();
    assert_eq!(WORKER_FORMATS.len(), 3);
    assert_eq!(WORKER_FORMATS[0], ArchiveFormat::CompactV2);

    let _open = |origin: &str, cache: &Path| NetworkEpoch::open(origin, 0, cache);
    let _range = NonZeroU32::new(1).unwrap();
    let _snapshot = ArchiveIoSnapshot::default();
}
