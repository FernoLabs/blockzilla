use std::{fs, path::Path};

const FORBIDDEN_DEPENDENCIES: [&str; 5] = [
    "blockzilla-firebase-indexer",
    "blockzilla-read-sdk",
    "of-car-reader",
    "sha2",
    "url",
];

const FORBIDDEN_SOURCE_TEXT: [&str; 12] = [
    "use blockzilla_firebase_indexer",
    "use blockzilla_read_sdk",
    "use of_car_reader",
    "use sha2::",
    "use url::",
    "CachedHttpRangeSource",
    "CanonicalBlockPlan",
    "CarHttpStream",
    "HttpRangeSource",
    "IndexerV3InstructionSource",
    "candidate_binding(",
    "derive_network_urls(",
];

#[test]
fn example_keeps_archive_setup_behind_the_facade() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifest = fs::read_to_string(root.join("Cargo.toml")).unwrap();
    assert!(
        manifest
            .lines()
            .any(|line| line.trim_start().starts_with("blockzilla-archive-sdk =")),
        "the example must depend on blockzilla-archive-sdk"
    );
    for dependency in FORBIDDEN_DEPENDENCIES {
        let assignment = format!("{dependency} =");
        assert!(
            !manifest
                .lines()
                .any(|line| line.trim_start().starts_with(&assignment)),
            "forbidden direct dependency: {dependency}"
        );
    }

    let mut source = String::new();
    collect_rust_source(&root.join("src"), &mut source);
    for forbidden in FORBIDDEN_SOURCE_TEXT {
        assert!(
            !source.contains(forbidden),
            "archive setup escaped the SDK boundary: {forbidden}"
        );
    }
    for required in [
        "NetworkEpoch::open(",
        "WORKER_FORMATS",
        "open_source_for(",
        "ArchiveSource",
        "ArchiveOpenReceipt",
        "ArchiveIoSnapshot",
        "finish_io(",
    ] {
        assert!(source.contains(required), "missing SDK flow: {required}");
    }
}

fn collect_rust_source(directory: &Path, output: &mut String) {
    let mut entries = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    entries.sort_unstable();
    for path in entries {
        if path.is_dir() {
            collect_rust_source(&path, output);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            output.push_str(&fs::read_to_string(path).unwrap());
        }
    }
}
