use criterion::{Criterion, criterion_group, criterion_main};
use of_signature_index::lookup_signature_response;
use std::{env, hint::black_box, path::PathBuf, time::Duration};

const DEFAULT_SIGNATURE: &str =
    "GzeEf7kokurDXMmj2Nm1k83AfiyyjVhcrBxhRoDuuVBhDjevQxrkpCBJf9b1FumToC8u8ThaxQdiB8LMc9FNA9D";

fn bench_get_transaction(c: &mut Criterion) {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root");

    let index_dir = env::var_os("BLOCKZILLA_BENCH_INDEX_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace_root.join("test-index"));
    let signature =
        env::var("BLOCKZILLA_BENCH_SIGNATURE").unwrap_or_else(|_| DEFAULT_SIGNATURE.to_string());

    if !index_dir.exists() {
        eprintln!(
            "skipping of-signature-index benchmark: missing {}",
            index_dir.display()
        );
        return;
    }

    let response = lookup_signature_response(&index_dir, &signature).expect("warm lookup");
    assert!(
        response.found,
        "signature should be present in benchmark index"
    );
    let missing_signature = find_missing_signature(&index_dir, &signature);

    let mut group = c.benchmark_group("of-signature-index");
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.bench_function("lookup_offset/hit", |b| {
        b.iter(|| {
            let response = lookup_signature_response(
                black_box(index_dir.as_path()),
                black_box(signature.as_str()),
            )
            .expect("bench lookup offset hit");
            black_box(response);
        });
    });
    group.bench_function("lookup_offset/miss", |b| {
        b.iter(|| {
            let response = lookup_signature_response(
                black_box(index_dir.as_path()),
                black_box(missing_signature.as_str()),
            )
            .expect("bench lookup offset miss");
            black_box(response);
        });
    });
    group.finish();
}

fn find_missing_signature(index_dir: &std::path::Path, present_signature: &str) -> String {
    let bytes = bs58::decode(present_signature)
        .into_vec()
        .expect("decode benchmark signature");
    assert_eq!(
        bytes.len(),
        64,
        "benchmark signature must decode to 64 bytes"
    );

    for bump in 1u16..=u8::MAX as u16 {
        let candidate = bytes.clone();
        let mut candidate = candidate;
        candidate[63] = candidate[63].wrapping_add(bump as u8);
        let candidate = bs58::encode(candidate).into_string();

        let response =
            lookup_signature_response(index_dir, &candidate).expect("probe missing signature");
        if !response.found {
            return candidate;
        }
    }

    panic!("could not find a missing signature candidate for benchmark")
}

criterion_group!(benches, bench_get_transaction);
criterion_main!(benches);
