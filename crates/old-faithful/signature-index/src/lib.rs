mod car;
mod remote;
mod store;
mod types;

use anyhow::{Context, Result, bail};
use std::{
    collections::BTreeMap,
    fs,
    fs::File,
    io::{self, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};
use tempfile::NamedTempFile;

#[derive(Debug, Clone)]
pub struct BatchBuildResult {
    pub epoch: u64,
    pub car_path: PathBuf,
    pub index_dir: PathBuf,
    pub indexed_txs: usize,
    pub skipped_txs: usize,
    pub reused_existing: bool,
}

pub use store::IndexMeta;
pub use types::{LookupResponse, MetadataView, TransactionView};

pub fn build_index(car_path: &Path, out_dir: &Path) -> Result<IndexMeta> {
    fs::create_dir_all(out_dir).with_context(|| format!("create {}", out_dir.display()))?;
    eprintln!(
        "of-signature-index: building {} -> {}",
        car_path.display(),
        out_dir.display()
    );

    let temp_car = if is_zstd_path(car_path) {
        eprintln!(
            "of-signature-index: decompressing {} to a temporary .car",
            car_path.display()
        );
        Some(decompress_car_zstd_to_temp(car_path)?)
    } else {
        None
    };
    let scan_path = temp_car
        .as_ref()
        .map(|file| file.path())
        .unwrap_or(car_path);
    eprintln!("of-signature-index: scanning {}", scan_path.display());

    let scan = car::scan_car(scan_path)?;
    if scan.keys.is_empty() {
        bail!(
            "no transaction signatures were indexed from {}",
            car_path.display()
        );
    }

    eprintln!(
        "of-signature-index: writing index data to {}",
        out_dir.display()
    );
    store::write_index(out_dir, car_path, scan_path, scan)
}

pub fn build_indexes_in_dir(
    car_dir: &Path,
    out_dir: &Path,
    resume: bool,
) -> Result<Vec<BatchBuildResult>> {
    fs::create_dir_all(out_dir).with_context(|| format!("create {}", out_dir.display()))?;

    let epoch_cars = discover_epoch_cars(car_dir)?;
    eprintln!(
        "of-signature-index: discovered {} epoch car files in {}",
        epoch_cars.len(),
        car_dir.display()
    );
    let total = epoch_cars.len();
    let mut results = Vec::with_capacity(total);
    for (index, epoch_car) in epoch_cars.into_iter().enumerate() {
        let index_dir = out_dir.join(format!("epoch-{}", epoch_car.epoch));
        eprintln!(
            "of-signature-index: [{}/{}] epoch {} from {}",
            index + 1,
            total,
            epoch_car.epoch,
            epoch_car.path.display()
        );
        if resume && let Some(meta) = reusable_index_meta(&index_dir, &epoch_car.path)? {
            eprintln!(
                "of-signature-index: epoch {} already indexed at {}, skipping",
                epoch_car.epoch,
                index_dir.display()
            );
            results.push(BatchBuildResult {
                epoch: epoch_car.epoch,
                car_path: epoch_car.path,
                index_dir,
                indexed_txs: meta.indexed_txs,
                skipped_txs: meta.skipped_txs,
                reused_existing: true,
            });
            continue;
        }
        let meta = build_index(&epoch_car.path, &index_dir)?;
        eprintln!(
            "of-signature-index: epoch {} complete (indexed {}, skipped {})",
            epoch_car.epoch, meta.indexed_txs, meta.skipped_txs
        );
        results.push(BatchBuildResult {
            epoch: epoch_car.epoch,
            car_path: epoch_car.path,
            index_dir,
            indexed_txs: meta.indexed_txs,
            skipped_txs: meta.skipped_txs,
            reused_existing: false,
        });
    }

    Ok(results)
}

fn reusable_index_meta(index_dir: &Path, car_path: &Path) -> Result<Option<IndexMeta>> {
    if !index_dir.exists() {
        return Ok(None);
    }

    let meta = match store::read_index_meta(index_dir) {
        Ok(meta) => meta,
        Err(err) => {
            eprintln!(
                "of-signature-index: existing index at {} is incomplete or unreadable ({}), rebuilding",
                index_dir.display(),
                err
            );
            return Ok(None);
        }
    };

    if meta.car_path != car_path.display().to_string() {
        eprintln!(
            "of-signature-index: existing index at {} was built from {}, expected {}, rebuilding",
            index_dir.display(),
            meta.car_path,
            car_path.display()
        );
        return Ok(None);
    }
    if meta.value_record_bytes != store::RECORD_BYTES {
        eprintln!(
            "of-signature-index: existing index at {} uses {}-byte records, expected {}, rebuilding",
            index_dir.display(),
            meta.value_record_bytes,
            store::RECORD_BYTES
        );
        return Ok(None);
    }

    for filename in ["mphf.bin", "values.bin", "xor.desc", "xor.fp"] {
        let path = index_dir.join(filename);
        if !path.is_file() {
            eprintln!(
                "of-signature-index: existing index at {} is missing {}, rebuilding",
                index_dir.display(),
                path.display()
            );
            return Ok(None);
        }
    }

    let expected_values_len =
        meta.n
            .checked_mul(store::RECORD_BYTES)
            .context("values.bin size overflow while validating existing index")? as u64;
    let values_path = index_dir.join("values.bin");
    let values_len = fs::metadata(&values_path)
        .with_context(|| format!("stat {}", values_path.display()))?
        .len();
    if values_len != expected_values_len {
        eprintln!(
            "of-signature-index: existing index at {} has {}-byte values.bin, expected {}, rebuilding",
            index_dir.display(),
            values_len,
            expected_values_len
        );
        return Ok(None);
    }

    Ok(Some(meta))
}

pub fn get_transaction_response(
    index_dir: &Path,
    signature: &str,
    car_override: Option<PathBuf>,
    download_from_network: bool,
) -> Result<LookupResponse> {
    let signature_bytes = decode_signature_base58(signature)?;
    let mut response = lookup_signature_response(index_dir, signature)?;
    if !response.found {
        return Ok(response);
    }

    let car_path = car_override.or_else(|| response.car_path.as_ref().map(PathBuf::from));
    response.car_path = car_path.as_ref().map(|p| p.display().to_string());
    let car_url: Option<String> = car_path
        .as_ref()
        .and_then(|path| default_old_faithful_car_url(path.as_path()));
    let offset = response.offset.expect("found response must have offset");
    let size = response.size.expect("found response must have size");

    if download_from_network {
        if let Some(car_url) = &car_url {
            match remote::load_transaction_view_from_http(car_url, offset, size, &signature_bytes) {
                Ok(view) => response.transaction = Some(view),
                Err(err) => response.note = Some(err.to_string()),
            }
        } else if let Some(car_path) = &car_path {
            response.note = Some(format!(
                "could not derive old-faithful URL from CAR path {}",
                car_path.display()
            ));
        } else {
            response.note = Some(
                "signature found in index, but no CAR path was provided or stored".to_string(),
            );
        }
    } else if let Some(car_path) = &car_path {
        if is_zstd_path(car_path) {
            response.note = Some(format!(
                "local zstd CAR is not supported for indexed lookups: {} (use --network)",
                car_path.display()
            ));
        } else if car_path.exists() {
            match car::load_transaction_view_from_file(car_path, offset, size, &signature_bytes) {
                Ok(view) => response.transaction = Some(view),
                Err(err) => response.note = Some(err.to_string()),
            }
        } else {
            response.note = Some(format!(
                "CAR file not found locally: {} (use --network)",
                car_path.display()
            ));
        }
    } else {
        response.note =
            Some("signature found in index, but no CAR path was provided or stored".to_string());
    }

    Ok(response)
}

fn decompress_car_zstd_to_temp(path: &Path) -> Result<NamedTempFile> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut decoder =
        zstd::Decoder::with_buffer(reader).with_context(|| format!("open {}", path.display()))?;

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let mut temp = tempfile::Builder::new()
        .prefix("of-signature-index-")
        .suffix(".car")
        .tempfile_in(parent)
        .with_context(|| format!("create temp car near {}", path.display()))?;

    {
        let mut writer = BufWriter::new(temp.as_file_mut());
        io::copy(&mut decoder, &mut writer)
            .with_context(|| format!("decompress {} to temp car", path.display()))?;
        writer.flush().context("flush temp car writer")?;
    }
    temp.as_file_mut()
        .sync_all()
        .context("sync temp car file")?;

    Ok(temp)
}

#[derive(Debug, Clone)]
struct EpochCar {
    epoch: u64,
    path: PathBuf,
    compressed: bool,
}

fn discover_epoch_cars(car_dir: &Path) -> Result<Vec<EpochCar>> {
    let mut cars_by_epoch: BTreeMap<u64, EpochCar> = BTreeMap::new();

    for entry in fs::read_dir(car_dir).with_context(|| format!("read {}", car_dir.display()))? {
        let entry = entry.with_context(|| format!("read entry in {}", car_dir.display()))?;
        let file_type = entry
            .file_type()
            .with_context(|| format!("stat {}", entry.path().display()))?;
        if !file_type.is_file() {
            continue;
        }

        let path = entry.path();
        let Some(epoch) = epoch_from_car_path(&path) else {
            continue;
        };

        let candidate = EpochCar {
            epoch,
            path,
            compressed: is_zstd_path(entry.path().as_path()),
        };

        match cars_by_epoch.get(&epoch) {
            Some(existing) if existing.compressed && !candidate.compressed => {}
            _ => {
                cars_by_epoch.insert(epoch, candidate);
            }
        }
    }

    if cars_by_epoch.is_empty() {
        bail!(
            "no epoch car files found in {} (expected names like epoch-0.car or epoch-0.car.zst)",
            car_dir.display()
        );
    }

    Ok(cars_by_epoch.into_values().collect())
}

pub fn lookup_signature_response(index_dir: &Path, signature: &str) -> Result<LookupResponse> {
    let signature_bytes = decode_signature_base58(signature)?;
    let meta = store::read_index_meta(index_dir).ok();
    let car_path = meta
        .as_ref()
        .map(|m| PathBuf::from(m.car_path.as_str()))
        .filter(|path| !path.as_os_str().is_empty());

    let mut response = LookupResponse {
        found: false,
        signature: signature.to_string(),
        slot: None,
        offset: None,
        size: None,
        offset_kind: meta.as_ref().map(|m| m.offset_kind.clone()),
        car_path: car_path.as_ref().map(|p| p.display().to_string()),
        transaction: None,
        note: None,
    };

    let Some(record) = store::lookup_record(index_dir, &signature_bytes)? else {
        return Ok(response);
    };

    response.found = true;
    response.slot = Some(record.slot);
    response.offset = Some(record.offset);
    response.size = Some(record.size);

    Ok(response)
}

pub(crate) fn decode_signature_base58(signature: &str) -> Result<[u8; 64]> {
    let bytes = bs58::decode(signature.trim())
        .into_vec()
        .with_context(|| format!("decode base58 signature {}", signature.trim()))?;
    if bytes.len() != 64 {
        bail!("signature must decode to 64 bytes, got {}", bytes.len());
    }
    let mut out = [0u8; 64];
    out.copy_from_slice(&bytes);
    Ok(out)
}

pub(crate) fn encode_signature_base58(signature: &[u8; 64]) -> String {
    bs58::encode(signature).into_string()
}

fn default_old_faithful_car_url(path: &Path) -> Option<String> {
    let epoch = epoch_from_car_path(path)?;
    Some(format!(
        "https://files.old-faithful.net/{epoch}/epoch-{epoch}.car"
    ))
}

fn epoch_from_car_path(path: &Path) -> Option<u64> {
    let filename = path.file_name()?.to_str()?;
    let rest = filename.strip_prefix("epoch-")?;
    let epoch = rest
        .strip_suffix(".car")
        .or_else(|| rest.strip_suffix(".car.zst"))?;
    epoch.parse().ok()
}

pub(crate) fn is_zstd_path(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| matches!(ext, "zst" | "zstd"))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::{
        build_index, build_indexes_in_dir, decode_signature_base58, default_old_faithful_car_url,
        discover_epoch_cars, encode_signature_base58, epoch_from_car_path, reusable_index_meta,
    };
    use std::{
        fs,
        path::{Path, PathBuf},
    };
    use tempfile::tempdir;

    #[test]
    fn signature_base58_round_trip() {
        let mut signature = [0u8; 64];
        for (index, byte) in signature.iter_mut().enumerate() {
            *byte = index as u8;
        }

        let encoded = encode_signature_base58(&signature);
        let decoded = decode_signature_base58(&encoded).expect("decode base58 signature");

        assert_eq!(decoded, signature);
    }

    #[test]
    fn derives_old_faithful_url_from_epoch_car_name() {
        let url = default_old_faithful_car_url(Path::new("/tmp/epoch-800.car"))
            .expect("derive old-faithful URL");

        assert_eq!(url, "https://files.old-faithful.net/800/epoch-800.car");
    }

    #[test]
    fn derives_old_faithful_url_from_epoch_zstd_name() {
        let url = default_old_faithful_car_url(Path::new("/tmp/epoch-800.car.zst"))
            .expect("derive old-faithful URL");

        assert_eq!(url, "https://files.old-faithful.net/800/epoch-800.car");
    }

    #[test]
    fn parses_epoch_from_supported_car_names() {
        assert_eq!(epoch_from_car_path(Path::new("/tmp/epoch-0.car")), Some(0));
        assert_eq!(
            epoch_from_car_path(Path::new("/tmp/epoch-12.car.zst")),
            Some(12)
        );
        assert_eq!(
            epoch_from_car_path(Path::new("/tmp/epoch-157-biggest.car")),
            None
        );
    }

    #[test]
    fn discovers_epoch_cars_preferring_zstd() {
        let dir = tempdir().expect("tempdir");
        fs::write(dir.path().join("epoch-0.car"), []).expect("write epoch-0.car");
        fs::write(dir.path().join("epoch-0.car.zst"), []).expect("write epoch-0.car.zst");
        fs::write(dir.path().join("epoch-1.car"), []).expect("write epoch-1.car");
        fs::write(dir.path().join("not-an-epoch.txt"), []).expect("write extra file");

        let discovered = discover_epoch_cars(dir.path()).expect("discover epoch cars");

        assert_eq!(discovered.len(), 2);
        assert_eq!(discovered[0].epoch, 0);
        assert!(discovered[0].compressed);
        assert_eq!(
            discovered[0]
                .path
                .file_name()
                .and_then(|name| name.to_str()),
            Some("epoch-0.car.zst")
        );
        assert_eq!(discovered[1].epoch, 1);
        assert!(!discovered[1].compressed);
    }

    #[test]
    fn validates_complete_existing_index_for_resume() {
        let dir = tempdir().expect("tempdir");
        let car_path = fixture_car_path();
        let index_dir = dir.path().join("epoch-157");
        build_index(&car_path, &index_dir).expect("build fixture index");

        let meta = reusable_index_meta(&index_dir, &car_path)
            .expect("validate reusable index")
            .expect("existing index should be reusable");
        assert_eq!(meta.indexed_txs, 4208);
    }

    #[test]
    fn resume_skips_completed_epochs_and_builds_remaining_ones() {
        let temp = tempdir().expect("tempdir");
        let car_dir = temp.path().join("cars");
        let out_dir = temp.path().join("indexes");
        fs::create_dir_all(&car_dir).expect("create car dir");

        let fixture = fixture_car_path();
        fs::copy(&fixture, car_dir.join("epoch-0.car")).expect("copy epoch 0 fixture");
        fs::copy(&fixture, car_dir.join("epoch-1.car")).expect("copy epoch 1 fixture");

        build_index(&car_dir.join("epoch-0.car"), &out_dir.join("epoch-0"))
            .expect("prebuild epoch 0");

        let results = build_indexes_in_dir(&car_dir, &out_dir, true).expect("resume build-all");
        assert_eq!(results.len(), 2);
        assert!(results[0].reused_existing, "epoch 0 should be skipped");
        assert!(!results[1].reused_existing, "epoch 1 should be built");
        assert!(out_dir.join("epoch-1/meta.json").is_file());
    }

    fn fixture_car_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../car-reader/benches/fixtures/epoch-157-biggest.car")
    }
}
