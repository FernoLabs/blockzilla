use crate::car;
use anyhow::{Context, Result, anyhow, bail};
use gxhash::GxHasher;
use memmap2::{Mmap, MmapMut};
use ph::fmph;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    hash::Hasher,
    path::Path,
};
use xorf::{BinaryFuse8, BinaryFuse8Ref, DmaSerializable, Filter, FilterRef};

pub(crate) const RECORD_BYTES: usize = 28;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMeta {
    pub n: usize,
    pub value_record_bytes: usize,
    pub car_path: String,
    pub compressed: bool,
    pub offset_kind: String,
    pub indexed_txs: usize,
    pub skipped_txs: usize,
}

#[derive(Debug, Default)]
pub(crate) struct ScanStats {
    pub(crate) indexed_txs: usize,
    pub(crate) skipped_txs: usize,
}

pub(crate) struct ScanOutput {
    pub(crate) keys: Vec<u64>,
    pub(crate) stats: ScanStats,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Record {
    pub(crate) fingerprint: u64,
    pub(crate) slot: u64,
    pub(crate) offset: u64,
    pub(crate) size: u32,
}

impl Record {
    pub(crate) fn encode(self) -> [u8; RECORD_BYTES] {
        let mut out = [0u8; RECORD_BYTES];
        out[0..8].copy_from_slice(&self.fingerprint.to_le_bytes());
        out[8..16].copy_from_slice(&self.slot.to_le_bytes());
        out[16..24].copy_from_slice(&self.offset.to_le_bytes());
        out[24..28].copy_from_slice(&self.size.to_le_bytes());
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != RECORD_BYTES {
            bail!(
                "invalid record size: expected {} bytes, got {}",
                RECORD_BYTES,
                bytes.len()
            );
        }
        Ok(Self {
            fingerprint: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            slot: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            offset: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            size: u32::from_le_bytes(bytes[24..28].try_into().unwrap()),
        })
    }
}

pub(crate) fn write_index(
    out_dir: &Path,
    car_path: &Path,
    scan_path: &Path,
    scan: ScanOutput,
) -> Result<IndexMeta> {
    let ScanOutput {
        mut keys,
        stats: first_pass_stats,
    } = scan;
    keys.sort_unstable();
    ensure_unique_keys(&keys)?;

    let xor = BinaryFuse8::try_from(keys.as_slice())
        .map_err(|err| anyhow!("build BinaryFuse8 (duplicates?): {err}"))?;
    let mut desc = vec![0u8; BinaryFuse8::DESCRIPTOR_LEN];
    xor.dma_copy_descriptor_to(&mut desc);
    fs::write(out_dir.join("xor.desc"), &desc).context("write xor.desc")?;
    fs::write(out_dir.join("xor.fp"), xor.dma_fingerprints()).context("write xor.fp")?;

    let record_count = keys.len();
    let mphf = fmph::Function::from(keys);
    let mut mphf_file = File::create(out_dir.join("mphf.bin")).context("create mphf.bin")?;
    mphf.write(&mut mphf_file).context("write mphf.bin")?;

    let values_path = out_dir.join("values.bin");
    let mut values_mmap = create_values_mmap(&values_path, record_count)?;
    let second_pass_stats =
        car::write_values_from_car(scan_path, &mphf, &mut values_mmap, record_count)?;
    values_mmap.flush().context("flush values.bin")?;
    drop(values_mmap);
    File::open(&values_path)
        .and_then(|file| file.sync_all())
        .with_context(|| format!("sync {}", values_path.display()))?;

    if second_pass_stats.indexed_txs != first_pass_stats.indexed_txs
        || second_pass_stats.skipped_txs != first_pass_stats.skipped_txs
    {
        bail!(
            "scan stats changed between passes: first indexed {} skipped {}, second indexed {} skipped {}",
            first_pass_stats.indexed_txs,
            first_pass_stats.skipped_txs,
            second_pass_stats.indexed_txs,
            second_pass_stats.skipped_txs
        );
    }

    let meta = IndexMeta {
        n: record_count,
        value_record_bytes: RECORD_BYTES,
        car_path: car_path.display().to_string(),
        compressed: car_path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| matches!(ext, "zst" | "zstd"))
            .unwrap_or(false),
        offset_kind: "car_file_offset".to_string(),
        indexed_txs: first_pass_stats.indexed_txs,
        skipped_txs: first_pass_stats.skipped_txs,
    };
    fs::write(
        out_dir.join("meta.json"),
        serde_json::to_vec_pretty(&meta).context("serialize meta.json")?,
    )
    .context("write meta.json")?;

    Ok(meta)
}

fn ensure_unique_keys(keys: &[u64]) -> Result<()> {
    for pair in keys.windows(2) {
        if pair[0] == pair[1] {
            bail!(
                "duplicate or colliding signature hash encountered while building index: {:016x}",
                pair[0]
            );
        }
    }
    Ok(())
}

fn create_values_mmap(path: &Path, record_count: usize) -> Result<MmapMut> {
    let values_len = record_count
        .checked_mul(RECORD_BYTES)
        .context("values.bin size overflow")?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .with_context(|| format!("create {}", path.display()))?;
    file.set_len(values_len as u64)
        .with_context(|| format!("resize {}", path.display()))?;
    unsafe { MmapMut::map_mut(&file) }.with_context(|| format!("mmap {}", path.display()))
}

pub(crate) fn read_index_meta(index_dir: &Path) -> Result<IndexMeta> {
    let bytes = fs::read(index_dir.join("meta.json")).context("read meta.json")?;
    serde_json::from_slice(&bytes).context("parse meta.json")
}

pub(crate) fn lookup_record(index_dir: &Path, signature: &[u8; 64]) -> Result<Option<Record>> {
    let key = hash_signature(signature);
    let fingerprint = signature_fingerprint(signature);

    let desc = fs::read(index_dir.join("xor.desc")).context("read xor.desc")?;
    let fp_file = File::open(index_dir.join("xor.fp")).context("open xor.fp")?;
    let fp_mmap = unsafe { Mmap::map(&fp_file) }.context("mmap xor.fp")?;
    let xor = BinaryFuse8Ref::from_dma(&desc, &fp_mmap);
    if !xor.contains(&key) {
        return Ok(None);
    }

    let mut mphf_file = File::open(index_dir.join("mphf.bin")).context("open mphf.bin")?;
    let mphf = fmph::Function::read(&mut mphf_file).context("read mphf.bin")?;
    let Some(index) = mphf.get(&key) else {
        return Ok(None);
    };
    let index = index as usize;

    let values_file = File::open(index_dir.join("values.bin")).context("open values.bin")?;
    let values_mmap = unsafe { Mmap::map(&values_file) }.context("mmap values.bin")?;

    let start = index
        .checked_mul(RECORD_BYTES)
        .context("record offset overflow")?;
    if start + RECORD_BYTES > values_mmap.len() {
        return Ok(None);
    }

    let record = Record::decode(&values_mmap[start..start + RECORD_BYTES])?;
    if record.fingerprint != fingerprint {
        return Ok(None);
    }

    Ok(Some(record))
}

pub(crate) fn hash_signature(signature: &[u8; 64]) -> u64 {
    let mut hasher = GxHasher::default();
    hasher.write(signature);
    hasher.finish()
}

pub(crate) fn signature_fingerprint(signature: &[u8; 64]) -> u64 {
    u64::from_le_bytes(signature[0..8].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::{RECORD_BYTES, Record, ensure_unique_keys};

    #[test]
    fn record_round_trip() {
        let record = Record {
            fingerprint: 11,
            slot: 22,
            offset: 33,
            size: 44,
        };

        let encoded = record.encode();
        assert_eq!(encoded.len(), RECORD_BYTES);

        let decoded = Record::decode(&encoded).expect("decode record");
        assert_eq!(decoded.fingerprint, record.fingerprint);
        assert_eq!(decoded.slot, record.slot);
        assert_eq!(decoded.offset, record.offset);
        assert_eq!(decoded.size, record.size);
    }

    #[test]
    fn detects_duplicate_keys_after_sort() {
        let err = ensure_unique_keys(&[1, 2, 2, 3]).expect_err("duplicate keys should fail");
        assert!(
            err.to_string()
                .contains("duplicate or colliding signature hash")
        );
    }
}
