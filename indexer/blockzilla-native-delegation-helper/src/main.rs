//! One bounded native frame in, exact filtered compact semantics out. The caller
//! must first establish frozen rooted ancestry and verify the whole frame SHA.
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    io::{Read, Write},
};

const MAX_FRAME: usize = 16 * 1024 * 1024 + 65_536 + 16;
const STATE: &str = "73dUbDAWUDZRfXM3DcuRgR4q74nr97MyuBt95738JnMz";
const FILES: &[&str] = &[
    "DIRECT-AGAVE-NORMALIZED.v1.json",
    "archive-v2-blocks.index",
    "archive-v2-blocks.zstd",
    "archive-v2-meta.wincode",
    "available-poh.wincode",
    "blockhash_registry.bin",
    "prev_blockhash_tail.bin",
    "registry.bin",
    "registry.mphf",
    "registry_counts.bin",
    "signatures.bin",
    "vote_hash_registry.bin",
];
const REQUIRED: &[&str] = &[
    "DIRECT-AGAVE-NORMALIZED.v1.json",
    "archive-v2-blocks.index",
    "archive-v2-blocks.zstd",
    "archive-v2-meta.wincode",
    "available-poh.wincode",
    "blockhash_registry.bin",
    "registry.bin",
    "signatures.bin",
];

fn sha(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn coordinate(v: &Value) -> Result<usize> {
    Ok(usize::try_from(
        v.as_u64().context("invalid_object_coordinate")?,
    )?)
}

fn validate_objects(wire: &[u8]) -> Result<()> {
    ensure!(
        wire.len() >= 16 && wire.len() <= MAX_FRAME && &wire[..8] == b"BZSTRM01",
        "invalid_frame"
    );
    let meta = u32::from_le_bytes(wire[8..12].try_into()?) as usize;
    let payload_len = u32::from_le_bytes(wire[12..16].try_into()?) as usize;
    ensure!(
        (1..=65_536).contains(&meta)
            && payload_len <= 16 * 1024 * 1024
            && wire.len() == 16 + meta + payload_len,
        "invalid_frame_length"
    );
    let e: Value = serde_json::from_slice(&wire[16..16 + meta])?;
    let m = &e["marker"];
    ensure!(
        e["schema"] == "blockzilla-stream-envelope-v1"
            && e["profile"] == "native-processed-compact-v1"
            && e["evidence_included"] == false
            && e["event"]["kind"] == "block",
        "invalid_native_envelope"
    );
    ensure!(
        m["schema"] == "hivezilla-direct-agave-processed-compact-v1"
            && m["state"] == "COMPLETE"
            && m["source_profile"] == "agave-bank-callbacks-v1"
            && m["finality_basis"] == "operator-trusted-agave-processed-bank-callback"
            && m["finalized"] == false
            && m["canonical"] == false
            && m["publication_ready"] == false,
        "invalid_native_provenance"
    );
    let files = m["files"].as_array().context("missing_marker_files")?;
    let objects = e["objects"].as_array().context("missing_objects")?;
    ensure!(
        files.len() <= 32 && objects.len() <= FILES.len(),
        "object_count_limit"
    );
    let payload = &wire[16 + meta..];
    let mut names = BTreeSet::new();
    let mut offset = 0usize;
    for o in objects {
        let name = o["path"].as_str().context("missing_object_path")?;
        ensure!(
            FILES.contains(&name) && names.insert(name) && coordinate(&o["offset"])? == offset,
            "invalid_object_layout"
        );
        let bytes = coordinate(&o["bytes"])?;
        let end = offset.checked_add(bytes).context("object_overflow")?;
        ensure!(
            end <= payload.len() && o["sha256"] == sha(&payload[offset..end]),
            "object_digest_conflict"
        );
        let matches: Vec<_> = files.iter().filter(|f| f["path"] == name).collect();
        ensure!(
            matches.len() == 1
                && matches[0]["bytes"] == bytes
                && matches[0]["sha256"] == o["sha256"],
            "marker_object_conflict"
        );
        offset = end;
    }
    ensure!(
        offset == payload.len()
            && REQUIRED.iter().all(|p| names.contains(p))
            && files
                .iter()
                .filter_map(|f| f["path"].as_str())
                .filter(|p| FILES.contains(p))
                .all(|p| names.contains(p)),
        "missing_decode_dependency"
    );
    Ok(())
}

fn process(wire: &[u8]) -> Result<Vec<u8>> {
    validate_objects(wire)?;
    blockzilla_stream_decoder::decode(
        wire,
        &blockzilla_stream_decoder::Options {
            programs: vec![],
            accounts: vec![STATE.into()],
            successful_only: true,
            include_details: true,
        },
    )
}

fn main() -> Result<()> {
    let mut input = std::io::stdin().lock();
    let mut output = std::io::stdout().lock();
    loop {
        let mut header = [0u8; 4];
        if input.read(&mut header[..1])? == 0 {
            break;
        }
        input.read_exact(&mut header[1..])?;
        let length = u32::from_le_bytes(header) as usize;
        ensure!((16..=MAX_FRAME).contains(&length), "input_frame_limit");
        let mut wire = vec![0; length];
        input.read_exact(&mut wire)?;
        let bytes = process(&wire).unwrap_or_else(|e| {
            serde_json::to_vec(&json!({"error":format!("{e:#}")})).expect("serialize error")
        });
        ensure!(bytes.len() <= 32 * 1024 * 1024, "decoded_output_limit");
        output.write_all(&(bytes.len() as u32).to_le_bytes())?;
        output.write_all(&bytes)?;
        output.flush()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_bad_frame_before_decode() {
        assert!(validate_objects(b"BZSTRM01").is_err());
        let mut wire = b"BZSTRM01".to_vec();
        wire.extend_from_slice(&1u32.to_le_bytes());
        wire.extend_from_slice(&0u32.to_le_bytes());
        wire.push(b'{');
        assert!(validate_objects(&wire).is_err());
    }
}
