use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, sync::OnceLock};

pub const MAGIC: &[u8; 8] = b"BZSTRM01";
pub const MAX_META: usize = 64 * 1024;
pub const MAX_PAYLOAD: usize = 16 * 1024 * 1024;
pub const MAX_FRAME: usize = 16 + MAX_META + MAX_PAYLOAD;
pub const PROCESSED: &str = "native-processed-compact-v1";
// v2 admission is separate; decode() below continues to reject v2 bytes.
pub use blockzilla_bank_stream_v2 as bank_v2;
pub const FILES: &[&str] = &[
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
pub const REQUIRED: &[&str] = &[
    "DIRECT-AGAVE-NORMALIZED.v1.json",
    "archive-v2-blocks.index",
    "archive-v2-blocks.zstd",
    "archive-v2-meta.wincode",
    "available-poh.wincode",
    "blockhash_registry.bin",
    "registry.bin",
    "signatures.bin",
];

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Object {
    pub path: String,
    pub offset: usize,
    pub bytes: usize,
    pub sha256: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Envelope {
    pub schema: String,
    pub profile: String,
    pub evidence_included: bool,
    pub marker: Value,
    pub objects: Vec<Object>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event: Option<BankEvent>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BankIdentity {
    pub slot: u64,
    pub bank_id: u64,
    pub blockhash: [u8; 32],
    pub parent_slot: u64,
    pub parent_blockhash: [u8; 32],
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BankEvent {
    pub sequence: u64,
    pub source_sequence: u64,
    pub kind: String,
    pub bank: BankIdentity,
    pub payload_sha256: String,
    pub reason: Option<String>,
}
#[derive(Clone, Debug)]
pub struct Chunk {
    pub wire: Bytes,
    pub envelope: Envelope,
    pub cursor: String,
    pub digest: String,
    pub session: String,
    pub first_slot: u64,
    pub last_slot: u64,
    pub blockhash_notice: OnceLock<Bytes>,
}
impl Chunk {
    pub fn position(&self) -> u64 {
        self.envelope
            .event
            .as_ref()
            .map_or(self.last_slot, |e| e.sequence)
    }
}

pub fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
pub fn session_name(s: &str) -> bool {
    !s.is_empty() && s.len() <= 128 && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}
pub fn parse_cursor(s: &str) -> Result<(&str, u64, &str)> {
    let parts: Vec<_> = s.split(':').collect();
    ensure!(parts.len() == 3 && session_name(parts[0]), "invalid_cursor");
    ensure!(
        parts[2] == "-"
            || (parts[2].len() == 64
                && parts[2]
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())),
        "invalid_cursor"
    );
    let number = parts[1].strip_prefix('p').unwrap_or(parts[1]);
    let slot = number.parse::<u64>()?;
    ensure!(
        number == slot.to_string() && slot <= 9_007_199_254_740_991,
        "invalid_cursor"
    );
    Ok((parts[0], slot, parts[2]))
}
pub fn anchor(session: &str, slot: u64) -> String {
    format!("{session}:{slot}:-")
}
pub fn processed_cursor(s: &str) -> bool {
    s.split(':').nth(1).is_some_and(|p| p.starts_with('p'))
}
pub fn processed_anchor(session: &str, sequence: u64) -> String {
    format!("{session}:p{sequence}:-")
}

pub fn encode(envelope: &Envelope, payload: &[u8]) -> Result<Bytes> {
    let meta = serde_json::to_vec(envelope)?;
    ensure!(
        meta.len() <= MAX_META && payload.len() <= MAX_PAYLOAD,
        "frame_too_large"
    );
    let mut wire = Vec::with_capacity(16 + meta.len() + payload.len());
    wire.extend_from_slice(MAGIC);
    wire.extend_from_slice(&(meta.len() as u32).to_le_bytes());
    wire.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    wire.extend_from_slice(&meta);
    wire.extend_from_slice(payload);
    Ok(wire.into())
}

pub fn frame_length(header: &[u8]) -> Result<usize> {
    ensure!(
        header.len() >= 16 && &header[..8] == MAGIC,
        "invalid_frame_magic"
    );
    let meta = u32::from_le_bytes(header[8..12].try_into()?) as usize;
    let payload = u32::from_le_bytes(header[12..16].try_into()?) as usize;
    ensure!(
        (1..=MAX_META).contains(&meta) && payload <= MAX_PAYLOAD,
        "frame_too_large"
    );
    Ok(16 + meta + payload)
}

pub fn decode(wire: Bytes) -> Result<Chunk> {
    ensure!(
        frame_length(&wire)? == wire.len(),
        "incomplete_or_trailing_frame"
    );
    let meta_len = u32::from_le_bytes(wire[8..12].try_into()?) as usize;
    let envelope: Envelope = serde_json::from_slice(&wire[16..16 + meta_len])?;
    ensure!(
        envelope.schema == "blockzilla-stream-envelope-v1"
            && matches!(
                envelope.profile.as_str(),
                "native-compact-projection-v1" | PROCESSED
            )
            && !envelope.evidence_included,
        "unsupported_profile"
    );
    let m = &envelope.marker;
    let processed = envelope.profile == PROCESSED;
    ensure!(
        m["state"] == "COMPLETE"
            && m["source_profile"] == "agave-bank-callbacks-v1"
            && m["canonical"] == false
            && m["publication_ready"] == false
            && ((!processed
                && envelope.event.is_none()
                && m["schema"] == "hivezilla-direct-agave-compact-v1"
                && m["finalized"] == true
                && m["finality_basis"] == "operator-trusted-agave-rooted-bank-callback")
                || (processed && m["finalized"] == false)),
        "invalid_provenance"
    );
    let session = m["session_id"]
        .as_str()
        .context("missing_session")?
        .to_string();
    ensure!(session_name(&session), "invalid_session");
    for k in ["genesis_hash", "node_identity", "agave_revision"] {
        ensure!(
            m[k].as_str()
                .is_some_and(|s| !s.is_empty() && s.len() <= 128),
            "missing_source_identity"
        );
    }
    if processed {
        let e = envelope.event.as_ref().context("missing_bank_event")?;
        ensure!(
            e.sequence > 0
                && e.sequence <= 9_007_199_254_740_991
                && e.source_sequence > 0
                && e.bank.slot <= 9_007_199_254_740_991
                && e.bank.bank_id <= 9_007_199_254_740_991
                && e.bank.parent_slot < e.bank.slot
                && matches!(e.kind.as_str(), "block" | "invalidate" | "root")
                && e.payload_sha256.len() == 64
                && e.payload_sha256
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
            "invalid_bank_event"
        );
        if e.kind != "block" {
            ensure!(
                m["schema"] == "hivezilla-processed-control-v1"
                    && envelope.objects.is_empty()
                    && wire.len() == 16 + meta_len
                    && m["produced_blocks"].is_null()
                    && m["files"].is_null(),
                "invalid_control_payload"
            );
            let slot = e.bank.slot;
            let digest = digest(&wire);
            let cursor = format!("{session}:p{}:{digest}", e.sequence);
            return Ok(Chunk {
                wire,
                envelope,
                cursor,
                digest,
                session,
                first_slot: slot,
                last_slot: slot,
                blockhash_notice: OnceLock::new(),
            });
        }
        ensure!(
            m["schema"] == "hivezilla-direct-agave-processed-compact-v1"
                && m["finality_basis"] == "operator-trusted-agave-processed-bank-callback",
            "invalid_processed_payload"
        );
    }
    let blocks = m["produced_blocks"].as_array().context("missing_blocks")?;
    if processed {
        let e = envelope.event.as_ref().unwrap();
        ensure!(
            blocks.len() == 1
                && serde_json::from_value::<BankIdentity>(serde_json::json!({
            "slot":blocks[0]["slot"],"bank_id":blocks[0]["bank_id"],"blockhash":blocks[0]["blockhash"],
            "parent_slot":blocks[0]["parent_slot"],"parent_blockhash":blocks[0]["parent_blockhash"]}))?
                    == e.bank,
            "event_bank_mismatch"
        );
    }
    ensure!(
        !blocks.is_empty()
            && blocks.len() <= 32
            && m["blocks"].as_u64() == Some(blocks.len() as u64),
        "invalid_block_count"
    );
    let first_slot = blocks[0]["slot"].as_u64().context("missing_slot")?;
    let last_slot = blocks.last().unwrap()["slot"]
        .as_u64()
        .context("missing_slot")?;
    ensure!(
        m["start_slot"].as_u64() == Some(first_slot)
            && last_slot.checked_add(1) == m["end_slot_exclusive"].as_u64()
            && m["epoch"].as_u64() == Some(first_slot / 432000)
            && first_slot / 432000 == last_slot / 432000,
        "invalid_range"
    );
    for (i, b) in blocks.iter().enumerate() {
        let slot = b["slot"].as_u64().context("missing_slot")?;
        ensure!(
            slot <= 9_007_199_254_740_991
                && b["block_id"].as_u64() == Some(i as u64)
                && b["parent_slot"].as_u64().is_some_and(|p| p < slot),
            "invalid_block_identity"
        );
        for k in ["blockhash", "parent_blockhash"] {
            ensure!(
                b[k].as_array().is_some_and(
                    |a| a.len() == 32 && a.iter().all(|v| v.as_u64().is_some_and(|n| n < 256))
                ),
                "invalid_block_hash"
            );
        }
        if i > 0 {
            ensure!(
                b["parent_slot"] == blocks[i - 1]["slot"]
                    && b["parent_blockhash"] == blocks[i - 1]["blockhash"],
                "parent_chain_gap"
            );
        }
    }
    let payload = &wire[16 + meta_len..];
    let marker_files = m["files"].as_array().context("missing_marker_files")?;
    ensure!(
        marker_files.len() <= 32 && envelope.objects.len() <= FILES.len(),
        "too_many_objects"
    );
    let mut names = BTreeSet::new();
    let mut offset = 0;
    for o in &envelope.objects {
        ensure!(
            FILES.contains(&o.path.as_str()) && names.insert(o.path.as_str()) && o.offset == offset,
            "invalid_object_layout"
        );
        offset = offset
            .checked_add(o.bytes)
            .context("object_size_overflow")?;
        ensure!(
            offset <= payload.len() && digest(&payload[o.offset..offset]) == o.sha256,
            "object_digest_mismatch"
        );
        let matches: Vec<_> = marker_files
            .iter()
            .filter(|f| f["path"] == o.path)
            .collect();
        ensure!(
            matches.len() == 1
                && matches[0]["bytes"].as_u64() == Some(o.bytes as u64)
                && matches[0]["sha256"] == o.sha256,
            "marker_object_mismatch"
        );
    }
    ensure!(
        offset == payload.len()
            && REQUIRED.iter().all(|p| names.contains(p))
            && marker_files
                .iter()
                .filter_map(|f| f["path"].as_str())
                .filter(|p| FILES.contains(p))
                .all(|p| names.contains(p)),
        "missing_decode_dependency"
    );
    let digest = digest(&wire);
    let cursor = if processed {
        format!(
            "{session}:p{}:{digest}",
            envelope.event.as_ref().unwrap().sequence
        )
    } else {
        format!("{session}:{last_slot}:{digest}")
    };
    Ok(Chunk {
        wire,
        envelope,
        cursor,
        digest,
        session,
        first_slot,
        last_slot,
        blockhash_notice: OnceLock::new(),
    })
}

pub fn follows(previous: &Chunk, next: &Chunk) -> Result<()> {
    ensure!(previous.session == next.session, "capture_session_changed");
    let a = &previous.envelope.marker;
    let b = &next.envelope.marker;
    ensure!(
        previous.envelope.profile == next.envelope.profile,
        "profile_changed"
    );
    for k in ["genesis_hash", "node_identity", "agave_revision"] {
        ensure!(a[k] == b[k], "source_identity_changed");
    }
    if next.envelope.profile == PROCESSED {
        ensure!(
            previous.position().checked_add(1) == Some(next.position()),
            "event_sequence_gap"
        );
        return Ok(());
    }
    let first = &b["produced_blocks"][0];
    let last = a["produced_blocks"].as_array().unwrap().last().unwrap();
    ensure!(
        first["parent_slot"] == last["slot"]
            && first["parent_blockhash"] == last["blockhash"]
            && next.first_slot > previous.last_slot,
        "parent_chain_gap"
    );
    Ok(())
}

#[cfg(test)]
pub(crate) fn fixture(slot: u64, parent: u64) -> Chunk {
    use serde_json::json;
    let files: Vec<_> = REQUIRED
        .iter()
        .map(|p| json!({"path":p,"bytes":1,"sha256":digest(&[42])}))
        .collect();
    let hash = [1u8; 32];
    let marker = json!({"schema":"hivezilla-direct-agave-compact-v1","state":"COMPLETE","source_profile":"agave-bank-callbacks-v1","finalized":true,"canonical":false,"publication_ready":false,"finality_basis":"operator-trusted-agave-rooted-bank-callback","session_id":"session1","genesis_hash":"genesis","node_identity":"operator","agave_revision":"rev","epoch":slot/432000,"start_slot":slot,"end_slot_exclusive":slot+1,"blocks":1,"produced_blocks":[{"slot":slot,"bank_id":slot,"block_id":0,"parent_slot":parent,"blockhash":hash,"parent_blockhash":hash}],"files":files});
    let e = Envelope {
        schema: "blockzilla-stream-envelope-v1".into(),
        profile: "native-compact-projection-v1".into(),
        evidence_included: false,
        marker,
        objects: REQUIRED
            .iter()
            .enumerate()
            .map(|(i, p)| Object {
                path: p.to_string(),
                offset: i,
                bytes: 1,
                sha256: digest(&[42]),
            })
            .collect(),
        event: None,
    };
    decode(encode(&e, &vec![42; REQUIRED.len()]).unwrap()).unwrap()
}

#[cfg(test)]
pub(crate) fn processed_fixture(sequence: u64, slot: u64, bank: u64, kind: &str) -> Chunk {
    let old = fixture(slot, slot - 1);
    let mut e = old.envelope;
    e.profile = PROCESSED.into();
    e.marker["schema"] = "hivezilla-direct-agave-processed-compact-v1".into();
    e.marker["finalized"] = false.into();
    e.marker["finality_basis"] = "operator-trusted-agave-processed-bank-callback".into();
    e.marker["produced_blocks"][0]["bank_id"] = bank.into();
    e.event = Some(BankEvent {
        sequence,
        source_sequence: sequence,
        kind: kind.into(),
        bank: BankIdentity {
            slot,
            bank_id: bank,
            blockhash: [1; 32],
            parent_slot: slot - 1,
            parent_blockhash: [1; 32],
        },
        payload_sha256: digest(b"marker"),
        reason: None,
    });
    let payload = if kind == "block" {
        vec![42; REQUIRED.len()]
    } else {
        e.marker["schema"] = "hivezilla-processed-control-v1".into();
        e.objects.clear();
        for key in ["produced_blocks", "files"] {
            e.marker.as_object_mut().unwrap().remove(key);
        }
        vec![]
    };
    decode(encode(&e, &payload).unwrap()).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn competing_banks_controls_and_sequence_gaps() {
        let a = processed_fixture(1, 100, 7, "block");
        let b = processed_fixture(2, 100, 8, "block");
        let clear = processed_fixture(3, 100, 7, "invalidate");
        let root = processed_fixture(4, 100, 8, "root");
        follows(&a, &b).unwrap();
        follows(&b, &clear).unwrap();
        follows(&clear, &root).unwrap();
        assert!(root.envelope.objects.is_empty());
        assert!(follows(&a, &clear).is_err());
        assert!(follows(&fixture(100, 99), &b).is_err());
        assert_eq!(parse_cursor(&root.cursor).unwrap().1, 4);
        let mut bad = root.envelope;
        bad.objects = a.envelope.objects;
        assert!(decode(encode(&bad, &[42]).unwrap()).is_err());
    }
    #[test]
    fn bounds_are_checked_before_allocation() {
        let mut h = *b"BZSTRM01\x01\x00\x00\x00\x00\x00\x00\x00";
        h[12..].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(frame_length(&h).is_err());
    }
    #[test]
    fn corruption_and_missing_dependencies_fail() {
        let c = fixture(100, 99);
        let mut bytes = c.wire.to_vec();
        *bytes.last_mut().unwrap() ^= 1;
        assert!(decode(bytes.into()).is_err());
        let mut e = c.envelope;
        e.objects.pop();
        assert!(decode(encode(&e, &vec![42; REQUIRED.len() - 1]).unwrap()).is_err());
    }
    #[test]
    fn skipped_slots_are_allowed_but_parent_gaps_are_not() {
        let a = fixture(100, 99);
        let b = fixture(103, 100);
        follows(&a, &b).unwrap();
        assert!(follows(&a, &fixture(103, 102)).is_err());
    }
    #[test]
    fn cursors_do_not_accept_ambiguous_or_foreign_syntax() {
        for s in [
            "session1:01:-",
            "../:1:-",
            "session1:2:abc",
            "session1:1:-:extra",
        ] {
            assert!(parse_cursor(s).is_err());
        }
        parse_cursor(&fixture(100, 99).cursor).unwrap();
    }
}
