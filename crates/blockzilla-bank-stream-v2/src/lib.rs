//! Strict, shared v2 admission. Original marker bytes and semantic compact
//! decoding remain separate checks; embedded JSON is never a marker-file digest.
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

pub const PROFILE: &str = "native-bank-compact-v2";
pub const ENVELOPE: &str = "blockzilla-stream-envelope-v2";
pub const HEAD: &str = "blockzilla-confirmed-head-v1";
pub const MAX_META: usize = 65_536;
pub const MAX_PAYLOAD: usize = 16 * 1024 * 1024;
pub const MAX_FRAME: usize = 16 + MAX_META + MAX_PAYLOAD;
pub const SAFE: u64 = 9_007_199_254_740_991;
pub const CLOCK_ALLOWANCE_MS: u64 = 5000;
pub const DEFAULT_MAX_LAG_MS: u64 = 5000;
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

fn required_option<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> std::result::Result<Option<T>, D::Error> {
    Option::<T>::deserialize(d)
}
pub fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
pub fn is_digest(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}
pub fn session_name(s: &str) -> bool {
    !s.is_empty() && s.len() <= 128 && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}
pub fn parse_cursor(s: &str) -> Result<(&str, u64, &str)> {
    let mut p = s.split(':');
    let session = p.next().context("invalid_cursor")?;
    let number = p
        .next()
        .and_then(|s| s.strip_prefix('p'))
        .context("invalid_cursor")?;
    let sha = p.next().context("invalid_cursor")?;
    let sequence: u64 = number.parse().context("invalid_cursor")?;
    ensure!(
        p.next().is_none()
            && session_name(session)
            && sequence <= SAFE
            && number == sequence.to_string()
            && (is_digest(sha) || (sha == "-" && sequence == 0)),
        "invalid_cursor"
    );
    Ok((session, sequence, sha))
}
pub fn cursor(session: &str, sequence: u64, sha: &str) -> String {
    format!("{session}:p{sequence}:{sha}")
}
pub fn hash(s: &str) -> Result<[u8; 32]> {
    let bytes: [u8; 32] = bs58::decode(s)
        .into_vec()?
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid_hash"))?;
    ensure!(bs58::encode(bytes).into_string() == s, "noncanonical_hash");
    Ok(bytes)
}
pub fn clock_check(timestamp: u64, now: u64) -> Result<()> {
    ensure!(
        timestamp <= SAFE && now <= SAFE && timestamp <= now.saturating_add(CLOCK_ALLOWANCE_MS),
        "future_timestamp"
    );
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SourceIdentity {
    pub genesis_hash: String,
    pub node_identity: String,
    pub agave_revision: String,
    pub session_id: String,
}
impl SourceIdentity {
    pub fn validate(&self) -> Result<()> {
        hash(&self.genesis_hash)?;
        hash(&self.node_identity)?;
        ensure!(
            session_name(&self.session_id)
                && !self.agave_revision.is_empty()
                && self.agave_revision.len() <= 128,
            "invalid_source"
        );
        Ok(())
    }
    pub fn from_marker(m: &Value) -> Result<Self> {
        let field = |k| {
            m[k].as_str()
                .map(str::to_owned)
                .with_context(|| format!("missing_source:{k}"))
        };
        let v = Self {
            genesis_hash: field("genesis_hash")?,
            node_identity: field("node_identity")?,
            agave_revision: field("agave_revision")?,
            session_id: field("session_id")?,
        };
        v.validate()?;
        Ok(v)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct BankIdentity {
    pub slot: u64,
    pub bank_id: u64,
    pub blockhash: [u8; 32],
    pub parent_slot: u64,
    pub parent_blockhash: [u8; 32],
}
impl BankIdentity {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.slot <= SAFE && self.bank_id <= SAFE && self.parent_slot < self.slot,
            "invalid_bank"
        );
        Ok(())
    }
    pub fn key(&self) -> (u64, u64) {
        (self.slot, self.bank_id)
    }
    pub fn parent_matches(&self, parent: &Self) -> bool {
        self.parent_slot == parent.slot && self.parent_blockhash == parent.blockhash
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecentBlockhash {
    pub blockhash: String,
    pub context_slot: u64,
    pub last_valid_block_height: u64,
}
impl RecentBlockhash {
    pub fn validate(&self, bank: &BankIdentity, height: u64) -> Result<()> {
        ensure!(
            self.context_slot == bank.slot
                && hash(&self.blockhash)? == bank.blockhash
                && self.last_valid_block_height > 0
                && self.last_valid_block_height <= SAFE
                && self.last_valid_block_height >= height,
            "invalid_native_expiry"
        );
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BankEvent {
    pub sequence: u64,
    pub source_sequence: u64,
    pub kind: String,
    pub bank: BankIdentity,
    pub payload_sha256: String,
    #[serde(deserialize_with = "required_option")]
    pub payload_frame_sha256: Option<String>,
    #[serde(deserialize_with = "required_option")]
    pub commitment: Option<String>,
    #[serde(deserialize_with = "required_option")]
    pub confirmation_basis: Option<String>,
    #[serde(deserialize_with = "required_option")]
    pub block_height: Option<u64>,
    #[serde(deserialize_with = "required_option")]
    pub recent_blockhash: Option<RecentBlockhash>,
    pub observed_at_unix_ms: u64,
    #[serde(deserialize_with = "required_option")]
    pub reason: Option<String>,
}
impl BankEvent {
    pub fn validate(&self) -> Result<()> {
        self.bank.validate()?;
        ensure!(
            self.sequence > 0
                && self.sequence <= SAFE
                && self.source_sequence > 0
                && self.source_sequence <= SAFE
                && self.observed_at_unix_ms <= SAFE
                && is_digest(&self.payload_sha256),
            "invalid_event"
        );
        let commitment = self.commitment.as_deref();
        let basis = self.confirmation_basis.as_deref();
        match self.kind.as_str() {
            "block" => ensure!(
                commitment == Some("processed")
                    && basis == Some("operator-trusted-agave-processed-bank-callback")
                    && self.payload_frame_sha256.is_none()
                    && self.block_height.is_some()
                    && self.recent_blockhash.is_none()
                    && self.reason.is_none(),
                "invalid_block_event"
            ),
            "confirmed" => ensure!(
                commitment == Some("confirmed")
                    && matches!(
                        basis,
                        Some(
                            "operator-trusted-agave-confirmed-bank-callback"
                                | "operator-trusted-agave-confirmed-ancestor"
                        )
                    )
                    && self.payload_frame_sha256.as_deref().is_some_and(is_digest)
                    && self.block_height.is_some()
                    && self.recent_blockhash.is_some()
                    && self.reason.is_none(),
                "invalid_confirmed_event"
            ),
            "root" => ensure!(
                commitment == Some("finalized")
                    && basis == Some("operator-trusted-agave-rooted-bank-callback")
                    && self.payload_frame_sha256.as_deref().is_some_and(is_digest)
                    && self.block_height.is_some()
                    && self.recent_blockhash.is_some()
                    && self.reason.is_none(),
                "invalid_root_event"
            ),
            "invalidate" => ensure!(
                commitment.is_none()
                    && basis.is_none()
                    && self.payload_frame_sha256.as_deref().is_some_and(is_digest)
                    && self.block_height.is_none()
                    && self.recent_blockhash.is_none()
                    && self
                        .reason
                        .as_deref()
                        .is_some_and(|r| !r.is_empty() && r.len() <= 1024),
                "invalid_invalidation_event"
            ),
            _ => anyhow::bail!("unsupported_event"),
        }
        ensure!(
            self.block_height.is_none_or(|h| h <= SAFE),
            "invalid_height"
        );
        if let Some(r) = &self.recent_blockhash {
            r.validate(&self.bank, self.block_height.context("missing_height")?)?;
        }
        Ok(())
    }
    pub fn matches_payload(&self, payload: &Frame) -> Result<()> {
        ensure!(
            payload.envelope.event.kind == "block"
                && self.bank == payload.envelope.event.bank
                && self.payload_sha256 == payload.envelope.event.payload_sha256
                && self.payload_frame_sha256.as_deref() == Some(&payload.sha256)
                && self
                    .block_height
                    .is_none_or(|h| Some(h) == payload.envelope.event.block_height),
            "payload_mismatch"
        );
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
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
    pub event: BankEvent,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlMarker {
    schema: String,
    state: String,
    source_profile: String,
    genesis_hash: String,
    node_identity: String,
    agave_revision: String,
    session_id: String,
    canonical: bool,
    publication_ready: bool,
    finalized: bool,
    #[serde(deserialize_with = "required_option")]
    commitment: Option<String>,
    #[serde(deserialize_with = "required_option")]
    confirmation_basis: Option<String>,
}
pub fn control_marker(source: &SourceIdentity, e: &BankEvent) -> Value {
    serde_json::to_value(ControlMarker {
        schema: "hivezilla-bank-control-v2".into(),
        state: "COMPLETE".into(),
        source_profile: "agave-bank-callbacks-v1".into(),
        genesis_hash: source.genesis_hash.clone(),
        node_identity: source.node_identity.clone(),
        agave_revision: source.agave_revision.clone(),
        session_id: source.session_id.clone(),
        canonical: false,
        publication_ready: false,
        finalized: e.kind == "root",
        commitment: e.commitment.clone(),
        confirmation_basis: e.confirmation_basis.clone(),
    })
    .unwrap()
}
#[derive(Clone, Debug)]
pub struct Frame {
    pub wire: Bytes,
    pub envelope: Envelope,
    pub sha256: String,
    pub cursor: String,
    pub source: SourceIdentity,
}
pub fn frame_length(header: &[u8]) -> Result<usize> {
    ensure!(
        header.len() >= 16 && &header[..8] == b"BZSTRM01",
        "invalid_frame_magic"
    );
    let meta = u32::from_le_bytes(header[8..12].try_into()?) as usize;
    let body = u32::from_le_bytes(header[12..16].try_into()?) as usize;
    ensure!(
        (1..=MAX_META).contains(&meta) && body <= MAX_PAYLOAD,
        "frame_too_large"
    );
    Ok(16 + meta + body)
}
pub fn encode(envelope: Envelope, payload: &[u8]) -> Result<Frame> {
    let meta = serde_json::to_vec(&envelope)?;
    ensure!(
        (1..=MAX_META).contains(&meta.len()) && payload.len() <= MAX_PAYLOAD,
        "frame_too_large"
    );
    let mut wire = Vec::with_capacity(16 + meta.len() + payload.len());
    wire.extend_from_slice(b"BZSTRM01");
    wire.extend_from_slice(&(meta.len() as u32).to_le_bytes());
    wire.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    wire.extend_from_slice(&meta);
    wire.extend_from_slice(payload);
    decode(wire.into())
}
pub fn decode(wire: Bytes) -> Result<Frame> {
    ensure!(
        frame_length(&wire)? == wire.len(),
        "incomplete_or_trailing_frame"
    );
    let n = u32::from_le_bytes(wire[8..12].try_into()?) as usize;
    let e: Envelope = serde_json::from_slice(&wire[16..16 + n])?;
    ensure!(
        e.schema == ENVELOPE && e.profile == PROFILE && !e.evidence_included,
        "unsupported_profile"
    );
    e.event.validate()?;
    let source = SourceIdentity::from_marker(&e.marker)?;
    let body = &wire[16 + n..];
    if e.event.kind == "block" {
        let m = &e.marker;
        ensure!(
            m["schema"] == "hivezilla-direct-agave-processed-compact-v1"
                && m["state"] == "COMPLETE"
                && m["source_profile"] == "agave-bank-callbacks-v1"
                && m["finalized"] == false
                && m["canonical"] == false
                && m["publication_ready"] == false,
            "invalid_processed_marker"
        );
        let blocks = m["produced_blocks"].as_array().context("missing_bank")?;
        ensure!(blocks.len() == 1, "one_bank_required");
        let b = serde_json::to_value(&e.event.bank)?;
        for k in [
            "slot",
            "bank_id",
            "blockhash",
            "parent_slot",
            "parent_blockhash",
        ] {
            ensure!(blocks[0][k] == b[k], "bank_marker_mismatch");
        }
        let mut names = BTreeSet::new();
        let mut offset = 0;
        ensure!(e.objects.len() <= FILES.len(), "too_many_objects");
        for object in &e.objects {
            ensure!(
                FILES.contains(&object.path.as_str())
                    && names.insert(object.path.as_str())
                    && object.offset == offset
                    && is_digest(&object.sha256),
                "invalid_object"
            );
            let end = offset
                .checked_add(object.bytes)
                .context("object_overflow")?;
            let bytes = body.get(offset..end).context("object_outside_payload")?;
            ensure!(digest(bytes) == object.sha256, "object_digest_mismatch");
            offset = end;
            let files = m["files"].as_array().context("missing_files")?;
            let matches: Vec<_> = files.iter().filter(|f| f["path"] == object.path).collect();
            ensure!(
                matches.len() == 1
                    && matches[0]["bytes"] == object.bytes
                    && matches[0]["sha256"] == object.sha256,
                "marker_object_mismatch"
            );
        }
        ensure!(
            offset == body.len() && REQUIRED.iter().all(|name| names.contains(name)),
            "incomplete_block_objects"
        );
    } else {
        let marker: ControlMarker = serde_json::from_value(e.marker.clone())?;
        ensure!(
            marker.schema == "hivezilla-bank-control-v2"
                && marker.state == "COMPLETE"
                && marker.source_profile == "agave-bank-callbacks-v1"
                && !marker.canonical
                && !marker.publication_ready
                && marker.finalized == (e.event.kind == "root")
                && marker.commitment == e.event.commitment
                && marker.confirmation_basis == e.event.confirmation_basis
                && e.objects.is_empty()
                && body.is_empty(),
            "invalid_control_marker"
        );
    }
    let sha256 = digest(&wire);
    let cursor = cursor(&source.session_id, e.event.sequence, &sha256);
    Ok(Frame {
        wire,
        envelope: e,
        sha256,
        cursor,
        source,
    })
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ConfirmedObservation {
    pub bank: BankIdentity,
    pub block_height: u64,
    pub payload_sha256: String,
    pub payload_frame_sha256: String,
    pub recent_blockhash: RecentBlockhash,
    pub observed_at_unix_ms: u64,
    pub commitment: String,
    pub event_sequence: u64,
    pub event_cursor: String,
}
impl ConfirmedObservation {
    pub fn from_frame(f: &Frame) -> Result<Self> {
        let e = &f.envelope.event;
        ensure!(
            matches!(e.kind.as_str(), "confirmed" | "root"),
            "not_confirmed"
        );
        Ok(Self {
            bank: e.bank.clone(),
            block_height: e.block_height.context("missing_height")?,
            payload_sha256: e.payload_sha256.clone(),
            payload_frame_sha256: e
                .payload_frame_sha256
                .clone()
                .context("missing_payload_frame")?,
            recent_blockhash: e.recent_blockhash.clone().context("missing_expiry")?,
            observed_at_unix_ms: e.observed_at_unix_ms,
            commitment: e.commitment.clone().context("missing_commitment")?,
            event_sequence: e.sequence,
            event_cursor: f.cursor.clone(),
        })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Head {
    pub schema: String,
    pub profile: String,
    pub source: SourceIdentity,
    #[serde(deserialize_with = "required_option")]
    pub cursor: Option<String>,
    #[serde(deserialize_with = "required_option")]
    pub earliest_after: Option<String>,
    pub status: String,
    pub sampled_at_unix_ms: u64,
    #[serde(deserialize_with = "required_option")]
    pub source_observed_at_unix_ms: Option<u64>,
    #[serde(deserialize_with = "required_option")]
    pub source_lag_ms: Option<u64>,
    #[serde(deserialize_with = "required_option")]
    pub latest_confirmed: Option<ConfirmedObservation>,
}
impl Head {
    /// Validate the factual sample. A delayed live sample stays admissible for
    /// replay; consumers check current age separately before usable readiness.
    pub fn validate(&self, expected: &SourceIdentity, now: u64, max_lag: u64) -> Result<()> {
        ensure!(
            self.schema == HEAD && self.profile == PROFILE && &self.source == expected,
            "source_changed"
        );
        self.source.validate()?;
        clock_check(self.sampled_at_unix_ms, now)?;
        ensure!(
            matches!(
                self.status.as_str(),
                "starting" | "live" | "stale" | "failed"
            ),
            "invalid_status"
        );
        let parse = |s: &str| -> Result<u64> {
            let (session, n, _) = parse_cursor(s)?;
            ensure!(session == self.source.session_id, "source_changed");
            Ok(n)
        };
        let current = self.cursor.as_deref().map(parse).transpose()?;
        let base = self.earliest_after.as_deref().map(parse).transpose()?;
        ensure!(
            base.zip(current).is_none_or(|(a, b)| a <= b),
            "invalid_replay_boundary"
        );
        if let Some(c) = &self.latest_confirmed {
            c.bank.validate()?;
            c.recent_blockhash.validate(&c.bank, c.block_height)?;
            clock_check(c.observed_at_unix_ms, now)?;
            ensure!(
                c.block_height <= SAFE
                    && is_digest(&c.payload_sha256)
                    && is_digest(&c.payload_frame_sha256)
                    && matches!(c.commitment.as_str(), "confirmed" | "finalized")
                    && c.event_sequence > 0
                    && parse(&c.event_cursor)? == c.event_sequence
                    && is_digest(parse_cursor(&c.event_cursor)?.2)
                    && current.is_some_and(|n| c.event_sequence <= n)
                    && self.source_observed_at_unix_ms == Some(c.observed_at_unix_ms)
                    && self.source_lag_ms
                        == Some(
                            self.sampled_at_unix_ms
                                .saturating_sub(c.observed_at_unix_ms)
                        ),
                "invalid_confirmed_head"
            );
        } else {
            ensure!(
                self.source_observed_at_unix_ms.is_none()
                    && self.source_lag_ms.is_none()
                    && self.status != "live",
                "false_live_head"
            );
        }
        if self.status == "live" {
            ensure!(
                current.is_some()
                    && base.is_some()
                    && self.source_lag_ms.is_some_and(|n| n <= max_lag),
                "stale_live_head"
            );
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Notice {
    pub cursor: String,
    pub sha256: String,
    pub bytes: usize,
    pub start_slot: u64,
    pub end_slot: u64,
    pub url: String,
}
impl Notice {
    pub fn preflight(
        &self,
        id: &str,
        expected_sequence: u64,
        source: &SourceIdentity,
    ) -> Result<()> {
        let (session, n, sha) = parse_cursor(&self.cursor)?;
        ensure!(
            id == self.cursor
                && session == source.session_id
                && n == expected_sequence
                && sha == self.sha256
                && is_digest(sha)
                && (17..=MAX_FRAME).contains(&self.bytes)
                && self.start_slot <= SAFE
                && self.end_slot == self.start_slot
                && self.url == format!("/v2/banks/chunks/{sha}"),
            "invalid_notice"
        );
        Ok(())
    }
    pub fn from_frame(f: &Frame) -> Self {
        Self {
            cursor: f.cursor.clone(),
            sha256: f.sha256.clone(),
            bytes: f.wire.len(),
            start_slot: f.envelope.event.bank.slot,
            end_slot: f.envelope.event.bank.slot,
            url: format!("/v2/banks/chunks/{}", f.sha256),
        }
    }
    pub fn validate(
        &self,
        id: &str,
        frame: &Frame,
        expected_sequence: u64,
        source: &SourceIdentity,
    ) -> Result<()> {
        ensure!(
            id == self.cursor
                && self.cursor == frame.cursor
                && self.sha256 == frame.sha256
                && self.bytes == frame.wire.len()
                && self.start_slot == frame.envelope.event.bank.slot
                && self.end_slot == self.start_slot
                && self.url == format!("/v2/banks/chunks/{}", frame.sha256)
                && &frame.source == source
                && frame.envelope.event.sequence == expected_sequence,
            "invalid_notice"
        );
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub mod artifact;
pub mod chain;
pub mod client;
#[cfg(any(test, feature = "test-fixtures"))]
pub mod fixtures;
#[cfg(test)]
mod tests;
