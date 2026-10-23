//! Immutable, source-bound native footer facts. This profile is independent of
//! execution Frame v2. Native bytes use the pinned Agave 4.3.2 grammar; the view
//! preserves observations, not a claim that a certificate or BLS signature was verified.
use crate::{BankIdentity, SAFE, SourceIdentity, is_digest};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize, Serializer};

pub const PROFILE: &str = "native-consensus-fact-v1";
pub const NATIVE_REVISION: &str = "87167f3906bdf5111e99ead64d8f4098b7874e33";
pub const OFFICIAL_REVISION: &str = "a203300045b405f86c9c84308b2eae76af69ac3b";
pub const NATIVE_SCHEMA: &str = "agave-4.3.2-versioned-block-footer-v1";
pub const VERIFICATION: &str = "captured_not_verified";
pub const MAGIC: &[u8; 8] = b"BZCNF001";
pub const VERSION: u16 = 1;
pub const MAX_BINDING_BYTES: usize = 4096;
/// Agave's enclosing LengthPrefixed block marker has a u16 byte length.
pub const MAX_FOOTER_BYTES: usize = u16::MAX as usize;
pub const HEADER_BYTES: usize = 20;
pub const MAX_FACT_BYTES: usize = HEADER_BYTES + MAX_BINDING_BYTES + MAX_FOOTER_BYTES;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FactBinding {
    pub source: SourceIdentity,
    pub bank: BankIdentity,
    pub execution_payload_sha256: String,
    pub source_event_sequence: u64,
    pub source_event_sha256: String,
}
impl FactBinding {
    pub fn validate(&self) -> Result<()> {
        self.source.validate()?;
        self.bank.validate()?;
        ensure!(
            (self.source.agave_revision == NATIVE_REVISION
                || self.source.agave_revision == OFFICIAL_REVISION),
            "unsupported_native_footer_revision"
        );
        ensure!(
            (1..=SAFE).contains(&self.source_event_sequence)
                && is_digest(&self.execution_payload_sha256)
                && is_digest(&self.source_event_sha256),
            "invalid_consensus_fact_binding"
        );
        Ok(())
    }
}

/// The exact native footer occurs once in the stored fact. Its parsed view is
/// borrowed on demand. The event digest is a binding to external captured
/// evidence, not a digest of the footer alone; the exporter must check that evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsensusFact {
    pub binding: FactBinding,
    footer_wire: Vec<u8>,
}
impl ConsensusFact {
    pub fn new(binding: FactBinding, footer_wire: Vec<u8>) -> Result<Self> {
        binding.validate()?;
        decode_footer(&footer_wire)?;
        Ok(Self {
            binding,
            footer_wire,
        })
    }
    pub fn footer_wire(&self) -> &[u8] {
        &self.footer_wire
    }
    pub fn footer(&self) -> Result<FooterView<'_>> {
        decode_footer(&self.footer_wire)
    }
    pub fn validate_binding(&self, expected: &FactBinding) -> Result<()> {
        self.binding.validate()?;
        expected.validate()?;
        ensure!(&self.binding == expected, "consensus_fact_binding_mismatch");
        Ok(())
    }
}

/// Fixed little-endian header: magic8, portable version u16, native grammar
/// code u16 (=1), binding JSON length u32, native footer length u32. JSON
/// carries only the binding. The footer is exact pinned native Wincode bytes.
pub fn encode(fact: &ConsensusFact) -> Result<Vec<u8>> {
    fact.binding.validate()?;
    decode_footer(fact.footer_wire())?;
    let binding = serde_json::to_vec(&fact.binding)?;
    ensure!(
        !binding.is_empty() && binding.len() <= MAX_BINDING_BYTES,
        "consensus_binding_too_large"
    );
    let mut output = Vec::with_capacity(HEADER_BYTES + binding.len() + fact.footer_wire.len());
    output.extend_from_slice(MAGIC);
    output.extend_from_slice(&VERSION.to_le_bytes());
    output.extend_from_slice(&1_u16.to_le_bytes());
    output.extend_from_slice(&(binding.len() as u32).to_le_bytes());
    output.extend_from_slice(&(fact.footer_wire.len() as u32).to_le_bytes());
    output.extend_from_slice(&binding);
    output.extend_from_slice(fact.footer_wire());
    Ok(output)
}
pub fn decode(bytes: &[u8]) -> Result<ConsensusFact> {
    ensure!(bytes.len() <= MAX_FACT_BYTES, "consensus_fact_too_large");
    let mut c = Cursor::new(bytes);
    ensure!(c.take(8)? == MAGIC, "invalid_consensus_fact_magic");
    ensure!(c.u16()? == VERSION, "unsupported_consensus_fact_version");
    ensure!(c.u16()? == 1, "unsupported_native_footer_schema");
    let binding_len = c.u32()? as usize;
    let footer_len = c.u32()? as usize;
    ensure!(
        (1..=MAX_BINDING_BYTES).contains(&binding_len)
            && (1..=MAX_FOOTER_BYTES).contains(&footer_len),
        "invalid_consensus_fact_lengths"
    );
    let expected = HEADER_BYTES
        .checked_add(binding_len)
        .and_then(|n| n.checked_add(footer_len))
        .context("consensus_fact_length_overflow")?;
    ensure!(bytes.len() == expected, "consensus_fact_length_mismatch");
    let binding: FactBinding = serde_json::from_slice(c.take(binding_len)?)?;
    binding.validate()?;
    let footer = c.take(footer_len)?;
    decode_footer(footer)?; // Validate all lengths before copying the one raw value.
    c.end()?;
    ConsensusFact::new(binding, footer.to_vec())
}
pub fn decode_bound(bytes: &[u8], expected: &FactBinding) -> Result<ConsensusFact> {
    let value = decode(bytes)?;
    value.validate_binding(expected)?;
    Ok(value)
}

fn decimal_u64<S: Serializer>(value: &u64, serializer: S) -> std::result::Result<S::Ok, S::Error> {
    serializer.serialize_str(&value.to_string())
}
/// A Votor consensus target identity. It is deliberately a different type
/// from an execution bank hash or a recent blockhash.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct ConsensusBlockId(pub [u8; 32]);

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Aggregate<'a> {
    /// Exactly 96 captured compressed BLS bytes. Not cryptographically verified.
    pub signature: &'a [u8],
    pub bitmap: &'a [u8],
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct FinalizationCertificate<'a> {
    #[serde(serialize_with = "decimal_u64")]
    pub slot: u64,
    /// Consensus BlockId targeted by this certificate, not its carrier's ID.
    pub block_id: ConsensusBlockId,
    pub final_aggregate: Aggregate<'a>,
    pub notar_aggregate: Option<Aggregate<'a>>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SkipRewardCertificate<'a> {
    #[serde(serialize_with = "decimal_u64")]
    pub slot: u64,
    pub aggregate: Aggregate<'a>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NotarRewardCertificate<'a> {
    #[serde(serialize_with = "decimal_u64")]
    pub slot: u64,
    pub block_id: ConsensusBlockId,
    pub aggregate: Aggregate<'a>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct FooterView<'a> {
    pub native_schema: &'static str,
    pub verification: &'static str,
    /// Execution bank hash from the footer; not the consensus BlockId or the
    /// BankIdentity recent blockhash. No equality between those is inferred.
    pub bank_hash: [u8; 32],
    #[serde(serialize_with = "decimal_u64")]
    pub block_producer_time_nanos: u64,
    pub block_user_agent: &'a [u8],
    pub block_final_cert: Option<FinalizationCertificate<'a>>,
    /// Reward aggregates are retained separately from finalization certificates.
    pub skip_reward_cert: Option<SkipRewardCertificate<'a>>,
    pub notar_reward_cert: Option<NotarRewardCertificate<'a>>,
}

/// Decode exactly VersionedBlockFooter::V1 as serialized by the pinned plugin.
/// No outer BlockMarker tag/length prefix, PoH inference, trial schema decode,
/// current-bank consensus ID or signature verification is introduced.
pub fn decode_footer(bytes: &[u8]) -> Result<FooterView<'_>> {
    ensure!(
        !bytes.is_empty() && bytes.len() <= MAX_FOOTER_BYTES,
        "native_footer_size_invalid"
    );
    let mut c = Cursor::new(bytes);
    ensure!(c.u8()? == 1, "unsupported_native_footer_version");
    let bank_hash = c.hash()?;
    let block_producer_time_nanos = c.u64()?;
    let user_agent_len = c.u8()? as usize;
    let block_user_agent = c.take(user_agent_len)?;
    let block_final_cert = if c.option()? {
        let slot = c.u64()?;
        let block_id = ConsensusBlockId(c.hash()?);
        let final_aggregate = c.aggregate(false)?;
        let notar_aggregate = if c.option()? {
            Some(c.aggregate(false)?)
        } else {
            None
        };
        Some(FinalizationCertificate {
            slot,
            block_id,
            final_aggregate,
            notar_aggregate,
        })
    } else {
        None
    };
    let skip_reward_cert = if c.option()? {
        Some(SkipRewardCertificate {
            slot: c.u64()?,
            aggregate: c.aggregate(true)?,
        })
    } else {
        None
    };
    let notar_reward_cert = if c.option()? {
        Some(NotarRewardCertificate {
            slot: c.u64()?,
            block_id: ConsensusBlockId(c.hash()?),
            aggregate: c.aggregate(true)?,
        })
    } else {
        None
    };
    c.end()?;
    Ok(FooterView {
        native_schema: NATIVE_SCHEMA,
        verification: VERIFICATION,
        bank_hash,
        block_producer_time_nanos,
        block_user_agent,
        block_final_cert,
        skip_reward_cert,
        notar_reward_cert,
    })
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(n)
            .context("native_footer_length_overflow")?;
        let value = self
            .bytes
            .get(self.position..end)
            .context("truncated_consensus_fact")?;
        self.position = end;
        Ok(value)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into()?))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into()?))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into()?))
    }
    fn hash(&mut self) -> Result<[u8; 32]> {
        Ok(self.take(32)?.try_into()?)
    }
    fn option(&mut self) -> Result<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => anyhow::bail!("invalid_native_footer_option"),
        }
    }
    fn short_u16(&mut self) -> Result<usize> {
        let a = self.u8()?;
        if a < 128 {
            return Ok(a as usize);
        }
        let b = self.u8()?;
        if b < 128 {
            ensure!(b != 0, "noncanonical_short_u16");
            return Ok((a as usize & 127) | ((b as usize) << 7));
        }
        let d = self.u8()?;
        ensure!((1..=3).contains(&d), "invalid_short_u16");
        Ok((a as usize & 127) | ((b as usize & 127) << 7) | ((d as usize) << 14))
    }
    fn aggregate(&mut self, short: bool) -> Result<Aggregate<'a>> {
        let signature = self.take(96)?;
        let len = if short {
            self.short_u16()?
        } else {
            self.u16()? as usize
        };
        let bitmap = self.take(len)?;
        Ok(Aggregate { signature, bitmap })
    }
    fn end(&self) -> Result<()> {
        ensure!(
            self.position == self.bytes.len(),
            "trailing_consensus_fact_bytes"
        );
        Ok(())
    }
}
