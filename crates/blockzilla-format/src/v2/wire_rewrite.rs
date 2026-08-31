//! Allocation-light wire transformers for Archive V2 hot transaction payloads.
//!
//! These functions decode one field and write its canonical representation immediately. They do
//! not build a message or metadata object graph, and they do not run a separate encoded-size pass.
//! Length-delimited byte bodies borrow the input until they are copied to the output.

use std::{error::Error as StdError, fmt};

use anyhow::anyhow;
use wincode::{
    SchemaRead, SchemaWrite,
    io::{Reader, Writer},
    len::SeqLen,
};

use crate::CompactPubkey;

type ArchiveV2WireBoundedConfig = wincode::config::Configuration<
    true,
    ARCHIVE_V2_WIRE_REWRITE_MAX_FRAME_BYTES,
    wincode::len::BincodeLen,
    wincode::int_encoding::LittleEndian,
    crate::Leb128,
>;

/// Default maximum accepted or produced size for one hot block payload.
pub const ARCHIVE_V2_WIRE_REWRITE_MAX_FRAME_BYTES: usize = 512 << 20;
/// Default maximum number of sequence items or typed pubkey references in one transformation.
pub const ARCHIVE_V2_WIRE_REWRITE_MAX_ITEMS: usize = 4 << 20;

// `put_small` is used only for scalar/tag/length values whose canonical wire representation is at
// most 32 bytes. Checking every scalar write showed up in the hot loop, so check once per 256
// writes instead. A rejected transformation can therefore extend the output by at most 8 KiB past
// `max_output_bytes` before it is rolled back. Variable byte bodies, pubkey expansions, and owned
// leaves use exact checks and are not part of this allowance.
const MAX_SMALL_PUT_WIRE_BYTES: usize = 32;
const MAX_TRANSIENT_SMALL_PUT_OVERSHOOT_BYTES: usize = 8 << 10;
const MAX_UNCHECKED_SMALL_PUTS: u16 =
    (MAX_TRANSIENT_SMALL_PUT_OVERSHOOT_BYTES / MAX_SMALL_PUT_WIRE_BYTES) as u16;

/// Registry-count class for one typed `CompactPubkey` reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveV2WireReferenceClass {
    /// A reference that contributes to the usage-sorted registry count.
    Eligible,
    /// A reference that is retained and remapped but does not contribute to that count.
    Excluded,
}

/// A reason that the allocation-light transformer asks its caller to use the owned fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveV2WireFallbackReason {
    /// Metadata has `err=Some`; current and legacy error schemas require the ambiguity-safe path.
    MetadataHasTransactionError,
    /// A present transaction error has valid current and legacy prefixes. The caller must apply
    /// the full value-level ambiguity rule before it selects either schema.
    MetadataErrorSchemaAmbiguous,
    /// Neither present-error prefix can be selected safely without the owned decoder.
    MetadataErrorPrefixRequiresOwnedFallback,
    /// A selected present-error wire path failed after it started. The transaction was rolled
    /// back, so the caller can retry with the owned decoder.
    MetadataErrorWireRollback,
    /// The message enum tag is not part of the current Archive V2 hot schema.
    UnsupportedMessageVariant(u32),
    /// The hot instruction-data enum tag is not part of the current schema.
    UnsupportedInstructionVariant(u32),
    /// A log enum tag is not part of the current schema.
    UnsupportedLogVariant { schema: &'static str, tag: u32 },
}

/// Stable category for a wire-transform failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveV2WireRewriteErrorKind {
    Fallback(ArchiveV2WireFallbackReason),
    InvalidInput,
    LimitExceeded,
    Visitor,
}

/// Error returned by an Archive V2 wire transformer.
#[derive(Debug)]
pub struct ArchiveV2WireRewriteError {
    kind: ArchiveV2WireRewriteErrorKind,
    detail: anyhow::Error,
}

impl ArchiveV2WireRewriteError {
    #[inline]
    pub fn kind(&self) -> ArchiveV2WireRewriteErrorKind {
        self.kind
    }

    #[inline]
    pub fn fallback_reason(&self) -> Option<ArchiveV2WireFallbackReason> {
        match self.kind {
            ArchiveV2WireRewriteErrorKind::Fallback(reason) => Some(reason),
            _ => None,
        }
    }

    fn invalid(context: &'static str, error: impl fmt::Display) -> Self {
        Self {
            kind: ArchiveV2WireRewriteErrorKind::InvalidInput,
            detail: anyhow!("{context}: {error}"),
        }
    }

    fn invalid_value(detail: impl fmt::Display) -> Self {
        Self {
            kind: ArchiveV2WireRewriteErrorKind::InvalidInput,
            detail: anyhow!("{detail}"),
        }
    }

    fn limit(detail: impl fmt::Display) -> Self {
        Self {
            kind: ArchiveV2WireRewriteErrorKind::LimitExceeded,
            detail: anyhow!("{detail}"),
        }
    }

    fn visitor(error: anyhow::Error) -> Self {
        Self {
            kind: ArchiveV2WireRewriteErrorKind::Visitor,
            detail: error,
        }
    }

    fn fallback(reason: ArchiveV2WireFallbackReason) -> Self {
        Self {
            kind: ArchiveV2WireRewriteErrorKind::Fallback(reason),
            detail: anyhow!("allocation-light wire rewrite requires owned fallback: {reason:?}"),
        }
    }
}

impl fmt::Display for ArchiveV2WireRewriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.detail.fmt(f)
    }
}

impl StdError for ArchiveV2WireRewriteError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.detail.source()
    }
}

pub type ArchiveV2WireRewriteResult<T> = Result<T, ArchiveV2WireRewriteError>;

/// Per-record safety limits for the wire transformer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveV2WireRewriteLimits {
    pub max_input_bytes: usize,
    pub max_output_bytes: usize,
    pub max_sequence_items: usize,
    pub max_pubkey_references: usize,
}

impl Default for ArchiveV2WireRewriteLimits {
    fn default() -> Self {
        Self {
            max_input_bytes: ARCHIVE_V2_WIRE_REWRITE_MAX_FRAME_BYTES,
            max_output_bytes: ARCHIVE_V2_WIRE_REWRITE_MAX_FRAME_BYTES,
            // This matches wincode's one-byte charge for a zero-sized sequence element. The
            // type-specific preallocation check below applies the tighter bound for larger types.
            max_sequence_items: ARCHIVE_V2_WIRE_REWRITE_MAX_FRAME_BYTES,
            max_pubkey_references: ARCHIVE_V2_WIRE_REWRITE_MAX_ITEMS,
        }
    }
}

/// Counts from one successful transformation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ArchiveV2WireRewriteStats {
    pub input_bytes: usize,
    pub output_bytes: usize,
    /// Counts of decoded source instruction-data tags `0..=8`.
    ///
    /// For a Pre-to-Post transcode, only entries `0..=6` can be nonzero. These are the
    /// historical source tags, before the transcoder maps tags `1..=6` to `3..=8`.
    pub source_instruction_data_tag_counts: [u64; 9],
    pub eligible_pubkey_references: usize,
    pub excluded_pubkey_references: usize,
    pub recent_blockhash_ids: usize,
    pub vote_hash_block_ids: usize,
    /// The selected source schema when metadata contains a transaction error.
    pub metadata_error_schema: Option<ArchiveV2WireMetadataErrorSchema>,
}

/// Source schema selected for a present metadata transaction error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveV2WireMetadataErrorSchema {
    Current,
    Legacy,
}

/// Transactional callbacks used while a message or metadata record is transformed.
///
/// The transformer takes a checkpoint before it writes or emits an event. On every error it
/// truncates the output and calls `rollback`. `commit` is called only after exact input
/// consumption and all limits are verified. Implementations can therefore update count vectors
/// and access-reference buffers directly without keeping a second per-reference journal.
pub trait ArchiveV2WireRewriteVisitor {
    type Checkpoint;

    fn checkpoint(&mut self) -> Self::Checkpoint;

    fn rewrite_pubkey(
        &mut self,
        pubkey: CompactPubkey,
        class: ArchiveV2WireReferenceClass,
    ) -> anyhow::Result<CompactPubkey>;

    fn recent_blockhash_id(&mut self, _id: i32) -> anyhow::Result<()> {
        Ok(())
    }

    /// Called for vote-state hashes and TowerSync block-id hashes. Switch-proof hashes are parsed
    /// and preserved but are intentionally not emitted.
    fn vote_hash_block_id(&mut self, _block_id: u32) -> anyhow::Result<()> {
        Ok(())
    }

    fn rollback(&mut self, checkpoint: Self::Checkpoint);

    fn commit(&mut self, _checkpoint: Self::Checkpoint) {}
}

/// Identity visitor for callers that need the bounded, exact schema selector
/// and canonical rewrite without changing registry references.
#[derive(Debug, Default, Clone, Copy)]
pub struct ArchiveV2WireIdentityVisitor;

impl ArchiveV2WireRewriteVisitor for ArchiveV2WireIdentityVisitor {
    type Checkpoint = ();

    fn checkpoint(&mut self) -> Self::Checkpoint {}

    fn rewrite_pubkey(
        &mut self,
        pubkey: CompactPubkey,
        _class: ArchiveV2WireReferenceClass,
    ) -> anyhow::Result<CompactPubkey> {
        Ok(pubkey)
    }

    fn rollback(&mut self, _checkpoint: Self::Checkpoint) {}
}

#[derive(Debug, SchemaRead)]
struct LegacyArchiveV2CompactMetaV1 {
    err: Option<Vec<u8>>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
    inner_instructions: Option<Vec<crate::CompactInnerInstructions>>,
    logs: Option<crate::CompactLogStream>,
    pre_token_balances: Vec<crate::CompactTokenBalance>,
    post_token_balances: Vec<crate::CompactTokenBalance>,
    rewards: Vec<crate::CompactReward>,
    loaded_writable_addresses: Vec<CompactPubkey>,
    loaded_readonly_addresses: Vec<CompactPubkey>,
    return_data: Option<crate::CompactReturnData>,
    compute_units_consumed: Option<u64>,
    cost_units: Option<u64>,
}

impl LegacyArchiveV2CompactMetaV1 {
    fn into_current(self) -> anyhow::Result<crate::CompactMetaV1> {
        Ok(crate::CompactMetaV1 {
            err: self
                .err
                .as_deref()
                .map(crate::CompactTransactionError::from_stored_wincode_bytes)
                .transpose()?,
            fee: self.fee,
            pre_balances: self.pre_balances,
            post_balances: self.post_balances,
            inner_instructions: self.inner_instructions,
            logs: self.logs,
            pre_token_balances: self.pre_token_balances,
            post_token_balances: self.post_token_balances,
            rewards: self.rewards,
            loaded_writable_addresses: self.loaded_writable_addresses,
            loaded_readonly_addresses: self.loaded_readonly_addresses,
            return_data: self.return_data,
            compute_units_consumed: self.compute_units_consumed,
            cost_units: self.cost_units,
        })
    }
}

/// Decode one complete current-or-legacy metadata value, apply the full
/// value-level ambiguity rule, and return canonical current-schema bytes.
///
/// This is the bounded fallback for the uncommon `err=Some` path. Successful
/// metadata should continue to use the streaming transformer.
pub fn canonicalize_archive_v2_metadata_owned(
    input: &[u8],
) -> anyhow::Result<(Vec<u8>, ArchiveV2WireMetadataErrorSchema)> {
    anyhow::ensure!(
        input.len() <= crate::ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES,
        "metadata input exceeds the Archive V2 owned-fallback limit"
    );
    let current = wincode::config::deserialize_exact::<crate::CompactMetaV1, _>(
        input,
        crate::bounded_wincode_leb128_config::<
            { crate::ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES },
        >(),
    );
    let legacy = wincode::config::deserialize_exact::<LegacyArchiveV2CompactMetaV1, _>(
        input,
        crate::bounded_wincode_leb128_config::<
            { crate::ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES },
        >(),
    )
    .map_err(anyhow::Error::from)
    .and_then(LegacyArchiveV2CompactMetaV1::into_current);
    let current_error = current.as_ref().err().map(ToString::to_string);
    let legacy_error = legacy.as_ref().err().map(ToString::to_string);

    let (value, schema) = match (current.ok(), legacy.ok()) {
        (Some(current), None) => (current, ArchiveV2WireMetadataErrorSchema::Current),
        (None, Some(legacy)) => (legacy, ArchiveV2WireMetadataErrorSchema::Legacy),
        (Some(current), Some(legacy)) => {
            let current_bytes =
                wincode::config::serialize(&current, crate::wincode_leb128_config())?;
            let legacy_bytes = wincode::config::serialize(&legacy, crate::wincode_leb128_config())?;
            anyhow::ensure!(
                current_bytes == legacy_bytes,
                "ambiguous compact metadata decodes as different current and legacy values"
            );
            return Ok((current_bytes, ArchiveV2WireMetadataErrorSchema::Current));
        }
        (None, None) => {
            return Err(anyhow!(
                "compact metadata is neither current nor legacy: current={}; legacy={}",
                current_error.as_deref().unwrap_or("unknown error"),
                legacy_error.as_deref().unwrap_or("unknown error")
            ));
        }
    };
    let output = wincode::config::serialize(&value, crate::wincode_leb128_config())?;
    anyhow::ensure!(
        output.len() <= crate::ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES,
        "canonical metadata output exceeds the Archive V2 owned-fallback limit"
    );
    Ok((output, schema))
}

/// Transform one current-schema `ArchiveV2HotMessagePayload` wire value.
pub fn rewrite_archive_v2_hot_message_wire<V: ArchiveV2WireRewriteVisitor>(
    input: &[u8],
    output: &mut Vec<u8>,
    visitor: &mut V,
    limits: ArchiveV2WireRewriteLimits,
) -> ArchiveV2WireRewriteResult<ArchiveV2WireRewriteStats> {
    transform_transactionally::<_, false, false>(input, output, visitor, limits, WireValue::Message)
}

/// Transform one historical, pre-unknown-fallback `ArchiveV2HotMessagePayload` wire value.
///
/// This grammar has `Raw` at instruction-data tag 0 and the semantic variants at tags 1 through
/// 6. It preserves those historical tags in the output. Profile selection belongs at the
/// generation boundary; callers must not use this function as a per-message fallback.
pub fn rewrite_archive_v2_hot_message_wire_pre_unknown_fallbacks<V: ArchiveV2WireRewriteVisitor>(
    input: &[u8],
    output: &mut Vec<u8>,
    visitor: &mut V,
    limits: ArchiveV2WireRewriteLimits,
) -> ArchiveV2WireRewriteResult<ArchiveV2WireRewriteStats> {
    transform_transactionally::<_, true, false>(input, output, visitor, limits, WireValue::Message)
}

/// Transcode one historical, pre-unknown-fallback message to the canonical current wire grammar.
///
/// The source grammar is exact and authoritative: this function never probes or retries the
/// current grammar. It maps historical instruction-data tags `0..=6` to canonical tags
/// `0, 3..=8` while streaming each payload through the same bounded field decoder used by the
/// wire rewriter. Unknown tags, malformed fields, trailing input, and limit violations fail the
/// complete transaction and restore both `output` and `visitor` to their checkpoints.
///
/// Profile admission belongs at the generation boundary. Callers must use this function only for
/// a generation that was independently admitted as
/// `PreUnknownInstructionFallbacksV1`; it is not a message-level profile detector.
pub fn transcode_archive_v2_hot_message_wire_pre_to_post<V: ArchiveV2WireRewriteVisitor>(
    input: &[u8],
    output: &mut Vec<u8>,
    visitor: &mut V,
    limits: ArchiveV2WireRewriteLimits,
) -> ArchiveV2WireRewriteResult<ArchiveV2WireRewriteStats> {
    transform_transactionally::<_, true, true>(input, output, visitor, limits, WireValue::Message)
}

/// Transform one successful (`err=None`) current-schema `CompactMetaV1` wire value.
///
/// `err=Some` returns `MetadataHasTransactionError` without consuming caller state. The caller
/// must use its current-vs-legacy ambiguity-safe decoder for that uncommon path.
pub fn rewrite_archive_v2_successful_metadata_wire<V: ArchiveV2WireRewriteVisitor>(
    input: &[u8],
    output: &mut Vec<u8>,
    visitor: &mut V,
    limits: ArchiveV2WireRewriteLimits,
) -> ArchiveV2WireRewriteResult<ArchiveV2WireRewriteStats> {
    transform_transactionally::<_, false, false>(
        input,
        output,
        visitor,
        limits,
        WireValue::SuccessfulMetadata,
    )
}

/// Transform one current- or legacy-schema `CompactMetaV1` wire value.
///
/// Successful metadata uses the common current tail. For `err=Some`, the transformer validates
/// the current compact-error prefix and the legacy stored-error prefix without decoding the
/// metadata object graph. One valid prefix selects the streaming path. Two valid prefixes return
/// `MetadataErrorSchemaAmbiguous`, so the caller can apply the existing full value-level
/// ambiguity rule. A failure after selection is transactional and returns
/// `MetadataErrorWireRollback`, so the caller can retry with its owned decoder.
pub fn rewrite_archive_v2_metadata_wire<V: ArchiveV2WireRewriteVisitor>(
    input: &[u8],
    output: &mut Vec<u8>,
    visitor: &mut V,
    limits: ArchiveV2WireRewriteLimits,
) -> ArchiveV2WireRewriteResult<ArchiveV2WireRewriteStats> {
    let has_error = input.first() == Some(&1);
    match transform_transactionally::<_, false, false>(
        input,
        output,
        visitor,
        limits,
        WireValue::Metadata,
    ) {
        Ok(stats) => Ok(stats),
        Err(error)
            if has_error
                && !matches!(
                    error.kind(),
                    ArchiveV2WireRewriteErrorKind::Fallback(
                        ArchiveV2WireFallbackReason::MetadataErrorSchemaAmbiguous
                            | ArchiveV2WireFallbackReason::MetadataErrorPrefixRequiresOwnedFallback
                            | ArchiveV2WireFallbackReason::UnsupportedLogVariant { .. }
                    )
                ) =>
        {
            Err(ArchiveV2WireRewriteError::fallback(
                ArchiveV2WireFallbackReason::MetadataErrorWireRollback,
            ))
        }
        Err(error) => Err(error),
    }
}

/// Transform one current- or legacy-schema metadata value while preserving
/// the selected transaction-error prefix byte-for-byte.
///
/// This is for portable dump consolidation where only typed
/// [`CompactPubkey`] references may change. An ambiguous `err=Some` prefix
/// still requests an owned or source-profile decision. Callers must not guess
/// its schema at record scope.
pub fn rewrite_archive_v2_metadata_wire_preserving_error_schema<V: ArchiveV2WireRewriteVisitor>(
    input: &[u8],
    output: &mut Vec<u8>,
    visitor: &mut V,
    limits: ArchiveV2WireRewriteLimits,
) -> ArchiveV2WireRewriteResult<ArchiveV2WireRewriteStats> {
    transform_transactionally::<_, false, false>(
        input,
        output,
        visitor,
        limits,
        WireValue::MetadataPreserveErrorSchema(None),
    )
}

/// Transform one metadata value while preserving the already selected
/// transaction-error schema and its source prefix byte-for-byte.
///
/// This entry point does not probe the other error schema. It is for callers
/// that selected the schema from a trusted generation profile or from a
/// complete value-level ambiguity check.
pub fn rewrite_archive_v2_metadata_wire_preserving_selected_error_schema<
    V: ArchiveV2WireRewriteVisitor,
>(
    input: &[u8],
    output: &mut Vec<u8>,
    visitor: &mut V,
    limits: ArchiveV2WireRewriteLimits,
    schema: ArchiveV2WireMetadataErrorSchema,
) -> ArchiveV2WireRewriteResult<ArchiveV2WireRewriteStats> {
    transform_transactionally::<_, false, false>(
        input,
        output,
        visitor,
        limits,
        WireValue::MetadataPreserveErrorSchema(Some(schema)),
    )
}

/// Transform one metadata value whose current typed-error schema was already
/// selected by generation admission or by the owned ambiguity rule.
///
/// This entry point is for callers that first use
/// [`canonicalize_archive_v2_metadata_owned`] on an ambiguous historical
/// `err=Some` value. It does not probe the legacy stored-error prefix. The
/// output always uses the current typed-error schema.
pub fn rewrite_archive_v2_current_metadata_wire<V: ArchiveV2WireRewriteVisitor>(
    input: &[u8],
    output: &mut Vec<u8>,
    visitor: &mut V,
    limits: ArchiveV2WireRewriteLimits,
) -> ArchiveV2WireRewriteResult<ArchiveV2WireRewriteStats> {
    transform_transactionally::<_, false, false>(
        input,
        output,
        visitor,
        limits,
        WireValue::MetadataPreserveErrorSchema(Some(ArchiveV2WireMetadataErrorSchema::Current)),
    )
}

#[derive(Clone, Copy)]
enum WireValue {
    Message,
    SuccessfulMetadata,
    Metadata,
    MetadataPreserveErrorSchema(Option<ArchiveV2WireMetadataErrorSchema>),
}

fn transform_transactionally<
    V: ArchiveV2WireRewriteVisitor,
    const PRE_UNKNOWN_FALLBACKS: bool,
    const TRANSCODE_PRE_TO_POST: bool,
>(
    input: &[u8],
    output: &mut Vec<u8>,
    visitor: &mut V,
    limits: ArchiveV2WireRewriteLimits,
    value: WireValue,
) -> ArchiveV2WireRewriteResult<ArchiveV2WireRewriteStats> {
    let output_start = output.len();
    let checkpoint = visitor.checkpoint();
    let result = (|| {
        if input.len() > limits.max_input_bytes {
            return Err(ArchiveV2WireRewriteError::limit(format_args!(
                "wire input has {} bytes, limit is {}",
                input.len(),
                limits.max_input_bytes
            )));
        }
        let mut transformer = Transformer {
            cursor: input,
            output,
            output_start,
            visitor,
            limits,
            stats: ArchiveV2WireRewriteStats {
                input_bytes: input.len(),
                ..ArchiveV2WireRewriteStats::default()
            },
            unchecked_small_puts: 0,
        };
        match value {
            WireValue::Message => {
                transformer.message::<PRE_UNKNOWN_FALLBACKS, TRANSCODE_PRE_TO_POST>()?
            }
            WireValue::SuccessfulMetadata => transformer.successful_metadata()?,
            WireValue::Metadata => transformer.metadata()?,
            WireValue::MetadataPreserveErrorSchema(schema) => {
                transformer.metadata_preserving_error_schema(schema)?
            }
        }
        if !transformer.cursor.is_empty() {
            return Err(ArchiveV2WireRewriteError::invalid_value(format_args!(
                "wire value has {} trailing bytes",
                transformer.cursor.len()
            )));
        }
        transformer.check_output_limit()?;
        transformer.stats.output_bytes = transformer.output.len() - output_start;
        Ok(transformer.stats)
    })();

    match result {
        Ok(stats) => {
            visitor.commit(checkpoint);
            Ok(stats)
        }
        Err(error) => {
            output.truncate(output_start);
            visitor.rollback(checkpoint);
            Err(error)
        }
    }
}

struct Transformer<'a, V> {
    cursor: &'a [u8],
    output: &'a mut Vec<u8>,
    output_start: usize,
    visitor: &'a mut V,
    limits: ArchiveV2WireRewriteLimits,
    stats: ArchiveV2WireRewriteStats,
    unchecked_small_puts: u16,
}

/// A wincode writer that appends without letting the logical record length cross its output limit.
///
/// This is used for the few schema-owned leaves. Their writers can emit an allocation-bearing
/// subtree in one call, so the periodic scalar guard cannot safely bound them.
struct BoundedAppendWriter<'a> {
    output: &'a mut Vec<u8>,
    output_start: usize,
    max_output_bytes: usize,
}

/// Message-coordinate reference carried by one transaction error.
///
/// The selected-schema metadata splitter reports this without constructing an
/// owned [`crate::CompactTransactionError`], so selective readers can apply
/// their message-specific index limits before they visit the metadata tail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveV2WireMetadataErrorIndex {
    TopLevelInstruction(u8),
    MessageAccount(u8),
}

#[derive(Clone, Copy)]
struct MetadataErrorPrefix<'a> {
    tail: &'a [u8],
    schema: ArchiveV2WireMetadataErrorSchema,
    error_index: Option<ArchiveV2WireMetadataErrorIndex>,
}

/// Borrowed metadata tail after an explicitly selected transaction-error
/// prefix was validated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BorrowedArchiveV2MetadataTail<'a> {
    pub bytes: &'a [u8],
    pub has_error: bool,
    pub error_index: Option<ArchiveV2WireMetadataErrorIndex>,
}

fn sequence_read_error(
    context: &'static str,
    error: wincode::error::ReadError,
) -> ArchiveV2WireRewriteError {
    match error {
        wincode::error::ReadError::PreallocationSizeLimit { .. } => {
            ArchiveV2WireRewriteError::limit(format_args!("{context}: {error}"))
        }
        _ => ArchiveV2WireRewriteError::invalid(context, error),
    }
}

fn read_current_error_scalar<'de, T>(
    input: &mut &'de [u8],
    context: &'static str,
) -> ArchiveV2WireRewriteResult<T>
where
    T: SchemaRead<'de, ArchiveV2WireBoundedConfig, Dst = T>,
{
    T::get(input).map_err(|error| ArchiveV2WireRewriteError::invalid(context, error))
}

fn validate_current_instruction_error(
    input: &mut &[u8],
    max_sequence_items: usize,
) -> ArchiveV2WireRewriteResult<()> {
    let tag = read_current_error_scalar::<u8>(input, "current instruction-error tag")?;
    match tag {
        0..=24 | 26..=43 | 45..=53 => Ok(()),
        25 => {
            read_current_error_scalar::<u32>(input, "current custom instruction error")?;
            Ok(())
        }
        44 => {
            let len = <wincode::len::BincodeLen as SeqLen<ArchiveV2WireBoundedConfig>>::read_prealloc_check::<u8>(&mut *input)
                .map_err(|error| sequence_read_error("current Borsh I/O error byte length", error))?;
            if len > max_sequence_items {
                return Err(ArchiveV2WireRewriteError::limit(format_args!(
                    "current Borsh I/O error has {len} bytes, item limit is {max_sequence_items}"
                )));
            }
            if len > input.len() {
                return Err(ArchiveV2WireRewriteError::invalid_value(format_args!(
                    "current Borsh I/O error has {len} bytes, but only {} input bytes remain",
                    input.len()
                )));
            }
            let bytes = input.take_borrowed(len).map_err(|error| {
                ArchiveV2WireRewriteError::invalid("current Borsh I/O error bytes", error)
            })?;
            std::str::from_utf8(bytes).map_err(|error| {
                ArchiveV2WireRewriteError::invalid("current Borsh I/O error UTF-8", error)
            })?;
            Ok(())
        }
        _ => Err(ArchiveV2WireRewriteError::invalid_value(format_args!(
            "current instruction-error tag {tag} is outside 0..=53"
        ))),
    }
}

fn validate_current_metadata_error_prefix(
    input: &[u8],
    max_sequence_items: usize,
) -> ArchiveV2WireRewriteResult<MetadataErrorPrefix<'_>> {
    let mut tail = input;
    let tag = read_current_error_scalar::<u8>(&mut tail, "current transaction-error tag")?;
    let error_index = match tag {
        0..=7 | 9..=29 | 32..=34 | 36..=38 => None,
        8 => {
            let index = read_current_error_scalar::<u8>(&mut tail, "current instruction index")?;
            validate_current_instruction_error(&mut tail, max_sequence_items)?;
            Some(ArchiveV2WireMetadataErrorIndex::TopLevelInstruction(index))
        }
        30 => Some(ArchiveV2WireMetadataErrorIndex::TopLevelInstruction(
            read_current_error_scalar::<u8>(&mut tail, "current duplicate-instruction index")?,
        )),
        31 | 35 => Some(ArchiveV2WireMetadataErrorIndex::MessageAccount(
            read_current_error_scalar::<u8>(&mut tail, "current transaction-error account index")?,
        )),
        _ => {
            return Err(ArchiveV2WireRewriteError::invalid_value(format_args!(
                "current transaction-error tag {tag} is outside 0..=38"
            )));
        }
    };
    Ok(MetadataErrorPrefix {
        tail,
        schema: ArchiveV2WireMetadataErrorSchema::Current,
        error_index,
    })
}

fn take_legacy_error_bytes<'a>(
    input: &mut &'a [u8],
    len: usize,
    context: &'static str,
) -> ArchiveV2WireRewriteResult<&'a [u8]> {
    if len > input.len() {
        return Err(ArchiveV2WireRewriteError::invalid_value(format_args!(
            "{context} needs {len} bytes, but only {} bytes remain",
            input.len()
        )));
    }
    input
        .take_borrowed(len)
        .map_err(|error| ArchiveV2WireRewriteError::invalid(context, error))
}

fn read_legacy_error_u32(
    input: &mut &[u8],
    context: &'static str,
) -> ArchiveV2WireRewriteResult<u32> {
    let bytes = take_legacy_error_bytes(input, 4, context)?;
    Ok(u32::from_le_bytes(
        bytes.try_into().expect("checked length"),
    ))
}

fn validate_legacy_stored_instruction_error(
    input: &mut &[u8],
    max_sequence_items: usize,
) -> ArchiveV2WireRewriteResult<()> {
    let tag = read_legacy_error_u32(input, "legacy instruction-error tag")?;
    match tag {
        0..=24 | 26..=43 | 45..=53 => Ok(()),
        25 => {
            take_legacy_error_bytes(input, 4, "legacy custom instruction error")?;
            Ok(())
        }
        44 if input.is_empty() => {
            // Old Solana archives encoded BorshIoError as a unit variant.
            Ok(())
        }
        44 => {
            let len_bytes = take_legacy_error_bytes(input, 8, "legacy Borsh I/O error length")?;
            let len_u64 = u64::from_le_bytes(len_bytes.try_into().expect("checked length"));
            let len = usize::try_from(len_u64).map_err(|_| {
                ArchiveV2WireRewriteError::limit(format_args!(
                    "legacy Borsh I/O error length {len_u64} does not fit usize"
                ))
            })?;
            if len > max_sequence_items {
                return Err(ArchiveV2WireRewriteError::limit(format_args!(
                    "legacy Borsh I/O error has {len} bytes, item limit is {max_sequence_items}"
                )));
            }
            let bytes = take_legacy_error_bytes(input, len, "legacy Borsh I/O error bytes")?;
            std::str::from_utf8(bytes).map_err(|error| {
                ArchiveV2WireRewriteError::invalid("legacy Borsh I/O error UTF-8", error)
            })?;
            Ok(())
        }
        _ => Err(ArchiveV2WireRewriteError::invalid_value(format_args!(
            "legacy instruction-error tag {tag} is outside 0..=53"
        ))),
    }
}

fn validate_legacy_stored_transaction_error(
    stored: &[u8],
    max_sequence_items: usize,
) -> ArchiveV2WireRewriteResult<Option<ArchiveV2WireMetadataErrorIndex>> {
    let mut tail = stored;
    let tag = read_legacy_error_u32(&mut tail, "legacy transaction-error tag")?;
    let error_index = match tag {
        0..=7 | 9..=29 | 32..=34 | 36..=38 => None,
        8 => {
            let index = take_legacy_error_bytes(&mut tail, 1, "legacy instruction index")?[0];
            validate_legacy_stored_instruction_error(&mut tail, max_sequence_items)?;
            Some(ArchiveV2WireMetadataErrorIndex::TopLevelInstruction(index))
        }
        30 => Some(ArchiveV2WireMetadataErrorIndex::TopLevelInstruction(
            take_legacy_error_bytes(&mut tail, 1, "legacy duplicate-instruction index")?[0],
        )),
        31 | 35 => Some(ArchiveV2WireMetadataErrorIndex::MessageAccount(
            take_legacy_error_bytes(&mut tail, 1, "legacy transaction-error account index")?[0],
        )),
        _ => {
            return Err(ArchiveV2WireRewriteError::invalid_value(format_args!(
                "legacy transaction-error tag {tag} is outside 0..=38"
            )));
        }
    };
    if !tail.is_empty() {
        return Err(ArchiveV2WireRewriteError::invalid_value(format_args!(
            "legacy transaction error has {} trailing bytes",
            tail.len()
        )));
    }
    Ok(error_index)
}

fn validate_legacy_metadata_error_prefix(
    input: &[u8],
    max_sequence_items: usize,
) -> ArchiveV2WireRewriteResult<MetadataErrorPrefix<'_>> {
    let mut tail = input;
    let len = <wincode::len::BincodeLen as SeqLen<ArchiveV2WireBoundedConfig>>::read_prealloc_check::<u8>(
        &mut tail,
    )
    .map_err(|error| {
        sequence_read_error("legacy metadata transaction-error byte length", error)
    })?;
    if len > max_sequence_items {
        return Err(ArchiveV2WireRewriteError::limit(format_args!(
            "legacy metadata transaction error has {len} bytes, item limit is {max_sequence_items}"
        )));
    }
    if len > tail.len() {
        return Err(ArchiveV2WireRewriteError::invalid_value(format_args!(
            "legacy metadata transaction error is {len} bytes, but only {} input bytes remain",
            tail.len()
        )));
    }
    let stored = tail.take_borrowed(len).map_err(|error| {
        ArchiveV2WireRewriteError::invalid("legacy metadata transaction-error bytes", error)
    })?;
    let error_index = validate_legacy_stored_transaction_error(stored, max_sequence_items)?;
    Ok(MetadataErrorPrefix {
        tail,
        schema: ArchiveV2WireMetadataErrorSchema::Legacy,
        error_index,
    })
}

/// Validate the metadata option and transaction-error prefix under one
/// explicitly selected wire schema, then borrow the common metadata tail.
///
/// This function does not inspect the returned tail. It is for selective
/// readers that validate the common `CompactMetaV1` tail themselves without
/// allocating an owned metadata graph. `max_sequence_items` bounds strings or
/// byte sequences inside the selected transaction error.
pub fn validate_archive_v2_metadata_error_prefix_for_selected_schema(
    input: &[u8],
    schema: ArchiveV2WireMetadataErrorSchema,
    max_sequence_items: usize,
) -> ArchiveV2WireRewriteResult<BorrowedArchiveV2MetadataTail<'_>> {
    let mut tail = input;
    match read_current_error_scalar::<u8>(&mut tail, "metadata transaction-error option")? {
        0 => Ok(BorrowedArchiveV2MetadataTail {
            bytes: tail,
            has_error: false,
            error_index: None,
        }),
        1 => {
            let selected = match schema {
                ArchiveV2WireMetadataErrorSchema::Current => {
                    validate_current_metadata_error_prefix(tail, max_sequence_items)?
                }
                ArchiveV2WireMetadataErrorSchema::Legacy => {
                    validate_legacy_metadata_error_prefix(tail, max_sequence_items)?
                }
            };
            debug_assert_eq!(selected.schema, schema);
            Ok(BorrowedArchiveV2MetadataTail {
                bytes: selected.tail,
                has_error: true,
                error_index: selected.error_index,
            })
        }
        tag => Err(ArchiveV2WireRewriteError::invalid_value(format_args!(
            "metadata transaction-error option has invalid tag {tag}"
        ))),
    }
}

fn decode_admitted_metadata_error_prefix(
    input: &[u8],
    selected: MetadataErrorPrefix<'_>,
) -> ArchiveV2WireRewriteResult<crate::CompactTransactionError> {
    let prefix_len = input
        .len()
        .checked_sub(selected.tail.len())
        .ok_or_else(|| {
            ArchiveV2WireRewriteError::invalid_value(
                "metadata transaction-error prefix is outside its input",
            )
        })?;
    let mut prefix = &input[..prefix_len];
    let error = match selected.schema {
        ArchiveV2WireMetadataErrorSchema::Current => {
            <crate::CompactTransactionError as SchemaRead<ArchiveV2WireBoundedConfig>>::get(
                &mut prefix,
            )
            .map_err(|error| {
                ArchiveV2WireRewriteError::invalid("current metadata transaction error", error)
            })?
        }
        ArchiveV2WireMetadataErrorSchema::Legacy => {
            let len = <wincode::len::BincodeLen as SeqLen<ArchiveV2WireBoundedConfig>>::read_prealloc_check::<u8>(
                &mut prefix,
            )
            .map_err(|error| {
                sequence_read_error("legacy metadata transaction-error byte length", error)
            })?;
            let stored = prefix.take_borrowed(len).map_err(|error| {
                ArchiveV2WireRewriteError::invalid("legacy metadata transaction-error bytes", error)
            })?;
            crate::CompactTransactionError::from_stored_wincode_bytes(stored).map_err(|error| {
                ArchiveV2WireRewriteError::invalid("legacy metadata transaction error", error)
            })?
        }
    };
    if !prefix.is_empty() {
        return Err(ArchiveV2WireRewriteError::invalid_value(format_args!(
            "metadata transaction-error prefix has {} trailing bytes",
            prefix.len()
        )));
    }
    Ok(error)
}

impl Writer for BoundedAppendWriter<'_> {
    #[inline]
    fn write(&mut self, src: &[u8]) -> wincode::io::WriteResult<()> {
        let used = self
            .output
            .len()
            .checked_sub(self.output_start)
            .ok_or(wincode::io::WriteError::WriteSizeLimit(src.len()))?;
        let projected = used
            .checked_add(src.len())
            .ok_or(wincode::io::WriteError::WriteSizeLimit(usize::MAX))?;
        if projected > self.max_output_bytes {
            return Err(wincode::io::WriteError::WriteSizeLimit(projected));
        }
        self.output
            .try_reserve(src.len())
            .map_err(|_| wincode::io::WriteError::WriteSizeLimit(projected))?;
        self.output.extend_from_slice(src);
        Ok(())
    }
}

impl<V: ArchiveV2WireRewriteVisitor> Transformer<'_, V> {
    fn get<T>(&mut self, context: &'static str) -> ArchiveV2WireRewriteResult<T>
    where
        for<'de> T: SchemaRead<'de, ArchiveV2WireBoundedConfig, Dst = T>,
    {
        T::get(&mut self.cursor).map_err(|error| ArchiveV2WireRewriteError::invalid(context, error))
    }

    fn put_small<T>(&mut self, value: &T, context: &'static str) -> ArchiveV2WireRewriteResult<()>
    where
        T: SchemaWrite<ArchiveV2WireBoundedConfig, Src = T>,
    {
        #[cfg(debug_assertions)]
        let output_before = self.output.len();
        T::write(&mut *self.output, value).map_err(|error| Self::write_error(context, error))?;
        #[cfg(debug_assertions)]
        debug_assert!(
            self.output.len() - output_before <= MAX_SMALL_PUT_WIRE_BYTES,
            "put_small emitted more than {MAX_SMALL_PUT_WIRE_BYTES} bytes"
        );
        self.unchecked_small_puts += 1;
        if self.unchecked_small_puts == MAX_UNCHECKED_SMALL_PUTS {
            self.check_output_limit()?;
        }
        Ok(())
    }

    fn write_error(
        context: &'static str,
        error: wincode::error::WriteError,
    ) -> ArchiveV2WireRewriteError {
        match error {
            error @ wincode::error::WriteError::PreallocationSizeLimit { .. }
            | error @ wincode::error::WriteError::Io(wincode::io::WriteError::WriteSizeLimit(_)) => {
                ArchiveV2WireRewriteError::limit(format_args!("{context}: {error}"))
            }
            error => ArchiveV2WireRewriteError::invalid(context, error),
        }
    }

    fn scalar<T>(&mut self, context: &'static str) -> ArchiveV2WireRewriteResult<T>
    where
        for<'de> T: SchemaRead<'de, ArchiveV2WireBoundedConfig, Dst = T>,
        T: SchemaWrite<ArchiveV2WireBoundedConfig, Src = T>,
    {
        let value = self.get::<T>(context)?;
        self.put_small(&value, context)?;
        Ok(value)
    }

    fn copy_owned<T>(&mut self, context: &'static str) -> ArchiveV2WireRewriteResult<()>
    where
        for<'de> T: SchemaRead<'de, ArchiveV2WireBoundedConfig, Dst = T>,
        T: SchemaWrite<ArchiveV2WireBoundedConfig, Src = T>,
    {
        let value = self.get::<T>(context)?;
        // Keep every write from this allocation-bearing leaf inside the exact logical output
        // bound. Its first write also detects any periodic scalar overshoot before it adds bytes.
        T::write(
            BoundedAppendWriter {
                output: self.output,
                output_start: self.output_start,
                max_output_bytes: self.limits.max_output_bytes,
            },
            &value,
        )
        .map_err(|error| Self::write_error(context, error))?;
        self.unchecked_small_puts = 0;
        Ok(())
    }

    fn put_owned<T>(&mut self, value: &T, context: &'static str) -> ArchiveV2WireRewriteResult<()>
    where
        T: SchemaWrite<ArchiveV2WireBoundedConfig, Src = T>,
    {
        T::write(
            BoundedAppendWriter {
                output: self.output,
                output_start: self.output_start,
                max_output_bytes: self.limits.max_output_bytes,
            },
            value,
        )
        .map_err(|error| Self::write_error(context, error))?;
        self.unchecked_small_puts = 0;
        Ok(())
    }

    fn sequence_len<T>(&mut self, context: &'static str) -> ArchiveV2WireRewriteResult<usize> {
        let len = <wincode::len::BincodeLen as SeqLen<ArchiveV2WireBoundedConfig>>::read_prealloc_check::<T>(
            &mut self.cursor,
        )
        .map_err(|error| sequence_read_error(context, error))?;
        if len > self.limits.max_sequence_items {
            return Err(ArchiveV2WireRewriteError::limit(format_args!(
                "{context} is {len}, item limit is {}",
                self.limits.max_sequence_items
            )));
        }
        if len > self.cursor.len() {
            return Err(ArchiveV2WireRewriteError::invalid_value(format_args!(
                "{context} is {len}, but only {} input bytes remain",
                self.cursor.len()
            )));
        }
        self.put_small(&(len as u64), context)?;
        Ok(len)
    }

    fn bytes(&mut self, context: &'static str) -> ArchiveV2WireRewriteResult<()> {
        let len = <wincode::len::BincodeLen as SeqLen<ArchiveV2WireBoundedConfig>>::read_prealloc_check::<u8>(
            &mut self.cursor,
        )
        .map_err(|error| sequence_read_error(context, error))?;
        if len > self.cursor.len() {
            return Err(ArchiveV2WireRewriteError::invalid_value(format_args!(
                "{context} byte length is {len}, but only {} input bytes remain",
                self.cursor.len()
            )));
        }
        self.put_small(&(len as u64), context)?;
        let bytes = self
            .cursor
            .take_borrowed(len)
            .map_err(|error| ArchiveV2WireRewriteError::invalid(context, error))?;
        self.append(bytes, context)
    }

    fn string(&mut self, context: &'static str) -> ArchiveV2WireRewriteResult<()> {
        let len = <wincode::len::BincodeLen as SeqLen<ArchiveV2WireBoundedConfig>>::read_prealloc_check::<u8>(
            &mut self.cursor,
        )
        .map_err(|error| sequence_read_error(context, error))?;
        if len > self.cursor.len() {
            return Err(ArchiveV2WireRewriteError::invalid_value(format_args!(
                "{context} byte length is {len}, but only {} input bytes remain",
                self.cursor.len()
            )));
        }
        let bytes = self
            .cursor
            .take_borrowed(len)
            .map_err(|error| ArchiveV2WireRewriteError::invalid(context, error))?;
        std::str::from_utf8(bytes).map_err(|error| {
            ArchiveV2WireRewriteError::invalid("wire String is not valid UTF-8", error)
        })?;
        self.put_small(&(len as u64), context)?;
        self.append(bytes, context)
    }

    fn append(&mut self, bytes: &[u8], context: &'static str) -> ArchiveV2WireRewriteResult<()> {
        let projected = self
            .output
            .len()
            .checked_sub(self.output_start)
            .and_then(|used| used.checked_add(bytes.len()))
            .ok_or_else(|| ArchiveV2WireRewriteError::limit("wire output length overflow"))?;
        if projected > self.limits.max_output_bytes {
            return Err(ArchiveV2WireRewriteError::limit(format_args!(
                "{context} makes wire output {projected} bytes, limit is {}",
                self.limits.max_output_bytes
            )));
        }
        self.output.try_reserve(bytes.len()).map_err(|error| {
            ArchiveV2WireRewriteError::limit(format_args!(
                "reserve {context} output bytes: {error}"
            ))
        })?;
        self.output.extend_from_slice(bytes);
        self.unchecked_small_puts = 0;
        Ok(())
    }

    fn check_output_limit(&mut self) -> ArchiveV2WireRewriteResult<()> {
        let used = self
            .output
            .len()
            .checked_sub(self.output_start)
            .ok_or_else(|| {
                ArchiveV2WireRewriteError::invalid_value("wire output shrank below its checkpoint")
            })?;
        if used > self.limits.max_output_bytes {
            return Err(ArchiveV2WireRewriteError::limit(format_args!(
                "wire output has {used} bytes, limit is {}",
                self.limits.max_output_bytes
            )));
        }
        self.unchecked_small_puts = 0;
        Ok(())
    }

    fn option_tag(&mut self, context: &'static str) -> ArchiveV2WireRewriteResult<bool> {
        match self.scalar::<u8>(context)? {
            0 => Ok(false),
            1 => Ok(true),
            tag => Err(ArchiveV2WireRewriteError::invalid_value(format_args!(
                "{context} has invalid Option tag {tag}"
            ))),
        }
    }

    fn enum_tag(&mut self, context: &'static str) -> ArchiveV2WireRewriteResult<u32> {
        self.scalar::<u32>(context)
    }

    fn pubkey(&mut self, class: ArchiveV2WireReferenceClass) -> ArchiveV2WireRewriteResult<()> {
        let id = self.get::<u32>("CompactPubkey id")?;
        let source = if id == CompactPubkey::RAW_SENTINEL {
            CompactPubkey::Raw(self.get::<[u8; 32]>("raw CompactPubkey")?)
        } else {
            CompactPubkey::Id(id)
        };

        let references = self
            .stats
            .eligible_pubkey_references
            .checked_add(self.stats.excluded_pubkey_references)
            .and_then(|count| count.checked_add(1))
            .ok_or_else(|| ArchiveV2WireRewriteError::limit("pubkey reference count overflow"))?;
        if references > self.limits.max_pubkey_references {
            return Err(ArchiveV2WireRewriteError::limit(format_args!(
                "typed pubkey references exceed limit {}",
                self.limits.max_pubkey_references
            )));
        }

        let rewritten = self
            .visitor
            .rewrite_pubkey(source, class)
            .map_err(ArchiveV2WireRewriteError::visitor)?;
        match rewritten {
            CompactPubkey::Id(CompactPubkey::RAW_SENTINEL) => {
                return Err(ArchiveV2WireRewriteError::invalid_value(
                    "visitor returned reserved CompactPubkey ID 0",
                ));
            }
            CompactPubkey::Id(id) => self.put_small(&id, "rewritten CompactPubkey id")?,
            CompactPubkey::Raw(bytes) => {
                self.put_small(&CompactPubkey::RAW_SENTINEL, "raw CompactPubkey sentinel")?;
                self.append(&bytes, "raw CompactPubkey")?;
            }
        }
        match class {
            ArchiveV2WireReferenceClass::Eligible => {
                self.stats.eligible_pubkey_references += 1;
            }
            ArchiveV2WireReferenceClass::Excluded => {
                self.stats.excluded_pubkey_references += 1;
            }
        }
        // A remap can increase an ID varint or replace an ID with 33 raw-key bytes. This is the
        // only normal scalar path that can make canonical output longer than consumed input.
        self.check_output_limit()
    }

    fn message<const PRE_UNKNOWN_FALLBACKS: bool, const TRANSCODE_PRE_TO_POST: bool>(
        &mut self,
    ) -> ArchiveV2WireRewriteResult<()> {
        debug_assert!(!TRANSCODE_PRE_TO_POST || PRE_UNKNOWN_FALLBACKS);
        let variant = self.enum_tag("hot message variant")?;
        match variant {
            0 => self.message_body::<PRE_UNKNOWN_FALLBACKS, TRANSCODE_PRE_TO_POST>(false),
            1 => self.message_body::<PRE_UNKNOWN_FALLBACKS, TRANSCODE_PRE_TO_POST>(true),
            tag => Err(ArchiveV2WireRewriteError::fallback(
                ArchiveV2WireFallbackReason::UnsupportedMessageVariant(tag),
            )),
        }
    }

    fn message_body<const PRE_UNKNOWN_FALLBACKS: bool, const TRANSCODE_PRE_TO_POST: bool>(
        &mut self,
        v0: bool,
    ) -> ArchiveV2WireRewriteResult<()> {
        self.scalar::<u8>("message required signature count")?;
        self.scalar::<u8>("message readonly signed account count")?;
        self.scalar::<u8>("message readonly unsigned account count")?;

        let account_count = self.sequence_len::<CompactPubkey>("message account-key count")?;
        for _ in 0..account_count {
            self.pubkey(ArchiveV2WireReferenceClass::Eligible)?;
        }
        self.recent_blockhash()?;

        let instruction_count =
            self.sequence_len::<crate::ArchiveV2HotInstruction>("message instruction count")?;
        for _ in 0..instruction_count {
            self.instruction::<PRE_UNKNOWN_FALLBACKS, TRANSCODE_PRE_TO_POST>()?;
        }

        if v0 {
            let lookup_count = self.sequence_len::<crate::OwnedCompactAddressTableLookup>(
                "message address-table lookup count",
            )?;
            for _ in 0..lookup_count {
                self.pubkey(ArchiveV2WireReferenceClass::Eligible)?;
                self.bytes("message writable address-table indexes")?;
                self.bytes("message readonly address-table indexes")?;
            }
        }
        Ok(())
    }

    fn recent_blockhash(&mut self) -> ArchiveV2WireRewriteResult<()> {
        match self.enum_tag("recent blockhash variant")? {
            0 => {
                let id = self.scalar::<i32>("recent blockhash id")?;
                self.visitor
                    .recent_blockhash_id(id)
                    .map_err(ArchiveV2WireRewriteError::visitor)?;
                self.stats.recent_blockhash_ids += 1;
                Ok(())
            }
            1 => {
                self.scalar::<[u8; 32]>("recent blockhash nonce")?;
                Ok(())
            }
            tag => Err(ArchiveV2WireRewriteError::invalid_value(format_args!(
                "recent blockhash has invalid variant {tag}"
            ))),
        }
    }

    fn instruction<const PRE_UNKNOWN_FALLBACKS: bool, const TRANSCODE_PRE_TO_POST: bool>(
        &mut self,
    ) -> ArchiveV2WireRewriteResult<()> {
        self.scalar::<u8>("instruction program-id index")?;
        self.bytes("instruction account indexes")?;
        let tag = if TRANSCODE_PRE_TO_POST {
            let source_tag = self.get::<u32>("historical hot instruction-data variant")?;
            let canonical_tag = match source_tag {
                0 => 0,
                1..=6 => source_tag + 2,
                tag => {
                    return Err(ArchiveV2WireRewriteError::invalid_value(format_args!(
                        "historical hot instruction-data has invalid variant {tag}"
                    )));
                }
            };
            self.put_small(&canonical_tag, "canonical hot instruction-data variant")?;
            source_tag
        } else {
            self.enum_tag("hot instruction-data variant")?
        };
        let maximum_source_tag = if PRE_UNKNOWN_FALLBACKS { 6 } else { 8 };
        if tag <= maximum_source_tag {
            let count = &mut self.stats.source_instruction_data_tag_counts[tag as usize];
            *count = count.checked_add(1).ok_or_else(|| {
                ArchiveV2WireRewriteError::limit("instruction-data tag count overflow")
            })?;
        }
        if PRE_UNKNOWN_FALLBACKS {
            match tag {
                0 => self.bytes("raw instruction data"),
                1 => self.copy_owned::<crate::ArchiveV2ComputeBudgetInstructionData>(
                    "compute-budget instruction data",
                ),
                2 => self.system_instruction(),
                3 => self.vote_state_update(true),
                4 => {
                    self.vote_state_update(true)?;
                    self.vote_hash_ref(false)
                }
                5 => self.vote_tower_sync(),
                6 => {
                    self.vote_tower_sync()?;
                    self.vote_hash_ref(false)
                }
                tag => Err(ArchiveV2WireRewriteError::fallback(
                    ArchiveV2WireFallbackReason::UnsupportedInstructionVariant(tag),
                )),
            }
        } else {
            match tag {
                0..=2 => self.bytes("raw instruction data"),
                3 => self.copy_owned::<crate::ArchiveV2ComputeBudgetInstructionData>(
                    "compute-budget instruction data",
                ),
                4 => self.system_instruction(),
                5 => self.vote_state_update(true),
                6 => {
                    self.vote_state_update(true)?;
                    self.vote_hash_ref(false)
                }
                7 => self.vote_tower_sync(),
                8 => {
                    self.vote_tower_sync()?;
                    self.vote_hash_ref(false)
                }
                tag => Err(ArchiveV2WireRewriteError::fallback(
                    ArchiveV2WireFallbackReason::UnsupportedInstructionVariant(tag),
                )),
            }
        }
    }

    fn system_instruction(&mut self) -> ArchiveV2WireRewriteResult<()> {
        match self.enum_tag("system instruction variant")? {
            0 | 13 => {
                self.scalar::<u64>("system instruction lamports")?;
                self.scalar::<u64>("system instruction space")?;
                self.scalar::<[u8; 32]>("system instruction owner")?;
            }
            1 | 6 | 7 => {
                self.scalar::<[u8; 32]>("system instruction authority")?;
            }
            2 | 5 | 8 => {
                self.scalar::<u64>("system instruction amount")?;
            }
            3 => {
                self.scalar::<[u8; 32]>("system instruction base")?;
                self.string("system instruction seed")?;
                self.scalar::<u64>("system instruction lamports")?;
                self.scalar::<u64>("system instruction space")?;
                self.scalar::<[u8; 32]>("system instruction owner")?;
            }
            4 | 12 => {}
            9 => {
                self.scalar::<[u8; 32]>("system instruction base")?;
                self.string("system instruction seed")?;
                self.scalar::<u64>("system instruction space")?;
                self.scalar::<[u8; 32]>("system instruction owner")?;
            }
            10 => {
                self.scalar::<[u8; 32]>("system instruction base")?;
                self.string("system instruction seed")?;
                self.scalar::<[u8; 32]>("system instruction owner")?;
            }
            11 => {
                self.scalar::<u64>("system instruction lamports")?;
                self.string("system instruction from seed")?;
                self.scalar::<[u8; 32]>("system instruction from owner")?;
            }
            tag => {
                return Err(ArchiveV2WireRewriteError::fallback(
                    ArchiveV2WireFallbackReason::UnsupportedLogVariant {
                        schema: "ArchiveV2SystemInstructionData",
                        tag,
                    },
                ));
            }
        }
        Ok(())
    }

    fn vote_state_update(&mut self, collect_hash: bool) -> ArchiveV2WireRewriteResult<()> {
        if self.option_tag("vote root")? {
            self.scalar::<u64>("vote root slot")?;
        }
        let lockout_count =
            self.sequence_len::<crate::ArchiveV2VoteLockoutOffset>("vote lockout count")?;
        for _ in 0..lockout_count {
            self.scalar::<u64>("vote lockout offset")?;
            self.scalar::<u8>("vote lockout confirmation count")?;
        }
        self.vote_hash_ref(collect_hash)?;
        if self.option_tag("vote timestamp")? {
            self.scalar::<i64>("vote timestamp value")?;
        }
        Ok(())
    }

    fn vote_tower_sync(&mut self) -> ArchiveV2WireRewriteResult<()> {
        self.vote_state_update(true)?;
        self.vote_hash_ref(true)
    }

    fn vote_hash_ref(&mut self, collect: bool) -> ArchiveV2WireRewriteResult<()> {
        match self.enum_tag("vote hash reference variant")? {
            0 => Ok(()),
            1 => {
                let block_id = self.scalar::<u32>("vote hash block id")?;
                if collect {
                    self.visitor
                        .vote_hash_block_id(block_id)
                        .map_err(ArchiveV2WireRewriteError::visitor)?;
                    self.stats.vote_hash_block_ids += 1;
                }
                Ok(())
            }
            2 => {
                self.scalar::<[u8; 32]>("raw vote hash")?;
                Ok(())
            }
            tag => Err(ArchiveV2WireRewriteError::invalid_value(format_args!(
                "vote hash reference has invalid variant {tag}"
            ))),
        }
    }

    fn successful_metadata(&mut self) -> ArchiveV2WireRewriteResult<()> {
        match self.scalar::<u8>("metadata transaction-error option")? {
            0 => {}
            1 => {
                return Err(ArchiveV2WireRewriteError::fallback(
                    ArchiveV2WireFallbackReason::MetadataHasTransactionError,
                ));
            }
            tag => {
                return Err(ArchiveV2WireRewriteError::invalid_value(format_args!(
                    "metadata transaction-error option has invalid tag {tag}"
                )));
            }
        }

        self.metadata_tail()
    }

    fn metadata(&mut self) -> ArchiveV2WireRewriteResult<()> {
        match self.get::<u8>("metadata transaction-error option")? {
            0 => {
                self.put_small(&0u8, "metadata transaction-error option")?;
            }
            1 => {
                let current = validate_current_metadata_error_prefix(
                    self.cursor,
                    self.limits.max_sequence_items,
                );
                let legacy = validate_legacy_metadata_error_prefix(
                    self.cursor,
                    self.limits.max_sequence_items,
                );
                let selected = match (current, legacy) {
                    (Ok(current), Err(_)) => current,
                    (Err(_), Ok(legacy)) => legacy,
                    (Ok(_), Ok(_)) => {
                        return Err(ArchiveV2WireRewriteError::fallback(
                            ArchiveV2WireFallbackReason::MetadataErrorSchemaAmbiguous,
                        ));
                    }
                    (Err(_), Err(_)) => {
                        return Err(ArchiveV2WireRewriteError::fallback(
                            ArchiveV2WireFallbackReason::MetadataErrorPrefixRequiresOwnedFallback,
                        ));
                    }
                };
                let error = decode_admitted_metadata_error_prefix(self.cursor, selected)?;
                self.put_small(&1u8, "metadata transaction-error option")?;
                self.put_owned(&error, "metadata transaction error")?;
                self.cursor = selected.tail;
                self.stats.metadata_error_schema = Some(selected.schema);
            }
            tag => {
                return Err(ArchiveV2WireRewriteError::invalid_value(format_args!(
                    "metadata transaction-error option has invalid tag {tag}"
                )));
            }
        }

        self.metadata_tail()
    }

    fn metadata_preserving_error_schema(
        &mut self,
        selected_schema: Option<ArchiveV2WireMetadataErrorSchema>,
    ) -> ArchiveV2WireRewriteResult<()> {
        match self.get::<u8>("metadata transaction-error option")? {
            0 => {
                self.put_small(&0u8, "metadata transaction-error option")?;
            }
            1 => {
                let selected = match selected_schema {
                    Some(ArchiveV2WireMetadataErrorSchema::Current) => {
                        validate_current_metadata_error_prefix(
                            self.cursor,
                            self.limits.max_sequence_items,
                        )?
                    }
                    Some(ArchiveV2WireMetadataErrorSchema::Legacy) => {
                        validate_legacy_metadata_error_prefix(
                            self.cursor,
                            self.limits.max_sequence_items,
                        )?
                    }
                    None => {
                        let current = validate_current_metadata_error_prefix(
                            self.cursor,
                            self.limits.max_sequence_items,
                        );
                        let legacy = validate_legacy_metadata_error_prefix(
                            self.cursor,
                            self.limits.max_sequence_items,
                        );
                        match (current, legacy) {
                            (Ok(current), Err(_)) => current,
                            (Err(_), Ok(legacy)) => legacy,
                            (Ok(_), Ok(_)) => {
                                return Err(ArchiveV2WireRewriteError::fallback(
                                    ArchiveV2WireFallbackReason::MetadataErrorSchemaAmbiguous,
                                ));
                            }
                            (Err(_), Err(_)) => {
                                return Err(ArchiveV2WireRewriteError::fallback(
                                    ArchiveV2WireFallbackReason::MetadataErrorPrefixRequiresOwnedFallback,
                                ));
                            }
                        }
                    }
                };
                let prefix_len = self
                    .cursor
                    .len()
                    .checked_sub(selected.tail.len())
                    .ok_or_else(|| {
                        ArchiveV2WireRewriteError::invalid_value(
                            "metadata transaction-error prefix is outside its input",
                        )
                    })?;
                self.put_small(&1u8, "metadata transaction-error option")?;
                self.append(
                    &self.cursor[..prefix_len],
                    "preserved metadata transaction error",
                )?;
                self.cursor = selected.tail;
                self.stats.metadata_error_schema = Some(selected.schema);
            }
            tag => {
                return Err(ArchiveV2WireRewriteError::invalid_value(format_args!(
                    "metadata transaction-error option has invalid tag {tag}"
                )));
            }
        }

        self.metadata_tail()
    }

    fn metadata_tail(&mut self) -> ArchiveV2WireRewriteResult<()> {
        self.scalar::<u64>("metadata fee")?;
        self.scalar_vec::<u64>("metadata pre-balance count", "metadata pre balance")?;
        self.scalar_vec::<u64>("metadata post-balance count", "metadata post balance")?;

        if self.option_tag("metadata inner-instructions option")? {
            let group_count = self.sequence_len::<crate::CompactInnerInstructions>(
                "metadata inner-instruction group count",
            )?;
            for _ in 0..group_count {
                self.scalar::<u32>("metadata inner-instruction group index")?;
                let instruction_count = self.sequence_len::<crate::CompactInnerInstruction>(
                    "metadata inner-instruction count",
                )?;
                for _ in 0..instruction_count {
                    self.scalar::<u32>("metadata inner-instruction program-id index")?;
                    self.bytes("metadata inner-instruction account indexes")?;
                    self.bytes("metadata inner-instruction data")?;
                    if self.option_tag("metadata inner-instruction stack-height option")? {
                        self.scalar::<u32>("metadata inner-instruction stack height")?;
                    }
                }
            }
        }

        self.logs()?;
        self.token_balances("metadata pre-token-balance count")?;
        self.token_balances("metadata post-token-balance count")?;
        self.rewards()?;
        self.pubkey_vec(
            ArchiveV2WireReferenceClass::Eligible,
            "metadata loaded-writable-address count",
        )?;
        self.pubkey_vec(
            ArchiveV2WireReferenceClass::Eligible,
            "metadata loaded-readonly-address count",
        )?;

        if self.option_tag("metadata return-data option")? {
            self.pubkey(ArchiveV2WireReferenceClass::Eligible)?;
            self.bytes("metadata return data")?;
        }
        if self.option_tag("metadata compute-units option")? {
            self.scalar::<u64>("metadata compute units")?;
        }
        if self.option_tag("metadata cost-units option")? {
            self.scalar::<u64>("metadata cost units")?;
        }
        Ok(())
    }

    fn scalar_vec<T>(
        &mut self,
        count_context: &'static str,
        item_context: &'static str,
    ) -> ArchiveV2WireRewriteResult<()>
    where
        for<'de> T: SchemaRead<'de, ArchiveV2WireBoundedConfig, Dst = T>,
        T: SchemaWrite<ArchiveV2WireBoundedConfig, Src = T>,
    {
        let count = self.sequence_len::<T>(count_context)?;
        for _ in 0..count {
            self.scalar::<T>(item_context)?;
        }
        Ok(())
    }

    fn pubkey_vec(
        &mut self,
        class: ArchiveV2WireReferenceClass,
        count_context: &'static str,
    ) -> ArchiveV2WireRewriteResult<()> {
        let count = self.sequence_len::<CompactPubkey>(count_context)?;
        for _ in 0..count {
            self.pubkey(class)?;
        }
        Ok(())
    }

    fn token_balances(&mut self, count_context: &'static str) -> ArchiveV2WireRewriteResult<()> {
        let count = self.sequence_len::<crate::CompactTokenBalance>(count_context)?;
        for _ in 0..count {
            self.scalar::<u32>("token-balance account index")?;
            for context in [
                "token-balance mint option",
                "token-balance owner option",
                "token-balance program-id option",
            ] {
                if self.option_tag(context)? {
                    self.pubkey(ArchiveV2WireReferenceClass::Eligible)?;
                }
            }
            self.scalar::<u64>("token-balance amount")?;
            self.scalar::<u8>("token-balance decimals")?;
        }
        Ok(())
    }

    fn rewards(&mut self) -> ArchiveV2WireRewriteResult<()> {
        let count = self.sequence_len::<crate::CompactReward>("metadata reward count")?;
        for _ in 0..count {
            self.pubkey(ArchiveV2WireReferenceClass::Eligible)?;
            self.scalar::<i64>("metadata reward lamports")?;
            self.scalar::<u64>("metadata reward post balance")?;
            self.scalar::<i32>("metadata reward type")?;
            if self.option_tag("metadata reward commission option")? {
                self.scalar::<u8>("metadata reward commission")?;
            }
        }
        Ok(())
    }

    fn logs(&mut self) -> ArchiveV2WireRewriteResult<()> {
        if !self.option_tag("metadata logs option")? {
            return Ok(());
        }

        let event_count = self.sequence_len::<crate::LogEvent>("metadata log-event count")?;
        for _ in 0..event_count {
            self.log_event()?;
        }

        self.scalar_vec::<u32>(
            "metadata log string-length count",
            "metadata log string length",
        )?;
        self.bytes("metadata log string-table bytes")?;

        let array_count = self.sequence_len::<crate::DataArray>("metadata log data-array count")?;
        for _ in 0..array_count {
            self.scalar::<u32>("metadata log data-array chunk count")?;
        }
        self.scalar_vec::<u32>(
            "metadata log chunk-length count",
            "metadata log chunk length",
        )?;
        self.bytes("metadata log data-table bytes")?;
        Ok(())
    }

    fn log_event(&mut self) -> ArchiveV2WireRewriteResult<()> {
        let tag = self.enum_tag("log-event variant")?;
        match tag {
            0 => self.system_program_log()?,
            1 | 2 | 9..=13 | 38 | 39 | 43 => {}
            3 | 4 | 15 | 18 | 19 | 24..=27 | 40..=42 => {
                self.pubkey(ArchiveV2WireReferenceClass::Excluded)?;
            }
            5 | 8 => self.program_log()?,
            6 | 28 | 29 | 31..=33 | 36 | 37 | 44 | 45 => {
                self.scalar::<u32>("log-event string/data/integer id")?;
            }
            7 => {
                self.pubkey(ArchiveV2WireReferenceClass::Excluded)?;
                self.program_log()?;
            }
            14 => {
                self.pubkey(ArchiveV2WireReferenceClass::Excluded)?;
                self.scalar::<u8>("log invoke depth")?;
            }
            16 => {
                self.pubkey(ArchiveV2WireReferenceClass::Excluded)?;
                self.scalar::<u32>("log consumed units")?;
                self.scalar::<u32>("log consumed limit")?;
            }
            17 => {
                self.scalar::<u32>("BPF consumed units")?;
                self.scalar::<u32>("BPF consumed limit")?;
            }
            20..=23 => {
                self.pubkey(ArchiveV2WireReferenceClass::Excluded)?;
                self.scalar::<u32>("log failure reason/code")?;
            }
            30 => {
                self.pubkey(ArchiveV2WireReferenceClass::Excluded)?;
                self.scalar::<u32>("log return-data id")?;
            }
            34 | 35 => {
                if self.option_tag("optional log program id")? {
                    self.pubkey(ArchiveV2WireReferenceClass::Excluded)?;
                }
            }
            tag => {
                return Err(ArchiveV2WireRewriteError::fallback(
                    ArchiveV2WireFallbackReason::UnsupportedLogVariant {
                        schema: "LogEvent",
                        tag,
                    },
                ));
            }
        }
        Ok(())
    }

    fn program_log(&mut self) -> ArchiveV2WireRewriteResult<()> {
        let tag = self.enum_tag("program-log variant")?;
        match tag {
            0 => {}
            1 => self.copy_owned::<crate::program_logs::token::TokenLog>("token program log")?,
            2 => self.token_2022_log()?,
            3 => self.copy_owned::<crate::program_logs::associated_token_account::TokenLog>(
                "associated-token program log",
            )?,
            4 => self
                .copy_owned::<crate::program_logs::address_lookup_table::AddressLookupTableLog>(
                    "address-lookup-table program log",
                )?,
            5 => self.copy_owned::<crate::program_logs::loader_v3::LoaderV3Log>(
                "loader-v3 program log",
            )?,
            6 => self.copy_owned::<crate::program_logs::loader_v4::LoaderV4Log>(
                "loader-v4 program log",
            )?,
            7 => self.copy_owned::<crate::program_logs::memo::MemoLog>("memo program log")?,
            8 => self.copy_owned::<crate::program_logs::record::RecordLog>("record program log")?,
            9 => self.copy_owned::<crate::program_logs::transfer_hook::TransferHookLog>(
                "transfer-hook program log",
            )?,
            10 => self
                .copy_owned::<crate::program_logs::account_compression::AccountCompressionLog>(
                    "account-compression program log",
                )?,
            11 => {
                self.copy_owned::<crate::program_logs::stake::StakeProgramLog>("stake program log")?
            }
            12 => self.copy_owned::<crate::program_logs::zk_elgamal_proof::ZkElgamalProofLog>(
                "zk-elgamal-proof program log",
            )?,
            13 | 16 => {
                self.scalar::<u32>("program-log string id")?;
            }
            14 => {
                self.scalar::<u32>("anchor error code string id")?;
                self.scalar::<u32>("anchor error number")?;
                self.scalar::<u32>("anchor error message string id")?;
            }
            15 => {
                self.scalar::<u32>("anchor error file string id")?;
                self.scalar::<u32>("anchor error line")?;
                self.scalar::<u32>("anchor error code string id")?;
                self.scalar::<u32>("anchor error number")?;
                self.scalar::<u32>("anchor error message string id")?;
            }
            17 => {
                #[cfg(feature = "known-program-logs")]
                self.copy_owned::<crate::program_logs::known_programs::KnownProgramLog>(
                    "known-program log",
                )?;
                #[cfg(not(feature = "known-program-logs"))]
                return Err(ArchiveV2WireRewriteError::fallback(
                    ArchiveV2WireFallbackReason::UnsupportedLogVariant {
                        schema: "ProgramLog",
                        tag,
                    },
                ));
            }
            tag => {
                return Err(ArchiveV2WireRewriteError::fallback(
                    ArchiveV2WireFallbackReason::UnsupportedLogVariant {
                        schema: "ProgramLog",
                        tag,
                    },
                ));
            }
        }
        Ok(())
    }

    fn token_2022_log(&mut self) -> ArchiveV2WireRewriteResult<()> {
        match self.enum_tag("token-2022 log variant")? {
            0 => self.copy_owned::<crate::program_logs::token_2022::Token2022ErrorLog>(
                "token-2022 error log",
            )?,
            1 => self.copy_owned::<crate::program_logs::token_2022::Token2022StaticLog>(
                "token-2022 static log",
            )?,
            2 => {
                self.scalar::<u64>("token-2022 calculated fee")?;
                self.scalar::<u64>("token-2022 fee")?;
            }
            3 | 4 => {
                self.scalar::<usize>("token-2022 resize byte count")?;
            }
            5..=8 => {
                self.pubkey(ArchiveV2WireReferenceClass::Excluded)?;
                self.scalar::<u32>("token-2022 error string id")?;
            }
            tag => {
                return Err(ArchiveV2WireRewriteError::fallback(
                    ArchiveV2WireFallbackReason::UnsupportedLogVariant {
                        schema: "Token2022Log",
                        tag,
                    },
                ));
            }
        }
        Ok(())
    }

    fn system_program_log(&mut self) -> ArchiveV2WireRewriteResult<()> {
        match self.enum_tag("system-program log variant")? {
            0 => self.copy_owned::<crate::program_logs::system_program::SystemInstructionLog>(
                "system-program instruction log",
            )?,
            1 | 13 => {
                self.pubkey(ArchiveV2WireReferenceClass::Excluded)?;
                self.pubkey_or_string()?;
            }
            2..=5 | 7 | 8 => self.system_address()?,
            6 => {
                self.scalar::<u64>("system-program requested allocation")?;
                self.scalar::<u64>("system-program maximum allocation")?;
            }
            9 => {
                self.scalar::<u64>("system-program inner instruction data-size limit")?;
            }
            10 | 14 | 15 => {}
            11 => self.pubkey(ArchiveV2WireReferenceClass::Excluded)?,
            12 => {
                self.scalar::<u64>("system-program available lamports")?;
                self.scalar::<u64>("system-program required lamports")?;
            }
            16 => {
                self.scalar::<u32>("system-program message string id")?;
            }
            17..=20 => {
                self.nonce_action()?;
                self.pubkey_or_string()?;
            }
            21 => {
                self.nonce_action()?;
                self.scalar::<u64>("nonce available lamports")?;
                self.scalar::<u64>("nonce required lamports")?;
            }
            22 => self.nonce_action()?,
            tag => {
                return Err(ArchiveV2WireRewriteError::fallback(
                    ArchiveV2WireFallbackReason::UnsupportedLogVariant {
                        schema: "SystemProgramLog",
                        tag,
                    },
                ));
            }
        }
        Ok(())
    }

    fn nonce_action(&mut self) -> ArchiveV2WireRewriteResult<()> {
        let tag = self.enum_tag("nonce-action variant")?;
        if tag > 3 {
            return Err(ArchiveV2WireRewriteError::fallback(
                ArchiveV2WireFallbackReason::UnsupportedLogVariant {
                    schema: "NonceAction",
                    tag,
                },
            ));
        }
        Ok(())
    }

    fn pubkey_or_string(&mut self) -> ArchiveV2WireRewriteResult<()> {
        match self.enum_tag("pubkey-or-string variant")? {
            0 => self.pubkey(ArchiveV2WireReferenceClass::Excluded),
            1 => {
                self.scalar::<u32>("pubkey-or-string text id")?;
                Ok(())
            }
            tag => Err(ArchiveV2WireRewriteError::fallback(
                ArchiveV2WireFallbackReason::UnsupportedLogVariant {
                    schema: "PubkeyOrString",
                    tag,
                },
            )),
        }
    }

    fn system_address(&mut self) -> ArchiveV2WireRewriteResult<()> {
        match self.enum_tag("system-address variant")? {
            0 => self.pubkey_or_string(),
            1 => {
                self.pubkey_or_string()?;
                if self.option_tag("system-address base option")? {
                    self.pubkey_or_string()?;
                }
                Ok(())
            }
            tag => Err(ArchiveV2WireRewriteError::fallback(
                ArchiveV2WireFallbackReason::UnsupportedLogVariant {
                    schema: "SystemAddress",
                    tag,
                },
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::program_logs::{ProgramLog, system_program, token_2022};
    use crate::{
        ArchiveV2ComputeBudgetInstructionData, ArchiveV2HotInstruction,
        ArchiveV2HotInstructionData, ArchiveV2HotLegacyMessage, ArchiveV2HotMessagePayload,
        ArchiveV2HotV0Message, ArchiveV2SystemInstructionData, ArchiveV2VoteHashRef,
        ArchiveV2VoteLockoutOffset, ArchiveV2VoteStateUpdate, ArchiveV2VoteTowerSync,
        CompactInnerInstruction, CompactInnerInstructions, CompactLogStream, CompactMessageHeader,
        CompactMetaV1, CompactReturnData, CompactReward, CompactTokenBalance, DataArray, DataTable,
        LogEvent, OwnedCompactAddressTableLookup, OwnedCompactRecentBlockhash, StringTable,
        wincode_leb128_config,
    };
    use of_car_reader::stored_transaction::{
        InstructionError as StoredInstructionError, StoredTransactionError,
    };

    const DELTA: u32 = 10_000;

    #[derive(Debug, SchemaRead, SchemaWrite)]
    enum HistoricalMessagePayload {
        Legacy(HistoricalLegacyMessage),
        V0(HistoricalV0Message),
    }

    #[derive(Debug, SchemaRead, SchemaWrite)]
    struct HistoricalLegacyMessage {
        header: CompactMessageHeader,
        account_keys: Vec<CompactPubkey>,
        recent_blockhash: OwnedCompactRecentBlockhash,
        instructions: Vec<HistoricalInstruction>,
    }

    #[derive(Debug, SchemaRead, SchemaWrite)]
    struct HistoricalV0Message {
        header: CompactMessageHeader,
        account_keys: Vec<CompactPubkey>,
        recent_blockhash: OwnedCompactRecentBlockhash,
        instructions: Vec<HistoricalInstruction>,
        address_table_lookups: Vec<OwnedCompactAddressTableLookup>,
    }

    #[derive(Debug, SchemaRead, SchemaWrite)]
    struct HistoricalInstruction {
        program_id_index: u8,
        accounts: Vec<u8>,
        data: HistoricalInstructionData,
    }

    #[derive(Debug, SchemaRead, SchemaWrite)]
    enum HistoricalInstructionData {
        Raw(Vec<u8>),
        ComputeBudget(ArchiveV2ComputeBudgetInstructionData),
        System(ArchiveV2SystemInstructionData),
        VoteCompactUpdateVoteState(ArchiveV2VoteStateUpdate),
        VoteCompactUpdateVoteStateSwitch {
            update: ArchiveV2VoteStateUpdate,
            switch_proof_hash: ArchiveV2VoteHashRef,
        },
        VoteTowerSync(ArchiveV2VoteTowerSync),
        VoteTowerSyncSwitch {
            tower: ArchiveV2VoteTowerSync,
            switch_proof_hash: ArchiveV2VoteHashRef,
        },
    }

    #[derive(Default)]
    struct RecordingVisitor {
        pubkeys: Vec<(CompactPubkey, CompactPubkey, ArchiveV2WireReferenceClass)>,
        recent_blockhashes: Vec<i32>,
        vote_hashes: Vec<u32>,
        fail_after: Option<usize>,
        expand_ids: bool,
        commits: usize,
        rollbacks: usize,
    }

    impl ArchiveV2WireRewriteVisitor for RecordingVisitor {
        type Checkpoint = (usize, usize, usize);

        fn checkpoint(&mut self) -> Self::Checkpoint {
            (
                self.pubkeys.len(),
                self.recent_blockhashes.len(),
                self.vote_hashes.len(),
            )
        }

        fn rewrite_pubkey(
            &mut self,
            pubkey: CompactPubkey,
            class: ArchiveV2WireReferenceClass,
        ) -> anyhow::Result<CompactPubkey> {
            if self.fail_after == Some(self.pubkeys.len()) {
                anyhow::bail!("injected visitor failure");
            }
            let rewritten = match pubkey {
                CompactPubkey::Id(id) if self.expand_ids => {
                    CompactPubkey::Raw([u8::try_from(id).unwrap_or(0x5a); 32])
                }
                CompactPubkey::Id(id) => CompactPubkey::Id(id.checked_add(DELTA).unwrap()),
                CompactPubkey::Raw(bytes) => CompactPubkey::Raw(bytes),
            };
            self.pubkeys.push((pubkey, rewritten, class));
            Ok(rewritten)
        }

        fn recent_blockhash_id(&mut self, id: i32) -> anyhow::Result<()> {
            self.recent_blockhashes.push(id);
            Ok(())
        }

        fn vote_hash_block_id(&mut self, block_id: u32) -> anyhow::Result<()> {
            self.vote_hashes.push(block_id);
            Ok(())
        }

        fn rollback(&mut self, checkpoint: Self::Checkpoint) {
            self.pubkeys.truncate(checkpoint.0);
            self.recent_blockhashes.truncate(checkpoint.1);
            self.vote_hashes.truncate(checkpoint.2);
            self.rollbacks += 1;
        }

        fn commit(&mut self, _checkpoint: Self::Checkpoint) {
            self.commits += 1;
        }
    }

    fn pk(id: u32, offset: u32) -> CompactPubkey {
        CompactPubkey::Id(id + offset)
    }

    fn vote_update(hash: ArchiveV2VoteHashRef, seed: u64) -> ArchiveV2VoteStateUpdate {
        ArchiveV2VoteStateUpdate {
            root: Some(seed),
            lockout_offsets: vec![
                ArchiveV2VoteLockoutOffset {
                    offset: seed + 1,
                    confirmation_count: 2,
                },
                ArchiveV2VoteLockoutOffset {
                    offset: seed + 3,
                    confirmation_count: 4,
                },
            ],
            hash,
            timestamp: Some(seed as i64 + 5),
        }
    }

    fn instruction(data: ArchiveV2HotInstructionData) -> ArchiveV2HotInstruction {
        ArchiveV2HotInstruction {
            program_id_index: 0,
            accounts: vec![],
            data,
        }
    }

    fn all_hot_instruction_variants() -> Vec<ArchiveV2HotInstruction> {
        let tower = ArchiveV2VoteTowerSync {
            update: vote_update(ArchiveV2VoteHashRef::Block(94), 30),
            block_id_hash: ArchiveV2VoteHashRef::Block(95),
        };
        let switched_tower = ArchiveV2VoteTowerSync {
            update: vote_update(ArchiveV2VoteHashRef::Block(96), 40),
            block_id_hash: ArchiveV2VoteHashRef::Block(97),
        };
        vec![
            ArchiveV2HotInstruction {
                program_id_index: 0,
                accounts: vec![0, 1],
                data: ArchiveV2HotInstructionData::Raw(vec![1, 2, 3]),
            },
            ArchiveV2HotInstruction {
                program_id_index: 1,
                accounts: vec![],
                data: ArchiveV2HotInstructionData::UnknownSystem(vec![4, 5]),
            },
            ArchiveV2HotInstruction {
                program_id_index: 2,
                accounts: vec![2],
                data: ArchiveV2HotInstructionData::UnknownVote(vec![6]),
            },
            instruction(ArchiveV2HotInstructionData::ComputeBudget(
                ArchiveV2ComputeBudgetInstructionData::Unused,
            )),
            instruction(ArchiveV2HotInstructionData::ComputeBudget(
                ArchiveV2ComputeBudgetInstructionData::RequestHeapFrame(32_768),
            )),
            instruction(ArchiveV2HotInstructionData::ComputeBudget(
                ArchiveV2ComputeBudgetInstructionData::SetComputeUnitLimit(200_000),
            )),
            instruction(ArchiveV2HotInstructionData::ComputeBudget(
                ArchiveV2ComputeBudgetInstructionData::SetComputeUnitPrice(123_456),
            )),
            instruction(ArchiveV2HotInstructionData::ComputeBudget(
                ArchiveV2ComputeBudgetInstructionData::SetLoadedAccountsDataSizeLimit(65_536),
            )),
            instruction(ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::CreateAccount {
                    lamports: 1,
                    space: 2,
                    owner: [3; 32],
                },
            )),
            instruction(ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::Assign { owner: [4; 32] },
            )),
            instruction(ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::Transfer { lamports: 5 },
            )),
            instruction(ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::CreateAccountWithSeed {
                    base: [7; 32],
                    seed: "seed-value".to_owned(),
                    lamports: 8,
                    space: 9,
                    owner: [10; 32],
                },
            )),
            instruction(ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::AdvanceNonceAccount,
            )),
            instruction(ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::WithdrawNonceAccount { lamports: 11 },
            )),
            instruction(ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::InitializeNonceAccount {
                    authority: [12; 32],
                },
            )),
            instruction(ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::AuthorizeNonceAccount {
                    authority: [13; 32],
                },
            )),
            instruction(ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::Allocate { space: 14 },
            )),
            instruction(ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::AllocateWithSeed {
                    base: [15; 32],
                    seed: "allocate".to_owned(),
                    space: 16,
                    owner: [17; 32],
                },
            )),
            instruction(ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::AssignWithSeed {
                    base: [18; 32],
                    seed: "assign".to_owned(),
                    owner: [19; 32],
                },
            )),
            instruction(ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::TransferWithSeed {
                    lamports: 20,
                    from_seed: "transfer".to_owned(),
                    from_owner: [21; 32],
                },
            )),
            instruction(ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::UpgradeNonceAccount,
            )),
            instruction(ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::CreateAccountAllowPrefund {
                    lamports: 22,
                    space: 23,
                    owner: [24; 32],
                },
            )),
            ArchiveV2HotInstruction {
                program_id_index: 0,
                accounts: vec![],
                data: ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(vote_update(
                    ArchiveV2VoteHashRef::Block(91),
                    10,
                )),
            },
            ArchiveV2HotInstruction {
                program_id_index: 0,
                accounts: vec![],
                data: ArchiveV2HotInstructionData::VoteCompactUpdateVoteStateSwitch {
                    update: vote_update(ArchiveV2VoteHashRef::Block(92), 20),
                    switch_proof_hash: ArchiveV2VoteHashRef::Block(900),
                },
            },
            ArchiveV2HotInstruction {
                program_id_index: 0,
                accounts: vec![],
                data: ArchiveV2HotInstructionData::VoteTowerSync(tower),
            },
            ArchiveV2HotInstruction {
                program_id_index: 0,
                accounts: vec![],
                data: ArchiveV2HotInstructionData::VoteTowerSyncSwitch {
                    tower: switched_tower,
                    switch_proof_hash: ArchiveV2VoteHashRef::Block(901),
                },
            },
        ]
    }

    fn message_fixture(offset: u32, v0: bool) -> ArchiveV2HotMessagePayload {
        let header = CompactMessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 1,
        };
        let account_keys = vec![pk(1, offset), pk(2, offset), CompactPubkey::Raw([3; 32])];
        if v0 {
            ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
                header,
                account_keys,
                recent_blockhash: OwnedCompactRecentBlockhash::Nonce([4; 32]),
                instructions: all_hot_instruction_variants(),
                address_table_lookups: vec![OwnedCompactAddressTableLookup {
                    account_key: pk(4, offset),
                    writable_indexes: vec![0, 2],
                    readonly_indexes: vec![1],
                }],
            })
        } else {
            ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
                header,
                account_keys,
                recent_blockhash: OwnedCompactRecentBlockhash::Id(-17),
                instructions: all_hot_instruction_variants(),
            })
        }
    }

    #[test]
    fn message_transform_is_byte_differential_for_all_instruction_shapes() {
        for v0 in [false, true] {
            let source =
                wincode::config::serialize(&message_fixture(0, v0), wincode_leb128_config())
                    .unwrap();
            let expected =
                wincode::config::serialize(&message_fixture(DELTA, v0), wincode_leb128_config())
                    .unwrap();
            let mut output = vec![0xaa, 0xbb];
            let mut visitor = RecordingVisitor::default();
            let stats = rewrite_archive_v2_hot_message_wire(
                &source,
                &mut output,
                &mut visitor,
                ArchiveV2WireRewriteLimits::default(),
            )
            .unwrap();

            assert_eq!(&output[2..], expected);
            assert_eq!(stats.eligible_pubkey_references, if v0 { 4 } else { 3 });
            assert_eq!(stats.excluded_pubkey_references, 0);
            assert_eq!(
                visitor.recent_blockhashes,
                if v0 { vec![] } else { vec![-17] }
            );
            assert_eq!(visitor.vote_hashes, [91, 92, 94, 95, 96, 97]);
            assert!(!visitor.vote_hashes.contains(&900));
            assert!(!visitor.vote_hashes.contains(&901));
            assert_eq!(visitor.commits, 1);
            assert_eq!(visitor.rollbacks, 0);
        }
    }

    #[test]
    fn pre_unknown_fallback_profile_rewrites_registry_keys_and_preserves_system_pubkey_bytes() {
        let embedded_owner = [0x7b; 32];
        let source = HistoricalMessagePayload::Legacy(HistoricalLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Id(7)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(-9),
            instructions: vec![HistoricalInstruction {
                program_id_index: 0,
                accounts: vec![0],
                data: HistoricalInstructionData::System(
                    ArchiveV2SystemInstructionData::CreateAccount {
                        lamports: 11,
                        space: 22,
                        owner: embedded_owner,
                    },
                ),
            }],
        });
        let source = wincode::config::serialize(&source, wincode_leb128_config()).unwrap();
        let mut output = Vec::new();
        let mut visitor = RecordingVisitor::default();

        let stats = rewrite_archive_v2_hot_message_wire_pre_unknown_fallbacks(
            &source,
            &mut output,
            &mut visitor,
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap();
        let decoded: HistoricalMessagePayload =
            wincode::config::deserialize_exact(&output, wincode_leb128_config()).unwrap();
        let HistoricalMessagePayload::Legacy(decoded) = decoded else {
            panic!("historical legacy message changed variant");
        };

        assert_eq!(decoded.account_keys, [CompactPubkey::Id(7 + DELTA)]);
        let HistoricalInstructionData::System(ArchiveV2SystemInstructionData::CreateAccount {
            owner,
            ..
        }) = &decoded.instructions[0].data
        else {
            panic!("historical System instruction changed variant");
        };
        assert_eq!(*owner, embedded_owner);
        assert_eq!(stats.eligible_pubkey_references, 1);
        assert_eq!(visitor.recent_blockhashes, [-9]);
    }

    #[test]
    fn pre_to_post_transcoder_maps_all_frozen_tags_in_one_mixed_message() {
        let source_update = vote_update(ArchiveV2VoteHashRef::Block(42), 10);
        let source_tower = ArchiveV2VoteTowerSync {
            update: vote_update(ArchiveV2VoteHashRef::Block(43), 20),
            block_id_hash: ArchiveV2VoteHashRef::Raw([44; 32]),
        };
        let source = HistoricalMessagePayload::Legacy(HistoricalLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(-11),
            instructions: vec![
                HistoricalInstruction {
                    program_id_index: 1,
                    accounts: vec![0],
                    data: HistoricalInstructionData::Raw(vec![1, 2, 3]),
                },
                HistoricalInstruction {
                    program_id_index: 1,
                    accounts: vec![],
                    data: HistoricalInstructionData::ComputeBudget(
                        ArchiveV2ComputeBudgetInstructionData::SetComputeUnitLimit(200_000),
                    ),
                },
                HistoricalInstruction {
                    program_id_index: 1,
                    accounts: vec![0, 1],
                    data: HistoricalInstructionData::System(
                        ArchiveV2SystemInstructionData::Transfer { lamports: 99 },
                    ),
                },
                HistoricalInstruction {
                    program_id_index: 1,
                    accounts: vec![],
                    data: HistoricalInstructionData::VoteCompactUpdateVoteState(
                        source_update.clone(),
                    ),
                },
                HistoricalInstruction {
                    program_id_index: 1,
                    accounts: vec![],
                    data: HistoricalInstructionData::VoteCompactUpdateVoteStateSwitch {
                        update: source_update,
                        switch_proof_hash: ArchiveV2VoteHashRef::Raw([45; 32]),
                    },
                },
                HistoricalInstruction {
                    program_id_index: 1,
                    accounts: vec![],
                    data: HistoricalInstructionData::VoteTowerSync(source_tower.clone()),
                },
                HistoricalInstruction {
                    program_id_index: 1,
                    accounts: vec![],
                    data: HistoricalInstructionData::VoteTowerSyncSwitch {
                        tower: source_tower,
                        switch_proof_hash: ArchiveV2VoteHashRef::Zero,
                    },
                },
            ],
        });

        let expected_update = vote_update(ArchiveV2VoteHashRef::Block(42), 10);
        let expected_tower = ArchiveV2VoteTowerSync {
            update: vote_update(ArchiveV2VoteHashRef::Block(43), 20),
            block_id_hash: ArchiveV2VoteHashRef::Raw([44; 32]),
        };
        let expected = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(-11),
            instructions: vec![
                ArchiveV2HotInstruction {
                    program_id_index: 1,
                    accounts: vec![0],
                    data: ArchiveV2HotInstructionData::Raw(vec![1, 2, 3]),
                },
                ArchiveV2HotInstruction {
                    program_id_index: 1,
                    accounts: vec![],
                    data: ArchiveV2HotInstructionData::ComputeBudget(
                        ArchiveV2ComputeBudgetInstructionData::SetComputeUnitLimit(200_000),
                    ),
                },
                ArchiveV2HotInstruction {
                    program_id_index: 1,
                    accounts: vec![0, 1],
                    data: ArchiveV2HotInstructionData::System(
                        ArchiveV2SystemInstructionData::Transfer { lamports: 99 },
                    ),
                },
                ArchiveV2HotInstruction {
                    program_id_index: 1,
                    accounts: vec![],
                    data: ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(
                        expected_update.clone(),
                    ),
                },
                ArchiveV2HotInstruction {
                    program_id_index: 1,
                    accounts: vec![],
                    data: ArchiveV2HotInstructionData::VoteCompactUpdateVoteStateSwitch {
                        update: expected_update,
                        switch_proof_hash: ArchiveV2VoteHashRef::Raw([45; 32]),
                    },
                },
                ArchiveV2HotInstruction {
                    program_id_index: 1,
                    accounts: vec![],
                    data: ArchiveV2HotInstructionData::VoteTowerSync(expected_tower.clone()),
                },
                ArchiveV2HotInstruction {
                    program_id_index: 1,
                    accounts: vec![],
                    data: ArchiveV2HotInstructionData::VoteTowerSyncSwitch {
                        tower: expected_tower,
                        switch_proof_hash: ArchiveV2VoteHashRef::Zero,
                    },
                },
            ],
        });

        let source = wincode::config::serialize(&source, wincode_leb128_config()).unwrap();
        let expected = wincode::config::serialize(&expected, wincode_leb128_config()).unwrap();
        assert_eq!(
            source.len(),
            expected.len(),
            "every frozen Pre tag and its canonical Post tag have the same wire width"
        );
        let changed_bytes = source
            .iter()
            .zip(&expected)
            .filter(|&(&source, &target)| {
                if source == target {
                    return false;
                }
                assert!((1..=6).contains(&source));
                assert_eq!(target, source + 2);
                true
            })
            .count();
        assert_eq!(changed_bytes, 6);
        let mut output = vec![0xaa, 0xbb];
        let stats = transcode_archive_v2_hot_message_wire_pre_to_post(
            &source,
            &mut output,
            &mut ArchiveV2WireIdentityVisitor,
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap();

        assert_eq!(&output[2..], expected);
        assert_eq!(stats.input_bytes, source.len());
        assert_eq!(stats.output_bytes, expected.len());
        assert_eq!(
            stats.source_instruction_data_tag_counts,
            [1, 1, 1, 1, 1, 1, 1, 0, 0]
        );
        assert_eq!(stats.eligible_pubkey_references, 2);
        assert_eq!(stats.recent_blockhash_ids, 1);
        assert_eq!(stats.vote_hash_block_ids, 4);
    }

    #[test]
    fn pre_to_post_transcoder_preserves_v0_envelope_and_message_length() {
        let source = HistoricalMessagePayload::V0(HistoricalV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![CompactPubkey::Id(10), CompactPubkey::Id(11)],
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([12; 32]),
            instructions: vec![
                HistoricalInstruction {
                    program_id_index: 1,
                    accounts: vec![0, 2],
                    data: HistoricalInstructionData::Raw(vec![13, 14]),
                },
                HistoricalInstruction {
                    program_id_index: 1,
                    accounts: vec![0],
                    data: HistoricalInstructionData::System(
                        ArchiveV2SystemInstructionData::Allocate { space: 15 },
                    ),
                },
            ],
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Id(16),
                writable_indexes: vec![0],
                readonly_indexes: vec![1],
            }],
        });
        let expected = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![CompactPubkey::Id(10), CompactPubkey::Id(11)],
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([12; 32]),
            instructions: vec![
                ArchiveV2HotInstruction {
                    program_id_index: 1,
                    accounts: vec![0, 2],
                    data: ArchiveV2HotInstructionData::Raw(vec![13, 14]),
                },
                ArchiveV2HotInstruction {
                    program_id_index: 1,
                    accounts: vec![0],
                    data: ArchiveV2HotInstructionData::System(
                        ArchiveV2SystemInstructionData::Allocate { space: 15 },
                    ),
                },
            ],
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Id(16),
                writable_indexes: vec![0],
                readonly_indexes: vec![1],
            }],
        });
        let source = wincode::config::serialize(&source, wincode_leb128_config()).unwrap();
        let expected = wincode::config::serialize(&expected, wincode_leb128_config()).unwrap();
        assert_eq!(source.len(), expected.len());

        let mut output = Vec::with_capacity(source.len());
        let stats = transcode_archive_v2_hot_message_wire_pre_to_post(
            &source,
            &mut output,
            &mut ArchiveV2WireIdentityVisitor,
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap();

        assert_eq!(output, expected);
        assert_eq!(stats.input_bytes, stats.output_bytes);
        assert_eq!(
            stats.source_instruction_data_tag_counts,
            [1, 0, 1, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(stats.eligible_pubkey_references, 3);
    }

    #[test]
    fn pre_to_post_transcoder_rejects_bad_input_and_rolls_back() {
        // Historical Legacy message with one account and one instruction whose Pre tag 7 is not
        // part of the frozen tag table. Every scalar uses its one-byte canonical LEB128 form.
        let unsupported = [
            0, // Legacy message
            0, 0, 0, // header
            1, // account-key count
            1, // CompactPubkey Id(1)
            0, 0, // recent blockhash Id(0)
            1, // instruction count
            0, // program-id index
            0, // account-index count
            7, // unsupported historical instruction-data tag
        ];
        let prefix = vec![0xc1, 0xc2];
        let mut output = prefix.clone();
        let mut visitor = RecordingVisitor::default();
        let error = transcode_archive_v2_hot_message_wire_pre_to_post(
            &unsupported,
            &mut output,
            &mut visitor,
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind(), ArchiveV2WireRewriteErrorKind::InvalidInput);
        assert_eq!(output, prefix);
        assert!(visitor.pubkeys.is_empty());
        assert_eq!(visitor.commits, 0);
        assert_eq!(visitor.rollbacks, 1);

        let mut valid = wincode::config::serialize(
            &HistoricalMessagePayload::Legacy(HistoricalLegacyMessage {
                header: CompactMessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: vec![CompactPubkey::Id(1)],
                recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
                instructions: vec![HistoricalInstruction {
                    program_id_index: 0,
                    accounts: vec![0],
                    data: HistoricalInstructionData::Raw(vec![9]),
                }],
            }),
            wincode_leb128_config(),
        )
        .unwrap();
        valid.push(0xff);
        let mut output = prefix.clone();
        let mut visitor = RecordingVisitor::default();
        let error = transcode_archive_v2_hot_message_wire_pre_to_post(
            &valid,
            &mut output,
            &mut visitor,
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind(), ArchiveV2WireRewriteErrorKind::InvalidInput);
        assert_eq!(output, prefix);
        assert!(visitor.pubkeys.is_empty());
        assert_eq!(visitor.commits, 0);
        assert_eq!(visitor.rollbacks, 1);
    }

    fn pubkey_or_string(id: u32, offset: u32) -> system_program::PubkeyOrString {
        system_program::PubkeyOrString::Pubkey(pk(id, offset))
    }

    fn all_key_bearing_log_events(offset: u32) -> Vec<LogEvent> {
        use system_program::{NonceAction, SystemAddress, SystemProgramLog};

        #[allow(unused_mut)] // `known-drift` appends one feature-gated fixture below.
        let mut events = vec![
            LogEvent::LogTruncated,
            LogEvent::StakeMergingAccounts,
            LogEvent::LoaderUpgradedProgram {
                program: pk(100, offset),
            },
            LogEvent::LoaderFinalizedAccount {
                account: pk(101, offset),
            },
            LogEvent::ProgramLog(ProgramLog::Token2022(
                token_2022::Token2022Log::ErrorHarvestingFrom {
                    account_key: pk(102, offset),
                    error: 1,
                },
            )),
            LogEvent::ProgramLog(ProgramLog::Token2022(token_2022::Token2022Log::Error(
                token_2022::Token2022ErrorLog::InsufficientFunds,
            ))),
            LogEvent::ProgramLog(ProgramLog::Token2022(token_2022::Token2022Log::Static(
                token_2022::Token2022StaticLog::InstructionTransfer,
            ))),
            LogEvent::ProgramLog(ProgramLog::Token2022(
                token_2022::Token2022Log::CalculatedFee {
                    calculated_fee: 30,
                    fee: 31,
                },
            )),
            LogEvent::ProgramLog(ProgramLog::Token2022(
                token_2022::Token2022Log::AccountNeedsResizePlusBytesDebug { bytes: 32 },
            )),
            LogEvent::ProgramLog(ProgramLog::Token2022(
                token_2022::Token2022Log::AccountNeedsResizePlusBytesDebug2 { bytes: 33 },
            )),
            LogEvent::ProgramLogError { msg: 10 },
            LogEvent::ProgramIdLog {
                program: pk(103, offset),
                log: ProgramLog::Token2022(token_2022::Token2022Log::ErrorHarvestingFrom2 {
                    account_key: pk(104, offset),
                    error: 2,
                }),
            },
            LogEvent::ProgramPlainLog(ProgramLog::Token2022(
                token_2022::Token2022Log::ErrorHarvestingFrom3 {
                    account_key: pk(105, offset),
                    error: 3,
                },
            )),
            LogEvent::ProgramAccountNotWritable,
            LogEvent::ProgramIdMismatch,
            LogEvent::ProgramNotUpgradeable,
            LogEvent::ProgramAndProgramDataAccountMismatch,
            LogEvent::ProgramWasExtendedInThisBlockAlready,
            LogEvent::Invoke {
                program: pk(106, offset),
                depth: 2,
            },
            LogEvent::BpfInvoke {
                program: pk(107, offset),
            },
            LogEvent::Consumed {
                program: pk(108, offset),
                used: 1,
                limit: 2,
            },
            LogEvent::BpfConsumed { used: 3, limit: 4 },
            LogEvent::Success {
                program: pk(109, offset),
            },
            LogEvent::BpfSuccess {
                program: pk(110, offset),
            },
            LogEvent::Failure {
                program: pk(111, offset),
                reason: 4,
            },
            LogEvent::BpfFailure {
                program: pk(112, offset),
                reason: 5,
            },
            LogEvent::FailureCustomProgramError {
                program: pk(113, offset),
                code: 6,
            },
            LogEvent::BpfFailureCustomProgramError {
                program: pk(114, offset),
                code: 7,
            },
            LogEvent::FailureInvalidAccountData {
                program: pk(115, offset),
            },
            LogEvent::BpfFailureInvalidAccountData {
                program: pk(116, offset),
            },
            LogEvent::FailureInvalidProgramArgument {
                program: pk(117, offset),
            },
            LogEvent::BpfFailureInvalidProgramArgument {
                program: pk(118, offset),
            },
            LogEvent::FailedToComplete { reason: 11 },
            LogEvent::CustomProgramError { code: 12 },
            LogEvent::Return {
                program: pk(119, offset),
                data: 8,
            },
            LogEvent::Data { data: 13 },
            LogEvent::Consumption { units: 14 },
            LogEvent::CbRequestUnits { units: 15 },
            LogEvent::ProgramNotDeployed {
                program: Some(pk(120, offset)),
            },
            LogEvent::ProgramNotCached {
                program: Some(pk(121, offset)),
            },
            LogEvent::UnknownProgram { program: 16 },
            LogEvent::UnknownAccount { account: 17 },
            LogEvent::VerifyEd25519,
            LogEvent::VerifySecp256k1,
            LogEvent::RuntimeWritablePrivilegeEscalated {
                account: pk(122, offset),
            },
            LogEvent::RuntimeSignerPrivilegeEscalated {
                account: pk(123, offset),
            },
            LogEvent::RuntimeAccountOwnerBalanceVerificationFailed {
                account: pk(124, offset),
            },
            LogEvent::CloseContextState,
            LogEvent::Plain { text: 18 },
            LogEvent::Unparsed { text: 19 },
            LogEvent::System(SystemProgramLog::CreateAddressMismatch {
                provided_addr: pk(125, offset),
                derived_addr: pubkey_or_string(126, offset),
            }),
            LogEvent::System(SystemProgramLog::Instruction(
                system_program::SystemInstructionLog::RevokePendingActivation,
            )),
            LogEvent::System(SystemProgramLog::CreateAccountAlreadyInUse {
                addr: SystemAddress::Debug {
                    address: pubkey_or_string(127, offset),
                    base: Some(pubkey_or_string(128, offset)),
                },
            }),
            LogEvent::System(SystemProgramLog::AllocateAlreadyInUse {
                addr: SystemAddress::Pubkey(pubkey_or_string(129, offset)),
            }),
            LogEvent::System(SystemProgramLog::AllocateToMustSign {
                addr: SystemAddress::Pubkey(pubkey_or_string(130, offset)),
            }),
            LogEvent::System(SystemProgramLog::AllocateAccountAlreadyInUse {
                addr: SystemAddress::Pubkey(pubkey_or_string(131, offset)),
            }),
            LogEvent::System(SystemProgramLog::AllocateRequestedTooLarge {
                requested: 20,
                max_allowed: 21,
            }),
            LogEvent::System(SystemProgramLog::AssignAccountMustSign {
                addr: SystemAddress::Pubkey(pubkey_or_string(132, offset)),
            }),
            LogEvent::System(SystemProgramLog::CreateAccountAccountAlreadyInUse {
                addr: SystemAddress::Pubkey(pubkey_or_string(133, offset)),
            }),
            LogEvent::System(
                SystemProgramLog::CreateAccountDataSizeLimitedInInnerInstructions { limit: 22 },
            ),
            LogEvent::System(SystemProgramLog::TransferFromMustNotCarryData),
            LogEvent::System(SystemProgramLog::TransferFromMustSign {
                from: pk(134, offset),
            }),
            LogEvent::System(SystemProgramLog::TransferInsufficient { have: 23, need: 24 }),
            LogEvent::System(SystemProgramLog::TransferFromAddressMismatch {
                provided_addr: pk(135, offset),
                derived_addr: pubkey_or_string(136, offset),
            }),
            LogEvent::System(SystemProgramLog::AdvanceNonceRecentBlockhashesEmpty),
            LogEvent::System(SystemProgramLog::InitializeNonceRecentBlockhashesEmpty),
            LogEvent::System(SystemProgramLog::AuthorizeNonceAccount { msg: 25 }),
            LogEvent::System(SystemProgramLog::NonceAccountMustBeWriteable {
                action: NonceAction::Advance,
                account: pubkey_or_string(137, offset),
            }),
            LogEvent::System(SystemProgramLog::NonceAccountMustBeSigner {
                action: NonceAction::Withdraw,
                account: pubkey_or_string(138, offset),
            }),
            LogEvent::System(SystemProgramLog::NonceAccountMustSign {
                action: NonceAction::Initialize,
                account: pubkey_or_string(139, offset),
            }),
            LogEvent::System(SystemProgramLog::NonceAccountStateInvalid {
                action: NonceAction::Authorize,
                account: pubkey_or_string(140, offset),
            }),
            LogEvent::System(SystemProgramLog::NonceInsufficientLamports {
                action: NonceAction::Advance,
                have: 26,
                need: 27,
            }),
            LogEvent::System(SystemProgramLog::NonceCanOnlyAdvanceOncePerSlot {
                action: NonceAction::Withdraw,
            }),
            LogEvent::ProgramLog(ProgramLog::Token2022(
                token_2022::Token2022Log::ErrorHarvestingFrom4 {
                    account_key: pk(141, offset),
                    error: 9,
                },
            )),
        ];

        #[cfg(all(feature = "known-program-logs", feature = "known-drift"))]
        events.push(LogEvent::ProgramLog(ProgramLog::Known(
            crate::program_logs::known_programs::KnownProgramLog::Drift(
                crate::program_logs::known_programs::drift::DriftLog::Event(vec![1, 2, 3]),
            ),
        )));
        events
    }

    fn metadata_fixture(offset: u32) -> CompactMetaV1 {
        CompactMetaV1 {
            err: None,
            fee: 5000,
            pre_balances: vec![10, 20],
            post_balances: vec![9, 21],
            inner_instructions: Some(vec![CompactInnerInstructions {
                index: 0,
                instructions: vec![CompactInnerInstruction {
                    program_id_index: 1,
                    accounts: vec![0, 1],
                    data: vec![2, 3, 4],
                    stack_height: Some(2),
                }],
            }]),
            logs: Some(CompactLogStream {
                events: all_key_bearing_log_events(offset),
                strings: StringTable {
                    lengths: vec![3, 2],
                    bytes: b"abcde".to_vec(),
                },
                data: DataTable {
                    arrays: vec![DataArray { chunk_count: 2 }],
                    chunk_lengths: vec![2, 3],
                    bytes: vec![1, 2, 3, 4, 5],
                },
            }),
            pre_token_balances: vec![CompactTokenBalance {
                account_index: 0,
                mint: Some(pk(200, offset)),
                owner: Some(pk(201, offset)),
                program_id: Some(pk(202, offset)),
                amount: 42,
                decimals: 9,
            }],
            post_token_balances: vec![CompactTokenBalance {
                account_index: 1,
                mint: Some(pk(203, offset)),
                owner: Some(pk(204, offset)),
                program_id: Some(pk(205, offset)),
                amount: 43,
                decimals: 6,
            }],
            rewards: vec![CompactReward {
                pubkey: pk(206, offset),
                lamports: -7,
                post_balance: 44,
                reward_type: -2,
                commission: Some(5),
            }],
            loaded_writable_addresses: vec![pk(207, offset)],
            loaded_readonly_addresses: vec![pk(208, offset)],
            return_data: Some(CompactReturnData {
                program_id: pk(209, offset),
                data: vec![8, 9],
            }),
            compute_units_consumed: Some(123),
            cost_units: Some(456),
        }
    }

    #[test]
    fn metadata_transform_is_byte_differential_for_every_pubkey_location() {
        let source =
            wincode::config::serialize(&metadata_fixture(0), wincode_leb128_config()).unwrap();
        let expected =
            wincode::config::serialize(&metadata_fixture(DELTA), wincode_leb128_config()).unwrap();
        let mut output = vec![0xfe];
        let mut visitor = RecordingVisitor::default();
        let stats = rewrite_archive_v2_successful_metadata_wire(
            &source,
            &mut output,
            &mut visitor,
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap();

        assert_eq!(&output[1..], expected);
        assert_eq!(stats.eligible_pubkey_references, 10);
        assert_eq!(
            stats.excluded_pubkey_references,
            visitor
                .pubkeys
                .iter()
                .filter(|entry| entry.2 == ArchiveV2WireReferenceClass::Excluded)
                .count()
        );
        assert!(stats.excluded_pubkey_references >= 42);
        assert_eq!(visitor.commits, 1);
        assert_eq!(visitor.rollbacks, 0);
    }

    #[test]
    fn current_error_metadata_streams_to_exact_owned_bytes() {
        let mut source_value = metadata_fixture(0);
        source_value.err = Some(crate::CompactTransactionError::AccountInUse);
        let source = wincode::config::serialize(&source_value, wincode_leb128_config()).unwrap();
        let mut expected_value = metadata_fixture(DELTA);
        expected_value.err = Some(crate::CompactTransactionError::AccountInUse);
        let expected =
            wincode::config::serialize(&expected_value, wincode_leb128_config()).unwrap();

        let mut output = vec![0xc1];
        let mut visitor = RecordingVisitor::default();
        let stats = rewrite_archive_v2_metadata_wire(
            &source,
            &mut output,
            &mut visitor,
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap();

        assert_eq!(&output[1..], expected);
        assert_eq!(
            stats.metadata_error_schema,
            Some(ArchiveV2WireMetadataErrorSchema::Current)
        );
        assert_eq!(visitor.commits, 1);
        assert_eq!(visitor.rollbacks, 0);
    }

    #[test]
    fn unambiguous_legacy_error_metadata_streams_to_current_owned_bytes() {
        let successful =
            wincode::config::serialize(&metadata_fixture(0), wincode_leb128_config()).unwrap();
        assert_eq!(successful.first(), Some(&0));
        let stored = wincode::serialize(&StoredTransactionError::InstructionError(
            7,
            StoredInstructionError::BorshIoError("x".repeat(96)),
        ))
        .unwrap();
        assert!(
            stored.len() > 39,
            "legacy prefix must reject the current tag schema"
        );
        let mut source =
            wincode::config::serialize(&Some(stored.clone()), wincode_leb128_config()).unwrap();
        source.extend_from_slice(&successful[1..]);

        let mut expected_value = metadata_fixture(DELTA);
        expected_value.err = Some(crate::CompactTransactionError::InstructionError(
            7,
            crate::CompactInstructionError::BorshIoError("x".repeat(96)),
        ));
        let expected =
            wincode::config::serialize(&expected_value, wincode_leb128_config()).unwrap();
        let mut output = Vec::new();
        let mut visitor = RecordingVisitor::default();
        let stats = rewrite_archive_v2_metadata_wire(
            &source,
            &mut output,
            &mut visitor,
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap();

        assert_eq!(output, expected);
        assert_eq!(
            stats.metadata_error_schema,
            Some(ArchiveV2WireMetadataErrorSchema::Legacy)
        );
        assert_eq!(visitor.commits, 1);
        assert_eq!(visitor.rollbacks, 0);

        let expected_successful =
            wincode::config::serialize(&metadata_fixture(DELTA), wincode_leb128_config()).unwrap();
        let mut preserved_expected =
            wincode::config::serialize(&Some(stored), wincode_leb128_config()).unwrap();
        preserved_expected.extend_from_slice(&expected_successful[1..]);
        let mut preserved = Vec::new();
        let mut preserving_visitor = RecordingVisitor::default();
        let preserving_stats = rewrite_archive_v2_metadata_wire_preserving_error_schema(
            &source,
            &mut preserved,
            &mut preserving_visitor,
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap();
        assert_eq!(preserved, preserved_expected);
        assert_eq!(
            preserving_stats.metadata_error_schema,
            Some(ArchiveV2WireMetadataErrorSchema::Legacy)
        );
        assert_eq!(preserving_visitor.commits, 1);
        assert_eq!(preserving_visitor.rollbacks, 0);
    }

    #[test]
    fn selected_error_prefix_splitter_borrows_current_and_legacy_tails() {
        let successful =
            wincode::config::serialize(&metadata_fixture(0), wincode_leb128_config()).unwrap();
        assert_eq!(successful.first(), Some(&0));

        let mut current_value = metadata_fixture(0);
        current_value.err = Some(crate::CompactTransactionError::InstructionError(
            7,
            crate::CompactInstructionError::GenericError,
        ));
        let current = wincode::config::serialize(&current_value, wincode_leb128_config()).unwrap();
        let current_tail = validate_archive_v2_metadata_error_prefix_for_selected_schema(
            &current,
            ArchiveV2WireMetadataErrorSchema::Current,
            current.len(),
        )
        .unwrap();
        assert!(current_tail.has_error);
        assert_eq!(current_tail.bytes, &successful[1..]);
        assert_eq!(
            current_tail.error_index,
            Some(ArchiveV2WireMetadataErrorIndex::TopLevelInstruction(7))
        );

        let stored = wincode::serialize(&StoredTransactionError::InstructionError(
            7,
            StoredInstructionError::GenericError,
        ))
        .unwrap();
        let mut legacy =
            wincode::config::serialize(&Some(stored), wincode_leb128_config()).unwrap();
        legacy.extend_from_slice(&successful[1..]);
        let legacy_tail = validate_archive_v2_metadata_error_prefix_for_selected_schema(
            &legacy,
            ArchiveV2WireMetadataErrorSchema::Legacy,
            legacy.len(),
        )
        .unwrap();
        assert!(legacy_tail.has_error);
        assert_eq!(legacy_tail.bytes, &successful[1..]);
        assert_eq!(
            legacy_tail.error_index,
            Some(ArchiveV2WireMetadataErrorIndex::TopLevelInstruction(7))
        );

        let successful_tail = validate_archive_v2_metadata_error_prefix_for_selected_schema(
            &successful,
            ArchiveV2WireMetadataErrorSchema::Legacy,
            successful.len(),
        )
        .unwrap();
        assert!(!successful_tail.has_error);
        assert_eq!(successful_tail.bytes, &successful[1..]);
        assert_eq!(successful_tail.error_index, None);
    }

    #[test]
    fn selected_legacy_schema_resolves_an_ambiguous_prefix_without_changing_it() {
        let successful =
            wincode::config::serialize(&metadata_fixture(0), wincode_leb128_config()).unwrap();
        let expected_successful =
            wincode::config::serialize(&metadata_fixture(DELTA), wincode_leb128_config()).unwrap();
        let stored = wincode::serialize(&StoredTransactionError::AccountInUse).unwrap();
        assert_eq!(stored, [0, 0, 0, 0]);

        let mut source =
            wincode::config::serialize(&Some(stored.clone()), wincode_leb128_config()).unwrap();
        source.extend_from_slice(&successful[1..]);
        let mut expected =
            wincode::config::serialize(&Some(stored), wincode_leb128_config()).unwrap();
        expected.extend_from_slice(&expected_successful[1..]);

        let prefix = vec![0xe1, 0xe2];
        let mut probing_output = prefix.clone();
        let mut probing_visitor = RecordingVisitor::default();
        let probing_error = rewrite_archive_v2_metadata_wire_preserving_error_schema(
            &source,
            &mut probing_output,
            &mut probing_visitor,
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap_err();
        assert_eq!(
            probing_error.fallback_reason(),
            Some(ArchiveV2WireFallbackReason::MetadataErrorSchemaAmbiguous)
        );
        assert_eq!(probing_output, prefix);
        assert!(probing_visitor.pubkeys.is_empty());
        assert_eq!(probing_visitor.rollbacks, 1);

        let mut output = Vec::new();
        let mut visitor = RecordingVisitor::default();
        let stats = rewrite_archive_v2_metadata_wire_preserving_selected_error_schema(
            &source,
            &mut output,
            &mut visitor,
            ArchiveV2WireRewriteLimits::default(),
            ArchiveV2WireMetadataErrorSchema::Legacy,
        )
        .unwrap();
        assert_eq!(output, expected);
        assert_eq!(
            stats.metadata_error_schema,
            Some(ArchiveV2WireMetadataErrorSchema::Legacy)
        );

        let mut identity_output = Vec::new();
        let mut identity = ArchiveV2WireIdentityVisitor;
        rewrite_archive_v2_metadata_wire_preserving_selected_error_schema(
            &source,
            &mut identity_output,
            &mut identity,
            ArchiveV2WireRewriteLimits::default(),
            ArchiveV2WireMetadataErrorSchema::Legacy,
        )
        .unwrap();
        assert_eq!(identity_output, source);
    }

    #[test]
    fn selected_current_schema_preserves_its_prefix_and_identity_bytes() {
        let mut source_value = metadata_fixture(0);
        source_value.err = Some(crate::CompactTransactionError::InstructionError(
            7,
            crate::CompactInstructionError::BorshIoError("current-error".to_owned()),
        ));
        let source = wincode::config::serialize(&source_value, wincode_leb128_config()).unwrap();
        let mut expected_value = metadata_fixture(DELTA);
        expected_value.err = Some(crate::CompactTransactionError::InstructionError(
            7,
            crate::CompactInstructionError::BorshIoError("current-error".to_owned()),
        ));
        let expected =
            wincode::config::serialize(&expected_value, wincode_leb128_config()).unwrap();

        let mut output = Vec::new();
        let mut visitor = RecordingVisitor::default();
        let stats = rewrite_archive_v2_metadata_wire_preserving_selected_error_schema(
            &source,
            &mut output,
            &mut visitor,
            ArchiveV2WireRewriteLimits::default(),
            ArchiveV2WireMetadataErrorSchema::Current,
        )
        .unwrap();
        assert_eq!(output, expected);
        assert_eq!(
            stats.metadata_error_schema,
            Some(ArchiveV2WireMetadataErrorSchema::Current)
        );

        let mut identity_output = Vec::new();
        let mut identity = ArchiveV2WireIdentityVisitor;
        rewrite_archive_v2_metadata_wire_preserving_selected_error_schema(
            &source,
            &mut identity_output,
            &mut identity,
            ArchiveV2WireRewriteLimits::default(),
            ArchiveV2WireMetadataErrorSchema::Current,
        )
        .unwrap();
        assert_eq!(identity_output, source);
    }

    #[test]
    fn selected_error_string_obeys_the_runtime_item_limit_without_partial_state() {
        let mut value = metadata_fixture(0);
        value.err = Some(crate::CompactTransactionError::InstructionError(
            0,
            crate::CompactInstructionError::BorshIoError("0123456789abcdef".to_owned()),
        ));
        let source = wincode::config::serialize(&value, wincode_leb128_config()).unwrap();
        let limits = ArchiveV2WireRewriteLimits {
            max_sequence_items: 8,
            ..ArchiveV2WireRewriteLimits::default()
        };
        let prefix = vec![0xf1];
        let mut output = prefix.clone();
        let mut visitor = RecordingVisitor::default();
        let error = rewrite_archive_v2_metadata_wire_preserving_selected_error_schema(
            &source,
            &mut output,
            &mut visitor,
            limits,
            ArchiveV2WireMetadataErrorSchema::Current,
        )
        .unwrap_err();

        assert_eq!(error.kind(), ArchiveV2WireRewriteErrorKind::LimitExceeded);
        assert_eq!(output, prefix);
        assert!(visitor.pubkeys.is_empty());
        assert_eq!(visitor.commits, 0);
        assert_eq!(visitor.rollbacks, 1);
    }

    #[test]
    fn selected_schemas_reject_nonexistent_transaction_error_tag_39() {
        let successful =
            wincode::config::serialize(&metadata_fixture(0), wincode_leb128_config()).unwrap();
        let mut current = vec![1, 39];
        current.extend_from_slice(&successful[1..]);

        let prefix = vec![0xfa];
        let mut current_output = prefix.clone();
        let mut current_visitor = RecordingVisitor::default();
        let current_error = rewrite_archive_v2_metadata_wire_preserving_selected_error_schema(
            &current,
            &mut current_output,
            &mut current_visitor,
            ArchiveV2WireRewriteLimits::default(),
            ArchiveV2WireMetadataErrorSchema::Current,
        )
        .unwrap_err();
        assert_eq!(
            current_error.kind(),
            ArchiveV2WireRewriteErrorKind::InvalidInput
        );
        assert_eq!(current_output, prefix);
        assert_eq!(current_visitor.rollbacks, 1);

        let mut legacy = wincode::config::serialize(
            &Some(39u32.to_le_bytes().to_vec()),
            wincode_leb128_config(),
        )
        .unwrap();
        legacy.extend_from_slice(&successful[1..]);
        let mut legacy_output = prefix.clone();
        let mut legacy_visitor = RecordingVisitor::default();
        let legacy_error = rewrite_archive_v2_metadata_wire_preserving_selected_error_schema(
            &legacy,
            &mut legacy_output,
            &mut legacy_visitor,
            ArchiveV2WireRewriteLimits::default(),
            ArchiveV2WireMetadataErrorSchema::Legacy,
        )
        .unwrap_err();
        assert_eq!(
            legacy_error.kind(),
            ArchiveV2WireRewriteErrorKind::InvalidInput
        );
        assert_eq!(legacy_output, prefix);
        assert_eq!(legacy_visitor.rollbacks, 1);
    }

    #[test]
    fn ambiguous_error_prefix_requests_owned_value_level_rule_transactionally() {
        // Current tag 4 is a unit error. The legacy interpretation is a four-byte stored
        // AccountInUse error. Both prefixes are valid, so only the complete owned decoders can
        // select a value safely.
        let mut source = vec![1, 4, 0, 0, 0, 0];
        source.extend_from_slice(&[0; 13]);
        assert!(validate_current_metadata_error_prefix(&source[1..], usize::MAX).is_ok());
        assert!(validate_legacy_metadata_error_prefix(&source[1..], usize::MAX).is_ok());

        let prefix = vec![0xa1, 0xa2];
        let mut output = prefix.clone();
        let mut visitor = RecordingVisitor::default();
        let error = rewrite_archive_v2_metadata_wire(
            &source,
            &mut output,
            &mut visitor,
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap_err();

        assert_eq!(
            error.fallback_reason(),
            Some(ArchiveV2WireFallbackReason::MetadataErrorSchemaAmbiguous)
        );
        assert_eq!(output, prefix);
        assert!(visitor.pubkeys.is_empty());
        assert_eq!(visitor.commits, 0);
        assert_eq!(visitor.rollbacks, 1);
    }

    #[test]
    fn admitted_current_metadata_does_not_probe_the_legacy_error_prefix() {
        // Current tag 4 is also a valid four-byte legacy error length prefix.
        // Generation admission or the owned ambiguity rule has already selected
        // the current value before this entry point is called.
        let fixture = |offset| CompactMetaV1 {
            err: Some(crate::CompactTransactionError::InsufficientFundsForFee),
            fee: 0,
            pre_balances: Vec::new(),
            post_balances: Vec::new(),
            inner_instructions: None,
            logs: None,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: vec![pk(207, offset)],
            loaded_readonly_addresses: Vec::new(),
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
        };
        let source_value = fixture(0);
        let source = wincode::config::serialize(&source_value, wincode_leb128_config()).unwrap();
        let expected_value = fixture(DELTA);
        let expected =
            wincode::config::serialize(&expected_value, wincode_leb128_config()).unwrap();

        let mut probing_output = Vec::new();
        let mut probing_visitor = RecordingVisitor::default();
        let probing_error = rewrite_archive_v2_metadata_wire(
            &source,
            &mut probing_output,
            &mut probing_visitor,
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap_err();
        assert_eq!(
            probing_error.fallback_reason(),
            Some(ArchiveV2WireFallbackReason::MetadataErrorSchemaAmbiguous)
        );

        let mut output = Vec::new();
        let mut visitor = RecordingVisitor::default();
        let stats = rewrite_archive_v2_current_metadata_wire(
            &source,
            &mut output,
            &mut visitor,
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap();

        assert_eq!(output, expected);
        assert_eq!(
            stats.metadata_error_schema,
            Some(ArchiveV2WireMetadataErrorSchema::Current)
        );
        assert_eq!(visitor.commits, 1);
        assert_eq!(visitor.rollbacks, 0);
    }

    #[test]
    fn malformed_selected_error_tail_requests_owned_rollback_without_partial_state() {
        let mut value = metadata_fixture(0);
        value.err = Some(crate::CompactTransactionError::AccountInUse);
        let mut source = wincode::config::serialize(&value, wincode_leb128_config()).unwrap();
        source.truncate(source.len() - 1);

        let prefix = vec![0xb1, 0xb2];
        let mut output = prefix.clone();
        let mut visitor = RecordingVisitor::default();
        let error = rewrite_archive_v2_metadata_wire(
            &source,
            &mut output,
            &mut visitor,
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap_err();

        assert_eq!(
            error.fallback_reason(),
            Some(ArchiveV2WireFallbackReason::MetadataErrorWireRollback)
        );
        assert_eq!(output, prefix);
        assert!(visitor.pubkeys.is_empty());
        assert_eq!(visitor.commits, 0);
        assert_eq!(visitor.rollbacks, 1);
    }

    #[test]
    fn current_error_visitor_failure_rolls_back_before_owned_retry() {
        let mut value = metadata_fixture(0);
        value.err = Some(crate::CompactTransactionError::AccountInUse);
        let source = wincode::config::serialize(&value, wincode_leb128_config()).unwrap();

        let prefix = vec![0xd1];
        let mut output = prefix.clone();
        let mut visitor = RecordingVisitor {
            fail_after: Some(5),
            ..RecordingVisitor::default()
        };
        let error = rewrite_archive_v2_metadata_wire(
            &source,
            &mut output,
            &mut visitor,
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap_err();

        assert_eq!(
            error.fallback_reason(),
            Some(ArchiveV2WireFallbackReason::MetadataErrorWireRollback)
        );
        assert_eq!(output, prefix);
        assert!(visitor.pubkeys.is_empty());
        assert_eq!(visitor.commits, 0);
        assert_eq!(visitor.rollbacks, 1);
    }

    #[test]
    fn overlong_leb_values_are_rejected_by_the_canonical_input_grammar() {
        // Legacy(0), zero header, one key, key Id(1), recent Id(0), no instructions. Every u32,
        // i32, or sequence length that can be overlong is deliberately overlong here.
        let input = [
            0x80, 0x00, // message variant 0
            0, 0, 0, // header
            0x81, 0x00, // account-key count 1
            0x81, 0x00, // CompactPubkey Id(1)
            0x80, 0x00, // recent-blockhash variant 0
            0x80, 0x00, // recent-blockhash Id(0), zig-zag encoded
            0x80, 0x00, // instruction count 0
        ];
        let mut output = Vec::new();
        let error = rewrite_archive_v2_hot_message_wire(
            &input,
            &mut output,
            &mut RecordingVisitor::default(),
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("non-minimal LEB128 integer"));
        assert!(output.is_empty());
    }

    #[test]
    fn fallback_invalid_and_trailing_failures_are_distinct_and_transactional() {
        let prefix = vec![0x11, 0x22];

        let mut output = prefix.clone();
        let mut visitor = RecordingVisitor::default();
        let error = rewrite_archive_v2_hot_message_wire(
            &[99],
            &mut output,
            &mut visitor,
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap_err();
        assert_eq!(
            error.kind(),
            ArchiveV2WireRewriteErrorKind::Fallback(
                ArchiveV2WireFallbackReason::UnsupportedMessageVariant(99)
            )
        );
        assert_eq!(output, prefix);
        assert_eq!(visitor.rollbacks, 1);

        for malformed in [&[][..], &[0][..], &[0, 0, 0, 0, 1][..]] {
            let mut output = prefix.clone();
            let mut visitor = RecordingVisitor::default();
            let error = rewrite_archive_v2_hot_message_wire(
                malformed,
                &mut output,
                &mut visitor,
                ArchiveV2WireRewriteLimits::default(),
            )
            .unwrap_err();
            assert_eq!(error.kind(), ArchiveV2WireRewriteErrorKind::InvalidInput);
            assert_eq!(output, prefix);
            assert!(visitor.pubkeys.is_empty());
        }

        let mut valid =
            wincode::config::serialize(&message_fixture(0, false), wincode_leb128_config())
                .unwrap();
        valid.push(0xff);
        let mut output = prefix.clone();
        let mut visitor = RecordingVisitor::default();
        let error = rewrite_archive_v2_hot_message_wire(
            &valid,
            &mut output,
            &mut visitor,
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind(), ArchiveV2WireRewriteErrorKind::InvalidInput);
        assert_eq!(output, prefix);
        assert!(visitor.pubkeys.is_empty());

        let mut metadata = metadata_fixture(0);
        metadata.err = Some(crate::CompactTransactionError::AccountInUse);
        let bytes = wincode::config::serialize(&metadata, wincode_leb128_config()).unwrap();
        let mut output = prefix.clone();
        let mut visitor = RecordingVisitor::default();
        let error = rewrite_archive_v2_successful_metadata_wire(
            &bytes,
            &mut output,
            &mut visitor,
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap_err();
        assert_eq!(
            error.fallback_reason(),
            Some(ArchiveV2WireFallbackReason::MetadataHasTransactionError)
        );
        assert_eq!(output, prefix);
        assert!(visitor.pubkeys.is_empty());
    }

    #[test]
    fn visitor_failure_rolls_back_output_and_events() {
        let source =
            wincode::config::serialize(&metadata_fixture(0), wincode_leb128_config()).unwrap();
        let mut output = vec![1, 2, 3];
        let mut visitor = RecordingVisitor {
            fail_after: Some(5),
            ..RecordingVisitor::default()
        };
        let error = rewrite_archive_v2_successful_metadata_wire(
            &source,
            &mut output,
            &mut visitor,
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind(), ArchiveV2WireRewriteErrorKind::Visitor);
        assert_eq!(output, [1, 2, 3]);
        assert!(visitor.pubkeys.is_empty());
        assert_eq!(visitor.rollbacks, 1);
        assert_eq!(visitor.commits, 0);
    }

    #[test]
    fn id_to_raw_expansion_matches_owned_codec_exactly() {
        let source_value = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 0,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Raw([2; 32])],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![],
        });
        let expected_value = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 0,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Raw([1; 32]), CompactPubkey::Raw([2; 32])],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![],
        });
        let source = wincode::config::serialize(&source_value, wincode_leb128_config()).unwrap();
        let expected =
            wincode::config::serialize(&expected_value, wincode_leb128_config()).unwrap();
        let mut output = Vec::new();
        let mut visitor = RecordingVisitor {
            expand_ids: true,
            ..RecordingVisitor::default()
        };
        rewrite_archive_v2_hot_message_wire(
            &source,
            &mut output,
            &mut visitor,
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap();
        assert_eq!(output, expected);
        assert!(output.len() > source.len());
    }

    #[test]
    fn every_full_fixture_truncation_is_invalid_and_transactional() {
        let message =
            wincode::config::serialize(&message_fixture(0, true), wincode_leb128_config()).unwrap();
        for end in 0..message.len() {
            let mut output = vec![0xa5];
            let mut visitor = RecordingVisitor::default();
            let error = rewrite_archive_v2_hot_message_wire(
                &message[..end],
                &mut output,
                &mut visitor,
                ArchiveV2WireRewriteLimits::default(),
            )
            .unwrap_err();
            assert_eq!(
                error.kind(),
                ArchiveV2WireRewriteErrorKind::InvalidInput,
                "message truncation at byte {end}: {error}"
            );
            assert_eq!(output, [0xa5]);
            assert!(visitor.pubkeys.is_empty());
            assert!(visitor.recent_blockhashes.is_empty());
            assert!(visitor.vote_hashes.is_empty());
        }

        let metadata =
            wincode::config::serialize(&metadata_fixture(0), wincode_leb128_config()).unwrap();
        for end in 0..metadata.len() {
            let mut output = vec![0x5a];
            let mut visitor = RecordingVisitor::default();
            let error = rewrite_archive_v2_successful_metadata_wire(
                &metadata[..end],
                &mut output,
                &mut visitor,
                ArchiveV2WireRewriteLimits::default(),
            )
            .unwrap_err();
            assert_eq!(
                error.kind(),
                ArchiveV2WireRewriteErrorKind::InvalidInput,
                "metadata truncation at byte {end}: {error}"
            );
            assert_eq!(output, [0x5a]);
            assert!(visitor.pubkeys.is_empty());
        }
    }

    #[test]
    fn nested_unsupported_tags_after_callbacks_roll_back() {
        // Legacy message, one account key, recent-blockhash Id(0), and one instruction whose
        // instruction-data tag is unsupported.
        let message = [
            0, // Legacy
            0, 0, 0, // header
            1, // one account key
            1, // CompactPubkey Id(1)
            0, 0,  // recent blockhash Id(0)
            1,  // one instruction
            0,  // program-id index
            0,  // empty account-index vector
            99, // unsupported instruction-data tag
        ];
        let mut output = vec![7];
        let mut visitor = RecordingVisitor::default();
        let error = rewrite_archive_v2_hot_message_wire(
            &message,
            &mut output,
            &mut visitor,
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap_err();
        assert_eq!(
            error.fallback_reason(),
            Some(ArchiveV2WireFallbackReason::UnsupportedInstructionVariant(
                99
            ))
        );
        assert_eq!(output, [7]);
        assert!(visitor.pubkeys.is_empty());

        // Successful metadata through logs, with two events. The first emits a pubkey callback;
        // the second has an unsupported LogEvent tag.
        let metadata = [
            0,  // err=None
            0,  // fee
            0,  // pre balances
            0,  // post balances
            0,  // inner instructions=None
            1,  // logs=Some
            2,  // two events
            3,  // LoaderUpgradedProgram
            1,  // CompactPubkey Id(1)
            99, // unsupported LogEvent
        ];
        let mut output = vec![8];
        let mut visitor = RecordingVisitor::default();
        let error = rewrite_archive_v2_successful_metadata_wire(
            &metadata,
            &mut output,
            &mut visitor,
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap_err();
        assert_eq!(
            error.fallback_reason(),
            Some(ArchiveV2WireFallbackReason::UnsupportedLogVariant {
                schema: "LogEvent",
                tag: 99,
            })
        );
        assert_eq!(output, [8]);
        assert!(visitor.pubkeys.is_empty());
    }

    #[test]
    fn output_limit_failure_after_id_expansion_rolls_back() {
        let source = wincode::config::serialize(
            &ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
                header: CompactMessageHeader {
                    num_required_signatures: 0,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: vec![CompactPubkey::Id(1)],
                recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
                instructions: vec![],
            }),
            wincode_leb128_config(),
        )
        .unwrap();
        let mut output = vec![9, 9];
        let mut visitor = RecordingVisitor {
            expand_ids: true,
            ..RecordingVisitor::default()
        };
        let limits = ArchiveV2WireRewriteLimits {
            max_output_bytes: source.len(),
            ..ArchiveV2WireRewriteLimits::default()
        };
        let error = rewrite_archive_v2_hot_message_wire(&source, &mut output, &mut visitor, limits)
            .unwrap_err();
        assert_eq!(error.kind(), ArchiveV2WireRewriteErrorKind::LimitExceeded);
        assert_eq!(output, [9, 9]);
        assert!(visitor.pubkeys.is_empty());
        assert_eq!(visitor.rollbacks, 1);
    }

    #[test]
    fn hostile_lengths_and_reference_caps_fail_without_partial_state() {
        let mut hostile_message = vec![0, 0, 0, 0];
        hostile_message
            .extend(wincode::config::serialize(&u64::MAX, wincode_leb128_config()).unwrap());
        let mut output = vec![1];
        let mut visitor = RecordingVisitor::default();
        let error = rewrite_archive_v2_hot_message_wire(
            &hostile_message,
            &mut output,
            &mut visitor,
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind(), ArchiveV2WireRewriteErrorKind::LimitExceeded);
        assert_eq!(output, [1]);
        assert!(visitor.pubkeys.is_empty());

        let source = wincode::config::serialize(
            &ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
                header: CompactMessageHeader {
                    num_required_signatures: 0,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
                recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
                instructions: vec![],
            }),
            wincode_leb128_config(),
        )
        .unwrap();
        let mut output = vec![2];
        let mut visitor = RecordingVisitor::default();
        let limits = ArchiveV2WireRewriteLimits {
            max_pubkey_references: 1,
            ..ArchiveV2WireRewriteLimits::default()
        };
        let error = rewrite_archive_v2_hot_message_wire(&source, &mut output, &mut visitor, limits)
            .unwrap_err();
        assert_eq!(error.kind(), ArchiveV2WireRewriteErrorKind::LimitExceeded);
        assert_eq!(output, [2]);
        assert!(visitor.pubkeys.is_empty());
    }

    #[test]
    fn ordinary_byte_vectors_above_reference_cap_keep_owned_codec_parity() {
        let large_len = ARCHIVE_V2_WIRE_REWRITE_MAX_ITEMS + 1;
        let mut source_meta = metadata_fixture(0);
        source_meta.logs = None;
        source_meta.pre_token_balances.clear();
        source_meta.post_token_balances.clear();
        source_meta.rewards.clear();
        source_meta.loaded_writable_addresses.clear();
        source_meta.loaded_readonly_addresses.clear();
        source_meta.return_data = Some(CompactReturnData {
            program_id: CompactPubkey::Raw([9; 32]),
            data: vec![0x5a; large_len],
        });
        let source = wincode::config::serialize(&source_meta, wincode_leb128_config()).unwrap();
        let mut output = Vec::new();
        rewrite_archive_v2_successful_metadata_wire(
            &source,
            &mut output,
            &mut RecordingVisitor::default(),
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap();
        assert_eq!(output, source);
    }

    #[test]
    fn scalar_heavy_output_limit_has_bounded_overshoot_and_rolls_back() {
        let mut metadata = metadata_fixture(0);
        metadata.pre_balances.clear();
        metadata.post_balances.clear();
        metadata.inner_instructions = None;
        metadata.logs = None;
        metadata.pre_token_balances = (0..2_000)
            .map(|index| CompactTokenBalance {
                account_index: index,
                mint: (index == 0).then_some(CompactPubkey::Id(1)),
                owner: None,
                program_id: None,
                amount: index as u64,
                decimals: 0,
            })
            .collect();
        metadata.post_token_balances.clear();
        metadata.rewards.clear();
        metadata.loaded_writable_addresses.clear();
        metadata.loaded_readonly_addresses.clear();
        metadata.return_data = None;
        metadata.compute_units_consumed = None;
        metadata.cost_units = None;
        let source = wincode::config::serialize(&metadata, wincode_leb128_config()).unwrap();
        let limits = ArchiveV2WireRewriteLimits {
            max_output_bytes: 64,
            ..ArchiveV2WireRewriteLimits::default()
        };

        // Inspect the internal failure point before the public transaction wrapper truncates it.
        // The periodic path can cross the logical limit, but never by more than the documented
        // 8 KiB allowance.
        let mut partial_output = vec![0xa1, 0xa2];
        let output_start = partial_output.len();
        let mut direct_visitor = RecordingVisitor::default();
        let mut transformer = Transformer {
            cursor: &source,
            output: &mut partial_output,
            output_start,
            visitor: &mut direct_visitor,
            limits,
            stats: ArchiveV2WireRewriteStats::default(),
            unchecked_small_puts: 0,
        };
        let error = transformer.successful_metadata().unwrap_err();
        assert_eq!(error.kind(), ArchiveV2WireRewriteErrorKind::LimitExceeded);
        drop(transformer);
        let partial_bytes = partial_output.len() - output_start;
        assert!(partial_bytes > limits.max_output_bytes);
        assert!(
            partial_bytes
                <= limits
                    .max_output_bytes
                    .checked_add(MAX_TRANSIENT_SMALL_PUT_OVERSHOOT_BYTES)
                    .unwrap()
        );

        let prefix = vec![0xb1, 0xb2, 0xb3];
        let mut output = prefix.clone();
        let mut visitor = RecordingVisitor::default();
        let error =
            rewrite_archive_v2_successful_metadata_wire(&source, &mut output, &mut visitor, limits)
                .unwrap_err();
        assert_eq!(error.kind(), ArchiveV2WireRewriteErrorKind::LimitExceeded);
        assert_eq!(output, prefix);
        assert!(visitor.pubkeys.is_empty());
        assert_eq!(visitor.rollbacks, 1);
        assert_eq!(visitor.commits, 0);
    }

    #[test]
    fn scalar_output_guard_checks_every_interval() {
        let mut metadata = metadata_fixture(0);
        metadata.pre_balances = vec![0; 600];
        metadata.post_balances.clear();
        metadata.inner_instructions = None;
        metadata.logs = None;
        metadata.pre_token_balances.clear();
        metadata.post_token_balances.clear();
        metadata.rewards.clear();
        metadata.loaded_writable_addresses.clear();
        metadata.loaded_readonly_addresses.clear();
        metadata.return_data = None;
        metadata.compute_units_consumed = None;
        metadata.cost_units = None;
        let source = wincode::config::serialize(&metadata, wincode_leb128_config()).unwrap();
        let limits = ArchiveV2WireRewriteLimits {
            // The first 256 small writes fit. The second interval must detect the limit.
            max_output_bytes: 300,
            ..ArchiveV2WireRewriteLimits::default()
        };

        let mut partial_output = Vec::new();
        let mut direct_visitor = RecordingVisitor::default();
        let mut transformer = Transformer {
            cursor: &source,
            output: &mut partial_output,
            output_start: 0,
            visitor: &mut direct_visitor,
            limits,
            stats: ArchiveV2WireRewriteStats::default(),
            unchecked_small_puts: 0,
        };
        let error = transformer.successful_metadata().unwrap_err();
        assert_eq!(error.kind(), ArchiveV2WireRewriteErrorKind::LimitExceeded);
        drop(transformer);
        assert!(partial_output.len() > MAX_UNCHECKED_SMALL_PUTS as usize);
        assert!(
            partial_output.len()
                <= limits
                    .max_output_bytes
                    .checked_add(MAX_TRANSIENT_SMALL_PUT_OVERSHOOT_BYTES)
                    .unwrap()
        );

        let prefix = vec![0xe1, 0xe2];
        let mut output = prefix.clone();
        let mut visitor = RecordingVisitor::default();
        let error =
            rewrite_archive_v2_successful_metadata_wire(&source, &mut output, &mut visitor, limits)
                .unwrap_err();
        assert_eq!(error.kind(), ArchiveV2WireRewriteErrorKind::LimitExceeded);
        assert_eq!(output, prefix);
        assert_eq!(visitor.rollbacks, 1);
        assert_eq!(visitor.commits, 0);
    }

    #[test]
    fn bounded_owned_leaf_never_crosses_output_limit() {
        let value = vec![0x5a; 32 << 10];
        let source = wincode::config::serialize(&value, wincode_leb128_config()).unwrap();
        let limits = ArchiveV2WireRewriteLimits {
            max_output_bytes: 128,
            ..ArchiveV2WireRewriteLimits::default()
        };
        let mut output = vec![0xc1, 0xc2];
        let output_start = output.len();
        let mut visitor = RecordingVisitor::default();
        let mut transformer = Transformer {
            cursor: &source,
            output: &mut output,
            output_start,
            visitor: &mut visitor,
            limits,
            stats: ArchiveV2WireRewriteStats::default(),
            unchecked_small_puts: 0,
        };
        let error = transformer
            .copy_owned::<Vec<u8>>("large owned leaf")
            .unwrap_err();
        assert_eq!(error.kind(), ArchiveV2WireRewriteErrorKind::LimitExceeded);
        drop(transformer);
        assert!(output.len() - output_start <= limits.max_output_bytes);
    }

    #[cfg(all(feature = "known-program-logs", feature = "known-drift"))]
    #[test]
    fn large_known_owned_leaf_failure_rolls_back_visitor_state() {
        let mut metadata = metadata_fixture(0);
        metadata.pre_balances.clear();
        metadata.post_balances.clear();
        metadata.inner_instructions = None;
        metadata.logs = Some(CompactLogStream {
            events: vec![
                LogEvent::LoaderUpgradedProgram {
                    program: CompactPubkey::Id(1),
                },
                LogEvent::ProgramLog(ProgramLog::Known(
                    crate::program_logs::known_programs::KnownProgramLog::Drift(
                        crate::program_logs::known_programs::drift::DriftLog::Event(vec![
                            0x6b;
                            32 << 10
                        ]),
                    ),
                )),
            ],
            strings: StringTable::default(),
            data: DataTable::default(),
        });
        metadata.pre_token_balances.clear();
        metadata.post_token_balances.clear();
        metadata.rewards.clear();
        metadata.loaded_writable_addresses.clear();
        metadata.loaded_readonly_addresses.clear();
        metadata.return_data = None;
        metadata.compute_units_consumed = None;
        metadata.cost_units = None;
        let source = wincode::config::serialize(&metadata, wincode_leb128_config()).unwrap();
        let limits = ArchiveV2WireRewriteLimits {
            max_output_bytes: 256,
            ..ArchiveV2WireRewriteLimits::default()
        };
        let prefix = vec![0xd1, 0xd2];
        let mut output = prefix.clone();
        let mut visitor = RecordingVisitor::default();
        let error =
            rewrite_archive_v2_successful_metadata_wire(&source, &mut output, &mut visitor, limits)
                .unwrap_err();
        assert_eq!(error.kind(), ArchiveV2WireRewriteErrorKind::LimitExceeded);
        assert_eq!(output, prefix);
        assert!(visitor.pubkeys.is_empty());
        assert_eq!(visitor.rollbacks, 1);
        assert_eq!(visitor.commits, 0);
    }

    #[test]
    fn typed_sequence_preallocation_boundary_matches_wincode() {
        let element_size = std::mem::size_of::<LogEvent>().max(1);
        assert!(element_size > 1);
        let accepted = ARCHIVE_V2_WIRE_REWRITE_MAX_FRAME_BYTES / element_size;
        let rejected = accepted + 1;

        fn length_prefix(value: usize) -> Vec<u8> {
            wincode::config::serialize(&(value as u64), wincode_leb128_config()).unwrap()
        }

        let mut accepted_input = length_prefix(accepted);
        accepted_input.resize(accepted_input.len() + accepted, 0);
        let direct_accepted = <wincode::len::BincodeLen as SeqLen<
            ArchiveV2WireBoundedConfig,
        >>::read_prealloc_check::<LogEvent>(&mut accepted_input.as_slice())
        .unwrap();
        assert_eq!(direct_accepted, accepted);

        let mut output = Vec::new();
        let mut visitor = RecordingVisitor::default();
        let mut transformer = Transformer {
            cursor: &accepted_input,
            output: &mut output,
            output_start: 0,
            visitor: &mut visitor,
            limits: ArchiveV2WireRewriteLimits::default(),
            stats: ArchiveV2WireRewriteStats::default(),
            unchecked_small_puts: 0,
        };
        assert_eq!(
            transformer
                .sequence_len::<LogEvent>("test log-event count")
                .unwrap(),
            accepted
        );

        let rejected_input = length_prefix(rejected);
        let direct_error = <wincode::len::BincodeLen as SeqLen<
            ArchiveV2WireBoundedConfig,
        >>::read_prealloc_check::<LogEvent>(&mut rejected_input.as_slice())
        .unwrap_err();
        assert!(matches!(
            direct_error,
            wincode::error::ReadError::PreallocationSizeLimit { .. }
        ));

        let mut output = Vec::new();
        let mut visitor = RecordingVisitor::default();
        let mut transformer = Transformer {
            cursor: &rejected_input,
            output: &mut output,
            output_start: 0,
            visitor: &mut visitor,
            limits: ArchiveV2WireRewriteLimits::default(),
            stats: ArchiveV2WireRewriteStats::default(),
            unchecked_small_puts: 0,
        };
        let error = transformer
            .sequence_len::<LogEvent>("test log-event count")
            .unwrap_err();
        assert_eq!(error.kind(), ArchiveV2WireRewriteErrorKind::LimitExceeded);
    }
}
