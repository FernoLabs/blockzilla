//! Count borrowed transaction bytes without allocating transaction or instruction objects.
use crate::{
    CompactV2MessageProjector, CompactV2MessageSchema, CompactV2MetadataProjector,
    CompactV2MetadataSchema, ProjectedCompactV2MessageVersion,
};
use blockzilla_archive_v2::{ARCHIVE_V2_TX_FLAG_HAS_ERROR, ARCHIVE_V2_TX_FLAG_HAS_INNER_IX, ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES, ARCHIVE_V2_TX_FLAG_HAS_METADATA, ARCHIVE_V2_TX_FLAG_MESSAGE_V0, ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK, ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK};
use blockzilla_query_sdk::BlockCounts;

pub enum CountMetadata<'a> {
    Full(&'a [u8]),
    Split {
        outcome: &'a [u8],
        loaded: &'a [u8],
        inner: &'a [u8],
        effect_state: u8,
    },
    Unavailable,
}

/// The same parser serves local and network V2/V3 count scans.
pub fn count_transaction(
    counts: &mut BlockCounts,
    flags: u32,
    signature_count: usize,
    message: &[u8],
    metadata: CountMetadata<'_>,
    message_schema: CompactV2MessageSchema,
    metadata_schema: CompactV2MetadataSchema,
    registry_entries: u32,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK == 0
            || flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0,
        "raw metadata flag without metadata"
    );
    counts.transactions += 1;
    if flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
        counts.incomplete_instructions += 1;
        counts.incomplete_cpi += 1;
        return Ok(());
    }
    let message =
        CompactV2MessageProjector::new(message_schema, registry_entries).count_message(message)?;
    anyhow::ensure!(
        usize::from(message.header().num_required_signatures) == signature_count,
        "signature count differs from message header"
    );
    anyhow::ensure!(
        matches!(
            message.version(),
            ProjectedCompactV2MessageVersion::V0 { .. }
        ) == (flags & ARCHIVE_V2_TX_FLAG_MESSAGE_V0 != 0),
        "message version differs from row flags"
    );
    let limits = message.count_limits();
    let has_metadata = flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0;
    let raw_metadata = flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0;
    if !has_metadata {
        anyhow::ensure!(
            flags
                & (ARCHIVE_V2_TX_FLAG_HAS_ERROR
                    | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX
                    | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES)
                == 0,
            "metadata flags without metadata"
        );
    }
    if !has_metadata || raw_metadata {
        counts.incomplete_cpi += 1;
        if message.expected_loaded_addresses() == 0 {
            counts.instructions += limits.top_level_instruction_count as u64;
        } else {
            counts.incomplete_instructions += 1;
        }
        return Ok(());
    }
    let projector = CompactV2MetadataProjector::new(metadata_schema, registry_entries);
    let value = match metadata {
        CountMetadata::Full(bytes) => projector.count(bytes, limits)?,
        CountMetadata::Split {
            outcome,
            loaded,
            inner,
            effect_state,
        } => {
            let value = projector.count_split_planes(outcome, loaded, inner, limits)?;
            let state = match value.inner {
                None => 1,
                Some((0, _)) => 2,
                Some(_) => 3,
            };
            anyhow::ensure!(
                effect_state & 7 == state,
                "CPI state differs from inner plane"
            );
            value
        }
        CountMetadata::Unavailable => anyhow::bail!("exact metadata is missing"),
    };
    anyhow::ensure!(
        (flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0) == !value.execution_status.is_success(),
        "error flag differs from metadata"
    );
    anyhow::ensure!(
        (flags & ARCHIVE_V2_TX_FLAG_HAS_INNER_IX != 0) == value.inner.is_some(),
        "CPI flag differs from metadata"
    );
    anyhow::ensure!(
        (flags & ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES != 0)
            == (message.expected_loaded_addresses() != 0),
        "loaded-key flag differs from metadata"
    );
    let inner = value.inner.map_or(0, |(_, count)| count);
    counts.instructions += limits.top_level_instruction_count as u64 + inner;
    counts.recorded_inner_instructions += inner;
    counts.incomplete_cpi += u64::from(value.inner.is_none());
    Ok(())
}
