//! Measurement-only account-posting codec experiments.
//!
//! This module is not called by the active builder. It compares the current
//! global-ordinal ULEB payload with an exact block-group payload and selects
//! the shorter form deterministically for each key.

use std::{
    fs,
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
    sync::{
        Arc, Condvar, Mutex,
        mpsc::{self, Receiver, SyncSender},
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_index_archive_format::{
    indexes::accounts as postings,
    varint::{read_uleb128, read_uleb128_u32, write_uleb128},
};
use serde::Serialize;

const KEY_CODEC_ORDINAL_ULEB: u8 = 0;
const KEY_CODEC_BLOCK_GROUPS: u8 = 1;

const LOCAL_CODEC_VARINT: u8 = 0;
const LOCAL_CODEC_BITPACK: u8 = 1;
const LOCAL_CODEC_BITMAP: u8 = 2;

pub const DEFAULT_ZSTD_LEVELS: [i32; 4] = [1, 3, 5, 9];
pub const MAX_MEASUREMENT_WORKERS: usize = 64;
pub const LIVE_BYTE_BUDGET: usize = 512 << 20;
const MAX_STANDALONE_INDEX_BYTES: u64 = 512 << 20;
pub const MAX_BLOCK_CODEC_SCRATCH_BYTES: usize = 4 << 20;
pub const MAX_ZSTD_COMPRESSION_WORKSPACE_BYTES: usize = 128 << 20;
const MAX_ZSTD_DECOMPRESSION_FIXED_BYTES: usize = 8 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockSpan {
    pub block_id: u32,
    pub first_tx_ordinal: u64,
    pub tx_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExactPosting {
    pub block_id: u32,
    pub tx_index: u32,
    pub roles: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactKeyPostings {
    pub key: u32,
    pub postings: Vec<ExactPosting>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MeasurementStats {
    pub current_page_bytes: usize,
    pub adaptive_page_bytes: usize,
    pub ordinal_keys: u32,
    pub block_group_keys: u32,
    pub local_varint_groups: u32,
    pub local_bitpack_groups: u32,
    pub local_bitmap_groups: u32,
    pub peak_block_codec_scratch_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedMeasurementPage {
    pub bytes: Vec<u8>,
    pub stats: MeasurementStats,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct LocalStats {
    varint: u32,
    bitpack: u32,
    bitmap: u32,
}

struct BlockLayout<'a> {
    spans: &'a [BlockSpan],
    transactions: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedBlockLayout {
    spans: Vec<BlockSpan>,
    transactions: u64,
}

impl ValidatedBlockLayout {
    pub fn new(spans: Vec<BlockSpan>) -> Result<Self> {
        let borrowed = BlockLayout::new(&spans)?;
        Ok(Self {
            transactions: borrowed.transactions,
            spans,
        })
    }

    pub const fn transactions(&self) -> u64 {
        self.transactions
    }

    fn borrowed(&self) -> BlockLayout<'_> {
        BlockLayout {
            spans: &self.spans,
            transactions: self.transactions,
        }
    }

    pub fn spans(&self) -> &[BlockSpan] {
        &self.spans
    }

    pub fn sorted_ordinal_cursor(&self) -> SortedOrdinalCursor<'_> {
        SortedOrdinalCursor {
            layout: self.borrowed(),
            hint: None,
            previous: None,
        }
    }
}

/// Monotonic transaction-ordinal resolver over one validated block layout.
pub struct SortedOrdinalCursor<'layout> {
    layout: BlockLayout<'layout>,
    hint: Option<usize>,
    previous: Option<u64>,
}

impl SortedOrdinalCursor<'_> {
    pub fn resolve(&mut self, ordinal: u64) -> Result<ExactPosting> {
        if let Some(previous) = self.previous {
            ensure!(
                ordinal > previous,
                "measurement sorted transaction ordinals do not strictly ascend"
            );
        }
        let (_, posting) = self
            .layout
            .resolve_ordinal_monotonic(ordinal, &mut self.hint)?;
        self.previous = Some(ordinal);
        Ok(posting)
    }
}

pub fn validate_block_spans(spans: &[BlockSpan]) -> Result<u64> {
    Ok(BlockLayout::new(spans)?.transactions)
}

impl<'a> BlockLayout<'a> {
    fn new(spans: &'a [BlockSpan]) -> Result<Self> {
        let mut next_ordinal = 0_u64;
        let mut previous_block = None;
        for span in spans {
            if let Some(previous) = previous_block {
                ensure!(
                    span.block_id > previous,
                    "measurement block IDs do not strictly ascend"
                );
            }
            ensure!(
                span.first_tx_ordinal == next_ordinal,
                "measurement block transaction ranges are not contiguous"
            );
            next_ordinal = next_ordinal
                .checked_add(u64::from(span.tx_count))
                .context("measurement transaction range overflow")?;
            previous_block = Some(span.block_id);
        }
        Ok(Self {
            spans,
            transactions: next_ordinal,
        })
    }

    fn resolve_ordinal(&self, ordinal: u64) -> Result<(usize, ExactPosting)> {
        ensure!(
            ordinal < self.transactions,
            "measurement transaction ordinal is outside block spans"
        );
        let mut low = 0_usize;
        let mut high = self.spans.len();
        while low < high {
            let middle = low + (high - low) / 2;
            let span = self.spans[middle];
            let end = span
                .first_tx_ordinal
                .checked_add(u64::from(span.tx_count))
                .context("measurement transaction range overflow")?;
            if ordinal < span.first_tx_ordinal {
                high = middle;
            } else if ordinal >= end {
                low = middle + 1;
            } else {
                return Ok((
                    middle,
                    ExactPosting {
                        block_id: span.block_id,
                        tx_index: u32::try_from(ordinal - span.first_tx_ordinal)
                            .context("measurement transaction index exceeds u32")?,
                        roles: 0,
                    },
                ));
            }
        }
        bail!("measurement transaction ordinal is not covered by a block span")
    }

    fn resolve_ordinal_monotonic(
        &self,
        ordinal: u64,
        hint: &mut Option<usize>,
    ) -> Result<(usize, ExactPosting)> {
        ensure!(
            ordinal < self.transactions,
            "measurement transaction ordinal is outside block spans"
        );
        if let Some(mut index) = *hint {
            while let Some(span) = self.spans.get(index) {
                let end = span
                    .first_tx_ordinal
                    .checked_add(u64::from(span.tx_count))
                    .context("measurement transaction range overflow")?;
                if ordinal < span.first_tx_ordinal {
                    break;
                }
                if ordinal < end {
                    *hint = Some(index);
                    return Ok((
                        index,
                        ExactPosting {
                            block_id: span.block_id,
                            tx_index: u32::try_from(ordinal - span.first_tx_ordinal)
                                .context("measurement transaction index exceeds u32")?,
                            roles: 0,
                        },
                    ));
                }
                index += 1;
            }
        }
        let resolved = self.resolve_ordinal(ordinal)?;
        *hint = Some(resolved.0);
        Ok(resolved)
    }

    fn span_for_block(&self, block_id: u32) -> Result<BlockSpan> {
        self.spans
            .binary_search_by_key(&block_id, |span| span.block_id)
            .ok()
            .map(|index| self.spans[index])
            .with_context(|| format!("measurement block {block_id} is not in the layout"))
    }
}

/// Encode one measurement page and retain the current schema-2 size for a
/// direct comparison. The active archive writer does not call this function.
pub fn encode_page(
    keys: &[postings::KeyPostings],
    spans: &[BlockSpan],
) -> Result<EncodedMeasurementPage> {
    let layout = BlockLayout::new(spans)?;
    encode_page_in_layout(keys, &layout)
}

pub fn encode_page_with_layout(
    keys: &[postings::KeyPostings],
    layout: &ValidatedBlockLayout,
) -> Result<EncodedMeasurementPage> {
    encode_page_in_layout(keys, &layout.borrowed())
}

fn encode_page_in_layout(
    keys: &[postings::KeyPostings],
    layout: &BlockLayout<'_>,
) -> Result<EncodedMeasurementPage> {
    let current = postings::encode_page(keys).context("encode current account page baseline")?;
    let current_len = current.len();
    drop(current);
    let mut output = Vec::new();
    output
        .try_reserve_exact(current_len.saturating_add(keys.len()))
        .context("reserve measurement account page")?;
    let mut previous_key = None;
    let mut stats = MeasurementStats {
        current_page_bytes: current_len,
        ..MeasurementStats::default()
    };

    for key in keys {
        let gap = match previous_key {
            None => 0,
            Some(previous) => key
                .key
                .checked_sub(previous)
                .filter(|gap| *gap != 0)
                .context("measurement account keys do not strictly ascend")?,
        };
        previous_key = Some(key.key);
        write_uleb128(&mut output, u64::from(gap));
        write_uleb128(
            &mut output,
            u64::try_from(key.postings.len()).context("measurement posting count exceeds u64")?,
        );

        let ordinal = encode_ordinal_payload(&key.postings)?;
        let (block_groups, local, scratch_bytes) =
            encode_block_group_payload(&key.postings, layout)?;
        let (codec, payload) = choose_key_codec(ordinal, block_groups);
        stats.peak_block_codec_scratch_bytes =
            stats.peak_block_codec_scratch_bytes.max(scratch_bytes);
        output.push(codec);
        output.extend_from_slice(&payload);
        if codec == KEY_CODEC_ORDINAL_ULEB {
            stats.ordinal_keys += 1;
        } else {
            stats.block_group_keys += 1;
            stats.local_varint_groups += local.varint;
            stats.local_bitpack_groups += local.bitpack;
            stats.local_bitmap_groups += local.bitmap;
        }
    }

    ensure!(
        output.len() <= postings::MAX_PAGE_DECODED_BYTES as usize,
        "measurement account page exceeds decode guard"
    );
    ensure!(
        output.capacity() <= current_len.saturating_add(keys.len()),
        "measurement account page capacity exceeds its ordinal fallback bound"
    );
    stats.adaptive_page_bytes = output.len();
    Ok(EncodedMeasurementPage {
        bytes: output,
        stats,
    })
}

/// Decode one measurement page to the exact block/transaction/role relation.
pub fn decode_page(
    input: &[u8],
    first_key: u32,
    key_count: u32,
    spans: &[BlockSpan],
) -> Result<Vec<ExactKeyPostings>> {
    let layout = ValidatedBlockLayout::new(spans.to_vec())?;
    decode_page_with_layout(input, first_key, key_count, &layout)
}

pub fn decode_page_with_layout(
    input: &[u8],
    first_key: u32,
    key_count: u32,
    layout: &ValidatedBlockLayout,
) -> Result<Vec<ExactKeyPostings>> {
    ensure!(
        input.len() <= postings::MAX_PAGE_DECODED_BYTES as usize,
        "measurement page exceeds decode guard"
    );
    ensure!(key_count != 0, "measurement page has no keys");
    ensure!(
        key_count <= postings::MAX_KEYS_PER_PAGE,
        "measurement page has too many keys"
    );
    ensure!(first_key != 0, "measurement account key zero is reserved");
    ensure!(
        usize::try_from(key_count)? <= input.len(),
        "measurement key count exceeds page bytes"
    );
    let layout = layout.borrowed();
    let mut cursor = 0_usize;
    let mut key = first_key;
    let mut total_postings = 0_usize;
    let mut result = Vec::with_capacity(usize::try_from(key_count)?);

    for index in 0..key_count {
        let gap = read_uleb128_u32(input, &mut cursor).context("read measurement account gap")?;
        if index == 0 {
            ensure!(gap == 0, "measurement first account gap is not zero");
        } else {
            ensure!(gap != 0, "measurement account keys do not strictly ascend");
            key = key
                .checked_add(gap)
                .context("measurement account key overflow")?;
        }
        let posting_count = read_bounded_count(
            input,
            &mut cursor,
            postings::MAX_POSTINGS_PER_PAGE as usize,
            "measurement posting count",
        )?;
        ensure!(posting_count != 0, "measurement account has no postings");
        total_postings = total_postings
            .checked_add(posting_count)
            .context("measurement page posting count overflow")?;
        ensure!(
            total_postings <= postings::MAX_POSTINGS_PER_PAGE as usize,
            "measurement page has too many postings"
        );
        let codec = read_byte(input, &mut cursor, "measurement key codec")?;
        let postings = match codec {
            KEY_CODEC_ORDINAL_ULEB => {
                decode_ordinal_payload(input, &mut cursor, posting_count, &layout)?
            }
            KEY_CODEC_BLOCK_GROUPS => {
                decode_block_group_payload(input, &mut cursor, posting_count, &layout)?
            }
            _ => bail!("unknown measurement key codec {codec}"),
        };
        result.push(ExactKeyPostings { key, postings });
    }
    ensure!(cursor == input.len(), "measurement page has trailing bytes");
    let ordinal = exact_to_ordinal_keys(&result, &layout)?;
    let canonical = encode_page_in_layout(&ordinal, &layout)?;
    ensure!(
        canonical.bytes == input,
        "measurement page does not use its canonical adaptive encoding"
    );
    Ok(result)
}

/// Result of one allocation-free target-key page visit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TargetKeyVisitSummary {
    pub first_key: u32,
    pub last_key: u32,
    pub postings: u64,
    pub blocks: u64,
    pub found: bool,
    pub first_posting: Option<(u32, u32)>,
    pub last_posting: Option<(u32, u32)>,
}

/// One matching block emitted by [`visit_page_key_blocks_with_layout`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoleMatchedBlock {
    pub block_id: u32,
    pub matching_postings: u64,
}

/// Result of one allocation-free target-key block visit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TargetKeyBlockVisitSummary {
    pub page: TargetKeyVisitSummary,
    pub matching_postings: u64,
    pub matching_blocks: u64,
}

/// Validate a complete adaptive page and stream only the requested key.
///
/// Each posting is validated before it is passed to the visitor. The parser
/// then continues to validate later keys and the final byte boundary without
/// allocating posting vectors. If that later validation fails, this function
/// returns an error after earlier visitor calls have occurred. A caller must
/// discard callback results when this function returns an error.
pub fn visit_page_key_with_layout(
    input: &[u8],
    first_key: u32,
    key_count: u32,
    layout: &ValidatedBlockLayout,
    target_key: u32,
    mut visit: impl FnMut(ExactPosting) -> Result<()>,
) -> Result<TargetKeyVisitSummary> {
    validate_streaming_page(
        input,
        first_key,
        key_count,
        &layout.borrowed(),
        target_key,
        &mut visit,
    )
}

/// Validate a complete adaptive page and emit one record per matching block.
///
/// This is the lean candidate-construction path. It does not resolve sparse
/// coverage for each posting and never materializes a posting or block list.
/// As with [`visit_page_key_with_layout`], callback effects are not atomic:
/// discard them if this function returns an error.
pub fn visit_page_key_blocks_with_layout(
    input: &[u8],
    first_key: u32,
    key_count: u32,
    layout: &ValidatedBlockLayout,
    target_key: u32,
    required_roles: u8,
    mut visit: impl FnMut(RoleMatchedBlock) -> Result<()>,
) -> Result<TargetKeyBlockVisitSummary> {
    ensure!(
        required_roles != 0 && required_roles & !postings::ROLE_MASK == 0,
        "measurement required role mask is invalid"
    );
    let mut current_block = None;
    let mut current_count = 0_u64;
    let mut matching_postings = 0_u64;
    let mut matching_blocks = 0_u64;
    let page =
        visit_page_key_with_layout(input, first_key, key_count, layout, target_key, |posting| {
            if posting.roles & required_roles == 0 {
                return Ok(());
            }
            matching_postings = matching_postings
                .checked_add(1)
                .context("measurement matching posting count overflow")?;
            if current_block == Some(posting.block_id) {
                current_count = current_count
                    .checked_add(1)
                    .context("measurement block posting count overflow")?;
                return Ok(());
            }
            if let Some(block_id) = current_block {
                visit(RoleMatchedBlock {
                    block_id,
                    matching_postings: current_count,
                })?;
                matching_blocks = matching_blocks
                    .checked_add(1)
                    .context("measurement matching block count overflow")?;
            }
            current_block = Some(posting.block_id);
            current_count = 1;
            Ok(())
        })?;
    if let Some(block_id) = current_block {
        visit(RoleMatchedBlock {
            block_id,
            matching_postings: current_count,
        })?;
        matching_blocks = matching_blocks
            .checked_add(1)
            .context("measurement matching block count overflow")?;
    }
    Ok(TargetKeyBlockVisitSummary {
        page,
        matching_postings,
        matching_blocks,
    })
}

fn validate_streaming_page(
    input: &[u8],
    first_key: u32,
    key_count: u32,
    layout: &BlockLayout<'_>,
    target_key: u32,
    visit: &mut dyn FnMut(ExactPosting) -> Result<()>,
) -> Result<TargetKeyVisitSummary> {
    ensure!(
        input.len() <= postings::MAX_PAGE_DECODED_BYTES as usize,
        "measurement page exceeds decode guard"
    );
    ensure!(key_count != 0, "measurement page has no keys");
    ensure!(
        key_count <= postings::MAX_KEYS_PER_PAGE,
        "measurement page has too many keys"
    );
    ensure!(first_key != 0, "measurement account key zero is reserved");
    ensure!(
        usize::try_from(key_count)? <= input.len(),
        "measurement key count exceeds page bytes"
    );
    let mut cursor = 0_usize;
    let mut key = first_key;
    let mut total_postings = 0_usize;
    let mut found = false;
    let mut target_postings = 0_u64;
    let mut target_blocks = 0_u64;
    let mut first_posting = None;
    let mut last_posting = None;
    for index in 0..key_count {
        let gap = read_uleb128_u32(input, &mut cursor).context("read measurement account gap")?;
        if index == 0 {
            ensure!(gap == 0, "measurement first account gap is not zero");
        } else {
            ensure!(gap != 0, "measurement account keys do not strictly ascend");
            key = key
                .checked_add(gap)
                .context("measurement account key overflow")?;
        }
        let posting_count = read_bounded_count(
            input,
            &mut cursor,
            postings::MAX_POSTINGS_PER_PAGE as usize,
            "measurement posting count",
        )?;
        ensure!(posting_count != 0, "measurement account has no postings");
        total_postings = total_postings
            .checked_add(posting_count)
            .context("measurement page posting count overflow")?;
        ensure!(
            total_postings <= postings::MAX_POSTINGS_PER_PAGE as usize,
            "measurement page has too many postings"
        );
        if key == target_key {
            found = true;
            let mut record_position = |posting: ExactPosting| {
                let position = (posting.block_id, posting.tx_index);
                if let Some(previous) = last_posting {
                    ensure!(
                        position > previous,
                        "measurement target postings do not strictly ascend"
                    );
                    if position.0 != previous.0 {
                        target_blocks = target_blocks
                            .checked_add(1)
                            .context("measurement target block count overflow")?;
                    }
                } else {
                    first_posting = Some(position);
                    target_blocks = 1;
                }
                last_posting = Some(position);
                visit(posting)
            };
            validate_key_payload(
                input,
                &mut cursor,
                posting_count,
                layout,
                Some(&mut record_position),
            )?;
            target_postings = u64::try_from(posting_count)?;
        } else {
            validate_key_payload(input, &mut cursor, posting_count, layout, None)?;
        }
    }
    ensure!(cursor == input.len(), "measurement page has trailing bytes");
    Ok(TargetKeyVisitSummary {
        first_key,
        last_key: key,
        postings: target_postings,
        blocks: target_blocks,
        found,
        first_posting,
        last_posting,
    })
}

fn validate_key_payload(
    input: &[u8],
    cursor: &mut usize,
    posting_count: usize,
    layout: &BlockLayout<'_>,
    mut visit: Option<&mut dyn FnMut(ExactPosting) -> Result<()>>,
) -> Result<()> {
    let codec = read_byte(input, cursor, "measurement key codec")?;
    let payload_start = *cursor;
    match codec {
        KEY_CODEC_ORDINAL_ULEB => {
            let mut ordinal = 0_u64;
            let mut span_hint = None;
            let mut block_length = BlockPayloadLength::default();
            for index in 0..posting_count {
                let packed =
                    read_uleb128(input, cursor).context("read measurement ordinal posting")?;
                let gap = packed >> 4;
                if index == 0 {
                    ordinal = gap;
                } else {
                    ensure!(gap != 0, "measurement transaction ordinals repeat");
                    ordinal = ordinal
                        .checked_add(gap)
                        .context("measurement transaction ordinal overflow")?;
                }
                let (span_index, mut posting) =
                    layout.resolve_ordinal_monotonic(ordinal, &mut span_hint)?;
                posting.roles = (packed & u64::from(postings::ROLE_MASK)) as u8;
                block_length.push(posting, layout.spans[span_index])?;
                if let Some(callback) = visit.as_deref_mut() {
                    callback(posting)?;
                }
            }
            let ordinal_length = cursor
                .checked_sub(payload_start)
                .context("measurement ordinal payload offset underflow")?;
            let block_length = block_length.finish()?;
            ensure!(
                ordinal_length <= block_length,
                "measurement key does not use its canonical ordinal codec"
            );
        }
        KEY_CODEC_BLOCK_GROUPS => {
            let group_count = read_bounded_count(
                input,
                cursor,
                posting_count,
                "measurement block group count",
            )?;
            ensure!(group_count != 0, "measurement key has no block groups");
            let mut block_id = 0_u32;
            let mut decoded_postings = 0_usize;
            let mut ordinal_length = OrdinalPayloadLength::default();
            for group_index in 0..group_count {
                let gap = read_uleb128_u32(input, cursor).context("read measurement block gap")?;
                if group_index == 0 {
                    block_id = gap;
                } else {
                    ensure!(gap != 0, "measurement block groups repeat");
                    block_id = block_id
                        .checked_add(gap)
                        .context("measurement block ID overflow")?;
                }
                let remaining = posting_count - decoded_postings;
                let count = read_bounded_count(
                    input,
                    cursor,
                    remaining,
                    "measurement local posting count",
                )?;
                ensure!(count != 0, "measurement block group has no postings");
                let span = layout.span_for_block(block_id)?;
                let local_codec = read_byte(input, cursor, "measurement local codec")?;
                validate_local_payload(
                    input,
                    cursor,
                    count,
                    span,
                    local_codec,
                    &mut ordinal_length,
                    &mut visit,
                )?;
                decoded_postings = decoded_postings
                    .checked_add(count)
                    .context("measurement decoded posting count overflow")?;
            }
            ensure!(
                decoded_postings == posting_count,
                "measurement block groups do not cover the declared postings"
            );
            let block_length = cursor
                .checked_sub(payload_start)
                .context("measurement block payload offset underflow")?;
            ensure!(
                block_length < ordinal_length.bytes,
                "measurement key does not use its canonical block-group codec"
            );
        }
        _ => bail!("unknown measurement key codec {codec}"),
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct LocalPayloadLength {
    span: BlockSpan,
    count: usize,
    base: u32,
    last: u32,
    varint_bytes: usize,
}

impl LocalPayloadLength {
    fn new(span: BlockSpan) -> Self {
        Self {
            span,
            count: 0,
            base: 0,
            last: 0,
            varint_bytes: 0,
        }
    }

    fn push(&mut self, posting: ExactPosting) -> Result<()> {
        ensure!(
            posting.block_id == self.span.block_id,
            "measurement local posting block differs"
        );
        ensure!(
            posting.roles & !postings::ROLE_MASK == 0,
            "measurement posting has unknown role bits"
        );
        ensure!(
            posting.tx_index < self.span.tx_count,
            "measurement local transaction index is outside block"
        );
        let gap = if self.count == 0 {
            self.base = posting.tx_index;
            posting.tx_index
        } else {
            posting
                .tx_index
                .checked_sub(self.last)
                .filter(|gap| *gap != 0)
                .context("measurement local transaction indexes do not strictly ascend")?
        };
        let packed = (u64::from(gap) << 4) | u64::from(posting.roles);
        self.varint_bytes = self
            .varint_bytes
            .checked_add(uleb128_len(packed))
            .context("measurement local varint length overflow")?;
        self.count = self
            .count
            .checked_add(1)
            .context("measurement local posting count overflow")?;
        self.last = posting.tx_index;
        Ok(())
    }

    fn bitpack_bytes(&self) -> Result<usize> {
        let width = bit_width(
            self.last
                .checked_sub(self.base)
                .context("measurement bitpack offset underflow")?,
        );
        uleb128_len(u64::from(self.base))
            .checked_add(uleb128_len(u64::from(width)))
            .and_then(|value| value.checked_add(packed_value_bytes(self.count, width).ok()?))
            .and_then(|value| value.checked_add(self.count.div_ceil(2)))
            .context("measurement bitpack payload length overflow")
    }

    fn bitmap_bytes(&self) -> Result<Option<usize>> {
        let bytes = bitmap_len(self.span.tx_count)?
            .checked_add(self.count.div_ceil(2))
            .context("measurement bitmap payload length overflow")?;
        Ok((bytes <= postings::MAX_PAGE_DECODED_BYTES as usize).then_some(bytes))
    }

    fn preferred(&self) -> Result<(u8, usize)> {
        ensure!(self.count != 0, "measurement local group has no postings");
        let mut selected = (LOCAL_CODEC_VARINT, self.varint_bytes);
        let bitpack = self.bitpack_bytes()?;
        if bitpack < selected.1 {
            selected = (LOCAL_CODEC_BITPACK, bitpack);
        }
        if let Some(bitmap) = self.bitmap_bytes()?
            && bitmap < selected.1
        {
            selected = (LOCAL_CODEC_BITMAP, bitmap);
        }
        Ok(selected)
    }

    fn bytes_for(&self, codec: u8) -> Result<usize> {
        match codec {
            LOCAL_CODEC_VARINT => Ok(self.varint_bytes),
            LOCAL_CODEC_BITPACK => self.bitpack_bytes(),
            LOCAL_CODEC_BITMAP => self
                .bitmap_bytes()?
                .context("measurement bitmap is outside the page guard"),
            _ => bail!("unknown measurement local codec {codec}"),
        }
    }
}

#[derive(Default)]
struct BlockPayloadLength {
    groups: usize,
    group_bytes: usize,
    previous_block: Option<u32>,
    current: Option<LocalPayloadLength>,
}

impl BlockPayloadLength {
    fn push(&mut self, posting: ExactPosting, span: BlockSpan) -> Result<()> {
        if self
            .current
            .is_some_and(|current| current.span.block_id != posting.block_id)
        {
            self.finish_current()?;
        }
        if self.current.is_none() {
            if let Some(previous) = self.previous_block {
                ensure!(
                    posting.block_id > previous,
                    "measurement block groups do not strictly ascend"
                );
            }
            self.current = Some(LocalPayloadLength::new(span));
        }
        self.current
            .as_mut()
            .expect("measurement current group is present")
            .push(posting)
    }

    fn finish_current(&mut self) -> Result<()> {
        let Some(current) = self.current.take() else {
            return Ok(());
        };
        let block_gap = match self.previous_block {
            None => current.span.block_id,
            Some(previous) => current
                .span
                .block_id
                .checked_sub(previous)
                .filter(|gap| *gap != 0)
                .context("measurement block groups do not strictly ascend")?,
        };
        let (_, local_bytes) = current.preferred()?;
        self.group_bytes = self
            .group_bytes
            .checked_add(uleb128_len(u64::from(block_gap)))
            .and_then(|value| value.checked_add(uleb128_len(current.count as u64)))
            .and_then(|value| value.checked_add(1))
            .and_then(|value| value.checked_add(local_bytes))
            .context("measurement block payload length overflow")?;
        self.groups = self
            .groups
            .checked_add(1)
            .context("measurement block group count overflow")?;
        self.previous_block = Some(current.span.block_id);
        Ok(())
    }

    fn finish(mut self) -> Result<usize> {
        self.finish_current()?;
        ensure!(self.groups != 0, "measurement key has no block groups");
        uleb128_len(self.groups as u64)
            .checked_add(self.group_bytes)
            .context("measurement block payload length overflow")
    }
}

#[derive(Default)]
struct OrdinalPayloadLength {
    previous: Option<u64>,
    bytes: usize,
}

impl OrdinalPayloadLength {
    fn push(&mut self, posting: ExactPosting, span: BlockSpan) -> Result<()> {
        ensure!(
            posting.roles & !postings::ROLE_MASK == 0,
            "measurement posting has unknown role bits"
        );
        ensure!(
            posting.tx_index < span.tx_count,
            "measurement transaction index is outside its block"
        );
        let ordinal = span
            .first_tx_ordinal
            .checked_add(u64::from(posting.tx_index))
            .context("measurement transaction ordinal overflow")?;
        let gap = match self.previous {
            None => ordinal,
            Some(previous) => ordinal
                .checked_sub(previous)
                .filter(|gap| *gap != 0)
                .context("measurement transaction ordinals do not strictly ascend")?,
        };
        let packed = gap
            .checked_shl(4)
            .filter(|packed| *packed >> 4 == gap)
            .context("measurement transaction ordinal gap overflows role packing")?
            | u64::from(posting.roles);
        self.bytes = self
            .bytes
            .checked_add(uleb128_len(packed))
            .context("measurement ordinal payload length overflow")?;
        self.previous = Some(ordinal);
        Ok(())
    }
}

fn validate_local_payload(
    input: &[u8],
    cursor: &mut usize,
    count: usize,
    span: BlockSpan,
    codec: u8,
    ordinal_length: &mut OrdinalPayloadLength,
    visit: &mut Option<&mut dyn FnMut(ExactPosting) -> Result<()>>,
) -> Result<()> {
    let payload_start = *cursor;
    let mut local = LocalPayloadLength::new(span);
    match codec {
        LOCAL_CODEC_VARINT => {
            let mut tx_index = 0_u32;
            for index in 0..count {
                let packed =
                    read_uleb128(input, cursor).context("read measurement local varint")?;
                let gap = u32::try_from(packed >> 4)
                    .context("measurement local transaction gap exceeds u32")?;
                if index == 0 {
                    tx_index = gap;
                } else {
                    ensure!(gap != 0, "measurement local transaction indexes repeat");
                    tx_index = tx_index
                        .checked_add(gap)
                        .context("measurement local transaction index overflow")?;
                }
                accept_local_posting(
                    ExactPosting {
                        block_id: span.block_id,
                        tx_index,
                        roles: (packed & u64::from(postings::ROLE_MASK)) as u8,
                    },
                    span,
                    &mut local,
                    ordinal_length,
                    visit,
                )?;
            }
        }
        LOCAL_CODEC_BITPACK => {
            let base = read_uleb128_u32(input, cursor).context("read measurement bitpack base")?;
            let width = u8::try_from(
                read_uleb128_u32(input, cursor).context("read measurement bit width")?,
            )
            .context("measurement bit width exceeds u8")?;
            ensure!(width <= 32, "measurement bit width exceeds u32");
            let packed_len = packed_value_bytes(count, width)?;
            let packed = take(input, cursor, packed_len, "measurement packed offsets")?;
            validate_zero_tail_bits(packed, count, width, "measurement packed offsets")?;
            let roles_len = count.div_ceil(2);
            let roles = take(input, cursor, roles_len, "measurement packed roles")?;
            validate_role_padding(roles, count)?;
            let mut accumulator = 0_u64;
            let mut bits = 0_u32;
            let mut position = 0_usize;
            let mask = if width == 32 {
                u64::from(u32::MAX)
            } else if width == 0 {
                0
            } else {
                (1_u64 << width) - 1
            };
            let mut previous = None;
            for index in 0..count {
                while bits < u32::from(width) {
                    let byte = *packed
                        .get(position)
                        .context("measurement packed offsets are truncated")?;
                    accumulator |= u64::from(byte) << bits;
                    bits += 8;
                    position += 1;
                }
                let offset = u32::try_from(accumulator & mask)?;
                if width != 0 {
                    accumulator >>= width;
                    bits -= u32::from(width);
                }
                if index == 0 {
                    ensure!(offset == 0, "measurement first bitpack offset is not zero");
                } else {
                    ensure!(
                        previous.is_some_and(|previous| offset > previous),
                        "measurement bitpack offsets do not strictly ascend"
                    );
                }
                let tx_index = base
                    .checked_add(offset)
                    .context("measurement bitpack transaction index overflow")?;
                accept_local_posting(
                    ExactPosting {
                        block_id: span.block_id,
                        tx_index,
                        roles: unpack_role(roles, index),
                    },
                    span,
                    &mut local,
                    ordinal_length,
                    visit,
                )?;
                previous = Some(offset);
            }
            ensure!(
                position == packed.len(),
                "measurement packed offset length differs"
            );
            ensure!(
                bit_width(previous.unwrap_or(0)) == width,
                "measurement bit width is not minimal"
            );
        }
        LOCAL_CODEC_BITMAP => {
            let bitmap_len = bitmap_len(span.tx_count)?;
            ensure!(
                bitmap_len <= postings::MAX_PAGE_DECODED_BYTES as usize,
                "measurement bitmap exceeds decode guard"
            );
            let bitmap = take(input, cursor, bitmap_len, "measurement membership bitmap")?;
            validate_bitmap_tail(bitmap, span.tx_count)?;
            let set_bits = bitmap.iter().try_fold(0_usize, |total, byte| {
                total.checked_add(byte.count_ones() as usize)
            });
            ensure!(
                set_bits == Some(count),
                "measurement bitmap population differs from posting count"
            );
            let roles_len = count.div_ceil(2);
            let roles = take(input, cursor, roles_len, "measurement bitmap roles")?;
            validate_role_padding(roles, count)?;
            let mut role_index = 0_usize;
            for (byte_index, &byte) in bitmap.iter().enumerate() {
                let mut remaining = byte;
                while remaining != 0 {
                    let bit = remaining.trailing_zeros() as usize;
                    let tx_index = byte_index
                        .checked_mul(8)
                        .and_then(|base| base.checked_add(bit))
                        .context("measurement bitmap transaction index overflow")?;
                    accept_local_posting(
                        ExactPosting {
                            block_id: span.block_id,
                            tx_index: u32::try_from(tx_index)?,
                            roles: unpack_role(roles, role_index),
                        },
                        span,
                        &mut local,
                        ordinal_length,
                        visit,
                    )?;
                    role_index += 1;
                    remaining &= remaining - 1;
                }
            }
            ensure!(
                role_index == count,
                "measurement bitmap roles do not cover postings"
            );
        }
        _ => bail!("unknown measurement local codec {codec}"),
    }
    let actual_length = cursor
        .checked_sub(payload_start)
        .context("measurement local payload offset underflow")?;
    ensure!(
        local.bytes_for(codec)? == actual_length,
        "measurement local payload length differs from decoded values"
    );
    ensure!(
        local.preferred()?.0 == codec,
        "measurement block group does not use its canonical local codec"
    );
    Ok(())
}

fn accept_local_posting(
    posting: ExactPosting,
    span: BlockSpan,
    local: &mut LocalPayloadLength,
    ordinal_length: &mut OrdinalPayloadLength,
    visit: &mut Option<&mut dyn FnMut(ExactPosting) -> Result<()>>,
) -> Result<()> {
    local.push(posting)?;
    ordinal_length.push(posting, span)?;
    if let Some(callback) = visit.as_mut() {
        (**callback)(posting)?;
    }
    Ok(())
}

fn uleb128_len(mut value: u64) -> usize {
    let mut length = 1_usize;
    while value >= 0x80 {
        value >>= 7;
        length += 1;
    }
    length
}

fn exact_to_ordinal_keys(
    keys: &[ExactKeyPostings],
    layout: &BlockLayout<'_>,
) -> Result<Vec<postings::KeyPostings>> {
    let mut output = Vec::new();
    output
        .try_reserve_exact(keys.len())
        .context("reserve canonical measurement account keys")?;
    for key in keys {
        let mut converted = Vec::new();
        converted
            .try_reserve_exact(key.postings.len())
            .context("reserve canonical measurement postings")?;
        for posting in &key.postings {
            ensure!(
                posting.roles & !postings::ROLE_MASK == 0,
                "measurement posting has unknown role bits"
            );
            let span = layout.span_for_block(posting.block_id)?;
            ensure!(
                posting.tx_index < span.tx_count,
                "measurement transaction index is outside its block"
            );
            let transaction_ordinal = span
                .first_tx_ordinal
                .checked_add(u64::from(posting.tx_index))
                .context("measurement transaction ordinal overflow")?;
            converted.push(postings::Posting {
                transaction_ordinal,
                roles: posting.roles,
            });
        }
        output.push(postings::KeyPostings {
            key: key.key,
            postings: converted,
        });
    }
    Ok(output)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateMeasurementOptions {
    pub workers: Option<usize>,
    pub zstd_levels: Vec<i32>,
}

impl Default for CandidateMeasurementOptions {
    fn default() -> Self {
        Self {
            workers: None,
            zstd_levels: DEFAULT_ZSTD_LEVELS.to_vec(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CurrentReverseBytes {
    pub decoded_page_bytes: u64,
    pub stored_page_bytes: u64,
    pub directory_bytes: u64,
    pub pages_file_bytes: u64,
    pub control_file_bytes: u64,
    pub coverage_file_bytes: u64,
    pub total_reverse_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct CodecSelectionTotals {
    pub ordinal_key_fragments: u64,
    pub block_group_key_fragments: u64,
    pub local_varint_groups: u64,
    pub local_bitpack_groups: u64,
    pub local_bitmap_groups: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ZstdByteProjection {
    pub level: i32,
    pub frame_bytes: u64,
    pub selected_stored_page_bytes: u64,
    pub projected_pages_file_bytes: u64,
    pub projected_total_reverse_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AdaptiveReverseBytes {
    pub decoded_page_bytes: u64,
    pub codec_selection: CodecSelectionTotals,
    pub zstd: Vec<ZstdByteProjection>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LevelTiming {
    pub level: i32,
    pub compression_worker_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReverseMeasurementTiming {
    pub open_wall_ms: u64,
    pub measurement_wall_ms: u64,
    pub total_wall_ms: u64,
    pub page_decode_worker_ms: u64,
    pub adaptive_encode_worker_ms: u64,
    pub zstd: Vec<LevelTiming>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct LiveBudgetReport {
    pub limit_bytes: u64,
    pub peak_permit_bytes: u64,
    pub peak_live_items: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReverseMeasurementReport {
    pub schema: &'static str,
    pub status: &'static str,
    pub candidate: String,
    pub epoch: u64,
    pub slots_per_epoch: u64,
    pub blocks: u64,
    pub transactions: u64,
    pub registry_entries: u32,
    pub pages: u64,
    pub continuation_pages: u64,
    pub key_fragments: u64,
    pub distinct_accounts: u64,
    pub postings: u64,
    pub available_parallelism: usize,
    pub requested_workers: Option<usize>,
    pub workers: usize,
    pub worker_cap: usize,
    pub work_window_pages: usize,
    pub candidate_open_file_bound: usize,
    pub live_budget: LiveBudgetReport,
    pub peak_observed_zstd_workspace_bytes: u64,
    pub current: CurrentReverseBytes,
    pub adaptive: AdaptiveReverseBytes,
    pub timing: ReverseMeasurementTiming,
}

#[derive(Debug, Default)]
struct BudgetState {
    live_bytes: usize,
    live_items: usize,
    peak_bytes: usize,
    peak_items: usize,
}

#[derive(Debug)]
struct LiveByteBudget {
    limit: usize,
    state: Mutex<BudgetState>,
    changed: Condvar,
}

impl LiveByteBudget {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            state: Mutex::new(BudgetState::default()),
            changed: Condvar::new(),
        }
    }

    fn acquire(self: &Arc<Self>, bytes: usize) -> Result<LiveBytePermit> {
        ensure!(
            bytes != 0 && bytes <= self.limit,
            "measurement page permit {bytes} exceeds live-byte budget {}",
            self.limit
        );
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("measurement live-byte budget is poisoned"))?;
        while state.live_bytes > self.limit - bytes {
            state = self
                .changed
                .wait(state)
                .map_err(|_| anyhow::anyhow!("measurement live-byte budget is poisoned"))?;
        }
        state.live_bytes += bytes;
        state.live_items += 1;
        state.peak_bytes = state.peak_bytes.max(state.live_bytes);
        state.peak_items = state.peak_items.max(state.live_items);
        Ok(LiveBytePermit {
            budget: Arc::clone(self),
            bytes,
        })
    }

    fn report(&self) -> Result<LiveBudgetReport> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("measurement live-byte budget is poisoned"))?;
        ensure!(
            state.live_bytes == 0 && state.live_items == 0,
            "measurement live-byte permits remain after workers stopped"
        );
        Ok(LiveBudgetReport {
            limit_bytes: u64::try_from(self.limit)?,
            peak_permit_bytes: u64::try_from(state.peak_bytes)?,
            peak_live_items: u64::try_from(state.peak_items)?,
        })
    }
}

struct LiveBytePermit {
    budget: Arc<LiveByteBudget>,
    bytes: usize,
}

impl Drop for LiveBytePermit {
    fn drop(&mut self) {
        let mut state = match self.budget.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.live_bytes -= self.bytes;
        state.live_items -= 1;
        self.budget.changed.notify_all();
    }
}

#[derive(Debug, Clone, Copy)]
struct PageJob {
    ordinal: usize,
    entry: postings::PageDirectoryEntry,
}

#[derive(Debug)]
struct PageLevelResult {
    level: i32,
    frame_bytes: u64,
    selected_bytes: u64,
    elapsed: Duration,
    workspace_bytes: usize,
}

#[derive(Debug)]
struct PageResult {
    key_fragments: u64,
    postings: u64,
    continuation: bool,
    stats: MeasurementStats,
    levels: Vec<PageLevelResult>,
    decode_elapsed: Duration,
    encode_elapsed: Duration,
}

#[derive(Debug)]
struct ReverseTotals {
    pages: u64,
    continuation_pages: u64,
    key_fragments: u64,
    postings: u64,
    current_decoded_bytes: u64,
    current_stored_bytes: u64,
    adaptive_decoded_bytes: u64,
    codec: CodecSelectionTotals,
    level_frame_bytes: Vec<u64>,
    level_selected_bytes: Vec<u64>,
    decode_elapsed: Duration,
    encode_elapsed: Duration,
    level_elapsed: Vec<Duration>,
    max_zstd_workspace_bytes: usize,
}

impl ReverseTotals {
    fn new(level_count: usize) -> Self {
        Self {
            pages: 0,
            continuation_pages: 0,
            key_fragments: 0,
            postings: 0,
            current_decoded_bytes: 0,
            current_stored_bytes: 0,
            adaptive_decoded_bytes: 0,
            codec: CodecSelectionTotals::default(),
            level_frame_bytes: vec![0; level_count],
            level_selected_bytes: vec![0; level_count],
            decode_elapsed: Duration::ZERO,
            encode_elapsed: Duration::ZERO,
            level_elapsed: vec![Duration::ZERO; level_count],
            max_zstd_workspace_bytes: 0,
        }
    }

    fn add(&mut self, page: PageResult, entry: postings::PageDirectoryEntry) -> Result<()> {
        self.pages = checked_add_u64(self.pages, 1, "measurement page count")?;
        self.continuation_pages = checked_add_u64(
            self.continuation_pages,
            u64::from(page.continuation),
            "measurement continuation count",
        )?;
        self.key_fragments = checked_add_u64(
            self.key_fragments,
            page.key_fragments,
            "measurement key fragment count",
        )?;
        self.postings = checked_add_u64(self.postings, page.postings, "measurement posting count")?;
        self.current_decoded_bytes = checked_add_u64(
            self.current_decoded_bytes,
            u64::from(entry.decoded_len),
            "current decoded page bytes",
        )?;
        self.current_stored_bytes = checked_add_u64(
            self.current_stored_bytes,
            u64::from(entry.stored_len),
            "current stored page bytes",
        )?;
        self.adaptive_decoded_bytes = checked_add_u64(
            self.adaptive_decoded_bytes,
            u64::try_from(page.stats.adaptive_page_bytes)?,
            "adaptive decoded page bytes",
        )?;
        self.codec.ordinal_key_fragments = checked_add_u64(
            self.codec.ordinal_key_fragments,
            u64::from(page.stats.ordinal_keys),
            "ordinal key fragment count",
        )?;
        self.codec.block_group_key_fragments = checked_add_u64(
            self.codec.block_group_key_fragments,
            u64::from(page.stats.block_group_keys),
            "block-group key fragment count",
        )?;
        self.codec.local_varint_groups = checked_add_u64(
            self.codec.local_varint_groups,
            u64::from(page.stats.local_varint_groups),
            "local varint group count",
        )?;
        self.codec.local_bitpack_groups = checked_add_u64(
            self.codec.local_bitpack_groups,
            u64::from(page.stats.local_bitpack_groups),
            "local bitpack group count",
        )?;
        self.codec.local_bitmap_groups = checked_add_u64(
            self.codec.local_bitmap_groups,
            u64::from(page.stats.local_bitmap_groups),
            "local bitmap group count",
        )?;
        ensure!(
            page.levels.len() == self.level_frame_bytes.len(),
            "measurement page zstd level count differs"
        );
        for (index, level) in page.levels.into_iter().enumerate() {
            self.level_frame_bytes[index] = checked_add_u64(
                self.level_frame_bytes[index],
                level.frame_bytes,
                "measurement zstd frame bytes",
            )?;
            self.level_selected_bytes[index] = checked_add_u64(
                self.level_selected_bytes[index],
                level.selected_bytes,
                "measurement selected page bytes",
            )?;
            self.level_elapsed[index] = self.level_elapsed[index]
                .checked_add(level.elapsed)
                .context("measurement zstd worker duration overflow")?;
            self.max_zstd_workspace_bytes =
                self.max_zstd_workspace_bytes.max(level.workspace_bytes);
        }
        self.decode_elapsed = self
            .decode_elapsed
            .checked_add(page.decode_elapsed)
            .context("measurement decode worker duration overflow")?;
        self.encode_elapsed = self
            .encode_elapsed
            .checked_add(page.encode_elapsed)
            .context("measurement encode worker duration overflow")?;
        Ok(())
    }
}

/// Measure the existing reverse pages without writing a replacement payload.
pub fn measure_candidate_reverse(
    root: impl AsRef<Path>,
    options: CandidateMeasurementOptions,
) -> Result<ReverseMeasurementReport> {
    let total_started = Instant::now();
    let root = root.as_ref();
    ensure!(root.is_dir(), "measurement candidate is not a directory");
    let index_bytes = fs::metadata(root.join(super::standalone_v2::INDEX_FILE))
        .context("inspect standalone block index before measurement open")?
        .len();
    ensure!(
        index_bytes <= MAX_STANDALONE_INDEX_BYTES,
        "standalone block index exceeds the measurement memory guard"
    );
    let levels = validate_measurement_zstd_levels(options.zstd_levels)?;
    let available_parallelism = thread::available_parallelism().map_or(1, usize::from);
    let requested = options
        .workers
        .unwrap_or(available_parallelism.min(MAX_MEASUREMENT_WORKERS));
    ensure!(requested != 0, "measurement worker count is zero");
    ensure!(
        requested <= MAX_MEASUREMENT_WORKERS,
        "measurement worker count exceeds {MAX_MEASUREMENT_WORKERS}"
    );

    let open_started = Instant::now();
    let reader = Arc::new(super::Reader::open(root)?);
    let open_elapsed = open_started.elapsed();
    let selected_blocks = usize::try_from(reader.standalone.header.selected_blocks)
        .context("standalone block count exceeds usize")?;
    let mut spans = Vec::new();
    spans
        .try_reserve_exact(selected_blocks)
        .context("reserve measurement block spans")?;
    for ordinal in 0..selected_blocks {
        let row = reader
            .standalone
            .block(ordinal)
            .context("standalone block row is missing")?;
        spans.push(BlockSpan {
            block_id: row.block_id,
            first_tx_ordinal: row.first_tx_ordinal,
            tx_count: row.tx_count,
        });
    }
    let layout = Arc::new(ValidatedBlockLayout::new(spans)?);
    ensure!(
        layout.transactions() == reader.header.binding.selected_transactions,
        "measurement block spans differ from reverse transaction binding"
    );
    let page_count = reader.directory.len();
    let workers = requested.min(page_count.max(1));
    let work_window_pages = workers
        .checked_mul(2)
        .context("measurement work window overflow")?;
    let budget = Arc::new(LiveByteBudget::new(LIVE_BYTE_BUDGET));
    let measure_started = Instant::now();
    let totals = run_reverse_workers(
        Arc::clone(&reader),
        layout,
        Arc::new(levels.clone()),
        workers,
        work_window_pages,
        Arc::clone(&budget),
    )?;
    let measurement_elapsed = measure_started.elapsed();
    ensure!(
        totals.pages == u64::try_from(page_count)?,
        "measurement page count differs from directory"
    );
    ensure!(
        totals.postings == reader.control.postings,
        "measurement posting total differs from control"
    );
    ensure!(
        totals.key_fragments
            == reader
                .directory
                .iter()
                .try_fold(0_u64, |total, entry| {
                    total.checked_add(u64::from(entry.key_count))
                })
                .context("directory key fragment count overflow")?,
        "measurement key fragment total differs from directory"
    );
    ensure!(
        totals
            .codec
            .ordinal_key_fragments
            .checked_add(totals.codec.block_group_key_fragments)
            == Some(totals.key_fragments),
        "measurement key codec counts differ from key fragments"
    );
    ensure!(
        totals.adaptive_decoded_bytes
            <= totals
                .current_decoded_bytes
                .checked_add(totals.key_fragments)
                .context("adaptive ordinal-fallback total bound overflow")?,
        "adaptive page total exceeds the ordinal-fallback bound"
    );

    let directory_bytes = u64::try_from(page_count)?
        .checked_mul(postings::DIRECTORY_ENTRY_LEN as u64)
        .context("measurement directory byte count overflow")?;
    let pages_file_bytes = reader.file.metadata()?.len();
    let fixed_page_file_bytes = (super::HEADER_LEN as u64)
        .checked_add(directory_bytes)
        .and_then(|value| value.checked_add(postings::DIRECTORY_FOOTER_LEN as u64))
        .context("measurement fixed page-file bytes overflow")?;
    ensure!(
        fixed_page_file_bytes.checked_add(totals.current_stored_bytes) == Some(pages_file_bytes),
        "current page byte totals differ from file length"
    );
    let control_file_bytes = fs::metadata(root.join(super::CONTROL_FILE))?.len();
    let coverage_file_bytes = fs::metadata(root.join(super::COVERAGE_FILE))?.len();
    let total_reverse_bytes = pages_file_bytes
        .checked_add(control_file_bytes)
        .and_then(|value| value.checked_add(coverage_file_bytes))
        .context("current reverse byte total overflow")?;
    let current = CurrentReverseBytes {
        decoded_page_bytes: totals.current_decoded_bytes,
        stored_page_bytes: totals.current_stored_bytes,
        directory_bytes,
        pages_file_bytes,
        control_file_bytes,
        coverage_file_bytes,
        total_reverse_bytes,
    };
    let mut zstd = Vec::with_capacity(levels.len());
    let mut level_timing = Vec::with_capacity(levels.len());
    for (index, &level) in levels.iter().enumerate() {
        ensure!(
            totals.level_selected_bytes[index] <= totals.adaptive_decoded_bytes
                && totals.level_selected_bytes[index] <= totals.level_frame_bytes[index],
            "selected adaptive page bytes exceed a source candidate"
        );
        let projected_pages_file_bytes = fixed_page_file_bytes
            .checked_add(totals.level_selected_bytes[index])
            .context("projected adaptive page-file bytes overflow")?;
        let projected_total_reverse_bytes = projected_pages_file_bytes
            .checked_add(control_file_bytes)
            .and_then(|value| value.checked_add(coverage_file_bytes))
            .context("projected adaptive reverse bytes overflow")?;
        zstd.push(ZstdByteProjection {
            level,
            frame_bytes: totals.level_frame_bytes[index],
            selected_stored_page_bytes: totals.level_selected_bytes[index],
            projected_pages_file_bytes,
            projected_total_reverse_bytes,
        });
        level_timing.push(LevelTiming {
            level,
            compression_worker_ms: duration_millis(totals.level_elapsed[index])?,
        });
    }
    let live_budget = budget.report()?;
    let total_elapsed = total_started.elapsed();
    Ok(ReverseMeasurementReport {
        schema: "account-postings-adaptive-measurement-v1",
        status: "read-only-counters-no-payload-output",
        candidate: root.display().to_string(),
        epoch: reader.header.binding.epoch,
        slots_per_epoch: reader.header.binding.slots_per_epoch,
        blocks: reader.header.binding.selected_blocks,
        transactions: reader.header.binding.selected_transactions,
        registry_entries: reader.header.binding.registry_entries,
        pages: totals.pages,
        continuation_pages: totals.continuation_pages,
        key_fragments: totals.key_fragments,
        distinct_accounts: reader.control.distinct_accounts,
        postings: totals.postings,
        available_parallelism,
        requested_workers: options.workers,
        workers,
        worker_cap: MAX_MEASUREMENT_WORKERS,
        work_window_pages,
        candidate_open_file_bound: super::standalone_v2::OBJECT_COUNT + 4,
        live_budget,
        peak_observed_zstd_workspace_bytes: u64::try_from(totals.max_zstd_workspace_bytes)?,
        current,
        adaptive: AdaptiveReverseBytes {
            decoded_page_bytes: totals.adaptive_decoded_bytes,
            codec_selection: totals.codec,
            zstd,
        },
        timing: ReverseMeasurementTiming {
            open_wall_ms: duration_millis(open_elapsed)?,
            measurement_wall_ms: duration_millis(measurement_elapsed)?,
            total_wall_ms: duration_millis(total_elapsed)?,
            page_decode_worker_ms: duration_millis(totals.decode_elapsed)?,
            adaptive_encode_worker_ms: duration_millis(totals.encode_elapsed)?,
            zstd: level_timing,
        },
    })
}

fn run_reverse_workers(
    reader: Arc<super::Reader>,
    layout: Arc<ValidatedBlockLayout>,
    levels: Arc<Vec<i32>>,
    workers: usize,
    work_window_pages: usize,
    budget: Arc<LiveByteBudget>,
) -> Result<ReverseTotals> {
    let (result_sender, result_receiver) = mpsc::sync_channel(work_window_pages);
    let mut job_senders = Vec::with_capacity(workers);
    let mut handles = Vec::with_capacity(workers);
    for worker in 0..workers {
        let (job_sender, job_receiver) = mpsc::sync_channel(2);
        job_senders.push(job_sender);
        let result_sender = result_sender.clone();
        let reader = Arc::clone(&reader);
        let layout = Arc::clone(&layout);
        let levels = Arc::clone(&levels);
        let budget = Arc::clone(&budget);
        handles.push(
            thread::Builder::new()
                .name(format!("reverse-measure-{worker}"))
                .spawn(move || {
                    reverse_worker(
                        job_receiver,
                        result_sender,
                        &reader,
                        &layout,
                        &levels,
                        &budget,
                    )
                })
                .context("spawn reverse measurement worker")?,
        );
    }
    drop(result_sender);

    let run_result = (|| -> Result<ReverseTotals> {
        let mut totals = ReverseTotals::new(levels.len());
        let mut batch_start = 0_usize;
        while batch_start < reader.directory.len() {
            let batch_end = batch_start
                .checked_add(work_window_pages)
                .map_or(reader.directory.len(), |end| {
                    end.min(reader.directory.len())
                });
            for ordinal in batch_start..batch_end {
                job_senders[ordinal % workers]
                    .send(PageJob {
                        ordinal,
                        entry: reader.directory[ordinal],
                    })
                    .context("send reverse measurement job")?;
            }
            let mut completed = Vec::with_capacity(batch_end - batch_start);
            for _ in batch_start..batch_end {
                completed.push(
                    result_receiver
                        .recv()
                        .context("receive reverse measurement result")?,
                );
            }
            completed.sort_by_key(|(ordinal, _)| *ordinal);
            for (offset, (ordinal, result)) in completed.into_iter().enumerate() {
                let expected = batch_start + offset;
                ensure!(
                    ordinal == expected,
                    "reverse measurement result order differs"
                );
                totals.add(result?, reader.directory[ordinal])?;
            }
            batch_start = batch_end;
        }
        Ok(totals)
    })();
    drop(job_senders);
    for handle in handles {
        ensure!(handle.join().is_ok(), "reverse measurement worker panicked");
    }
    run_result
}

fn reverse_worker(
    jobs: Receiver<PageJob>,
    results: SyncSender<(usize, Result<PageResult>)>,
    reader: &super::Reader,
    layout: &ValidatedBlockLayout,
    levels: &[i32],
    budget: &Arc<LiveByteBudget>,
) {
    while let Ok(job) = jobs.recv() {
        let result = catch_measurement_panic("reverse measurement worker", || {
            measure_reverse_page(reader, layout, levels, budget, job)
                .with_context(|| format!("measure reverse page {}", job.ordinal))
        });
        if results.send((job.ordinal, result)).is_err() {
            break;
        }
    }
}

fn catch_measurement_panic<T>(label: &str, work: impl FnOnce() -> Result<T>) -> Result<T> {
    match catch_unwind(AssertUnwindSafe(work)) {
        Ok(result) => result,
        Err(payload) => {
            let detail = payload
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("non-string panic payload");
            bail!("{label} panicked: {detail}")
        }
    }
}

fn measure_reverse_page(
    reader: &super::Reader,
    layout: &ValidatedBlockLayout,
    levels: &[i32],
    budget: &Arc<LiveByteBudget>,
    job: PageJob,
) -> Result<PageResult> {
    let permit_bytes = reverse_page_permit_bytes(job.entry)?;
    let _permit = budget.acquire(permit_bytes)?;
    let decode_started = Instant::now();
    let keys = reader.read_page(job.entry)?;
    let decode_elapsed = decode_started.elapsed();
    let key_fragments = u64::try_from(keys.len())?;
    ensure!(
        key_fragments == u64::from(job.entry.key_count),
        "decoded reverse key count differs from directory"
    );
    let posting_count = keys.iter().try_fold(0_u64, |total, key| {
        total.checked_add(u64::try_from(key.postings.len()).ok()?)
    });
    let posting_count = posting_count.context("reverse page posting count overflow")?;
    let encode_started = Instant::now();
    let encoded = encode_page_with_layout(&keys, layout)?;
    let encode_elapsed = encode_started.elapsed();
    drop(keys);
    ensure!(
        encoded.stats.current_page_bytes == job.entry.decoded_len as usize,
        "current reverse page does not re-encode to its declared length"
    );
    let adaptive_bound = (job.entry.decoded_len as usize)
        .checked_add(job.entry.key_count as usize)
        .context("adaptive reverse page bound overflow")?;
    ensure!(
        encoded.bytes.len() <= adaptive_bound,
        "adaptive reverse page exceeds ordinal fallback bound"
    );
    let compression_bound = zstd::zstd_safe::compress_bound(adaptive_bound);
    let mut level_results = Vec::with_capacity(levels.len());
    for &level in levels {
        let started = Instant::now();
        let (frame, workspace_bytes) = compress_measurement_frame(&encoded.bytes, level)?;
        let elapsed = started.elapsed();
        ensure!(
            frame.len() <= compression_bound,
            "measurement zstd frame exceeds compression bound"
        );
        level_results.push(PageLevelResult {
            level,
            frame_bytes: u64::try_from(frame.len())?,
            selected_bytes: u64::try_from(frame.len().min(encoded.bytes.len()))?,
            elapsed,
            workspace_bytes,
        });
    }
    ensure!(
        level_results
            .iter()
            .map(|result| result.level)
            .eq(levels.iter().copied()),
        "measurement zstd result order differs"
    );
    Ok(PageResult {
        key_fragments,
        postings: posting_count,
        continuation: job.entry.flags != 0,
        stats: encoded.stats,
        levels: level_results,
        decode_elapsed,
        encode_elapsed,
    })
}

fn reverse_page_permit_bytes(entry: postings::PageDirectoryEntry) -> Result<usize> {
    let stored = entry.stored_len as usize;
    let decoded = entry.decoded_len as usize;
    let adaptive = decoded
        .checked_add(entry.key_count as usize)
        .context("adaptive reverse permit bound overflow")?;
    ensure!(
        adaptive <= postings::MAX_PAGE_DECODED_BYTES as usize,
        "adaptive reverse page cannot fit the decode guard"
    );
    let key_heap = (entry.key_count as usize)
        .checked_mul(std::mem::size_of::<postings::KeyPostings>())
        .and_then(|value| {
            value.checked_add(
                (postings::MAX_POSTINGS_PER_PAGE as usize)
                    .checked_mul(std::mem::size_of::<postings::Posting>())?,
            )
        })
        .context("decoded reverse posting heap bound overflow")?;
    let decode_phase = stored
        .checked_add(decoded)
        .and_then(|value| value.checked_add(decoded))
        .and_then(|value| value.checked_add(key_heap))
        .and_then(|value| value.checked_add(MAX_ZSTD_DECOMPRESSION_FIXED_BYTES))
        .context("reverse decode live-byte bound overflow")?;
    let encode_phase = key_heap
        .checked_add(adaptive)
        .and_then(|value| value.checked_add(decoded.checked_mul(2)?))
        .and_then(|value| value.checked_add(MAX_BLOCK_CODEC_SCRATCH_BYTES))
        .context("reverse encode live-byte bound overflow")?;
    let compression_phase = adaptive
        .checked_add(zstd::zstd_safe::compress_bound(adaptive))
        .and_then(|value| value.checked_add(MAX_ZSTD_COMPRESSION_WORKSPACE_BYTES))
        .context("reverse compression live-byte bound overflow")?;
    Ok(decode_phase.max(encode_phase).max(compression_phase))
}

fn compress_measurement_frame(input: &[u8], level: i32) -> Result<(Vec<u8>, usize)> {
    let mut compressor = zstd::bulk::Compressor::new(level)
        .with_context(|| format!("create zstd-{level} measurement compressor"))?;
    compressor
        .include_checksum(true)
        .with_context(|| format!("enable zstd-{level} measurement checksum"))?;
    let frame = compressor
        .compress(input)
        .with_context(|| format!("compress zstd-{level} measurement frame"))?;
    let workspace = compressor.context_mut().sizeof();
    ensure!(
        workspace <= MAX_ZSTD_COMPRESSION_WORKSPACE_BYTES,
        "zstd-{level} workspace {workspace} exceeds measurement bound {MAX_ZSTD_COMPRESSION_WORKSPACE_BYTES}"
    );
    Ok((frame, workspace))
}

pub fn validate_measurement_zstd_levels(mut levels: Vec<i32>) -> Result<Vec<i32>> {
    ensure!(
        !levels.is_empty(),
        "no measurement zstd levels were selected"
    );
    ensure!(
        levels.iter().all(|level| matches!(level, 1 | 3 | 5 | 9)),
        "measurement zstd level must be one of 1, 3, 5, or 9"
    );
    levels.sort_unstable();
    ensure!(
        levels.windows(2).all(|pair| pair[0] != pair[1]),
        "measurement zstd levels repeat"
    );
    Ok(levels)
}

fn checked_add_u64(left: u64, right: u64, label: &str) -> Result<u64> {
    left.checked_add(right)
        .with_context(|| format!("{label} overflow"))
}

fn duration_millis(duration: Duration) -> Result<u64> {
    u64::try_from(duration.as_millis()).context("measurement duration exceeds u64 milliseconds")
}

fn encode_ordinal_payload(input: &[postings::Posting]) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut previous = None;
    for posting in input {
        ensure!(
            posting.roles & !postings::ROLE_MASK == 0,
            "measurement posting has unknown role bits"
        );
        let gap = match previous {
            None => posting.transaction_ordinal,
            Some(previous) => posting
                .transaction_ordinal
                .checked_sub(previous)
                .filter(|gap| *gap != 0)
                .context("measurement transaction ordinals do not strictly ascend")?,
        };
        let packed = gap
            .checked_shl(4)
            .filter(|packed| *packed >> 4 == gap)
            .context("measurement transaction ordinal gap overflows role packing")?
            | u64::from(posting.roles);
        write_uleb128(&mut output, packed);
        previous = Some(posting.transaction_ordinal);
    }
    Ok(output)
}

fn decode_ordinal_payload(
    input: &[u8],
    cursor: &mut usize,
    count: usize,
    layout: &BlockLayout<'_>,
) -> Result<Vec<ExactPosting>> {
    let mut output = Vec::with_capacity(count);
    let mut ordinal = 0_u64;
    for index in 0..count {
        let packed = read_uleb128(input, cursor).context("read measurement ordinal posting")?;
        let gap = packed >> 4;
        if index == 0 {
            ordinal = gap;
        } else {
            ensure!(gap != 0, "measurement transaction ordinals repeat");
            ordinal = ordinal
                .checked_add(gap)
                .context("measurement transaction ordinal overflow")?;
        }
        let (_, mut posting) = layout.resolve_ordinal(ordinal)?;
        posting.roles = (packed & u64::from(postings::ROLE_MASK)) as u8;
        output.push(posting);
    }
    Ok(output)
}

fn encode_block_group_payload(
    input: &[postings::Posting],
    layout: &BlockLayout<'_>,
) -> Result<(Vec<u8>, LocalStats, usize)> {
    let mut resolved = Vec::new();
    resolved
        .try_reserve_exact(input.len())
        .context("reserve resolved measurement postings")?;
    for posting in input {
        let (_, mut exact) = layout.resolve_ordinal(posting.transaction_ordinal)?;
        exact.roles = posting.roles;
        resolved.push(exact);
    }
    let group_count = 1_usize
        .checked_add(
            resolved
                .windows(2)
                .filter(|pair| pair[0].block_id != pair[1].block_id)
                .count(),
        )
        .context("measurement block group count overflow")?;
    let mut output = Vec::new();
    write_uleb128(&mut output, u64::try_from(group_count)?);
    let mut local_stats = LocalStats::default();
    let mut peak_scratch_bytes = 0_usize;
    let mut start = 0_usize;
    let mut previous_block = None;
    while start < resolved.len() {
        let block_id = resolved[start].block_id;
        let end = resolved[start..]
            .iter()
            .position(|posting| posting.block_id != block_id)
            .map_or(resolved.len(), |offset| start + offset);
        let group = &resolved[start..end];
        let block_gap = match previous_block {
            None => block_id,
            Some(previous) => block_id
                .checked_sub(previous)
                .filter(|gap| *gap != 0)
                .context("measurement block groups do not strictly ascend")?,
        };
        write_uleb128(&mut output, u64::from(block_gap));
        write_uleb128(&mut output, u64::try_from(group.len())?);
        let span = layout.span_for_block(block_id)?;
        let varint = encode_local_varint(group)?;
        let bitpack = encode_local_bitpack(group)?;
        peak_scratch_bytes = peak_scratch_bytes.max(ensure_block_codec_scratch(&[
            resolved
                .capacity()
                .checked_mul(std::mem::size_of::<ExactPosting>())
                .context("resolved measurement posting capacity overflow")?,
            output.capacity(),
            varint.capacity(),
            bitpack.capacity(),
        ])?);
        let bitmap_length = local_bitmap_length(group, span.tx_count)?;
        let (mut codec, mut payload) = choose_local_codec(varint, bitpack, None);
        if bitmap_length.is_some_and(|length| length < payload.len()) {
            let bitmap = encode_local_bitmap(group, span.tx_count)?
                .context("selected measurement bitmap is outside the page guard")?;
            peak_scratch_bytes = peak_scratch_bytes.max(ensure_block_codec_scratch(&[
                resolved
                    .capacity()
                    .checked_mul(std::mem::size_of::<ExactPosting>())
                    .context("resolved measurement posting capacity overflow")?,
                output.capacity(),
                payload.capacity(),
                bitmap.capacity(),
            ])?);
            ensure!(
                Some(bitmap.len()) == bitmap_length,
                "measurement bitmap length differs from its selection"
            );
            codec = LOCAL_CODEC_BITMAP;
            payload = bitmap;
        }
        output.push(codec);
        output.extend_from_slice(&payload);
        match codec {
            LOCAL_CODEC_VARINT => local_stats.varint += 1,
            LOCAL_CODEC_BITPACK => local_stats.bitpack += 1,
            LOCAL_CODEC_BITMAP => local_stats.bitmap += 1,
            _ => unreachable!("chooser returned a known local codec"),
        }
        previous_block = Some(block_id);
        start = end;
    }
    Ok((output, local_stats, peak_scratch_bytes))
}

fn ensure_block_codec_scratch(capacities: &[usize]) -> Result<usize> {
    let total = capacities
        .iter()
        .try_fold(0_usize, |total, capacity| total.checked_add(*capacity));
    ensure!(
        total.is_some_and(|total| total <= MAX_BLOCK_CODEC_SCRATCH_BYTES),
        "measurement block codec scratch exceeds {MAX_BLOCK_CODEC_SCRATCH_BYTES} bytes"
    );
    Ok(total.expect("bounded scratch total is present"))
}

fn decode_block_group_payload(
    input: &[u8],
    cursor: &mut usize,
    posting_count: usize,
    layout: &BlockLayout<'_>,
) -> Result<Vec<ExactPosting>> {
    let group_count = read_bounded_count(
        input,
        cursor,
        posting_count,
        "measurement block group count",
    )?;
    ensure!(group_count != 0, "measurement key has no block groups");
    let mut output = Vec::with_capacity(posting_count);
    let mut block_id = 0_u32;
    let mut decoded_postings = 0_usize;
    for group_index in 0..group_count {
        let gap = read_uleb128_u32(input, cursor).context("read measurement block gap")?;
        if group_index == 0 {
            block_id = gap;
        } else {
            ensure!(gap != 0, "measurement block groups repeat");
            block_id = block_id
                .checked_add(gap)
                .context("measurement block ID overflow")?;
        }
        let remaining = posting_count - decoded_postings;
        let count =
            read_bounded_count(input, cursor, remaining, "measurement local posting count")?;
        ensure!(count != 0, "measurement block group has no postings");
        let span = layout.span_for_block(block_id)?;
        let codec = read_byte(input, cursor, "measurement local codec")?;
        match codec {
            LOCAL_CODEC_VARINT => decode_local_varint(input, cursor, count, span, &mut output)?,
            LOCAL_CODEC_BITPACK => decode_local_bitpack(input, cursor, count, span, &mut output)?,
            LOCAL_CODEC_BITMAP => decode_local_bitmap(input, cursor, count, span, &mut output)?,
            _ => bail!("unknown measurement local codec {codec}"),
        }
        decoded_postings += count;
    }
    ensure!(
        decoded_postings == posting_count,
        "measurement block groups do not cover the declared postings"
    );
    Ok(output)
}

fn encode_local_varint(group: &[ExactPosting]) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut previous = None;
    for posting in group {
        let gap = match previous {
            None => posting.tx_index,
            Some(previous) => posting
                .tx_index
                .checked_sub(previous)
                .filter(|gap| *gap != 0)
                .context("measurement local transaction indexes do not strictly ascend")?,
        };
        write_uleb128(
            &mut output,
            (u64::from(gap) << 4) | u64::from(posting.roles),
        );
        previous = Some(posting.tx_index);
    }
    Ok(output)
}

fn encode_local_bitpack(group: &[ExactPosting]) -> Result<Vec<u8>> {
    let base = group
        .first()
        .context("measurement bitpack group is empty")?
        .tx_index;
    let mut offsets = Vec::new();
    offsets
        .try_reserve_exact(group.len())
        .context("reserve measurement bitpack offsets")?;
    let mut previous = None;
    for posting in group {
        if let Some(previous) = previous {
            ensure!(
                posting.tx_index > previous,
                "measurement bitpack transaction indexes do not strictly ascend"
            );
        }
        offsets.push(
            posting
                .tx_index
                .checked_sub(base)
                .context("measurement bitpack offset underflow")?,
        );
        previous = Some(posting.tx_index);
    }
    let maximum = offsets.last().copied().unwrap_or(0);
    let width = bit_width(maximum);
    let mut output = Vec::new();
    write_uleb128(&mut output, u64::from(base));
    write_uleb128(&mut output, u64::from(width));
    pack_u32_values(&offsets, width, &mut output)?;
    pack_roles(group, &mut output);
    Ok(output)
}

fn encode_local_bitmap(group: &[ExactPosting], tx_count: u32) -> Result<Option<Vec<u8>>> {
    let Some(total) = local_bitmap_length(group, tx_count)? else {
        return Ok(None);
    };
    let bitmap_len = bitmap_len(tx_count)?;
    let mut output = vec![0_u8; bitmap_len];
    let mut previous = None;
    for posting in group {
        ensure!(
            posting.tx_index < tx_count,
            "measurement bitmap transaction index is outside block"
        );
        if let Some(previous) = previous {
            ensure!(
                posting.tx_index > previous,
                "measurement bitmap transaction indexes do not strictly ascend"
            );
        }
        let index = usize::try_from(posting.tx_index)?;
        output[index / 8] |= 1 << (index % 8);
        previous = Some(posting.tx_index);
    }
    pack_roles(group, &mut output);
    ensure!(
        output.len() == total,
        "measurement bitmap payload length differs"
    );
    Ok(Some(output))
}

fn local_bitmap_length(group: &[ExactPosting], tx_count: u32) -> Result<Option<usize>> {
    let total = bitmap_len(tx_count)?
        .checked_add(group.len().div_ceil(2))
        .context("measurement bitmap payload length overflow")?;
    Ok((total <= postings::MAX_PAGE_DECODED_BYTES as usize).then_some(total))
}

fn decode_local_varint(
    input: &[u8],
    cursor: &mut usize,
    count: usize,
    span: BlockSpan,
    output: &mut Vec<ExactPosting>,
) -> Result<()> {
    let mut tx_index = 0_u32;
    for index in 0..count {
        let packed = read_uleb128(input, cursor).context("read measurement local varint")?;
        let gap =
            u32::try_from(packed >> 4).context("measurement local transaction gap exceeds u32")?;
        if index == 0 {
            tx_index = gap;
        } else {
            ensure!(gap != 0, "measurement local transaction indexes repeat");
            tx_index = tx_index
                .checked_add(gap)
                .context("measurement local transaction index overflow")?;
        }
        ensure!(
            tx_index < span.tx_count,
            "measurement local transaction index is outside block"
        );
        output.push(ExactPosting {
            block_id: span.block_id,
            tx_index,
            roles: (packed & u64::from(postings::ROLE_MASK)) as u8,
        });
    }
    Ok(())
}

fn decode_local_bitpack(
    input: &[u8],
    cursor: &mut usize,
    count: usize,
    span: BlockSpan,
    output: &mut Vec<ExactPosting>,
) -> Result<()> {
    let base = read_uleb128_u32(input, cursor).context("read measurement bitpack base")?;
    let width =
        u8::try_from(read_uleb128_u32(input, cursor).context("read measurement bit width")?)
            .context("measurement bit width exceeds u8")?;
    ensure!(width <= 32, "measurement bit width exceeds u32");
    let packed_len = packed_value_bytes(count, width)?;
    let packed = take(input, cursor, packed_len, "measurement packed offsets")?;
    validate_zero_tail_bits(packed, count, width, "measurement packed offsets")?;
    let offsets = unpack_u32_values(packed, count, width)?;
    let roles_len = count.div_ceil(2);
    let roles = take(input, cursor, roles_len, "measurement packed roles")?;
    validate_role_padding(roles, count)?;
    let mut previous = None;
    for (index, offset) in offsets.into_iter().enumerate() {
        if index == 0 {
            ensure!(offset == 0, "measurement first bitpack offset is not zero");
        } else {
            ensure!(
                previous.is_some_and(|previous| offset > previous),
                "measurement bitpack offsets do not strictly ascend"
            );
        }
        let tx_index = base
            .checked_add(offset)
            .context("measurement bitpack transaction index overflow")?;
        ensure!(
            tx_index < span.tx_count,
            "measurement bitpack transaction index is outside block"
        );
        output.push(ExactPosting {
            block_id: span.block_id,
            tx_index,
            roles: unpack_role(roles, index),
        });
        previous = Some(offset);
    }
    let maximum = previous.unwrap_or(0);
    ensure!(
        bit_width(maximum) == width,
        "measurement bit width is not minimal"
    );
    Ok(())
}

fn decode_local_bitmap(
    input: &[u8],
    cursor: &mut usize,
    count: usize,
    span: BlockSpan,
    output: &mut Vec<ExactPosting>,
) -> Result<()> {
    let bitmap_len = bitmap_len(span.tx_count)?;
    ensure!(
        bitmap_len <= postings::MAX_PAGE_DECODED_BYTES as usize,
        "measurement bitmap exceeds decode guard"
    );
    let bitmap = take(input, cursor, bitmap_len, "measurement membership bitmap")?;
    validate_bitmap_tail(bitmap, span.tx_count)?;
    let set_bits = bitmap.iter().try_fold(0_usize, |total, byte| {
        total.checked_add(byte.count_ones() as usize)
    });
    ensure!(
        set_bits == Some(count),
        "measurement bitmap population differs from posting count"
    );
    let roles_len = count.div_ceil(2);
    let roles = take(input, cursor, roles_len, "measurement bitmap roles")?;
    validate_role_padding(roles, count)?;
    let mut role_index = 0_usize;
    for (byte_index, &byte) in bitmap.iter().enumerate() {
        let mut remaining = byte;
        while remaining != 0 {
            let bit = remaining.trailing_zeros() as usize;
            let tx_index = byte_index
                .checked_mul(8)
                .and_then(|base| base.checked_add(bit))
                .context("measurement bitmap transaction index overflow")?;
            ensure!(
                tx_index < span.tx_count as usize,
                "measurement bitmap transaction index is outside block"
            );
            output.push(ExactPosting {
                block_id: span.block_id,
                tx_index: u32::try_from(tx_index)?,
                roles: unpack_role(roles, role_index),
            });
            role_index += 1;
            remaining &= remaining - 1;
        }
    }
    ensure!(
        role_index == count,
        "measurement bitmap roles do not cover postings"
    );
    Ok(())
}

fn choose_key_codec(ordinal: Vec<u8>, block_groups: Vec<u8>) -> (u8, Vec<u8>) {
    if block_groups.len() < ordinal.len() {
        (KEY_CODEC_BLOCK_GROUPS, block_groups)
    } else {
        (KEY_CODEC_ORDINAL_ULEB, ordinal)
    }
}

fn choose_local_codec(varint: Vec<u8>, bitpack: Vec<u8>, bitmap: Option<Vec<u8>>) -> (u8, Vec<u8>) {
    let mut selected = (LOCAL_CODEC_VARINT, varint);
    if bitpack.len() < selected.1.len() {
        selected = (LOCAL_CODEC_BITPACK, bitpack);
    }
    if let Some(bitmap) = bitmap
        && bitmap.len() < selected.1.len()
    {
        selected = (LOCAL_CODEC_BITMAP, bitmap);
    }
    selected
}

fn pack_u32_values(values: &[u32], width: u8, output: &mut Vec<u8>) -> Result<()> {
    ensure!(width <= 32, "measurement pack width exceeds u32");
    if width == 0 {
        ensure!(
            values.iter().all(|value| *value == 0),
            "nonzero measurement value uses zero bit width"
        );
        return Ok(());
    }
    let mask = if width == 32 {
        u64::from(u32::MAX)
    } else {
        (1_u64 << width) - 1
    };
    let mut accumulator = 0_u64;
    let mut bits = 0_u32;
    for &value in values {
        ensure!(
            u64::from(value) <= mask,
            "measurement value exceeds bit width"
        );
        accumulator |= u64::from(value) << bits;
        bits += u32::from(width);
        while bits >= 8 {
            output.push(accumulator as u8);
            accumulator >>= 8;
            bits -= 8;
        }
    }
    if bits != 0 {
        output.push(accumulator as u8);
    }
    Ok(())
}

fn unpack_u32_values(input: &[u8], count: usize, width: u8) -> Result<Vec<u32>> {
    let mut output = Vec::with_capacity(count);
    if width == 0 {
        output.resize(count, 0);
        return Ok(output);
    }
    let mask = if width == 32 {
        u64::from(u32::MAX)
    } else {
        (1_u64 << width) - 1
    };
    let mut accumulator = 0_u64;
    let mut bits = 0_u32;
    let mut position = 0_usize;
    for _ in 0..count {
        while bits < u32::from(width) {
            let byte = *input
                .get(position)
                .context("measurement packed offsets are truncated")?;
            accumulator |= u64::from(byte) << bits;
            bits += 8;
            position += 1;
        }
        output.push(u32::try_from(accumulator & mask)?);
        accumulator >>= width;
        bits -= u32::from(width);
    }
    ensure!(
        position == input.len(),
        "measurement packed offset length differs"
    );
    Ok(output)
}

fn pack_roles(group: &[ExactPosting], output: &mut Vec<u8>) {
    for pair in group.chunks(2) {
        let mut byte = pair[0].roles;
        if let Some(second) = pair.get(1) {
            byte |= second.roles << 4;
        }
        output.push(byte);
    }
}

fn unpack_role(input: &[u8], index: usize) -> u8 {
    let byte = input[index / 2];
    if index.is_multiple_of(2) {
        byte & postings::ROLE_MASK
    } else {
        byte >> 4
    }
}

fn validate_role_padding(input: &[u8], count: usize) -> Result<()> {
    if !count.is_multiple_of(2) {
        ensure!(
            input.last().is_some_and(|byte| byte & 0xf0 == 0),
            "measurement role padding is nonzero"
        );
    }
    Ok(())
}

fn validate_bitmap_tail(bitmap: &[u8], tx_count: u32) -> Result<()> {
    let used = tx_count % 8;
    if used != 0 {
        let mask = (1_u8 << used) - 1;
        ensure!(
            bitmap.last().is_some_and(|byte| byte & !mask == 0),
            "measurement bitmap tail bits are nonzero"
        );
    }
    Ok(())
}

fn validate_zero_tail_bits(input: &[u8], count: usize, width: u8, label: &str) -> Result<()> {
    let bits = count
        .checked_mul(usize::from(width))
        .context("measurement packed bit count overflow")?;
    let used = bits % 8;
    if used != 0 {
        let mask = (1_u8 << used) - 1;
        ensure!(
            input.last().is_some_and(|byte| byte & !mask == 0),
            "{label} tail bits are nonzero"
        );
    }
    Ok(())
}

fn bit_width(value: u32) -> u8 {
    u8::try_from(u32::BITS - value.leading_zeros()).expect("u32 bit width fits u8")
}

fn packed_value_bytes(count: usize, width: u8) -> Result<usize> {
    count
        .checked_mul(usize::from(width))
        .and_then(|bits| bits.checked_add(7))
        .map(|bits| bits / 8)
        .context("measurement packed byte count overflow")
}

fn bitmap_len(tx_count: u32) -> Result<usize> {
    usize::try_from(u64::from(tx_count).div_ceil(8))
        .context("measurement bitmap length does not fit memory")
}

fn read_bounded_count(
    input: &[u8],
    cursor: &mut usize,
    maximum: usize,
    label: &str,
) -> Result<usize> {
    let value = read_uleb128(input, cursor).with_context(|| format!("read {label}"))?;
    let value = usize::try_from(value).with_context(|| format!("{label} exceeds usize"))?;
    ensure!(value <= maximum, "{label} exceeds guard");
    Ok(value)
}

fn read_byte(input: &[u8], cursor: &mut usize, label: &str) -> Result<u8> {
    let byte = *input
        .get(*cursor)
        .with_context(|| format!("{label} is truncated"))?;
    *cursor += 1;
    Ok(byte)
}

fn take<'a>(input: &'a [u8], cursor: &mut usize, length: usize, label: &str) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(length)
        .with_context(|| format!("{label} range overflows"))?;
    let bytes = input
        .get(*cursor..end)
        .with_context(|| format!("{label} is truncated"))?;
    *cursor = end;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn posting(transaction_ordinal: u64, roles: u8) -> postings::Posting {
        postings::Posting {
            transaction_ordinal,
            roles,
        }
    }

    fn expected(keys: &[postings::KeyPostings], spans: &[BlockSpan]) -> Vec<ExactKeyPostings> {
        let layout = BlockLayout::new(spans).unwrap();
        keys.iter()
            .map(|key| ExactKeyPostings {
                key: key.key,
                postings: key
                    .postings
                    .iter()
                    .map(|posting| {
                        let (_, mut exact) =
                            layout.resolve_ordinal(posting.transaction_ordinal).unwrap();
                        exact.roles = posting.roles;
                        exact
                    })
                    .collect(),
            })
            .collect()
    }

    #[test]
    fn all_local_codecs_round_trip_large_u32_block_indexes() {
        let first_one = u64::from(u32::MAX);
        let first_two = first_one + 70_000;
        let spans = [
            BlockSpan {
                block_id: 0,
                first_tx_ordinal: 0,
                tx_count: u32::MAX,
            },
            BlockSpan {
                block_id: 1,
                first_tx_ordinal: first_one,
                tx_count: 70_000,
            },
            BlockSpan {
                block_id: 2,
                first_tx_ordinal: first_two,
                tx_count: 64,
            },
        ];
        let keys = vec![
            postings::KeyPostings {
                key: 7,
                postings: vec![posting(first_one, postings::ROLE_SIGNER)],
            },
            postings::KeyPostings {
                key: 8,
                postings: (0_u64..64)
                    .map(|index| {
                        posting(
                            first_one + index * 1_024,
                            if index.is_multiple_of(2) {
                                postings::ROLE_WRITABLE
                            } else {
                                postings::ROLE_CPI_PROGRAM
                            },
                        )
                    })
                    .collect(),
            },
            postings::KeyPostings {
                key: 9,
                postings: (0_u64..64)
                    .map(|index| posting(first_two + index, (index as u8) & postings::ROLE_MASK))
                    .collect(),
            },
        ];
        let encoded = encode_page(&keys, &spans).unwrap();
        assert_eq!(encoded.stats.block_group_keys, 3);
        assert_eq!(encoded.stats.ordinal_keys, 0);
        assert_eq!(encoded.stats.local_varint_groups, 1);
        assert_eq!(encoded.stats.local_bitpack_groups, 1);
        assert_eq!(encoded.stats.local_bitmap_groups, 1);
        assert_eq!(encoded.stats.current_page_bytes, 276);
        assert_eq!(encoded.stats.adaptive_page_bytes, 224);
        let exact = expected(&keys, &spans);
        assert_eq!(encode_local_varint(&exact[0].postings).unwrap().len(), 1);
        assert_eq!(encode_local_bitpack(&exact[0].postings).unwrap().len(), 3);
        assert_eq!(
            encode_local_bitmap(&exact[0].postings, spans[1].tx_count)
                .unwrap()
                .unwrap()
                .len(),
            8_751
        );
        assert_eq!(encode_local_varint(&exact[1].postings).unwrap().len(), 190);
        assert_eq!(encode_local_bitpack(&exact[1].postings).unwrap().len(), 162);
        assert_eq!(
            encode_local_bitmap(&exact[1].postings, spans[1].tx_count)
                .unwrap()
                .unwrap()
                .len(),
            8_782
        );
        assert_eq!(encode_local_varint(&exact[2].postings).unwrap().len(), 64);
        assert_eq!(encode_local_bitpack(&exact[2].postings).unwrap().len(), 82);
        assert_eq!(
            encode_local_bitmap(&exact[2].postings, spans[2].tx_count)
                .unwrap()
                .unwrap()
                .len(),
            40
        );
        assert_eq!(
            decode_page(&encoded.bytes, 7, keys.len() as u32, &spans).unwrap(),
            exact
        );

        let layout = ValidatedBlockLayout::new(spans.to_vec()).unwrap();
        for expected_key in &exact {
            let mut streamed = Vec::new();
            let summary = visit_page_key_with_layout(
                &encoded.bytes,
                7,
                keys.len() as u32,
                &layout,
                expected_key.key,
                |posting| {
                    streamed.push(posting);
                    Ok(())
                },
            )
            .unwrap();
            assert!(summary.found);
            assert_eq!(summary.postings as usize, expected_key.postings.len());
            assert_eq!(streamed, expected_key.postings);
        }

        let mut blocks = Vec::new();
        let block_summary = visit_page_key_blocks_with_layout(
            &encoded.bytes,
            7,
            keys.len() as u32,
            &layout,
            8,
            postings::ROLE_WRITABLE,
            |block| {
                blocks.push(block);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(block_summary.matching_postings, 32);
        assert_eq!(block_summary.matching_blocks, 1);
        assert_eq!(
            blocks,
            vec![RoleMatchedBlock {
                block_id: 1,
                matching_postings: 32,
            }]
        );
    }

    #[test]
    fn streaming_decoder_matches_materializing_decoder_under_corruption() {
        let spans = [
            BlockSpan {
                block_id: 3,
                first_tx_ordinal: 0,
                tx_count: 96,
            },
            BlockSpan {
                block_id: 9,
                first_tx_ordinal: 96,
                tx_count: 256,
            },
        ];
        let keys = vec![
            postings::KeyPostings {
                key: 5,
                postings: vec![posting(0, postings::ROLE_SIGNER)],
            },
            postings::KeyPostings {
                key: 8,
                postings: (0_u64..64)
                    .map(|index| {
                        posting(
                            96 + index * 4,
                            if index.is_multiple_of(3) {
                                postings::ROLE_TOP_LEVEL_PROGRAM
                            } else {
                                postings::ROLE_WRITABLE
                            },
                        )
                    })
                    .collect(),
            },
        ];
        let encoded = encode_page(&keys, &spans).unwrap();
        let layout = ValidatedBlockLayout::new(spans.to_vec()).unwrap();

        let compare = |bytes: &[u8]| {
            let materialized = decode_page_with_layout(bytes, 5, 2, &layout);
            let mut streamed = Vec::new();
            let streaming = visit_page_key_with_layout(bytes, 5, 2, &layout, 8, |posting| {
                streamed.push(posting);
                Ok(())
            });
            assert_eq!(
                streaming.is_ok(),
                materialized.is_ok(),
                "decoder acceptance differs for {bytes:02x?}"
            );
            if let Ok(materialized) = materialized {
                let expected = materialized
                    .iter()
                    .find(|key| key.key == 8)
                    .map_or(&[][..], |key| key.postings.as_slice());
                assert_eq!(streamed, expected);
            }
        };

        for length in 0..=encoded.bytes.len() {
            compare(&encoded.bytes[..length]);
        }
        for index in 0..encoded.bytes.len() {
            for bit in 0..8 {
                let mut corrupted = encoded.bytes.clone();
                corrupted[index] ^= 1 << bit;
                compare(&corrupted);
            }
        }
        let mut trailing = encoded.bytes.clone();
        trailing.push(0);
        compare(&trailing);
    }

    #[test]
    fn bitpack_round_trips_a_true_32_bit_local_offset() {
        let span = BlockSpan {
            block_id: 9,
            first_tx_ordinal: 0,
            tx_count: u32::MAX,
        };
        let postings = vec![
            ExactPosting {
                block_id: span.block_id,
                tx_index: 0,
                roles: postings::ROLE_SIGNER,
            },
            ExactPosting {
                block_id: span.block_id,
                tx_index: u32::MAX - 1,
                roles: postings::ROLE_WRITABLE,
            },
        ];
        let encoded = encode_local_bitpack(&postings).unwrap();
        let mut width_cursor = 0;
        assert_eq!(read_uleb128_u32(&encoded, &mut width_cursor).unwrap(), 0);
        assert_eq!(read_uleb128_u32(&encoded, &mut width_cursor).unwrap(), 32);
        let mut cursor = 0;
        let mut decoded = Vec::new();
        decode_local_bitpack(&encoded, &mut cursor, postings.len(), span, &mut decoded).unwrap();
        assert_eq!(cursor, encoded.len());
        assert_eq!(decoded, postings);
    }

    #[test]
    fn sorted_ordinal_cursor_crosses_blocks_and_rejects_nonascending_input() {
        let layout = ValidatedBlockLayout::new(vec![
            BlockSpan {
                block_id: 4,
                first_tx_ordinal: 0,
                tx_count: 3,
            },
            BlockSpan {
                block_id: 9,
                first_tx_ordinal: 3,
                tx_count: 0,
            },
            BlockSpan {
                block_id: 12,
                first_tx_ordinal: 3,
                tx_count: 5,
            },
        ])
        .unwrap();
        let mut cursor = layout.sorted_ordinal_cursor();
        assert_eq!(
            cursor.resolve(1).unwrap(),
            ExactPosting {
                block_id: 4,
                tx_index: 1,
                roles: 0,
            }
        );
        assert_eq!(
            cursor.resolve(3).unwrap(),
            ExactPosting {
                block_id: 12,
                tx_index: 0,
                roles: 0,
            }
        );
        assert_eq!(
            cursor.resolve(7).unwrap(),
            ExactPosting {
                block_id: 12,
                tx_index: 4,
                roles: 0,
            }
        );
        assert!(cursor.resolve(7).is_err());

        let mut cursor = layout.sorted_ordinal_cursor();
        cursor.resolve(5).unwrap();
        assert!(cursor.resolve(2).is_err());
        let mut cursor = layout.sorted_ordinal_cursor();
        assert!(cursor.resolve(8).is_err());
    }

    #[test]
    fn sparse_fixture_keeps_current_ordinal_bytes() {
        let spans = [BlockSpan {
            block_id: 0,
            first_tx_ordinal: 0,
            tx_count: 100,
        }];
        let keys = vec![postings::KeyPostings {
            key: 5,
            postings: vec![posting(0, 0)],
        }];
        let encoded = encode_page(&keys, &spans).unwrap();
        assert_eq!(encoded.stats.current_page_bytes, 3);
        assert_eq!(encoded.stats.adaptive_page_bytes, 4);
        assert_eq!(encoded.stats.ordinal_keys, 1);
        assert_eq!(encoded.stats.block_group_keys, 0);
        assert_eq!(encoded.bytes, vec![0, 1, KEY_CODEC_ORDINAL_ULEB, 0]);
        assert_eq!(
            decode_page(&encoded.bytes, 5, 1, &spans).unwrap(),
            expected(&keys, &spans)
        );

        let noncanonical_block_group =
            vec![0, 1, KEY_CODEC_BLOCK_GROUPS, 1, 0, 1, LOCAL_CODEC_VARINT, 0];
        assert!(decode_page(&noncanonical_block_group, 5, 1, &spans).is_err());
    }

    #[test]
    fn codec_ties_have_one_frozen_order() {
        assert_eq!(choose_key_codec(vec![1], vec![2]).0, KEY_CODEC_ORDINAL_ULEB);
        assert_eq!(
            choose_local_codec(vec![1], vec![2], Some(vec![3])).0,
            LOCAL_CODEC_VARINT
        );
        assert_eq!(
            choose_local_codec(vec![1, 2], vec![3], Some(vec![4])).0,
            LOCAL_CODEC_BITPACK
        );
        assert_eq!(
            choose_local_codec(vec![1, 2], vec![3, 4], Some(vec![5])).0,
            LOCAL_CODEC_BITMAP
        );
    }

    #[test]
    fn encoding_is_deterministic() {
        let spans = [BlockSpan {
            block_id: 0,
            first_tx_ordinal: 0,
            tx_count: 512,
        }];
        let keys = vec![postings::KeyPostings {
            key: 11,
            postings: (0_u64..512)
                .map(|ordinal| posting(ordinal, (ordinal as u8) & postings::ROLE_MASK))
                .collect(),
        }];
        let first = encode_page(&keys, &spans).unwrap();
        for _ in 0..20 {
            assert_eq!(encode_page(&keys, &spans).unwrap(), first);
        }
    }

    #[test]
    fn page_decoder_rejects_truncation_unknown_codecs_and_trailing_bytes() {
        let spans = [BlockSpan {
            block_id: 0,
            first_tx_ordinal: 0,
            tx_count: 100,
        }];
        let keys = vec![postings::KeyPostings {
            key: 5,
            postings: vec![posting(0, 0)],
        }];
        let encoded = encode_page(&keys, &spans).unwrap();
        for length in 0..encoded.bytes.len() {
            assert!(decode_page(&encoded.bytes[..length], 5, 1, &spans).is_err());
        }
        let mut unknown = encoded.bytes.clone();
        unknown[2] = 99;
        assert!(decode_page(&unknown, 5, 1, &spans).is_err());
        let mut trailing = encoded.bytes.clone();
        trailing.push(0);
        assert!(decode_page(&trailing, 5, 1, &spans).is_err());

        let mut too_many = vec![0];
        write_uleb128(
            &mut too_many,
            u64::from(postings::MAX_POSTINGS_PER_PAGE) + 1,
        );
        too_many.push(KEY_CODEC_ORDINAL_ULEB);
        assert!(decode_page(&too_many, 5, 1, &spans).is_err());

        let padded_posting_count = [0x00, 0x81, 0x00, KEY_CODEC_ORDINAL_ULEB, 0x00];
        assert!(decode_page(&padded_posting_count, 5, 1, &spans).is_err());
    }

    #[test]
    fn local_decoders_reject_bounds_padding_and_noncanonical_shapes() {
        let span = BlockSpan {
            block_id: 7,
            first_tx_ordinal: 0,
            tx_count: 10,
        };

        let mut bitmap_cursor = 0;
        let bad_bitmap = [0x01, 0x80, 0x00];
        assert!(
            decode_local_bitmap(&bad_bitmap, &mut bitmap_cursor, 1, span, &mut Vec::new()).is_err()
        );

        let mut count_cursor = 0;
        let bad_count = [0x03, 0x00, 0x00];
        assert!(
            decode_local_bitmap(&bad_count, &mut count_cursor, 1, span, &mut Vec::new()).is_err()
        );

        let mut bitpack_cursor = 0;
        let bad_bitpack_padding = [0x00, 0x01, 0x82, 0x00];
        assert!(
            decode_local_bitpack(
                &bad_bitpack_padding,
                &mut bitpack_cursor,
                2,
                span,
                &mut Vec::new()
            )
            .is_err()
        );

        let mut width_cursor = 0;
        let nonminimal_width = [0x00, 0x02, 0x04, 0x00];
        assert!(
            decode_local_bitpack(
                &nonminimal_width,
                &mut width_cursor,
                2,
                span,
                &mut Vec::new()
            )
            .is_err()
        );

        let mut padded_width_cursor = 0;
        let padded_width = [0x00, 0x81, 0x00, 0x00, 0x00];
        assert!(
            decode_local_bitpack(
                &padded_width,
                &mut padded_width_cursor,
                1,
                span,
                &mut Vec::new()
            )
            .is_err()
        );

        let mut varint_cursor = 0;
        let mut outside = Vec::new();
        write_uleb128(&mut outside, u64::from(span.tx_count) << 4);
        assert!(
            decode_local_varint(&outside, &mut varint_cursor, 1, span, &mut Vec::new()).is_err()
        );

        let mut role_cursor = 0;
        let bad_role_padding = [0x00, 0x00, 0xf0];
        assert!(
            decode_local_bitpack(
                &bad_role_padding,
                &mut role_cursor,
                1,
                span,
                &mut Vec::new()
            )
            .is_err()
        );
    }

    #[test]
    fn runner_permit_covers_each_live_phase_and_releases_exactly() {
        let entry = postings::PageDirectoryEntry {
            first_key: 1,
            last_key: postings::MAX_KEYS_PER_PAGE,
            offset: 80,
            stored_len: 4 << 20,
            decoded_len: 8 << 20,
            key_count: postings::MAX_KEYS_PER_PAGE,
            flags: 0,
        };
        let stored = entry.stored_len as usize;
        let decoded = entry.decoded_len as usize;
        let adaptive = decoded + entry.key_count as usize;
        let key_heap = entry.key_count as usize * std::mem::size_of::<postings::KeyPostings>()
            + postings::MAX_POSTINGS_PER_PAGE as usize * std::mem::size_of::<postings::Posting>();
        let expected = (stored + 2 * decoded + key_heap + MAX_ZSTD_DECOMPRESSION_FIXED_BYTES)
            .max(key_heap + adaptive + 2 * decoded + MAX_BLOCK_CODEC_SCRATCH_BYTES)
            .max(
                adaptive
                    + zstd::zstd_safe::compress_bound(adaptive)
                    + MAX_ZSTD_COMPRESSION_WORKSPACE_BYTES,
            );
        let permit_bytes = reverse_page_permit_bytes(entry).unwrap();
        assert_eq!(permit_bytes, expected);
        assert!(permit_bytes <= LIVE_BYTE_BUDGET);

        let budget = Arc::new(LiveByteBudget::new(LIVE_BYTE_BUDGET));
        let permit = budget.acquire(permit_bytes).unwrap();
        drop(permit);
        assert_eq!(
            budget.report().unwrap(),
            LiveBudgetReport {
                limit_bytes: LIVE_BYTE_BUDGET as u64,
                peak_permit_bytes: permit_bytes as u64,
                peak_live_items: 1,
            }
        );

        let outside_adaptive_guard = postings::PageDirectoryEntry {
            decoded_len: postings::MAX_PAGE_DECODED_BYTES,
            ..entry
        };
        assert!(reverse_page_permit_bytes(outside_adaptive_guard).is_err());
    }

    #[test]
    fn layouts_and_source_postings_are_strict() {
        assert!(
            BlockLayout::new(&[BlockSpan {
                block_id: 1,
                first_tx_ordinal: 1,
                tx_count: 1,
            }])
            .is_err()
        );
        assert!(
            BlockLayout::new(&[
                BlockSpan {
                    block_id: 2,
                    first_tx_ordinal: 0,
                    tx_count: 1,
                },
                BlockSpan {
                    block_id: 1,
                    first_tx_ordinal: 1,
                    tx_count: 1,
                },
            ])
            .is_err()
        );
        let spans = [BlockSpan {
            block_id: 0,
            first_tx_ordinal: 0,
            tx_count: 2,
        }];
        let repeated = vec![postings::KeyPostings {
            key: 1,
            postings: vec![posting(1, 0), posting(1, postings::ROLE_SIGNER)],
        }];
        assert!(encode_page(&repeated, &spans).is_err());
        let outside = vec![postings::KeyPostings {
            key: 1,
            postings: vec![posting(2, 0)],
        }];
        assert!(encode_page(&outside, &spans).is_err());
    }
}
