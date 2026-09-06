//! Measurement-only block-group role-summary wire canary.
//!
//! This module does not change the adaptive V3 builder or reader. It tests a
//! possible block-first reverse-index layout. Each block header carries an
//! exact histogram for the 15 nonzero four-bit role masks. After one full
//! admission pass proves that the histogram agrees with the local posting
//! payload, candidate queries can count matching postings per block without
//! decoding that payload.

use anyhow::{Context, Result, ensure};
use blockzilla_index_archive_format::{
    indexes::accounts as postings,
    varint::{read_uleb128, read_uleb128_u32, write_uleb128},
};

use super::block_group_measurement::ExactPosting;

/// Distinct experimental wire marker. It is not an active archive object.
pub const MAGIC: [u8; 8] = *b"BZV3RS01";
pub const FORMAT_VERSION: u16 = 1;
pub const HEADER_LEN: usize = 20;
pub const LOCAL_CODEC_ORDINAL_GAP_ULEB: u8 = 0;

const HISTOGRAM_MASK: u16 = 0x7fff;
const HISTOGRAM_PADDING_MASK: u16 = !HISTOGRAM_MASK;

/// One block that has at least one posting matching the requested role bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoleMatchedBlock {
    pub block_id: u32,
    pub matching_postings: u32,
}

/// Work completed by a header-only candidate pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CandidateVisitSummary {
    pub blocks: u32,
    pub postings: u32,
    pub matching_blocks: u32,
    pub matching_postings: u64,
    pub skipped_local_payload_bytes: u64,
}

/// Size comparison against the former block-group body with the same local
/// ULEB payload. The baseline has a ULEB group count and no fixed outer header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteDeltaReport {
    /// Each page represents one account, so this is the exact number of
    /// distinct `(account, block)` pairs in the fixture.
    pub distinct_account_block_groups: u32,
    pub postings: u32,
    pub local_payload_bytes: usize,
    pub baseline_body_bytes: usize,
    pub baseline_encoded_overhead_bytes: usize,
    pub role_summary_body_bytes: usize,
    pub role_summary_total_bytes: usize,
    pub role_summary_encoded_overhead_bytes: usize,
    pub body_delta_bytes: i64,
    pub total_delta_bytes: i64,
}

/// Bytes that completed the full semantic admission pass.
///
/// The borrowed bytes cannot change while this value exists. Repeated role
/// queries can therefore trust the proved headers and skip all local payloads.
#[derive(Debug, Clone, Copy)]
pub struct ValidatedRoleSummaryPage<'a> {
    bytes: &'a [u8],
    groups: u32,
    postings: u32,
}

impl ValidatedRoleSummaryPage<'_> {
    pub const fn groups(&self) -> u32 {
        self.groups
    }

    pub const fn postings(&self) -> u32 {
        self.postings
    }

    pub const fn as_bytes(&self) -> &[u8] {
        self.bytes
    }

    /// Read block headers and skip each local transaction payload.
    pub fn visit_matching_blocks(
        &self,
        required_roles: u8,
        mut visit: impl FnMut(RoleMatchedBlock) -> Result<()>,
    ) -> Result<CandidateVisitSummary> {
        ensure_required_roles(required_roles)?;
        visit_admitted_headers(self, required_roles, &mut visit)
    }
}

/// Encode one account's exact postings in block order.
pub fn encode_page(input: &[ExactPosting]) -> Result<Vec<u8>> {
    ensure!(
        input.len() <= postings::MAX_POSTINGS_PER_PAGE as usize,
        "role-summary posting count exceeds guard"
    );
    let posting_count =
        u32::try_from(input.len()).context("role-summary posting count exceeds u32")?;
    let group_count = group_count(input)?;
    let mut output = Vec::new();
    output
        .try_reserve(HEADER_LEN)
        .context("reserve role-summary header")?;
    write_header(&mut output, group_count, posting_count);

    let mut start = 0_usize;
    let mut previous_block = None;
    while start < input.len() {
        let block_id = input[start].block_id;
        let end = input[start..]
            .iter()
            .position(|posting| posting.block_id != block_id)
            .map_or(input.len(), |offset| start + offset);
        let group = &input[start..end];
        let block_gap = match previous_block {
            None => block_id,
            Some(previous) => block_id
                .checked_sub(previous)
                .filter(|gap| *gap != 0)
                .context("role-summary block IDs do not strictly ascend")?,
        };
        let (union_roles, histogram, histogram_bits) = summarize_group(group)?;
        let payload = encode_local_payload(group)?;

        write_uleb128(&mut output, u64::from(block_gap));
        write_uleb128(&mut output, u64::try_from(group.len())?);
        output.push(union_roles);
        output.extend_from_slice(&histogram_bits.to_le_bytes());
        for role_mask in 1_u8..=postings::ROLE_MASK {
            let count = histogram[usize::from(role_mask)];
            if count != 0 {
                write_uleb128(&mut output, u64::from(count));
            }
        }
        write_uleb128(&mut output, u64::try_from(payload.len())?);
        output.push(LOCAL_CODEC_ORDINAL_GAP_ULEB);
        output.extend_from_slice(&payload);
        ensure!(
            output.len() <= postings::MAX_PAGE_DECODED_BYTES as usize,
            "role-summary page exceeds byte guard"
        );
        previous_block = Some(block_id);
        start = end;
    }
    Ok(output)
}

/// Complete the exact semantic admission pass without retaining postings.
pub fn validate_page(input: &[u8]) -> Result<ValidatedRoleSummaryPage<'_>> {
    let parsed = parse_exact(input, &mut |_| Ok(()))?;
    Ok(ValidatedRoleSummaryPage {
        bytes: input,
        groups: parsed.groups,
        postings: parsed.postings,
    })
}

/// Decode all exact postings and apply the same full admission checks.
pub fn decode_page(input: &[u8]) -> Result<Vec<ExactPosting>> {
    let header = read_header(input)?;
    let capacity =
        usize::try_from(header.postings).context("role-summary capacity exceeds usize")?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(capacity)
        .context("reserve decoded role-summary postings")?;
    parse_exact(input, &mut |posting| {
        output.push(posting);
        Ok(())
    })?;
    Ok(output)
}

/// Measure the role-summary overhead on the supplied exact fixture.
pub fn byte_delta_report(input: &[ExactPosting]) -> Result<ByteDeltaReport> {
    let encoded = encode_page(input)?;
    let group_count = group_count(input)?;
    let mut baseline_body_bytes = uleb128_len(u64::from(group_count));
    let mut local_payload_bytes = 0_usize;
    let mut start = 0_usize;
    let mut previous_block = None;
    while start < input.len() {
        let block_id = input[start].block_id;
        let end = input[start..]
            .iter()
            .position(|posting| posting.block_id != block_id)
            .map_or(input.len(), |offset| start + offset);
        let group = &input[start..end];
        let gap = match previous_block {
            None => block_id,
            Some(previous) => block_id
                .checked_sub(previous)
                .filter(|gap| *gap != 0)
                .context("role-summary block IDs do not strictly ascend")?,
        };
        let payload = encode_local_payload(group)?;
        local_payload_bytes = local_payload_bytes
            .checked_add(payload.len())
            .context("role-summary payload measurement overflow")?;
        baseline_body_bytes = baseline_body_bytes
            .checked_add(uleb128_len(u64::from(gap)))
            .and_then(|total| total.checked_add(uleb128_len(group.len() as u64)))
            .and_then(|total| total.checked_add(1))
            .and_then(|total| total.checked_add(payload.len()))
            .context("role-summary baseline measurement overflow")?;
        previous_block = Some(block_id);
        start = end;
    }
    let role_summary_body_bytes = encoded
        .len()
        .checked_sub(HEADER_LEN)
        .context("role-summary body measurement underflow")?;
    Ok(ByteDeltaReport {
        distinct_account_block_groups: group_count,
        postings: u32::try_from(input.len())?,
        local_payload_bytes,
        baseline_body_bytes,
        baseline_encoded_overhead_bytes: baseline_body_bytes
            .checked_sub(local_payload_bytes)
            .context("role-summary baseline overhead underflow")?,
        role_summary_body_bytes,
        role_summary_total_bytes: encoded.len(),
        role_summary_encoded_overhead_bytes: encoded
            .len()
            .checked_sub(local_payload_bytes)
            .context("role-summary encoded overhead underflow")?,
        body_delta_bytes: signed_delta(role_summary_body_bytes, baseline_body_bytes)?,
        total_delta_bytes: signed_delta(encoded.len(), baseline_body_bytes)?,
    })
}

#[derive(Debug, Clone, Copy)]
struct Header {
    groups: u32,
    postings: u32,
}

fn write_header(output: &mut Vec<u8>, groups: u32, postings: u32) {
    output.extend_from_slice(&MAGIC);
    output.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    output.extend_from_slice(&(HEADER_LEN as u16).to_le_bytes());
    output.extend_from_slice(&groups.to_le_bytes());
    output.extend_from_slice(&postings.to_le_bytes());
}

fn read_header(input: &[u8]) -> Result<Header> {
    ensure!(
        input.len() <= postings::MAX_PAGE_DECODED_BYTES as usize,
        "role-summary page exceeds byte guard"
    );
    let header = input
        .get(..HEADER_LEN)
        .context("role-summary header is truncated")?;
    ensure!(header[0..8] == MAGIC, "role-summary magic differs");
    let version = u16::from_le_bytes(header[8..10].try_into().expect("two bytes"));
    ensure!(
        version == FORMAT_VERSION,
        "unknown role-summary format version {version}"
    );
    let header_len = u16::from_le_bytes(header[10..12].try_into().expect("two bytes"));
    ensure!(
        usize::from(header_len) == HEADER_LEN,
        "role-summary header length differs"
    );
    let groups = u32::from_le_bytes(header[12..16].try_into().expect("four bytes"));
    let posting_count = u32::from_le_bytes(header[16..20].try_into().expect("four bytes"));
    ensure!(
        posting_count <= postings::MAX_POSTINGS_PER_PAGE,
        "role-summary posting count exceeds guard"
    );
    ensure!(
        groups <= posting_count,
        "role-summary group count exceeds posting count"
    );
    ensure!(
        (groups == 0) == (posting_count == 0),
        "role-summary empty group and posting counts differ"
    );
    Ok(Header {
        groups,
        postings: posting_count,
    })
}

fn group_count(input: &[ExactPosting]) -> Result<u32> {
    if input.is_empty() {
        return Ok(0);
    }
    let mut groups = 1_u32;
    let mut previous = input[0];
    validate_posting(previous)?;
    for &posting in &input[1..] {
        validate_posting(posting)?;
        let previous_position = (previous.block_id, previous.tx_index);
        let position = (posting.block_id, posting.tx_index);
        ensure!(
            position > previous_position,
            "role-summary postings do not strictly ascend"
        );
        if posting.block_id != previous.block_id {
            groups = groups
                .checked_add(1)
                .context("role-summary group count overflow")?;
        }
        previous = posting;
    }
    Ok(groups)
}

fn validate_posting(posting: ExactPosting) -> Result<()> {
    ensure!(
        posting.roles & !postings::ROLE_MASK == 0,
        "role-summary posting has unknown role bits"
    );
    Ok(())
}

fn summarize_group(group: &[ExactPosting]) -> Result<(u8, [u32; 16], u16)> {
    ensure!(!group.is_empty(), "role-summary block group is empty");
    let block_id = group[0].block_id;
    let mut previous_tx = None;
    let mut union_roles = 0_u8;
    let mut histogram = [0_u32; 16];
    for &posting in group {
        validate_posting(posting)?;
        ensure!(
            posting.block_id == block_id,
            "role-summary block group contains another block"
        );
        if let Some(previous) = previous_tx {
            ensure!(
                posting.tx_index > previous,
                "role-summary local transaction indexes do not strictly ascend"
            );
        }
        histogram[usize::from(posting.roles)] = histogram[usize::from(posting.roles)]
            .checked_add(1)
            .context("role-summary histogram count overflow")?;
        union_roles |= posting.roles;
        previous_tx = Some(posting.tx_index);
    }
    let mut histogram_bits = 0_u16;
    for role_mask in 1_u8..=postings::ROLE_MASK {
        if histogram[usize::from(role_mask)] != 0 {
            histogram_bits |= 1_u16 << (role_mask - 1);
        }
    }
    Ok((union_roles, histogram, histogram_bits))
}

fn encode_local_payload(group: &[ExactPosting]) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut previous = None;
    for &posting in group {
        validate_posting(posting)?;
        let gap = match previous {
            None => posting.tx_index,
            Some(previous) => posting
                .tx_index
                .checked_sub(previous)
                .filter(|gap| *gap != 0)
                .context("role-summary local transaction indexes do not strictly ascend")?,
        };
        write_uleb128(
            &mut output,
            (u64::from(gap) << 4) | u64::from(posting.roles),
        );
        previous = Some(posting.tx_index);
    }
    Ok(output)
}

#[derive(Debug, Clone, Copy)]
struct ParsedPage {
    groups: u32,
    postings: u32,
}

fn parse_exact(
    input: &[u8],
    visit: &mut dyn FnMut(ExactPosting) -> Result<()>,
) -> Result<ParsedPage> {
    let header = read_header(input)?;
    let mut cursor = HEADER_LEN;
    let mut block_id = 0_u32;
    let mut decoded_postings = 0_u32;
    for group_index in 0..header.groups {
        let group = read_group_header(
            input,
            &mut cursor,
            group_index,
            block_id,
            header.postings - decoded_postings,
        )?;
        block_id = group.block_id;
        let payload = take(
            input,
            &mut cursor,
            usize::try_from(group.payload_len)?,
            "role-summary local payload",
        )?;
        let actual = decode_local_payload(payload, block_id, group.posting_count, visit)?;
        ensure!(
            actual.union_roles == group.union_roles,
            "role-summary union mask differs from local payload"
        );
        ensure!(
            actual.histogram == group.histogram,
            "role-summary histogram differs from local payload"
        );
        decoded_postings = decoded_postings
            .checked_add(group.posting_count)
            .context("role-summary decoded posting count overflow")?;
    }
    ensure!(
        decoded_postings == header.postings,
        "role-summary groups do not cover declared postings"
    );
    ensure!(
        cursor == input.len(),
        "role-summary page has trailing bytes"
    );
    Ok(ParsedPage {
        groups: header.groups,
        postings: header.postings,
    })
}

#[derive(Debug, Clone, Copy)]
struct GroupHeader {
    block_id: u32,
    posting_count: u32,
    union_roles: u8,
    histogram: [u32; 16],
    payload_len: u32,
}

fn read_group_header(
    input: &[u8],
    cursor: &mut usize,
    group_index: u32,
    previous_block: u32,
    remaining_postings: u32,
) -> Result<GroupHeader> {
    let gap = read_uleb128_u32(input, cursor).context("read role-summary block gap")?;
    let block_id = if group_index == 0 {
        gap
    } else {
        ensure!(gap != 0, "role-summary block groups repeat");
        previous_block
            .checked_add(gap)
            .context("role-summary block ID overflow")?
    };
    let posting_count =
        read_uleb128_u32(input, cursor).context("read role-summary local posting count")?;
    ensure!(
        posting_count != 0 && posting_count <= remaining_postings,
        "role-summary local posting count exceeds remaining postings"
    );
    let union_roles = read_byte(input, cursor, "role-summary union role mask")?;
    ensure!(
        union_roles & !postings::ROLE_MASK == 0,
        "role-summary union has unknown role bits"
    );
    let histogram_bits = read_u16(input, cursor, "role-summary histogram bitmap")?;
    ensure!(
        histogram_bits & HISTOGRAM_PADDING_MASK == 0,
        "role-summary histogram padding bit is nonzero"
    );
    let mut histogram = [0_u32; 16];
    let mut histogram_total = 0_u32;
    let mut histogram_union = 0_u8;
    for role_mask in 1_u8..=postings::ROLE_MASK {
        if histogram_bits & (1_u16 << (role_mask - 1)) == 0 {
            continue;
        }
        let count = read_uleb128_u32(input, cursor)
            .with_context(|| format!("read role-summary mask {role_mask:#x} count"))?;
        ensure!(count != 0, "role-summary histogram stores a zero count");
        histogram_total = histogram_total
            .checked_add(count)
            .context("role-summary histogram total overflow")?;
        ensure!(
            histogram_total <= posting_count,
            "role-summary histogram exceeds local posting count"
        );
        histogram[usize::from(role_mask)] = count;
        histogram_union |= role_mask;
    }
    ensure!(
        histogram_union == union_roles,
        "role-summary histogram masks differ from union mask"
    );
    histogram[0] = posting_count
        .checked_sub(histogram_total)
        .context("role-summary zero-role count underflow")?;
    let payload_len =
        read_uleb128_u32(input, cursor).context("read role-summary local payload length")?;
    ensure!(
        payload_len <= postings::MAX_PAGE_DECODED_BYTES,
        "role-summary local payload length exceeds guard"
    );
    let codec = read_byte(input, cursor, "role-summary local codec")?;
    ensure!(
        codec == LOCAL_CODEC_ORDINAL_GAP_ULEB,
        "unknown role-summary local codec {codec}"
    );
    Ok(GroupHeader {
        block_id,
        posting_count,
        union_roles,
        histogram,
        payload_len,
    })
}

#[derive(Debug, Clone, Copy)]
struct ActualSummary {
    union_roles: u8,
    histogram: [u32; 16],
}

fn decode_local_payload(
    payload: &[u8],
    block_id: u32,
    posting_count: u32,
    visit: &mut dyn FnMut(ExactPosting) -> Result<()>,
) -> Result<ActualSummary> {
    let mut cursor = 0_usize;
    let mut tx_index = 0_u32;
    let mut union_roles = 0_u8;
    let mut histogram = [0_u32; 16];
    for index in 0..posting_count {
        let packed = read_uleb128(payload, &mut cursor)
            .context("read role-summary local transaction posting")?;
        let gap =
            u32::try_from(packed >> 4).context("role-summary local transaction gap exceeds u32")?;
        if index == 0 {
            tx_index = gap;
        } else {
            ensure!(gap != 0, "role-summary local transaction indexes repeat");
            tx_index = tx_index
                .checked_add(gap)
                .context("role-summary local transaction index overflow")?;
        }
        let roles = (packed & u64::from(postings::ROLE_MASK)) as u8;
        histogram[usize::from(roles)] = histogram[usize::from(roles)]
            .checked_add(1)
            .context("role-summary decoded histogram overflow")?;
        union_roles |= roles;
        visit(ExactPosting {
            block_id,
            tx_index,
            roles,
        })?;
    }
    ensure!(
        cursor == payload.len(),
        "role-summary local payload has trailing bytes"
    );
    Ok(ActualSummary {
        union_roles,
        histogram,
    })
}

fn visit_admitted_headers(
    page: &ValidatedRoleSummaryPage<'_>,
    required_roles: u8,
    visit: &mut dyn FnMut(RoleMatchedBlock) -> Result<()>,
) -> Result<CandidateVisitSummary> {
    let header = read_header(page.bytes)?;
    ensure!(
        header.groups == page.groups && header.postings == page.postings,
        "role-summary admitted header changed"
    );
    let mut summary = CandidateVisitSummary {
        blocks: header.groups,
        postings: header.postings,
        ..CandidateVisitSummary::default()
    };
    let mut cursor = HEADER_LEN;
    let mut block_id = 0_u32;
    let mut covered_postings = 0_u32;
    for group_index in 0..header.groups {
        let group = read_group_header(
            page.bytes,
            &mut cursor,
            group_index,
            block_id,
            header.postings - covered_postings,
        )?;
        block_id = group.block_id;
        let payload_len = usize::try_from(group.payload_len)?;
        take(
            page.bytes,
            &mut cursor,
            payload_len,
            "role-summary skipped local payload",
        )?;
        summary.skipped_local_payload_bytes = summary
            .skipped_local_payload_bytes
            .checked_add(u64::from(group.payload_len))
            .context("role-summary skipped payload count overflow")?;
        covered_postings = covered_postings
            .checked_add(group.posting_count)
            .context("role-summary covered posting count overflow")?;
        if group.union_roles & required_roles == 0 {
            continue;
        }
        let matching = (1_u8..=postings::ROLE_MASK).try_fold(0_u32, |total, role_mask| {
            if role_mask & required_roles == 0 {
                return Ok(total);
            }
            total
                .checked_add(group.histogram[usize::from(role_mask)])
                .context("role-summary matching block count overflow")
        })?;
        ensure!(
            matching != 0,
            "role-summary union produced no histogram match"
        );
        visit(RoleMatchedBlock {
            block_id,
            matching_postings: matching,
        })?;
        summary.matching_blocks = summary
            .matching_blocks
            .checked_add(1)
            .context("role-summary matching block total overflow")?;
        summary.matching_postings = summary
            .matching_postings
            .checked_add(u64::from(matching))
            .context("role-summary matching posting total overflow")?;
    }
    ensure!(
        covered_postings == header.postings,
        "role-summary header pass does not cover declared postings"
    );
    ensure!(
        cursor == page.bytes.len(),
        "role-summary header pass has trailing bytes"
    );
    Ok(summary)
}

fn ensure_required_roles(required_roles: u8) -> Result<()> {
    ensure!(
        required_roles != 0 && required_roles & !postings::ROLE_MASK == 0,
        "role-summary required role mask is invalid"
    );
    Ok(())
}

fn read_byte(input: &[u8], cursor: &mut usize, label: &str) -> Result<u8> {
    let byte = *input
        .get(*cursor)
        .with_context(|| format!("{label} is truncated"))?;
    *cursor += 1;
    Ok(byte)
}

fn read_u16(input: &[u8], cursor: &mut usize, label: &str) -> Result<u16> {
    let bytes = take(input, cursor, 2, label)?;
    Ok(u16::from_le_bytes(bytes.try_into().expect("two bytes")))
}

fn take<'a>(input: &'a [u8], cursor: &mut usize, length: usize, label: &str) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(length)
        .with_context(|| format!("{label} range overflows"))?;
    let value = input
        .get(*cursor..end)
        .with_context(|| format!("{label} is truncated"))?;
    *cursor = end;
    Ok(value)
}

fn uleb128_len(mut value: u64) -> usize {
    let mut length = 1_usize;
    while value >= 0x80 {
        value >>= 7;
        length += 1;
    }
    length
}

fn signed_delta(left: usize, right: usize) -> Result<i64> {
    let left = i64::try_from(left).context("role-summary left byte count exceeds i64")?;
    let right = i64::try_from(right).context("role-summary right byte count exceeds i64")?;
    left.checked_sub(right)
        .context("role-summary byte delta overflow")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn posting(block_id: u32, tx_index: u32, roles: u8) -> ExactPosting {
        ExactPosting {
            block_id,
            tx_index,
            roles,
        }
    }

    fn fixture() -> Vec<ExactPosting> {
        vec![
            posting(7, 0, 0),
            posting(7, 2, postings::ROLE_SIGNER),
            posting(7, 9, postings::ROLE_SIGNER | postings::ROLE_WRITABLE),
            posting(11, 1, postings::ROLE_WRITABLE),
            posting(
                11,
                8,
                postings::ROLE_TOP_LEVEL_PROGRAM | postings::ROLE_CPI_PROGRAM,
            ),
            posting(300, 150, postings::ROLE_CPI_PROGRAM),
        ]
    }

    #[derive(Debug)]
    struct FirstGroupOffsets {
        block_gap: usize,
        union_roles: usize,
        histogram_bitmap: usize,
        first_histogram_count: usize,
        payload_len: usize,
        codec: usize,
        payload: usize,
    }

    fn first_group_offsets(input: &[u8]) -> FirstGroupOffsets {
        let mut cursor = HEADER_LEN;
        let block_gap = cursor;
        read_uleb128_u32(input, &mut cursor).unwrap();
        read_uleb128_u32(input, &mut cursor).unwrap();
        let union_roles = cursor;
        cursor += 1;
        let histogram_bitmap = cursor;
        let bits = u16::from_le_bytes(input[cursor..cursor + 2].try_into().unwrap());
        cursor += 2;
        let first_histogram_count = cursor;
        for role_mask in 1_u8..=postings::ROLE_MASK {
            if bits & (1_u16 << (role_mask - 1)) != 0 {
                read_uleb128_u32(input, &mut cursor).unwrap();
            }
        }
        let payload_len = cursor;
        read_uleb128_u32(input, &mut cursor).unwrap();
        let codec = cursor;
        cursor += 1;
        FirstGroupOffsets {
            block_gap,
            union_roles,
            histogram_bitmap,
            first_histogram_count,
            payload_len,
            codec,
            payload: cursor,
        }
    }

    #[test]
    fn empty_page_is_exact_and_has_no_candidates() {
        let encoded = encode_page(&[]).unwrap();
        assert_eq!(encoded.len(), HEADER_LEN);
        let admitted = validate_page(&encoded).unwrap();
        assert_eq!(admitted.groups(), 0);
        assert_eq!(admitted.postings(), 0);
        assert!(decode_page(&encoded).unwrap().is_empty());
        let mut blocks = Vec::new();
        let summary = admitted
            .visit_matching_blocks(postings::ROLE_SIGNER, |block| {
                blocks.push(block);
                Ok(())
            })
            .unwrap();
        assert_eq!(summary, CandidateVisitSummary::default());
        assert!(blocks.is_empty());
    }

    #[test]
    fn one_and_multiple_blocks_round_trip_exactly() {
        for expected in [
            vec![posting(0, 0, 0)],
            vec![
                posting(0, 0, postings::ROLE_SIGNER),
                posting(0, 3, postings::ROLE_WRITABLE),
                posting(5, 1, postings::ROLE_CPI_PROGRAM),
            ],
            fixture(),
        ] {
            let encoded = encode_page(&expected).unwrap();
            let admitted = validate_page(&encoded).unwrap();
            assert_eq!(admitted.postings(), expected.len() as u32);
            assert_eq!(decode_page(&encoded).unwrap(), expected);
        }
    }

    #[test]
    fn overlapping_role_masks_give_exact_block_counts() {
        let input = fixture();
        let encoded = encode_page(&input).unwrap();
        let admitted = validate_page(&encoded).unwrap();
        let required = postings::ROLE_SIGNER | postings::ROLE_CPI_PROGRAM;
        let mut blocks = Vec::new();
        let summary = admitted
            .visit_matching_blocks(required, |block| {
                blocks.push(block);
                Ok(())
            })
            .unwrap();
        assert_eq!(
            blocks,
            vec![
                RoleMatchedBlock {
                    block_id: 7,
                    matching_postings: 2,
                },
                RoleMatchedBlock {
                    block_id: 11,
                    matching_postings: 1,
                },
                RoleMatchedBlock {
                    block_id: 300,
                    matching_postings: 1,
                },
            ]
        );
        assert_eq!(summary.matching_blocks, 3);
        assert_eq!(summary.matching_postings, 4);
        assert!(summary.skipped_local_payload_bytes > 0);
    }

    #[test]
    fn absent_role_skips_every_block() {
        let input = vec![
            posting(1, 0, 0),
            posting(1, 2, postings::ROLE_SIGNER),
            posting(4, 1, postings::ROLE_WRITABLE),
        ];
        let encoded = encode_page(&input).unwrap();
        let admitted = validate_page(&encoded).unwrap();
        let mut called = false;
        let summary = admitted
            .visit_matching_blocks(postings::ROLE_CPI_PROGRAM, |_| {
                called = true;
                Ok(())
            })
            .unwrap();
        assert!(!called);
        assert_eq!(summary.matching_blocks, 0);
        assert_eq!(summary.matching_postings, 0);
        assert!(summary.skipped_local_payload_bytes > 0);
    }

    #[test]
    fn every_required_mask_matches_full_posting_iteration() {
        let input = fixture();
        let encoded = encode_page(&input).unwrap();
        let decoded = decode_page(&encoded).unwrap();
        let admitted = validate_page(&encoded).unwrap();
        for required in 1_u8..=postings::ROLE_MASK {
            let mut expected = Vec::<RoleMatchedBlock>::new();
            for posting in decoded
                .iter()
                .filter(|posting| posting.roles & required != 0)
            {
                if let Some(last) = expected.last_mut()
                    && last.block_id == posting.block_id
                {
                    last.matching_postings += 1;
                } else {
                    expected.push(RoleMatchedBlock {
                        block_id: posting.block_id,
                        matching_postings: 1,
                    });
                }
            }
            let mut actual = Vec::new();
            let summary = admitted
                .visit_matching_blocks(required, |block| {
                    actual.push(block);
                    Ok(())
                })
                .unwrap();
            assert_eq!(actual, expected, "required role mask {required:#x}");
            assert_eq!(
                summary.matching_postings,
                expected
                    .iter()
                    .map(|block| u64::from(block.matching_postings))
                    .sum::<u64>()
            );
        }
    }

    #[test]
    fn encoding_is_deterministic() {
        let input = fixture();
        let first = encode_page(&input).unwrap();
        let second = encode_page(&input).unwrap();
        assert_eq!(first, second);
        assert_eq!(validate_page(&first).unwrap().as_bytes(), first);
    }

    #[test]
    fn every_truncation_is_rejected() {
        let encoded = encode_page(&fixture()).unwrap();
        for length in 0..encoded.len() {
            assert!(
                validate_page(&encoded[..length]).is_err(),
                "accepted truncated length {length}"
            );
        }
    }

    #[test]
    fn corrupt_fixed_header_and_unknown_version_are_rejected() {
        let encoded = encode_page(&fixture()).unwrap();
        for mutation in [0_usize, 10, 12, 16] {
            let mut corrupt = encoded.clone();
            corrupt[mutation] ^= 1;
            assert!(validate_page(&corrupt).is_err(), "header byte {mutation}");
        }
        let mut unknown_version = encoded.clone();
        unknown_version[8..10].copy_from_slice(&2_u16.to_le_bytes());
        assert!(validate_page(&unknown_version).is_err());
    }

    #[test]
    fn corrupt_histogram_and_padding_are_rejected() {
        let encoded = encode_page(&fixture()).unwrap();
        let offsets = first_group_offsets(&encoded);

        let mut union = encoded.clone();
        union[offsets.union_roles] = 0;
        assert!(validate_page(&union).is_err());

        let mut histogram = encoded.clone();
        histogram[offsets.first_histogram_count] += 1;
        assert!(validate_page(&histogram).is_err());

        let mut padding = encoded.clone();
        padding[offsets.histogram_bitmap + 1] |= 0x80;
        assert!(validate_page(&padding).is_err());
    }

    #[test]
    fn corrupt_payload_length_codec_payload_and_trailing_bytes_are_rejected() {
        let encoded = encode_page(&fixture()).unwrap();
        let offsets = first_group_offsets(&encoded);

        let mut length = encoded.clone();
        length[offsets.payload_len] += 1;
        assert!(validate_page(&length).is_err());

        let mut codec = encoded.clone();
        codec[offsets.codec] = 9;
        assert!(validate_page(&codec).is_err());

        let mut payload = encoded.clone();
        payload[offsets.payload] ^= postings::ROLE_SIGNER;
        assert!(validate_page(&payload).is_err());

        let mut trailing = encoded;
        trailing.push(0);
        assert!(validate_page(&trailing).is_err());
    }

    #[test]
    fn noncanonical_uleb_and_invalid_queries_are_rejected() {
        let encoded = encode_page(&fixture()).unwrap();
        let offsets = first_group_offsets(&encoded);
        assert_eq!(encoded[offsets.block_gap], 7);
        let mut padded = encoded.clone();
        padded.splice(offsets.block_gap..=offsets.block_gap, [0x87, 0x00]);
        assert!(validate_page(&padded).is_err());

        let admitted = validate_page(&encoded).unwrap();
        assert!(admitted.visit_matching_blocks(0, |_| Ok(())).is_err());
        assert!(admitted.visit_matching_blocks(0x10, |_| Ok(())).is_err());
    }

    #[test]
    fn ordering_roles_counts_and_bounds_are_rejected() {
        assert!(
            encode_page(&[
                posting(1, 2, postings::ROLE_SIGNER),
                posting(1, 2, postings::ROLE_WRITABLE),
            ])
            .is_err()
        );
        assert!(encode_page(&[posting(1, 0, 0x10)]).is_err());

        let mut too_many = encode_page(&[]).unwrap();
        too_many[12..16].copy_from_slice(&1_u32.to_le_bytes());
        too_many[16..20].copy_from_slice(&(postings::MAX_POSTINGS_PER_PAGE + 1).to_le_bytes());
        assert!(validate_page(&too_many).is_err());
    }

    #[test]
    fn byte_delta_report_is_exact_on_fixtures() {
        for input in [
            vec![posting(0, 0, 0)],
            vec![
                posting(4, 1, postings::ROLE_SIGNER),
                posting(4, 9, postings::ROLE_WRITABLE),
            ],
            fixture(),
        ] {
            let report = byte_delta_report(&input).unwrap();
            assert_eq!(
                report.role_summary_total_bytes,
                HEADER_LEN + report.role_summary_body_bytes
            );
            assert_eq!(
                report.body_delta_bytes,
                report.role_summary_body_bytes as i64 - report.baseline_body_bytes as i64
            );
            assert_eq!(
                report.total_delta_bytes,
                report.role_summary_total_bytes as i64 - report.baseline_body_bytes as i64
            );
            assert!(report.local_payload_bytes <= report.baseline_body_bytes);
            assert_eq!(
                report.baseline_encoded_overhead_bytes,
                report.baseline_body_bytes - report.local_payload_bytes
            );
            assert_eq!(
                report.role_summary_encoded_overhead_bytes,
                report.role_summary_total_bytes - report.local_payload_bytes
            );
            eprintln!("role-summary byte delta: {report:?}");
        }
    }
}
