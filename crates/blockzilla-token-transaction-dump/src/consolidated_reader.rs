//! Reusable readers for the immutable schema-3 token-transaction stream.
//!
//! A frame owns no decoded transaction payload. [`BorrowedDumpRecord`] borrows
//! the reader's one reusable payload buffer, so callers must finish with one
//! frame before they request the next one.

use std::{
    fs::File,
    io::{BufReader, Read},
    path::Path,
};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_archive_v2::{ArchiveV2WireMetadataErrorSchema, validate_archive_v2_metadata_error_prefix_for_selected_schema};
use blockzilla_primitives::{WINCODE_LEB128_MAX_FRAME_BYTES, bounded_wincode_leb128_config, read_u32_varint};
use wincode::{SchemaRead, SchemaWrite};

use crate::format::{
    DumpWireProfile, TokenTransactionBlockContext, TokenTransactionDumpFooter,
    TokenTransactionDumpHeader,
};

/// Buffered I/O capacity used by the sequential schema-3 reader.
pub const CONSOLIDATED_READER_IO_BUFFER_BYTES: usize = 8 << 20;

/// The exact byte range of one frame payload in `transactions.wincode`.
///
/// The leading LEB128 frame length is not part of this range. The locator can
/// therefore be passed directly to [`read_frame_at`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameLocator {
    pub payload_offset: u64,
    pub payload_len: u32,
}

/// One schema-3 dump record whose byte fields borrow the frame payload.
#[derive(Debug, SchemaRead, SchemaWrite)]
#[allow(clippy::large_enum_variant)] // Keep the direct Wincode record shape allocation-free.
pub enum BorrowedDumpRecord<'a> {
    #[wincode(tag = 0)]
    Header(TokenTransactionDumpHeader),
    #[wincode(tag = 1)]
    Transaction(BorrowedTransactionRecord<'a>),
    #[wincode(tag = 2)]
    Footer(TokenTransactionDumpFooter),
}

/// One schema-3 transaction whose message and metadata borrow the frame.
#[derive(Debug, SchemaRead, SchemaWrite)]
pub struct BorrowedTransactionRecord<'a> {
    pub source_epoch: u64,
    pub source_generation_digest: [u8; 32],
    pub source_wire_profile: DumpWireProfile,
    pub source_block_id: u32,
    pub block: TokenTransactionBlockContext,
    pub tx_index: u32,
    pub flags: u32,
    pub source_first_signature_ordinal: u64,
    pub signature_count: u8,
    pub dump_signature_ordinal: Option<u64>,
    pub message_bytes: &'a [u8],
    pub metadata_bytes: &'a [u8],
}

/// One decoded frame and its stable source locator.
#[derive(Debug)]
pub struct BorrowedDumpFrame<'a> {
    pub locator: FrameLocator,
    pub record: BorrowedDumpRecord<'a>,
    payload: &'a [u8],
}

impl BorrowedDumpFrame<'_> {
    /// Exact Wincode payload bytes for this frame.
    pub fn payload(&self) -> &[u8] {
        self.payload
    }
}

/// Sequential schema-3 reader with one reusable frame payload buffer.
pub struct ConsolidatedFrameReader<R> {
    reader: BufReader<R>,
    logical_offset: u64,
    payload: Vec<u8>,
}

impl<R: Read> ConsolidatedFrameReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::with_capacity(CONSOLIDATED_READER_IO_BUFFER_BYTES, reader),
            logical_offset: 0,
            payload: Vec::new(),
        }
    }

    /// Read and exact-decode the next frame.
    ///
    /// The returned record borrows the reader's payload buffer. The next call
    /// reuses that buffer and is possible only after the prior frame borrow is
    /// no longer used.
    pub fn next_frame(&mut self) -> Result<Option<BorrowedDumpFrame<'_>>> {
        let Some(payload_len) =
            read_u32_varint(&mut self.reader).context("read schema-3 frame length")?
        else {
            return Ok(None);
        };
        let payload_len_usize =
            usize::try_from(payload_len).context("frame length exceeds usize")?;
        ensure!(
            payload_len_usize <= WINCODE_LEB128_MAX_FRAME_BYTES,
            "schema-3 frame exceeds the Wincode limit"
        );
        let payload_offset = self
            .logical_offset
            .checked_add(u64::from(leb128_u32_len(payload_len)))
            .context("schema-3 frame payload offset overflow")?;
        let next_offset = payload_offset
            .checked_add(u64::from(payload_len))
            .context("schema-3 frame end offset overflow")?;

        self.payload.resize(payload_len_usize, 0);
        self.reader
            .read_exact(&mut self.payload)
            .context("read schema-3 frame payload")?;
        self.logical_offset = next_offset;

        let record = decode_borrowed_frame(&self.payload)?;
        Ok(Some(BorrowedDumpFrame {
            locator: FrameLocator {
                payload_offset,
                payload_len,
            },
            record,
            payload: &self.payload,
        }))
    }

    /// Byte offset immediately after the last successfully read payload.
    pub const fn logical_offset(&self) -> u64 {
        self.logical_offset
    }

    /// Current reusable payload-buffer capacity.
    pub fn payload_capacity(&self) -> usize {
        self.payload.capacity()
    }

    pub fn get_ref(&self) -> &R {
        self.reader.get_ref()
    }
}

impl ConsolidatedFrameReader<File> {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file =
            File::open(path).with_context(|| format!("open schema-3 stream {}", path.display()))?;
        Ok(Self::new(file))
    }
}

/// Exact-decode one schema-3 frame payload without allocating byte fields.
pub fn decode_borrowed_frame(payload: &[u8]) -> Result<BorrowedDumpRecord<'_>> {
    wincode::config::deserialize_exact(
        payload,
        bounded_wincode_leb128_config::<WINCODE_LEB128_MAX_FRAME_BYTES>(),
    )
    .map_err(Into::into)
}

/// Read and exact-decode one located payload without changing the file cursor.
///
/// `scratch` is resized and reused. The returned record borrows it.
pub fn read_frame_at<'a>(
    file: &File,
    locator: FrameLocator,
    scratch: &'a mut Vec<u8>,
) -> Result<BorrowedDumpRecord<'a>> {
    let payload_len = usize::try_from(locator.payload_len).context("frame length exceeds usize")?;
    ensure!(
        payload_len <= WINCODE_LEB128_MAX_FRAME_BYTES,
        "schema-3 frame exceeds the Wincode limit"
    );
    locator
        .payload_offset
        .checked_add(u64::from(locator.payload_len))
        .context("schema-3 frame locator end offset overflow")?;
    scratch.resize(payload_len, 0);
    positioned_read_exact(file, scratch, locator.payload_offset)
        .context("read located schema-3 frame payload")?;
    decode_borrowed_frame(scratch)
}

/// Exact current/legacy selection result for one metadata record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExactMetadataSchemaSelection {
    /// The transaction has no metadata bytes.
    NoMetadata,
    /// `err=None`; both wire layouts use the canonical current parse.
    NoError,
    /// Only the current typed-error interpretation is exact and valid.
    CurrentOnly,
    /// Only the historical stored-error interpretation is exact and valid.
    LegacyOnly,
    /// Both interpretations have the same tail, error index, and caller view.
    BothIdentical,
}

impl ExactMetadataSchemaSelection {
    /// Schema to pass to the exact metadata visitors.
    pub const fn selected_schema(self) -> Option<ArchiveV2WireMetadataErrorSchema> {
        match self {
            Self::NoMetadata => None,
            Self::NoError | Self::CurrentOnly | Self::BothIdentical => {
                Some(ArchiveV2WireMetadataErrorSchema::Current)
            }
            Self::LegacyOnly => Some(ArchiveV2WireMetadataErrorSchema::Legacy),
        }
    }
}

/// Select the exact metadata error schema and fail closed on ambiguity.
///
/// `project` must exact-validate the complete metadata value and replace its
/// target with the caller's semantic view. The two targets are caller-owned
/// scratch and can be reused for every record. If both schemas are valid, this
/// function accepts the record only when the metadata tail, transaction-error
/// index, and projected semantic view are identical.
pub fn select_exact_metadata_schema<T>(
    input: &[u8],
    current: &mut T,
    legacy: &mut T,
    mut project: impl FnMut(&mut T, ArchiveV2WireMetadataErrorSchema) -> Result<()>,
) -> Result<ExactMetadataSchemaSelection>
where
    T: PartialEq,
{
    if input.is_empty() {
        return Ok(ExactMetadataSchemaSelection::NoMetadata);
    }
    if input.first() == Some(&0) {
        project(current, ArchiveV2WireMetadataErrorSchema::Current)
            .context("exact-validate metadata without a transaction error")?;
        return Ok(ExactMetadataSchemaSelection::NoError);
    }

    let current_tail = validate_archive_v2_metadata_error_prefix_for_selected_schema(
        input,
        ArchiveV2WireMetadataErrorSchema::Current,
        input.len(),
    )
    .ok();
    let current_valid = current_tail.is_some()
        && project(current, ArchiveV2WireMetadataErrorSchema::Current).is_ok();

    let legacy_tail = validate_archive_v2_metadata_error_prefix_for_selected_schema(
        input,
        ArchiveV2WireMetadataErrorSchema::Legacy,
        input.len(),
    )
    .ok();
    let legacy_valid =
        legacy_tail.is_some() && project(legacy, ArchiveV2WireMetadataErrorSchema::Legacy).is_ok();

    match (current_valid, legacy_valid) {
        (true, false) => Ok(ExactMetadataSchemaSelection::CurrentOnly),
        (false, true) => Ok(ExactMetadataSchemaSelection::LegacyOnly),
        (true, true) => {
            let current_tail = current_tail.expect("validated current metadata has a tail");
            let legacy_tail = legacy_tail.expect("validated legacy metadata has a tail");
            if current_tail.bytes == legacy_tail.bytes
                && current_tail.error_index == legacy_tail.error_index
                && current == legacy
            {
                Ok(ExactMetadataSchemaSelection::BothIdentical)
            } else {
                bail!("dual-valid metadata has different exact selected semantics")
            }
        }
        (false, false) => bail!("metadata is invalid under both exact error schemas"),
    }
}

const fn leb128_u32_len(mut value: u32) -> u8 {
    let mut len = 1u8;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

#[cfg(unix)]
fn positioned_read_exact(file: &File, bytes: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;

    file.read_exact_at(bytes, offset)
}

#[cfg(windows)]
fn positioned_read_exact(file: &File, bytes: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};
    use std::os::windows::fs::FileExt;

    let mut read = 0usize;
    while read < bytes.len() {
        let read_offset = offset
            .checked_add(u64::try_from(read).map_err(Error::other)?)
            .ok_or_else(|| {
                Error::new(ErrorKind::InvalidInput, "positioned read offset overflow")
            })?;
        let count = file.seek_read(&mut bytes[read..], read_offset)?;
        if count == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "positioned read reached end of file",
            ));
        }
        read += count;
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn positioned_read_exact(_file: &File, _bytes: &mut [u8], _offset: u64) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "positioned file reads are not supported on this platform",
    ))
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::{Cursor, Seek, SeekFrom},
    };

    use blockzilla_compact::{CompactMetaV1, CompactTransactionError};
    use blockzilla_primitives::{WincodeLeb128FramedWriter, wincode_leb128_config};
    use blockzilla_read_sdk::{
        ArchiveV2MetadataProjectionLimits, LogPayloadValidation,
        ProjectedArchiveV2TokenMetadataSummary,
        visit_archive_v2_token_metadata_exact_ordered_with_selected_error_schema,
    };
    use tempfile::tempdir;

    use super::*;
    use crate::format::{
        DUMP_SCHEMA_VERSION, DumpStreamKind, PUBKEY_REGISTRY_ID_BASE, TokenTransactionDumpRecord,
        TokenTransactionRecord,
    };

    fn transaction(message_byte: u8) -> TokenTransactionDumpRecord {
        TokenTransactionDumpRecord::Transaction(TokenTransactionRecord {
            source_epoch: 801,
            source_generation_digest: [8; 32],
            source_wire_profile: DumpWireProfile::PostUnknownInstructionFallbacksV1,
            source_block_id: 9,
            block: TokenTransactionBlockContext {
                slot: 346_066_298,
                parent_slot: 346_066_297,
                blockhash_id: 12,
                previous_blockhash_id: 11,
                block_time: Some(10),
                block_height: Some(20),
                transaction_count: 3,
            },
            tx_index: 2,
            flags: 0,
            source_first_signature_ordinal: 42,
            signature_count: 1,
            dump_signature_ordinal: Some(7),
            message_bytes: vec![message_byte; 32],
            metadata_bytes: vec![0; 16],
        })
    }

    fn framed_fixture() -> Vec<u8> {
        let records = [
            TokenTransactionDumpRecord::Header(TokenTransactionDumpHeader {
                schema_version: DUMP_SCHEMA_VERSION,
                stream_kind: DumpStreamKind::Consolidated,
                mint: [3; 32],
                mint_slot: 346_066_298,
                mint_signature: [4; 64],
                source_epoch: None,
                source_generation_digest: None,
                source_wire_profile: None,
                pubkey_registry_id_base: PUBKEY_REGISTRY_ID_BASE,
            }),
            transaction(5),
            transaction(6),
            TokenTransactionDumpRecord::Footer(TokenTransactionDumpFooter {
                epochs: 1,
                blocks_scanned: 1,
                transactions_scanned: 2,
                transactions_written: 2,
                pubkeys: 10,
                signatures: 2,
                owned_block_fallbacks: 0,
                raw_transaction_fallbacks: 0,
                raw_metadata_fallbacks: 0,
            }),
        ];
        let mut writer = WincodeLeb128FramedWriter::new(Vec::new());
        for record in &records {
            writer.write(record).unwrap();
        }
        writer.into_inner()
    }

    #[test]
    fn sequential_and_positioned_readers_round_trip_and_reuse_payload() {
        let bytes = framed_fixture();
        let mut reader = ConsolidatedFrameReader::new(Cursor::new(bytes.clone()));

        {
            let header = reader.next_frame().unwrap().unwrap();
            assert!(matches!(header.record, BorrowedDumpRecord::Header(_)));
        }

        let (first_locator, first_message_pointer, first_payload) = {
            let frame = reader.next_frame().unwrap().unwrap();
            let BorrowedDumpRecord::Transaction(record) = &frame.record else {
                panic!("second frame is not a transaction")
            };
            assert_eq!(record.message_bytes, [5; 32]);
            (
                frame.locator,
                record.message_bytes.as_ptr(),
                frame.payload().to_vec(),
            )
        };
        let capacity = reader.payload_capacity();
        let second_message_pointer = {
            let frame = reader.next_frame().unwrap().unwrap();
            let BorrowedDumpRecord::Transaction(record) = &frame.record else {
                panic!("third frame is not a transaction")
            };
            assert_eq!(record.message_bytes, [6; 32]);
            record.message_bytes.as_ptr()
        };
        assert_eq!(reader.payload_capacity(), capacity);
        assert_eq!(second_message_pointer, first_message_pointer);
        assert!(matches!(
            reader.next_frame().unwrap().unwrap().record,
            BorrowedDumpRecord::Footer(_)
        ));
        assert!(reader.next_frame().unwrap().is_none());
        assert_eq!(reader.logical_offset(), u64::try_from(bytes.len()).unwrap());

        let directory = tempdir().unwrap();
        let path = directory.path().join("transactions.wincode");
        fs::write(&path, &bytes).unwrap();
        let mut file = File::open(path).unwrap();
        file.seek(SeekFrom::Start(3)).unwrap();
        let mut scratch = Vec::new();
        let located = read_frame_at(&file, first_locator, &mut scratch).unwrap();
        let BorrowedDumpRecord::Transaction(record) = located else {
            panic!("located frame is not a transaction")
        };
        assert_eq!(record.message_bytes, [5; 32]);
        assert_eq!(scratch, first_payload);
        assert_eq!(file.stream_position().unwrap(), 3);
    }

    fn metadata(error: Option<CompactTransactionError>) -> CompactMetaV1 {
        CompactMetaV1 {
            err: error,
            fee: 0,
            pre_balances: Vec::new(),
            post_balances: Vec::new(),
            inner_instructions: None,
            logs: None,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: Vec::new(),
            loaded_readonly_addresses: Vec::new(),
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
        }
    }

    fn select_metadata(
        bytes: &[u8],
    ) -> Result<(
        ExactMetadataSchemaSelection,
        Option<ProjectedArchiveV2TokenMetadataSummary>,
        Option<ProjectedArchiveV2TokenMetadataSummary>,
    )> {
        let mut current = None;
        let mut legacy = None;
        let selection =
            select_exact_metadata_schema(bytes, &mut current, &mut legacy, |target, schema| {
                *target = Some(
                    visit_archive_v2_token_metadata_exact_ordered_with_selected_error_schema(
                        bytes,
                        schema,
                        ArchiveV2MetadataProjectionLimits {
                            total_message_accounts: 0,
                            top_level_instruction_count: 0,
                        },
                        0,
                        LogPayloadValidation::Full,
                        |_, _| {},
                        |_, _| {},
                        |_, _, _| {},
                    )?,
                );
                Ok(())
            })?;
        Ok((selection, current, legacy))
    }

    #[test]
    fn metadata_selector_accepts_current_legacy_and_no_error_values() {
        let no_error =
            wincode::config::serialize(&metadata(None), wincode_leb128_config()).unwrap();
        assert_eq!(
            select_metadata(&no_error).unwrap().0,
            ExactMetadataSchemaSelection::NoError
        );

        let current = wincode::config::serialize(
            &metadata(Some(CompactTransactionError::AccountInUse)),
            wincode_leb128_config(),
        )
        .unwrap();
        let (selection, current_stage, legacy_stage) = select_metadata(&current).unwrap();
        assert_eq!(selection, ExactMetadataSchemaSelection::CurrentOnly);
        assert!(current_stage.unwrap().has_error);
        assert!(legacy_stage.is_none());

        let successful =
            wincode::config::serialize(&metadata(None), wincode_leb128_config()).unwrap();
        let stored_account_in_use = vec![0, 0, 0, 0];
        let mut legacy =
            wincode::config::serialize(&Some(stored_account_in_use), wincode_leb128_config())
                .unwrap();
        legacy.extend_from_slice(&successful[1..]);
        let (selection, current_stage, legacy_stage) = select_metadata(&legacy).unwrap();
        assert_eq!(selection, ExactMetadataSchemaSelection::LegacyOnly);
        assert!(current_stage.is_none());
        assert!(legacy_stage.unwrap().has_error);
    }

    #[test]
    fn metadata_selector_fails_closed_for_invalid_and_divergent_dual_valid_inputs() {
        let mut current = 0u8;
        let mut legacy = 0u8;
        assert!(
            select_exact_metadata_schema(&[1], &mut current, &mut legacy, |stage, _| {
                *stage = 1;
                Ok(())
            })
            .is_err()
        );

        // Current tag 4 and a four-byte stored AccountInUse error both have a
        // valid prefix, but their selected meanings and metadata tails differ.
        let mut ambiguous = vec![1, 4, 0, 0, 0, 0];
        ambiguous.extend_from_slice(&[0; 13]);
        assert!(
            select_exact_metadata_schema(&ambiguous, &mut current, &mut legacy, |stage, schema| {
                *stage = match schema {
                    ArchiveV2WireMetadataErrorSchema::Current => 1,
                    ArchiveV2WireMetadataErrorSchema::Legacy => 2,
                };
                Ok(())
            },)
            .is_err()
        );
    }
}
