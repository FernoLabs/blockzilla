use crate::car_block_group::CarBlockGroup;
use crate::error::CarReadError;
use crate::error::CarReadResult;
use crate::node::peek_node_type;
use crate::reconstruct::{Cid36, NodeLocation};
use std::io;
use std::io::BufRead;
use std::io::Read;

const MAX_UVARINT_LEN_64: usize = 10;
const CAR_CID_LEN: usize = 36;
const NODE_KIND_PREFIX_BYTES: usize = 16;

/// Low-level streaming reader for Old Faithful CAR archives.
///
/// `CarBlockReader` keeps track of the current CAR byte offset and entry index.
/// Use it directly when building indexes or scanners. For normal block-by-block
/// processing, [`crate::CarStream`] is the smaller wrapper around this type.
pub struct CarBlockReader<R: Read> {
    pub reader: io::BufReader<R>,
    /// Current byte offset in the CAR stream, including the header if it has
    /// already been read.
    pub offset: u64,
    /// Number of CAR entries read after the header.
    pub entry_index: u64,
}

/// Borrowed payload for one fully loaded CAR entry.
pub struct CarEntryPayload<'a> {
    /// Entry index and CAR byte offset.
    pub location: NodeLocation,
    /// CAR CID bytes for this entry.
    pub cid: Cid36,
    /// Entry payload bytes without the CID prefix.
    pub payload: &'a [u8],
    /// Payload length in bytes.
    pub payload_len: usize,
    /// CAR entry length from the entry varint, including CID and payload.
    pub entry_len: usize,
    /// Number of bytes used by the entry length varint.
    pub varint_len: usize,
    /// Total on-wire entry length, including varint, CID, and payload.
    pub total_len: usize,
}

/// Borrowed payload for an entry where the caller may have skipped the body.
pub struct CarEntryMaybePayload<'a> {
    pub location: NodeLocation,
    pub cid: Cid36,
    /// Prefix bytes that were always read before the selection callback ran.
    pub prefix: &'a [u8],
    /// Full payload when selected, or `None` when skipped.
    pub payload: Option<&'a [u8]>,
    pub payload_len: usize,
    pub entry_len: usize,
    pub varint_len: usize,
    pub total_len: usize,
}

/// One fully decoded CAR node and its framing lengths.
///
/// The node owns the data-frame bytes that it needs. The lengths describe the
/// original CAR entry and let scanners account for input bytes without encoding
/// the node again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedNodeRecord {
    /// Decoded lossless node.
    pub node: crate::reconstruct::RawNode,
    /// Payload length without the CID or entry-length varint.
    pub payload_len: usize,
    /// Entry length from the CAR varint, including the CID and payload.
    pub entry_len: usize,
    /// Total on-wire length, including the varint, CID, and payload.
    pub total_len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecodedPayloadSource {
    DirectBuffer,
    Scratch,
}

struct InternalDecodedRecord<T> {
    value: T,
    payload_len: usize,
    entry_len: usize,
    total_len: usize,
    payload_source: DecodedPayloadSource,
}

/// Physical CAR nodes and bytes consumed by one lossless block read.
///
/// These counters describe nodes in the CAR stream. They do not count logical
/// entry or transaction references after CID resolution.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LosslessBlockReadStats {
    pub car_entries: u64,
    pub payload_bytes: u64,
    pub wire_bytes: u64,
    /// Entries decoded directly from the reader's buffered bytes.
    pub direct_buffer_entries: u64,
    /// Payload bytes decoded directly from the reader's buffered bytes.
    pub direct_buffer_payload_bytes: u64,
    /// Entries that crossed an input-buffer boundary and used caller scratch.
    pub scratch_entries: u64,
    /// Payload bytes copied into caller scratch before decoding.
    pub scratch_payload_bytes: u64,
    pub transactions: u64,
    pub entries: u64,
    pub blocks: u64,
    pub rewards: u64,
    pub dataframes: u64,
    pub subsets: u64,
    pub epochs: u64,
}

impl LosslessBlockReadStats {
    fn record(
        &mut self,
        record: &DecodedNodeRecord,
        payload_source: DecodedPayloadSource,
    ) -> CarReadResult<()> {
        let kind = match &record.node {
            crate::reconstruct::RawNode::Transaction(_) => 0,
            crate::reconstruct::RawNode::Entry(_) => 1,
            crate::reconstruct::RawNode::Block(_) => 2,
            crate::reconstruct::RawNode::Subset(_) => 3,
            crate::reconstruct::RawNode::Epoch(_) => 4,
            crate::reconstruct::RawNode::Rewards(_) => 5,
            crate::reconstruct::RawNode::DataFrame(_) => 6,
        };
        self.record_parts(record.payload_len, record.total_len, payload_source, kind)
    }

    fn record_parts(
        &mut self,
        payload_len: usize,
        total_len: usize,
        payload_source: DecodedPayloadSource,
        kind: u64,
    ) -> CarReadResult<()> {
        self.car_entries = checked_stat_add(self.car_entries, 1, "CAR entry count")?;
        self.payload_bytes = checked_stat_add(
            self.payload_bytes,
            payload_len as u64,
            "CAR payload byte count",
        )?;
        self.wire_bytes =
            checked_stat_add(self.wire_bytes, total_len as u64, "CAR wire byte count")?;
        match payload_source {
            DecodedPayloadSource::DirectBuffer => {
                self.direct_buffer_entries = checked_stat_add(
                    self.direct_buffer_entries,
                    1,
                    "direct-buffer CAR entry count",
                )?;
                self.direct_buffer_payload_bytes = checked_stat_add(
                    self.direct_buffer_payload_bytes,
                    payload_len as u64,
                    "direct-buffer CAR payload byte count",
                )?;
            }
            DecodedPayloadSource::Scratch => {
                self.scratch_entries =
                    checked_stat_add(self.scratch_entries, 1, "scratch CAR entry count")?;
                self.scratch_payload_bytes = checked_stat_add(
                    self.scratch_payload_bytes,
                    payload_len as u64,
                    "scratch CAR payload byte count",
                )?;
            }
        }
        let counter = match kind {
            0 => &mut self.transactions,
            1 => &mut self.entries,
            2 => &mut self.blocks,
            3 => &mut self.subsets,
            4 => &mut self.epochs,
            5 => &mut self.rewards,
            6 => &mut self.dataframes,
            _ => {
                return Err(CarReadError::InvalidData(format!(
                    "unknown CAR node kind {kind}"
                )));
            }
        };
        *counter = checked_stat_add(*counter, 1, "CAR node count")?;
        Ok(())
    }
}

/// Result of one lossless block read.
///
/// `has_block` is false at clean EOF. `stats` still includes trailing subset
/// and epoch nodes that were consumed before that EOF.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LosslessBlockRead {
    pub has_block: bool,
    pub stats: LosslessBlockReadStats,
}

/// Payload loading decision for [`CarBlockReader::read_entry_payload_select_with_scratch`].
pub enum CarPayloadRead {
    /// Skip the rest of the payload after reading the requested prefix.
    Skip,
    /// Read only this many bytes from the payload.
    Prefix(usize),
    /// Read the full payload.
    Full,
}

/// Caller-selected limits for one lossless block scan.
///
/// These limits stop declared CAR payload sizes before the lossless reader
/// allocates their bodies. They do not make legacy CAR decoding suitable for
/// untrusted input; callers must still require an operator-trusted source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LosslessBlockReadLimits {
    pub max_entry_payload_bytes: usize,
    pub max_block_payload_bytes: usize,
    pub max_entries_per_block: usize,
    pub max_transactions_per_block: usize,
}

impl<R: Read> CarBlockReader<R> {
    /// Create a CAR reader with a specific internal I/O buffer size.
    pub fn with_capacity(inner: R, io_buf_bytes: usize) -> Self {
        Self {
            reader: io::BufReader::with_capacity(io_buf_bytes, inner),
            offset: 0,
            entry_index: 0,
        }
    }

    /// Read and return the raw CAR header bytes, including its length varint.
    pub fn read_header_bytes(&mut self) -> CarReadResult<Vec<u8>> {
        let (header_len, header_varint) = read_uvarint64_with_bytes(&mut self.reader)?;
        let header_len = header_len as usize;
        let mut out = header_varint;
        let start = out.len();
        out.resize(start + header_len, 0u8);
        self.reader
            .read_exact(&mut out[start..])
            .map_err(|e| CarReadError::Io(e.to_string()))?;
        self.offset += out.len() as u64;
        Ok(out)
    }

    /// Read a CAR header only when its declared payload fits `max_header_bytes`.
    pub fn read_header_bytes_bounded(&mut self, max_header_bytes: usize) -> CarReadResult<Vec<u8>> {
        if max_header_bytes == 0 {
            return Err(CarReadError::InvalidData(
                "CAR header byte limit must be nonzero".to_string(),
            ));
        }
        let (header_len, header_varint) = read_uvarint64_with_bytes(&mut self.reader)?;
        let header_len = usize::try_from(header_len)
            .map_err(|_| CarReadError::InvalidData("CAR header exceeds usize".to_string()))?;
        if header_len > max_header_bytes {
            return Err(CarReadError::InvalidData(format!(
                "CAR header {header_len} bytes exceeds configured limit {max_header_bytes}"
            )));
        }
        let mut output = header_varint;
        let start = output.len();
        let total = start
            .checked_add(header_len)
            .ok_or_else(|| CarReadError::InvalidData("CAR header size overflow".to_string()))?;
        output.resize(total, 0u8);
        self.reader
            .read_exact(&mut output[start..])
            .map_err(|error| CarReadError::Io(error.to_string()))?;
        self.offset = self
            .offset
            .checked_add(u64::try_from(output.len()).map_err(|_| {
                CarReadError::InvalidData("CAR header size exceeds u64".to_string())
            })?)
            .ok_or_else(|| CarReadError::InvalidData("CAR offset overflow".to_string()))?;
        Ok(output)
    }

    /// Read and discard the CAR header.
    ///
    /// Call this once before reading entries or blocks from a normal CAR file.
    pub fn skip_header(&mut self) -> CarReadResult<()> {
        let _ = self.read_header_bytes()?;
        Ok(())
    }

    /// Read and discard a CAR header under an explicit payload limit.
    pub fn skip_header_bounded(&mut self, max_header_bytes: usize) -> CarReadResult<()> {
        let _ = self.read_header_bytes_bounded(max_header_bytes)?;
        Ok(())
    }

    /// Safe group: follows block->entry->tx links.
    ///
    /// Reads CAR sections until it finds a "block" node (kind == 2) in the entry payload.
    /// Fills `out` (reusing its internal allocations) and returns:
    /// - Ok(true)  => group produced
    /// - Ok(false) => clean EOF (no more groups)
    pub fn read_until_block_into(&mut self, out: &mut CarBlockGroup) -> CarReadResult<bool> {
        out.clear();

        if !out.reads_transaction_payloads() {
            return self.read_until_block_selecting_payloads_into(out);
        }

        loop {
            let entry_len = match read_uvarint64_with_len(&mut self.reader) {
                Ok((v, varint_len)) => {
                    self.offset += varint_len as u64;
                    v as usize
                }
                Err(CarReadError::Eof) => {
                    return Ok(false);
                }
                Err(e) => return Err(e),
            };

            let mut cid_buf = [0; 36];
            self.reader.read_exact(&mut cid_buf)?;
            self.offset += cid_buf.len() as u64;

            let payload_len = entry_len
                .checked_sub(cid_buf.len())
                .ok_or_else(|| CarReadError::InvalidData("entry_len < cid_len".to_string()))?;

            let cid = Cid36::from_car_bytes(cid_buf);
            let done = out.read_entry_payload_with_cid_into(
                Some(cid.car_bytes()),
                &mut self.reader,
                payload_len,
            )?;
            self.offset += payload_len as u64;
            self.entry_index += 1;
            if done {
                return Ok(true);
            }
        }
    }

    fn read_until_block_selecting_payloads_into(
        &mut self,
        out: &mut CarBlockGroup,
    ) -> CarReadResult<bool> {
        let mut scratch = Vec::new();
        let transaction_prefix_bytes = out.transaction_prefix_bytes();
        loop {
            let Some(entry) = self.read_entry_payload_select_with_scratch(
                &mut scratch,
                NODE_KIND_PREFIX_BYTES,
                |prefix| match peek_node_type(prefix) {
                    Ok(0) => transaction_prefix_bytes
                        .map(CarPayloadRead::Prefix)
                        .unwrap_or(CarPayloadRead::Skip),
                    Ok(_) | Err(_) => CarPayloadRead::Full,
                },
            )?
            else {
                return Ok(false);
            };

            let done = out.read_entry_maybe_payload_with_cid_into(
                Some(entry.cid.car_bytes()),
                entry.prefix,
                entry.payload,
                entry.payload_len,
            )?;
            if done {
                return Ok(true);
            }
        }
    }

    pub fn read_until_block_lossless(
        &mut self,
        out: &mut crate::reconstruct::LosslessCarBlock,
    ) -> CarReadResult<bool> {
        Ok(self.read_until_block_lossless_with_stats(out)?.has_block)
    }

    /// Read and resolve one lossless block with physical per-call counters.
    ///
    /// The block owns a bounded data-frame buffer pool. Reusing the same block,
    /// including through a worker recycle queue, reuses those allocations.
    /// A clean EOF after subset or epoch nodes returns `has_block == false` and
    /// includes those trailing nodes in `stats`. An EOF with an unfinished
    /// transaction, entry, rewards, or data-frame group is invalid data.
    pub fn read_until_block_lossless_with_stats(
        &mut self,
        out: &mut crate::reconstruct::LosslessCarBlock,
    ) -> CarReadResult<LosslessBlockRead> {
        out.clear();
        let mut stats = LosslessBlockReadStats::default();

        loop {
            let checkpoint = out.data_buffer_pool.checkpoint();
            let record = {
                let pool = &mut out.data_buffer_pool;
                self.read_decoded_node_record_with_scratch_tracked(
                    &mut out.scratch,
                    &mut |required| pool.take(required),
                )
            };
            let record = match record {
                Ok(record) => record,
                Err(err) => {
                    out.data_buffer_pool.rollback_to_checkpoint(checkpoint);
                    return Err(err);
                }
            };
            let Some((record, payload_source)) = record else {
                if let Some(error) = out.unterminated_block_group_error() {
                    return Err(CarReadError::InvalidData(error.to_string()));
                }
                return Ok(LosslessBlockRead {
                    has_block: false,
                    stats,
                });
            };

            stats.record(&record, payload_source)?;
            let done = out.push_raw_node(record.node)?;
            if done {
                return Ok(LosslessBlockRead {
                    has_block: true,
                    stats,
                });
            }
        }
    }

    /// Read one lossless block in canonical Old Faithful physical order.
    ///
    /// This path appends transaction and entry nodes directly. It does not
    /// build CID lookup tables for transactions, entries, or rewards. Use the
    /// generic lossless method when input can use a different physical order.
    pub fn read_until_block_ordered_lossless(
        &mut self,
        out: &mut crate::ordered_lossless::OrderedLosslessCarBlock,
    ) -> CarReadResult<bool> {
        Ok(self
            .read_until_block_ordered_lossless_with_stats(out)?
            .has_block)
    }

    /// Read one canonical ordered lossless block with physical read counters.
    ///
    /// This method keeps the direct-buffer decode path and the same bounded
    /// dataframe buffer pool as the generic lossless reader.
    pub fn read_until_block_ordered_lossless_with_stats(
        &mut self,
        out: &mut crate::ordered_lossless::OrderedLosslessCarBlock,
    ) -> CarReadResult<LosslessBlockRead> {
        self.read_until_block_ordered_lossless_inner(out, None)
    }

    /// Read one canonical ordered block with explicit payload limits.
    pub fn read_until_block_ordered_lossless_bounded(
        &mut self,
        out: &mut crate::ordered_lossless::OrderedLosslessCarBlock,
        limits: LosslessBlockReadLimits,
    ) -> CarReadResult<bool> {
        if limits.max_entry_payload_bytes == 0
            || limits.max_block_payload_bytes == 0
            || limits.max_entries_per_block == 0
            || limits.max_transactions_per_block == 0
        {
            return Err(CarReadError::InvalidData(
                "ordered lossless block read limits must be nonzero".to_string(),
            ));
        }
        Ok(self
            .read_until_block_ordered_lossless_inner(out, Some(limits))?
            .has_block)
    }

    fn read_until_block_ordered_lossless_inner(
        &mut self,
        out: &mut crate::ordered_lossless::OrderedLosslessCarBlock,
        limits: Option<LosslessBlockReadLimits>,
    ) -> CarReadResult<LosslessBlockRead> {
        out.clear();
        let mut stats = LosslessBlockReadStats::default();

        loop {
            let checkpoint = out.data_buffer_pool.checkpoint();
            let record = {
                let pool = &mut out.data_buffer_pool;
                let recycled_shredding = &mut out.recycled_shredding;
                self.read_node_record_with_scratch_tracked(
                    &mut out.scratch,
                    &mut |location, cid, payload| {
                        crate::ordered_lossless::decode_ordered_raw_node_with_data_buffers(
                            location,
                            cid,
                            payload,
                            &mut |required| pool.take(required),
                            recycled_shredding,
                        )
                    },
                )
            };
            let record = match record {
                Ok(record) => record,
                Err(error) => {
                    out.data_buffer_pool.rollback_to_checkpoint(checkpoint);
                    return Err(error);
                }
            };
            let Some(record) = record else {
                if let Some(error) = out.unterminated_block_group_error() {
                    return Err(CarReadError::InvalidData(error.to_string()));
                }
                return Ok(LosslessBlockRead {
                    has_block: false,
                    stats,
                });
            };

            stats.record_parts(
                record.payload_len,
                record.total_len,
                record.payload_source,
                record.value.kind(),
            )?;
            if let Some(limits) = limits {
                if record.payload_len > limits.max_entry_payload_bytes {
                    return Err(CarReadError::InvalidData(format!(
                        "CAR entry payload {} exceeds configured limit {}",
                        record.payload_len, limits.max_entry_payload_bytes
                    )));
                }
                if stats.payload_bytes > limits.max_block_payload_bytes as u64 {
                    return Err(CarReadError::InvalidData(format!(
                        "CAR block payload bytes {} exceed configured limit {}",
                        stats.payload_bytes, limits.max_block_payload_bytes
                    )));
                }
                if stats.car_entries > limits.max_entries_per_block as u64 {
                    return Err(CarReadError::InvalidData(format!(
                        "CAR block entry count {} exceeds configured limit {}",
                        stats.car_entries, limits.max_entries_per_block
                    )));
                }
                if stats.transactions > limits.max_transactions_per_block as u64 {
                    return Err(CarReadError::InvalidData(format!(
                        "CAR block transaction count {} exceeds configured limit {}",
                        stats.transactions, limits.max_transactions_per_block
                    )));
                }
            }
            let done = out.push_ordered_node(record.value)?;
            if done {
                return Ok(LosslessBlockRead {
                    has_block: true,
                    stats,
                });
            }
        }
    }

    /// Read one CID-resolved block with explicit raw-payload limits.
    ///
    /// The existing unbounded method is kept for compatibility. Query adapters
    /// should use this method and must still label the source operator-trusted.
    pub fn read_until_block_lossless_bounded(
        &mut self,
        out: &mut crate::reconstruct::LosslessCarBlock,
        limits: LosslessBlockReadLimits,
    ) -> CarReadResult<bool> {
        if limits.max_entry_payload_bytes == 0
            || limits.max_block_payload_bytes == 0
            || limits.max_entries_per_block == 0
            || limits.max_transactions_per_block == 0
        {
            return Err(CarReadError::InvalidData(
                "lossless block read limits must be nonzero".to_string(),
            ));
        }

        out.clear();
        let mut block_payload_bytes = 0usize;
        let mut block_entries = 0usize;

        loop {
            let entry_offset = self.offset;
            let current_entry_index = self.entry_index;
            let entry_len = match read_uvarint64_with_len(&mut self.reader) {
                Ok((value, varint_len)) => {
                    self.offset = self
                        .offset
                        .checked_add(u64::try_from(varint_len).map_err(|_| {
                            CarReadError::InvalidData("CAR varint length exceeds u64".to_string())
                        })?)
                        .ok_or_else(|| {
                            CarReadError::InvalidData("CAR offset overflow".to_string())
                        })?;
                    usize::try_from(value).map_err(|_| {
                        CarReadError::InvalidData("entry length exceeds usize".to_string())
                    })?
                }
                // A valid Old Faithful CAR can end with subset and epoch index
                // nodes after its last block. Those nodes are not retained in
                // this pending block group. A retained transaction, entry,
                // reward, or dataframe without a block is truncated input.
                Err(CarReadError::Eof) => {
                    if let Some(error) = out.unterminated_block_group_error() {
                        return Err(CarReadError::InvalidData(error.to_string()));
                    }
                    return Ok(false);
                }
                Err(error) => return Err(error),
            };
            let payload_len = entry_len
                .checked_sub(CAR_CID_LEN)
                .ok_or_else(|| CarReadError::InvalidData("entry_len < cid_len".to_string()))?;
            if payload_len > limits.max_entry_payload_bytes {
                return Err(CarReadError::InvalidData(format!(
                    "CAR entry payload {payload_len} exceeds configured limit {}",
                    limits.max_entry_payload_bytes
                )));
            }
            block_payload_bytes =
                block_payload_bytes
                    .checked_add(payload_len)
                    .ok_or_else(|| {
                        CarReadError::InvalidData(
                            "CAR block payload byte count overflow".to_string(),
                        )
                    })?;
            if block_payload_bytes > limits.max_block_payload_bytes {
                return Err(CarReadError::InvalidData(format!(
                    "CAR block payload bytes {block_payload_bytes} exceed configured limit {}",
                    limits.max_block_payload_bytes
                )));
            }
            block_entries = block_entries.checked_add(1).ok_or_else(|| {
                CarReadError::InvalidData("CAR block entry count overflow".to_string())
            })?;
            if block_entries > limits.max_entries_per_block {
                return Err(CarReadError::InvalidData(format!(
                    "CAR block entry count {block_entries} exceeds configured limit {}",
                    limits.max_entries_per_block
                )));
            }

            let mut cid_buf = [0u8; CAR_CID_LEN];
            self.reader.read_exact(&mut cid_buf)?;
            self.offset = self
                .offset
                .checked_add(u64::try_from(cid_buf.len()).map_err(|_| {
                    CarReadError::InvalidData("CAR CID length exceeds u64".to_string())
                })?)
                .ok_or_else(|| CarReadError::InvalidData("CAR offset overflow".to_string()))?;

            let done = out.read_entry_payload_into_bounded(
                &mut self.reader,
                payload_len,
                crate::reconstruct::NodeLocation {
                    entry_index: current_entry_index,
                    car_offset: entry_offset,
                },
                cid_buf,
                limits.max_transactions_per_block,
            )?;
            self.offset = self
                .offset
                .checked_add(u64::try_from(payload_len).map_err(|_| {
                    CarReadError::InvalidData("CAR payload length exceeds u64".to_string())
                })?)
                .ok_or_else(|| CarReadError::InvalidData("CAR offset overflow".to_string()))?;
            self.entry_index = self
                .entry_index
                .checked_add(1)
                .ok_or_else(|| CarReadError::InvalidData("CAR entry index overflow".to_string()))?;
            if done {
                return Ok(true);
            }
        }
    }

    /// Reads a single CAR entry payload into caller-owned scratch.
    ///
    /// This is the lowest-level reusable scanner API: it preserves CAR offset
    /// and entry index for index builders while keeping the payload allocation
    /// under caller control.
    pub fn read_entry_payload_with_scratch<'a>(
        &mut self,
        scratch: &'a mut Vec<u8>,
    ) -> CarReadResult<Option<CarEntryPayload<'a>>> {
        let entry_offset = self.offset;
        let current_entry_index = self.entry_index;

        let (entry_len, varint_len) = match read_uvarint64_with_len(&mut self.reader) {
            Ok((value, varint_len)) => {
                self.offset += varint_len as u64;
                (value as usize, varint_len)
            }
            Err(CarReadError::Eof) => return Ok(None),
            Err(err) => return Err(err),
        };

        let mut cid_buf = [0u8; 36];
        self.reader.read_exact(&mut cid_buf)?;
        self.offset += cid_buf.len() as u64;

        let payload_len = entry_len
            .checked_sub(CAR_CID_LEN)
            .ok_or_else(|| CarReadError::InvalidData("entry_len < cid_len".to_string()))?;
        let total_len = varint_len
            .checked_add(entry_len)
            .ok_or_else(|| CarReadError::InvalidData("entry length overflow".to_string()))?;

        scratch.clear();
        append_exact_from_bufread(&mut self.reader, scratch, payload_len)?;
        self.offset += payload_len as u64;
        self.entry_index += 1;

        Ok(Some(CarEntryPayload {
            location: NodeLocation {
                entry_index: current_entry_index,
                car_offset: entry_offset,
            },
            cid: Cid36::from_car_bytes(cid_buf),
            payload: scratch,
            payload_len,
            entry_len,
            varint_len,
            total_len,
        }))
    }

    /// Reads only a payload prefix first, then either reads the rest or skips it.
    ///
    /// This keeps transaction-only scanners fast: they can peek the CBOR node
    /// kind from a tiny prefix and avoid copying large block/entry/reward
    /// payloads they do not need.
    pub fn read_entry_payload_if_prefix_with_scratch<'a, F>(
        &mut self,
        scratch: &'a mut Vec<u8>,
        prefix_len: usize,
        should_read_payload: F,
    ) -> CarReadResult<Option<CarEntryMaybePayload<'a>>>
    where
        F: FnOnce(&[u8]) -> bool,
    {
        let entry_offset = self.offset;
        let current_entry_index = self.entry_index;

        let (entry_len, varint_len) = match read_uvarint64_with_len(&mut self.reader) {
            Ok((value, varint_len)) => {
                self.offset += varint_len as u64;
                (value as usize, varint_len)
            }
            Err(CarReadError::Eof) => return Ok(None),
            Err(err) => return Err(err),
        };

        let mut cid_buf = [0u8; CAR_CID_LEN];
        self.reader.read_exact(&mut cid_buf)?;
        self.offset += cid_buf.len() as u64;

        let payload_len = entry_len
            .checked_sub(CAR_CID_LEN)
            .ok_or_else(|| CarReadError::InvalidData("entry_len < cid_len".to_string()))?;
        let total_len = varint_len
            .checked_add(entry_len)
            .ok_or_else(|| CarReadError::InvalidData("entry length overflow".to_string()))?;
        let prefix_len = prefix_len.min(payload_len);

        scratch.clear();
        append_exact_from_bufread(&mut self.reader, scratch, prefix_len)?;
        self.offset += prefix_len as u64;

        let read_payload = should_read_payload(scratch);
        if read_payload {
            append_exact_from_bufread(&mut self.reader, scratch, payload_len - prefix_len)?;
            self.offset += (payload_len - prefix_len) as u64;
        } else {
            self.skip_payload_bytes(payload_len - prefix_len)?;
        }
        self.entry_index += 1;

        Ok(Some(CarEntryMaybePayload {
            location: NodeLocation {
                entry_index: current_entry_index,
                car_offset: entry_offset,
            },
            cid: Cid36::from_car_bytes(cid_buf),
            prefix: &scratch[..prefix_len],
            payload: read_payload.then_some(&scratch[..]),
            payload_len,
            entry_len,
            varint_len,
            total_len,
        }))
    }

    /// Reads a small initial payload prefix, then lets the caller decide whether
    /// to skip, extend to a larger prefix, or materialize the full payload.
    pub fn read_entry_payload_select_with_scratch<'a, F>(
        &mut self,
        scratch: &'a mut Vec<u8>,
        initial_prefix_len: usize,
        select_payload: F,
    ) -> CarReadResult<Option<CarEntryMaybePayload<'a>>>
    where
        F: FnOnce(&[u8]) -> CarPayloadRead,
    {
        let entry_offset = self.offset;
        let current_entry_index = self.entry_index;

        let (entry_len, varint_len) = match read_uvarint64_with_len(&mut self.reader) {
            Ok((value, varint_len)) => {
                self.offset += varint_len as u64;
                (value as usize, varint_len)
            }
            Err(CarReadError::Eof) => return Ok(None),
            Err(err) => return Err(err),
        };

        let mut cid_buf = [0u8; CAR_CID_LEN];
        self.reader.read_exact(&mut cid_buf)?;
        self.offset += cid_buf.len() as u64;

        let payload_len = entry_len
            .checked_sub(CAR_CID_LEN)
            .ok_or_else(|| CarReadError::InvalidData("entry_len < cid_len".to_string()))?;
        let total_len = varint_len
            .checked_add(entry_len)
            .ok_or_else(|| CarReadError::InvalidData("entry length overflow".to_string()))?;
        let initial_prefix_len = initial_prefix_len.min(payload_len);

        scratch.clear();
        append_exact_from_bufread(&mut self.reader, scratch, initial_prefix_len)?;
        self.offset += initial_prefix_len as u64;

        let target_len = match select_payload(scratch) {
            CarPayloadRead::Skip => initial_prefix_len,
            CarPayloadRead::Prefix(len) => len.min(payload_len),
            CarPayloadRead::Full => payload_len,
        };

        if target_len > initial_prefix_len {
            append_exact_from_bufread(&mut self.reader, scratch, target_len - initial_prefix_len)?;
            self.offset += (target_len - initial_prefix_len) as u64;
        }

        self.skip_payload_bytes(payload_len - target_len)?;
        self.entry_index += 1;

        Ok(Some(CarEntryMaybePayload {
            location: NodeLocation {
                entry_index: current_entry_index,
                car_offset: entry_offset,
            },
            cid: Cid36::from_car_bytes(cid_buf),
            prefix: &scratch[..target_len],
            payload: (target_len == payload_len).then_some(&scratch[..]),
            payload_len,
            entry_len,
            varint_len,
            total_len,
        }))
    }

    fn skip_payload_bytes(&mut self, mut len: usize) -> CarReadResult<()> {
        while len > 0 {
            let buf = self
                .reader
                .fill_buf()
                .map_err(|err| CarReadError::Io(err.to_string()))?;
            if buf.is_empty() {
                return Err(CarReadError::UnexpectedEof(
                    "EOF while skipping CAR payload".to_string(),
                ));
            }
            let consumed = len.min(buf.len());
            self.reader.consume(consumed);
            self.offset += consumed as u64;
            len -= consumed;
        }
        Ok(())
    }

    /// Reads a single CAR entry as a fully decoded raw node.
    ///
    /// The caller can reuse `scratch` across calls to avoid reallocating the
    /// payload buffer while scanning a whole archive.
    pub fn read_lossless_node_with_scratch(
        &mut self,
        scratch: &mut Vec<u8>,
    ) -> CarReadResult<Option<crate::reconstruct::RawNode>> {
        let mut take_data_buffer = |len| Vec::with_capacity(len);
        Ok(self
            .read_decoded_node_record_with_scratch(scratch, &mut take_data_buffer)?
            .map(|record| record.node))
    }

    /// Reads one decoded node, its CAR framing lengths, and obtains owned
    /// data-frame storage from the caller.
    ///
    /// Reuse `scratch` for the encoded payload. `take_data_buffer` is called for
    /// each embedded or standalone data frame. It can return a buffer from a
    /// pool; existing contents are cleared before the decoded bytes are copied.
    pub fn read_decoded_node_record_with_scratch<F>(
        &mut self,
        scratch: &mut Vec<u8>,
        take_data_buffer: &mut F,
    ) -> CarReadResult<Option<DecodedNodeRecord>>
    where
        F: FnMut(usize) -> Vec<u8>,
    {
        self.read_decoded_node_record_with_scratch_tracked(scratch, take_data_buffer)
            .map(|record| record.map(|(record, _)| record))
    }

    fn read_decoded_node_record_with_scratch_tracked<F>(
        &mut self,
        scratch: &mut Vec<u8>,
        take_data_buffer: &mut F,
    ) -> CarReadResult<Option<(DecodedNodeRecord, DecodedPayloadSource)>>
    where
        F: FnMut(usize) -> Vec<u8>,
    {
        self.read_node_record_with_scratch_tracked(scratch, &mut |location, cid, payload| {
            crate::reconstruct::decode_raw_node_with_data_buffers(
                location,
                cid,
                payload,
                take_data_buffer,
            )
        })
        .map(|record| {
            record.map(|record| {
                (
                    DecodedNodeRecord {
                        node: record.value,
                        payload_len: record.payload_len,
                        entry_len: record.entry_len,
                        total_len: record.total_len,
                    },
                    record.payload_source,
                )
            })
        })
    }

    fn read_node_record_with_scratch_tracked<T, F>(
        &mut self,
        scratch: &mut Vec<u8>,
        decode: &mut F,
    ) -> CarReadResult<Option<InternalDecodedRecord<T>>>
    where
        F: FnMut(NodeLocation, Cid36, &[u8]) -> Result<T, crate::reconstruct::ReconstructError>,
    {
        let entry_offset = self.offset;
        let current_entry_index = self.entry_index;
        let (entry_len, varint_len) = match read_uvarint64_with_len(&mut self.reader) {
            Ok((value, varint_len)) => {
                self.offset += varint_len as u64;
                (value as usize, varint_len)
            }
            Err(CarReadError::Eof) => return Ok(None),
            Err(err) => return Err(err),
        };

        let mut cid_buf = [0u8; CAR_CID_LEN];
        self.reader.read_exact(&mut cid_buf)?;
        self.offset += cid_buf.len() as u64;

        let payload_len = entry_len
            .checked_sub(CAR_CID_LEN)
            .ok_or_else(|| CarReadError::InvalidData("entry_len < cid_len".to_string()))?;
        let total_len = varint_len
            .checked_add(entry_len)
            .ok_or_else(|| CarReadError::InvalidData("entry length overflow".to_string()))?;
        let location = NodeLocation {
            entry_index: current_entry_index,
            car_offset: entry_offset,
        };
        let cid = Cid36::from_car_bytes(cid_buf);

        // The decoded raw node owns every byte that must outlive this call. If
        // the full payload is already buffered, decode it before consuming the
        // buffer and avoid the otherwise redundant entry-scratch copy.
        scratch.clear();
        let direct_decode = {
            let available = loop {
                match self.reader.fill_buf() {
                    Ok(available) => break available,
                    Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                    Err(err) => return Err(CarReadError::Io(err.to_string())),
                }
            };
            (available.len() >= payload_len)
                .then(|| decode(location, cid, &available[..payload_len]))
        };

        let (decoded, payload_source) = if let Some(decoded) = direct_decode {
            self.reader.consume(payload_len);
            (decoded, DecodedPayloadSource::DirectBuffer)
        } else {
            append_exact_from_bufread(&mut self.reader, scratch, payload_len)?;
            (
                decode(location, cid, scratch),
                DecodedPayloadSource::Scratch,
            )
        };

        // Preserve the existing consumption contract: a complete payload is
        // consumed and counted even when its node fails to decode.
        self.offset += payload_len as u64;
        self.entry_index += 1;
        let value = decoded.map_err(|err| {
            CarReadError::InvalidData(format!(
                "entry {} at offset {}: {}",
                location.entry_index, location.car_offset, err
            ))
        })?;

        Ok(Some(InternalDecodedRecord {
            value,
            payload_len,
            entry_len,
            total_len,
            payload_source,
        }))
    }

    /// Reads one decoded node record using fresh payload and data-frame buffers.
    pub fn read_decoded_node_record(&mut self) -> CarReadResult<Option<DecodedNodeRecord>> {
        let mut scratch = Vec::new();
        let mut take_data_buffer = |len| Vec::with_capacity(len);
        self.read_decoded_node_record_with_scratch(&mut scratch, &mut take_data_buffer)
    }

    pub fn read_lossless_node(&mut self) -> CarReadResult<Option<crate::reconstruct::RawNode>> {
        let mut scratch = Vec::new();
        self.read_lossless_node_with_scratch(&mut scratch)
    }
}

fn checked_stat_add(value: u64, added: u64, label: &str) -> CarReadResult<u64> {
    value
        .checked_add(added)
        .ok_or_else(|| CarReadError::InvalidData(format!("{label} overflow")))
}

/// Append exactly `additional` bytes without first zero-filling the
/// destination's spare capacity.
///
/// `Vec::extend_from_slice` safely writes into spare capacity, while the
/// `BufRead` interface lets us avoid exposing uninitialized bytes to an
/// arbitrary `Read` implementation.
fn append_exact_from_bufread<R: BufRead>(
    reader: &mut R,
    out: &mut Vec<u8>,
    additional: usize,
) -> CarReadResult<()> {
    let target_len = out
        .len()
        .checked_add(additional)
        .ok_or_else(|| CarReadError::InvalidData("payload length overflow".to_string()))?;
    out.reserve(additional);

    while out.len() < target_len {
        let available = match reader.fill_buf() {
            Ok(available) => available,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(CarReadError::Io(err.to_string())),
        };
        if available.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            )
            .into());
        }

        let consumed = (target_len - out.len()).min(available.len());
        out.extend_from_slice(&available[..consumed]);
        reader.consume(consumed);
    }

    Ok(())
}

/// Returns the payload slice from a complete raw CAR entry frame.
///
/// The input must include the entry length varint, 36-byte CID, and payload.
/// This is useful for offset indexes that fetch a single CAR frame directly.
pub fn entry_payload_slice(entry: &[u8]) -> CarReadResult<&[u8]> {
    let mut cursor = std::io::Cursor::new(entry);
    let (entry_len, varint_len) = match read_uvarint64_with_len(&mut cursor) {
        Ok(value) => value,
        Err(CarReadError::Eof) => {
            return Err(CarReadError::UnexpectedEof("empty CAR entry".to_string()));
        }
        Err(err) => return Err(err),
    };
    let entry_len = usize::try_from(entry_len)
        .map_err(|_| CarReadError::InvalidData("entry length exceeds usize".to_string()))?;
    let total_len = varint_len
        .checked_add(entry_len)
        .ok_or_else(|| CarReadError::InvalidData("entry size overflow".to_string()))?;
    if total_len != entry.len() {
        return Err(CarReadError::InvalidData(format!(
            "entry length mismatch: header says {total_len} bytes, fetched {}",
            entry.len()
        )));
    }
    if entry_len < CAR_CID_LEN {
        return Err(CarReadError::InvalidData(format!(
            "invalid entry len {entry_len}"
        )));
    }
    Ok(&entry[varint_len + CAR_CID_LEN..total_len])
}

/// Reads a uvarint64 without recording bytes.
pub fn read_uvarint64<R: BufRead>(r: &mut R) -> CarReadResult<u64> {
    read_uvarint64_with_len(r).map(|(value, _)| value)
}

/// Reads a uvarint64 and returns `(value, encoded_bytes)`.
pub fn read_uvarint64_with_bytes<R: BufRead>(r: &mut R) -> CarReadResult<(u64, Vec<u8>)> {
    let mut x: u64 = 0;
    let mut shift: u32 = 0;
    let mut i: usize = 0;
    let mut bytes = Vec::with_capacity(MAX_UVARINT_LEN_64);

    loop {
        if i >= MAX_UVARINT_LEN_64 {
            return Err(CarReadError::VarintOverflow("uvarint overflow".to_string()));
        }

        let buf = r.fill_buf().map_err(|e| CarReadError::Io(e.to_string()))?;
        if buf.is_empty() {
            if i != 0 {
                return Err(CarReadError::UnexpectedEof(
                    "EOF while reading uvarint".to_string(),
                ));
            }
            return Err(CarReadError::Eof);
        }

        let byte = buf[0];
        bytes.push(byte);
        r.consume(1);
        i += 1;

        if byte < 0x80 {
            if i == MAX_UVARINT_LEN_64 && byte > 1 {
                return Err(CarReadError::VarintOverflow("uvarint overflow".to_string()));
            }
            x |= (byte as u64) << shift;
            return Ok((x, bytes));
        }

        x |= ((byte & 0x7f) as u64) << shift;
        shift += 7;

        if shift > 63 {
            return Err(CarReadError::VarintOverflow("uvarint overflow".to_string()));
        }
    }
}

/// Reads a uvarint64 and returns `(value, consumed_len)`.
pub fn read_uvarint64_with_len<R: BufRead>(r: &mut R) -> CarReadResult<(u64, usize)> {
    let mut x: u64 = 0;
    let mut shift: u32 = 0;
    let mut i: usize = 0;

    loop {
        if i >= MAX_UVARINT_LEN_64 {
            return Err(CarReadError::VarintOverflow("uvarint overflow".to_string()));
        }

        let buf = r.fill_buf().map_err(|e| CarReadError::Io(e.to_string()))?;
        if buf.is_empty() {
            if i != 0 {
                return Err(CarReadError::UnexpectedEof(
                    "EOF while reading uvarint".to_string(),
                ));
            }
            return Err(CarReadError::Eof);
        }

        let mut consumed = 0usize;

        for &byte in buf {
            consumed += 1;
            i += 1;

            if byte < 0x80 {
                if i == MAX_UVARINT_LEN_64 && byte > 1 {
                    return Err(CarReadError::VarintOverflow("uvarint overflow".to_string()));
                }
                x |= (byte as u64) << shift;
                r.consume(consumed);
                return Ok((x, i));
            }

            x |= ((byte & 0x7f) as u64) << shift;
            shift += 7;

            if shift > 63 {
                r.consume(consumed);
                return Err(CarReadError::VarintOverflow("uvarint too long".to_string()));
            }

            if i >= MAX_UVARINT_LEN_64 {
                r.consume(consumed);
                return Err(CarReadError::VarintOverflow("uvarint overflow".to_string()));
            }
        }

        r.consume(consumed);
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, BufReader, Cursor, Read};

    use super::{
        CarBlockReader, CarPayloadRead, DecodedPayloadSource, LosslessBlockReadStats,
        append_exact_from_bufread, entry_payload_slice, read_uvarint64_with_bytes,
        read_uvarint64_with_len,
    };
    use crate::error::CarReadError;
    use crate::reconstruct::RawNode;

    fn framing_car(payloads: &[&[u8]]) -> Vec<u8> {
        let mut car = vec![0]; // Empty CAR header for framing-level tests.
        for (index, payload) in payloads.iter().enumerate() {
            let entry_len = 36usize.checked_add(payload.len()).unwrap();
            assert!(
                entry_len < 128,
                "test helper only supports one-byte lengths"
            );
            car.push(entry_len as u8);
            car.extend_from_slice(&[index as u8; 36]);
            car.extend_from_slice(payload);
        }
        car
    }

    fn dataframe_payload(data: &[u8]) -> Vec<u8> {
        let mut encoder = minicbor::Encoder::new(Vec::new());
        encoder.array(5).unwrap();
        encoder.u64(6).unwrap();
        encoder.null().unwrap();
        encoder.null().unwrap();
        encoder.null().unwrap();
        encoder.bytes(data).unwrap();
        encoder.into_writer()
    }

    #[test]
    fn decoded_node_record_reports_lengths_and_uses_supplied_buffer() {
        let payload = dataframe_payload(&[1, 2, 3, 4]);
        let car = framing_car(&[&payload]);
        let mut reader = CarBlockReader::with_capacity(&car[..], 2);
        reader.skip_header().unwrap();
        let mut scratch = Vec::new();
        let supplied = Vec::with_capacity(64);
        let supplied_capacity = supplied.capacity();
        let mut supplied = Some(supplied);
        let mut requested = Vec::new();

        let record = reader
            .read_decoded_node_record_with_scratch(&mut scratch, &mut |len| {
                requested.push(len);
                supplied.take().unwrap()
            })
            .unwrap()
            .unwrap();

        assert_eq!(record.payload_len, payload.len());
        assert_eq!(record.entry_len, 36 + payload.len());
        assert_eq!(record.total_len, car.len() - 1);
        assert_eq!(requested, [4]);
        let RawNode::DataFrame(frame) = record.node else {
            panic!("expected dataframe node");
        };
        assert_eq!(frame.frame.data, [1, 2, 3, 4]);
        assert_eq!(frame.frame.data.capacity(), supplied_capacity);
        assert_eq!(reader.offset, car.len() as u64);
        assert_eq!(reader.entry_index, 1);
    }

    #[test]
    fn decoded_node_record_uses_direct_buffer_when_payload_is_available() {
        let payload = dataframe_payload(&[1, 2, 3, 4]);
        let car = framing_car(&[&payload]);
        let mut reader = CarBlockReader::with_capacity(&car[..], car.len());
        reader.skip_header().unwrap();
        let mut scratch = vec![0xaa; 16];
        let mut take_data_buffer = |len| Vec::with_capacity(len);

        let (record, source) = reader
            .read_decoded_node_record_with_scratch_tracked(&mut scratch, &mut take_data_buffer)
            .unwrap()
            .unwrap();

        assert_eq!(source, DecodedPayloadSource::DirectBuffer);
        assert!(scratch.is_empty());
        assert_eq!(record.payload_len, payload.len());
        assert_eq!(reader.offset, car.len() as u64);
        assert_eq!(reader.entry_index, 1);
        let RawNode::DataFrame(frame) = record.node else {
            panic!("expected dataframe node");
        };
        assert_eq!(frame.frame.data, [1, 2, 3, 4]);
    }

    #[test]
    fn direct_and_split_decodes_have_exact_node_and_offset_parity() {
        let first = dataframe_payload(&[1, 2, 3, 4]);
        let second = dataframe_payload(&[5, 6, 7]);
        let car = framing_car(&[&first, &second]);
        let mut direct = CarBlockReader::with_capacity(&car[..], car.len());
        let mut split = CarBlockReader::with_capacity(&car[..], 2);
        direct.skip_header().unwrap();
        split.skip_header().unwrap();
        let mut direct_scratch = Vec::new();
        let mut split_scratch = Vec::new();

        for expected_index in 0..2 {
            let mut direct_buffer = |len| Vec::with_capacity(len);
            let (direct_record, direct_source) = direct
                .read_decoded_node_record_with_scratch_tracked(
                    &mut direct_scratch,
                    &mut direct_buffer,
                )
                .unwrap()
                .unwrap();
            let mut split_buffer = |len| Vec::with_capacity(len);
            let (split_record, split_source) = split
                .read_decoded_node_record_with_scratch_tracked(
                    &mut split_scratch,
                    &mut split_buffer,
                )
                .unwrap()
                .unwrap();

            assert_eq!(direct_source, DecodedPayloadSource::DirectBuffer);
            assert_eq!(split_source, DecodedPayloadSource::Scratch);
            assert_eq!(direct_record, split_record);
            assert_eq!(direct_record.node.location().entry_index, expected_index);
            assert_eq!(
                direct_record.node.location().car_offset,
                if expected_index == 0 {
                    1
                } else {
                    (2 + 36 + first.len()) as u64
                }
            );
            assert_eq!(direct.offset, split.offset);
            assert_eq!(direct.entry_index, split.entry_index);
        }

        assert_eq!(direct.offset, car.len() as u64);
        assert_eq!(direct.entry_index, 2);
    }

    #[test]
    fn direct_and_split_decode_errors_consume_the_same_complete_entry() {
        let invalid_payload = [0xff, 0x00, 0x01];
        let car = framing_car(&[&invalid_payload]);

        for capacity in [car.len(), 2] {
            let mut reader = CarBlockReader::with_capacity(&car[..], capacity);
            reader.skip_header().unwrap();
            let mut scratch = Vec::new();
            let mut take_data_buffer = |len| Vec::with_capacity(len);
            let error = reader
                .read_decoded_node_record_with_scratch_tracked(&mut scratch, &mut take_data_buffer)
                .unwrap_err();

            assert!(matches!(error, CarReadError::InvalidData(_)));
            assert_eq!(reader.offset, car.len() as u64);
            assert_eq!(reader.entry_index, 1);
            assert!(
                reader
                    .read_decoded_node_record_with_scratch_tracked(
                        &mut scratch,
                        &mut take_data_buffer,
                    )
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[test]
    fn lossless_stats_count_direct_and_scratch_payloads() {
        let payload = dataframe_payload(&[1, 2, 3, 4]);
        let car = framing_car(&[&payload]);

        for (capacity, expected_source) in [
            (car.len(), DecodedPayloadSource::DirectBuffer),
            (2, DecodedPayloadSource::Scratch),
        ] {
            let mut reader = CarBlockReader::with_capacity(&car[..], capacity);
            reader.skip_header().unwrap();
            let mut scratch = Vec::new();
            let mut take_data_buffer = |len| Vec::with_capacity(len);
            let (record, source) = reader
                .read_decoded_node_record_with_scratch_tracked(&mut scratch, &mut take_data_buffer)
                .unwrap()
                .unwrap();
            assert_eq!(source, expected_source);

            let mut stats = LosslessBlockReadStats::default();
            stats.record(&record, source).unwrap();
            assert_eq!(stats.car_entries, 1);
            assert_eq!(stats.payload_bytes, payload.len() as u64);
            match source {
                DecodedPayloadSource::DirectBuffer => {
                    assert_eq!(stats.direct_buffer_entries, 1);
                    assert_eq!(stats.direct_buffer_payload_bytes, payload.len() as u64);
                    assert_eq!(stats.scratch_entries, 0);
                    assert_eq!(stats.scratch_payload_bytes, 0);
                }
                DecodedPayloadSource::Scratch => {
                    assert_eq!(stats.direct_buffer_entries, 0);
                    assert_eq!(stats.direct_buffer_payload_bytes, 0);
                    assert_eq!(stats.scratch_entries, 1);
                    assert_eq!(stats.scratch_payload_bytes, payload.len() as u64);
                }
            }
        }
    }

    #[test]
    fn partial_zero_payload_varint_is_unexpected_eof() {
        let mut with_len = Cursor::new([0x80]);
        assert!(matches!(
            read_uvarint64_with_len(&mut with_len),
            Err(CarReadError::UnexpectedEof(_))
        ));

        let mut with_bytes = Cursor::new([0x80]);
        assert!(matches!(
            read_uvarint64_with_bytes(&mut with_bytes),
            Err(CarReadError::UnexpectedEof(_))
        ));
    }

    #[test]
    fn entry_payload_slice_extracts_payload() {
        let mut entry = Vec::with_capacity(1 + 36 + 2);
        entry.push(38);
        entry.extend_from_slice(&[0u8; 36]);
        entry.extend_from_slice(&[1u8, 2u8]);

        assert_eq!(entry_payload_slice(&entry).unwrap(), [1u8, 2u8]);
    }

    #[test]
    fn prefix_reader_can_skip_payload_tail() {
        let car = framing_car(&[&[0xaa, 0xbb, 0xcc, 0xdd]]);

        let mut reader = CarBlockReader::with_capacity(&car[..], 16);
        reader.skip_header().unwrap();
        let mut scratch = Vec::new();
        let entry = reader
            .read_entry_payload_if_prefix_with_scratch(&mut scratch, 2, |prefix| {
                assert_eq!(prefix, [0xaa, 0xbb]);
                false
            })
            .unwrap()
            .unwrap();

        assert_eq!(entry.prefix, [0xaa, 0xbb]);
        assert!(entry.payload.is_none());
        assert_eq!(entry.payload_len, 4);
        assert_eq!(entry.total_len, 41);
        assert_eq!(reader.offset, car.len() as u64);
        assert_eq!(reader.entry_index, 1);
    }

    #[test]
    fn prefix_reader_can_materialize_payload_tail() {
        let payload = [0xaau8, 0xbb, 0xcc, 0xdd, 0xee];
        let car = framing_car(&[&payload]);
        let mut reader = CarBlockReader::with_capacity(&car[..], 2);
        reader.skip_header().unwrap();
        let mut scratch = Vec::with_capacity(32);
        let capacity = scratch.capacity();

        let entry = reader
            .read_entry_payload_if_prefix_with_scratch(&mut scratch, 2, |prefix| {
                assert_eq!(prefix, [0xaa, 0xbb]);
                true
            })
            .unwrap()
            .unwrap();

        assert_eq!(entry.prefix, [0xaa, 0xbb]);
        assert_eq!(entry.payload, Some(payload.as_slice()));
        assert_eq!(scratch.capacity(), capacity);
        assert_eq!(reader.offset, car.len() as u64);
        assert_eq!(reader.entry_index, 1);
    }

    #[test]
    fn exact_append_reuses_capacity_across_small_fill_buf_chunks() {
        let input = [1u8, 2, 3, 4, 5];
        let mut reader = BufReader::with_capacity(2, Cursor::new(input));
        let mut out = Vec::with_capacity(32);
        out.extend_from_slice(&[9, 8]);
        let capacity = out.capacity();

        append_exact_from_bufread(&mut reader, &mut out, input.len()).unwrap();

        assert_eq!(out, [9, 8, 1, 2, 3, 4, 5]);
        assert_eq!(out.capacity(), capacity);
    }

    #[test]
    fn exact_append_retries_interrupted_reads() {
        struct InterruptOnce<R> {
            inner: R,
            interrupted: bool,
        }

        impl<R: Read> Read for InterruptOnce<R> {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if !self.interrupted {
                    self.interrupted = true;
                    return Err(io::Error::from(io::ErrorKind::Interrupted));
                }
                self.inner.read(buf)
            }
        }

        let input = [4u8, 3, 2, 1];
        let source = InterruptOnce {
            inner: Cursor::new(input),
            interrupted: false,
        };
        let mut reader = BufReader::with_capacity(2, source);
        let mut out = Vec::new();

        append_exact_from_bufread(&mut reader, &mut out, input.len()).unwrap();

        assert_eq!(out, input);
    }

    #[test]
    fn full_payload_reader_reuses_scratch_capacity() {
        let payload = [0x10u8, 0x20, 0x30, 0x40, 0x50];
        let car = framing_car(&[&payload]);
        let mut reader = CarBlockReader::with_capacity(&car[..], 2);
        reader.skip_header().unwrap();
        let mut scratch = Vec::with_capacity(64);
        scratch.extend_from_slice(&[0xff; 16]);
        let capacity = scratch.capacity();

        let entry = reader
            .read_entry_payload_with_scratch(&mut scratch)
            .unwrap()
            .unwrap();
        assert_eq!(entry.payload, payload);
        assert_eq!(entry.payload_len, payload.len());
        assert_eq!(entry.total_len, 1 + 36 + payload.len());

        assert_eq!(scratch.capacity(), capacity);
        assert_eq!(reader.offset, car.len() as u64);
        assert_eq!(reader.entry_index, 1);
    }

    #[test]
    fn selective_reader_extends_prefix_or_materializes_full_payload() {
        let first = [1u8, 2, 3, 4, 5];
        let second = [6u8, 7, 8, 9];
        let car = framing_car(&[&first, &second]);
        let mut reader = CarBlockReader::with_capacity(&car[..], 2);
        reader.skip_header().unwrap();
        let mut scratch = Vec::with_capacity(64);
        let capacity = scratch.capacity();

        let prefix = reader
            .read_entry_payload_select_with_scratch(&mut scratch, 1, |initial| {
                assert_eq!(initial, [1]);
                CarPayloadRead::Prefix(3)
            })
            .unwrap()
            .unwrap();
        assert_eq!(prefix.prefix, [1, 2, 3]);
        assert!(prefix.payload.is_none());

        let full = reader
            .read_entry_payload_select_with_scratch(&mut scratch, 1, |initial| {
                assert_eq!(initial, [6]);
                CarPayloadRead::Full
            })
            .unwrap()
            .unwrap();
        assert_eq!(full.prefix, second);
        assert_eq!(full.payload, Some(second.as_slice()));

        assert_eq!(scratch.capacity(), capacity);
        assert_eq!(reader.offset, car.len() as u64);
        assert_eq!(reader.entry_index, 2);
    }

    #[test]
    fn truncated_full_payload_preserves_reader_error_and_offset_semantics() {
        let mut car = vec![0, 40]; // Header, then CID + declared four-byte payload.
        car.extend_from_slice(&[0u8; 36]);
        car.extend_from_slice(&[0xaa, 0xbb]);
        let mut reader = CarBlockReader::with_capacity(&car[..], 2);
        reader.skip_header().unwrap();
        let mut scratch = Vec::with_capacity(8);

        let error = match reader.read_entry_payload_with_scratch(&mut scratch) {
            Err(error) => error,
            Ok(_) => panic!("truncated payload unexpectedly succeeded"),
        };

        assert!(matches!(error, CarReadError::Io(_)));
        assert_eq!(reader.offset, 1 + 1 + 36);
        assert_eq!(reader.entry_index, 0);
    }
}
