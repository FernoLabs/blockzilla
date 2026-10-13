//! Portable reader for the current, operator-trusted native projection.
//! Transport admission and SHA-256 checks belong to the JavaScript source.
//! The existing Rust schemas, rather than a second codec, decode the payload.
use anyhow::{Context, Result, ensure};
use blockzilla_archive_v2::*;
use blockzilla_compact::CompactMetaV1;
use blockzilla_primitives::{CompactPubkey, bounded_historical_source_wincode_leb128_config};
use ruzstd::decoding::StreamingDecoder;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    io::{Read, Write},
};

const MAX_FRAME: usize = 16 * 1024 * 1024 + 65_536 + 16;
const MAX_BLOCK: usize = 64 * 1024 * 1024;
const MAX_OUTPUT: usize = 32 * 1024 * 1024;
const MAX_TX: usize = 65_536;
const SAFE_INTEGER: u64 = 9_007_199_254_740_991;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Options {
    #[serde(default)]
    pub programs: Vec<String>,
    #[serde(default)]
    pub accounts: Vec<String>,
    #[serde(default)]
    pub successful_only: bool,
    #[serde(default = "yes")]
    pub include_details: bool,
}
fn yes() -> bool {
    true
}
impl Default for Options {
    fn default() -> Self {
        Self {
            programs: vec![],
            accounts: vec![],
            successful_only: false,
            include_details: true,
        }
    }
}

struct Filter {
    programs: BTreeSet<[u8; 32]>,
    accounts: BTreeSet<[u8; 32]>,
    successful_only: bool,
}
impl Filter {
    fn new(o: &Options) -> Result<Self> {
        fn keys(values: &[String]) -> Result<BTreeSet<[u8; 32]>> {
            ensure!(values.len() <= 256, "too_many_filter_keys");
            values
                .iter()
                .map(|s| {
                    ensure!(s.len() <= 44, "invalid_filter_key");
                    let key: [u8; 32] = bs58::decode(s)
                        .into_vec()?
                        .try_into()
                        .map_err(|_| anyhow::anyhow!("invalid_filter_key"))?;
                    ensure!(bs58::encode(key).into_string() == *s, "invalid_filter_key");
                    Ok(key)
                })
                .collect()
        }
        Ok(Self {
            programs: keys(&o.programs)?,
            accounts: keys(&o.accounts)?,
            successful_only: o.successful_only,
        })
    }
}

#[derive(Deserialize)]
struct Object {
    path: String,
    offset: usize,
    bytes: usize,
}
#[derive(Deserialize)]
struct Envelope {
    profile: String,
    #[serde(default)]
    event: Option<Value>,
    marker: Marker,
    objects: Vec<Object>,
}
#[derive(Deserialize)]
struct Marker {
    epoch: u64,
    session_id: String,
    finalized: bool,
    canonical: bool,
    publication_ready: bool,
    produced_blocks: Vec<Identity>,
}
#[derive(Deserialize)]
struct Identity {
    #[serde(default)]
    bank_id: u64,
    block_id: u32,
    slot: u64,
    parent_slot: u64,
    blockhash: [u8; 32],
    parent_blockhash: [u8; 32],
}

#[derive(Clone, Copy)]
struct IndexRow {
    block_id: u32,
    slot: u64,
    offset: usize,
    compressed: usize,
    uncompressed: usize,
    transactions: usize,
    first_tx: usize,
    first_signature: usize,
    signatures: usize,
}
fn u32_at(bytes: &[u8], at: usize) -> Result<u32> {
    Ok(u32::from_le_bytes(
        bytes
            .get(at..at + 4)
            .context("truncated_index")?
            .try_into()?,
    ))
}
fn u64_at(bytes: &[u8], at: usize) -> Result<u64> {
    Ok(u64::from_le_bytes(
        bytes
            .get(at..at + 8)
            .context("truncated_index")?
            .try_into()?,
    ))
}
fn index(bytes: &[u8], blob_len: usize) -> Result<(u32, Vec<IndexRow>)> {
    ensure!(
        bytes.get(..8) == Some(b"BZV2HIX1"),
        "invalid_hot_index_magic"
    );
    ensure!(
        bytes.get(8..12) == Some(&[1, 0, 0, 0]),
        "unsupported_hot_index_version"
    );
    let n = usize::try_from(u64_at(bytes, 12)?)?;
    ensure!(
        (1..=32).contains(&n) && bytes.len() == 36 + n * 52,
        "invalid_hot_index_length"
    );
    ensure!(
        u64_at(bytes, 20)? == blob_len as u64,
        "hot_blob_length_mismatch"
    );
    let flags = u32_at(bytes, 32)?;
    ensure!(
        flags == 0 || flags == 2,
        "unsupported_zstd_dictionary_or_flags"
    );
    let mut rows = Vec::with_capacity(n);
    let (mut offset, mut tx, mut sig) = (0, 0, 0);
    for b in bytes[36..].chunks_exact(52) {
        let r = IndexRow {
            block_id: u32_at(b, 0)?,
            slot: u64_at(b, 4)?,
            offset: usize::try_from(u64_at(b, 12)?)?,
            compressed: u32_at(b, 20)? as usize,
            uncompressed: u32_at(b, 24)? as usize,
            transactions: u32_at(b, 28)? as usize,
            first_tx: usize::try_from(u64_at(b, 32)?)?,
            first_signature: usize::try_from(u64_at(b, 40)?)?,
            signatures: u32_at(b, 48)? as usize,
        };
        ensure!(
            r.block_id as usize == rows.len()
                && r.offset == offset
                && r.first_tx == tx
                && r.first_signature == sig,
            "non_contiguous_hot_index"
        );
        ensure!(
            r.compressed > 0
                && r.uncompressed > 0
                && r.uncompressed <= MAX_BLOCK
                && r.transactions <= MAX_TX,
            "block_limit_exceeded"
        );
        offset = offset.checked_add(r.compressed).context("index_overflow")?;
        tx = tx.checked_add(r.transactions).context("index_overflow")?;
        sig = sig.checked_add(r.signatures).context("index_overflow")?;
        ensure!(offset <= blob_len, "hot_range_outside_blob");
        rows.push(r);
    }
    ensure!(offset == blob_len, "unindexed_hot_bytes");
    Ok((flags, rows))
}
fn key(k: &CompactPubkey, registry: &[u8]) -> Result<[u8; 32]> {
    match k {
        CompactPubkey::Raw(k) => Ok(*k),
        CompactPubkey::Id(id) => {
            ensure!(*id > 0, "zero_registry_id");
            let at = (*id as usize - 1)
                .checked_mul(32)
                .context("registry_overflow")?;
            Ok(registry
                .get(at..at + 32)
                .context("registry_id_outside_dictionary")?
                .try_into()?)
        }
    }
}
fn base58(k: &[u8]) -> String {
    bs58::encode(k).into_string()
}
fn exact<
    'a,
    T: wincode::SchemaRead<
            'a,
            blockzilla_primitives::BoundedHistoricalSourceWincodeLeb128Config<1_048_576>,
            Dst = T,
        >,
>(
    bytes: &'a [u8],
) -> Result<T> {
    Ok(wincode::config::deserialize_exact::<T, _>(
        bytes,
        bounded_historical_source_wincode_leb128_config::<1_048_576>(),
    )?)
}

/// Return semantic compact values. This is not an RPC JSON or exact signed-wire renderer.
pub fn decode(wire: &[u8], options: &Options) -> Result<Vec<u8>> {
    decode_projection(wire, options, false, false)
}

/// Opt-in v2 entry point. Controls carry identity and native expiry, never a
/// second block. The v1 entry points continue to reject this profile.
pub fn decode_banks(wire: &[u8], options: &Options) -> Result<Vec<u8>> {
    let frame = blockzilla_bank_stream_v2::decode(wire.to_vec().into())?;
    if frame.envelope.event.kind != "block" {
        return Ok(serde_json::to_vec(
            &json!({"profile":frame.envelope.profile,"event":frame.envelope.event}),
        )?);
    }
    let mut value: Value = serde_json::from_slice(&decode_projection(wire, options, false, true)?)?;
    ensure!(
        value["blocks"].as_array().is_some_and(|b| b.len() == 1)
            && value["blocks"][0]["blockHeight"]
                == frame
                    .envelope
                    .event
                    .block_height
                    .context("missing_height")?,
        "native_payload_height_mismatch"
    );
    value["event"] = serde_json::to_value(&frame.envelope.event)?;
    Ok(serde_json::to_vec(&value)?)
}

/// Native consumer projection. Uses the same typed metadata and key resolver,
/// and emits exact recorded token balances without expanding messages or logs.
#[cfg(not(target_arch = "wasm32"))]
pub fn decode_token_balances(wire: &[u8]) -> Result<Vec<u8>> {
    decode_projection(
        wire,
        &Options {
            include_details: false,
            ..Options::default()
        },
        true,
        false,
    )
}

fn decode_projection(
    wire: &[u8],
    options: &Options,
    token_only: bool,
    bank_v2: bool,
) -> Result<Vec<u8>> {
    ensure!(
        wire.len() >= 16 && wire.len() <= MAX_FRAME && &wire[..8] == b"BZSTRM01",
        "invalid_transport_frame"
    );
    let meta = u32_at(wire, 8)? as usize;
    let payload_len = u32_at(wire, 12)? as usize;
    ensure!(
        meta > 0 && meta <= 65_536 && wire.len() == 16 + meta + payload_len,
        "invalid_transport_length"
    );
    let e: Envelope = serde_json::from_slice(&wire[16..16 + meta])?;
    ensure!(
        !token_only || (e.profile == "native-compact-projection-v1" && e.marker.finalized),
        "processed_token_index_requires_fork_undo"
    );
    ensure!(
        ((e.profile == "native-compact-projection-v1" && e.marker.finalized)
            || (e.profile == "native-processed-compact-v1"
                && !e.marker.finalized
                && e.event.as_ref().is_some_and(|v| v["kind"] == "block"))
            || (bank_v2
                && e.profile == "native-bank-compact-v2"
                && !e.marker.finalized
                && e.event.as_ref().is_some_and(|v| v["kind"] == "block")))
            && !e.marker.canonical
            && !e.marker.publication_ready,
        "unsupported_projection"
    );
    let payload = &wire[16 + meta..];
    let object = |name: &str| -> Result<&[u8]> {
        let mut matches = e.objects.iter().filter(|o| o.path == name);
        let o = matches
            .next()
            .with_context(|| format!("missing_object:{name}"))?;
        ensure!(matches.next().is_none(), "duplicate_object");
        let end = o.offset.checked_add(o.bytes).context("object_overflow")?;
        payload.get(o.offset..end).context("object_outside_payload")
    };
    let blob = object("archive-v2-blocks.zstd")?;
    let (flags, rows) = index(object("archive-v2-blocks.index")?, blob.len())?;
    let registry = object("registry.bin")?;
    let blockhashes = object("blockhash_registry.bin")?;
    let parent_tail = object("prev_blockhash_tail.bin")?;
    let signatures = object("signatures.bin")?;
    ensure!(
        registry.len().is_multiple_of(32)
            && blockhashes.len().is_multiple_of(32)
            && signatures.len().is_multiple_of(64),
        "invalid_dictionary_or_signatures"
    );
    ensure!(
        rows.len() == e.marker.produced_blocks.len(),
        "block_identity_count_mismatch"
    );
    // Native batches use produced-only zero-based hashes. The first predecessor
    // is the separate hash+slot tail, not a boundary prefix in this registry.
    ensure!(
        blockhashes.len() == rows.len() * 32 && parent_tail.len() == 40,
        "invalid_native_blockhash_layout"
    );
    ensure!(
        parent_tail[..32] == e.marker.produced_blocks[0].parent_blockhash
            && u64_at(parent_tail, 32)? == e.marker.produced_blocks[0].parent_slot,
        "native_parent_tail_mismatch"
    );
    let last = rows.last().unwrap();
    ensure!(
        last.first_signature + last.signatures == signatures.len() / 64,
        "signature_file_count_mismatch"
    );
    let filter = Filter::new(options)?;
    let mut blocks = Vec::with_capacity(rows.len());
    let mut total = 0;
    for (r, identity) in rows.iter().zip(&e.marker.produced_blocks) {
        ensure!(
            r.slot <= SAFE_INTEGER && r.slot == identity.slot && r.block_id == identity.block_id,
            "index_identity_mismatch"
        );
        let input = &blob[r.offset..r.offset + r.compressed];
        let mut decoded = Vec::with_capacity(r.uncompressed.min(1_048_576));
        if flags == 2 {
            decoded.extend_from_slice(input);
        } else {
            let mut zstd = StreamingDecoder::new(input).context("zstd_header")?;
            (&mut zstd)
                .take(r.uncompressed as u64 + 1)
                .read_to_end(&mut decoded)
                .context("zstd_decode")?;
            ensure!(zstd.into_inner().is_empty(), "trailing_zstd_bytes");
        }
        ensure!(
            decoded.len() == r.uncompressed,
            "decoded_block_length_mismatch"
        );
        let block =
            deserialize_archive_v2_hot_block_blob_borrowed_current_with_preallocation_limit::<
                1_048_576,
            >(&decoded)?;
        ensure!(
            block.header.slot == r.slot
                && block.header.parent_slot == identity.parent_slot
                && block.tx_count as usize == r.transactions
                && block.tx_rows_len() == r.transactions,
            "decoded_block_identity_mismatch"
        );
        ensure!(
            block.header.blockhash_id == r.block_id
                && block.header.previous_blockhash_id == r.block_id.saturating_sub(1),
            "native_blockhash_id_mismatch"
        );
        let start = r.block_id as usize * 32;
        ensure!(
            blockhashes.get(start..start + 32) == Some(identity.blockhash.as_slice()),
            "decoded_blockhash_mismatch"
        );
        if r.block_id > 0 {
            ensure!(
                blockhashes.get(start - 32..start) == Some(identity.parent_blockhash.as_slice()),
                "decoded_parent_blockhash_mismatch"
            );
        }
        let mut transactions = Vec::new();
        let mut signature_at = r.first_signature * 64;
        let mut message_at = 0usize;
        let mut metadata_at = 0usize;
        for (tx_index, row) in block.tx_rows().enumerate() {
            ensure!(
                row.tx_index as usize == tx_index
                    && row.reserved == [0; 3]
                    && row.flags & !2047 == 0,
                "invalid_transaction_row"
            );
            ensure!(
                row.flags
                    & (ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK
                        | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK)
                    == 0,
                "raw_fallback_not_supported"
            );
            ensure!(
                row.message_offset as usize == message_at
                    && row.metadata_offset as usize == metadata_at,
                "non_contiguous_transaction_regions"
            );
            message_at = message_at
                .checked_add(row.message_len as usize)
                .context("message_overflow")?;
            metadata_at = metadata_at
                .checked_add(row.metadata_len as usize)
                .context("metadata_overflow")?;
            let message: ArchiveV2HotMessagePayload = exact(
                block
                    .message_bytes
                    .get(row.message_offset as usize..message_at)
                    .context("message_outside_block")?,
            )?;
            let metadata: Option<CompactMetaV1> =
                if row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0 {
                    Some(exact(
                        block
                            .metadata_bytes
                            .get(row.metadata_offset as usize..metadata_at)
                            .context("metadata_outside_block")?,
                    )?)
                } else {
                    ensure!(
                        row.metadata_len == 0 && row.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR == 0,
                        "missing_metadata"
                    );
                    None
                };
            let (header, static_keys, instructions, expected_loaded, is_v0) = match &message {
                ArchiveV2HotMessagePayload::Legacy(m) => {
                    (&m.header, &m.account_keys, &m.instructions, (0, 0), false)
                }
                ArchiveV2HotMessagePayload::V0(m) => (
                    &m.header,
                    &m.account_keys,
                    &m.instructions,
                    (
                        m.address_table_lookups
                            .iter()
                            .map(|l| l.writable_indexes.len())
                            .sum(),
                        m.address_table_lookups
                            .iter()
                            .map(|l| l.readonly_indexes.len())
                            .sum(),
                    ),
                    true,
                ),
                ArchiveV2HotMessagePayload::V1(m) => {
                    (&m.header, &m.account_keys, &m.instructions, (0, 0), false)
                }
            };
            ensure!(
                header.num_required_signatures == row.signature_count
                    && row.signature_count > 0
                    && usize::from(row.signature_count) <= static_keys.len(),
                "signature_header_mismatch"
            );
            ensure!(
                header.num_readonly_signed_accounts <= header.num_required_signatures
                    && usize::from(header.num_readonly_unsigned_accounts)
                        <= static_keys.len() - usize::from(header.num_required_signatures),
                "invalid_account_header"
            );
            ensure!(
                is_v0 == (row.flags & ARCHIVE_V2_TX_FLAG_MESSAGE_V0 != 0),
                "message_version_flag_mismatch"
            );
            ensure!(
                (expected_loaded != (0, 0))
                    == (row.flags & ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES != 0),
                "loaded_address_flag_mismatch"
            );
            let mut accounts = static_keys
                .iter()
                .map(|k| key(k, registry))
                .collect::<Result<Vec<_>>>()?;
            if let Some(m) = &metadata {
                ensure!(
                    (
                        m.loaded_writable_addresses.len(),
                        m.loaded_readonly_addresses.len()
                    ) == expected_loaded,
                    "loaded_address_count_mismatch"
                );
                ensure!(
                    m.err.is_some() == (row.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0),
                    "execution_status_flag_mismatch"
                );
                ensure!(
                    m.inner_instructions.is_some()
                        == (row.flags & ARCHIVE_V2_TX_FLAG_HAS_INNER_IX != 0),
                    "inner_instruction_flag_mismatch"
                );
                for k in m
                    .loaded_writable_addresses
                    .iter()
                    .chain(&m.loaded_readonly_addresses)
                {
                    accounts.push(key(k, registry)?);
                }
            } else {
                ensure!(expected_loaded == (0, 0), "loaded_addresses_unavailable");
            }
            ensure!(
                accounts.len() <= 256 && instructions.len() <= 65_536,
                "transaction_limit_exceeded"
            );
            if let Some(m) = &metadata {
                ensure!(
                    m.pre_balances.len() == accounts.len()
                        && m.post_balances.len() == accounts.len(),
                    "balance_account_count_mismatch"
                );
            }
            let mut top = BTreeSet::new();
            let mut inner = BTreeSet::new();
            for ix in instructions {
                top.insert(
                    *accounts
                        .get(ix.program_id_index as usize)
                        .context("invalid_program_index")?,
                );
                ensure!(
                    ix.accounts.iter().all(|i| (*i as usize) < accounts.len()),
                    "invalid_instruction_account_index"
                );
            }
            if let Some(groups) = metadata
                .as_ref()
                .and_then(|m| m.inner_instructions.as_ref())
            {
                for group in groups {
                    ensure!(
                        (group.index as usize) < instructions.len(),
                        "invalid_inner_instruction_group"
                    );
                    for ix in &group.instructions {
                        inner.insert(
                            *accounts
                                .get(ix.program_id_index as usize)
                                .context("invalid_cpi_program_index")?,
                        );
                        ensure!(
                            ix.accounts.iter().all(|i| (*i as usize) < accounts.len()),
                            "invalid_cpi_account_index"
                        );
                    }
                }
            }
            let success = metadata.as_ref().map(|m| m.err.is_none());
            let end_signature = signature_at
                .checked_add(row.signature_count as usize * 64)
                .context("signature_overflow")?;
            let sig = signatures
                .get(signature_at..end_signature)
                .context("signature_outside_file")?;
            signature_at = end_signature;
            let any_filter = !filter.programs.is_empty() || !filter.accounts.is_empty();
            let matched = !any_filter
                || top
                    .iter()
                    .chain(&inner)
                    .any(|p| filter.programs.contains(p))
                || accounts.iter().any(|a| filter.accounts.contains(a));
            if !matched || (filter.successful_only && success != Some(true)) {
                continue;
            }
            let mut tx = if token_only {
                json!({"index":tx_index,"signature":base58(&sig[..64]),"success":success})
            } else {
                json!({"index":tx_index,"signature":base58(&sig[..64]),"success":success,
                "innerCoverage": if metadata.as_ref().is_some_and(|m| m.inner_instructions.is_some()) {"recorded"} else {"unavailable"},
                "signatures":sig.chunks_exact(64).map(base58).collect::<Vec<_>>(),
                "topLevelPrograms":top.iter().map(|k| base58(k)).collect::<Vec<_>>(),
                "innerPrograms":inner.iter().map(|k| base58(k)).collect::<Vec<_>>()})
            };
            if token_only {
                let m = metadata.as_ref().context("token_metadata_unavailable")?;
                let mut balances = Vec::new();
                for (side, rows) in [
                    ("pre", &m.pre_token_balances),
                    ("post", &m.post_token_balances),
                ] {
                    for (index, b) in rows.iter().enumerate() {
                        let resolve = |k: &Option<CompactPubkey>| -> Result<Option<String>> {
                            k.as_ref()
                                .map(|k| key(k, registry).map(|k| base58(&k)))
                                .transpose()
                        };
                        let account = accounts
                            .get(b.account_index as usize)
                            .context("invalid_token_account_index")?;
                        balances.push(json!({"side":side,"balanceIndex":index,"accountIndex":b.account_index,
                            "account":base58(account),"mint":resolve(&b.mint)?,"owner":resolve(&b.owner)?,
                            "tokenProgram":resolve(&b.program_id)?,"amount":b.amount.to_string(),"decimals":b.decimals}));
                    }
                }
                tx["tokenBalances"] = json!(balances);
            }
            if options.include_details {
                tx["accountKeys"] = json!(accounts.iter().map(|k| base58(k)).collect::<Vec<_>>());
                tx["message"] = serde_json::to_value(&message)?;
                tx["metadata"] = serde_json::to_value(&metadata)?;
            }
            if bank_v2 {
                tx["err"] = serde_json::to_value(metadata.as_ref().and_then(|m| m.err.as_ref()))?;
            }
            transactions.push(tx);
        }
        ensure!(
            signature_at == (r.first_signature + r.signatures) * 64
                && message_at == block.message_bytes.len()
                && metadata_at == block.metadata_bytes.len(),
            "transaction_regions_or_signatures_mismatch"
        );
        total += r.transactions;
        let mut result = json!({"slot":r.slot,"parentSlot":identity.parent_slot,"blockhash":base58(&identity.blockhash),
            "parentBlockhash":base58(&identity.parent_blockhash),"bankId":identity.bank_id,"finality":if e.marker.finalized {"finalized"}else{"processed"},"transactionCount":r.transactions,
            "transactions":transactions});
        if bank_v2 {
            result["blockHeight"] = json!(block.header.block_height);
        }
        blocks.push(result);
    }
    let mut value = json!({"profile":e.profile,"epoch":e.marker.epoch,"session":e.marker.session_id,
        "canonical":false,"publicationReady":false,"transactionCount":total,"blocks":blocks});
    preserve_large_integers(&mut value);
    let mut out = BoundedOutput(Vec::new());
    serde_json::to_writer(&mut out, &value)?;
    Ok(out.0)
}
fn preserve_large_integers(v: &mut Value) {
    match v {
        Value::Number(n)
            if n.as_u64().is_some_and(|v| v > SAFE_INTEGER)
                || n.as_i64().is_some_and(|v| v < -(SAFE_INTEGER as i64)) =>
        {
            *v = Value::String(n.to_string())
        }
        Value::Array(values) => values.iter_mut().for_each(preserve_large_integers),
        Value::Object(values) => values.values_mut().for_each(preserve_large_integers),
        _ => {}
    }
}
struct BoundedOutput(Vec<u8>);
impl Write for BoundedOutput {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if data.len() > MAX_OUTPUT.saturating_sub(self.0.len()) {
            return Err(std::io::Error::other("decoded_output_limit_exceeded"));
        }
        self.0.extend_from_slice(data);
        Ok(data.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// Tiny owned-buffer ABI. The SDK allocates and frees every buffer in finally.
// This needs no generated JavaScript runtime or host imports.
#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub extern "C" fn bz_alloc(len: u32) -> u32 {
    Box::into_raw(vec![0u8; len as usize].into_boxed_slice()) as *mut u8 as u32
}
#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bz_free(ptr: u32, len: u32) {
    unsafe {
        drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
            ptr as *mut u8,
            len as usize,
        )));
    }
}
#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bz_decode(ptr: u32, len: u32, opts_ptr: u32, opts_len: u32) -> u64 {
    let result = (|| -> Result<Vec<u8>> {
        ensure!(
            len as usize <= MAX_FRAME && opts_len <= 65_536,
            "decoder_input_limit_exceeded"
        );
        let bytes = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
        let options: Options = serde_json::from_slice(unsafe {
            std::slice::from_raw_parts(opts_ptr as *const u8, opts_len as usize)
        })?;
        decode(bytes, &options)
    })();
    let bytes =
        result.unwrap_or_else(|e| serde_json::to_vec(&json!({"error":format!("{e:#}")})).unwrap());
    let len = bytes.len() as u64;
    let ptr = Box::into_raw(bytes.into_boxed_slice()) as *mut u8 as u32;
    (len << 32) | u64::from(ptr)
}

#[cfg(test)]
mod tests;

// Separate v2 exports. The original bz_decode admission stays unchanged.
#[cfg(target_arch = "wasm32")]
fn bank_output(result: Result<Vec<u8>>) -> u64 {
    let bytes =
        result.unwrap_or_else(|e| serde_json::to_vec(&json!({"error":format!("{e:#}")})).unwrap());
    let len = bytes.len() as u64;
    let ptr = Box::into_raw(bytes.into_boxed_slice()) as *mut u8 as u32;
    (len << 32) | u64::from(ptr)
}
#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bz_decode_banks(ptr: u32, len: u32, opts_ptr: u32, opts_len: u32) -> u64 {
    bank_output((|| {
        ensure!(
            len as usize <= MAX_FRAME && opts_len <= 65_536,
            "decoder_input_limit_exceeded"
        );
        let bytes = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
        let options: Options = serde_json::from_slice(unsafe {
            std::slice::from_raw_parts(opts_ptr as *const u8, opts_len as usize)
        })?;
        decode_banks(bytes, &options)
    })())
}
#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bz_validate_bank_head(
    ptr: u32,
    len: u32,
    opts_ptr: u32,
    opts_len: u32,
) -> u64 {
    bank_output((|| {
        ensure!(
            len <= 65_536 && opts_len <= 65_536,
            "head_input_limit_exceeded"
        );
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Config {
            source: blockzilla_bank_stream_v2::SourceIdentity,
            now: u64,
        }
        let config: Config = serde_json::from_slice(unsafe {
            std::slice::from_raw_parts(opts_ptr as *const u8, opts_len as usize)
        })?;
        let head: blockzilla_bank_stream_v2::Head = serde_json::from_slice(unsafe {
            std::slice::from_raw_parts(ptr as *const u8, len as usize)
        })?;
        head.validate(
            &config.source,
            config.now,
            blockzilla_bank_stream_v2::DEFAULT_MAX_LAG_MS,
        )?;
        Ok(serde_json::to_vec(&head)?)
    })())
}
