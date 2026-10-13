//! Finite current-schema execution normalization. No canonical publication authority.
//! Raw current envelopes remain authoritative for fields not present in Compact V2.
use crate::grpc_raw::current::{self, proto::prelude as pb};
use anyhow::{Context, Result, ensure};
use blockzilla_archive_v2::*;
use blockzilla_compact::{
    CompactBlockHeader, CompactInnerInstruction, CompactInnerInstructions, CompactMessageHeader,
    CompactPohEntry, CompactTransactionConfig, CompactTransactionError,
};
use blockzilla_live_format::execution_canary_v1::*;
use blockzilla_primitives::{WincodeLeb128FramedWriter, encode_with_scratch};
use prost::Message as _;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{BufWriter, Read, Write},
    path::Path,
};

const MAX_CANARY_BLOCKS: usize = 4096;
const JSON_LIMIT: u64 = 16 << 20;

pub fn normalize_current_execution(
    input: &Path,
    coverage_path: &Path,
    output: &Path,
    max_record_bytes: u64,
    max_blocks: usize,
) -> Result<()> {
    ensure!(
        (1..=MAX_CANARY_BLOCKS).contains(&max_blocks),
        "canary block limit must be 1..=4096"
    );
    ensure!(
        (1..=(256 << 20)).contains(&max_record_bytes),
        "record limit must be 1..=256 MiB"
    );
    ensure!(!output.exists(), "output already exists");
    let coverage_bytes = bounded_read(coverage_path, JSON_LIMIT)?;
    let coverage: FinalizedRpcCoverageV1 = serde_json::from_slice(&coverage_bytes)?;
    validate_coverage(&coverage, max_blocks)?;
    let rpc_path = coverage_path
        .parent()
        .context("coverage has no parent")?
        .join("rpc-get-blocks.json");
    let rpc_bytes = bounded_read(&rpc_path, JSON_LIMIT)?;
    ensure!(
        hash_bytes(&rpc_bytes) == coverage.rpc_response_sha256,
        "RPC reply digest mismatch"
    );
    let rpc: serde_json::Value = serde_json::from_slice(&rpc_bytes)?;
    ensure!(rpc.get("error").is_none(), "RPC reply contains an error");
    let rpc_slots: Vec<u64> = serde_json::from_value(
        rpc.get("result")
            .context("RPC reply missing result")?
            .clone(),
    )?;
    ensure!(
        rpc_slots == coverage.produced_slots,
        "RPC reply differs from coverage slots"
    );
    let seal_path = input.join("CURRENT-GRPC-SEALED.v1.json");
    let seal_bytes = bounded_read(&seal_path, JSON_LIMIT)?;
    let seal_sha = hash_bytes(&seal_bytes);
    ensure!(
        seal_sha == coverage.source_receipt_sha256,
        "coverage is bound to a different source seal"
    );
    let seal_value: serde_json::Value = serde_json::from_slice(&seal_bytes)?;
    ensure!(
        seal_value
            .get("requested_commitment")
            .and_then(|v| v.as_str())
            == Some("finalized"),
        "source seal does not claim a finalized subscription"
    );
    ensure!(
        seal_value.get("source_profile").and_then(|v| v.as_str()) == Some(EXECUTION_SOURCE_PROFILE),
        "source seal has the wrong source profile"
    );
    let identity = seal_value
        .get("validator_identity")
        .context("source lacks validator RPC identity")?;
    ensure!(
        identity.get("genesis_hash").and_then(|v| v.as_str()) == Some(&coverage.genesis_hash)
            && identity.get("node_identity").and_then(|v| v.as_str())
                == Some(&coverage.node_identity),
        "coverage and capture validator identity differ"
    );
    ensure!(
        fs::metadata(input.join("events.wal"))?.len() <= 8u64 << 30,
        "canary source exceeds 8 GiB"
    );
    let parent = output.parent().context("output has no parent")?;
    let name = output
        .file_name()
        .context("output has no filename")?
        .to_string_lossy();
    let stage = parent.join(format!(".{name}.normalizing"));
    fs::create_dir(&stage)
        .context("create fresh normalization stage; an old stage must be inspected first")?;
    fs::create_dir(stage.join("parts"))?;
    fs::create_dir(stage.join("blocks"))?;
    fs::create_dir(stage.join("index"))?;
    fs::create_dir(stage.join("evidence"))?;
    let mut rows = BTreeMap::new();
    let mut invalidated = BTreeSet::new();
    current::read_sealed_current_events(input, max_record_bytes, |source, update| {
        if let Some((slot, bank)) = invalidated_bank(&update) {
            if (coverage.start_slot..coverage.end_slot_exclusive).contains(&slot) {
                invalidated.insert((slot, bank));
                ensure!(
                    invalidated.len() <= 1_000_000,
                    "too many in-range bank invalidations"
                );
            }
        }
        let Some(pb::subscribe_update::UpdateOneof::Block(block)) = update.update_oneof else {
            return Ok(());
        };
        if block.slot < coverage.start_slot || block.slot >= coverage.end_slot_exclusive {
            return Ok(());
        }
        let index = coverage
            .produced_slots
            .binary_search(&block.slot)
            .map_err(|_| {
                anyhow::anyhow!(
                    "captured slot {} absent from finalized RPC coverage",
                    block.slot
                )
            })?;
        ensure!(
            !rows.contains_key(&block.slot),
            "duplicate block observation at slot {}; select exact evidence before normalization",
            block.slot
        );
        let (normalized, poh, signatures) = convert_block(&block, u32::try_from(index)?)?;
        let row = ExecutionBlockV1 {
            block_id: u32::try_from(index)?,
            slot: block.slot,
            bank_id: block.bank_id,
            blockhash: hash_string(&block.blockhash)?,
            parent_slot: block.parent_slot,
            parent_blockhash: hash_string(&block.parent_blockhash)?,
            transactions: block.executed_transaction_count,
            signatures: signatures.len() as u64 / 64,
            entries: block.entries_count,
            source_sequence: source.sequence,
            source_raw_sha256: source.raw_sha256,
        };
        let mut bytes = Vec::new();
        encode_with_scratch(&normalized, &mut bytes)?;
        ensure!(bytes.len() <= 256 << 20, "normalized frame exceeds bound");
        let mut writer = WincodeLeb128FramedWriter::new(BufWriter::new(create(
            &stage.join(format!("parts/{index}.block")),
        )?));
        writer.write_bytes(&bytes)?;
        writer.flush()?;
        let mut writer = WincodeLeb128FramedWriter::new(BufWriter::new(create(
            &stage.join(format!("parts/{index}.poh")),
        )?));
        writer.write(&WincodeArchiveV2PohRecord {
            block_id: row.block_id,
            slot: row.slot,
            entries: poh,
        })?;
        writer.flush()?;
        fs::write(stage.join(format!("parts/{index}.signatures")), signatures)?;
        rows.insert(row.slot, row);
        Ok(())
    })?;
    ensure!(
        rows.keys().copied().collect::<Vec<_>>() == coverage.produced_slots,
        "captured blocks do not cover finalized RPC produced slots"
    );
    let rows = rows.into_values().collect::<Vec<_>>();
    validate_no_invalidated_banks(&rows, &invalidated)?;
    validate_chain(&rows, coverage.start_slot)?;
    let mut blocks_file = create(&stage.join("blocks/live-no-registry-blocks.bin"))?;
    let mut poh_file = create(&stage.join("evidence/available-poh.wincode"))?;
    let mut signatures_file = create(&stage.join("index/signatures.bin"))?;
    let mut blockhash_file = create(&stage.join("index/blockhash_registry.bin"))?;
    for row in &rows {
        for (suffix, file) in [
            ("block", &mut blocks_file),
            ("poh", &mut poh_file),
            ("signatures", &mut signatures_file),
        ] {
            let path = stage.join(format!("parts/{}.{suffix}", row.block_id));
            std::io::copy(&mut File::open(&path)?, file)?;
            fs::remove_file(path)?;
        }
        blockhash_file.write_all(&row.blockhash)?;
    }
    for file in [blocks_file, poh_file, signatures_file, blockhash_file] {
        file.sync_all()?;
    }
    fs::remove_dir(stage.join("parts"))?;
    // Retain every source object named by the sealed custody receipt, including RPC identity replies.
    let source_files = seal_value
        .get("files")
        .and_then(|v| v.as_array())
        .context("source seal lacks files")?;
    ensure!(
        !source_files.is_empty() && source_files.len() <= 8,
        "source file count exceeds bound"
    );
    let mut evidence_names = Vec::new();
    for record in source_files {
        let name = record
            .get("path")
            .and_then(|v| v.as_str())
            .context("source file missing path")?;
        ensure!(
            !name.starts_with('.')
                && !name.is_empty()
                && name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)),
            "unsafe source filename"
        );
        ensure!(
            !evidence_names.iter().any(|n| n == name),
            "duplicate source filename"
        );
        copy_stable(&input.join(name), &stage.join("evidence").join(name))?;
        let actual = file_record(&stage.join("evidence"), name)?;
        ensure!(
            record.get("bytes").and_then(|v| v.as_u64()) == Some(actual.bytes)
                && record.get("sha256").and_then(|v| v.as_str()) == Some(actual.sha256.as_str()),
            "copied source object differs from seal"
        );
        evidence_names.push(name.to_string());
    }
    for name in [
        "CURRENT-GRPC-SEALED.v1.json",
        "CURRENT-GRPC-INVENTORY.v1.json",
    ] {
        copy_stable(&input.join(name), &stage.join("evidence").join(name))?;
        evidence_names.push(name.to_string());
    }
    copy_stable(
        coverage_path,
        &stage.join("evidence/finalized-coverage.json"),
    )?;
    copy_stable(&rpc_path, &stage.join("evidence/rpc-get-blocks.json"))?;
    ensure!(
        bounded_read(&seal_path, JSON_LIMIT)? == seal_bytes,
        "source seal changed"
    );
    ensure!(
        bounded_read(
            &stage.join("evidence/CURRENT-GRPC-SEALED.v1.json"),
            JSON_LIMIT
        )? == seal_bytes,
        "copied seal differs"
    );
    ensure!(
        bounded_read(&stage.join("evidence/finalized-coverage.json"), JSON_LIMIT)?
            == coverage_bytes,
        "coverage changed"
    );
    ensure!(
        bounded_read(&stage.join("evidence/rpc-get-blocks.json"), JSON_LIMIT)? == rpc_bytes,
        "RPC response changed"
    );
    // Re-audit the copied raw stream under its original names before issuing the receipt.
    current::read_sealed_current_events(&stage.join("evidence"), max_record_bytes, |_, _| Ok(()))?;
    let mut paths = vec![
        "blocks/live-no-registry-blocks.bin".to_string(),
        "index/signatures.bin".into(),
        "index/blockhash_registry.bin".into(),
        "evidence/available-poh.wincode".into(),
        "evidence/finalized-coverage.json".into(),
        "evidence/rpc-get-blocks.json".into(),
    ];
    paths.extend(evidence_names.into_iter().map(|n| format!("evidence/{n}")));
    let files = paths
        .iter()
        .map(|path| file_record(&stage, path))
        .collect::<Result<Vec<_>>>()?;
    let receipt = NormalizedExecutionV1 {
        schema: NORMALIZED_EXECUTION_SCHEMA.into(), source_profile: EXECUTION_SOURCE_PROFILE.into(), state: "COMPLETE".into(),
        canonical: false, finalized: true, finality_basis: "operator-trusted-own-validator-finalized-subscription-and-rpc".into(),
        epoch: coverage.start_slot / 432_000, start_slot: coverage.start_slot, end_slot_exclusive: coverage.end_slot_exclusive,
        genesis_hash: coverage.genesis_hash, node_identity: coverage.node_identity, source_receipt_sha256: seal_sha,
        coverage_receipt_sha256: hash_bytes(&coverage_bytes), rpc_response_sha256: coverage.rpc_response_sha256,
        transactions: rows.iter().map(|r| r.transactions).sum(), signatures: rows.iter().map(|r| r.signatures).sum(),
        entries: rows.iter().map(|r| r.entries).sum(), blocks: rows, files,
        poh_structure_verified: true, poh_hashes_recomputed: false, signatures_cryptographically_verified: false,
        shred_evidence: "absent".into(), execution_projection: "compact-v2-known-fields; commission_bps remains in authoritative raw envelopes".into(),
        authoritative_execution: "evidence/events.wal; yellowstone-grpc-proto@14.0.0 known-schema envelopes, including standalone footer and parent events".into(),
    };
    let mut file = create(&stage.join(NORMALIZED_EXECUTION_FILE))?;
    serde_json::to_writer_pretty(&mut file, &receipt)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    for record in &receipt.files {
        make_readonly(&stage.join(&record.path))?;
    }
    make_readonly(&stage.join(NORMALIZED_EXECUTION_FILE))?;
    for dir in [
        stage.join("blocks"),
        stage.join("index"),
        stage.join("evidence"),
        stage.clone(),
    ] {
        File::open(dir)?.sync_all()?;
    }
    fs::rename(&stage, output)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn validate_coverage(c: &FinalizedRpcCoverageV1, max_blocks: usize) -> Result<()> {
    ensure!(
        c.schema == "hivezilla-finalized-rpc-coverage-v1" && c.commitment == "finalized",
        "wrong coverage schema or commitment"
    );
    hash_string(&c.genesis_hash)?;
    hash_string(&c.node_identity)?;
    ensure!(
        c.start_slot > 0 && c.start_slot < c.end_slot_exclusive,
        "invalid range"
    );
    ensure!(
        c.start_slot / 432_000 == (c.end_slot_exclusive - 1) / 432_000,
        "canary range crosses epoch"
    );
    ensure!(
        c.finalized_slot >= c.end_slot_exclusive - 1,
        "coverage end exceeds finalized root"
    );
    ensure!(
        !c.produced_slots.is_empty() && c.produced_slots.len() <= max_blocks,
        "empty or oversized produced block set"
    );
    let mut last = None;
    for &slot in &c.produced_slots {
        ensure!(
            (c.start_slot..c.end_slot_exclusive).contains(&slot) && last.is_none_or(|p| p < slot),
            "unsorted, duplicate, or out-of-range produced slot"
        );
        last = Some(slot);
    }
    Ok(())
}

fn validate_chain(rows: &[ExecutionBlockV1], start: u64) -> Result<()> {
    ensure!(
        rows.first().is_some_and(|r| r.parent_slot < start),
        "first produced block does not anchor before range"
    );
    for pair in rows.windows(2) {
        ensure!(
            pair[1].parent_slot == pair[0].slot && pair[1].parent_blockhash == pair[0].blockhash,
            "produced parent/hash chain does not match finalized range"
        );
    }
    Ok(())
}

fn invalidated_bank(update: &pb::SubscribeUpdate) -> Option<(u64, Option<u64>)> {
    use pb::subscribe_update::UpdateOneof;
    match update.update_oneof.as_ref()? {
        UpdateOneof::EntryUpdateParent(parent) => Some((parent.slot, Some(parent.cleared_bank_id))),
        UpdateOneof::Slot(slot)
            if pb::SlotStatus::try_from(slot.status)
                .is_ok_and(|status| status.as_str_name() == "SLOT_DEAD") =>
        {
            Some((slot.slot, slot.bank_id))
        }
        _ => None,
    }
}

fn validate_no_invalidated_banks(
    rows: &[ExecutionBlockV1],
    invalidated: &BTreeSet<(u64, Option<u64>)>,
) -> Result<()> {
    for row in rows {
        ensure!(
            !invalidated.contains(&(row.slot, Some(row.bank_id)))
                && !invalidated.contains(&(row.slot, None)),
            "selected slot {} bank {} has contradictory dead/cleared evidence",
            row.slot,
            row.bank_id
        );
    }
    Ok(())
}

fn convert_block(
    block: &pb::SubscribeUpdateBlock,
    block_id: u32,
) -> Result<(
    WincodeArchiveV2NoRegistryBlock,
    Vec<CompactPohEntry>,
    Vec<u8>,
)> {
    ensure!(block.parent_slot < block.slot, "invalid block parent");
    ensure!(
        block.executed_transaction_count == block.transactions.len() as u64
            && block.entries_count == block.entries.len() as u64
            && block.entries_count > 0,
        "block transaction or entry count mismatch"
    );
    let mut txs = block.transactions.iter().collect::<Vec<_>>();
    txs.sort_unstable_by_key(|t| t.index);
    let mut normalized = Vec::with_capacity(txs.len());
    let mut signatures = Vec::new();
    let mut signature_counts = Vec::new();
    for (index, row) in txs.into_iter().enumerate() {
        ensure!(
            row.index == index as u64,
            "transaction indices are not dense"
        );
        let tx = row.transaction.as_ref().context("missing transaction")?;
        let message = tx.message.as_ref().context("missing message")?;
        let converted = convert_message(message)?;
        let header = message.header.as_ref().context("missing header")?;
        ensure!(
            tx.signatures.len() == header.num_required_signatures as usize
                && !tx.signatures.is_empty(),
            "signature count mismatch"
        );
        ensure!(
            row.signature == tx.signatures[0],
            "transaction ID differs from first signature"
        );
        for signature in &tx.signatures {
            ensure!(signature.len() == 64, "invalid signature length");
            signatures.extend_from_slice(signature);
        }
        signature_counts.push(tx.signatures.len() as u32);
        let meta = row.meta.as_ref().context("missing execution metadata")?;
        normalized.push(WincodeArchiveV2NoRegistryTransaction {
            tx_index: u32::try_from(index)?,
            tx: WincodeArchiveV2Payload::Decoded {
                source_len: tx.encoded_len() as u64,
                value: WincodeArchiveV2NoRegistryTx {
                    signatures: tx.signatures.clone(),
                    message: converted,
                },
            },
            metadata: Some(WincodeArchiveV2Payload::Decoded {
                source_len: meta.encoded_len() as u64,
                value: convert_meta(meta)?,
            }),
        });
    }
    let mut entries = block.entries.iter().collect::<Vec<_>>();
    entries.sort_unstable_by_key(|e| e.index);
    let mut count = 0usize;
    let mut poh = Vec::with_capacity(entries.len());
    for (index, entry) in entries.into_iter().enumerate() {
        ensure!(
            entry.index == index as u64
                && entry.slot == block.slot
                && entry.bank_id == block.bank_id,
            "entry slot/bank/index mismatch"
        );
        ensure!(
            entry.starting_transaction_index == count as u64,
            "entry transaction start mismatch"
        );
        let end = count
            .checked_add(usize::try_from(entry.executed_transaction_count)?)
            .context("entry count overflow")?;
        ensure!(
            end <= signature_counts.len(),
            "entry transaction range exceeds block"
        );
        let signature_count = signature_counts[count..end].iter().try_fold(0u32, |a, b| {
            a.checked_add(*b).context("entry signature count overflow")
        })?;
        poh.push(CompactPohEntry {
            num_hashes: entry.num_hashes,
            hash: bytes32(&entry.hash)?,
            tx_count: u32::try_from(entry.executed_transaction_count)?,
            signature_count,
        });
        count = end;
    }
    ensure!(
        count == normalized.len()
            && poh.last().context("no entries")?.hash == hash_string(&block.blockhash)?,
        "PoH final hash or count mismatch"
    );
    let rewards = block
        .rewards
        .as_ref()
        .map(|r| -> Result<_> {
            Ok(WincodeArchiveV2NoRegistryRewards {
                source_len: r.encoded_len() as u64,
                num_partitions: r.num_partitions.as_ref().map(|p| p.num_partitions),
                decoded: Some(
                    r.rewards
                        .iter()
                        .map(convert_reward)
                        .collect::<Result<_>>()?,
                ),
                raw_fallback: None,
                decode_error: None,
            })
        })
        .transpose()?;
    Ok((
        WincodeArchiveV2NoRegistryBlock {
            header: WincodeArchiveV2NoRegistryBlockHeader {
                compact: CompactBlockHeader {
                    slot: block.slot,
                    parent_slot: block.parent_slot,
                    blockhash: block_id,
                    previous_blockhash: block_id.saturating_sub(1),
                    block_time: block.block_time.as_ref().map(|v| v.timestamp),
                    block_height: block.block_height.as_ref().map(|v| v.block_height),
                    shredding: vec![],
                    poh_entries: vec![],
                    rewards: None,
                },
                rewards,
            },
            txs: normalized,
        },
        poh,
        signatures,
    ))
}

fn convert_message(m: &pb::Message) -> Result<WincodeArchiveV2NoRegistryMessage> {
    let h = m.header.as_ref().context("missing message header")?;
    let header = CompactMessageHeader {
        num_required_signatures: u8::try_from(h.num_required_signatures)?,
        num_readonly_signed_accounts: u8::try_from(h.num_readonly_signed_accounts)?,
        num_readonly_unsigned_accounts: u8::try_from(h.num_readonly_unsigned_accounts)?,
    };
    ensure!(
        usize::from(header.num_required_signatures) <= m.account_keys.len()
            && header.num_readonly_signed_accounts <= header.num_required_signatures
            && usize::from(header.num_readonly_unsigned_accounts)
                <= m.account_keys.len() - usize::from(header.num_required_signatures),
        "invalid message header account counts"
    );
    let account_keys = m
        .account_keys
        .iter()
        .map(|v| bytes32(v))
        .collect::<Result<Vec<_>>>()?;
    let recent_blockhash = bytes32(&m.recent_blockhash)?;
    let instructions = m
        .instructions
        .iter()
        .map(|ix| {
            Ok(WincodeArchiveV2NoRegistryInstruction {
                program_id_index: u8::try_from(ix.program_id_index)?,
                accounts: ix.accounts.clone(),
                data: ix.data.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if let Some(c) = &m.config {
        ensure!(
            m.versioned && m.address_table_lookups.is_empty(),
            "V1 config requires versioned message without lookup tables"
        );
        return Ok(WincodeArchiveV2NoRegistryMessage::V1(
            WincodeArchiveV2NoRegistryV1Message {
                header,
                account_keys,
                recent_blockhash,
                instructions,
                config: CompactTransactionConfig {
                    priority_fee: c.priority_fee,
                    compute_unit_limit: c.compute_unit_limit,
                    loaded_accounts_data_size_limit: c.loaded_accounts_data_size_limit,
                    heap_size: c.heap_size,
                },
            },
        ));
    }
    if m.versioned {
        let address_table_lookups = m
            .address_table_lookups
            .iter()
            .map(|l| {
                Ok(WincodeArchiveV2NoRegistryAddressTableLookup {
                    account_key: bytes32(&l.account_key)?,
                    writable_indexes: l.writable_indexes.clone(),
                    readonly_indexes: l.readonly_indexes.clone(),
                })
            })
            .collect::<Result<_>>()?;
        Ok(WincodeArchiveV2NoRegistryMessage::V0(
            WincodeArchiveV2NoRegistryV0Message {
                header,
                account_keys,
                recent_blockhash,
                instructions,
                address_table_lookups,
            },
        ))
    } else {
        ensure!(
            m.address_table_lookups.is_empty() && header.num_required_signatures < 128,
            "invalid legacy message shape"
        );
        Ok(WincodeArchiveV2NoRegistryMessage::Legacy(
            WincodeArchiveV2NoRegistryLegacyMessage {
                header,
                account_keys,
                recent_blockhash,
                instructions,
            },
        ))
    }
}

fn convert_meta(m: &pb::TransactionStatusMeta) -> Result<WincodeArchiveV2NoRegistryMeta> {
    ensure!(
        m.pre_balances.len() == m.post_balances.len(),
        "pre/post balance cardinality mismatch"
    );
    Ok(WincodeArchiveV2NoRegistryMeta {
        err: m
            .err
            .as_ref()
            .map(|e| CompactTransactionError::from_stored_wincode_bytes(&e.err))
            .transpose()?,
        fee: m.fee,
        pre_balances: m.pre_balances.clone(),
        post_balances: m.post_balances.clone(),
        inner_instructions: if m.inner_instructions_none {
            ensure!(
                m.inner_instructions.is_empty(),
                "absent inner instructions has records"
            );
            None
        } else {
            Some(
                m.inner_instructions
                    .iter()
                    .map(|r| CompactInnerInstructions {
                        index: r.index,
                        instructions: r
                            .instructions
                            .iter()
                            .map(|i| CompactInnerInstruction {
                                program_id_index: i.program_id_index,
                                accounts: i.accounts.clone(),
                                data: i.data.clone(),
                                stack_height: i.stack_height,
                            })
                            .collect(),
                    })
                    .collect(),
            )
        },
        logs: if m.log_messages_none {
            ensure!(m.log_messages.is_empty(), "absent logs has records");
            None
        } else {
            Some(WincodeArchiveV2NoRegistryLogs::Raw(m.log_messages.clone()))
        },
        pre_token_balances: m
            .pre_token_balances
            .iter()
            .map(convert_token)
            .collect::<Result<_>>()?,
        post_token_balances: m
            .post_token_balances
            .iter()
            .map(convert_token)
            .collect::<Result<_>>()?,
        rewards: m
            .rewards
            .iter()
            .map(convert_reward)
            .collect::<Result<_>>()?,
        loaded_writable_addresses: m
            .loaded_writable_addresses
            .iter()
            .map(|v| bytes32(v))
            .collect::<Result<_>>()?,
        loaded_readonly_addresses: m
            .loaded_readonly_addresses
            .iter()
            .map(|v| bytes32(v))
            .collect::<Result<_>>()?,
        return_data: if m.return_data_none {
            ensure!(m.return_data.is_none(), "absent return data has payload");
            None
        } else {
            m.return_data
                .as_ref()
                .map(|r| {
                    Ok::<_, anyhow::Error>(WincodeArchiveV2NoRegistryReturnData {
                        program_id: bytes32(&r.program_id)?,
                        data: r.data.clone(),
                    })
                })
                .transpose()?
        },
        compute_units_consumed: m.compute_units_consumed,
        cost_units: m.cost_units,
    })
}
fn convert_token(b: &pb::TokenBalance) -> Result<WincodeArchiveV2NoRegistryTokenBalance> {
    let ui = b.ui_token_amount.as_ref().context("missing token amount")?;
    let optional = |s: &str| -> Result<Option<[u8; 32]>> {
        if s.is_empty() {
            Ok(None)
        } else {
            hash_string(s).map(Some)
        }
    };
    Ok(WincodeArchiveV2NoRegistryTokenBalance {
        account_index: b.account_index,
        mint: optional(&b.mint)?,
        owner: optional(&b.owner)?,
        program_id: optional(&b.program_id)?,
        amount: ui.amount.parse()?,
        decimals: u8::try_from(ui.decimals)?,
    })
}
fn convert_reward(r: &pb::Reward) -> Result<WincodeArchiveV2NoRegistryReward> {
    // commission_bps is not rounded or substituted into legacy percent. Full r stays in raw evidence.
    Ok(WincodeArchiveV2NoRegistryReward {
        pubkey: hash_string(&r.pubkey)?,
        lamports: r.lamports,
        post_balance: r.post_balance,
        reward_type: r.reward_type,
        commission: if r.commission.is_empty() {
            None
        } else {
            Some(r.commission.parse()?)
        },
    })
}
fn bytes32(bytes: &[u8]) -> Result<[u8; 32]> {
    bytes.try_into().context("expected 32 bytes")
}
fn hash_string(s: &str) -> Result<[u8; 32]> {
    crate::grpc::decode_required_hash_string(s)
}
fn hash_bytes(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
fn create(path: &Path) -> Result<File> {
    Ok(OpenOptions::new().create_new(true).write(true).open(path)?)
}
fn make_readonly(path: &Path) -> Result<()> {
    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_readonly(true);
    fs::set_permissions(path, permissions)?;
    File::open(path)?.sync_all()?;
    Ok(())
}
fn bounded_read(path: &Path, limit: u64) -> Result<Vec<u8>> {
    ensure!(
        fs::symlink_metadata(path)?.file_type().is_file(),
        "not a regular file: {}",
        path.display()
    );
    let mut bytes = Vec::new();
    File::open(path)?.take(limit + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= limit, "file exceeds read limit");
    Ok(bytes)
}
fn copy_stable(source: &Path, target: &Path) -> Result<()> {
    ensure!(
        fs::symlink_metadata(source)?.file_type().is_file(),
        "source is not a regular file"
    );
    let mut source_file = File::open(source)?;
    let before = source_file.metadata()?;
    let mut target_file = create(target)?;
    let bytes = std::io::copy(&mut source_file, &mut target_file)?;
    let after = source_file.metadata()?;
    ensure!(
        bytes == before.len()
            && before.len() == after.len()
            && before.modified()? == after.modified()?,
        "source changed during copy"
    );
    target_file.sync_all()?;
    Ok(())
}
fn file_record(root: &Path, path: &str) -> Result<ExecutionFileV1> {
    let mut file = File::open(root.join(path))?;
    let mut sha = Sha256::new();
    let mut buffer = [0u8; 65536];
    let mut bytes = 0;
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        sha.update(&buffer[..n]);
        bytes += n as u64;
    }
    Ok(ExecutionFileV1 {
        path: path.into(),
        bytes,
        sha256: sha
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
    })
}

#[cfg(test)]
pub(crate) fn current_test_block() -> pb::SubscribeUpdateBlock {
    tests::block()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn key(byte: u8) -> String {
        let mut out = [0u8; 44];
        let len = five8::encode_32(&[byte; 32], &mut out) as usize;
        String::from_utf8(out[..len].to_vec()).unwrap()
    }
    fn message() -> pb::Message {
        pb::Message {
            header: Some(pb::MessageHeader {
                num_required_signatures: 1,
                ..Default::default()
            }),
            account_keys: vec![vec![2; 32]],
            recent_blockhash: vec![3; 32],
            versioned: true,
            config: Some(pb::TransactionConfig {
                priority_fee: Some(123),
                compute_unit_limit: Some(456),
                loaded_accounts_data_size_limit: Some(789),
                heap_size: Some(1024),
            }),
            ..Default::default()
        }
    }
    pub(super) fn block() -> pb::SubscribeUpdateBlock {
        pb::SubscribeUpdateBlock {
            slot: 42,
            parent_slot: 40,
            bank_id: 91,
            blockhash: key(8),
            parent_blockhash: key(7),
            executed_transaction_count: 1,
            entries_count: 2,
            transactions: vec![pb::SubscribeUpdateTransactionInfo {
                index: 0,
                signature: vec![4; 64],
                transaction: Some(pb::Transaction {
                    signatures: vec![vec![4; 64]],
                    message: Some(message()),
                }),
                meta: Some(pb::TransactionStatusMeta {
                    pre_balances: vec![10],
                    post_balances: vec![9],
                    ..Default::default()
                }),
                ..Default::default()
            }],
            entries: vec![
                pb::SubscribeUpdateEntry {
                    slot: 42,
                    bank_id: 91,
                    index: 0,
                    num_hashes: 3,
                    hash: vec![5; 32],
                    executed_transaction_count: 1,
                    starting_transaction_index: 0,
                },
                pb::SubscribeUpdateEntry {
                    slot: 42,
                    bank_id: 91,
                    index: 1,
                    num_hashes: 7,
                    hash: vec![8; 32],
                    executed_transaction_count: 0,
                    starting_transaction_index: 1,
                },
            ],
            ..Default::default()
        }
    }
    #[test]
    fn v1_config_and_all_signatures_survive_the_projection() {
        let (converted, poh, signatures) = convert_block(&block(), 0).unwrap();
        let WincodeArchiveV2Payload::Decoded { value, .. } = &converted.txs[0].tx else {
            panic!("decoded transaction expected")
        };
        let WincodeArchiveV2NoRegistryMessage::V1(v1) = &value.message else {
            panic!("V1 expected")
        };
        assert_eq!(
            v1.config,
            CompactTransactionConfig {
                priority_fee: Some(123),
                compute_unit_limit: Some(456),
                loaded_accounts_data_size_limit: Some(789),
                heap_size: Some(1024)
            }
        );
        assert_eq!(value.signatures, vec![vec![4; 64]]);
        assert_eq!(signatures, vec![4; 64]);
        assert_eq!(poh.len(), 2);
        assert_eq!(poh[0].signature_count, 1);
        assert_eq!(poh[1].tx_count, 0);
        assert!(converted.header.compact.shredding.is_empty());
        assert!(converted.header.compact.poh_entries.is_empty());
    }
    #[test]
    fn order_is_normalized_but_duplicate_missing_and_cross_bank_entries_fail() {
        let mut valid = block();
        valid.entries.reverse();
        assert!(convert_block(&valid, 0).is_ok());
        let mut wrong = block();
        wrong.entries[0].index = 1;
        assert!(convert_block(&wrong, 0).is_err());
        let mut wrong = block();
        wrong.entries[1].bank_id += 1;
        assert!(convert_block(&wrong, 0).is_err());
        let mut wrong = block();
        wrong.entries[1].starting_transaction_index = 0;
        assert!(convert_block(&wrong, 0).is_err());
        let mut wrong = block();
        wrong.entries[0].executed_transaction_count = 2;
        assert!(convert_block(&wrong, 0).is_err());
        let mut wrong = block();
        wrong.entries[1].hash = vec![9; 32];
        assert!(convert_block(&wrong, 0).is_err());
    }
    #[test]
    fn incomplete_execution_or_bad_transaction_identity_is_rejected() {
        let mut wrong = block();
        wrong.executed_transaction_count = 2;
        assert!(convert_block(&wrong, 0).is_err());
        let mut wrong = block();
        wrong.transactions[0].index = 1;
        assert!(convert_block(&wrong, 0).is_err());
        let mut wrong = block();
        wrong.transactions[0].signature[0] ^= 1;
        assert!(convert_block(&wrong, 0).is_err());
        let mut wrong = block();
        wrong.transactions[0].meta = None;
        assert!(convert_block(&wrong, 0).is_err());
        let mut wrong = block();
        wrong.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .signatures[0]
            .pop();
        assert!(convert_block(&wrong, 0).is_err());
    }
    #[test]
    fn v1_shape_and_legacy_header_guards_do_not_coerce_versions() {
        let mut wrong = message();
        wrong.versioned = false;
        assert!(convert_message(&wrong).is_err());
        let mut wrong = message();
        wrong.address_table_lookups.push(Default::default());
        assert!(convert_message(&wrong).is_err());
        let mut valid = message();
        valid.config = None;
        assert!(matches!(
            convert_message(&valid).unwrap(),
            WincodeArchiveV2NoRegistryMessage::V0(_)
        ));
        valid.versioned = false;
        assert!(matches!(
            convert_message(&valid).unwrap(),
            WincodeArchiveV2NoRegistryMessage::Legacy(_)
        ));
        valid.header.as_mut().unwrap().num_readonly_signed_accounts = 2;
        assert!(convert_message(&valid).is_err());
    }
    #[test]
    fn bps_is_not_rounded_into_the_percent_projection() {
        let mut reward = pb::Reward {
            pubkey: key(2),
            commission_bps: "275".into(),
            ..Default::default()
        };
        assert_eq!(convert_reward(&reward).unwrap().commission, None);
        reward.commission = "2".into();
        assert_eq!(convert_reward(&reward).unwrap().commission, Some(2));
        assert_eq!(reward.commission_bps, "275");
    }
    #[test]
    fn metadata_absence_flags_and_balance_lengths_are_checked() {
        let mut m = pb::TransactionStatusMeta {
            pre_balances: vec![1],
            post_balances: vec![],
            ..Default::default()
        };
        assert!(convert_meta(&m).is_err());
        m.post_balances.push(1);
        m.log_messages_none = true;
        m.log_messages.push("retained".into());
        assert!(convert_meta(&m).is_err());
        m.log_messages.clear();
        assert!(convert_meta(&m).is_ok());
    }
    fn coverage() -> FinalizedRpcCoverageV1 {
        FinalizedRpcCoverageV1 {
            schema: "hivezilla-finalized-rpc-coverage-v1".into(),
            genesis_hash: key(1),
            node_identity: key(2),
            commitment: "finalized".into(),
            start_slot: 41,
            end_slot_exclusive: 45,
            finalized_slot: 44,
            produced_slots: vec![42, 44],
            rpc_response_sha256: "a".repeat(64),
            source_receipt_sha256: "b".repeat(64),
        }
    }
    #[test]
    fn finalized_coverage_accepts_gaps_and_rejects_incomplete_or_ambiguous_ranges() {
        assert!(validate_coverage(&coverage(), 2).is_ok());
        let mut wrong = coverage();
        wrong.finalized_slot = 43;
        assert!(validate_coverage(&wrong, 2).is_err());
        let mut wrong = coverage();
        wrong.produced_slots = vec![42, 42];
        assert!(validate_coverage(&wrong, 2).is_err());
        let mut wrong = coverage();
        wrong.produced_slots.reverse();
        assert!(validate_coverage(&wrong, 2).is_err());
        let mut wrong = coverage();
        wrong.end_slot_exclusive = 432001;
        assert!(validate_coverage(&wrong, 2).is_err());
        assert!(validate_coverage(&coverage(), 1).is_err());
    }
    #[test]
    fn produced_chain_requires_exact_parent_hash_and_slot() {
        let row = ExecutionBlockV1 {
            block_id: 0,
            slot: 42,
            bank_id: 91,
            blockhash: [8; 32],
            parent_slot: 40,
            parent_blockhash: [7; 32],
            transactions: 1,
            signatures: 1,
            entries: 2,
            source_sequence: 0,
            source_raw_sha256: "a".repeat(64),
        };
        let mut next = row.clone();
        next.block_id = 1;
        next.slot = 44;
        next.parent_slot = 42;
        next.parent_blockhash = row.blockhash;
        assert!(validate_chain(&[row.clone(), next.clone()], 41).is_ok());
        next.parent_blockhash[0] ^= 1;
        assert!(validate_chain(&[row.clone(), next], 41).is_err());
        assert!(validate_chain(&[row], 40).is_err());
    }
    #[test]
    fn selected_bank_invalidation_is_rejected_but_other_banks_remain_valid() {
        let row = ExecutionBlockV1 {
            block_id: 0,
            slot: 42,
            bank_id: 91,
            blockhash: [8; 32],
            parent_slot: 40,
            parent_blockhash: [7; 32],
            transactions: 1,
            signatures: 1,
            entries: 2,
            source_sequence: 0,
            source_raw_sha256: "a".repeat(64),
        };
        let mut parent = pb::SubscribeUpdate {
            update_oneof: Some(pb::subscribe_update::UpdateOneof::EntryUpdateParent(
                pb::SubscribeUpdateEntryUpdateParent {
                    slot: 42,
                    cleared_bank_id: 90,
                    parent_slot: 40,
                    parent_block_id: vec![7; 32],
                },
            )),
            ..Default::default()
        };
        let invalid = BTreeSet::from([invalidated_bank(&parent).unwrap()]);
        assert!(validate_no_invalidated_banks(std::slice::from_ref(&row), &invalid).is_ok());
        if let Some(pb::subscribe_update::UpdateOneof::EntryUpdateParent(event)) =
            parent.update_oneof.as_mut()
        {
            event.cleared_bank_id = 91;
        }
        let invalid = BTreeSet::from([invalidated_bank(&parent).unwrap()]);
        assert!(validate_no_invalidated_banks(std::slice::from_ref(&row), &invalid).is_err());
        for bank_id in [Some(91), None] {
            let dead = pb::SubscribeUpdate {
                update_oneof: Some(pb::subscribe_update::UpdateOneof::Slot(
                    pb::SubscribeUpdateSlot {
                        slot: 42,
                        bank_id,
                        status: 6,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            };
            let invalid = BTreeSet::from([invalidated_bank(&dead).unwrap()]);
            assert!(validate_no_invalidated_banks(std::slice::from_ref(&row), &invalid).is_err());
        }
    }
}
