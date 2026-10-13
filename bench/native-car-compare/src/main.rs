//! Read-only sampled metadata comparison. Public-key IDs are resolved before hashing.
use anyhow::{Context, Result, ensure};
use blockzilla_archive_v2::*;
use blockzilla_compact::{CompactMetaV1, render_logs};
use blockzilla_primitives::{PubkeyResolver, bounded_historical_source_wincode_leb128_config};
use memmap2::Mmap;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::File,
    io::Read,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};

fn normalize(value: &mut Value, registry: &[u8]) -> Result<()> {
    match value {
        Value::Object(map) if map.len() == 1 && map.contains_key("Id") => {
            let id = map["Id"].as_u64().context("invalid pubkey ID")?;
            ensure!(id > 0 && id <= u32::MAX as u64, "invalid pubkey ID");
            let at = usize::try_from(id - 1)?
                .checked_mul(32)
                .context("pubkey offset overflow")?;
            let bytes = registry
                .get(at..at + 32)
                .context("pubkey ID outside registry")?;
            *value = json!(bytes);
        }
        Value::Object(map)
            if map.len() == 1
                && map.contains_key("Raw")
                && map["Raw"].as_array().is_some_and(|a| a.len() == 32) =>
        {
            *value = map["Raw"].clone();
        }
        Value::Object(map) => {
            for value in map.values_mut() {
                normalize(value, registry)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                normalize(value, registry)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn add(hasher: &mut Sha256, value: &Value) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
    Ok(())
}

fn canonicalize_rewards(header: &mut Value) -> Result<Option<String>> {
    let Some(rewards) = header
        .get_mut("rewards")
        .and_then(|value| value.get_mut("decoded"))
        .and_then(Value::as_array_mut)
    else {
        return Ok(None);
    };
    let original = serde_json::to_vec(rewards)?;
    // Reward vectors have source-dependent order. Preserve duplicates while
    // comparing every resolved reward value in a stable order.
    rewards.sort_by_cached_key(|reward| serde_json::to_vec(reward).expect("JSON reward"));
    Ok(Some(format!("{:x}", Sha256::digest(original))))
}

struct RegistryResolver<'a>(&'a [u8]);
impl PubkeyResolver for RegistryResolver<'_> {
    fn resolve_pubkey(&self, id: u32) -> Option<[u8; 32]> {
        let at = usize::try_from(id.checked_sub(1)?).ok()?.checked_mul(32)?;
        self.0.get(at..at.checked_add(32)?)?.try_into().ok()
    }
}

struct Archive {
    rows: ArchiveV2HotBlockIndex,
    blob: File,
    registry: Option<Mmap>,
}
impl Archive {
    fn open(path: &Path) -> Result<Self> {
        let rows = read_archive_v2_hot_block_index(&path.join("archive-v2-blocks.index"))?;
        ensure!(rows.flags == 0, "unsupported compact blob flags");
        let blob = File::open(path.join("archive-v2-blocks.zstd"))?;
        ensure!(
            blob.metadata()?.len() == rows.blob_file_bytes,
            "blob size mismatch"
        );
        let registry_file = File::open(path.join("registry.bin"))?;
        ensure!(
            registry_file.metadata()?.len().is_multiple_of(32),
            "invalid registry length"
        );
        // Both source archives are sealed. Only immutable pubkey bytes are mapped.
        let registry = if registry_file.metadata()?.len() == 0 {
            None
        } else {
            Some(unsafe { Mmap::map(&registry_file)? })
        };
        Ok(Self {
            rows,
            blob,
            registry,
        })
    }
    fn check(&self, slot: u64, detail_logs: bool) -> Result<Value> {
        let registry = self.registry.as_deref().unwrap_or(&[]);
        let at = self
            .rows
            .rows
            .binary_search_by_key(&slot, |r| r.slot)
            .map_err(|_| anyhow::anyhow!("slot missing: {slot}"))?;
        let row = self.rows.rows[at];
        ensure!(
            row.uncompressed_len <= 64 << 20 && row.compressed_len <= 64 << 20,
            "block size limit"
        );
        let mut compressed = vec![0; row.compressed_len as usize];
        self.blob
            .read_exact_at(&mut compressed, row.compressed_offset)?;
        let mut decoder = ruzstd::decoding::StreamingDecoder::new(compressed.as_slice())?;
        let mut raw = Vec::with_capacity(row.uncompressed_len as usize);
        (&mut decoder)
            .take(row.uncompressed_len as u64 + 1)
            .read_to_end(&mut raw)?;
        ensure!(
            raw.len() == row.uncompressed_len as usize && decoder.into_inner().is_empty(),
            "zstd range mismatch"
        );
        let block =
            deserialize_archive_v2_hot_block_blob_borrowed_current_with_preallocation_limit::<
                1_048_576,
            >(&raw)?;
        ensure!(
            block.header.slot == slot
                && block.tx_count == row.tx_count
                && block.tx_rows_len() == row.tx_count as usize,
            "block/index mismatch"
        );
        let mut header = json!({"slot":block.header.slot,"parent_slot":block.header.parent_slot,
                               "block_time":block.header.block_time,"block_height":block.header.block_height,
                               "rewards":block.header.rewards});
        normalize(&mut header, registry)?;
        let reward_original_order_sha256 = canonicalize_rewards(&mut header)?;
        let mut fields: BTreeMap<String, Sha256> = BTreeMap::new();
        add(fields.entry("block_header".into()).or_default(), &header)?;
        // Keep reports small; the field digest above covers every reward value.
        if let Some(rewards) = header["rewards"]["decoded"].as_array() {
            let summary = json!({"num_partitions":header["rewards"]["num_partitions"],
                                 "count":rewards.len(),"resolved_sorted_sha256":format!("{:x}",Sha256::digest(serde_json::to_vec(rewards)?))});
            header["rewards"] = summary;
        }
        let mut log_details = Vec::new();
        let mut log_truncated_transactions = Vec::new();
        let mut message_end = 0usize;
        let mut metadata_end = 0usize;
        for (i, tx) in block.tx_rows().enumerate() {
            ensure!(
                tx.tx_index as usize == i && tx.reserved == [0; 3],
                "invalid tx order"
            );
            ensure!(
                tx.flags
                    & (ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK
                        | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK)
                    == 0,
                "raw fallback unsupported"
            );
            ensure!(
                tx.message_offset as usize == message_end
                    && tx.metadata_offset as usize == metadata_end,
                "noncontiguous tx payload"
            );
            message_end = message_end
                .checked_add(tx.message_len as usize)
                .context("message overflow")?;
            metadata_end = metadata_end
                .checked_add(tx.metadata_len as usize)
                .context("metadata overflow")?;
            let message: ArchiveV2HotMessagePayload = wincode::config::deserialize_exact(
                block
                    .message_bytes
                    .get(tx.message_offset as usize..message_end)
                    .context("message range")?,
                bounded_historical_source_wincode_leb128_config::<1_048_576>(),
            )?;
            let mut accounts = match &message {
                ArchiveV2HotMessagePayload::Legacy(m) => {
                    json!({"version":"legacy","header":m.header,"keys":m.account_keys,
                    "instruction_accounts":m.instructions.iter().map(|i|&i.accounts).collect::<Vec<_>>(),
                    "program_indexes":m.instructions.iter().map(|i|i.program_id_index).collect::<Vec<_>>()})
                }
                ArchiveV2HotMessagePayload::V0(m) => {
                    json!({"version":"v0","header":m.header,"keys":m.account_keys,
                    "instruction_accounts":m.instructions.iter().map(|i|&i.accounts).collect::<Vec<_>>(),
                    "lookups":m.address_table_lookups,"program_indexes":m.instructions.iter().map(|i|i.program_id_index).collect::<Vec<_>>()})
                }
                ArchiveV2HotMessagePayload::V1(m) => {
                    json!({"version":"v1","header":m.header,"keys":m.account_keys,"config":m.config,
                    "instruction_accounts":m.instructions.iter().map(|i|&i.accounts).collect::<Vec<_>>(),
                    "program_indexes":m.instructions.iter().map(|i|i.program_id_index).collect::<Vec<_>>()})
                }
            };
            normalize(&mut accounts, registry)?;
            add(
                fields
                    .entry("message_accounts_and_program_indexes".into())
                    .or_default(),
                &accounts,
            )?;
            ensure!(
                tx.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0,
                "transaction metadata unavailable"
            );
            let metadata: CompactMetaV1 = wincode::config::deserialize_exact(
                block
                    .metadata_bytes
                    .get(tx.metadata_offset as usize..metadata_end)
                    .context("metadata range")?,
                bounded_historical_source_wincode_leb128_config::<1_048_576>(),
            )?;
            let mut value = serde_json::to_value(&metadata)?;
            normalize(&mut value, registry)?;
            // Different compact event tags and string-table layouts can render
            // the same original log lines. Compare the decoded public value.
            let logs = metadata
                .logs
                .as_ref()
                .map(|logs| render_logs(logs, &RegistryResolver(registry)));
            if logs
                .as_ref()
                .is_some_and(|lines| lines.iter().any(|line| line == "Log truncated"))
            {
                log_truncated_transactions.push(i);
            }
            value["logs"] = json!(logs);
            if detail_logs {
                log_details.push(value["logs"].clone());
            }
            for (name, value) in value.as_object().context("metadata object")? {
                add(fields.entry(name.clone()).or_default(), value)?;
            }
        }
        ensure!(
            message_end == block.message_bytes.len() && metadata_end == block.metadata_bytes.len(),
            "trailing tx payload"
        );
        let hashes: BTreeMap<_, _> = fields
            .into_iter()
            .map(|(name, hash)| (name, format!("{:x}", hash.finalize())))
            .collect();
        let mut result = json!({"slot":slot,"transactions":row.tx_count,"fields":hashes,"header":header,
                               "reward_original_order_sha256":reward_original_order_sha256,
                               "log_truncated_transactions":log_truncated_transactions});
        if detail_logs {
            result["log_details"] = json!(log_details);
        }
        Ok(result)
    }
}

fn run() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    ensure!(
        args.len() == 2,
        "usage: blockzilla-native-car-metadata-check SPEC_JSON OUTPUT_JSON"
    );
    let spec: Value = serde_json::from_slice(&std::fs::read(&args[0])?)?;
    let mut archives: BTreeMap<PathBuf, Archive> = BTreeMap::new();
    let mut rows = Vec::new();
    for item in spec.as_array().context("spec array")? {
        let path = PathBuf::from(item["archive"].as_str().context("archive path")?);
        let slot = item["slot"].as_u64().context("slot")?;
        if !archives.contains_key(&path) {
            archives.insert(path.clone(), Archive::open(&path)?);
        }
        rows.push(
            archives[&path]
                .check(slot, item["detail_logs"].as_bool().unwrap_or(false))
                .with_context(|| format!("check slot {slot}"))?,
        );
    }
    let transactions: u64 = rows
        .iter()
        .map(|r| r["transactions"].as_u64().unwrap())
        .sum();
    let report = json!({"schema":"blockzilla-compact-metadata-sample-v3","state":"COMPLETE",
                        "blocks":rows.len(),"transactions":transactions,"rows":rows,
                        "metadata_pubkey_ids_resolved":true,"logs_rendered_before_comparison":true,
                        "block_reward_order_normalized":true,"instruction_account_indexes_checked":true,
                        "message_instruction_data_comparison":false});
    std::fs::write(&args[1], serde_json::to_vec_pretty(&report)?)?;
    println!(
        "{}",
        json!({"state":"COMPLETE","blocks":report["blocks"],"transactions":transactions})
    );
    Ok(())
}

fn main() -> Result<()> {
    run()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn raw_and_indexed_keys_match() {
        let registry = [7; 32];
        let mut raw = json!({"mint":{"Raw":vec![7u8;32]}});
        let mut indexed = json!({"mint":{"Id":1}});
        normalize(&mut raw, &registry).unwrap();
        normalize(&mut indexed, &registry).unwrap();
        assert_eq!(raw, indexed);
    }
    #[test]
    fn wrong_dictionary_key_stays_different() {
        let mut a = json!({"Id":1});
        let mut b = json!({"Id":1});
        normalize(&mut a, &[7; 32]).unwrap();
        normalize(&mut b, &[8; 32]).unwrap();
        assert_ne!(a, b);
    }
    #[test]
    fn missing_dictionary_key_is_rejected() {
        assert!(normalize(&mut json!({"Id":2}), &[7; 32]).is_err());
    }
    #[test]
    fn log_values_ignore_string_table_layout_and_pubkey_ids() {
        use blockzilla_compact::{CompactLogStream, DataTable, LogEvent};
        use blockzilla_primitives::{CompactPubkey, StringTable};
        let mut a = CompactLogStream {
            events: vec![],
            strings: StringTable::default(),
            data: DataTable::default(),
        };
        let mut b = CompactLogStream {
            events: vec![],
            strings: StringTable::default(),
            data: DataTable::default(),
        };
        let ai = a.strings.push("same error");
        b.strings.push("unused");
        let bi = b.strings.push("same error");
        a.events = vec![
            LogEvent::Invoke {
                program: CompactPubkey::Raw([7; 32]),
                depth: 1,
            },
            LogEvent::ProgramLogError { msg: ai },
        ];
        b.events = vec![
            LogEvent::Invoke {
                program: CompactPubkey::Id(1),
                depth: 1,
            },
            LogEvent::ProgramLogError { msg: bi },
        ];
        let resolver = RegistryResolver(&[7; 32]);
        assert_eq!(render_logs(&a, &resolver), render_logs(&b, &resolver));
    }
    #[test]
    fn reward_order_is_normalized_without_losing_duplicates() {
        let mut a = json!({"rewards":{"decoded":[{"value":2},{"value":1},{"value":1}]}});
        let mut b = json!({"rewards":{"decoded":[{"value":1},{"value":2},{"value":1}]}});
        assert_ne!(
            canonicalize_rewards(&mut a).unwrap(),
            canonicalize_rewards(&mut b).unwrap()
        );
        assert_eq!(a, b);
        let mut c = json!({"rewards":{"decoded":[{"value":1},{"value":2}]}});
        canonicalize_rewards(&mut c).unwrap();
        assert_ne!(a, c);
    }
}
