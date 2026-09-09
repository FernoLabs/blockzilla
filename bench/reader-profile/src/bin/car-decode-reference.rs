//! Full CAR transaction/metadata decode probe. This is a benchmark adapter,
//! not a new public SDK path. See README-car-decode.md for comparison limits.
use anyhow::{Context, Result, ensure};
use clap::Parser;
use of_car_reader::{
    CarBlockReader, LosslessBlockReadLimits, OrderedLosslessCarBlock,
    confirmed_block::{Rewards, TransactionStatusMeta},
    metadata_decoder::{
        ZstdReusableDecoder, decode_rewards_from_frame, decode_transaction_status_meta_from_frame,
    },
    query_sdk_http::{CarHttpOptions, CarHttpSession},
    short_vec::decode_shortu16_len,
    versioned_transaction::{VersionedMessage, VersionedTransactionReuse},
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{Read, Write},
    path::PathBuf,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::Instant,
};

#[cfg(feature = "reference-mimalloc")]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
struct Args {
    #[arg(long, required_unless_present = "file", conflicts_with = "file")]
    url: Option<String>,
    #[arg(long)]
    file: Option<PathBuf>,
    /// Explicit trust for public Old Faithful sources without a strong ETag.
    #[arg(long, requires = "url")]
    operator_trusted: bool,
    /// Canonical prefix plan from extract_index_plan.py, starting at epoch start.
    #[arg(long)]
    plan: PathBuf,
    #[arg(long, default_value_t = 12)]
    workers: usize,
    /// New output file. Never overwrites an earlier receipt.
    #[arg(long)]
    output: PathBuf,
}

#[derive(Debug, PartialEq, Eq)]
struct Row {
    slot: u64,
    tx: u64,
    votes: u64,
    failed: u64,
    digest: String,
}

#[derive(Default)]
struct Decoder {
    transaction: VersionedTransactionReuse,
    metadata: TransactionStatusMeta,
    zstd: ZstdReusableDecoder,
    rewards: Rewards,
}
impl Decoder {
    fn decode(&mut self, raw: &OrderedLosslessCarBlock) -> Result<Row> {
        let slot = raw.block.as_ref().context("terminal block missing")?.slot;
        let mut votes = 0;
        let mut failed = 0;
        let mut digest = blake3::Hasher::new();
        let vote_key: [u8; 32] = bs58::decode("Vote111111111111111111111111111111111111111")
            .into_vec()?
            .try_into()
            .unwrap();
        for (index, raw_tx) in raw.transactions.iter().enumerate() {
            ensure!(
                raw_tx.slot == slot && raw_tx.index == Some(index as u64),
                "transaction order differs at slot {slot}"
            );
            ensure!(
                raw_tx.data.next.is_empty() && raw_tx.metadata.next.is_empty(),
                "transaction continuation requires the general lossless reader"
            );
            let tx = self
                .transaction
                .deserialize_transaction(&raw_tx.data.data)?;
            ensure!(!tx.signatures.is_empty(), "transaction has no signature");
            decode_transaction_status_meta_from_frame(
                slot,
                &raw_tx.metadata.data,
                &mut self.metadata,
                &mut self.zstd,
            )?;
            let vote = match &tx.message {
                VersionedMessage::Legacy(m)
                    if (1..=2).contains(&tx.signatures.len()) && m.instructions.len() == 1 =>
                {
                    m.account_keys
                        .get(m.instructions[0].program_id_index as usize)
                        .is_some_and(|key| **key == vote_key)
                }
                _ => false,
            };
            // Same message hash domain and encoded message bytes as Solana's
            // VersionedMessage::hash; avoid serializing data already on the wire.
            let (count, prefix) = decode_shortu16_len(&raw_tx.data.data)
                .map_err(|_| anyhow::anyhow!("bad signature prefix"))?;
            let message = &raw_tx.data.data[prefix + count * 64..];
            let mut hash = blake3::Hasher::new();
            hash.update(b"solana-tx-message-v1");
            hash.update(message);
            digest.update(&slot.to_le_bytes());
            digest.update(&(index as u64).to_le_bytes());
            digest.update(tx.signatures[0]);
            digest.update(hash.finalize().as_bytes());
            digest.update(&[u8::from(vote), u8::from(self.metadata.err.is_some())]);
            votes += u64::from(vote);
            failed += u64::from(self.metadata.err.is_some());
            std::hint::black_box(&self.metadata);
            self.transaction.recycle_transaction(tx);
        }
        if let Some(rewards) = &raw.rewards {
            ensure!(
                rewards.data.next.is_empty(),
                "unresolved rewards continuation"
            );
            decode_rewards_from_frame(&rewards.data.data, &mut self.rewards, &mut self.zstd)?;
            std::hint::black_box(&self.rewards);
        }
        ensure!(
            raw.entries
                .iter()
                .map(|e| u64::from(e.transaction_count))
                .sum::<u64>()
                == raw.transactions.len() as u64,
            "entry counts differ"
        );
        Ok(Row {
            slot,
            tx: raw.transactions.len() as u64,
            votes,
            failed,
            digest: digest.finalize().to_hex().to_string(),
        })
    }
}

fn scan(input: impl Read, end_slot: u64, workers: usize) -> Result<Vec<Row>> {
    ensure!((1..=12).contains(&workers), "workers must be 1..=12");
    let mut reader = CarBlockReader::with_capacity(input, 8 << 20);
    reader.skip_header_bounded(1 << 20)?;
    let limits = LosslessBlockReadLimits {
        max_entry_payload_bytes: 16 << 20,
        max_block_payload_bytes: 16 << 20,
        max_entries_per_block: 65536,
        max_transactions_per_block: 65535,
    };
    // Exactly workers+2 reusable raw blocks. A large block above the stated
    // probe bound fails the run; it is never silently skipped.
    let buffers = workers + 2;
    let (free_tx, free_rx) = mpsc::sync_channel(buffers);
    let (work_tx, work_rx) = mpsc::sync_channel(buffers);
    let work_rx = Mutex::new(work_rx);
    let cancel = AtomicBool::new(false);
    for _ in 0..buffers {
        free_tx.send(OrderedLosslessCarBlock::default())?;
    }
    thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..workers {
            let free = free_tx.clone();
            let work = &work_rx;
            let cancel = &cancel;
            handles.push(scope.spawn(move || -> Result<Vec<Row>> {
                let mut decoder = Decoder::default();
                let mut rows = Vec::new();
                loop {
                    let next = {
                        work.lock()
                            .map_err(|_| anyhow::anyhow!("input queue poisoned"))?
                            .recv()
                    };
                    let Ok(mut block) = next else {
                        break;
                    };
                    if cancel.load(Ordering::Acquire) {
                        break;
                    }
                    let result = decoder.decode(&block);
                    block.clear();
                    if block.data_buffer_pool_stats().retained_capacity > 16 << 20 {
                        block.release_reusable_data_buffers();
                    }
                    if result.is_err() {
                        cancel.store(true, Ordering::Release);
                    }
                    if free.send(block).is_err() {
                        break;
                    }
                    rows.push(result?);
                }
                Ok(rows)
            }));
        }
        drop(free_tx);
        let input_result = (|| -> Result<()> {
            loop {
                if cancel.load(Ordering::Acquire) {
                    break;
                }
                let mut block = free_rx.recv().context("all decode workers stopped")?;
                if cancel.load(Ordering::Acquire) {
                    break;
                }
                if !reader.read_until_block_ordered_lossless_bounded(&mut block, limits)? {
                    break;
                }
                let slot = block.block.as_ref().context("missing block")?.slot;
                if slot >= end_slot {
                    break;
                }
                work_tx.send(block).context("decode input closed")?;
                if slot + 1 == end_slot {
                    break;
                }
            }
            Ok(())
        })();
        drop(work_tx);
        let mut rows = Vec::new();
        let mut error = input_result.err();
        for handle in handles {
            match handle.join() {
                Ok(Ok(mut batch)) => rows.append(&mut batch),
                Ok(Err(e)) => {
                    error.get_or_insert(e);
                }
                Err(_) => {
                    error.get_or_insert_with(|| anyhow::anyhow!("decode worker panicked"));
                }
            }
        }
        if let Some(e) = error {
            return Err(e);
        }
        rows.sort_unstable_by_key(|row| row.slot);
        ensure!(
            rows.windows(2).all(|r| r[0].slot < r[1].slot),
            "duplicate or unordered blocks"
        );
        Ok(rows)
    })
}

fn main() -> Result<()> {
    let args = Args::parse();
    ensure!((1..=12).contains(&args.workers), "workers must be 1..=12");
    let plan: Value = serde_json::from_reader(File::open(&args.plan)?)?;
    let epoch = plan["epoch"].as_u64().context("plan epoch")?;
    let end = plan["end_slot_exclusive"].as_u64().context("plan end")?;
    ensure!(
        plan["start_slot"].as_u64() == epoch.checked_mul(432000),
        "only epoch prefixes are supported"
    );
    let expected: Vec<[u64; 2]> = serde_json::from_value(plan["block_transaction_rows"].clone())?;
    ensure!(
        !expected.is_empty() && expected.windows(2).all(|r| r[0][0] < r[1][0]),
        "invalid plan order"
    );
    ensure!(
        expected.last().unwrap()[0].checked_add(1) == Some(end),
        "plan end differs"
    );
    let mut output = File::options()
        .write(true)
        .create_new(true)
        .open(&args.output)?;
    let started = Instant::now();
    let mut transport = None;
    let mut identity = json!(null);
    let input: Box<dyn Read> = if let Some(url) = &args.url {
        let session = CarHttpSession::new(CarHttpOptions::default())?;
        if args.operator_trusted {
            let stream = session.open_operator_trusted(url)?;
            identity = json!({"url": stream.identity().normalized_url, "bytes": stream.identity().content_length, "operator_trusted": true});
            transport = Some(stream.stats_handle());
            Box::new(stream)
        } else {
            let stream = session.open(url)?;
            identity = json!({"url": stream.identity().normalized_url, "bytes": stream.identity().content_length, "etag": stream.identity().strong_etag, "operator_trusted": false});
            transport = Some(stream.stats_handle());
            Box::new(stream)
        }
    } else {
        Box::new(File::open(args.file.as_ref().unwrap())?)
    };
    let setup_s = started.elapsed().as_secs_f64();
    let scan_start = Instant::now();
    let result = scan(input, end, args.workers);
    let scan_s = scan_start.elapsed().as_secs_f64();
    let rows = match result {
        Ok(rows) => rows,
        Err(error) => {
            serde_json::to_writer_pretty(
                &mut output,
                &json!({"valid": false, "error": format!("{error:#}"), "scan_seconds": scan_s}),
            )?;
            return Err(error);
        }
    };
    let by_slot: BTreeMap<_, _> = rows.iter().map(|r| (r.slot, r)).collect();
    let expected_by_slot: BTreeMap<_, _> = expected.iter().map(|r| (r[0], r[1])).collect();
    let valid = rows
        .iter()
        .all(|r| expected_by_slot.get(&r.slot) == Some(&r.tx))
        && expected.iter().all(|r| {
            by_slot
                .get(&r[0])
                .map_or(r[1] == 0, |actual| actual.tx == r[1])
        });
    let tx = rows.iter().map(|r| r.tx).sum::<u64>();
    let http = transport.map(|h| { let s = h.snapshot(); json!({"get_requests":s.get_requests,"body_bytes":s.get_body_bytes_received,"bytes_delivered":s.bytes_delivered,"incomplete_body_retries":s.incomplete_body_retries,"workers_finished":s.workers_finished}) });
    let receipt = json!({"schema":"blockzilla-car-decode-reference-v1", "valid":valid, "epoch":epoch,
        "allocator": if cfg!(feature="reference-mimalloc") {"mimalloc"} else {"system"},
        "workers":args.workers, "setup_seconds":setup_s, "scan_seconds":scan_s,
        "transactions":tx, "blocks":expected.len(), "physical_blocks":rows.len(),
        "scan_tps": if valid {Some(tx as f64 / scan_s)} else {None},
        "votes":rows.iter().map(|r|r.votes).sum::<u64>(),"failed":rows.iter().map(|r|r.failed).sum::<u64>(),
        "identity":identity,"http":http,"signature_verification":false,
        "block_transaction_rows":expected,"block_decode_digests":rows.iter().map(|r|json!([r.slot,r.digest])).collect::<Vec<_>>()});
    serde_json::to_writer_pretty(&mut output, &receipt)?;
    writeln!(output)?;
    ensure!(valid, "decoded rows do not match the canonical plan");
    println!("{tx} transactions in {scan_s:.3} s; exact block counts passed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn real_car_decode_is_identical_with_one_and_twelve_workers() {
        for bytes in [include_bytes!("../../../../crates/old-faithful/of-car-reader/benches/fixtures/epoch-157-biggest.car").as_slice(),
            include_bytes!("../../../../crates/old-faithful/of-car-reader/benches/fixtures/epoch-822-biggest.car").as_slice()] {
            let one = scan(bytes, u64::MAX, 1).unwrap();
            let twelve = scan(bytes, u64::MAX, 12).unwrap();
            assert!(!one.is_empty()); assert_eq!(one, twelve);
        }
    }
}
