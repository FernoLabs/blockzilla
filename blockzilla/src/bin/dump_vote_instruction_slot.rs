use std::{
    fs::{self, File},
    io::BufReader,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow};
use bincode::Options;
use clap::Parser;
use data_encoding::HEXLOWER;
use of_car_reader::{
    CarBlockReader,
    reconstruct::LosslessCarBlock,
    versioned_transaction::{CompiledInstruction, VersionedMessage, VersionedTransaction},
};
use solana_vote_interface::instruction::VoteInstruction;

const BUFFER_SIZE: usize = 256 << 20;
const VOTE_INSTRUCTION_DECODE_LIMIT: u64 = 1232;

#[derive(Parser, Debug)]
#[command(
    name = "dump-vote-instruction-slot",
    about = "Dump vote instruction payloads from one slot in an Old Faithful CAR/CAR.ZST"
)]
struct Args {
    /// Input CAR or CAR.ZST file.
    input: PathBuf,

    /// Absolute slot to inspect.
    #[arg(long)]
    slot: u64,

    /// Output directory for TSV and binary payload fixtures.
    #[arg(long)]
    output_dir: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    fs::create_dir_all(&args.output_dir)
        .with_context(|| format!("create {}", args.output_dir.display()))?;

    let input = open_input(&args.input)?;
    let mut reader = CarBlockReader::with_capacity(input, BUFFER_SIZE);
    reader
        .skip_header()
        .with_context(|| format!("skip CAR header in {}", args.input.display()))?;

    let mut group = LosslessCarBlock::default();
    let mut blocks = 0u64;
    while reader
        .read_until_block_lossless(&mut group)
        .with_context(|| format!("read block after {blocks} blocks"))?
    {
        blocks += 1;
        let block = group
            .block
            .as_ref()
            .ok_or_else(|| anyhow!("lossless scanner returned block group without block node"))?;
        if block.slot < args.slot {
            continue;
        }
        if block.slot > args.slot {
            anyhow::bail!(
                "slot {} not found; stopped at later slot {} after {} blocks",
                args.slot,
                block.slot,
                blocks
            );
        }

        return dump_slot(&args.output_dir, &group, args.slot, blocks);
    }

    anyhow::bail!(
        "slot {} not found; reached EOF after {blocks} blocks",
        args.slot
    );
}

fn open_input(path: &Path) -> Result<Box<dyn std::io::Read>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::with_capacity(BUFFER_SIZE, file);
    if path.extension().and_then(|ext| ext.to_str()) == Some("zst") {
        Ok(Box::new(
            zstd::stream::read::Decoder::new(reader)
                .with_context(|| format!("open zstd decoder for {}", path.display()))?,
        ))
    } else {
        Ok(Box::new(reader))
    }
}

fn dump_slot(
    output_dir: &Path,
    group: &LosslessCarBlock,
    slot: u64,
    blocks_seen: u64,
) -> Result<()> {
    let vote_program = solana_pubkey::pubkey!("Vote111111111111111111111111111111111111111");
    let vote_program = vote_program.to_bytes();
    let mut rows = String::from(
        "slot\tblocks_seen\ttx_ordinal\ttx_index\tix_index\tsignature0_hex\tvariant\tlen\tdecode_status\tpayload_hex\tfile\n",
    );
    let mut tx_bytes = Vec::new();
    let mut visited = std::collections::HashSet::new();
    let mut dumped = 0usize;

    for (tx_ordinal, raw_tx) in group.transactions.iter().enumerate() {
        raw_tx
            .transaction_bytes_into(&group.dataframes, &mut tx_bytes, &mut visited)
            .with_context(|| format!("slot {slot} tx#{tx_ordinal} reassemble transaction"))?;
        let versioned = wincode::deserialize::<VersionedTransaction<'_>>(&tx_bytes)
            .map_err(|err| anyhow!("{err}"))
            .with_context(|| format!("slot {slot} tx#{tx_ordinal} decode transaction"))?;

        let signature0_hex = versioned
            .signatures
            .first()
            .map(|sig| HEXLOWER.encode(sig.as_slice()))
            .unwrap_or_default();

        for (ix_index, ix) in message_instructions(&versioned.message).iter().enumerate() {
            let Some(program_id) = message_account_key(&versioned.message, ix.program_id_index)
            else {
                continue;
            };
            if program_id != vote_program.as_slice() {
                continue;
            }

            let tx_file = format!("slot-{slot}-tx-{tx_ordinal}.bin");
            let ix_file = format!("slot-{slot}-tx-{tx_ordinal}-ix-{ix_index}-vote.bin");
            if !output_dir.join(&tx_file).exists() {
                fs::write(output_dir.join(&tx_file), &tx_bytes)
                    .with_context(|| format!("write {}", output_dir.join(&tx_file).display()))?;
            }
            fs::write(output_dir.join(&ix_file), &ix.data)
                .with_context(|| format!("write {}", output_dir.join(&ix_file).display()))?;

            let decode_status = canonical_vote_decode_status(&ix.data);
            rows.push_str(&format!(
                "{slot}\t{blocks_seen}\t{tx_ordinal}\t{}\t{ix_index}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                raw_tx
                    .index
                    .map(|index| index.to_string())
                    .unwrap_or_else(|| tx_ordinal.to_string()),
                signature0_hex,
                vote_instruction_variant_label(&ix.data),
                ix.data.len(),
                decode_status,
                HEXLOWER.encode(&ix.data),
                ix_file
            ));
            dumped += 1;
        }
    }

    fs::write(output_dir.join("vote-instructions.tsv"), rows).with_context(|| {
        format!(
            "write {}",
            output_dir.join("vote-instructions.tsv").display()
        )
    })?;

    println!(
        "slot={slot} blocks_seen={blocks_seen} txs={} vote_instructions={} out={}",
        group.transactions.len(),
        dumped,
        output_dir.display()
    );
    Ok(())
}

fn message_account_key<'a>(message: &'a VersionedMessage<'a>, index: u8) -> Option<&'a [u8; 32]> {
    match message {
        VersionedMessage::Legacy(message) => message.account_keys.get(index as usize).copied(),
        VersionedMessage::V0(message) => message.account_keys.get(index as usize).copied(),
    }
}

fn message_instructions<'a>(message: &'a VersionedMessage<'a>) -> &'a [CompiledInstruction] {
    match message {
        VersionedMessage::Legacy(message) => &message.instructions,
        VersionedMessage::V0(message) => &message.instructions,
    }
}

fn canonical_vote_decode_status(data: &[u8]) -> String {
    let result: Result<VoteInstruction, _> = bincode::options()
        .with_limit(VOTE_INSTRUCTION_DECODE_LIMIT)
        .with_fixint_encoding()
        .reject_trailing_bytes()
        .deserialize(data);
    match result {
        Ok(instruction) => format!("ok:{instruction:?}"),
        Err(err) => format!("err:{err}"),
    }
}

fn vote_instruction_variant_label(data: &[u8]) -> String {
    if data.len() < 4 {
        return "short".to_string();
    }
    u32::from_le_bytes([data[0], data[1], data[2], data[3]]).to_string()
}
