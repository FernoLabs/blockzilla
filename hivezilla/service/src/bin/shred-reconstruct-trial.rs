use anyhow::{Context, Result};
use clap::Parser;
use hivezilla::ingest::{
    ReplicationStreamId, ShredSpoolTrialConfig, SpoolJournalIdentity,
    read_receiver_durable_progress, trial_deshred_spool,
};

#[derive(Debug, Parser)]
#[command(about = "Read-only reconstruction trial against a durable raw-shred spool")]
struct Args {
    #[arg(long)]
    spool_root: std::path::PathBuf,
    #[arg(long)]
    cluster_id: String,
    #[arg(long)]
    origin_node_id: String,
    #[arg(long)]
    source_id: String,
    #[arg(long, value_parser = parse_journal_id)]
    journal_id: [u8; 16],
    /// Receiver progress WAL. When supplied, its fsynced cursor bounds the read.
    #[arg(long, conflicts_with = "durable_through_sequence")]
    receiver_progress_wal: Option<std::path::PathBuf>,
    /// Explicit durable cursor, for an offline copied spool only.
    #[arg(long, conflicts_with = "receiver_progress_wal")]
    durable_through_sequence: Option<u64>,
    #[arg(long, default_value_t = 4096)]
    max_record_bytes: u64,
    #[arg(long, default_value_t = 100_000)]
    max_records: usize,
    #[arg(long, default_value_t = 256)]
    max_candidate_slots: usize,
    /// Ignore older logical slots while scanning a durable spool prefix.
    #[arg(long)]
    min_slot: Option<u64>,
    /// Include at most this many representative slots per failure category in the JSON report.
    #[arg(long, default_value_t = 16)]
    max_failure_samples: usize,
}

fn parse_journal_id(value: &str) -> Result<[u8; 16], String> {
    if value.len() != 32 {
        return Err("journal id must be exactly 32 hexadecimal characters".into());
    }
    let mut journal_id = [0u8; 16];
    for (index, byte) in journal_id.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| "journal id must be hexadecimal")?;
    }
    Ok(journal_id)
}

fn main() -> Result<()> {
    let args = Args::parse();
    let stream = ReplicationStreamId {
        cluster_id: args.cluster_id.clone(),
        origin_node_id: args.origin_node_id.clone(),
        source_id: args.source_id.clone(),
        journal_id: args.journal_id,
    };
    let durable_through_sequence = match (args.receiver_progress_wal, args.durable_through_sequence)
    {
        (Some(progress_wal), None) => {
            read_receiver_durable_progress(progress_wal, &stream)?
                .context("receiver progress WAL has no durable records")?
                .through_sequence
        }
        (None, Some(sequence)) => sequence,
        (None, None) => anyhow::bail!(
            "provide --receiver-progress-wal for a live receiver, or --durable-through-sequence for an offline spool"
        ),
        (Some(_), Some(_)) => unreachable!("clap enforces conflicting cursor options"),
    };
    let report = trial_deshred_spool(ShredSpoolTrialConfig {
        spool_root: args.spool_root,
        identity: SpoolJournalIdentity {
            cluster_id: args.cluster_id,
            origin_node_id: args.origin_node_id,
            source_id: args.source_id,
            journal_id: args.journal_id,
        },
        durable_through_sequence,
        max_record_bytes: args.max_record_bytes,
        max_records: args.max_records,
        max_candidate_slots: args.max_candidate_slots,
        min_slot: args.min_slot,
        max_failure_samples: args.max_failure_samples,
    })?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report).context("encode trial report")?
    );
    Ok(())
}
