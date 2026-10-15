//! Write one `jetstreamer_slot_status` row per CAR block.
//!
//! The row matches Jetstreamer's `--no-plugins` table: slot, transaction
//! count, vote count, non-vote count, thread id, and block time.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::time::Instant;

use clap::Parser;
use of_car_reader::clickhouse_slot_status::SlotStatusSink;
use of_car_reader::slot_status::slot_status_from_group;
use of_car_reader::{CarBlockGroup, CarBlockReader};

const CAR_BUF: usize = 128 << 20;

#[derive(Parser, Debug)]
#[command(
    name = "car-slot-status",
    about = "Fill ClickHouse jetstreamer_slot_status from an Old Faithful CAR"
)]
struct Args {
    /// CAR file, `.car` or `.car.zst`.
    #[arg(long)]
    input: PathBuf,

    /// ClickHouse HTTP URL. User and password may be included.
    #[arg(long)]
    clickhouse_dsn: String,

    /// Database that receives `jetstreamer_slot_status`.
    #[arg(long)]
    database: String,

    /// Value stored in `thread_id`. A single reader uses 0.
    #[arg(long, default_value_t = 0)]
    thread_id: u8,

    /// Rows per ClickHouse insert.
    #[arg(long, default_value_t = 1000)]
    batch_rows: usize,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run(args))
}

async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let mut sink = SlotStatusSink::connect(&args.clickhouse_dsn, &args.database, args.batch_rows)?;
    sink.ensure_schema().await?;

    let started = Instant::now();
    let mut blocks = 0u64;
    let mut transactions = 0u64;
    let input = args.input.clone();
    let summary = if is_zstd(&input) {
        let file = File::open(&input)?;
        let file = BufReader::with_capacity(CAR_BUF, file);
        let decoder = zstd::Decoder::with_buffer(file)?;
        scan(decoder, &mut sink, args.thread_id, &mut blocks, &mut transactions).await?
    } else {
        let file = File::open(&input)?;
        let file = BufReader::with_capacity(CAR_BUF, file);
        scan(file, &mut sink, args.thread_id, &mut blocks, &mut transactions).await?
    };

    let written = sink.finish().await?;
    let elapsed = started.elapsed().as_secs_f64().max(1e-9);
    eprintln!(
        "database={} table=jetstreamer_slot_status blocks={blocks} rows={written} transactions={transactions} first_slot={} last_slot={} elapsed={elapsed:.1}s",
        args.database,
        summary.first_slot,
        summary.last_slot,
    );
    Ok(())
}

struct ScanSummary {
    first_slot: u64,
    last_slot: u64,
}

async fn scan<R: Read>(
    reader: R,
    sink: &mut SlotStatusSink,
    thread_id: u8,
    blocks: &mut u64,
    transactions: &mut u64,
) -> Result<ScanSummary, Box<dyn std::error::Error>> {
    let mut car = CarBlockReader::with_capacity(reader, CAR_BUF);
    car.skip_header()?;
    let mut group = CarBlockGroup::without_rewards();
    let mut summary = ScanSummary {
        first_slot: 0,
        last_slot: 0,
    };
    while car.read_until_block_into(&mut group)? {
        let row = slot_status_from_group(&mut group, thread_id)?;
        if *blocks == 0 {
            summary.first_slot = row.slot;
        }
        summary.last_slot = row.slot;
        *transactions += u64::from(row.transaction_count);
        *blocks += 1;
        if *blocks % 10_000 == 0 {
            eprintln!("blocks={blocks} slot={}", row.slot);
        }
        sink.push(row).await?;
    }
    Ok(summary)
}

fn is_zstd(path: &std::path::Path) -> bool {
    path.extension().and_then(|extension| extension.to_str()) == Some("zst")
}
