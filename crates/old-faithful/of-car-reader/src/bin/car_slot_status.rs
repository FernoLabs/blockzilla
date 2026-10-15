//! Write one `jetstreamer_slot_status` row per CAR block.
//!
//! The row matches Jetstreamer's `--no-plugins` table: slot, transaction
//! count, vote count, non-vote count, thread id, and block time.
//!
//! A local CAR uses `--input`. An HTTPS CAR uses `--url`. Public
//! `files.old-faithful.net` objects have no strong ETag, so that source also
//! needs `--operator-trusted`.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::time::Instant;

use clap::Parser;
use of_car_reader::clickhouse_slot_status::SlotStatusSink;
use of_car_reader::query_sdk_http::{
    CarHttpOptions, CarHttpSession, DEFAULT_HTTP_CHUNK_BYTES, DEFAULT_HTTP_WINDOW_CHUNKS,
    DEFAULT_HTTP_WORKERS,
};
use of_car_reader::slot_status::slot_status_from_group;
use of_car_reader::{CarBlockGroup, CarBlockReader};

const CAR_BUF: usize = 128 << 20;

#[derive(Parser, Debug)]
#[command(
    name = "car-slot-status",
    about = "Fill ClickHouse jetstreamer_slot_status from an Old Faithful CAR"
)]
struct Args {
    /// Local CAR file, `.car` or `.car.zst`.
    #[arg(long, required_unless_present = "url", conflicts_with = "url")]
    input: Option<PathBuf>,

    /// HTTPS CAR object, for example `https://files.old-faithful.net/900/epoch-900.car`.
    #[arg(long, required_unless_present = "input", conflicts_with = "input")]
    url: Option<String>,

    /// Admit one HTTPS CAR that has no strong ETag.
    ///
    /// Required for `files.old-faithful.net`. Omit it when the source returns a
    /// strong ETag on HEAD and on every range response.
    #[arg(long, requires = "url", conflicts_with = "input")]
    operator_trusted: bool,

    /// Parallel HTTPS range requests. The default is 4 and the maximum is 16.
    ///
    /// One worker matches `JETSTREAMER_THREADS=1`: a single range is downloaded
    /// at a time. The value cannot exceed `--http-window-chunks`.
    #[arg(long, requires = "url", conflicts_with = "input")]
    http_workers: Option<usize>,

    /// Maximum number of range bodies kept in memory. The default is 8 and the
    /// maximum is 16.
    #[arg(long, requires = "url", conflicts_with = "input")]
    http_window_chunks: Option<usize>,

    /// Bytes in one range, in mebibytes. The default and the maximum are 32.
    #[arg(long, requires = "url", conflicts_with = "input")]
    http_chunk_mib: Option<usize>,

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
    let summary = match (&args.input, &args.url) {
        (Some(input), None) => {
            scan_file(
                input,
                &mut sink,
                args.thread_id,
                &mut blocks,
                &mut transactions,
            )
            .await?
        }
        (None, Some(url)) => {
            scan_http(
                &url,
                args.operator_trusted,
                args.http_options()?,
                &mut sink,
                args.thread_id,
                &mut blocks,
                &mut transactions,
            )
            .await?
        }
        _ => return Err("choose one of --input or --url".into()),
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

async fn scan_file(
    input: &std::path::Path,
    sink: &mut SlotStatusSink,
    thread_id: u8,
    blocks: &mut u64,
    transactions: &mut u64,
) -> Result<ScanSummary, Box<dyn std::error::Error>> {
    eprintln!("source={}", input.display());
    if is_zstd(input) {
        let file = File::open(input)?;
        let file = BufReader::with_capacity(CAR_BUF, file);
        let decoder = zstd::Decoder::with_buffer(file)?;
        scan(decoder, sink, thread_id, blocks, transactions).await
    } else {
        let file = File::open(input)?;
        let file = BufReader::with_capacity(CAR_BUF, file);
        scan(file, sink, thread_id, blocks, transactions).await
    }
}

impl Args {
    fn http_options(&self) -> Result<CarHttpOptions, Box<dyn std::error::Error>> {
        let chunk_mib = self.http_chunk_mib.unwrap_or(DEFAULT_HTTP_CHUNK_BYTES >> 20);
        let chunk_bytes = chunk_mib
            .checked_mul(1 << 20)
            .ok_or("http-chunk-mib overflows the byte count")?;
        let options = CarHttpOptions {
            workers: self.http_workers.unwrap_or(DEFAULT_HTTP_WORKERS),
            window_chunks: self.http_window_chunks.unwrap_or(DEFAULT_HTTP_WINDOW_CHUNKS),
            chunk_bytes,
            ..CarHttpOptions::default()
        };
        options.body_window_bytes()?;
        Ok(options)
    }
}

async fn scan_http(
    url: &str,
    operator_trusted: bool,
    options: CarHttpOptions,
    sink: &mut SlotStatusSink,
    thread_id: u8,
    blocks: &mut u64,
    transactions: &mut u64,
) -> Result<ScanSummary, Box<dyn std::error::Error>> {
    eprintln!(
        "http_workers={} http_window_chunks={} http_chunk_mib={} body_window_bytes={}",
        options.workers,
        options.window_chunks,
        options.chunk_bytes >> 20,
        options.body_window_bytes()?,
    );
    let session = CarHttpSession::new(options)?;
    if operator_trusted {
        let stream = session.open_operator_trusted(url)?;
        let identity = stream.identity();
        eprintln!(
            "source={} bytes={} operator_trusted=true",
            identity.normalized_url, identity.content_length
        );
        scan(stream, sink, thread_id, blocks, transactions).await
    } else {
        let stream = session.open(url)?;
        let identity = stream.identity();
        eprintln!(
            "source={} bytes={} operator_trusted=false",
            identity.normalized_url, identity.content_length
        );
        scan(stream, sink, thread_id, blocks, transactions).await
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(extra: &[&str]) -> Result<Args, clap::Error> {
        Args::try_parse_from(
            [
                "car_slot_status",
                "--clickhouse-dsn",
                "http://127.0.0.1:8123",
                "--database",
                "jetstreamer",
            ]
            .into_iter()
            .chain(extra.iter().copied()),
        )
    }

    #[test]
    fn local_input_is_accepted() {
        let args = parse(&["--input", "epoch-900.car"]).unwrap();
        assert_eq!(args.input.unwrap(), PathBuf::from("epoch-900.car"));
        assert!(args.url.is_none());
        assert!(!args.operator_trusted);
    }

    #[test]
    fn operator_trusted_old_faithful_url_is_accepted() {
        let args = parse(&[
            "--url",
            "https://files.old-faithful.net/900/epoch-900.car",
            "--operator-trusted",
        ])
        .unwrap();
        assert!(args.input.is_none());
        assert_eq!(
            args.url.as_deref(),
            Some("https://files.old-faithful.net/900/epoch-900.car")
        );
        assert!(args.operator_trusted);
    }

    #[test]
    fn url_without_trust_selects_the_etag_reader() {
        let args = parse(&["--url", "https://archive.example/900/epoch-900.car"]).unwrap();
        assert!(!args.operator_trusted);
    }

    #[test]
    fn input_and_url_conflict() {
        assert!(
            parse(&[
                "--input",
                "epoch-900.car",
                "--url",
                "https://files.old-faithful.net/900/epoch-900.car",
            ])
            .is_err()
        );
    }

    #[test]
    fn operator_trusted_requires_a_url() {
        assert!(parse(&["--input", "epoch-900.car", "--operator-trusted"]).is_err());
        assert!(parse(&["--operator-trusted"]).is_err());
    }

    #[test]
    fn a_source_is_required() {
        assert!(parse(&[]).is_err());
    }

    #[test]
    fn http_defaults_match_the_sdk() {
        let args = parse(&[
            "--url",
            "https://files.old-faithful.net/900/epoch-900.car",
            "--operator-trusted",
        ])
        .unwrap();
        let options = args.http_options().unwrap();
        assert_eq!(options.workers, DEFAULT_HTTP_WORKERS);
        assert_eq!(options.window_chunks, DEFAULT_HTTP_WINDOW_CHUNKS);
        assert_eq!(options.chunk_bytes, DEFAULT_HTTP_CHUNK_BYTES);
    }

    #[test]
    fn one_worker_and_sixteen_chunks_hold_512_mebibytes() {
        let args = parse(&[
            "--url",
            "https://files.old-faithful.net/900/epoch-900.car",
            "--operator-trusted",
            "--http-workers",
            "1",
            "--http-window-chunks",
            "16",
            "--http-chunk-mib",
            "32",
        ])
        .unwrap();
        let options = args.http_options().unwrap();
        assert_eq!(options.workers, 1);
        assert_eq!(options.window_chunks, 16);
        assert_eq!(options.chunk_bytes, 32 << 20);
        assert_eq!(options.body_window_bytes().unwrap(), 512 << 20);
    }

    #[test]
    fn http_flags_require_a_url() {
        assert!(parse(&["--input", "epoch-900.car", "--http-workers", "1"]).is_err());
    }

    #[test]
    fn http_caps_reject_values_past_the_sdk_limits() {
        let mut args = parse(&[
            "--url",
            "https://files.old-faithful.net/900/epoch-900.car",
            "--http-workers",
            "17",
        ])
        .unwrap();
        assert!(args.http_options().is_err());
        args.http_workers = Some(1);
        args.http_chunk_mib = Some(33);
        assert!(args.http_options().is_err());
        args.http_chunk_mib = Some(32);
        args.http_window_chunks = Some(1);
        args.http_workers = Some(4);
        assert!(args.http_options().is_err());
    }
}
