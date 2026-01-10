use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use clap::Parser;
use tracing::{info, Level};

use car_reader::error::{CarReadError, CarReadResult};
use car_reader::node::is_block_node;

const MAX_UVARINT_LEN_64: usize = 10;
const CID_LEN: usize = 36;

const BLOCKS_PER_EPOCH: u64 = 432_000;

// Deterministic no-alloc hot loop contract: biggest block payload must fit in this.
// (Allocations happen once at startup to create the fixed buffers.)
const FIXED_PAYLOAD_CAP: usize = 8 << 20; // 8 MiB

/// Minimal valid CARv1 header with empty roots:
/// { "version": 1, "roots": [] }
const CARV1_EMPTY_HEADER_CBOR: &[u8] = &[
    0xa2, 0x67, b'v', b'e', b'r', b's', b'i', b'o', b'n', 0x01, 0x65, b'r', b'o', b'o', b't',
    b's', 0x80,
];

#[derive(Parser, Debug)]
#[command(
    name = "build-test-file",
    about = "Build a CAR containing the biggest block node of an epoch (no CarStream/CarBlockGroup)"
)]
struct Args {
    /// Epoch to scan
    #[arg(long)]
    epoch: u64,

    /// Cache directory containing epoch-<N>.car(.zst) files
    #[arg(long, default_value = "epochs")]
    cache_dir: PathBuf,

    /// Base URL for fetching epoch-<N>.car files
    #[arg(long, default_value = "https://files.old-faithful.net")]
    base_url: String,

    /// Fetch .car.zst from remote instead of .car
    #[arg(long, default_value_t = false)]
    remote_zst: bool,

    /// Output CAR file path (defaults to epoch-<epoch>-biggest.car)
    #[arg(long)]
    output: Option<PathBuf>,

    /// Buffer size for reading input (bytes)
    #[arg(long, default_value_t = 128 << 20)]
    buf_size: usize,

    /// Log progress every N seconds (0 = disable)
    #[arg(long, default_value_t = 2)]
    progress_every: u64,
}

fn main() -> CarReadResult<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    let args = Args::parse();

    let output_path = args
        .output
        .clone()
        .unwrap_or_else(|| PathBuf::from(format!("epoch-{}-biggest.car", args.epoch)));

    info!(
        "scanning epoch {} for biggest block node (fixed payload cap={} MiB)",
        args.epoch,
        FIXED_PAYLOAD_CAP / (1 << 20)
    );

    let reader = open_epoch_reader(&args, args.epoch)?;
    let best = scan_biggest_block_noalloc(reader, args.epoch, args.buf_size, args.progress_every)?;

    info!(
        "epoch {}: biggest block payload={} bytes (entry_len={})",
        args.epoch,
        best.best_payload_len,
        CID_LEN + best.best_payload_len
    );

    let mut out = File::create(&output_path)
        .map_err(|e| CarReadError::Io(format!("create {}: {e}", output_path.display())))?;

    // Valid empty CAR header
    write_uvarint64(&mut out, CARV1_EMPTY_HEADER_CBOR.len() as u64)?;
    out.write_all(CARV1_EMPTY_HEADER_CBOR)
        .map_err(|e| CarReadError::Io(format!("write header: {e}")))?;

    // Single entry: varint(entry_len) + cid + payload
    let entry_len = CID_LEN + best.best_payload_len;
    write_uvarint64(&mut out, entry_len as u64)?;
    out.write_all(&best.best_cid)
        .map_err(|e| CarReadError::Io(format!("write cid: {e}")))?;
    out.write_all(best.best_payload())
        .map_err(|e| CarReadError::Io(format!("write payload: {e}")))?;

    out.flush()
        .map_err(|e| CarReadError::Io(format!("flush {}: {e}", output_path.display())))?;

    info!("wrote {}", output_path.display());
    Ok(())
}

fn open_epoch_reader(args: &Args, epoch: u64) -> CarReadResult<Box<dyn Read>> {
    let zst_path = args.cache_dir.join(format!("epoch-{}.car.zst", epoch));
    let car_path = args.cache_dir.join(format!("epoch-{}.car", epoch));

    if zst_path.is_file() {
        info!("epoch {}: using cache {}", epoch, zst_path.display());
        let file = File::open(&zst_path)
            .map_err(|e| CarReadError::Io(format!("open {}: {e}", zst_path.display())))?;
        let decoder =
            zstd::Decoder::new(file).map_err(|_| CarReadError::InvalidData("zstd decoder init failed".to_string()))?;
        return Ok(Box::new(decoder));
    }

    if car_path.is_file() {
        info!("epoch {}: using cache {}", epoch, car_path.display());
        let file = File::open(&car_path)
            .map_err(|e| CarReadError::Io(format!("open {}: {e}", car_path.display())))?;
        return Ok(Box::new(file));
    }

    info!("epoch {}: fetching from network", epoch);
    let url = format!(
        "{}/{}/epoch-{}.car{}",
        args.base_url,
        epoch,
        epoch,
        if args.remote_zst { ".zst" } else { "" }
    );

    let client = reqwest::blocking::Client::builder()
        .timeout(None)
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .build()
        .map_err(|e| CarReadError::Io(format!("build http client: {e}")))?;

    let resp = client
        .get(&url)
        .header(reqwest::header::ACCEPT_ENCODING, "identity")
        .send()
        .map_err(|e| CarReadError::Io(format!("GET {url}: {e}")))?
        .error_for_status()
        .map_err(|e| CarReadError::Io(format!("GET {url} (http error): {e}")))?;

    if args.remote_zst {
        let decoder =
            zstd::Decoder::new(resp).map_err(|_| CarReadError::InvalidData("zstd decoder init failed".to_string()))?;
        Ok(Box::new(decoder))
    } else {
        Ok(Box::new(resp))
    }
}

struct Progress {
    start: Instant,
    last_log: Instant,
    last_seen: u64,
    ema_bps: f64,
    every: Duration,
}

impl Progress {
    fn new(every_secs: u64) -> Self {
        let now = Instant::now();
        let every = if every_secs == 0 {
            Duration::from_secs(u64::MAX / 2)
        } else {
            Duration::from_secs(every_secs)
        };
        Self {
            start: now,
            last_log: now,
            last_seen: 0,
            ema_bps: 0.0,
            every,
        }
    }

    fn maybe_log(&mut self, epoch: u64, seen_blocks: u64) {
        let now = Instant::now();
        if now.duration_since(self.last_log) < self.every {
            return;
        }

        let dt = now.duration_since(self.last_log).as_secs_f64();
        let ds = (seen_blocks.saturating_sub(self.last_seen)) as f64;

        if dt > 0.0 {
            let inst_bps = ds / dt;
            let alpha = 0.2;
            self.ema_bps = if self.ema_bps == 0.0 {
                inst_bps
            } else {
                alpha * inst_bps + (1.0 - alpha) * self.ema_bps
            };
        }

        self.last_log = now;
        self.last_seen = seen_blocks;

        let pct = (seen_blocks as f64 / BLOCKS_PER_EPOCH as f64) * 100.0;
        let remaining = BLOCKS_PER_EPOCH.saturating_sub(seen_blocks);
        let eta_secs = if self.ema_bps > 0.0 {
            remaining as f64 / self.ema_bps
        } else {
            f64::NAN
        };

        let elapsed = now.duration_since(self.start).as_secs_f64();

        info!(
            "epoch {}: {}/{} blocks ({:.2}%) | {:.0} blocks/s | elapsed {} | ETA {}",
            epoch,
            seen_blocks,
            BLOCKS_PER_EPOCH,
            pct,
            self.ema_bps,
            fmt_secs(elapsed),
            fmt_secs(eta_secs),
        );
    }
}

fn fmt_secs(secs: f64) -> String {
    if !secs.is_finite() || secs < 0.0 {
        return "?".to_string();
    }
    let s = secs.round() as u64;
    let h = s / 3600;
    let m = (s % 3600) / 60;
    let ss = s % 60;
    if h > 0 {
        format!("{h}h{m:02}m{ss:02}s")
    } else if m > 0 {
        format!("{m}m{ss:02}s")
    } else {
        format!("{ss}s")
    }
}

/// Discard exactly `n` bytes without allocating.
fn discard_exact<R: Read>(r: &mut R, mut n: usize) -> CarReadResult<()> {
    let mut tmp = [0u8; 64 * 1024];
    while n > 0 {
        let take = n.min(tmp.len());
        r.read_exact(&mut tmp[..take])?;
        n -= take;
    }
    Ok(())
}

struct BestFixedPayload {
    // Two fixed payload buffers, allocated once.
    // We store payload only (not CID) to keep cap focused.
    buf0: Vec<u8>,
    buf1: Vec<u8>,

    best_idx: usize,
    best_payload_len: usize,
    best_cid: [u8; CID_LEN],
}

impl BestFixedPayload {
    fn new() -> Self {
        // Allocate once. Length is FIXED_PAYLOAD_CAP so we can slice without resizing.
        Self {
            buf0: vec![0u8; FIXED_PAYLOAD_CAP],
            buf1: vec![0u8; FIXED_PAYLOAD_CAP],
            best_idx: 0,
            best_payload_len: 0,
            best_cid: [0u8; CID_LEN],
        }
    }

    fn payload_mut(&mut self, idx: usize) -> &mut [u8] {
        if idx == 0 {
            &mut self.buf0[..]
        } else {
            &mut self.buf1[..]
        }
    }

    fn best_payload(&self) -> &[u8] {
        if self.best_idx == 0 {
            &self.buf0[..self.best_payload_len]
        } else {
            &self.buf1[..self.best_payload_len]
        }
    }
}

/// Fast scanner:
/// - Skip header
/// - For each entry: read CID, read 3 bytes of payload
/// - Call `is_block_node(&prefix3)` (works if is_block_node is just a prefix check)
/// - If not block: discard rest
/// - If block: if payload_len can beat best, read rest into one of 2 fixed buffers and update best_idx
///
/// No per-entry allocations, no unsafe.
fn scan_biggest_block_noalloc<R: Read>(
    reader: R,
    epoch: u64,
    buf_size: usize,
    progress_every: u64,
) -> CarReadResult<BestFixedPayload> {
    let mut reader = BufReader::with_capacity(buf_size, reader);

    // Skip CAR header
    let header_len = read_uvarint64(&mut reader)? as usize;
    if header_len > 0 {
        discard_exact(&mut reader, header_len)?;
    }

    let mut prog = Progress::new(progress_every);
    let mut best = BestFixedPayload::new();

    let mut cur_idx: usize = 0;
    let mut seen_blocks: u64 = 0;
    let mut total_entries: u64 = 0;

    let mut cid = [0u8; CID_LEN];
    let mut prefix = [0u8; 3];

    loop {
        let entry_len = match read_uvarint64(&mut reader) {
            Ok(v) => v as usize,
            Err(CarReadError::Eof) => break,
            Err(e) => return Err(e),
        };

        total_entries += 1;

        if entry_len == 0 {
            return Err(CarReadError::InvalidEntryLen("entry len 0".to_string()));
        }
        if entry_len <= CID_LEN {
            return Err(CarReadError::InvalidEntryLen(format!(
                "entry smaller than cid ({entry_len})"
            )));
        }

        reader.read_exact(&mut cid)?;

        let payload_len = entry_len - CID_LEN;
        if payload_len < 3 {
            // Too small, just discard payload.
            if payload_len > 0 {
                discard_exact(&mut reader, payload_len)?;
            }
            continue;
        }

        // Read 3-byte prefix and decide
        reader.read_exact(&mut prefix)?;
        if !is_block_node(&prefix) {
            discard_exact(&mut reader, payload_len - 3)?;
            continue;
        }

        // It's a block node.
        seen_blocks += 1;
        prog.maybe_log(epoch, seen_blocks);

        // If it cannot beat the best, discard remaining payload and continue.
        if payload_len <= best.best_payload_len {
            discard_exact(&mut reader, payload_len - 3)?;
            continue;
        }

        if payload_len > FIXED_PAYLOAD_CAP {
            return Err(CarReadError::InvalidData(format!(
                "block payload_len {} exceeds fixed cap {} (increase FIXED_PAYLOAD_CAP)",
                payload_len, FIXED_PAYLOAD_CAP
            )));
        }

        // Ensure we never overwrite the best buffer.
        if best.best_payload_len != 0 && cur_idx == best.best_idx {
            cur_idx ^= 1;
        }

        // Read remaining payload bytes into current buffer.
        let dst = best.payload_mut(cur_idx);
        dst[..3].copy_from_slice(&prefix);
        reader.read_exact(&mut dst[3..payload_len])?;

        // Update best pointers.
        best.best_idx = cur_idx;
        best.best_payload_len = payload_len;
        best.best_cid = cid;

        // Next time, flip buffer.
        cur_idx ^= 1;
    }

    info!(
        "epoch {}: scan done | entries={} block_nodes_seen={}",
        epoch, total_entries, seen_blocks
    );

    if best.best_payload_len == 0 {
        return Err(CarReadError::InvalidData(
            "no block nodes found in epoch (is_block_node never matched?)".to_string(),
        ));
    }

    Ok(best)
}

fn read_uvarint64<R: BufRead>(r: &mut R) -> CarReadResult<u64> {
    let mut x: u64 = 0;
    let mut shift: u32 = 0;
    let mut i: usize = 0;

    loop {
        if i >= MAX_UVARINT_LEN_64 {
            return Err(CarReadError::VarintOverflow("uvarint overflow".to_string()));
        }

        let buf = r.fill_buf().map_err(|e| CarReadError::Io(e.to_string()))?;
        if buf.is_empty() {
            if x != 0 {
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
                return Ok(x);
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

fn write_uvarint64<W: Write>(w: &mut W, mut x: u64) -> CarReadResult<()> {
    let mut buf = [0u8; MAX_UVARINT_LEN_64];
    let mut i = 0usize;

    while x >= 0x80 {
        buf[i] = (x as u8) | 0x80;
        x >>= 7;
        i += 1;
    }

    buf[i] = x as u8;
    i += 1;

    w.write_all(&buf[..i])
        .map_err(|e| CarReadError::Io(e.to_string()))?;
    Ok(())
}
