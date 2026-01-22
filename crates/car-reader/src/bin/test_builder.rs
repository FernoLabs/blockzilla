use std::env;
use std::fs::File;
use std::io::{BufReader, Read, Write};

const MAX_UVARINT_LEN_64: usize = 10;
const CID_LEN: usize = 36;

// For each epoch, skip the first N block groups (often empty reward/vote heavy),
// then copy the next M block groups.
const SKIP_BLOCKS_PER_EPOCH: u64 = 1;
const TAKE_BLOCKS_PER_EPOCH: u64 = 5;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        usage(&args[0]);
    }

    match args[1].as_str() {
        "range" => run_range(&args),
        "biggest" => run_biggest(&args),
        _ => usage(&args[0]),
    }
}

fn usage(bin: &str) -> ! {
    eprintln!("Usage:");
    eprintln!("  {} range   <start-epoch> <end-epoch>", bin);
    eprintln!("  {} biggest <epoch> <output.car>", bin);
    std::process::exit(1);
}

fn http_client() -> Result<reqwest::blocking::Client> {
    Ok(reqwest::blocking::Client::builder()
        .timeout(None)
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .build()?)
}

fn run_range(args: &[String]) -> Result<()> {
    if args.len() != 4 {
        usage(&args[0]);
    }

    let start_epoch: u64 = args[2].parse()?;
    let end_epoch: u64 = args[3].parse()?;
    if start_epoch > end_epoch {
        return Err("start-epoch must be <= end-epoch".into());
    }

    let out_path = format!("bench-{}-{}.car", start_epoch, end_epoch);
    let mut out = File::create(&out_path)?;
    write_uvarint64(&mut out, 0)?; // CAR header placeholder (matches your current format)

    let client = http_client()?;

    for epoch in start_epoch..=end_epoch {
        let url = format!(
            "https://files.old-faithful.net/{}/epoch-{}.car",
            epoch, epoch
        );
        println!("epoch {} -> {}", epoch, url);

        let resp = client.get(&url).send()?.error_for_status()?;
        let mut reader = BufReader::with_capacity(256 << 10, resp);

        let (entries, blocks) = copy_blocks(
            &mut reader,
            &mut out,
            SKIP_BLOCKS_PER_EPOCH,
            TAKE_BLOCKS_PER_EPOCH,
        )?;

        println!(
            "  copied {} entries for {} blocks (skipped {} blocks)",
            entries, blocks, SKIP_BLOCKS_PER_EPOCH
        );
    }

    out.flush()?;
    println!("wrote {}", out_path);
    Ok(())
}

fn run_biggest(args: &[String]) -> Result<()> {
    if args.len() != 4 {
        usage(&args[0]);
    }

    let epoch: u64 = args[2].parse()?;
    let out_path = &args[3];

    let url = format!(
        "https://files.old-faithful.net/{}/epoch-{}.car",
        epoch, epoch
    );
    println!("scanning {} ...", url);

    let client = http_client()?;
    let resp = client.get(&url).send()?.error_for_status()?;
    let reader = BufReader::with_capacity(256 << 10, resp);

    let mut out = File::create(out_path)?;
    extract_biggest_block(reader, &mut out)?;

    println!("wrote {}", out_path);
    Ok(())
}

/// Copy `take_blocks` block groups, after skipping `skip_blocks` block groups.
/// Important: We skip by block *groups* (everything up to and including the block node).
/// This keeps the output starting at a clean block boundary.
fn copy_blocks<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    skip_blocks: u64,
    take_blocks: u64,
) -> Result<(u64, u64)> {
    let header_len = read_uvarint64(reader)? as usize;
    if header_len > 0 {
        skip_bytes(reader, header_len)?;
    }

    let mut entries_written = 0u64;
    let mut blocks_seen = 0u64;
    let mut blocks_written = 0u64;

    let mut cid = [0u8; CID_LEN];
    let mut buf = [0u8; 256 * 1024];

    loop {
        let len = read_uvarint64(reader)? as usize;
        reader.read_exact(&mut cid)?;

        let mut rem = len - CID_LEN;
        let mut is_block = false;
        let mut first = true;

        // We only start writing after we have fully skipped `skip_blocks` block groups.
        let should_write = blocks_seen >= skip_blocks;

        if should_write {
            write_uvarint64(writer, len as u64)?;
            writer.write_all(&cid)?;
        }

        while rem > 0 {
            let n = rem.min(buf.len());
            reader.read_exact(&mut buf[..n])?;

            if first && n >= 2 {
                is_block = is_block_node_peek(buf[0], buf[1]);
                first = false;
            }

            if should_write {
                writer.write_all(&buf[..n])?;
            }

            rem -= n;
        }

        if should_write {
            entries_written += 1;
        }

        if is_block {
            blocks_seen += 1;

            if blocks_seen > skip_blocks {
                // We wrote everything for this block group if we were in write mode.
                // That means we have completed one written block group.
                if should_write {
                    blocks_written += 1;
                    if blocks_written == take_blocks {
                        return Ok((entries_written, blocks_written));
                    }
                }
            }

            // Note: when blocks_seen == skip_blocks, we just finished skipping and the next
            // entries will belong to the first written block group.
        }
    }
}

fn extract_biggest_block<R: Read, W: Write>(mut reader: R, mut out: W) -> Result<()> {
    const MAX_BLOCKS: u64 = 432_000;

    let header_len = read_uvarint64(&mut reader)? as usize;
    if header_len > 0 {
        skip_bytes(&mut reader, header_len)?;
    }

    let mut cur = Vec::with_capacity(1 << 20);
    let mut best = Vec::new();

    let mut cid = [0u8; CID_LEN];
    let mut buf = [0u8; 256 * 1024];

    let start = std::time::Instant::now();
    let mut last_log = start;

    let mut blocks_seen = 0u64;

    loop {
        let len = match read_uvarint64(&mut reader) {
            Ok(v) => v as usize,
            Err(_) => break,
        };

        let mut entry = Vec::with_capacity(len + 16);
        write_uvarint64(&mut entry, len as u64)?;

        reader.read_exact(&mut cid)?;
        entry.extend_from_slice(&cid);

        let mut rem = len - CID_LEN;
        let mut is_block = false;
        let mut first = true;

        while rem > 0 {
            let n = rem.min(buf.len());
            reader.read_exact(&mut buf[..n])?;
            if first && n >= 2 {
                is_block = is_block_node_peek(buf[0], buf[1]);
                first = false;
            }
            entry.extend_from_slice(&buf[..n]);
            rem -= n;
        }

        cur.extend_from_slice(&entry);

        if is_block {
            blocks_seen += 1;

            if cur.len() > best.len() {
                best.clear();
                best.extend_from_slice(&cur);
            }
            cur.clear();

            let now = std::time::Instant::now();
            if now.duration_since(last_log).as_secs() >= 2 {
                let elapsed = now.duration_since(start).as_secs_f64().max(1e-9);
                let blk_s = blocks_seen as f64 / elapsed;
                let pct = (blocks_seen as f64 / MAX_BLOCKS as f64) * 100.0;
                let eta = if blk_s > 0.0 {
                    ((MAX_BLOCKS - blocks_seen) as f64 / blk_s) as u64
                } else {
                    0
                };

                println!(
                    "blocks={} ({:.1}%) | {:.0} blk/s | ETA {}s | biggest {:.2} MiB",
                    blocks_seen,
                    pct,
                    blk_s,
                    eta,
                    best.len() as f64 / (1024.0 * 1024.0),
                );

                last_log = now;
            }
        }
    }

    println!(
        "done: blocks={} | biggest {:.2} MiB",
        blocks_seen,
        best.len() as f64 / (1024.0 * 1024.0)
    );

    write_uvarint64(&mut out, 0)?;
    out.write_all(&best)?;

    Ok(())
}

fn is_block_node_peek(b0: u8, b1: u8) -> bool {
    b0 >= 0x80 && b0 < 0xA0 && b1 == 0x02
}

fn skip_bytes<R: Read>(r: &mut R, mut n: usize) -> Result<()> {
    let mut buf = [0u8; 1024];
    while n > 0 {
        let k = n.min(buf.len());
        r.read_exact(&mut buf[..k])?;
        n -= k;
    }
    Ok(())
}

fn read_uvarint64<R: Read>(r: &mut R) -> Result<u64> {
    let mut x = 0u64;
    let mut shift = 0;
    let mut b = [0u8; 1];

    for _ in 0..MAX_UVARINT_LEN_64 {
        r.read_exact(&mut b)?;
        let byte = b[0];
        if byte < 0x80 {
            return Ok(x | ((byte as u64) << shift));
        }
        x |= ((byte & 0x7f) as u64) << shift;
        shift += 7;
    }
    Err("uvarint overflow".into())
}

fn write_uvarint64<W: Write>(w: &mut W, mut x: u64) -> Result<()> {
    let mut buf = [0u8; MAX_UVARINT_LEN_64];
    let mut i = 0;
    while x >= 0x80 {
        buf[i] = (x as u8) | 0x80;
        x >>= 7;
        i += 1;
    }
    buf[i] = x as u8;
    i += 1;
    w.write_all(&buf[..i])?;
    Ok(())
}
