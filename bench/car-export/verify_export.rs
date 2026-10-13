//! Standalone common-export verifier. Build with `rustc -O verify_export.rs`.
use std::convert::{TryFrom, TryInto};
use std::env;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
use std::path::PathBuf;
use std::time::{Duration, Instant};

const MAGIC: &[u8; 16] = b"CAR-TX-EXPORT-01";
const MAX_BLOCK_BYTES: u64 = 64 << 20;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn invalid(message: impl Into<String>) -> Box<dyn std::error::Error> {
    io::Error::new(io::ErrorKind::InvalidData, message.into()).into()
}

fn read_plan(reader: impl BufRead) -> Result<Vec<(u64, u64)>> {
    let mut rows = Vec::new();
    for (line_number, line) in reader.lines().enumerate() {
        let line = line?;
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.len() != 2 {
            return Err(invalid(format!(
                "plan line {} needs slot and count",
                line_number + 1
            )));
        }
        let slot: u64 = fields[0].parse()?;
        let count: u64 = fields[1].parse()?;
        if rows.last().is_some_and(|&(previous, _)| previous >= slot) {
            return Err(invalid("plan slots must be strictly increasing"));
        }
        rows.push((slot, count));
    }
    if rows.is_empty() {
        return Err(invalid("empty plan"));
    }
    Ok(rows)
}

fn number(bytes: &[u8], offset: usize) -> Result<u64> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| invalid("offset overflow"))?;
    let field = bytes
        .get(offset..end)
        .ok_or_else(|| invalid("truncated integer"))?;
    Ok(u64::from_le_bytes(field.try_into()?))
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Counts {
    output_bytes: u64,
    blocks: u64,
    transactions: u64,
    votes: u64,
    failed: u64,
}

fn add(counter: &mut u64, value: u64) -> Result<()> {
    *counter = counter
        .checked_add(value)
        .ok_or_else(|| invalid("counter overflow"))?;
    Ok(())
}

fn verify(mut input: impl Read, rows: &[(u64, u64)], progress: bool) -> Result<Counts> {
    let mut magic = [0; 16];
    input.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(invalid("export magic differs"));
    }
    let mut counts = Counts {
        output_bytes: 16,
        ..Counts::default()
    };
    let mut block = Vec::new();
    let started = Instant::now();
    let mut last_progress = started;
    for &(slot, count) in rows {
        let mut header = [0; 24];
        input.read_exact(&mut header)?;
        if number(&header, 0)? != slot || number(&header, 8)? != count {
            return Err(invalid(format!("block coverage differs at slot {slot}")));
        }
        let size = number(&header, 16)?;
        if size > MAX_BLOCK_BYTES || count > size / 138 {
            return Err(invalid(format!(
                "invalid block size or transaction count at slot {slot}"
            )));
        }
        block.resize(usize::try_from(size)?, 0);
        input.read_exact(&mut block)?;
        let mut offset = 0usize;
        for index in 0..count {
            if block.len().saturating_sub(offset) < 122 {
                return Err(invalid(format!(
                    "truncated transaction at slot {slot} index {index}"
                )));
            }
            if number(&block, offset)? != slot || number(&block, offset + 8)? != index {
                return Err(invalid(format!(
                    "transaction order differs at slot {slot} index {index}"
                )));
            }
            let vote = block[offset + 112];
            let failed = block[offset + 113];
            if vote > 1 || failed > 1 {
                return Err(invalid(format!(
                    "invalid flags at slot {slot} index {index}"
                )));
            }
            add(&mut counts.votes, u64::from(vote))?;
            add(&mut counts.failed, u64::from(failed))?;
            offset += 122;
            for _ in 0..2 {
                let length = number(&block, offset)?;
                offset = offset
                    .checked_add(8)
                    .ok_or_else(|| invalid("balance offset overflow"))?;
                let bytes = usize::try_from(length)?
                    .checked_mul(8)
                    .ok_or_else(|| invalid("balance length overflow"))?;
                offset = offset
                    .checked_add(bytes)
                    .ok_or_else(|| invalid("balance offset overflow"))?;
                if offset > block.len() {
                    return Err(invalid(format!(
                        "truncated balances at slot {slot} index {index}"
                    )));
                }
            }
        }
        if offset != block.len() {
            return Err(invalid(format!("extra block bytes at slot {slot}")));
        }
        add(&mut counts.blocks, 1)?;
        add(&mut counts.transactions, count)?;
        add(&mut counts.output_bytes, 24 + size)?;
        if progress && last_progress.elapsed() >= Duration::from_secs(5) {
            eprintln!(
                "verified {}/{} blocks, {} transactions, {} bytes in {:.1}s",
                counts.blocks,
                rows.len(),
                counts.transactions,
                counts.output_bytes,
                started.elapsed().as_secs_f64()
            );
            last_progress = Instant::now();
        }
    }
    let mut extra = [0; 1];
    if input.read(&mut extra)? != 0 {
        return Err(invalid("extra data after final block"));
    }
    Ok(counts)
}

fn run() -> Result<()> {
    let mut plan_path: Option<PathBuf> = None;
    let mut input_path: Option<PathBuf> = None;
    let mut args = env::args_os().skip(1);
    while let Some(flag) = args.next() {
        let target = if flag == "--plan-tsv" {
            &mut plan_path
        } else if flag == "--input" {
            &mut input_path
        } else {
            return Err(invalid("usage: verify-export --plan-tsv PATH --input PATH"));
        };
        if target.is_some() {
            return Err(invalid("duplicate argument"));
        }
        *target = Some(
            args.next()
                .ok_or_else(|| invalid("missing argument value"))?
                .into(),
        );
    }
    let plan_path = plan_path.ok_or_else(|| invalid("--plan-tsv is required"))?;
    let input_path = input_path.ok_or_else(|| invalid("--input is required"))?;
    let rows = read_plan(BufReader::new(File::open(plan_path)?))?;
    let counts = verify(
        BufReader::with_capacity(1 << 20, File::open(input_path)?),
        &rows,
        true,
    )?;
    println!(
        "{{\"output_bytes\":{},\"blocks\":{},\"transactions\":{},\"votes\":{},\"failed\":{}}}",
        counts.output_bytes, counts.blocks, counts.transactions, counts.votes, counts.failed
    );
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("export verification failed: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transaction(slot: u64, index: u64, vote: u8, failed: u8) -> Vec<u8> {
        let mut bytes = vec![0; 122];
        bytes[..8].copy_from_slice(&slot.to_le_bytes());
        bytes[8..16].copy_from_slice(&index.to_le_bytes());
        bytes[112] = vote;
        bytes[113] = failed;
        for balances in [vec![1u64, 4], vec![3u64]] {
            bytes.extend_from_slice(&(balances.len() as u64).to_le_bytes());
            for balance in balances {
                bytes.extend_from_slice(&balance.to_le_bytes());
            }
        }
        bytes
    }

    fn fixture() -> (Vec<u8>, Vec<(u64, u64)>) {
        let mut output = MAGIC.to_vec();
        for (slot, count, payload) in [
            (40u64, 0u64, vec![]),
            (
                42,
                2,
                [transaction(42, 0, 1, 0), transaction(42, 1, 0, 1)].concat(),
            ),
        ] {
            for number in [slot, count, payload.len() as u64] {
                output.extend_from_slice(&number.to_le_bytes());
            }
            output.extend_from_slice(&payload);
        }
        (output, vec![(40, 0), (42, 2)])
    }

    #[test]
    fn validates_empty_blocks_order_balances_and_counters() {
        let (bytes, rows) = fixture();
        assert_eq!(
            verify(bytes.as_slice(), &rows, false).unwrap(),
            Counts {
                output_bytes: bytes.len() as u64,
                blocks: 2,
                transactions: 2,
                votes: 1,
                failed: 1
            }
        );
    }

    #[test]
    fn rejects_wrong_magic_headers_order_and_flags() {
        let (bytes, rows) = fixture();
        for offset in [0, 16, 24, 40, 48, 64, 72, 64 + 112, 64 + 113] {
            let mut bad = bytes.clone();
            bad[offset] = 7;
            assert!(
                verify(bad.as_slice(), &rows, false).is_err(),
                "offset {offset}"
            );
        }
    }

    #[test]
    fn rejects_truncation_extra_data_and_invalid_balance_lengths() {
        let (bytes, rows) = fixture();
        for length in [0, 15, 39, 63, bytes.len() - 1] {
            assert!(verify(&bytes[..length], &rows, false).is_err());
        }
        let mut extra = bytes.clone();
        extra.push(0);
        assert!(verify(extra.as_slice(), &rows, false).is_err());
        for length in [u64::MAX, 1000] {
            let mut bad = bytes.clone();
            bad[64 + 122..64 + 130].copy_from_slice(&length.to_le_bytes());
            assert!(verify(bad.as_slice(), &rows, false).is_err());
        }
        let mut oversized = bytes.clone();
        oversized[56..64].copy_from_slice(&(MAX_BLOCK_BYTES + 1).to_le_bytes());
        assert!(verify(oversized.as_slice(), &rows, false).is_err());
        let mut extra_block_byte = bytes.clone();
        extra_block_byte.push(0);
        extra_block_byte[56..64].copy_from_slice(&325u64.to_le_bytes());
        assert!(verify(extra_block_byte.as_slice(), &rows, false).is_err());
    }

    #[test]
    fn validates_plan_shape_and_order() {
        assert_eq!(
            read_plan(&b"40\t0\n42\t2\n"[..]).unwrap(),
            vec![(40, 0), (42, 2)]
        );
        for bad in [
            "",
            "40\t0\n40\t2\n",
            "40\t0\n39\t2\n",
            "40\n",
            "40\t0\t1\n",
            "-1\t2\n",
        ] {
            assert!(read_plan(bad.as_bytes()).is_err(), "{bad:?}");
        }
    }
}
