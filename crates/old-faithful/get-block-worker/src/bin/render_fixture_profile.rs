use of_car_reader::{CarBlockReader, car_block_group::CarBlockGroup};
use of_get_block_worker::get_block::{
    GetBlockConfig, TransactionDetails, render_get_block_json_bytes_profiled,
};
use std::{
    io::{Cursor, Seek, Write},
    time::Instant,
};

static CAR_BYTES: &[u8] =
    include_bytes!("../../../car-reader/benches/fixtures/epoch-822-biggest.car");

#[derive(Clone, Copy)]
enum Mode {
    Full,
    Accounts,
    Signatures,
    None,
}

impl Mode {
    fn label(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Accounts => "accounts",
            Self::Signatures => "signatures",
            Self::None => "none",
        }
    }

    fn transaction_details(self) -> TransactionDetails {
        match self {
            Self::Full => TransactionDetails::Full,
            Self::Accounts => TransactionDetails::Accounts,
            Self::Signatures => TransactionDetails::Signatures,
            Self::None => TransactionDetails::None,
        }
    }
}

#[derive(Default)]
struct Totals {
    read_car_us: u128,
    decode_transactions_us: u128,
    write_transaction_json_us: u128,
    write_meta_json_us: u128,
    write_transactions_us: u128,
    total_us: u128,
}

fn main() -> Result<(), String> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.first().is_some_and(|arg| arg == "dump") {
        let mode = args
            .get(1)
            .ok_or_else(|| "dump requires mode: full|accounts|signatures|none".to_string())
            .and_then(|value| parse_mode(value))?;
        let block_bytes = fixture_block_bytes()?;
        let config = GetBlockConfig {
            rewards: false,
            transaction_details: mode.transaction_details(),
            ..Default::default()
        };
        let (bytes, _profile) =
            render_get_block_json_bytes_profiled(block_bytes, Some([9u8; 32]), config)?;
        std::io::stdout()
            .write_all(&bytes)
            .map_err(|err| format!("write dump: {err}"))?;
        return Ok(());
    }

    let iterations = std::env::args()
        .nth(1)
        .map(|value| value.parse::<usize>())
        .transpose()
        .map_err(|err| format!("invalid iteration count: {err}"))?
        .unwrap_or(15)
        .max(1);
    let previous_blockhash = Some([9u8; 32]);
    let block_bytes = fixture_block_bytes()?;

    for mode in [Mode::Full, Mode::Accounts, Mode::Signatures, Mode::None] {
        let config = GetBlockConfig {
            rewards: false,
            transaction_details: mode.transaction_details(),
            ..Default::default()
        };
        let mut samples = Vec::with_capacity(iterations);
        let mut totals = Totals::default();
        let mut output_bytes = 0usize;
        let mut transactions = 0usize;

        for _ in 0..iterations {
            let started = Instant::now();
            let (bytes, profile) = render_get_block_json_bytes_profiled(
                block_bytes.clone(),
                previous_blockhash,
                config,
            )?;
            let elapsed_us = started.elapsed().as_micros();
            samples.push(elapsed_us);
            output_bytes = bytes.len();
            transactions = profile.transactions;
            totals.read_car_us += profile.read_car_us;
            totals.decode_transactions_us += profile.decode_transactions_us;
            totals.write_transaction_json_us += profile.write_transaction_json_us;
            totals.write_meta_json_us += profile.write_meta_json_us;
            totals.write_transactions_us += profile.write_transactions_us;
            totals.total_us += profile.total_us;
        }

        samples.sort_unstable();
        let p50 = percentile(&samples, 0.50);
        let p90 = percentile(&samples, 0.90);
        let avg = samples.iter().sum::<u128>() as f64 / samples.len() as f64;
        let n = iterations as f64;
        println!(
            concat!(
                "mode={mode} iterations={iterations} tx={transactions} bytes={bytes} ",
                "avg_ms={avg:.3} p50_ms={p50:.3} p90_ms={p90:.3} ",
                "stage_avg_ms=",
                "read_car:{read_car:.3},",
                "decode_tx:{decode_tx:.3},",
                "write_tx_json:{write_tx_json:.3},",
                "write_meta_json:{write_meta_json:.3},",
                "write_transactions:{write_transactions:.3},",
                "profile_total:{profile_total:.3}"
            ),
            mode = mode.label(),
            iterations = iterations,
            transactions = transactions,
            bytes = output_bytes,
            avg = avg / 1000.0,
            p50 = p50 as f64 / 1000.0,
            p90 = p90 as f64 / 1000.0,
            read_car = totals.read_car_us as f64 / n / 1000.0,
            decode_tx = totals.decode_transactions_us as f64 / n / 1000.0,
            write_tx_json = totals.write_transaction_json_us as f64 / n / 1000.0,
            write_meta_json = totals.write_meta_json_us as f64 / n / 1000.0,
            write_transactions = totals.write_transactions_us as f64 / n / 1000.0,
            profile_total = totals.total_us as f64 / n / 1000.0,
        );
    }

    Ok(())
}

fn percentile(samples: &[u128], percentile: f64) -> u128 {
    let index = ((samples.len() - 1) as f64 * percentile).round() as usize;
    samples[index]
}

fn parse_mode(value: &str) -> Result<Mode, String> {
    match value {
        "full" => Ok(Mode::Full),
        "accounts" => Ok(Mode::Accounts),
        "signatures" => Ok(Mode::Signatures),
        "none" => Ok(Mode::None),
        _ => Err(format!("unknown mode {value:?}")),
    }
}

fn fixture_block_bytes() -> Result<Vec<u8>, String> {
    let cursor = Cursor::new(CAR_BYTES);
    let mut car = CarBlockReader::with_capacity(cursor, 8 * 1024 * 1024);
    car.skip_header()
        .map_err(|err| format!("skip fixture CAR header: {err}"))?;
    let start = car
        .reader
        .stream_position()
        .map_err(|err| format!("read fixture start position: {err}"))?;
    let mut group = CarBlockGroup::without_rewards();
    let ok = car
        .read_until_block_into(&mut group)
        .map_err(|err| format!("read fixture block: {err}"))?;
    if !ok {
        return Err("fixture did not contain a block".to_string());
    }
    let end = car
        .reader
        .stream_position()
        .map_err(|err| format!("read fixture end position: {err}"))?;
    Ok(CAR_BYTES[start as usize..end as usize].to_vec())
}
