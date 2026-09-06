use std::{
    env,
    fs::File,
    io::{self, BufReader},
    path::Path,
};

use of_car_reader::reconstruct::{ValidationStats, validate_car_stream};

const CAR_BUF: usize = 128 << 20;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let Some(input) = args.next() else {
        eprintln!("usage: car_validate_lossless <input.car|input.car.zst>");
        std::process::exit(1);
    };
    if args.next().is_some() {
        eprintln!("usage: car_validate_lossless <input.car|input.car.zst>");
        std::process::exit(1);
    }

    let input_path = Path::new(&input);
    let stats = if input_path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("zst"))
    {
        let file = File::open(input_path)?;
        let file = BufReader::with_capacity(CAR_BUF, file);
        let zstd = zstd::Decoder::with_buffer(file)?;
        validate_car_stream(zstd, CAR_BUF)?
    } else {
        let file = File::open(input_path)?;
        validate_car_stream(file, CAR_BUF)?
    };

    print_stats(input_path, &stats)?;
    Ok(())
}

fn print_stats(path: &Path, stats: &ValidationStats) -> io::Result<()> {
    println!("validated {}", path.display());
    println!(
        "entries={} bytes={} blocks={} entries_nodes={} txs={} rewards={} dataframes={} subsets={} epochs={}",
        stats.car_entries,
        stats.bytes_read,
        stats.blocks,
        stats.entries,
        stats.transactions,
        stats.rewards,
        stats.dataframes,
        stats.subsets,
        stats.epochs
    );
    println!(
        "continuations tx_data={} tx_meta={} rewards={} dataframe={}",
        stats.tx_data_continuation_refs,
        stats.tx_metadata_continuation_refs,
        stats.rewards_continuation_refs,
        stats.dataframe_continuation_refs
    );
    Ok(())
}
