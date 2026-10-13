//! Convert saved Linux perf script stacks without running or sampling a process.
use anyhow::{Context, Result, ensure};
use inferno::collapse::Collapse;
use std::{
    fs::File,
    io::{BufReader, BufWriter},
    path::PathBuf,
};
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).map(PathBuf::from).collect();
    ensure!(
        args.len() == 3,
        "usage: perf-flamegraph PERF_SCRIPT NEW_FOLDED NEW_SVG"
    );
    let mut collapsed = Vec::new();
    inferno::collapse::perf::Folder::default()
        .collapse(BufReader::new(File::open(&args[0])?), &mut collapsed)?;
    ensure!(!collapsed.is_empty(), "perf produced no collapsed stacks");
    std::io::Write::write_all(
        &mut File::options()
            .write(true)
            .create_new(true)
            .open(&args[1])?,
        &collapsed,
    )?;
    let mut options = inferno::flamegraph::Options::default();
    options.title = "Zilla: resident full-epoch decode CPU profile".into();
    options.count_name = "event units".into();
    options.min_width = 0.05;
    let output = File::options()
        .write(true)
        .create_new(true)
        .open(&args[2])?;
    inferno::flamegraph::from_reader(
        &mut options,
        BufReader::new(collapsed.as_slice()),
        BufWriter::new(output),
    )
    .context("render flamegraph")?;
    Ok(())
}
