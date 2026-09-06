//! Compatibility command for the Archive V3 converter.

use blockzilla_archive_v3_convert::convert::{ConvertOptions, run};
use clap::Parser;

#[derive(Parser)]
#[command(name = "blockzilla-archive-v3-convert")]
#[command(about = "Convert a Compact V2 generation into Archive V3 planes")]
struct Cli {
    #[command(flatten)]
    options: ConvertOptions,
}

fn main() -> anyhow::Result<()> {
    run(Cli::parse().options)
}
