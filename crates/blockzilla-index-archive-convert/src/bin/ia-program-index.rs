use std::{path::PathBuf, time::Instant};

use anyhow::{Context, Result, bail};
use blockzilla_index_archive_convert::program_index::{
    ProgramIndexBuildOptions, build_program_index,
};

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let root = PathBuf::from(
        args.next()
            .context("usage: ia-program-index <generation-dir> [sort-memory-mib]")?,
    );
    let mut options = ProgramIndexBuildOptions::default();
    if let Some(value) = args.next() {
        let mib: usize = value
            .parse()
            .context("program sort-memory MiB must be a number")?;
        options.sort_memory_bytes = mib
            .checked_mul(1 << 20)
            .context("program sort-memory bytes overflow")?;
    }
    if args.next().is_some() {
        bail!("usage: ia-program-index <generation-dir> [sort-memory-mib]");
    }

    let started = Instant::now();
    let report = build_program_index(&root, options)?;
    println!("program index built");
    println!("  archive ID          {}", report.archive_id.to_hex());
    println!("  blocks              {}", report.blocks);
    println!("  transactions        {}", report.transactions);
    println!(
        "  instructions        {} top-level + {} CPI",
        report.top_level_instructions, report.cpi_instructions
    );
    println!("  distinct programs   {}", report.distinct_programs);
    println!("  postings            {}", report.postings);
    println!("  sort runs           {}", report.sort_runs);
    println!("  pages               {}", report.pages);
    println!("  continuation pages  {}", report.continuation_pages);
    println!("  object bytes        {}", report.object_bytes);
    println!("  elapsed             {:?}", started.elapsed());
    Ok(())
}
