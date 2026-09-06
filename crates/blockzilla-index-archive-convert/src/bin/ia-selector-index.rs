use std::{path::PathBuf, time::Instant};

use anyhow::{Context, Result, bail};
use blockzilla_index_archive_convert::selector_index::{
    SelectorIndexBuildOptions, build_selector_index,
};

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let root = PathBuf::from(
        args.next()
            .context("usage: ia-selector-index <generation-dir> [sort-memory-mib]")?,
    );
    let mut options = SelectorIndexBuildOptions::default();
    if let Some(value) = args.next() {
        let mib: usize = value
            .parse()
            .context("selector sort-memory MiB must be a number")?;
        options.sort_memory_bytes = mib
            .checked_mul(1 << 20)
            .context("selector sort-memory bytes overflow")?;
    }
    if args.next().is_some() {
        bail!("usage: ia-selector-index <generation-dir> [sort-memory-mib]");
    }

    let started = Instant::now();
    let report = build_selector_index(&root, options)?;
    println!("selector index built");
    println!("  blocks              {}", report.blocks);
    println!("  transactions        {}", report.transactions);
    println!(
        "  instructions        {} top-level + {} CPI",
        report.top_level_instructions, report.cpi_instructions
    );
    println!("  postings            {}", report.postings);
    println!("  sort runs           {}", report.sort_runs);
    println!("  merge passes        {}", report.merge_passes);
    println!("  pages               {}", report.pages);
    println!("  continuation pages  {}", report.continuation_pages);
    println!("  object bytes        {}", report.object_bytes);
    println!("  elapsed             {:?}", started.elapsed());
    Ok(())
}
