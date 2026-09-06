use std::{path::PathBuf, time::Instant};

use anyhow::{Context, Result, bail};
use blockzilla_index_archive_convert::account_index::{
    AccountIndexBuildOptions, build_account_index, find_account,
};

const DEFAULT_SORT_MEMORY_MIB: usize = 128;

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let root = PathBuf::from(
        args.next()
            .context("usage: ia-index <generation-dir> [sort-memory-mib]")?,
    );
    let second = args.next();
    if let Some(flag) = second.as_deref()
        && flag == "--find"
    {
        let ordinal: u32 = args
            .next()
            .context("usage: ia-index <generation-dir> --find <account-ordinal>")?
            .parse()
            .context("account ordinal must be a number")?;
        if args.next().is_some() {
            bail!("usage: ia-index <generation-dir> --find <account-ordinal>");
        }
        return find_account(&root, ordinal);
    }
    let sort_memory_mib: usize = match second {
        Some(value) => value.parse().context("sort-memory MiB must be a number")?,
        None => DEFAULT_SORT_MEMORY_MIB,
    };
    if args.next().is_some() {
        bail!("usage: ia-index <generation-dir> [sort-memory-mib]");
    }
    let sort_memory_bytes = sort_memory_mib
        .checked_mul(1 << 20)
        .context("sort-memory bytes overflow")?;

    let started = Instant::now();
    let stats = build_account_index(&root, AccountIndexBuildOptions { sort_memory_bytes })?;

    println!("account index built");
    println!("  distinct accounts   {}", stats.distinct_accounts);
    println!("  postings            {}", stats.postings);
    println!("  sort runs           {}", stats.sort_runs);
    println!("  merge passes        {}", stats.merge_passes);
    println!("  pages               {}", stats.pages);
    println!("  pages bytes         {}", stats.page_bytes);
    println!("  directory bytes     {}", stats.directory_bytes);
    println!("  object bytes        {}", stats.object_bytes);
    if stats.postings > 0 {
        println!(
            "  bytes per posting   {:.2}",
            stats.page_bytes as f64 / stats.postings as f64
        );
    }
    println!("  elapsed             {:?}", started.elapsed());
    Ok(())
}
