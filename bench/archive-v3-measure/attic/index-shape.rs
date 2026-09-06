//! Measure the three things that decide what an index costs.
//!
//! 1. **Posting collapse at block granularity.** Reads are block-by-block, so a
//!    posting list that names transactions is finer than any reader needs. How
//!    many postings disappear if a posting names a block instead?
//! 2. **Reference skew by registry ordinal.** The pubkey registry is
//!    usage-ordered, so the ordinal is already a frequency rank. If references
//!    are steeply skewed, a hot prefix can use a different encoding from the
//!    cold tail with no extra metadata to decide which — the ordinal decides.
//! 3. **Account bytes versus ledger bytes.** Index building only needs the
//!    account lists. If those are a small share of the ledger, making them
//!    separately addressable saves the rest of every read.
use std::{
    collections::BTreeMap,
    env, fs,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
};

use anyhow::{Context, Result};
use blockzilla_archive_v3::{
    catalog::blocks::{self, PageSpan},
    ledger::accounts,
};

fn read_page(file: &mut fs::File, span: PageSpan) -> Result<Vec<u8>> {
    if span.stored_len == 0 {
        return Ok(Vec::new());
    }
    file.seek(SeekFrom::Start(span.offset))?;
    let mut stored = vec![0u8; span.stored_len as usize];
    file.read_exact(&mut stored)?;
    if span.stored_len == span.decoded_len {
        Ok(stored)
    } else {
        zstd::decode_all(&stored[..]).context("zstd decode page")
    }
}

fn main() -> Result<()> {
    let dir = PathBuf::from(
        env::args()
            .nth(1)
            .context("usage: index-shape <generation-dir>")?,
    );
    let rows = blocks::decode_table(&fs::read(dir.join("catalog/blocks.tbl"))?)?;
    let accounts_col = blocks::column_index("ledger/accounts.pages").expect("known column");
    let mut accounts_file = fs::File::open(dir.join("ledger/accounts.pages"))?;

    let mut references = 0u64; // (account, transaction) pairs
    let mut block_postings = 0u64; // (account, block) pairs
    let mut per_account: BTreeMap<u32, u64> = BTreeMap::new();

    for row in &rows {
        let tx_count = row.transaction_count as usize;
        let block = accounts::decode_page_flat(
            &read_page(&mut accounts_file, row.page(accounts_col).unwrap())?,
            tx_count,
        )?;
        let mut in_block = std::collections::BTreeSet::new();
        for tx in 0..tx_count {
            for id in block.accounts(tx).context("accounts for transaction")? {
                references += 1;
                *per_account.entry(*id).or_default() += 1;
                in_block.insert(*id);
            }
        }
        block_postings += in_block.len() as u64;
    }

    println!("=== 1. posting collapse at block granularity ===");
    println!("blocks                              {:>12}", rows.len());
    println!("(account, transaction) postings     {references:>12}");
    println!("(account, block) postings           {block_postings:>12}");
    println!(
        "collapse                            {:>11.2}x\n",
        references as f64 / block_postings.max(1) as f64
    );

    // Ordinal order is usage order, so rank == ordinal. Walk it and report the
    // share of all references covered by each prefix.
    let mut ranked: Vec<(u32, u64)> = per_account.iter().map(|(k, v)| (*k, *v)).collect();
    ranked.sort_by_key(|(ordinal, _)| *ordinal);
    println!("=== 2. reference skew by registry ordinal (usage rank) ===");
    println!("distinct accounts                   {:>12}", ranked.len());
    let mut running = 0u64;
    let mut cut = 0usize;
    for share in [0.001_f64, 0.01, 0.05, 0.10, 0.25, 0.50] {
        let target = (ranked.len() as f64 * share).ceil() as usize;
        while cut < target && cut < ranked.len() {
            running += ranked[cut].1;
            cut += 1;
        }
        println!(
            "  top {:>5.1}% of accounts ({:>6})    {:>6.2}% of all references",
            share * 100.0,
            cut,
            running as f64 / references as f64 * 100.0
        );
    }
    let singletons = ranked.iter().filter(|(_, n)| *n == 1).count();
    println!(
        "  accounts referenced exactly once  {singletons:>6}  ({:.1}% of accounts, {:.2}% of references)",
        singletons as f64 / ranked.len() as f64 * 100.0,
        singletons as f64 / references as f64 * 100.0
    );

    println!("\n=== 3. account bytes vs the rest of the ledger ===");
    let mut group = |cols: &[&str]| -> Result<(u64, u64)> {
        let (mut stored, mut decoded) = (0u64, 0u64);
        for name in cols {
            let index = blocks::column_index(name).expect("known column");
            for row in &rows {
                if let Some(span) = row.page(index) {
                    stored += u64::from(span.stored_len);
                    decoded += u64::from(span.decoded_len);
                }
            }
        }
        Ok((stored, decoded))
    };
    let (acc_stored, acc_decoded) = group(&["ledger/accounts.pages"])?;
    let (led_stored, led_decoded) = group(&[
        "ledger/core.pages",
        "ledger/accounts.pages",
        "ledger/instructions.pages",
        "ledger/instruction_data.pages",
        "ledger/lookups.pages",
    ])?;
    println!(
        "ledger  stored {led_stored:>10}  decoded {led_decoded:>10}",
    );
    println!(
        "accounts stored {acc_stored:>9}  decoded {acc_decoded:>10}   = {:.1}% stored, {:.1}% decoded",
        acc_stored as f64 / led_stored.max(1) as f64 * 100.0,
        acc_decoded as f64 / led_decoded.max(1) as f64 * 100.0
    );
    println!(
        "\nAn index build that must decode whole transactions reads {:.1}x the bytes\nof one that can address the account lists alone.",
        led_decoded as f64 / acc_decoded.max(1) as f64
    );
    Ok(())
}
