//! Measure what an account-relation sidecar would cost and answer.
//!
//! Two questions the flag table raises and cannot answer itself:
//!
//! 1. How many accounts fall through to "derived" because they neither signed
//!    nor were invoked — and how much of that bucket is really just passive
//!    wallets and mints?
//! 2. If we recorded, per account, which programs used it, how big is that
//!    relation and how often is it a single program (a storable owner) rather
//!    than a set?
use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
};

use anyhow::{Context, Result};
use blockzilla_archive_v3::{
    catalog::blocks::{self, PageSpan},
    ledger::{accounts, core, instructions},
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
            .context("usage: account-relations <generation-dir>")?,
    );
    let table = fs::read(dir.join("catalog/blocks.tbl")).context("read catalog")?;
    let rows = blocks::decode_table(&table)?;
    let registry_entries = fs::metadata(dir.join("dictionary/pubkeys.pages"))?.len() / 32;

    let mut open = |column: &str| -> Result<fs::File> {
        fs::File::open(dir.join(column)).with_context(|| format!("open {column}"))
    };
    let mut core_file = open("ledger/core.pages")?;
    let mut accounts_file = open("ledger/accounts.pages")?;
    let mut ix_file = open("ledger/instructions.pages")?;
    let mut inner_file = open("runtime/inner_instructions.pages")?;

    let idx = |path: &str| blocks::column_index(path).expect("known column");
    let (core_col, accounts_col) = (idx("ledger/core.pages"), idx("ledger/accounts.pages"));
    let (ix_col, inner_col) = (
        idx("ledger/instructions.pages"),
        idx("runtime/inner_instructions.pages"),
    );

    let mut signers: BTreeSet<u32> = BTreeSet::new();
    let mut programs: BTreeSet<u32> = BTreeSet::new();
    let mut cpi_only: BTreeSet<u32> = BTreeSet::new();
    // account -> programs that named it in an instruction it participates in.
    let mut used_by: BTreeMap<u32, BTreeSet<u32>> = BTreeMap::new();
    let mut references = 0u64;
    let (mut txs, mut ix_count) = (0u64, 0u64);

    for row in &rows {
        let tx_count = row.transaction_count as usize;
        txs += tx_count as u64;
        let core_rows =
            core::decode_page(&read_page(&mut core_file, row.page(core_col).unwrap())?)?;
        let block = accounts::decode_page_flat(
            &read_page(&mut accounts_file, row.page(accounts_col).unwrap())?,
            tx_count,
        )?;
        let top = instructions::decode_top_level_page(
            &read_page(&mut ix_file, row.page(ix_col).unwrap())?,
            tx_count,
        )?;
        let inner = instructions::decode_inner_page(
            &read_page(&mut inner_file, row.page(inner_col).unwrap())?,
            tx_count,
        )?;

        for tx in 0..tx_count {
            let keys = block.accounts(tx).context("accounts for transaction")?;
            references += keys.len() as u64;
            // Signing positions are the leading num_required_signatures of the
            // static keys, which is the definition the message header carries.
            let signing = core_rows[tx].num_required_signatures as usize;
            for id in keys.iter().take(signing) {
                signers.insert(*id);
            }

            let mut note = |ix: &instructions::Instruction, is_cpi: bool| {
                let Some(program) = keys.get(ix.program_position as usize).copied() else {
                    return;
                };
                programs.insert(program);
                if is_cpi {
                    cpi_only.insert(program);
                }
                for position in &ix.account_positions {
                    if let Some(account) = keys.get(*position as usize).copied() {
                        used_by.entry(account).or_default().insert(program);
                    }
                }
            };
            for ix in &top[tx] {
                ix_count += 1;
                note(ix, false);
            }
            for group in &inner[tx].groups {
                for (_, ix) in &group.instructions {
                    ix_count += 1;
                    note(ix, true);
                }
            }
        }
    }

    // A program reached only through CPI never appears as a top-level program
    // id, so a filter that walks top-level instructions alone misses it.
    let top_level_programs: BTreeSet<u32> = programs.difference(&cpi_only).copied().collect();
    let derived: Vec<u32> = (1..=registry_entries as u32)
        .filter(|id| !signers.contains(id) && !programs.contains(id))
        .collect();
    let derived_set: BTreeSet<u32> = derived.iter().copied().collect();

    let mut owner_histogram: BTreeMap<usize, u32> = BTreeMap::new();
    let mut derived_unseen = 0u32;
    for id in &derived {
        match used_by.get(id) {
            Some(set) => *owner_histogram.entry(set.len()).or_default() += 1,
            None => derived_unseen += 1,
        }
    }
    let pairs: u64 = used_by.values().map(|set| set.len() as u64).sum();

    println!("blocks                          {:>10}", rows.len());
    println!("transactions                    {txs:>10}");
    println!("instructions (top + cpi)        {ix_count:>10}");
    println!("registry accounts               {registry_entries:>10}");
    println!();
    println!(
        "signers                         {:>10}  {:>5.1}%",
        signers.len(),
        signers.len() as f64 / registry_entries as f64 * 100.0
    );
    println!(
        "programs                        {:>10}  {:>5.1}%   ({} top-level, {} cpi-reached)",
        programs.len(),
        programs.len() as f64 / registry_entries as f64 * 100.0,
        top_level_programs.len(),
        cpi_only.len()
    );
    println!(
        "both signer and program         {:>10}",
        signers.intersection(&programs).count()
    );
    println!(
        "neither -> \"derived\"            {:>10}  {:>5.1}%",
        derived.len(),
        derived.len() as f64 / registry_entries as f64 * 100.0
    );
    println!();
    println!("of the derived bucket, distinct programs that used the account:");
    println!(
        "  never used in any instruction {derived_unseen:>8}   (present only as a fee/balance participant)"
    );
    for (count, accounts) in &owner_histogram {
        println!(
            "  used by {count:>3} program(s)       {accounts:>8}   {:>5.1}%",
            *accounts as f64 / derived.len() as f64 * 100.0
        );
    }
    let single = owner_histogram.get(&1).copied().unwrap_or(0);
    println!();
    println!(
        "derived accounts with exactly one program:  {single} of {} ({:.1}%)",
        derived.len(),
        single as f64 / derived.len() as f64 * 100.0
    );
    println!("account->program relation pairs             {pairs}");
    println!(
        "  vs account references in instructions    {references}  ({:.2}x fewer)",
        references as f64 / pairs.max(1) as f64
    );
    println!(
        "  as a u32 owner column per account        {} bytes",
        registry_entries * 4
    );
    println!(
        "  as an explicit pair list                 {} bytes at 8 B/pair",
        pairs * 8
    );
    let _ = derived_set;
    Ok(())
}
