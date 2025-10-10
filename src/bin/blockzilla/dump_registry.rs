use anyhow::Result;
use rusqlite::Connection;
use solana_sdk::pubkey::Pubkey;
use std::fs::File;
use std::io::{BufWriter, Write};

pub fn dump_registry_to_csv(registry_path: &str, output_path: &str) -> Result<()> {
    let conn = Connection::open(registry_path)?;
    let mut stmt = conn.prepare("SELECT id, pubkey FROM keymap_ranked ORDER BY id")?;
    let mut rows = stmt.query([])?;
    let file = File::create(output_path)?;
    let mut writer = BufWriter::new(file);

    let mut count = 0u64;
    while let Some(row) = rows.next()? {
        let id: u32 = row.get(0)?;
        let blob: Vec<u8> = row.get(1)?;

        let pk = Pubkey::new_from_array(blob.try_into().unwrap());
        writeln!(writer, "{},{}", id, pk)?;
        count += 1;
        if count.is_multiple_of(1_000_000) {
            eprintln!("🧮 dumped {} entries...", count);
        }
    }

    writer.flush()?;
    eprintln!("✅ Dumped {} entries → {}", count, output_path);
    Ok(())
}
