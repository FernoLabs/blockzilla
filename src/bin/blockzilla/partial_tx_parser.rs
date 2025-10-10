use ahash::AHashSet;
use anyhow::{Context, Result};
use bincode::{Decode, Encode, config, serde::Compat};
use serde::{Deserialize, Serialize};
use solana_sdk::{pubkey::Pubkey, signature::Signature, transaction::VersionedTransaction};

pub fn parse_bincode_tx_static_accounts(tx: &[u8], out: &mut AHashSet<Pubkey>) -> Result<()> {
    if tx.is_empty() {
        return Ok(());
    }

    let cfg = config::legacy();
    let (Compat(vt), _): (Compat<VersionedTransaction>, usize) =
        bincode::decode_from_slice(tx, cfg).context("bincode decode failed")?;
    out.extend(vt.message.static_account_keys());
    if let Some(lookups) = vt.message.address_table_lookups() {
        for lookup in lookups {
            out.insert(lookup.account_key);
        }
    }

    Ok(())
}
