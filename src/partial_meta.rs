use anyhow::{Context, Result};
use prost::Message;
use smallvec::SmallVec;
use solana_pubkey::Pubkey;
use std::str::FromStr;
use tracing::warn;

use crate::confirmed_block::TransactionStatusMeta;

#[inline]
fn is_zstd(buf: &[u8]) -> bool {
    buf.get(0..4)
        .map(|m| m == [0x28, 0xB5, 0x2F, 0xFD])
        .unwrap_or(false)
}

#[inline]
fn push_pk_bytes32(b: &[u8], out: &mut SmallVec<[Pubkey; 256]>) {
    if b.len() == 32 {
        let mut arr = [0u8; 32];
        arr.copy_from_slice(b);
        out.push(Pubkey::new_from_array(arr));
    } else {
        warn!(
            "invalid loaded address length: expected 32 bytes, got {}",
            b.len()
        );
    }
}

#[inline]
fn push_pk_base58(s: &str, out: &mut SmallVec<[Pubkey; 256]>) {
    match Pubkey::from_str(s) {
        Ok(pk) => out.push(pk),
        Err(_) => warn!("invalid base58 pubkey string: {}", s),
    }
}

pub fn extract_metadata_pubkeys(
    meta_zstd_or_raw: &[u8],
    out: &mut SmallVec<[Pubkey; 256]>,
) -> Result<()> {
    if meta_zstd_or_raw.is_empty() {
        warn!("empty metadata buffer (no meta bytes provided)");
        return Ok(());
    }

    // 1) Decompress if needed
    let raw = if is_zstd(meta_zstd_or_raw) {
        match zstd::decode_all(meta_zstd_or_raw) {
            Ok(data) => data,
            Err(e) => {
                warn!("zstd decompression failed: {}", e);
                return Err(e).context("zstd decompress meta");
            }
        }
    } else {
        meta_zstd_or_raw.to_vec()
    };

    // 2) Decode protobuf
    let pb: TransactionStatusMeta = match TransactionStatusMeta::decode(raw.as_slice()) {
        Ok(v) => v,
        Err(e) => {
            warn!("failed to decode TransactionStatusMeta protobuf: {}", e);
            return Err(e).context("prost decode meta");
        }
    };

    // 3) Collect keys
    for r in &pb.rewards {
        if r.pubkey.is_empty() {
            warn!("reward entry missing pubkey");
            continue;
        }
        push_pk_base58(&r.pubkey, out);
    }

    for b in &pb.loaded_writable_addresses {
        push_pk_bytes32(b, out);
    }
    for b in &pb.loaded_readonly_addresses {
        push_pk_bytes32(b, out);
    }

    for tb in pb
        .pre_token_balances
        .iter()
        .chain(pb.post_token_balances.iter())
    {
        if tb.mint.is_empty() {
            warn!("token balance missing mint field");
        } else {
            push_pk_base58(&tb.mint, out);
        }
        if tb.owner.is_empty() {
            warn!("token balance missing owner field");
        } else {
            push_pk_base58(&tb.owner, out);
        }
    }

    Ok(())
}
