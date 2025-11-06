use anyhow::{Context, Result};
use bytes::Bytes;
use prost::Message;
use smallvec::SmallVec;
use solana_pubkey::Pubkey;
use std::str::FromStr;

// TransactionStatusMeta { rewards: [Reward], loaded_addresses: LoadedAddresses }
#[derive(Clone, PartialEq, Message)]
pub struct TxStatusMeta {
    #[prost(message, repeated, tag = "9")]
    pub rewards: Vec<Reward>,

    // loaded_addresses is a nested message at field 10
    #[prost(message, optional, tag = "10")]
    pub loaded_addresses: Option<LoadedAddresses>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Reward {
    // reward pubkey as a string (base58)
    // use bytes="bytes" to avoid an extra String allocation
    #[prost(bytes = "bytes", tag = "1")]
    pub pubkey: Bytes,
}

#[derive(Clone, PartialEq, Message)]
pub struct LoadedAddresses {
    // repeated string writable=1, readonly=2
    #[prost(bytes = "bytes", repeated, tag = "1")]
    pub writable: Vec<Bytes>,

    #[prost(bytes = "bytes", repeated, tag = "2")]
    pub readonly: Vec<Bytes>,
}

pub fn extract_metadata_pubkeys(meta_zstd: &[u8], out: &mut SmallVec<[Pubkey; 256]>) -> Result<()> {
    if meta_zstd.is_empty() {
        return Ok(());
    }

    // Fully decompress metadata
    let decompressed = zstd::decode_all(meta_zstd)
        .context("failed to decompress metadata with zstd")?;

    // Decode minimal TxStatusMeta
    let meta = TxStatusMeta::decode(&*decompressed)
        .context("failed to decode TransactionStatusMeta protobuf")?;

    // Rewards
    for r in meta.rewards {
        if let Ok(s) = core::str::from_utf8(&r.pubkey) {
            if let Ok(pk) = Pubkey::from_str(s) {
                out.push(pk);
            }
        }
    }

    // Loaded addresses
    if let Some(la) = meta.loaded_addresses {
        for b in la.writable.into_iter().chain(la.readonly.into_iter()) {
            if let Ok(s) = core::str::from_utf8(&b) {
                if let Ok(pk) = Pubkey::from_str(s) {
                    out.push(pk);
                }
            }
        }
    }

    Ok(())
}
