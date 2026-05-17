use core::mem::MaybeUninit;

use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;
use wincode::{
    ReadResult, SchemaRead, SchemaWrite, WriteResult,
    config::ConfigCore,
    io::{Reader, Writer},
};

use crate::KeyStore;

/// Compact pubkey reference.
///
/// Wire format is tagless for the hot path:
/// - `1..` is a 1-based registry id encoded with the archive integer encoding.
/// - `0` means the next 32 bytes are the raw pubkey.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CompactPubkey {
    Id(u32),
    Raw([u8; 32]),
}

impl CompactPubkey {
    pub const RAW_SENTINEL: u32 = 0;

    #[inline]
    pub fn id(id: u32) -> Self {
        debug_assert!(id != Self::RAW_SENTINEL);
        Self::Id(id)
    }

    #[inline]
    pub fn raw(pubkey: [u8; 32]) -> Self {
        Self::Raw(pubkey)
    }

    #[inline]
    pub fn resolve(self, store: &KeyStore) -> Option<[u8; 32]> {
        match self {
            Self::Id(id) => store.get(id).copied(),
            Self::Raw(pubkey) => Some(pubkey),
        }
    }

    #[inline]
    pub fn to_pubkey(self, store: &KeyStore) -> Option<Pubkey> {
        self.resolve(store).map(Pubkey::new_from_array)
    }
}

unsafe impl<C: ConfigCore> SchemaWrite<C> for CompactPubkey {
    type Src = Self;

    #[inline]
    fn size_of(src: &Self::Src) -> WriteResult<usize> {
        match src {
            CompactPubkey::Id(id) => <u32 as SchemaWrite<C>>::size_of(id),
            CompactPubkey::Raw(_) => {
                Ok(<u32 as SchemaWrite<C>>::size_of(&Self::RAW_SENTINEL)? + 32)
            }
        }
    }

    #[inline]
    fn write(mut writer: impl Writer, src: &Self::Src) -> WriteResult<()> {
        match src {
            CompactPubkey::Id(id) => <u32 as SchemaWrite<C>>::write(writer, id),
            CompactPubkey::Raw(pubkey) => {
                <u32 as SchemaWrite<C>>::write(writer.by_ref(), &Self::RAW_SENTINEL)?;
                writer.write(pubkey)?;
                Ok(())
            }
        }
    }
}

unsafe impl<'de, C: ConfigCore> SchemaRead<'de, C> for CompactPubkey {
    type Dst = Self;

    #[inline]
    fn read(mut reader: impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let id = <u32 as SchemaRead<'de, C>>::get(reader.by_ref())?;
        if id != Self::RAW_SENTINEL {
            dst.write(CompactPubkey::Id(id));
            return Ok(());
        }

        let bytes = reader.take_array::<32>()?;
        dst.write(CompactPubkey::Raw(bytes));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leb128_config() -> impl wincode::config::Config {
        wincode::config::Configuration::default().with_int_encoding::<crate::Leb128>()
    }

    #[test]
    fn indexed_pubkey_is_a_bare_one_based_varint() {
        let bytes = wincode::config::serialize(&CompactPubkey::id(1), leb128_config()).unwrap();
        assert_eq!(bytes, [1]);
        let decoded: CompactPubkey =
            wincode::config::deserialize_exact(&bytes, leb128_config()).unwrap();
        assert_eq!(decoded, CompactPubkey::id(1));

        let bytes = wincode::config::serialize(&CompactPubkey::id(127), leb128_config()).unwrap();
        assert_eq!(bytes, [127]);

        let bytes = wincode::config::serialize(&CompactPubkey::id(128), leb128_config()).unwrap();
        assert_eq!(bytes, [0x80, 0x01]);
    }

    #[test]
    fn raw_pubkey_is_zero_sentinel_plus_32_bytes() {
        let pubkey = [7u8; 32];
        let bytes =
            wincode::config::serialize(&CompactPubkey::raw(pubkey), leb128_config()).unwrap();

        assert_eq!(bytes.len(), 33);
        assert_eq!(bytes[0], 0);
        assert_eq!(&bytes[1..], pubkey.as_slice());

        let decoded: CompactPubkey =
            wincode::config::deserialize_exact(&bytes, leb128_config()).unwrap();
        assert_eq!(decoded, CompactPubkey::raw(pubkey));
    }

    #[test]
    fn raw_zero_pubkey_is_valid() {
        let pubkey = [0u8; 32];
        let bytes =
            wincode::config::serialize(&CompactPubkey::raw(pubkey), leb128_config()).unwrap();

        assert_eq!(bytes.len(), 33);
        assert_eq!(bytes[0], 0);
        assert_eq!(&bytes[1..], pubkey.as_slice());

        let decoded: CompactPubkey =
            wincode::config::deserialize_exact(&bytes, leb128_config()).unwrap();
        assert_eq!(decoded, CompactPubkey::raw(pubkey));
    }
}
