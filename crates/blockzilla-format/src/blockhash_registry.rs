#[cfg(not(target_arch = "wasm32"))]
use gxhash::{HashMap as RegistryHashMap, HashMapExt};
#[cfg(target_arch = "wasm32")]
use std::collections::HashMap as RegistryHashMap;

/// Blockhash registry for one epoch.
///
/// Record 0 is the boundary hash. It is the genesis hash for epoch 0 and the
/// final registry record from the prior epoch for every later epoch. Produced
/// blocks start at ID 1 and follow block order.
#[derive(Debug, Clone)]
pub struct BlockhashRegistry {
    /// Boundary hash followed by one hash for each produced block.
    pub hashes: Vec<[u8; 32]>,
    /// Map blockhash bytes to their non-negative registry ID.
    pub index: RegistryHashMap<[u8; 32], i32>,
}

impl BlockhashRegistry {
    pub fn new(hashes: Vec<[u8; 32]>) -> Self {
        let mut index = RegistryHashMap::with_capacity(hashes.len());
        for (id, hash) in hashes.iter().enumerate() {
            let id = i32::try_from(id).expect("blockhash registry ID exceeds i32::MAX");
            index.insert(*hash, id);
        }
        Self { hashes, index }
    }

    #[inline(always)]
    pub fn lookup(&self, hash: &[u8; 32]) -> Option<i32> {
        self.index.get(hash).copied()
    }

    #[inline(always)]
    pub fn contains(&self, hash: &[u8; 32]) -> bool {
        self.index.contains_key(hash)
    }

    #[inline(always)]
    pub fn get(&self, id: i32) -> Option<&[u8; 32]> {
        usize::try_from(id).ok().and_then(|id| self.hashes.get(id))
    }

    /// Return the blockhash ID for a zero-based produced-block ordinal.
    #[inline(always)]
    pub fn block_id_for_pos(pos: u32) -> Option<u32> {
        pos.checked_add(1)
    }

    /// Return the previous-blockhash ID for a zero-based block ordinal.
    #[inline(always)]
    pub fn previous_id_for_pos(pos: u32) -> u32 {
        pos
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boundary_is_id_zero_and_produced_blocks_start_at_one() {
        let boundary = [1; 32];
        let first_block = [2; 32];
        let registry = BlockhashRegistry::new(vec![boundary, first_block]);

        assert_eq!(registry.lookup(&boundary), Some(0));
        assert_eq!(registry.lookup(&first_block), Some(1));
        assert_eq!(BlockhashRegistry::block_id_for_pos(0), Some(1));
        assert_eq!(BlockhashRegistry::previous_id_for_pos(0), 0);
    }
}
