use ahash::AHashMap;

/// Hard requirement: we always keep exactly the last 150 blockhashes from previous epoch (if any).
pub const PREV_TAIL_LEN: usize = 150;

/// Blockhash registry for one epoch + last 150 blockhashes of previous epoch.
///
/// Index convention (signed):
///   >= 0  → current epoch (index in `hashes`)
///   <  0  → previous epoch tail
///           -1 = last (newest)
///           -2 = second last
///           ...
///           -150 = 150th last
#[derive(Debug, Clone)]
pub struct BlockhashRegistry {
    /// All blockhashes for the current epoch (in seen order).
    pub hashes: Vec<[u8; 32]>,
    /// Last PREV_TAIL_LEN of previous epoch, oldest → newest, len <= PREV_TAIL_LEN.
    pub prev_tail: Vec<[u8; 32]>,
    /// Map blockhash bytes → signed id (>=0 current, <0 previous tail).
    pub index: AHashMap<[u8; 32], i32>,
}

impl BlockhashRegistry {
    pub fn new(hashes: Vec<[u8; 32]>, mut prev_tail: Vec<[u8; 32]>) -> Self {
        // Enforce invariant: at most PREV_TAIL_LEN.
        if prev_tail.len() > PREV_TAIL_LEN {
            prev_tail.drain(0..prev_tail.len() - PREV_TAIL_LEN);
        }

        let mut index = AHashMap::with_capacity(hashes.len() + prev_tail.len());

        // 1) Insert previous-epoch tail with NEGATIVE ids.
        //
        // prev_tail is oldest → newest, so:
        //   newest => -1
        //   second newest => -2
        //   ...
        let m = prev_tail.len();
        for (i, h) in prev_tail.iter().enumerate() {
            let id = -((m - i) as i32);
            index.insert(*h, id);
        }

        // 2) Insert current epoch with NON-NEGATIVE ids.
        // If a hash exists in both prev_tail and current epoch, current overwrites (preferred).
        for (i, h) in hashes.iter().enumerate() {
            index.insert(*h, i as i32);
        }

        Self {
            hashes,
            prev_tail,
            index,
        }
    }

    #[inline(always)]
    pub fn lookup(&self, h: &[u8; 32]) -> Option<i32> {
        // This checks both current epoch and prev_tail.
        self.index.get(h).copied()
    }

    #[inline(always)]
    pub fn contains(&self, h: &[u8; 32]) -> bool {
        self.index.contains_key(h)
    }

    /// Resolve a signed id back to a hash.
    #[inline(always)]
    pub fn get(&self, id: i32) -> Option<&[u8; 32]> {
        if id >= 0 {
            self.hashes.get(id as usize)
        } else {
            let k = (-id) as usize;
            if k == 0 || k > self.prev_tail.len() {
                None
            } else {
                // -1 => last element (newest)
                self.prev_tail.get(self.prev_tail.len() - k)
            }
        }
    }

    /// Helper: for a given current-epoch position `pos` (0-based),
    /// return the "previous blockhash id" you want in your CompactBlockHeader.
    ///
    /// For pos == 0, returns 0 (your convention).
    /// For pos > 0, returns pos - 1.
    #[inline(always)]
    pub fn previous_id_for_pos(pos: u32) -> u32 {
        if pos == 0 { 0 } else { pos - 1 }
    }
}
