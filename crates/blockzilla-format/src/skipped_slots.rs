use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::Path,
};

use anyhow::{Context, Result, ensure};

pub const ARCHIVE_V2_SKIPPED_SLOTS_FILE: &str = "skipped_slots.bin";
pub const SKIPPED_SLOT_MAP_MAGIC: [u8; 8] = *b"BZSKIP1!";
pub const SKIPPED_SLOT_MAP_VERSION: u16 = 1;
pub const SKIPPED_SLOT_MAP_HEADER_LEN: usize = 32;
pub const SKIPPED_SLOT_MAP_FLAG_ONE_IS_SKIPPED: u32 = 1;

/// One fixed-width epoch bitmap. A set bit means that the slot was skipped.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SkippedSlotMap {
    epoch: Option<u64>,
    slots_per_epoch: u32,
    present_slots: u32,
    skipped: Vec<u8>,
}

impl SkippedSlotMap {
    pub fn new(slots_per_epoch: u32) -> Result<Self> {
        ensure!(slots_per_epoch > 0, "slots per epoch must be nonzero");
        let bytes = usize::try_from(slots_per_epoch)
            .context("slots per epoch exceeds usize")?
            .div_ceil(8);
        let mut skipped = vec![u8::MAX; bytes];
        mask_unused_trailing_bits(&mut skipped, slots_per_epoch);
        Ok(Self {
            epoch: None,
            slots_per_epoch,
            present_slots: 0,
            skipped,
        })
    }

    pub fn record_present(&mut self, slot: u64) -> Result<()> {
        let slots_per_epoch = u64::from(self.slots_per_epoch);
        let epoch = slot / slots_per_epoch;
        let expected_epoch = *self.epoch.get_or_insert(epoch);
        ensure!(
            epoch == expected_epoch,
            "slot {slot} is in epoch {epoch}, expected epoch {expected_epoch}"
        );
        let slot_in_epoch =
            usize::try_from(slot % slots_per_epoch).context("slot position exceeds usize")?;
        let byte = slot_in_epoch / 8;
        let mask = 1u8 << (slot_in_epoch % 8);
        ensure!(
            self.skipped[byte] & mask != 0,
            "duplicate produced slot {slot}"
        );
        self.skipped[byte] &= !mask;
        self.present_slots = self
            .present_slots
            .checked_add(1)
            .context("present slot count overflow")?;
        Ok(())
    }

    pub fn epoch(&self) -> Option<u64> {
        self.epoch
    }

    pub fn slots_per_epoch(&self) -> u32 {
        self.slots_per_epoch
    }

    pub fn present_slots(&self) -> u32 {
        self.present_slots
    }

    pub fn skipped_slots(&self) -> u32 {
        self.slots_per_epoch - self.present_slots
    }

    pub fn is_skipped(&self, slot: u64) -> Option<bool> {
        let epoch = self.epoch?;
        let slots_per_epoch = u64::from(self.slots_per_epoch);
        if slot / slots_per_epoch != epoch {
            return None;
        }
        let slot_in_epoch = usize::try_from(slot % slots_per_epoch).ok()?;
        let byte = *self.skipped.get(slot_in_epoch / 8)?;
        Some(byte & (1u8 << (slot_in_epoch % 8)) != 0)
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let epoch = self
            .epoch
            .context("skipped-slot map has no produced slots")?;
        let mut bytes = Vec::with_capacity(SKIPPED_SLOT_MAP_HEADER_LEN + self.skipped.len());
        bytes.extend_from_slice(&SKIPPED_SLOT_MAP_MAGIC);
        bytes.extend_from_slice(&SKIPPED_SLOT_MAP_VERSION.to_le_bytes());
        bytes.extend_from_slice(&(SKIPPED_SLOT_MAP_HEADER_LEN as u16).to_le_bytes());
        bytes.extend_from_slice(&SKIPPED_SLOT_MAP_FLAG_ONE_IS_SKIPPED.to_le_bytes());
        bytes.extend_from_slice(&epoch.to_le_bytes());
        bytes.extend_from_slice(&self.slots_per_epoch.to_le_bytes());
        bytes.extend_from_slice(&self.present_slots.to_le_bytes());
        bytes.extend_from_slice(&self.skipped);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() >= SKIPPED_SLOT_MAP_HEADER_LEN,
            "skipped-slot map is shorter than its header"
        );
        ensure!(
            bytes[..8] == SKIPPED_SLOT_MAP_MAGIC,
            "invalid skipped-slot map magic"
        );
        ensure!(
            u16::from_le_bytes(bytes[8..10].try_into().unwrap()) == SKIPPED_SLOT_MAP_VERSION,
            "unsupported skipped-slot map version"
        );
        ensure!(
            usize::from(u16::from_le_bytes(bytes[10..12].try_into().unwrap()))
                == SKIPPED_SLOT_MAP_HEADER_LEN,
            "invalid skipped-slot map header length"
        );
        ensure!(
            u32::from_le_bytes(bytes[12..16].try_into().unwrap())
                == SKIPPED_SLOT_MAP_FLAG_ONE_IS_SKIPPED,
            "unsupported skipped-slot map flags"
        );
        let epoch = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        let slots_per_epoch = u32::from_le_bytes(bytes[24..28].try_into().unwrap());
        let present_slots = u32::from_le_bytes(bytes[28..32].try_into().unwrap());
        ensure!(slots_per_epoch > 0, "slots per epoch must be nonzero");
        ensure!(
            present_slots <= slots_per_epoch,
            "present slot count exceeds slots per epoch"
        );
        let bitmap_len = usize::try_from(slots_per_epoch)
            .context("slots per epoch exceeds usize")?
            .div_ceil(8);
        ensure!(
            bytes.len() == SKIPPED_SLOT_MAP_HEADER_LEN + bitmap_len,
            "skipped-slot map has invalid bitmap length"
        );
        let skipped = bytes[SKIPPED_SLOT_MAP_HEADER_LEN..].to_vec();
        ensure_trailing_bits_are_zero(&skipped, slots_per_epoch)?;
        let skipped_bits: u32 = skipped.iter().map(|byte| byte.count_ones()).sum();
        ensure!(
            skipped_bits == slots_per_epoch - present_slots,
            "skipped-slot bitmap count does not match header"
        );
        Ok(Self {
            epoch: Some(epoch),
            slots_per_epoch,
            present_slots,
            skipped,
        })
    }
}

pub fn write_skipped_slot_map(path: &Path, map: &SkippedSlotMap) -> Result<()> {
    let bytes = map.encode()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .with_context(|| format!("create {}", temporary.display()))?;
    let result = (|| -> Result<()> {
        file.write_all(&bytes)
            .with_context(|| format!("write {}", temporary.display()))?;
        file.sync_all()
            .with_context(|| format!("sync {}", temporary.display()))?;
        drop(file);
        std::fs::rename(&temporary, path).with_context(|| format!("publish {}", path.display()))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

pub fn read_skipped_slot_map(path: &Path) -> Result<SkippedSlotMap> {
    let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let length = usize::try_from(
        file.metadata()
            .with_context(|| format!("stat {}", path.display()))?
            .len(),
    )
    .context("skipped-slot map size exceeds usize")?;
    let mut bytes = vec![0u8; length];
    file.read_exact(&mut bytes)
        .with_context(|| format!("read {}", path.display()))?;
    SkippedSlotMap::decode(&bytes)
}

fn mask_unused_trailing_bits(bitmap: &mut [u8], slots_per_epoch: u32) {
    let used_bits = slots_per_epoch % 8;
    if used_bits != 0 {
        let mask = (1u8 << used_bits) - 1;
        *bitmap.last_mut().expect("nonzero slots have a bitmap") &= mask;
    }
}

fn ensure_trailing_bits_are_zero(bitmap: &[u8], slots_per_epoch: u32) -> Result<()> {
    let used_bits = slots_per_epoch % 8;
    if used_bits != 0 {
        let unused_mask = !((1u8 << used_bits) - 1);
        ensure!(
            bitmap.last().expect("nonzero slots have a bitmap") & unused_mask == 0,
            "skipped-slot map has nonzero trailing bits"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_includes_leading_inner_and_trailing_skipped_slots() {
        let mut map = SkippedSlotMap::new(16).unwrap();
        map.record_present(18).unwrap();
        map.record_present(20).unwrap();
        let decoded = SkippedSlotMap::decode(&map.encode().unwrap()).unwrap();

        assert_eq!(decoded.epoch(), Some(1));
        assert_eq!(decoded.present_slots(), 2);
        assert_eq!(decoded.skipped_slots(), 14);
        assert_eq!(decoded.is_skipped(16), Some(true));
        assert_eq!(decoded.is_skipped(18), Some(false));
        assert_eq!(decoded.is_skipped(19), Some(true));
        assert_eq!(decoded.is_skipped(20), Some(false));
        assert_eq!(decoded.is_skipped(31), Some(true));
        assert_eq!(decoded.is_skipped(32), None);
    }

    #[test]
    fn rejects_duplicate_and_cross_epoch_slots() {
        let mut map = SkippedSlotMap::new(8).unwrap();
        map.record_present(9).unwrap();
        assert!(map.record_present(9).is_err());
        assert!(map.record_present(17).is_err());
    }
}
