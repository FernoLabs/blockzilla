//! Which directory an Archive V2 object belongs in, and why.
//!
//! Archive V2 produces two products with different obligations, and until now
//! they shared one directory:
//!
//! - the **archive tier** — everything we promise to keep. Sized per byte of
//!   chain, read by indexers, replay and verification, and required for every
//!   epoch.
//! - the **edge tier** — `archive-v2-block-access.*`, a denormalised per-block
//!   blob that lets `workers/blockzilla-get-block` answer one request with a
//!   single ranged read against object storage. Derived, rebuildable from the
//!   archive, sized per *block* rather than per byte, and needed only for the
//!   epochs we choose to serve.
//!
//! Keeping them together made three things wrong at once: archive size reports
//! included a cache, coverage of the cache read as archive incompleteness, and
//! `registry_reprocess` rewrote ~100 GiB of cache per epoch that it could have
//! regenerated afterwards.
//!
//! The blob is not an index despite the shared prefix. The index is
//! `archive-v2-block-access.index` at [`ARCHIVE_V2_BLOCK_ACCESS_INDEX_ROW_LEN`]
//! = 32 bytes per block; the blob averages ~250 KiB per block because it
//! carries a second copy of the block's signatures plus a per-block
//! `id -> [u8; 32]` table. See `docs/design/storage-tiers.md`.

use std::path::{Path, PathBuf};

use super::archive::{
    ARCHIVE_V2_BLOCK_ACCESS_FILE, ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
    ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
};

/// Directory name for the durable archive tier under a shared base.
pub const ARCHIVE_TIER_DIR: &str = "archive";
/// Directory name for the edge/serving tier under a shared base.
pub const EDGE_TIER_DIR: &str = "edgezilla";
/// Directory name for retained upstream CARs under a shared base.
pub const OLD_FAITHFUL_DIR: &str = "old-faithful";

/// Which product an object belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageTier {
    /// Durable. Must exist for every epoch and must never be lost.
    Archive,
    /// Derived and rebuildable. Present only for epochs we choose to serve.
    Edge,
}

/// Objects that belong to the edge tier.
///
/// `archive-v2-get-block.index` is here for a reason that is easy to get wrong.
/// It reads like an archive index, but its rows carry
/// `(block_offset, block_len, access_offset, access_len)` — offsets into the
/// archive's `blocks.zstd` *and* into the edge blob — and
/// [`super::ArchiveV2GetBlockIndexRow::is_missing`] reports a block as missing
/// when `access_len == 0`. Its only production reader is
/// `workers/blockzilla-get-block`. It is a serving index that happens to be
/// stored beside the archive, and leaving it in the archive tier would break
/// the invariant below.
pub const EDGE_TIER_FILES: &[&str] = &[
    ARCHIVE_V2_BLOCK_ACCESS_FILE,
    ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
    ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
];

/// Classify one Archive V2 object by file name.
///
/// **The invariant: the durable tier never references the derived tier; the
/// derived tier may reference the durable tier.** An archive object may only
/// hold offsets into other archive objects, so the archive stays readable and
/// verifiable with no edge tier present at all. Edge objects are free to point
/// back into `blocks.zstd`, because losing the edge tier costs a rebuild rather
/// than data.
///
/// Any new object carrying an `access_offset` belongs in [`EDGE_TIER_FILES`].
pub fn tier_for_file(file_name: &str) -> StorageTier {
    if EDGE_TIER_FILES.contains(&file_name) {
        StorageTier::Edge
    } else {
        StorageTier::Archive
    }
}

/// Resolves the directory for each tier of one epoch.
///
/// Construct with [`ArchiveLayout::split`] for the tiered layout, or
/// [`ArchiveLayout::colocated`] to preserve the historical behaviour where both
/// tiers share one epoch directory. Colocated is what every generation written
/// before 2026-08-22 used, so readers must keep supporting it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveLayout {
    archive_root: PathBuf,
    edge_root: PathBuf,
}

impl ArchiveLayout {
    /// Tiered layout: `<base>/archive` and `<base>/edgezilla`.
    pub fn split(base: impl AsRef<Path>) -> Self {
        let base = base.as_ref();
        Self {
            archive_root: base.join(ARCHIVE_TIER_DIR),
            edge_root: base.join(EDGE_TIER_DIR),
        }
    }

    /// Historical layout: both tiers in the same epoch directory.
    pub fn colocated(root: impl AsRef<Path>) -> Self {
        let root = root.as_ref().to_path_buf();
        Self {
            archive_root: root.clone(),
            edge_root: root,
        }
    }

    /// Explicit roots, for deployments that place the tiers on different mounts.
    pub fn from_roots(archive_root: impl AsRef<Path>, edge_root: impl AsRef<Path>) -> Self {
        Self {
            archive_root: archive_root.as_ref().to_path_buf(),
            edge_root: edge_root.as_ref().to_path_buf(),
        }
    }

    /// True when the two tiers resolve to different directories.
    pub fn is_split(&self) -> bool {
        self.archive_root != self.edge_root
    }

    pub fn archive_root(&self) -> &Path {
        &self.archive_root
    }

    pub fn edge_root(&self) -> &Path {
        &self.edge_root
    }

    pub fn archive_dir(&self, epoch: u64) -> PathBuf {
        self.archive_root.join(epoch_dir_name(epoch))
    }

    pub fn edge_dir(&self, epoch: u64) -> PathBuf {
        self.edge_root.join(epoch_dir_name(epoch))
    }

    /// Directory an object with this file name belongs in, for `epoch`.
    pub fn dir_for_file(&self, epoch: u64, file_name: &str) -> PathBuf {
        match tier_for_file(file_name) {
            StorageTier::Archive => self.archive_dir(epoch),
            StorageTier::Edge => self.edge_dir(epoch),
        }
    }

    /// Full path for an object with this file name, for `epoch`.
    pub fn path_for_file(&self, epoch: u64, file_name: &str) -> PathBuf {
        self.dir_for_file(epoch, file_name).join(file_name)
    }

    pub fn block_access_blob(&self, epoch: u64) -> PathBuf {
        self.edge_dir(epoch).join(ARCHIVE_V2_BLOCK_ACCESS_FILE)
    }

    pub fn block_access_index(&self, epoch: u64) -> PathBuf {
        self.edge_dir(epoch)
            .join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE)
    }

    /// The getBlock serving index. Edge tier — see [`EDGE_TIER_FILES`].
    pub fn get_block_index(&self, epoch: u64) -> PathBuf {
        self.edge_dir(epoch).join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE)
    }
}

pub fn epoch_dir_name(epoch: u64) -> String {
    format!("epoch-{epoch}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_SIGNATURES_FILE};

    #[test]
    fn serving_objects_are_edge_tier() {
        for edge_file in [
            ARCHIVE_V2_BLOCK_ACCESS_FILE,
            ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
            // carries access_offset/access_len, so it cannot sit in the archive
            ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
        ] {
            assert_eq!(tier_for_file(edge_file), StorageTier::Edge, "{edge_file}");
        }
        for archive_file in [
            ARCHIVE_V2_BLOCKS_FILE,
            ARCHIVE_V2_BLOCK_INDEX_FILE,
            ARCHIVE_V2_SIGNATURES_FILE,
            ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
        ] {
            assert_eq!(
                tier_for_file(archive_file),
                StorageTier::Archive,
                "{archive_file}"
            );
        }
    }

    /// The durable tier must never reference the derived tier. Every object
    /// whose index rows carry an `access_offset` therefore has to be edge tier.
    #[test]
    fn nothing_in_the_archive_tier_holds_an_access_offset() {
        for file in [
            ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE, // access_offset -> edge blob
            ARCHIVE_V2_GET_BLOCK_INDEX_FILE,    // access_offset -> edge blob
        ] {
            assert_eq!(
                tier_for_file(file),
                StorageTier::Edge,
                "{file} holds an access offset and must not be archive tier"
            );
        }
    }

    #[test]
    fn edge_dir_holds_the_blob_with_its_own_index() {
        let layout = ArchiveLayout::split("/volume1/blockzilla");
        let dir = layout.edge_dir(1018);
        // one directory per epoch, containing the blob and the indexes that
        // address it -- nothing in the archive points here.
        for p in [
            layout.block_access_blob(1018),
            layout.block_access_index(1018),
            layout.get_block_index(1018),
        ] {
            assert_eq!(p.parent().unwrap(), dir);
        }
    }

    #[test]
    fn split_layout_separates_the_tiers() {
        let layout = ArchiveLayout::split("/volume1/blockzilla");
        assert!(layout.is_split());
        assert_eq!(
            layout.archive_dir(1018),
            Path::new("/volume1/blockzilla/archive/epoch-1018")
        );
        assert_eq!(
            layout.edge_dir(1018),
            Path::new("/volume1/blockzilla/edgezilla/epoch-1018")
        );
        assert_eq!(
            layout.block_access_blob(1018),
            Path::new("/volume1/blockzilla/edgezilla/epoch-1018/archive-v2-block-access.wincode")
        );
    }

    #[test]
    fn colocated_layout_preserves_historical_paths() {
        let layout = ArchiveLayout::colocated("/volume1/blockzilla/archive");
        assert!(!layout.is_split());
        assert_eq!(
            layout.block_access_blob(864),
            Path::new("/volume1/blockzilla/archive/epoch-864/archive-v2-block-access.wincode")
        );
        // every object resolves under the one directory
        assert_eq!(
            layout.path_for_file(864, ARCHIVE_V2_BLOCKS_FILE),
            layout.archive_dir(864).join(ARCHIVE_V2_BLOCKS_FILE)
        );
        assert_eq!(
            layout.dir_for_file(864, ARCHIVE_V2_BLOCK_ACCESS_FILE),
            layout.archive_dir(864)
        );
    }

    #[test]
    fn path_for_file_routes_by_tier() {
        let layout = ArchiveLayout::split("/base");
        assert_eq!(
            layout.path_for_file(7, ARCHIVE_V2_SIGNATURES_FILE),
            Path::new("/base/archive/epoch-7/signatures.bin")
        );
        assert_eq!(
            layout.path_for_file(7, ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE),
            Path::new("/base/edgezilla/epoch-7/archive-v2-block-access.index")
        );
    }
}
