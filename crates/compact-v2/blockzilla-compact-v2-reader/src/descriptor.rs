use std::collections::HashSet;

use blockzilla_archive_v2::{
    ARCHIVE_V2_BLOCK_ACCESS_FILE, ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE, ARCHIVE_V2_BLOCK_INDEX_FILE,
    ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE, ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, ARCHIVE_V2_BLOCKS_FILE,
    ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE, ARCHIVE_V2_GENESIS_BIN_FILE,
    ARCHIVE_V2_GET_BLOCK_INDEX_FILE, ARCHIVE_V2_META_FILE, ARCHIVE_V2_POH_FILE,
    ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE, ARCHIVE_V2_PUBKEY_HOT_SEED_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE, ARCHIVE_V2_SHREDDING_FILE, ARCHIVE_V2_SIGNATURES_FILE,
    ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
};

use crate::{Error, Result};

pub const BLOCKS_FILE: &str = ARCHIVE_V2_BLOCKS_FILE;
pub const BLOCK_INDEX_FILE: &str = ARCHIVE_V2_BLOCK_INDEX_FILE;
pub const META_FILE: &str = ARCHIVE_V2_META_FILE;
pub const GENESIS_BIN_FILE: &str = ARCHIVE_V2_GENESIS_BIN_FILE;
pub const REGISTRY_FILE: &str = ARCHIVE_V2_PUBKEY_REGISTRY_FILE;
pub const SIGNATURES_FILE: &str = ARCHIVE_V2_SIGNATURES_FILE;

/// Files required by every readable Compact V2 archive.
pub const COMPACT_V2_REQUIRED_OBJECTS: [&str; 4] =
    [BLOCKS_FILE, BLOCK_INDEX_FILE, META_FILE, REGISTRY_FILE];

/// Format-defined sidecars that a Compact V2 archive can contain.
///
/// Readers probe only this fixed list. A directory listing or a separately
/// published inventory is not part of admission.
pub const COMPACT_V2_OPTIONAL_OBJECTS: [&str; 14] = [
    SIGNATURES_FILE,
    ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
    ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE,
    ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
    ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
    ARCHIVE_V2_POH_FILE,
    ARCHIVE_V2_SHREDDING_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE,
    ARCHIVE_V2_PUBKEY_HOT_SEED_FILE,
    ARCHIVE_V2_BLOCK_ACCESS_FILE,
    ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
    ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
];

/// Caller-supplied identity for one Compact V2 archive build.
///
/// `generation_id` is an opaque operator label. It is not derived from file
/// content. Change it when the object set is replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveIdentity {
    pub cluster_id: String,
    pub epoch: u64,
    pub generation_id: String,
    /// Exact first slot. This is explicit because Solana can use warm-up epochs.
    pub first_slot: u64,
    pub slots_per_epoch: u64,
}

impl ArchiveIdentity {
    pub fn mainnet(epoch: u64, generation_id: impl Into<String>) -> Result<Self> {
        const SLOTS_PER_EPOCH: u64 = 432_000;
        let first_slot = epoch
            .checked_mul(SLOTS_PER_EPOCH)
            .ok_or(Error::Overflow("mainnet epoch first slot"))?;
        Ok(Self {
            cluster_id: "mainnet-beta".into(),
            epoch,
            generation_id: generation_id.into(),
            first_slot,
            slots_per_epoch: SLOTS_PER_EPOCH,
        })
    }
}

/// Return the canonical mainnet epoch, first slot, and slot count for `slot`.
pub fn mainnet_identity_for_slot(
    slot: u64,
    generation_id: impl Into<String>,
) -> Result<ArchiveIdentity> {
    const SLOTS_PER_EPOCH: u64 = 432_000;
    let epoch = slot / SLOTS_PER_EPOCH;
    ArchiveIdentity::mainnet(epoch, generation_id)
}

/// How the source object set is kept stable while it is read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArchiveSourceBinding {
    /// The local range source pins regular-file identity and rejects changes.
    PinnedLocal,
    /// The network source pins the exact length and strong ETag of each object.
    /// `object_set_id` is an opaque metadata label made from those validators.
    StrongEtags { object_set_id: String },
}

impl ArchiveSourceBinding {
    pub fn object_set_id(&self) -> Option<&str> {
        match self {
            Self::PinnedLocal => None,
            Self::StrongEtags { object_set_id } => Some(object_set_id),
        }
    }
}

/// One exact-size object observed through the selected range source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveObject {
    pub name: String,
    pub size: u64,
}

/// Runtime identity and format-defined object inventory for one reader.
///
/// This value is made during open. It is not serialized or published, and it
/// has no content-digest fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveDescriptor {
    pub identity: ArchiveIdentity,
    pub objects: Vec<ArchiveObject>,
    pub source_binding: ArchiveSourceBinding,
}

impl ArchiveDescriptor {
    pub(crate) fn new(
        identity: ArchiveIdentity,
        objects: Vec<(String, u64)>,
        source_binding: ArchiveSourceBinding,
    ) -> Result<Self> {
        let descriptor = Self {
            identity,
            objects: objects
                .into_iter()
                .map(|(name, size)| ArchiveObject { name, size })
                .collect(),
            source_binding,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    pub fn validate(&self) -> Result<()> {
        validate_identity("cluster_id", &self.identity.cluster_id)?;
        validate_identity("generation_id", &self.identity.generation_id)?;
        if self.identity.slots_per_epoch == 0 {
            return Err(Error::InvalidLocalDescriptor(
                "slots_per_epoch must be greater than zero".into(),
            ));
        }
        self.identity
            .first_slot
            .checked_add(self.identity.slots_per_epoch - 1)
            .ok_or_else(|| {
                Error::InvalidLocalDescriptor("epoch slot range overflows u64".into())
            })?;
        if let ArchiveSourceBinding::StrongEtags { object_set_id } = &self.source_binding {
            validate_identity("object_set_id", object_set_id)?;
        }

        let mut names = HashSet::with_capacity(self.objects.len());
        for object in &self.objects {
            validate_object_name(&object.name)
                .map_err(|message| Error::InvalidLocalDescriptor(message.to_owned()))?;
            if !names.insert(object.name.as_str()) {
                return Err(Error::InvalidLocalDescriptor(format!(
                    "duplicate object entry {}",
                    object.name
                )));
            }
        }
        for required in COMPACT_V2_REQUIRED_OBJECTS {
            self.required_object(required)?;
        }
        Ok(())
    }

    pub fn object(&self, name: &str) -> Option<&ArchiveObject> {
        self.objects.iter().find(|object| object.name == name)
    }

    pub fn required_object(&self, name: &str) -> Result<&ArchiveObject> {
        self.object(name)
            .ok_or_else(|| Error::MissingLocalFile(name.to_owned()))
    }

    pub fn epoch_start_slot(&self) -> u64 {
        self.identity.first_slot
    }
}

fn validate_identity(field: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(Error::InvalidLocalDescriptor(format!("{field} is empty")));
    }
    if value.len() > 4096
        || value
            .bytes()
            .any(|byte| byte == 0 || byte.is_ascii_control())
    {
        return Err(Error::InvalidLocalDescriptor(format!(
            "{field} is too long or contains a control character"
        )));
    }
    Ok(())
}

pub(crate) fn validate_object_name(name: &str) -> std::result::Result<(), &'static str> {
    if name.is_empty() {
        return Err("object name is empty");
    }
    if name == "." || name == ".." || name.contains('/') || name.contains('\\') {
        return Err("object name must be one safe path component");
    }
    if name
        .bytes()
        .any(|byte| byte == 0 || byte.is_ascii_control())
    {
        return Err("object name contains a control character");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> ArchiveIdentity {
        ArchiveIdentity {
            cluster_id: "mainnet-beta".into(),
            epoch: 900,
            generation_id: "epoch-900-v3".into(),
            first_slot: 388_324_256,
            slots_per_epoch: 432_000,
        }
    }

    #[test]
    fn descriptor_requires_the_fixed_core_object_set() {
        let objects = COMPACT_V2_REQUIRED_OBJECTS
            .into_iter()
            .map(|name| (name.to_owned(), 32))
            .collect();
        let descriptor =
            ArchiveDescriptor::new(identity(), objects, ArchiveSourceBinding::PinnedLocal).unwrap();
        assert_eq!(descriptor.objects.len(), 4);
    }

    #[test]
    fn descriptor_rejects_path_traversal_and_empty_etag_binding() {
        let mut objects: Vec<_> = COMPACT_V2_REQUIRED_OBJECTS
            .into_iter()
            .map(|name| (name.to_owned(), 32))
            .collect();
        objects.push(("../secret".into(), 1));
        assert!(
            ArchiveDescriptor::new(identity(), objects, ArchiveSourceBinding::PinnedLocal).is_err()
        );
        let objects = COMPACT_V2_REQUIRED_OBJECTS
            .into_iter()
            .map(|name| (name.to_owned(), 32))
            .collect();
        assert!(
            ArchiveDescriptor::new(
                identity(),
                objects,
                ArchiveSourceBinding::StrongEtags {
                    object_set_id: String::new()
                },
            )
            .is_err()
        );
    }

    #[test]
    fn mainnet_geometry_uses_the_non_warmup_schedule() {
        let epoch_0 = ArchiveIdentity::mainnet(0, "e0").unwrap();
        assert_eq!((epoch_0.first_slot, epoch_0.slots_per_epoch), (0, 432_000));
        let epoch_900 = ArchiveIdentity::mainnet(900, "e900").unwrap();
        assert_eq!(
            (epoch_900.first_slot, epoch_900.slots_per_epoch),
            (388_800_000, 432_000)
        );
        assert_eq!(
            mainnet_identity_for_slot(388_800_000, "e900")
                .unwrap()
                .epoch,
            900
        );
    }
}
