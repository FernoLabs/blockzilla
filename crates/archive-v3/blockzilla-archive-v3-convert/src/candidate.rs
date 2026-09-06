//! Structural validation for one complete, unpublished Index Archive candidate.
//!
//! This gate proves that every required layout object exists, is a regular
//! file, has the same archive ID, and has the exact declared role and schema.
//! It is intentionally smaller than the future publication verifier, which
//! must also prove semantic parity, chain context, and finality.

use std::{
    fs::{self, File},
    os::unix::fs::FileExt,
    path::Path,
};

use anyhow::{Context, Result, ensure};
use blockzilla_archive_v3::{
    ArchiveId, FILE_HEADER_LEN, FileEncoding, FileHeader, LAYOUT, catalog::blocks as catalog_blocks,
};

use crate::container::validate_open_file;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CandidateValidation {
    pub archive_id: ArchiveId,
    pub required_objects: u64,
}

fn require_regular_file(path: &Path, relative_path: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect required candidate object {relative_path}"))?;
    ensure!(
        metadata.file_type().is_file(),
        "required candidate object {relative_path} is not a regular file"
    );
    Ok(())
}

fn archive_id_from_catalog(root: &Path) -> Result<ArchiveId> {
    let path = root.join(catalog_blocks::PATH);
    require_regular_file(&path, catalog_blocks::PATH)?;
    let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let mut bytes = [0_u8; FILE_HEADER_LEN];
    file.read_exact_at(&mut bytes, 0)
        .context("read candidate catalog header")?;
    let header = FileHeader::decode(&bytes).context("decode candidate catalog header")?;
    validate_open_file(&file, catalog_blocks::PATH, header.archive_id)?;
    Ok(header.archive_id)
}

/// Validate the complete physical layout of an unpublished candidate.
///
/// This function does not follow a symbolic link for a required object. The
/// converter can therefore validate the same staging tree that it will rename
/// without accepting bytes outside that tree.
pub fn validate_complete_candidate(root: &Path, epoch: u64) -> Result<CandidateValidation> {
    ensure!(root.is_dir(), "candidate root is not a directory");
    let archive_id = archive_id_from_catalog(root)?;
    let mut required_objects = 0_u64;

    for spec in LAYOUT.iter().filter(|spec| spec.required_for_epoch(epoch)) {
        let path = root.join(spec.path);
        require_regular_file(&path, spec.path)?;
        match spec.encoding {
            FileEncoding::HeaderedBinary => {
                let file = File::open(&path)
                    .with_context(|| format!("open required candidate object {}", spec.path))?;
                validate_open_file(&file, spec.path, archive_id)?;
            }
            FileEncoding::ExactBytes => {
                ensure!(
                    path.metadata()?.len() != 0,
                    "required exact-byte object {} is empty",
                    spec.path
                );
            }
            FileEncoding::Json => {
                anyhow::bail!(
                    "required control object {} needs a typed JSON validator",
                    spec.path
                );
            }
        }
        required_objects = required_objects
            .checked_add(1)
            .context("required object count overflow")?;
    }

    Ok(CandidateValidation {
        archive_id,
        required_objects,
    })
}

#[cfg(test)]
mod tests {
    use std::{fs, os::unix::fs::symlink};

    use blockzilla_archive_v3::{FileEncoding, LAYOUT};
    use tempfile::tempdir;

    use crate::container::write_payload;

    use super::*;

    fn complete_candidate(root: &Path, epoch: u64, archive_id: ArchiveId) {
        for spec in LAYOUT.iter().filter(|spec| spec.required_for_epoch(epoch)) {
            match spec.encoding {
                FileEncoding::HeaderedBinary => {
                    write_payload(root, spec.path, archive_id, 0, &[]).unwrap();
                }
                FileEncoding::ExactBytes => {
                    let path = root.join(spec.path);
                    fs::create_dir_all(path.parent().unwrap()).unwrap();
                    fs::write(path, [1]).unwrap();
                }
                FileEncoding::Json => unreachable!("no required JSON object in layout"),
            }
        }
    }

    #[test]
    fn complete_layout_has_one_archive_identity() {
        let root = tempdir().unwrap();
        let archive_id = ArchiveId::new([7; 16]);
        complete_candidate(root.path(), 1, archive_id);
        let result = validate_complete_candidate(root.path(), 1).unwrap();
        assert_eq!(result.archive_id, archive_id);
        assert_eq!(
            result.required_objects,
            LAYOUT
                .iter()
                .filter(|spec| spec.required_for_epoch(1))
                .count() as u64
        );
    }

    #[test]
    fn cross_archive_and_missing_objects_fail() {
        let root = tempdir().unwrap();
        complete_candidate(root.path(), 1, ArchiveId::new([7; 16]));
        write_payload(
            root.path(),
            blockzilla_archive_v3::indexes::slots::PATH,
            ArchiveId::new([8; 16]),
            0,
            &[],
        )
        .unwrap();
        assert!(validate_complete_candidate(root.path(), 1).is_err());

        fs::remove_file(
            root.path()
                .join(blockzilla_archive_v3::indexes::slots::PATH),
        )
        .unwrap();
        assert!(validate_complete_candidate(root.path(), 1).is_err());
    }

    #[test]
    fn required_object_symlinks_are_not_followed() {
        let root = tempdir().unwrap();
        complete_candidate(root.path(), 1, ArchiveId::new([7; 16]));
        let path = root
            .path()
            .join(blockzilla_archive_v3::indexes::slots::PATH);
        let saved = root.path().join("saved-slots");
        fs::rename(&path, &saved).unwrap();
        symlink(&saved, &path).unwrap();
        let error = validate_complete_candidate(root.path(), 1).unwrap_err();
        assert!(error.to_string().contains("not a regular file"));
    }
}
