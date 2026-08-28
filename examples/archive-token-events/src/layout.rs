use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_archive_sdk::create_private_directory;

/// Isolated paths for one epoch of the three-format example.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputLayout {
    pub root: PathBuf,
    pub archive_cache: PathBuf,
    pub car: FormatLayout,
    pub compact_v2: FormatLayout,
    pub indexer_v3: FormatLayout,
    pub comparison_dir: PathBuf,
    pub comparison_report: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatLayout {
    pub directory: PathBuf,
    pub database: PathBuf,
    pub report: PathBuf,
}

impl OutputLayout {
    pub fn new(root: impl Into<PathBuf>, epoch: u64) -> Self {
        let root = root.into();
        let archive_cache = root.join("archive-cache");
        let car = format_layout(&root, "car", epoch);
        let compact_v2 = format_layout(&root, "compact-v2", epoch);
        let indexer_v3 = format_layout(&root, "indexer-v3", epoch);
        let comparison_dir = root.join("comparison").join(format!("epoch-{epoch}"));
        let comparison_report = comparison_dir.join("comparison.json");
        Self {
            root,
            archive_cache,
            car,
            compact_v2,
            indexer_v3,
            comparison_dir,
            comparison_report,
        }
    }

    /// Create and verify only directories owned by this example.
    pub fn prepare(&self) -> Result<()> {
        create_private_directory(&self.root)?;
        for path in [
            &self.archive_cache,
            &self.car.directory,
            &self.compact_v2.directory,
            &self.indexer_v3.directory,
            &self.comparison_dir,
        ] {
            create_private_directory(path)?;
        }
        Ok(())
    }
}

fn format_layout(root: &Path, format: &str, epoch: u64) -> FormatLayout {
    let directory = root.join(format).join(format!("epoch-{epoch}"));
    FormatLayout {
        database: directory.join("token-events.sqlite"),
        report: directory.join("report.json"),
        directory,
    }
}

/// Refuse a symbolic-link report target before an atomic replacement.
pub fn validate_report_target(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            bail!("report target {} is a symbolic link", path.display())
        }
        Ok(metadata) => ensure!(
            metadata.is_file(),
            "report target {} is not a regular file",
            path.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| format!("inspect report {}", path.display()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_keeps_each_archive_in_its_own_epoch_folder() {
        let layout = OutputLayout::new("/tmp/example", 600);
        assert_eq!(
            layout.car.database,
            Path::new("/tmp/example/car/epoch-600/token-events.sqlite")
        );
        assert_eq!(
            layout.archive_cache,
            Path::new("/tmp/example/archive-cache")
        );
        assert_eq!(
            layout.indexer_v3.report,
            Path::new("/tmp/example/indexer-v3/epoch-600/report.json")
        );
        assert_eq!(
            layout.comparison_report,
            Path::new("/tmp/example/comparison/epoch-600/comparison.json")
        );
    }
}
