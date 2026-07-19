use std::{
    fs::{self, File},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerLayout {
    pub archive_dir: PathBuf,
    pub blocks_dir: PathBuf,
    pub index_dir: PathBuf,
    pub runtime_dir: PathBuf,
    pub poh_dir: PathBuf,
    pub shreds_dir: PathBuf,
    pub repair_dir: PathBuf,
    pub journal_dir: PathBuf,
}

impl ProducerLayout {
    pub fn new(archive_dir: impl Into<PathBuf>) -> Self {
        let archive_dir = archive_dir.into();
        Self {
            blocks_dir: archive_dir.join("blocks"),
            index_dir: archive_dir.join("index"),
            runtime_dir: archive_dir.join("runtime"),
            poh_dir: archive_dir.join("poh"),
            shreds_dir: archive_dir.join("shreds"),
            repair_dir: archive_dir.join("repair"),
            journal_dir: archive_dir.join("journal"),
            archive_dir,
        }
    }

    pub fn create(archive_dir: impl AsRef<Path>) -> Result<Self> {
        let layout = Self::new(archive_dir.as_ref());
        layout.create_dirs()?;
        layout.write_manifest()?;
        Ok(layout)
    }

    fn create_dirs(&self) -> Result<()> {
        for dir in [
            &self.archive_dir,
            &self.blocks_dir,
            &self.index_dir,
            &self.runtime_dir,
            &self.poh_dir,
            &self.shreds_dir,
            &self.repair_dir,
            &self.journal_dir,
        ] {
            fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        }
        Ok(())
    }

    fn write_manifest(&self) -> Result<()> {
        let manifest_path = self.archive_dir.join("producer-layout.json");
        let file = File::create(&manifest_path)
            .with_context(|| format!("create {}", manifest_path.display()))?;
        serde_json::to_writer_pretty(file, self)
            .with_context(|| format!("write {}", manifest_path.display()))
    }
}
