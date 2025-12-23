use crate::format::CompactBlock;
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

/// A complete compacted archive that contains all blocks and the
/// pubkey table needed to interpret their contents.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactArchive {
    pub pubkeys: Vec<[u8; 32]>,
    pub blocks: Vec<CompactBlock>,
}

impl CompactArchive {
    pub fn new(pubkeys: Vec<[u8; 32]>, blocks: Vec<CompactBlock>) -> Self {
        Self { pubkeys, blocks }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_allocvec(self)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }

    pub fn write_to_path<P: AsRef<Path>>(&self, path: P) -> std::io::Result<()> {
        let data = self
            .to_bytes()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        fs::write(path, data)
    }

    pub fn read_from_path<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        let data = fs::read(path)?;
        Self::from_bytes(&data).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }
}
