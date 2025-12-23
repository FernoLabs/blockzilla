use anyhow::Result;
use std::io::{self, Read, Write};

use crate::carblock_to_compact::CompactBlock;
use crate::optimized_cbor::{decode_owned_compact_block, encode_compact_block_to_vec};
use crate::optimized_postcard;

#[derive(Clone, Copy, Debug)]
pub enum ArchiveFormat {
    Wincode,
    Postcard,
    Cbor,
}

impl ArchiveFormat {
    pub fn from_extension(path: &std::path::Path) -> Option<Self> {
        match path.extension().and_then(|s| s.to_str()) {
            Some("bin") => Some(Self::Wincode),
            Some("postcard") => Some(Self::Postcard),
            Some("cbor") => Some(Self::Cbor),
            _ => None,
        }
    }
}

pub fn write_block<W: Write>(writer: &mut W, block: &CompactBlock, format: ArchiveFormat) -> Result<()> {
    let encoded = match format {
        ArchiveFormat::Wincode => wincode::serialize(block)?,
        ArchiveFormat::Postcard => optimized_postcard::encode_compact_block(block)?,
        ArchiveFormat::Cbor => encode_compact_block_to_vec(block)?,
    };

    let len = encoded.len() as u32;
    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(&encoded)?;
    Ok(())
}

pub fn read_block<R: Read>(reader: &mut R, format: ArchiveFormat) -> Result<Option<CompactBlock>> {
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf) {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }

    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;

    let block = match format {
        ArchiveFormat::Wincode => wincode::deserialize(&buf)?,
        ArchiveFormat::Postcard => optimized_postcard::decode_compact_block(&buf)?,
        ArchiveFormat::Cbor => decode_owned_compact_block(&buf)?,
    };

    Ok(Some(block))
}
