use crate::carblock_to_compact::CompactBlock;

pub type EncodeError = postcard::Error;
pub type DecodeError = postcard::Error;

pub fn encode_compact_block_to_vec(
    block: &CompactBlock,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    buf.clear();
    postcard::to_io(block, buf)?;
    Ok(())
}

pub fn decode_owned_compact_block(bytes: &[u8]) -> Result<CompactBlock, DecodeError> {
    postcard::from_bytes(bytes)
}
