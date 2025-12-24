use crate::error::{CarReadError, CarReadResult};

const MAX_UVARINT_LEN_64: usize = 10;

/// Reads uvarint from an in-memory slice, returning (value, bytes_used).
#[inline]
fn read_uvarint_slice(buf: &[u8]) -> Option<(u64, usize)> {
    let mut x = 0u64;
    let mut s = 0u32;

    for (i, &b) in buf.iter().take(MAX_UVARINT_LEN_64).enumerate() {
        if b < 0x80 {
            return Some((x | ((b as u64) << s), i + 1));
        }
        x |= ((b & 0x7f) as u64) << s;
        s += 7;
        if s > 63 {
            return None;
        }
    }
    None
}

/// Returns the length in bytes of the CID at the beginning of a CAR entry,
/// without decoding it into a `Cid`. This is "header+digest".
///
/// Assumes CIDv1:
/// 0x01 + codec(uvarint) + mh_code(uvarint) + mh_len(uvarint) + digest[mh_len]
#[inline]
pub fn cid_bytes_len(entry: &[u8]) -> CarReadResult<usize> {
    if entry.is_empty() {
        return Err(CarReadError::Cid("empty entry".to_string()));
    }

    if entry[0] != 0x01 {
        return Err(CarReadError::Cid("expected CIDv1 (0x01)".to_string()));
    }

    let mut off = 1;

    let (_, used) = read_uvarint_slice(&entry[off..])
        .ok_or_else(|| CarReadError::Cid("truncated codec".to_string()))?;
    off += used;

    let (_, used) = read_uvarint_slice(&entry[off..])
        .ok_or_else(|| CarReadError::Cid("truncated mh_code".to_string()))?;
    off += used;

    let (mh_len, used) = read_uvarint_slice(&entry[off..])
        .ok_or_else(|| CarReadError::Cid("truncated mh_len".to_string()))?;
    off += used;

    let end = off + mh_len as usize;
    if entry.len() < end {
        return Err(CarReadError::Cid("multihash digest truncated".to_string()));
    }

    Ok(end)
}
