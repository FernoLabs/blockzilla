#[inline]
pub fn read_prefix<const N: usize>(data: &[u8]) -> Option<[u8; N]> {
    data.get(..N)?.try_into().ok()
}

#[inline]
pub fn read_u8(data: &[u8], offset: usize) -> Option<u8> {
    data.get(offset).copied()
}

#[inline]
pub fn read_u32_le(data: &[u8], offset: usize) -> Option<u32> {
    let end = offset.checked_add(4)?;
    Some(u32::from_le_bytes(data.get(offset..end)?.try_into().ok()?))
}

#[inline]
pub fn read_u64_le(data: &[u8], offset: usize) -> Option<u64> {
    let end = offset.checked_add(8)?;
    Some(u64::from_le_bytes(data.get(offset..end)?.try_into().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readers_reject_short_or_overflowing_ranges() {
        assert_eq!(read_prefix::<2>(&[1]), None);
        assert_eq!(read_u8(&[], 0), None);
        assert_eq!(read_u32_le(&[0; 3], 0), None);
        assert_eq!(read_u32_le(&[0; 4], usize::MAX), None);
        assert_eq!(read_u32_le(&1_u32.to_le_bytes(), 0), Some(1));
        assert_eq!(read_u64_le(&[0; 7], 0), None);
        assert_eq!(read_u64_le(&[0; 8], usize::MAX), None);
        assert_eq!(read_u64_le(&1_u64.to_le_bytes(), 0), Some(1));
    }
}
