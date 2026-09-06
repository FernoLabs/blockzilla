//! Length-prefixed payload planes.
//!
//! Several planes are a sequence of opaque payloads, one per transaction or per
//! instruction: instruction data, logs, balances, token balances, return data,
//! rewards. They differ in what the bytes mean, not in how they are framed, so
//! they share this encoding.
//!
//! Absent and empty are distinct. A transaction with no recorded logs is not a
//! transaction with an empty log list, and collapsing the two would turn a
//! missing fact into a false negative — the same rule the coverage flags exist
//! to enforce. `None` encodes as `0`; an empty payload encodes as `1` followed
//! by zero bytes.

use thiserror::Error;

use crate::varint::{VarintError, read_uleb128, write_uleb128};

/// Decode guard on one payload.
pub const MAX_PAYLOAD_LEN: u64 = 64 << 20;

/// Append one optional payload.
pub fn write_payload(out: &mut Vec<u8>, payload: Option<&[u8]>) -> Result<(), BlobError> {
    match payload {
        None => write_uleb128(out, 0),
        Some(bytes) => {
            let len = bytes.len() as u64;
            if len > MAX_PAYLOAD_LEN {
                return Err(BlobError::TooLong(len));
            }
            // Shift by one so that zero can mean absent.
            write_uleb128(out, len + 1);
            out.extend_from_slice(bytes);
        }
    }
    Ok(())
}

/// Read one optional payload, advancing `cursor`.
pub fn read_payload<'a>(
    payload: &'a [u8],
    cursor: &mut usize,
) -> Result<Option<&'a [u8]>, BlobError> {
    let tagged = read_uleb128(payload, cursor)?;
    if tagged == 0 {
        return Ok(None);
    }
    let len = tagged - 1;
    if len > MAX_PAYLOAD_LEN {
        return Err(BlobError::TooLong(len));
    }
    let len = usize::try_from(len).map_err(|_| BlobError::TooLong(len))?;
    let remaining = payload.len() - *cursor;
    if len > remaining {
        return Err(BlobError::PayloadExceedsPage { len, remaining });
    }
    let bytes = &payload[*cursor..*cursor + len];
    *cursor += len;
    Ok(Some(bytes))
}

/// Encode a page of optional payloads.
pub fn encode_page(payloads: &[Option<Vec<u8>>]) -> Result<Vec<u8>, BlobError> {
    let mut out = Vec::new();
    for payload in payloads {
        write_payload(&mut out, payload.as_deref())?;
    }
    Ok(out)
}

/// Decode a page holding exactly `count` payloads.
pub fn decode_page(page: &[u8], count: usize) -> Result<Vec<Option<&[u8]>>, BlobError> {
    let mut cursor = 0_usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        out.push(read_payload(page, &mut cursor)?);
    }
    if cursor != page.len() {
        return Err(BlobError::TrailingBytes {
            consumed: cursor,
            total: page.len(),
        });
    }
    Ok(out)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum BlobError {
    #[error("payload stream: {0}")]
    Varint(#[from] VarintError),
    #[error("payload is {0} bytes, above the decode guard")]
    TooLong(u64),
    #[error("payload declares {len} bytes but the page has {remaining} left")]
    PayloadExceedsPage { len: usize, remaining: usize },
    #[error("page has {consumed} of {total} bytes consumed after the last payload")]
    TrailingBytes { consumed: usize, total: usize },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_and_empty_are_distinct() {
        let page = encode_page(&[None, Some(Vec::new()), Some(b"x".to_vec())]).unwrap();
        let decoded = decode_page(&page, 3).unwrap();
        assert_eq!(decoded[0], None);
        assert_eq!(decoded[1], Some(&[][..]));
        assert_eq!(decoded[2], Some(&b"x"[..]));
    }

    #[test]
    fn page_round_trips_and_is_deterministic() {
        let payloads = vec![
            Some(b"instruction payload".to_vec()),
            None,
            Some(vec![0u8; 300]),
        ];
        let page = encode_page(&payloads).unwrap();
        assert_eq!(page, encode_page(&payloads).unwrap());
        let decoded = decode_page(&page, payloads.len()).unwrap();
        assert_eq!(decoded[0], Some(&b"instruction payload"[..]));
        assert_eq!(decoded[1], None);
        assert_eq!(decoded[2].unwrap().len(), 300);
    }

    #[test]
    fn a_corrupt_length_cannot_force_a_large_read() {
        // Far out of range: caught by the fixed guard.
        let mut page = Vec::new();
        write_uleb128(&mut page, u64::from(u32::MAX));
        assert!(matches!(decode_page(&page, 1), Err(BlobError::TooLong(_))));

        // Under the guard but larger than the page can supply: caught by the
        // remaining-bytes bound, so allocation stays bounded by the page even
        // when the guard is generous.
        let mut page = Vec::new();
        write_uleb128(&mut page, 4096);
        assert!(matches!(
            decode_page(&page, 1),
            Err(BlobError::PayloadExceedsPage { len: 4095, .. })
        ));
    }

    #[test]
    fn a_wrong_count_is_detected() {
        let page = encode_page(&[Some(b"a".to_vec()), Some(b"b".to_vec())]).unwrap();
        assert!(matches!(
            decode_page(&page, 1),
            Err(BlobError::TrailingBytes { .. })
        ));
        assert!(decode_page(&page, 3).is_err());
    }
}
