//! `runtime/logs.wincode`: dense transaction log records.
//!
//! Whole-record absence is owned by the transaction `EffectState`. A present
//! empty vector is a recorded empty log stream. Each line keeps exact text
//! fragments around dictionary-owned pubkey IDs.

use thiserror::Error;
use wincode::{SchemaRead, SchemaWrite};

use crate::wincode::{self as wire, ArchiveWincodeConfig};

pub const PATH: &str = "runtime/logs.wincode";
pub const SCHEMA: u16 = 1;
pub const MAX_LINES_PER_TRANSACTION: usize = 1 << 16;
pub const MAX_PUBKEYS_PER_LINE: usize = 1 << 12;
pub const MAX_FRAGMENT_LEN: usize = 1 << 20;
pub const MAX_TEXT_BYTES_PER_TRANSACTION: usize = 16 << 20;

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct LogLine {
    /// Must contain exactly one more item than `pubkey_ids`.
    pub fragments: Vec<String>,
    pub pubkey_ids: Vec<u32>,
}

impl LogLine {
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            fragments: vec![text.into()],
            pubkey_ids: Vec::new(),
        }
    }

    fn validate(&self) -> Result<usize, LogError> {
        if self.fragments.len() != self.pubkey_ids.len().saturating_add(1) {
            return Err(LogError::FragmentCountMismatch {
                fragments: self.fragments.len(),
                pubkeys: self.pubkey_ids.len(),
            });
        }
        if self.pubkey_ids.len() > MAX_PUBKEYS_PER_LINE {
            return Err(LogError::TooManyPubkeys(self.pubkey_ids.len()));
        }
        if self.pubkey_ids.contains(&0) {
            return Err(LogError::ReservedPubkeyId);
        }
        let mut total = 0_usize;
        for fragment in &self.fragments {
            if fragment.len() > MAX_FRAGMENT_LEN {
                return Err(LogError::FragmentTooLong(fragment.len()));
            }
            total = total
                .checked_add(fragment.len())
                .ok_or(LogError::TooManyTextBytes(usize::MAX))?;
        }
        Ok(total)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
struct LogRecord {
    lines: Vec<LogLine>,
}

fn validate_lines(lines: &[LogLine]) -> Result<(), LogError> {
    if lines.len() > MAX_LINES_PER_TRANSACTION {
        return Err(LogError::TooManyLines(lines.len()));
    }
    let mut text_bytes = 0_usize;
    for line in lines {
        text_bytes = text_bytes
            .checked_add(line.validate()?)
            .ok_or(LogError::TooManyTextBytes(usize::MAX))?;
        if text_bytes > MAX_TEXT_BYTES_PER_TRANSACTION {
            return Err(LogError::TooManyTextBytes(text_bytes));
        }
    }
    Ok(())
}

pub fn append_record(chunk: &mut Vec<u8>, lines: &[LogLine]) -> Result<(), LogError> {
    validate_lines(lines)?;
    let record = LogRecord {
        lines: lines.to_vec(),
    };
    wincode::config::serialize_into(chunk, &record, wire::archive_wincode_config())?;
    Ok(())
}

pub fn encode_record(lines: &[LogLine]) -> Result<Vec<u8>, LogError> {
    let mut bytes = Vec::new();
    append_record(&mut bytes, lines)?;
    Ok(bytes)
}

pub fn decode_chunk(bytes: &[u8], record_count: u32) -> Result<Vec<Vec<LogLine>>, LogError> {
    let mut remaining = bytes;
    let mut records = Vec::with_capacity(record_count as usize);
    for _ in 0..record_count {
        let record = <LogRecord as SchemaRead<'_, ArchiveWincodeConfig>>::get(&mut remaining)?;
        validate_lines(&record.lines)?;
        records.push(record.lines);
    }
    if !remaining.is_empty() {
        return Err(LogError::TrailingBytes(remaining.len()));
    }
    Ok(records)
}

#[derive(Debug, Error)]
pub enum LogError {
    #[error("log Wincode: {0}")]
    WincodeRead(#[from] wincode::ReadError),
    #[error("log Wincode: {0}")]
    WincodeWrite(#[from] wincode::WriteError),
    #[error("transaction has {0} log lines, above the decode guard")]
    TooManyLines(usize),
    #[error("line has {0} pubkeys, above the decode guard")]
    TooManyPubkeys(usize),
    #[error("line has {fragments} fragments for {pubkeys} pubkeys")]
    FragmentCountMismatch { fragments: usize, pubkeys: usize },
    #[error("pubkey ID zero is reserved")]
    ReservedPubkeyId,
    #[error("log fragment has {0} bytes, above the decode guard")]
    FragmentTooLong(usize),
    #[error("transaction logs have {0} text bytes, above the decode guard")]
    TooManyTextBytes(usize),
    #[error("log chunk has {0} trailing bytes")]
    TrailingBytes(usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dense_records_preserve_empty_and_pubkey_tokens() {
        let records = [
            vec![LogLine {
                fragments: vec!["Program ".into(), " invoke".into()],
                pubkey_ids: vec![7],
            }],
            Vec::new(),
        ];
        let mut chunk = Vec::new();
        for record in &records {
            append_record(&mut chunk, record).unwrap();
        }
        assert_eq!(decode_chunk(&chunk, 2).unwrap(), records);
    }

    #[test]
    fn invalid_line_shape_is_rejected() {
        let line = LogLine {
            fragments: Vec::new(),
            pubkey_ids: Vec::new(),
        };
        assert!(matches!(
            encode_record(&[line]),
            Err(LogError::FragmentCountMismatch { .. })
        ));
    }
}
