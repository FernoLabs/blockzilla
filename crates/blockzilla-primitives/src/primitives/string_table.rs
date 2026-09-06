//! Length-prefixed string storage shared by log rendering and the record model.

use super::StrId;
use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

#[derive(Debug, Default, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct StringTable {
    pub lengths: Vec<u32>,
    pub bytes: Vec<u8>,
}

impl StringTable {
    #[inline]
    pub fn push(&mut self, s: &str) -> StrId {
        let id = self.lengths.len() as StrId;
        let len = u32::try_from(s.len()).expect("log string too large");
        self.lengths.push(len);
        self.bytes.extend_from_slice(s.as_bytes());
        id
    }

    #[inline]
    pub fn resolve(&self, id: StrId) -> &str {
        let id = id as usize;
        let start = self
            .lengths
            .iter()
            .take(id)
            .fold(0usize, |offset, len| offset + *len as usize);
        let end = start + self.lengths[id] as usize;
        std::str::from_utf8(&self.bytes[start..end]).expect("StringTable stores valid utf-8")
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.lengths.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.lengths.is_empty()
    }

    #[inline]
    pub fn iter(&self) -> StringTableIter<'_> {
        StringTableIter {
            table: self,
            next: 0,
            offset: 0,
        }
    }
}

pub struct StringTableIter<'a> {
    table: &'a StringTable,
    next: usize,
    offset: usize,
}

impl<'a> Iterator for StringTableIter<'a> {
    type Item = &'a str;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let len = *self.table.lengths.get(self.next)? as usize;
        let start = self.offset;
        let end = start + len;
        self.next += 1;
        self.offset = end;
        Some(
            std::str::from_utf8(&self.table.bytes[start..end])
                .expect("StringTable stores valid utf-8"),
        )
    }
}
