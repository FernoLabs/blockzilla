use crate::compact_log::CompactLogStream;
use car_reader::cbor_utils::CborArrayView;
use minicbor::Decoder;
use minicbor::decode::Error as DecodeError;

#[derive(Debug, Clone)]
pub struct CompactLogStreamRef<'a> {
    pub bytes: &'a [u8],
    pub strings: CborArrayView<'a, StrRef<'a>>,
}

impl<'b, C> minicbor::Decode<'b, C> for CompactLogStreamRef<'b> {
    fn decode(d: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, DecodeError> {
        let len = d.array()?.unwrap_or(0);
        if len != 2 {
            return Err(DecodeError::message("CompactLogStream expects array[2]"));
        }
        let bytes = d.bytes()?;
        let strings = d.decode_with(&mut ())?;
        Ok(Self { bytes, strings })
    }
}

impl<'a> CompactLogStreamRef<'a> {
    pub fn to_owned(&self) -> Result<CompactLogStream, DecodeError> {
        let mut strings = Vec::with_capacity(self.strings.len());
        for s in self.strings.iter() {
            strings.push(s?.to_string());
        }
        Ok(CompactLogStream {
            bytes: self.bytes.to_vec(),
            strings,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StrRef<'a> {
    pub value: &'a str,
}

impl<'b, C> minicbor::Decode<'b, C> for StrRef<'b> {
    fn decode(d: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, DecodeError> {
        let value = d.str()?;
        Ok(StrRef { value })
    }
}

impl<'a> StrRef<'a> {
    pub(super) fn to_string(&self) -> String {
        self.value.to_owned()
    }
}
