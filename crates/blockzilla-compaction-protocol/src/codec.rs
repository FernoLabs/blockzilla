use crate::{ProtocolError, Result};

pub(crate) fn put_u8(output: &mut Vec<u8>, value: u8) {
    output.push(value);
}

pub(crate) fn put_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_be_bytes());
}

pub(crate) fn put_u32(output: &mut Vec<u8>, value: usize, field: &'static str) -> Result<()> {
    let value = u32::try_from(value).map_err(|_| ProtocolError::IntegerOverflow { field })?;
    output.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

pub(crate) fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_be_bytes());
}

pub(crate) fn put_bytes(output: &mut Vec<u8>, value: &[u8], field: &'static str) -> Result<()> {
    put_u32(output, value.len(), field)?;
    output.extend_from_slice(value);
    Ok(())
}

pub(crate) fn put_option<T>(
    output: &mut Vec<u8>,
    value: Option<&T>,
    encode: impl FnOnce(&mut Vec<u8>, &T) -> Result<()>,
) -> Result<()> {
    match value {
        None => put_u8(output, 0),
        Some(value) => {
            put_u8(output, 1);
            encode(output, value)?;
        }
    }
    Ok(())
}

pub(crate) struct Reader<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    pub(crate) const fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.input.len().saturating_sub(self.offset)
    }

    pub(crate) fn take(&mut self, length: usize, context: &'static str) -> Result<&'a [u8]> {
        let remaining = self.input.len().saturating_sub(self.offset);
        if length > remaining {
            return Err(ProtocolError::Truncated {
                context,
                needed: length,
                remaining,
            });
        }
        let start = self.offset;
        self.offset += length;
        Ok(&self.input[start..self.offset])
    }

    pub(crate) fn array<const N: usize>(&mut self, context: &'static str) -> Result<[u8; N]> {
        Ok(self
            .take(N, context)?
            .try_into()
            .expect("slice length was checked"))
    }

    pub(crate) fn u8(&mut self, context: &'static str) -> Result<u8> {
        Ok(self.take(1, context)?[0])
    }

    pub(crate) fn u16(&mut self, context: &'static str) -> Result<u16> {
        Ok(u16::from_be_bytes(self.array(context)?))
    }

    pub(crate) fn u32(&mut self, context: &'static str) -> Result<u32> {
        Ok(u32::from_be_bytes(self.array(context)?))
    }

    pub(crate) fn u64(&mut self, context: &'static str) -> Result<u64> {
        Ok(u64::from_be_bytes(self.array(context)?))
    }

    pub(crate) fn count(&mut self, max: usize, field: &'static str) -> Result<usize> {
        let count = u64::from(self.u32(field)?);
        if count > max as u64 {
            return Err(ProtocolError::CountOutOfBounds {
                field,
                max,
                actual: count,
            });
        }
        usize::try_from(count).map_err(|_| ProtocolError::IntegerOverflow { field })
    }

    pub(crate) fn bytes(&mut self, min: usize, max: usize, field: &'static str) -> Result<Vec<u8>> {
        let length = u64::from(self.u32(field)?);
        if length < min as u64 || length > max as u64 {
            return Err(ProtocolError::LengthOutOfBounds {
                field,
                min,
                max,
                actual: usize::try_from(length).unwrap_or(usize::MAX),
            });
        }
        let length =
            usize::try_from(length).map_err(|_| ProtocolError::IntegerOverflow { field })?;
        Ok(self.take(length, field)?.to_vec())
    }

    pub(crate) fn option<T>(
        &mut self,
        field: &'static str,
        decode: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<Option<T>> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => decode(self).map(Some),
            value => Err(ProtocolError::InvalidOptionTag { field, value }),
        }
    }

    pub(crate) fn finish(self, context: &'static str) -> Result<()> {
        let count = self.input.len().saturating_sub(self.offset);
        if count == 0 {
            Ok(())
        } else {
            Err(ProtocolError::TrailingBytes { context, count })
        }
    }
}

pub(crate) fn validate_len(
    value: &[u8],
    min: usize,
    max: usize,
    field: &'static str,
) -> Result<()> {
    if value.len() < min || value.len() > max {
        return Err(ProtocolError::LengthOutOfBounds {
            field,
            min,
            max,
            actual: value.len(),
        });
    }
    Ok(())
}
