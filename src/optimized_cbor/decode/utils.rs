use crate::transaction_parser::MessageHeader;
use minicbor::Decoder;
use minicbor::data::Type;
use minicbor::decode::Error as DecodeError;

pub(super) fn decode_message_header(d: &mut Decoder<'_>) -> Result<MessageHeader, DecodeError> {
    let len = d.array()?.unwrap_or(0);
    if len != 3 {
        return Err(DecodeError::message("MessageHeader expects array[3]"));
    }
    let num_required_signatures = d.u32()? as u8;
    let num_readonly_signed_accounts = d.u32()? as u8;
    let num_readonly_unsigned_accounts = d.u32()? as u8;
    Ok(MessageHeader {
        num_required_signatures,
        num_readonly_signed_accounts,
        num_readonly_unsigned_accounts,
    })
}

pub(super) fn decode_bytes_fixed<'a>(
    d: &mut Decoder<'a>,
    expected: usize,
    label: &str,
) -> Result<&'a [u8], DecodeError> {
    let bytes = d.bytes()?;
    if bytes.len() != expected {
        return Err(DecodeError::message(&format!(
            "{label} expected {expected} bytes"
        )));
    }
    Ok(bytes)
}

pub(super) fn peek_is_null(d: &mut Decoder<'_>) -> Result<Option<()>, DecodeError> {
    if d.datatype()? == Type::Null {
        d.null()?;
        Ok(Some(()))
    } else {
        Ok(None)
    }
}
