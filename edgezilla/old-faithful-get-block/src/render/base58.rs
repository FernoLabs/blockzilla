const ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

pub(crate) fn encode_string(bytes: &[u8]) -> Result<String, String> {
    let mut out = Vec::with_capacity(base58_turbo::BITCOIN.encoded_len(bytes.len()));
    encode_into_vec(bytes, &mut out)?;
    String::from_utf8(out).map_err(|err| format!("base58 output was not utf8: {err}"))
}

pub(crate) fn encode_into_vec(bytes: &[u8], out: &mut Vec<u8>) -> Result<(), String> {
    match bytes.len() {
        32 => {
            let bytes: &[u8; 32] = bytes.try_into().expect("checked 32-byte base58 input");
            let mut buf = [0u8; five8::BASE58_ENCODED_32_MAX_LEN];
            let len = five8::encode_32(bytes, &mut buf) as usize;
            out.extend_from_slice(&buf[..len]);
        }
        64 => {
            let bytes: &[u8; 64] = bytes.try_into().expect("checked 64-byte base58 input");
            let mut buf = [0u8; five8::BASE58_ENCODED_64_MAX_LEN];
            let len = five8::encode_64(bytes, &mut buf) as usize;
            out.extend_from_slice(&buf[..len]);
        }
        _ => encode_variable(bytes, out)?,
    }
    Ok(())
}

fn encode_variable(bytes: &[u8], out: &mut Vec<u8>) -> Result<(), String> {
    let start = out.len();
    out.resize(start + base58_turbo::BITCOIN.encoded_len(bytes.len()), 0);
    match base58_turbo::BITCOIN.encode_into(bytes, &mut out[start..]) {
        Ok(len) => {
            out.truncate(start + len);
            Ok(())
        }
        Err(base58_turbo::Error::InputTooBig) => {
            out.truncate(start);
            encode_large(bytes, out);
            Ok(())
        }
        Err(err) => {
            out.truncate(start);
            Err(format!("failed to encode base58: {err:?}"))
        }
    }
}

fn encode_large(bytes: &[u8], out: &mut Vec<u8>) {
    let zeroes = bytes.iter().take_while(|byte| **byte == 0).count();
    let mut digits = Vec::<u8>::with_capacity(base58_turbo::BITCOIN.encoded_len(bytes.len()));

    for byte in bytes {
        let mut carry = *byte as u32;
        for digit in digits.iter_mut().rev() {
            carry += (*digit as u32) << 8;
            *digit = (carry % 58) as u8;
            carry /= 58;
        }
        while carry > 0 {
            digits.insert(0, (carry % 58) as u8);
            carry /= 58;
        }
    }

    out.extend(std::iter::repeat_n(b'1', zeroes));
    out.extend(digits.into_iter().map(|digit| ALPHABET[digit as usize]));
}
