use data_encoding::BASE64;

pub fn parse_canonical_base64(payload: &str) -> Option<Vec<u8>> {
    let payload = payload.trim();
    if payload.len() < 16 || payload.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return None;
    }

    let decoded_len = BASE64.decode_len(payload.len()).ok()?;
    let mut bytes = vec![0; decoded_len];
    let used = BASE64.decode_mut(payload.as_bytes(), &mut bytes).ok()?;
    bytes.truncate(used);

    (BASE64.encode(&bytes) == payload).then_some(bytes)
}

pub fn render_base64(bytes: &[u8]) -> String {
    BASE64.encode(bytes)
}
