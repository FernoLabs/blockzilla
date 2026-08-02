use crate::public_json::public_json_bytes;

// The live scheduler's initial snapshot is currently about 4.33 MiB. Keep the
// line bound comfortably above that production payload while remaining finite.
pub const MAX_SSE_LINE_BYTES: usize = 8 * 1024 * 1024;
const MAX_SAFE_INTEGER: u64 = (1_u64 << 53) - 1;

pub fn public_sse_line(line: &[u8]) -> Option<Vec<u8>> {
    if line.len() > MAX_SSE_LINE_BYTES {
        return None;
    }
    let has_newline = line.ends_with(b"\n");
    let mut content = if has_newline {
        &line[..line.len().saturating_sub(1)]
    } else {
        line
    };
    if content.ends_with(b"\r") {
        content = &content[..content.len().saturating_sub(1)];
    }
    if content.contains(&b'\r') || content.contains(&b'\n') {
        return None;
    }
    if content.is_empty() {
        return Some(if has_newline {
            b"\n".to_vec()
        } else {
            Vec::new()
        });
    }
    if content.starts_with(b":") {
        return Some(Vec::new());
    }

    let (field, mut raw) = content
        .iter()
        .position(|byte| *byte == b':')
        .map(|separator| (&content[..separator], &content[separator + 1..]))
        .unwrap_or((content, &[]));
    if raw.starts_with(b" ") {
        raw = &raw[1..];
    }
    let newline = if has_newline { b"\n".as_slice() } else { &[] };

    match field {
        b"data" => {
            if raw.is_empty() {
                return None;
            }
            let public = public_json_bytes(raw).ok()?;
            let mut output = Vec::with_capacity(5 + public.len() + newline.len());
            output.extend_from_slice(b"data:");
            output.extend_from_slice(&public);
            output.extend_from_slice(newline);
            Some(output)
        }
        b"event" => {
            if !matches!(raw, b"resync" | b"snapshot" | b"snapshot_patch") {
                return None;
            }
            let mut output = Vec::with_capacity(6 + raw.len() + newline.len());
            output.extend_from_slice(b"event:");
            output.extend_from_slice(raw);
            output.extend_from_slice(newline);
            Some(output)
        }
        b"id" | b"retry" => {
            if raw.is_empty() || raw.len() > 20 || !raw.iter().all(u8::is_ascii_digit) {
                return None;
            }
            let number = std::str::from_utf8(raw).ok()?.parse::<u64>().ok()?;
            if number > MAX_SAFE_INTEGER {
                return None;
            }
            let canonical = number.to_string();
            let mut output = Vec::with_capacity(field.len() + canonical.len() + newline.len() + 1);
            output.extend_from_slice(field);
            output.push(b':');
            output.extend_from_slice(canonical.as_bytes());
            output.extend_from_slice(newline);
            Some(output)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitizes_snapshot_data() {
        let line = br#"data: {"data":{"live":[{"capture_dir":"/tmp/private/live"}]}}
"#;
        let public = public_sse_line(line).unwrap();
        assert!(public.starts_with(b"data:"));
        assert!(
            !public
                .windows(b"/tmp/private".len())
                .any(|item| item == b"/tmp/private")
        );
        assert!(
            public
                .windows(b"\"capture_dir\":\"live\"".len())
                .any(|item| item == b"\"capture_dir\":\"live\"")
        );
    }

    #[test]
    fn canonicalizes_allowlisted_fields() {
        assert_eq!(
            public_sse_line(b"event: snapshot_patch\r\n").unwrap(),
            b"event:snapshot_patch\n"
        );
        assert_eq!(public_sse_line(b"id: 00042\n").unwrap(), b"id:42\n");
        assert_eq!(public_sse_line(b"retry: 1500\n").unwrap(), b"retry:1500\n");
        assert_eq!(public_sse_line(b": private heartbeat\n").unwrap(), b"");
        assert!(public_sse_line(b"event: private_debug\n").is_none());
        assert!(public_sse_line(b"x-internal: secret\n").is_none());
    }

    #[test]
    fn rejects_invalid_data_and_oversized_lines() {
        assert!(public_sse_line(b"data: not-json\n").is_none());
        assert!(public_sse_line(&vec![b'x'; MAX_SSE_LINE_BYTES + 1]).is_none());
    }

    #[test]
    fn accepts_a_production_sized_snapshot_line() {
        let payload_bytes = 4_331_439;
        let mut line = Vec::with_capacity(payload_bytes + 32);
        line.extend_from_slice(b"data:{\"padding\":\"");
        line.resize(line.len() + payload_bytes, b'a');
        line.extend_from_slice(b"\"}\n");
        assert!(line.len() < MAX_SSE_LINE_BYTES);
        assert!(public_sse_line(&line).is_some());
    }
}
