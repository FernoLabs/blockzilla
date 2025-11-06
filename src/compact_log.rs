// src/compact_log.rs
//! Compact, lossless encoding for Solana logs with structured patterns.
//!
//! Structured patterns (round-trippable):
//!   - Program <PK> invoke [n]
//!   - Program <PK> consumed X of Y compute units
//!   - Program <PK> success
//!   - Program <PK> failed: <reason>
//!   - Program return: <PK> <base64>
//!   - Program data: <base64>
//!   - Program log: <free text>   (all "Program log: ..." captured as free text)
//!
//! Anything we cannot parse is recorded as **UNPARSED** and we `tracing::warn!`
//! with the original line for observability.

use std::collections::HashMap;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};

/// Event kind tags.
const K_INVOKE:       u8 = 0;
const K_CONSUMED:     u8 = 1;
const K_SUCCESS:      u8 = 2;
const K_FAILURE:      u8 = 3;
const K_CB_INVOKE:    u8 = 4; // compute-budget shorthand
const K_CB_SUCCESS:   u8 = 5; // compute-budget shorthand
const K_MSG:          u8 = 6; // "Program log: <text>"
const K_RETURN:       u8 = 7; // "Program return: <PK> <base64-bytes>"
const K_DATA:         u8 = 8; // "Program data: <base64-bytes>"
const K_UNPARSED:     u8 = 9; // fallback for any unrecognized/undecodable line

/// Compact stream: event bytes plus a string table (reasons/free text/unparsed).
#[derive(Debug, Clone)]
pub struct CompactLogStream {
    pub bytes: Vec<u8>,
    pub strings: Vec<String>,
}

/// Encoder configuration.
#[derive(Debug, Clone, Copy, Default)]
pub struct EncodeConfig {
    /// Program-id index of ComputeBudget; when set, its invoke/success elide pid.
    pub compute_budget_program_id: Option<u32>,
    /// If false, we still store unparsed lines as UNPARSED but we won't attempt
    /// other passthrough shapes. (Kept for back-compat of intent; UNPARSED is always kept.)
    pub keep_unknown_lines: bool,
}

/// Decoder configuration.
#[derive(Debug, Clone, Copy, Default)]
pub struct DecodeConfig {
    /// Same value used at encode time for ComputeBudget (if any).
    pub compute_budget_program_id: Option<u32>,
    /// Emit UNPARSED lines on decode; if false, skip them.
    pub emit_unparsed_lines: bool,
}

/// Encode a list of legacy log lines into a compact, lossless stream.
/// `lookup_pid`: base58 program pubkey -> u32 id (from your registry).
pub fn encode_logs<F>(
    lines: &[String],
    mut lookup_pid: F,
    cfg: EncodeConfig,
) -> CompactLogStream
where
    F: FnMut(&str) -> Option<u32>,
{
    let mut out = Vec::<u8>::with_capacity(lines.len() * 2);
    let mut strings = Vec::<String>::new();
    let mut str_ix = HashMap::<String, u16>::new();

    // call stack frames: (pid, used_so_far, last_limit)
    let mut stack: Vec<(u32, u32, u32)> = Vec::with_capacity(8);

    // intern helper
    let mut intern = |s: &str| -> u16 {
        if let Some(&ix) = str_ix.get(s) {
            ix
        } else {
            let ix = strings.len() as u16;
            strings.push(s.to_string());
            str_ix.insert(s.to_string(), ix);
            ix
        }
    };

    for line in lines {
        // Fast path: Program-prefixed lines
        if let Some(rest) = line.strip_prefix("Program ") {
            // Handle "return" and "data" first (different header)
            if let Some(ret_tail) = rest.strip_prefix("return: ") {
                // Format: "<PK> <b64>"
                if let Some(space) = ret_tail.find(' ') {
                    let pk = &ret_tail[..space];
                    let b64 = &ret_tail[space + 1..];
                    match (lookup_pid(pk), B64.decode(b64.as_bytes())) {
                        (Some(pid), Ok(bytes)) => {
                            out.push(K_RETURN);
                            put_u32(&mut out, pid);
                            put_bytes(&mut out, &bytes);
                            continue;
                        }
                        (Some(_pid), Err(e)) => {
                            tracing::warn!(target: "compact_log", "base64 decode failed for Program return: {} …: {}", pk, e);
                        }
                        (None, _) => {
                            tracing::warn!(target: "compact_log", "unknown program id for Program return: {}", pk);
                        }
                    }
                } else {
                    tracing::warn!(target: "compact_log", "malformed Program return: {}", line);
                }
                // fallthrough → UNPARSED
            } else if let Some(data_tail) = rest.strip_prefix("data: ") {
                // Format: "<b64>"
                match B64.decode(data_tail.as_bytes()) {
                    Ok(bytes) => {
                        out.push(K_DATA);
                        put_bytes(&mut out, &bytes);
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!(target: "compact_log", "base64 decode failed for Program data: {} …", e);
                    }
                }
                // fallthrough → UNPARSED
            } else if let Some(space_pos) = rest.find(' ') {
                // Handle common "Program <PK> ..." forms
                let pk = &rest[..space_pos];
                let after_pk = &rest[space_pos + 1..];

                match lookup_pid(pk) {
                    Some(pid) => {
                        // invoke
                        if let Some(depth_str) = after_pk.strip_prefix("invoke [") {
                            if depth_str.ends_with(']') {
                                if Some(pid) == cfg.compute_budget_program_id {
                                    out.push(K_CB_INVOKE);
                                } else {
                                    out.push(K_INVOKE);
                                    put_u32(&mut out, pid);
                                }
                                stack.push((pid, 0, 0));
                                continue;
                            }
                        }

                        // consumed
                        if let Some(consumed_tail) = after_pk.strip_prefix("consumed ") {
                            if let Some(of_pos) = consumed_tail.find(" of ") {
                                let used_str = &consumed_tail[..of_pos];
                                let rest2 = &consumed_tail[of_pos + 4..];
                                if let Some(cu_pos) = rest2.find(" compute units") {
                                    let limit_str = &rest2[..cu_pos];
                                    if let (Ok(used), Ok(limit)) =
                                        (used_str.parse::<u32>(), limit_str.parse::<u32>())
                                    {
                                        if let Some((_pid, used_so_far, last_limit)) =
                                            stack.last_mut()
                                        {
                                            let delta = used.saturating_sub(*used_so_far);
                                            *used_so_far = used;

                                            out.push(K_CONSUMED);
                                            put_u32(&mut out, delta);

                                            // encode limit: 0 = same as previous; else (limit+1)
                                            if limit == *last_limit {
                                                put_u32(&mut out, 0);
                                            } else {
                                                put_u32(&mut out, limit.wrapping_add(1));
                                                *last_limit = limit;
                                            }
                                            continue;
                                        } else {
                                            // no frame: encode as absolute (delta from 0)
                                            out.push(K_CONSUMED);
                                            put_u32(&mut out, used);
                                            put_u32(&mut out, limit.wrapping_add(1));
                                            continue;
                                        }
                                    }
                                }
                            }
                        }

                        // success
                        if after_pk == "success" {
                            if Some(pid) == cfg.compute_budget_program_id {
                                out.push(K_CB_SUCCESS);
                            } else {
                                out.push(K_SUCCESS);
                            }
                            let _ = stack.pop();
                            continue;
                        }

                        // failed: <reason>
                        if let Some(reason) = after_pk.strip_prefix("failed: ") {
                            out.push(K_FAILURE);
                            put_u16(&mut out, intern(reason));
                            let _ = stack.pop();
                            continue;
                        }
                    }
                    None => {
                        tracing::warn!(target: "compact_log", "unknown program id: {}", pk);
                    }
                }
                // fallthrough → UNPARSED
            }
        }

        // "Program log: <text>"   (everything is free text)
        if let Some(text) = line.strip_prefix("Program log: ") {
            out.push(K_MSG);
            put_u16(&mut out, intern(text));
            continue;
        }

        // Fallback: UNPARSED (always kept; also logged)
        tracing::warn!(target: "compact_log", "unparsed log line: {}", line);
        out.push(K_UNPARSED);
        put_u16(&mut out, intern(line));
        // If caller set keep_unknown_lines=false, we still store as UNPARSED to be lossless.
        let _ = cfg.keep_unknown_lines;
    }

    CompactLogStream { bytes: out, strings }
}

/// Decode a compact stream back to legacy lines.
/// `pid_to_string`: u32 id -> base58 program pubkey string.
pub fn decode_logs<G>(
    cls: &CompactLogStream,
    mut pid_to_string: G,
    cfg: DecodeConfig,
) -> Vec<String>
where
    G: FnMut(u32) -> String,
{
    let mut cur = 0usize;
    let mut out = Vec::<String>::new();
    let mut stack: Vec<(u32, u32, u32)> = Vec::with_capacity(8); // (pid, used, last_limit)

    let cb_pk_string = cfg
        .compute_budget_program_id
        .map(|pid| pid_to_string(pid))
        .unwrap_or_else(|| "ComputeBudget111111111111111111111111111111".to_string());

    while cur < cls.bytes.len() {
        let kind = cls.bytes[cur];
        cur += 1;
        match kind {
            K_INVOKE => {
                if let Some(pid) = get_u32(&cls.bytes, &mut cur) {
                    let depth = stack.len() + 1;
                    let pk = pid_to_string(pid);
                    out.push(format!("Program {} invoke [{}]", pk, depth));
                    stack.push((pid, 0, 0));
                } else { break; }
            }
            K_CB_INVOKE => {
                let depth = stack.len() + 1;
                out.push(format!("Program {} invoke [{}]", cb_pk_string, depth));
                let pid = cfg.compute_budget_program_id.unwrap_or(0);
                stack.push((pid, 0, 0));
            }
            K_CONSUMED => {
                if let (Some(delta), Some(limit_enc)) =
                    (get_u32(&cls.bytes, &mut cur), get_u32(&cls.bytes, &mut cur))
                {
                    if let Some((pid, used_so_far, last_limit)) = stack.last_mut() {
                        let used_now = used_so_far.saturating_add(delta);
                        let limit = if limit_enc == 0 { *last_limit } else { limit_enc - 1 };
                        let pk = pid_to_string(*pid);
                        out.push(format!(
                            "Program {} consumed {} of {} compute units",
                            pk, used_now, limit
                        ));
                        *used_so_far = used_now;
                        *last_limit = limit;
                    }
                } else { break; }
            }
            K_SUCCESS => {
                if let Some((pid, _u, _l)) = stack.pop() {
                    let pk = pid_to_string(pid);
                    out.push(format!("Program {} success", pk));
                } else {
                    out.push("Program ? success".to_string());
                }
            }
            K_CB_SUCCESS => {
                let _ = stack.pop();
                out.push(format!("Program {} success", cb_pk_string));
            }
            K_FAILURE => {
                if let Some(idx) = get_u16(&cls.bytes, &mut cur) {
                    let reason = cls.strings.get(idx as usize).cloned().unwrap_or_else(|| "?".into());
                    if let Some((pid, _u, _l)) = stack.pop() {
                        let pk = pid_to_string(pid);
                        out.push(format!("Program {} failed: {}", pk, reason));
                    } else {
                        out.push(format!("Program ? failed: {}", reason));
                    }
                } else { break; }
            }
            K_MSG => {
                if let Some(idx) = get_u16(&cls.bytes, &mut cur) {
                    if let Some(s) = cls.strings.get(idx as usize) {
                        out.push(format!("Program log: {}", s));
                    }
                } else { break; }
            }
            K_RETURN => {
                if let Some(pid) = get_u32(&cls.bytes, &mut cur) {
                    if let Some(bytes) = get_bytes(&cls.bytes, &mut cur) {
                        let pk = pid_to_string(pid);
                        let b64 = B64.encode(bytes);
                        out.push(format!("Program return: {} {}", pk, b64));
                    } else { break; }
                } else { break; }
            }
            K_DATA => {
                if let Some(bytes) = get_bytes(&cls.bytes, &mut cur) {
                    let b64 = B64.encode(bytes);
                    out.push(format!("Program data: {}", b64));
                } else { break; }
            }
            K_UNPARSED => {
                if let Some(idx) = get_u16(&cls.bytes, &mut cur) {
                    if cfg.emit_unparsed_lines {
                        if let Some(s) = cls.strings.get(idx as usize) {
                            out.push(s.clone());
                        }
                    }
                } else { break; }
            }
            _ => break,
        }
    }

    out
}

//
// ----------------------------- low-level helpers -----------------------------
//

#[inline]
fn put_u32(dst: &mut Vec<u8>, mut v: u32) {
    while v >= 0x80 {
        dst.push(((v as u8) & 0x7F) | 0x80);
        v >>= 7;
    }
    dst.push(v as u8);
}

#[inline]
fn get_u32(src: &[u8], cur: &mut usize) -> Option<u32> {
    let mut shift = 0u32;
    let mut out = 0u32;
    while *cur < src.len() && shift <= 28 {
        let b = src[*cur];
        *cur += 1;
        out |= ((b & 0x7F) as u32) << shift;
        if b & 0x80 == 0 { return Some(out); }
        shift += 7;
    }
    None
}

#[inline]
fn put_u16(dst: &mut Vec<u8>, mut v: u16) {
    while v >= 0x80 {
        dst.push(((v as u8) & 0x7F) | 0x80);
        v >>= 7;
    }
    dst.push(v as u8);
}

#[inline]
fn get_u16(src: &[u8], cur: &mut usize) -> Option<u16> {
    let mut shift = 0u32;
    let mut out = 0u16;
    while *cur < src.len() && shift <= 14 {
        let b = src[*cur];
        *cur += 1;
        out |= (((b & 0x7F) as u16) << shift) as u16;
        if b & 0x80 == 0 { return Some(out); }
        shift += 7;
    }
    None
}

#[inline]
fn put_bytes(dst: &mut Vec<u8>, bytes: &[u8]) {
    put_u32(dst, bytes.len() as u32);
    dst.extend_from_slice(bytes);
}

#[inline]
fn get_bytes<'a>(src: &'a [u8], cur: &mut usize) -> Option<&'a [u8]> {
    let Some(len) = get_u32(src, cur) else { return None; };
    let len = len as usize;
    if *cur + len > src.len() { return None; }
    let s = &src[*cur .. *cur + len];
    *cur += len;
    Some(s)
}

//
// ----------------------------------- tests -----------------------------------
//

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn pid_lookup() -> HashMap<String, u32> {
        let mut map = HashMap::new();
        map.insert("ComputeBudget111111111111111111111111111111".into(), 1);
        map.insert("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL".into(), 2);
        map.insert("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".into(), 3);
        map.insert("11111111111111111111111111111111".into(), 4);
        map.insert("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P".into(), 5);
        map.insert("AxiomfHaWDemCFBLBayqnEnNwE6b7B2Qz3UmzMpgbMG6".into(), 6);
        map
    }

    #[test]
    fn roundtrip_with_return_data_and_unparsed() {
        let logs = vec![
            "Program ComputeBudget111111111111111111111111111111 invoke [1]".into(),
            "Program ComputeBudget111111111111111111111111111111 success".into(),
            "Program ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL invoke [1]".into(),
            "Program log: CreateIdempotent".into(),
            "Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA invoke [2]".into(),
            "Program log: Instruction: GetAccountDataSize".into(),
            "Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA consumed 1569 of 94295 compute units".into(),
            "Program return: TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA pQAAAAAAAAA=".into(),
            "Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA success".into(),
            "Program 11111111111111111111111111111111 invoke [2]".into(),
            "Program 11111111111111111111111111111111 success".into(),
            "Program log: Initialize the associated token account".into(),
            "Program data: AQIDBAU=".into(),
            // an intentionally weird/unparsed line:
            "Program ??? mystery".into(),
        ];

        let map = pid_lookup();

        let cls = encode_logs(
            &logs,
            |s| map.get(s).copied(),
            EncodeConfig {
                compute_budget_program_id: Some(1),
                keep_unknown_lines: true,
            },
        );

        let back = decode_logs(
            &cls,
            |id| {
                for (k, v) in map.iter() {
                    if *v == id { return k.clone(); }
                }
                format!("PID{}", id)
            },
            DecodeConfig {
                compute_budget_program_id: Some(1),
                emit_unparsed_lines: true,
            },
        );

        assert_eq!(back, logs);
    }
}
