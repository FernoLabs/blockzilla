use ahash::AHashMap;
use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};

/// Well-known ComputeBudget program id (base58).
const CB_PK: &str = "ComputeBudget111111111111111111111111111111";

/// Typed events stored via postcard.
/// References to free text are via indices into `CompactLogStream.strings`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogEvent {
    /// "Program <PK> invoke [n]"
    /// - `pid`: usage-id from registry if known
    /// - `is_cb`: marks ComputeBudget by well-known base58
    /// - `pk_str_idx`: present when pid is unknown so we can print the original base58
    Invoke {
        pid: Option<u32>,
        is_cb: bool,
        pk_str_idx: Option<u16>,
    },

    /// "Program <PK> consumed used of limit compute units" (absolute values).
    Consumed { used: u32, limit: u32 },

    /// "Program <PK> success"
    Success,

    /// "Program <PK> failed: <reason>" — `reason_idx` is in the shared string table.
    Failure { reason_idx: u16 },

    /// "Program log: <text>"
    Msg { str_idx: u16 },

    /// "Program return: <PK> <bytes...>" decoded from one or more base64 chunks
    Return { pid: u32, data: Vec<u8> },

    /// "Program data: <bytes...>" decoded from one or more base64 chunks
    Data { data: Vec<u8> },

    /// "Program consumption: <num> units remaining"
    Consumption { units: u32 },

    /// "Transfer: insufficient lamports <have>, need <need>"
    TransferInsufficient { have: u64, need: u64 },

    /// "Create Account: account Address { <fields> } already in use"
    CreateAccountAlreadyInUse { addr_fields_idx: u16 },

    /// "Allocate: account Address { <fields> } already in use"
    AllocateAlreadyInUse { addr_fields_idx: u16 },

    /// Plain line (no "Program " prefix) — preserved as-is, no warning.
    Plain { str_idx: u16 },

    /// Full original line for truly malformed structured cases we chose not to parse.
    Unparsed { str_idx: u16 },
}

/// Compact stream with postcard-encoded events + shared string table.
/// `bytes` is `postcard::to_allocvec(&Vec<LogEvent>)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactLogStream {
    pub bytes: Vec<u8>,
    pub strings: Vec<String>,
}

/// Encoder configuration (kept for forward-compat; currently empty).
#[derive(Debug, Clone, Copy, Default)]
pub struct EncodeConfig {}

/// Decoder configuration.
#[derive(Debug, Clone, Copy, Default)]
pub struct DecodeConfig {
    /// Emit UNPARSED lines on decode; if false, skip them.
    pub emit_unparsed_lines: bool,
}

/// Encode a list of legacy log lines into a compact stream with postcard.
/// `lookup_pid`: base58 program pubkey -> u32 id (from your registry).
pub fn encode_logs<F>(lines: &[String], mut lookup_pid: F, _cfg: EncodeConfig) -> CompactLogStream
where
    F: FnMut(&str) -> Option<u32>,
{
    let mut strings = Vec::<String>::new();
    // OPTIMIZATION 1: Use AHashMap instead of std HashMap for faster hashing
    let mut str_ix = AHashMap::<String, u16>::new();
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

    let mut events = Vec::<LogEvent>::with_capacity(lines.len());

    // Resolve CB pid if present in the registry (optional).
    let cb_pid_auto = lookup_pid(CB_PK);

    'lines: for line in lines {
        // OPTIMIZATION 2: Use as_bytes() for prefix checks when possible
        let line_bytes = line.as_bytes();

        // Known non-Program shapes first
        // Create Account: account Address { ... } already in use
        if line_bytes.starts_with(b"Create Account: account Address { ")
            && line_bytes.ends_with(b" } already in use")
        {
            let pfx_len = "Create Account: account Address { ".len();
            let sfx_len = " } already in use".len();
            let inner = &line[pfx_len..line.len() - sfx_len];
            let idx = intern(inner);
            events.push(LogEvent::CreateAccountAlreadyInUse {
                addr_fields_idx: idx,
            });
            continue 'lines;
        }

        // Allocate: account Address { ... } already in use
        if line_bytes.starts_with(b"Allocate: account Address { ")
            && line_bytes.ends_with(b" } already in use")
        {
            let pfx_len = "Allocate: account Address { ".len();
            let sfx_len = " } already in use".len();
            let inner = &line[pfx_len..line.len() - sfx_len];
            let idx = intern(inner);
            events.push(LogEvent::AllocateAlreadyInUse {
                addr_fields_idx: idx,
            });
            continue 'lines;
        }

        // 1) Standalone transfer error: "Transfer: insufficient lamports <have>, need <need>"
        if let Some(rest) = line.strip_prefix("Transfer: insufficient lamports ") {
            if let Some(need_pos) = rest.find(", need ") {
                let have_str = &rest[..need_pos];
                let need_str = &rest[need_pos + 7..];

                // OPTIMIZATION 3: Parse numbers without string allocation when no commas
                let have_result = if have_str.contains(',') {
                    have_str.replace(',', "").parse::<u64>()
                } else {
                    have_str.parse::<u64>()
                };

                let need_result = if need_str.contains(',') {
                    need_str.replace(',', "").parse::<u64>()
                } else {
                    need_str.parse::<u64>()
                };

                if let (Ok(have), Ok(need)) = (have_result, need_result) {
                    events.push(LogEvent::TransferInsufficient { have, need });
                    continue 'lines;
                }
            }
            // Malformed → keep lossless
            tracing::warn!(target: "compact_log", "malformed Transfer insufficient lamports line: {}", line);
            events.push(LogEvent::Unparsed {
                str_idx: intern(line),
            });
            continue 'lines;
        }

        // 2) "Program log: <text>" first so it never becomes "Program <PK> ..." with pk="log:"
        if let Some(text) = line.strip_prefix("Program log: ") {
            events.push(LogEvent::Msg {
                str_idx: intern(text),
            });
            continue 'lines;
        }

        // 3) "Program ..." umbrella
        if let Some(rest) = line.strip_prefix("Program ") {
            // 3a) Consumption lines (no PK): "Program consumption: <num> units remaining"
            if let Some(rem) = rest.strip_prefix("consumption: ") {
                if let Some(pos) = rem.find(" units remaining") {
                    let num_str = &rem[..pos];
                    // OPTIMIZATION 4: Avoid allocation for comma removal when not needed
                    let units_result = if num_str.contains(',') {
                        num_str.replace(',', "").parse::<u32>()
                    } else {
                        num_str.parse::<u32>()
                    };

                    if let Ok(units) = units_result {
                        events.push(LogEvent::Consumption { units });
                        continue 'lines;
                    }
                }
                tracing::warn!(target: "compact_log", "malformed Program consumption line: {}", line);
                events.push(LogEvent::Unparsed {
                    str_idx: intern(line),
                });
                continue 'lines;
            }

            // 3b) Return lines: "Program return: <PK> <b64> [<b64> ...]"
            if let Some(ret_tail) = rest.strip_prefix("return: ") {
                if let Some(space) = ret_tail.find(' ') {
                    let pk = &ret_tail[..space];
                    let b64_tail = &ret_tail[space + 1..];
                    if let Some(pid) = lookup_pid(pk) {
                        let mut buf = Vec::<u8>::new();
                        let mut ok = true;
                        for token in b64_tail.split_whitespace() {
                            match B64.decode(token.as_bytes()) {
                                Ok(bytes) => buf.extend_from_slice(&bytes),
                                Err(e) => {
                                    tracing::warn!(target: "compact_log",
                                        "base64 decode failed for Program return line: {} (err: {})",
                                        line, e);
                                    events.push(LogEvent::Unparsed {
                                        str_idx: intern(line),
                                    });
                                    ok = false;
                                    break;
                                }
                            }
                        }
                        if ok {
                            events.push(LogEvent::Return { pid, data: buf });
                        }
                        continue 'lines;
                    } else {
                        // Unknown PK for return: cannot reconstruct the PK w/o pid; keep lossless.
                        tracing::warn!(target: "compact_log", "unknown program id in Program return line: {}", line);
                        events.push(LogEvent::Unparsed {
                            str_idx: intern(line),
                        });
                        continue 'lines;
                    }
                }
                // malformed
                events.push(LogEvent::Unparsed {
                    str_idx: intern(line),
                });
                continue 'lines;
            }

            // 3c) Data lines: "Program data: <b64> [<b64> ...]"
            if let Some(data_tail) = rest.strip_prefix("data: ") {
                let mut buf = Vec::<u8>::new();
                let mut ok = true;
                for token in data_tail.split_whitespace() {
                    match B64.decode(token.as_bytes()) {
                        Ok(bytes) => buf.extend_from_slice(&bytes),
                        Err(e) => {
                            tracing::warn!(target: "compact_log",
                                "base64 decode failed for Program data line: {} (err: {})",
                                line, e);
                            events.push(LogEvent::Unparsed {
                                str_idx: intern(line),
                            });
                            ok = false;
                            break;
                        }
                    }
                }
                if ok && !buf.is_empty() {
                    events.push(LogEvent::Data { data: buf });
                } else if ok {
                    events.push(LogEvent::Unparsed {
                        str_idx: intern(line),
                    });
                }
                continue 'lines;
            }

            // 3d) Generic "Program <PK> ...": invoke, consumed, success, failed
            if let Some(space_pos) = rest.find(' ') {
                let pk = &rest[..space_pos];
                let after_pk = &rest[space_pos + 1..];

                // Try resolve pid; if missing, keep pk string index so we can still format exactly.
                let pid_res = lookup_pid(pk);
                let is_cb = pid_res == cb_pid_auto || pk == CB_PK;
                let pk_idx_opt = if pid_res.is_none() {
                    Some(intern(pk))
                } else {
                    None
                };

                // REQUIREMENT: unknown program id is an error and should be logged
                if pid_res.is_none() && pk != CB_PK {
                    tracing::warn!(target: "compact_log", "unknown program id in line: {}", line);
                }

                // invoke
                if let Some(depth_str) = after_pk.strip_prefix("invoke [")
                    && depth_str.ends_with(']') {
                        events.push(LogEvent::Invoke {
                            pid: pid_res,
                            is_cb,
                            pk_str_idx: pk_idx_opt,
                        });
                        continue 'lines;
                    }

                // consumed
                if let Some(consumed_tail) = after_pk.strip_prefix("consumed ")
                    && let Some(of_pos) = consumed_tail.find(" of ") {
                        let used_str = &consumed_tail[..of_pos];
                        let rest2 = &consumed_tail[of_pos + 4..];
                        if let Some(cu_pos) = rest2.find(" compute units") {
                            let limit_str = &rest2[..cu_pos];
                            if let (Ok(used), Ok(limit)) =
                                (used_str.parse::<u32>(), limit_str.parse::<u32>())
                            {
                                events.push(LogEvent::Consumed { used, limit });
                                continue 'lines;
                            }
                        }
                    }

                // success
                if after_pk == "success" {
                    events.push(LogEvent::Success);
                    continue 'lines;
                }

                // failed: <reason>
                if let Some(reason) = after_pk.strip_prefix("failed: ") {
                    events.push(LogEvent::Failure {
                        reason_idx: intern(reason),
                    });
                    continue 'lines;
                }

                // known-ish pk but unknown suffix → keep as plain to avoid warning spam
                events.push(LogEvent::Plain {
                    str_idx: intern(line),
                });
                continue 'lines;
            }

            // "Program " but not a known shape — keep as plain
            events.push(LogEvent::Plain {
                str_idx: intern(line),
            });
            continue 'lines;
        }

        // 4) Plain, non-Program line — keep as Plain (no warning)
        events.push(LogEvent::Plain {
            str_idx: intern(line),
        });
    }

    let bytes = to_allocvec(&events).expect("postcard serialize");
    CompactLogStream { bytes, strings }
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
    let events: Vec<LogEvent> = from_bytes(&cls.bytes).expect("postcard decode");
    let mut out = Vec::<String>::with_capacity(events.len());

    // Track call stack: (pid, is_cb, pk_str_idx) to format consumed/success/failure.
    let mut stack: Vec<(Option<u32>, bool, Option<u16>)> = Vec::with_capacity(8);

    for ev in events {
        match ev {
            LogEvent::Invoke {
                pid,
                is_cb,
                pk_str_idx,
            } => {
                let depth = stack.len() + 1;
                let who = if is_cb {
                    CB_PK.to_string()
                } else if let Some(pidv) = pid {
                    pid_to_string(pidv)
                } else if let Some(ix) = pk_str_idx {
                    cls.strings
                        .get(ix as usize)
                        .cloned()
                        .unwrap_or_else(|| "?".into())
                } else {
                    "?".into()
                };
                // OPTIMIZATION 5: Use format! directly instead of intermediate String
                out.push(format!("Program {} invoke [{}]", who, depth));
                stack.push((pid, is_cb, pk_str_idx));
            }

            LogEvent::Consumed { used, limit } => {
                let who = match stack.last().cloned() {
                    Some((Some(pidv), false, _)) => pid_to_string(pidv),
                    Some((_, true, _)) => CB_PK.to_string(),
                    Some((None, false, Some(ix))) => cls
                        .strings
                        .get(ix as usize)
                        .cloned()
                        .unwrap_or_else(|| "?".into()),
                    _ => "?".into(),
                };
                out.push(format!(
                    "Program {} consumed {} of {} compute units",
                    who, used, limit
                ));
            }

            LogEvent::Success => {
                let ctx = stack.pop();
                let who = match ctx {
                    Some((Some(pidv), false, _)) => pid_to_string(pidv),
                    Some((_, true, _)) => CB_PK.to_string(),
                    Some((None, false, Some(ix))) => cls
                        .strings
                        .get(ix as usize)
                        .cloned()
                        .unwrap_or_else(|| "?".into()),
                    _ => "?".into(),
                };
                out.push(format!("Program {} success", who));
            }

            LogEvent::Failure { reason_idx } => {
                let reason = cls
                    .strings
                    .get(reason_idx as usize)
                    .cloned()
                    .unwrap_or_else(|| "?".into());
                let ctx = stack.pop();
                let who = match ctx {
                    Some((Some(pidv), false, _)) => pid_to_string(pidv),
                    Some((_, true, _)) => CB_PK.to_string(),
                    Some((None, false, Some(ix))) => cls
                        .strings
                        .get(ix as usize)
                        .cloned()
                        .unwrap_or_else(|| "?".into()),
                    _ => "?".into(),
                };
                out.push(format!("Program {} failed: {}", who, reason));
            }

            LogEvent::Msg { str_idx } => {
                if let Some(s) = cls.strings.get(str_idx as usize) {
                    out.push(format!("Program log: {}", s));
                }
            }

            LogEvent::Return { pid, data } => {
                let b64 = B64.encode(&data);
                out.push(format!("Program return: {} {}", pid_to_string(pid), b64));
            }

            LogEvent::Data { data } => {
                let b64 = B64.encode(&data);
                out.push(format!("Program data: {}", b64));
            }

            LogEvent::Consumption { units } => {
                out.push(format!("Program consumption: {} units remaining", units));
            }

            LogEvent::TransferInsufficient { have, need } => {
                out.push(format!(
                    "Transfer: insufficient lamports {}, need {}",
                    have, need
                ));
            }

            LogEvent::CreateAccountAlreadyInUse { addr_fields_idx } => {
                if let Some(inner) = cls.strings.get(addr_fields_idx as usize) {
                    out.push(format!(
                        "Create Account: account Address {{ {} }} already in use",
                        inner
                    ));
                } else {
                    out.push("Create Account: account Address { ? } already in use".to_string());
                }
            }

            LogEvent::AllocateAlreadyInUse { addr_fields_idx } => {
                if let Some(inner) = cls.strings.get(addr_fields_idx as usize) {
                    out.push(format!(
                        "Allocate: account Address {{ {} }} already in use",
                        inner
                    ));
                } else {
                    out.push("Allocate: account Address { ? } already in use".to_string());
                }
            }

            LogEvent::Plain { str_idx } => {
                if let Some(s) = cls.strings.get(str_idx as usize) {
                    out.push(s.clone());
                }
            }

            LogEvent::Unparsed { str_idx } => {
                if cfg.emit_unparsed_lines
                    && let Some(s) = cls.strings.get(str_idx as usize) {
                        out.push(s.clone());
                    }
            }
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use ahash::AHashMap;

    fn pid_lookup() -> AHashMap<String, u32> {
        let mut map = AHashMap::new();
        map.insert(CB_PK.into(), 1);
        map.insert("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".into(), 2);
        map.insert("11111111111111111111111111111111".into(), 3);
        map
    }

    #[test]
    fn roundtrip_unknown_pid_and_plain_and_inuse() {
        let logs = vec![
            // unknown id program — should parse structurally and WARN:
            "Program G6EoTTTgpkNBtVXo96EQp2m6uwwVh2Kt6YidjkmQqoha invoke [2]".into(),
            "Program G6EoTTTgpkNBtVXo96EQp2m6uwwVh2Kt6YidjkmQqoha consumed 69464 of 185153 compute units".into(),
            "Program G6EoTTTgpkNBtVXo96EQp2m6uwwVh2Kt6YidjkmQqoha success".into(),
            // plain, non-Program lines:
            "Checking if destination stake is mergeable".into(),
            "Checking if source stake is mergeable".into(),
            "Merging stake accounts".into(),
            // in-use shapes:
            "Create Account: account Address { address: HQ1Z9F6..., base: None } already in use".into(),
            "Allocate: account Address { address: J7oxhNg..., base: None } already in use".into(),
        ];

        let map = pid_lookup();

        let cls = encode_logs(&logs, |s| map.get(s).copied(), EncodeConfig::default());
        let back = decode_logs(
            &cls,
            |id| {
                for (k, v) in map.iter() {
                    if *v == id {
                        return k.clone();
                    }
                }
                format!("PID{}", id)
            },
            DecodeConfig {
                emit_unparsed_lines: true,
            },
        );

        assert_eq!(back, logs);
    }
}
