use ahash::AHashMap;
use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use compact_archive::format::CompactLogStream;
use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

const CB_PK: &str = "ComputeBudget111111111111111111111111111111";

#[derive(Debug, Clone, SchemaRead, SchemaWrite, Serialize, Deserialize)]
pub enum LogEvent {
    Invoke {
        pid: Option<u32>,
        is_cb: bool,
        pk_str_idx: Option<u16>,
    },

    Consumed {
        used: u32,
        limit: u32,
    },

    Success,

    Failure {
        reason_idx: u16,
    },

    Msg {
        str_idx: u16,
    },

    Return {
        pid: u32,
        data: Vec<u8>,
    },

    Data {
        data: Vec<u8>,
    },

    Consumption {
        units: u32,
    },

    CbSetComputeUnitLimit {
        units: u32,
    },
    CbSetComputeUnitPrice {
        micro_lamports: u64,
    },
    CbRequestUnits {
        units: u32,
    },
    CbSetHeapFrame {
        bytes: u32,
    },

    ProgramNotDeployed {
        pk_str_idx: Option<u16>,
    },

    TransferInsufficient {
        have: u64,
        need: u64,
    },

    CreateAccountAlreadyInUse {
        addr_fields_idx: u16,
    },

    AllocateAlreadyInUse {
        addr_fields_idx: u16,
    },

    Plain {
        str_idx: u16,
    },

    Unparsed {
        str_idx: u16,
    },
}

#[derive(Debug, Clone, Copy, Default)]
pub struct EncodeConfig {}

#[derive(Debug, Clone, Copy, Default)]
pub struct DecodeConfig {
    pub emit_unparsed_lines: bool,
}

#[inline(always)]
fn append_event(events: &mut Vec<LogEvent>, ev: LogEvent) {
    events.push(ev);
}

pub fn encode_logs<F>(lines: &[String], mut lookup_pid: F, _cfg: EncodeConfig) -> CompactLogStream
where
    F: FnMut(&str) -> Option<u32>,
{
    let mut strings = Vec::<String>::new();
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

    let cb_pid_auto = lookup_pid(CB_PK);
    let mut events = Vec::<LogEvent>::new();

    'lines: for line in lines {
        let line_bytes = line.as_bytes();

        if line_bytes.starts_with(b"Create Account: account Address { ")
            && line_bytes.ends_with(b" } already in use")
        {
            let pfx_len = "Create Account: account Address { ".len();
            let sfx_len = " } already in use".len();
            let inner = &line[pfx_len..line.len() - sfx_len];
            let idx = intern(inner);
            let ev = LogEvent::CreateAccountAlreadyInUse {
                addr_fields_idx: idx,
            };
            append_event(&mut events, ev);
            continue 'lines;
        }

        if line_bytes.starts_with(b"Allocate: account Address { ")
            && line_bytes.ends_with(b" } already in use")
        {
            let pfx_len = "Allocate: account Address { ".len();
            let sfx_len = " } already in use".len();
            let inner = &line[pfx_len..line.len() - sfx_len];
            let idx = intern(inner);
            let ev = LogEvent::AllocateAlreadyInUse {
                addr_fields_idx: idx,
            };
            append_event(&mut events, ev);
            continue 'lines;
        }

        if let Some(rest) = line.strip_prefix("Transfer: insufficient lamports ") {
            if let Some(need_pos) = rest.find(", need ") {
                let have_str = &rest[..need_pos];
                let need_str = &rest[need_pos + 7..];
                let have_result = have_str.replace(',', "").parse::<u64>();
                let need_result = need_str.replace(',', "").parse::<u64>();
                if let (Ok(have), Ok(need)) = (have_result, need_result) {
                    let ev = LogEvent::TransferInsufficient { have, need };
                    append_event(&mut events, ev);
                    continue 'lines;
                }
            }
            tracing::warn!(target: "compact_log", "malformed Transfer insufficient lamports line: {}", line);
            let ev = LogEvent::Unparsed {
                str_idx: intern(line),
            };
            append_event(&mut events, ev);
            continue 'lines;
        }

        if let Some(text) = line.strip_prefix("Program log: ") {
            let ev = LogEvent::Msg {
                str_idx: intern(text),
            };
            append_event(&mut events, ev);
            continue 'lines;
        }

        if let Some(rest) = line.strip_prefix("Program ") {
            if rest == "is not deployed" {
                let ev = LogEvent::ProgramNotDeployed { pk_str_idx: None };
                append_event(&mut events, ev);
                continue 'lines;
            }
            if let Some(pk) = rest.strip_suffix(" is not deployed") {
                let ev = LogEvent::ProgramNotDeployed {
                    pk_str_idx: Some(intern(pk.trim())),
                };
                append_event(&mut events, ev);
                continue 'lines;
            }

            if let Some(rem) = rest.strip_prefix("consumption: ") {
                if let Some(pos) = rem.find(" units remaining")
                    && let Ok(units) = rem[..pos].replace(',', "").parse::<u32>()
                {
                    let ev = LogEvent::Consumption { units };
                    append_event(&mut events, ev);
                    continue 'lines;
                }
                let ev = LogEvent::Unparsed {
                    str_idx: intern(line),
                };
                append_event(&mut events, ev);
                continue 'lines;
            }

            if let Some(space_pos) = rest.find(' ') {
                let pk = &rest[..space_pos];
                let after_pk = &rest[space_pos + 1..];
                let pid_res = lookup_pid(pk);
                let is_cb = pid_res == cb_pid_auto || pk == CB_PK;
                let pk_idx_opt = if pid_res.is_none() {
                    Some(intern(pk))
                } else {
                    None
                };

                if is_cb {
                    let norm = after_pk.replace(':', "").to_lowercase();

                    if let Some(tail) = norm.strip_prefix("request units ")
                        && let Ok(units) = tail.trim().replace(',', "").parse::<u32>()
                    {
                        let ev = LogEvent::CbRequestUnits { units };
                        append_event(&mut events, ev);
                        continue 'lines;
                    }
                }

                if let Some(depth_str) = after_pk.strip_prefix("invoke [")
                    && depth_str.ends_with(']')
                {
                    let ev = LogEvent::Invoke {
                        pid: pid_res,
                        is_cb,
                        pk_str_idx: pk_idx_opt,
                    };
                    append_event(&mut events, ev);
                    continue 'lines;
                }

                if after_pk == "success" {
                    let ev = LogEvent::Success;
                    append_event(&mut events, ev);
                    continue 'lines;
                }
            }
        }
        let ev = LogEvent::Plain {
            str_idx: intern(line),
        };
        append_event(&mut events, ev);
    }

    let bytes = wincode::serialize(&events).expect("encode events");

    CompactLogStream { bytes, strings }
}

pub fn decode_logs<G>(
    cls: &CompactLogStream,
    mut pid_to_string: G,
    cfg: DecodeConfig,
) -> Vec<String>
where
    G: FnMut(u32) -> String,
{
    let events: Vec<LogEvent> = wincode::deserialize(&cls.bytes).expect("wincode decode");
    let mut out = Vec::<String>::with_capacity(events.len());
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
                out.push(format!("Program {} invoke [{}]", who, depth));
                stack.push((pid, is_cb, pk_str_idx));
            }

            LogEvent::Consumed { used, limit } => {
                out.push(format!(
                    "Program consumed {} of {} compute units",
                    used, limit
                ));
            }

            LogEvent::Success => {
                out.push("Program success".into());
            }

            LogEvent::Failure { reason_idx } => {
                let reason = cls
                    .strings
                    .get(reason_idx as usize)
                    .cloned()
                    .unwrap_or_else(|| "?".into());
                out.push(format!("Program failed: {}", reason));
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

            LogEvent::CbSetComputeUnitLimit { units } => {
                out.push(format!(
                    "Program {} set compute unit limit {}",
                    CB_PK, units
                ));
            }

            LogEvent::CbSetComputeUnitPrice { micro_lamports } => {
                out.push(format!(
                    "Program {} set compute unit price {}",
                    CB_PK, micro_lamports
                ));
            }

            LogEvent::CbRequestUnits { units } => {
                out.push(format!("Program {} request units {}", CB_PK, units));
            }

            LogEvent::CbSetHeapFrame { bytes } => {
                out.push(format!("Program {} set heap frame {}", CB_PK, bytes));
            }

            LogEvent::ProgramNotDeployed { pk_str_idx } => {
                if let Some(ix) = pk_str_idx {
                    if let Some(pk) = cls.strings.get(ix as usize) {
                        out.push(format!("Program {} is not deployed", pk));
                    } else {
                        out.push("Program is not deployed".to_string());
                    }
                } else {
                    out.push("Program is not deployed".to_string());
                }
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
                }
            }

            LogEvent::AllocateAlreadyInUse { addr_fields_idx } => {
                if let Some(inner) = cls.strings.get(addr_fields_idx as usize) {
                    out.push(format!(
                        "Allocate: account Address {{ {} }} already in use",
                        inner
                    ));
                }
            }

            LogEvent::Plain { str_idx } => {
                if let Some(s) = cls.strings.get(str_idx as usize) {
                    out.push(s.clone());
                }
            }

            LogEvent::Unparsed { str_idx } => {
                if cfg.emit_unparsed_lines
                    && let Some(s) = cls.strings.get(str_idx as usize)
                {
                    out.push(s.clone());
                }
            }
        }
    }

    out
}
