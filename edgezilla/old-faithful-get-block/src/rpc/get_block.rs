use crate::render::{car_bytes_to_block_time, car_bytes_to_json_config};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GetBlockEncoding {
    Json,
    Base64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionDetails {
    Full,
    Signatures,
    None,
    Accounts,
}

#[derive(Debug, Clone, Copy)]
pub struct GetBlockConfig {
    pub encoding: GetBlockEncoding,
    pub transaction_details: TransactionDetails,
    pub rewards: bool,
}

impl Default for GetBlockConfig {
    fn default() -> Self {
        Self {
            encoding: GetBlockEncoding::Json,
            transaction_details: TransactionDetails::Full,
            rewards: true,
        }
    }
}

pub fn parse_get_block_config(value: Option<&Value>) -> Result<GetBlockConfig, String> {
    let mut config = GetBlockConfig::default();

    match value {
        None => {}
        Some(Value::String(encoding)) => {
            config.encoding = parse_encoding(encoding)?;
        }
        Some(Value::Object(map)) => {
            if let Some(encoding) = map.get("encoding").and_then(Value::as_str) {
                config.encoding = parse_encoding(encoding)?;
            }
            if let Some(details) = map.get("transactionDetails").and_then(Value::as_str) {
                config.transaction_details = match details {
                    "full" => TransactionDetails::Full,
                    "signatures" => TransactionDetails::Signatures,
                    "none" => TransactionDetails::None,
                    "accounts" => TransactionDetails::Accounts,
                    _ => return Err(format!("unsupported transactionDetails {details}")),
                };
            }
            if let Some(rewards) = map.get("rewards") {
                config.rewards = rewards
                    .as_bool()
                    .ok_or_else(|| "rewards must be a boolean".to_string())?;
            }
        }
        Some(_) => return Err("getBlock config must be a string or object".to_string()),
    }

    Ok(config)
}

pub fn render_get_block_json_bytes(
    bytes: Vec<u8>,
    previous_blockhash: Option<[u8; 32]>,
    config: GetBlockConfig,
) -> Result<Vec<u8>, String> {
    car_bytes_to_json_config(bytes, previous_blockhash, config)
}

pub fn render_get_block_time(bytes: Vec<u8>) -> Result<Option<i64>, String> {
    car_bytes_to_block_time(bytes)
}

pub fn contains_json_parsed(value: &Value) -> bool {
    match value {
        Value::String(value) => value == "jsonParsed",
        Value::Array(items) => items.iter().any(contains_json_parsed),
        Value::Object(map) => map.values().any(contains_json_parsed),
        _ => false,
    }
}

fn parse_encoding(value: &str) -> Result<GetBlockEncoding, String> {
    match value {
        "json" => Ok(GetBlockEncoding::Json),
        "jsonParsed" => Err("The jsonParsed encoding is not supported by this archive RPC".into()),
        "base58" => Err("The base58 transaction encoding is not supported by this archive RPC; use json or base64".into()),
        "base64" => Ok(GetBlockEncoding::Base64),
        _ => Err(format!("unsupported transaction encoding {value}")),
    }
}
