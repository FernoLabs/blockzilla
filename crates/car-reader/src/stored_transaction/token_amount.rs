use std::mem::MaybeUninit;
use wincode::io::Reader;
use wincode::{ReadError, ReadResult, SchemaRead};
use crate::stored_transaction::OptionEof;

#[derive(Clone, Debug, PartialEq)]
pub struct StoredTokenAmount {
    pub ui_amount: f64,
    pub decimals: u8,
    pub amount: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StoredTokenAmount2 {
    pub ui_amount: OptionEof<f64>,
    pub decimals: u8,
    pub amount: String,
    pub ui_amount_string: String,
}

impl<'de> SchemaRead<'de> for StoredTokenAmount {
    type Dst = StoredTokenAmount;

    fn read(reader: &mut impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        // Try v2 format if first byte looks like a discriminant (0 or 1)
        if *reader.peek()? <= 1 {
            if let Some(v2) = try_decode_v2(reader) {
                dst.write(v2);
                return Ok(());
            }
        }

        // Fall back to v1: (f64, u8, String)
        dst.write(decode_v1(reader)?);
        Ok(())
    }
}

fn decode_v1<'de>(reader: &mut impl Reader<'de>) -> ReadResult<StoredTokenAmount> {
    let mut ui_amount = MaybeUninit::<f64>::uninit();
    f64::read(reader, &mut ui_amount)?;
    
    let mut decimals = MaybeUninit::<u8>::uninit();
    u8::read(reader, &mut decimals)?;
    
    let mut amount = MaybeUninit::<String>::uninit();
    String::read(reader, &mut amount)?;

    Ok(StoredTokenAmount {
        ui_amount: unsafe { ui_amount.assume_init() },
        decimals: unsafe { decimals.assume_init() },
        amount: unsafe { amount.assume_init() },
    })
}

fn try_decode_v2<'de>(reader: &mut impl Reader<'de>) -> Option<StoredTokenAmount> {
    // V2 layout: disc(u8) + optional_ui_amount(f64) + decimals(u8) + amount(String) + ui_amount_string(String)
    
    let disc = *reader.peek().ok()?;
    if disc > 1 {
        return None;
    }

    // Calculate header size and read it
    let header_len = 1 + if disc == 1 { 8 } else { 0 } + 1 + 8;
    let header = reader.fill_exact(header_len).ok()?;

    let mut pos = 0;
    
    // Skip discriminant
    pos += 1;
    
    // Read optional ui_amount
    let ui_amount_opt = if disc == 1 {
        let val = f64::from_le_bytes(header[pos..pos + 8].try_into().ok()?);
        pos += 8;
        Some(val)
    } else {
        None
    };

    // Read decimals (sanity check)
    let decimals = header[pos];
    pos += 1;
    if decimals > 18 {
        return None;
    }

    // Read amount length
    let amount_len = u64::from_le_bytes(header[pos..pos + 8].try_into().ok()?) as usize;
    if amount_len == 0 || amount_len > 128 {
        return None;
    }

    // Read amount + ui_amount_string length
    let prefix_len = header_len + amount_len + 8;
    let prefix = reader.fill_exact(prefix_len).ok()?;
    
    let ui_len = u64::from_le_bytes(
        prefix[header_len + amount_len..][..8].try_into().ok()?
    ) as usize;
    
    if ui_len > 128 {
        return None;
    }

    // Read complete structure
    let total_len = prefix_len + ui_len;
    let data = reader.fill_exact(total_len).ok()?;

    // Extract amount string
    let amount_bytes = &data[header_len..header_len + amount_len];
    let amount = std::str::from_utf8(amount_bytes).ok()?.to_owned();
    
    // Validate amount is all digits
    if !amount.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }

    // Extract ui_amount_string (handle empty case for zero amounts)
    let ui_str = if ui_len == 0 {
        if amount != "0" {
            return None;
        }
        "0"
    } else {
        let ui_bytes = &data[prefix_len..prefix_len + ui_len];
        std::str::from_utf8(ui_bytes).ok()?
    };

    // Validate consistency
    if !validate_ui_amount(amount.as_str(), decimals, ui_str) {
        return None;
    }

    let parsed_ui: f64 = ui_str.parse().ok()?;

    // Check ui_amount matches parsed value if present
    if let Some(ui_f) = ui_amount_opt {
        let tolerance = 1e-9_f64.max(parsed_ui.abs() * 1e-12);
        if (ui_f - parsed_ui).abs() > tolerance {
            return None;
        }
    }

    reader.consume(total_len).ok()?;

    Some(StoredTokenAmount {
        ui_amount: ui_amount_opt.unwrap_or(parsed_ui),
        decimals,
        amount,
    })
}

fn validate_ui_amount(amount: &str, decimals: u8, ui_amount_string: &str) -> bool {
    let Ok(ui_f) = ui_amount_string.parse::<f64>() else {
        return false;
    };
    let Ok(raw) = amount.parse::<u128>() else {
        return false;
    };
    
    let scale = 10f64.powi(decimals as i32);
    if !scale.is_finite() || scale == 0.0 {
        return false;
    }

    let expected = (raw as f64) / scale;
    let tolerance = 1e-9_f64.max(expected.abs() * 1e-12);
    
    (expected - ui_f).abs() <= tolerance
}