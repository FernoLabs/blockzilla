use crate::{car, types::TransactionView};
use anyhow::{Context, Result, bail};
use reqwest::{
    StatusCode,
    blocking::Client,
    header::{self, HeaderValue},
};
use std::time::Duration;

pub(crate) fn load_transaction_view_from_http(
    car_url: &str,
    target_offset: u64,
    entry_size: u32,
    expected_signature: &[u8; 64],
) -> Result<TransactionView> {
    let entry = fetch_entry_range(car_url, target_offset, entry_size)?;
    car::decode_transaction_view_from_entry(&entry, expected_signature)
}

fn fetch_entry_range(car_url: &str, target_offset: u64, entry_size: u32) -> Result<Vec<u8>> {
    if entry_size == 0 {
        bail!("entry size cannot be zero");
    }

    let end = target_offset
        .checked_add(entry_size as u64)
        .and_then(|value| value.checked_sub(1))
        .context("range end overflow")?;
    let range = format!("bytes={target_offset}-{end}");

    let client = Client::builder()
        .http1_only()
        .tcp_nodelay(true)
        .timeout(Duration::from_secs(30))
        .build()
        .context("build HTTP client")?;

    let response = client
        .get(car_url)
        .header(header::RANGE, HeaderValue::from_str(&range)?)
        .send()
        .with_context(|| format!("GET {car_url}"))?;

    if response.status() != StatusCode::PARTIAL_CONTENT {
        bail!(
            "server did not honor byte range {} for {} (status {})",
            range,
            car_url,
            response.status()
        );
    }

    let body = response.bytes().context("read ranged CAR response")?;
    if body.len() != entry_size as usize {
        bail!(
            "ranged CAR response size mismatch: expected {} bytes, got {}",
            entry_size,
            body.len()
        );
    }

    Ok(body.to_vec())
}
