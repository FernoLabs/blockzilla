#![allow(dead_code)]

use anyhow::{Result, anyhow};
use of_car_reader::compact_index::decode_offset_and_size;
use of_car_reader::slot_ranges::write_slot_ranges_raw;
use of_slot_ranges::{
    AsyncCompactIndex, BuildSlotRangesConfig, RangeReader, build_slot_ranges_from_indexes,
    decode_car_header_total_size,
};
use std::future::Future;
use std::pin::Pin;
use worker::{Fetch, Headers, Method, Request, RequestInit};

const OLD_FAITHFUL_URL: &str = "https://files.old-faithful.net";
const CAR_HEADER_PREFIX_READ_LEN: usize = 16;

pub async fn build_slot_ranges_raw_for_epoch(
    epoch: u64,
    max_bucket_payload_bytes: usize,
) -> Result<Vec<u8>> {
    let epoch_cid_url = format!("{OLD_FAITHFUL_URL}/{epoch}/epoch-{epoch}.cid");
    let epoch_cid = fetch_text(&epoch_cid_url).await?;
    let epoch_cid = epoch_cid.trim();
    if epoch_cid.is_empty() {
        return Err(anyhow!("empty epoch cid from {epoch_cid_url}"));
    }

    let slot_index_url =
        format!("{OLD_FAITHFUL_URL}/{epoch}/epoch-{epoch}-{epoch_cid}-mainnet-slot-to-cid.index");
    let cid_index_url = format!(
        "{OLD_FAITHFUL_URL}/{epoch}/epoch-{epoch}-{epoch_cid}-mainnet-cid-to-offset-and-size.index"
    );
    let car_url = format!("{OLD_FAITHFUL_URL}/{epoch}/epoch-{epoch}.car");

    let car_prefix = fetch_range_exact(&car_url, 0, CAR_HEADER_PREFIX_READ_LEN).await?;
    let car_header_size = decode_car_header_total_size(&car_prefix, &car_url)?;

    let slot_reader = WorkerRangeReader::new(slot_index_url.clone());
    let cid_reader = WorkerRangeReader::new(cid_index_url.clone());
    let mut slot_index = AsyncCompactIndex::open(slot_reader, slot_index_url).await?;
    let mut cid_index = AsyncCompactIndex::open(cid_reader, cid_index_url).await?;

    let output = build_slot_ranges_from_indexes(
        epoch,
        car_header_size,
        &mut slot_index,
        &mut cid_index,
        BuildSlotRangesConfig {
            max_bucket_payload_bytes,
            allow_node_read_fallback: false,
        },
    )
    .await?;

    let mut out =
        Vec::with_capacity(output.ranges.len() * of_car_reader::slot_ranges::SLOT_RANGE_ENTRY_SIZE);
    write_slot_ranges_raw(&mut out, &output.ranges)?;
    Ok(out)
}

pub async fn lookup_car_entry_range_for_cid(epoch: u64, cid: &[u8]) -> Result<(u64, u32)> {
    let epoch_cid_url = format!("{OLD_FAITHFUL_URL}/{epoch}/epoch-{epoch}.cid");
    let epoch_cid = fetch_text(&epoch_cid_url).await?;
    let epoch_cid = epoch_cid.trim();
    if epoch_cid.is_empty() {
        return Err(anyhow!("empty epoch cid from {epoch_cid_url}"));
    }

    let cid_index_url = format!(
        "{OLD_FAITHFUL_URL}/{epoch}/epoch-{epoch}-{epoch_cid}-mainnet-cid-to-offset-and-size.index"
    );
    let cid_reader = WorkerRangeReader::new(cid_index_url.clone());
    let mut cid_index = AsyncCompactIndex::open(cid_reader, cid_index_url).await?;

    let mut offset_and_size = vec![0u8; cid_index.value_size()];
    let found = cid_index
        .lookup_into_node_reads(cid, &mut offset_and_size)
        .await?;
    if !found {
        return Err(anyhow!("cid not found in epoch {epoch} cid index"));
    }

    let (offset, size) = decode_offset_and_size(&offset_and_size)?;
    Ok((offset, size))
}

struct WorkerRangeReader {
    url: String,
}

impl WorkerRangeReader {
    fn new(url: String) -> Self {
        Self { url }
    }
}

impl RangeReader for WorkerRangeReader {
    type ReadFuture<'a>
        = Pin<Box<dyn Future<Output = Result<()>> + 'a>>
    where
        Self: 'a;

    fn read_exact_at<'a>(&'a mut self, offset: u64, out: &'a mut [u8]) -> Self::ReadFuture<'a> {
        let url = self.url.clone();
        Box::pin(async move {
            let bytes = fetch_range_exact(&url, offset, out.len()).await?;
            out.copy_from_slice(&bytes);
            Ok(())
        })
    }
}

async fn fetch_text(url: &str) -> Result<String> {
    let request = Request::new(url, Method::Get)?;
    let mut response = Fetch::Request(request).send().await?;
    let status = response.status_code();
    if status != 200 {
        return Err(anyhow!("{url} returned HTTP {status}"));
    }
    response
        .text()
        .await
        .map_err(|err| anyhow!("read text body from {url}: {err}"))
}

async fn fetch_range_exact(url: &str, offset: u64, len: usize) -> Result<Vec<u8>> {
    if len == 0 {
        return Ok(Vec::new());
    }

    let end = offset
        .checked_add(len as u64)
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(|| anyhow!("range end overflow for {url}"))?;

    let headers = Headers::new();
    headers.set("Range", &format!("bytes={offset}-{end}"))?;

    let mut init = RequestInit::new();
    init.with_method(Method::Get);
    init.with_headers(headers);

    let request = Request::new_with_init(url, &init)?;
    let mut response = Fetch::Request(request).send().await?;
    let status = response.status_code();
    if status != 206 {
        return Err(anyhow!(
            "{url} range {offset}-{end} returned HTTP {status}, expected 206"
        ));
    }

    let bytes = response
        .bytes()
        .await
        .map_err(|err| anyhow!("read range body from {url}: {err}"))?;
    if bytes.len() != len {
        return Err(anyhow!(
            "{url} range {offset}-{end} returned {} bytes, expected {len}",
            bytes.len()
        ));
    }
    Ok(bytes)
}
