use anyhow::{Context, Result, anyhow};
use clap::Parser;
use of_car_reader::{
    compact_index::decode_offset_and_size,
    slot_ranges::{SLOTS_PER_EPOCH, epoch_for_slot},
};
use of_slot_ranges::{AsyncCompactIndex, RangeReader, decode_car_header_total_size};
use reqwest::{
    blocking::Client,
    header::{HeaderMap, HeaderValue, RANGE},
};
use std::{
    fs::File,
    future::{Ready, ready},
    io::{Read, Write},
    path::PathBuf,
};

const CAR_HEADER_PREFIX_READ_LEN: usize = 16;

#[derive(Debug, Parser)]
#[command(name = "fetch-slot-slice")]
struct Cli {
    /// Epoch number.
    #[arg(long)]
    epoch: u64,

    /// First absolute slot to include.
    #[arg(long)]
    start_slot: u64,

    /// Last absolute slot to include.
    #[arg(long)]
    end_slot: u64,

    /// Output CAR subset path.
    #[arg(long)]
    output: PathBuf,

    /// Base URL for Old Faithful files.
    #[arg(long, default_value = "https://files.old-faithful.net")]
    base_url: String,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    anyhow::ensure!(
        cli.start_slot <= cli.end_slot,
        "start-slot must be <= end-slot"
    );
    anyhow::ensure!(
        epoch_for_slot(cli.start_slot) == cli.epoch && epoch_for_slot(cli.end_slot) == cli.epoch,
        "slot range {}..{} is not fully inside epoch {}",
        cli.start_slot,
        cli.end_slot,
        cli.epoch
    );

    let http = Client::builder()
        .user_agent("fetch-slot-slice/1.0")
        .build()
        .context("build HTTP client")?;

    let cid_url = format!("{}/{}/epoch-{}.cid", cli.base_url, cli.epoch, cli.epoch);
    let epoch_cid = get_text(&http, &cid_url)
        .with_context(|| format!("fetch {cid_url}"))?
        .trim()
        .to_string();
    let slot_index_url = format!(
        "{}/{}/epoch-{}-{}-mainnet-slot-to-cid.index",
        cli.base_url, cli.epoch, cli.epoch, epoch_cid
    );
    let cid_index_url = format!(
        "{}/{}/epoch-{}-{}-mainnet-cid-to-offset-and-size.index",
        cli.base_url, cli.epoch, cli.epoch, epoch_cid
    );
    let car_url = format!("{}/{}/epoch-{}.car", cli.base_url, cli.epoch, cli.epoch);

    let car_prefix = http_range_get(&http, &car_url, 0, CAR_HEADER_PREFIX_READ_LEN as u64 - 1)
        .with_context(|| format!("fetch CAR prefix from {car_url}"))?;
    let car_header_size =
        decode_car_header_total_size(&car_prefix, &car_url).context("decode CAR header length")?;

    let slot_reader = HttpRangeReader::new(http.clone(), slot_index_url.clone());
    let cid_reader = HttpRangeReader::new(http.clone(), cid_index_url.clone());
    let mut slot_index =
        futures::executor::block_on(AsyncCompactIndex::open(slot_reader, slot_index_url.clone()))
            .with_context(|| format!("open {slot_index_url}"))?;
    let mut cid_index =
        futures::executor::block_on(AsyncCompactIndex::open(cid_reader, cid_index_url.clone()))
            .with_context(|| format!("open {cid_index_url}"))?;

    let start_offset = find_previous_end_or_header(
        &mut slot_index,
        &mut cid_index,
        cli.epoch,
        cli.start_slot,
        car_header_size,
    )?;
    let (last_present_slot, end_offset) =
        find_last_present_end(&mut slot_index, &mut cid_index, cli.epoch, cli.end_slot)?;
    anyhow::ensure!(
        end_offset > start_offset,
        "computed empty slice: start_offset={start_offset} end_offset={end_offset}"
    );

    let body_len = end_offset - start_offset;
    let total_len = car_header_size + body_len;
    eprintln!(
        "epoch={} slots={}..{} last_present_slot={} car_offsets={}..{} body_bytes={} total_output_bytes={}",
        cli.epoch,
        cli.start_slot,
        cli.end_slot,
        last_present_slot,
        start_offset,
        end_offset,
        body_len,
        total_len
    );

    let tmp_path = cli.output.with_extension("tmp");
    if let Some(parent) = cli.output.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output dir {}", parent.display()))?;
    }
    let mut output =
        File::create(&tmp_path).with_context(|| format!("create {}", tmp_path.display()))?;
    fetch_range_to_writer(&http, &car_url, 0, car_header_size, &mut output)
        .context("write CAR header")?;
    fetch_range_to_writer(&http, &car_url, start_offset, body_len, &mut output)
        .context("write CAR body slice")?;
    output
        .flush()
        .with_context(|| format!("flush {}", tmp_path.display()))?;
    std::fs::rename(&tmp_path, &cli.output)
        .with_context(|| format!("publish {} -> {}", tmp_path.display(), cli.output.display()))?;
    println!(
        "slot_slice epoch={} slots={}..{} output={} body_bytes={} total_output_bytes={}",
        cli.epoch,
        cli.start_slot,
        cli.end_slot,
        cli.output.display(),
        body_len,
        total_len
    );
    Ok(())
}

fn find_previous_end_or_header<S, C>(
    slot_index: &mut AsyncCompactIndex<S>,
    cid_index: &mut AsyncCompactIndex<C>,
    epoch: u64,
    start_slot: u64,
    car_header_size: u64,
) -> Result<u64>
where
    S: RangeReader,
    C: RangeReader,
{
    let epoch_start = epoch
        .checked_mul(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch start overflow"))?;
    let mut slot = start_slot;
    while slot > epoch_start {
        slot -= 1;
        if let Some(end) = lookup_slot_end(slot_index, cid_index, slot)? {
            return Ok(end);
        }
    }
    Ok(car_header_size)
}

fn find_last_present_end<S, C>(
    slot_index: &mut AsyncCompactIndex<S>,
    cid_index: &mut AsyncCompactIndex<C>,
    epoch: u64,
    end_slot: u64,
) -> Result<(u64, u64)>
where
    S: RangeReader,
    C: RangeReader,
{
    let epoch_start = epoch
        .checked_mul(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch start overflow"))?;
    let mut slot = end_slot;
    loop {
        if let Some(end) = lookup_slot_end(slot_index, cid_index, slot)? {
            return Ok((slot, end));
        }
        anyhow::ensure!(
            slot > epoch_start,
            "no present slot found at or before {end_slot} in epoch {epoch}"
        );
        slot -= 1;
    }
}

fn lookup_slot_end<S, C>(
    slot_index: &mut AsyncCompactIndex<S>,
    cid_index: &mut AsyncCompactIndex<C>,
    slot: u64,
) -> Result<Option<u64>>
where
    S: RangeReader,
    C: RangeReader,
{
    let mut cid = vec![0u8; slot_index.value_size()];
    let found = futures::executor::block_on(
        slot_index.lookup_into_node_reads(&slot.to_le_bytes(), &mut cid),
    )?;
    if !found {
        return Ok(None);
    }

    let mut offset_and_size = vec![0u8; cid_index.value_size()];
    let found =
        futures::executor::block_on(cid_index.lookup_into_node_reads(&cid, &mut offset_and_size))?;
    if !found {
        return Ok(None);
    }
    let (offset, size) = decode_offset_and_size(&offset_and_size)?;
    let end = offset
        .checked_add(size as u64)
        .ok_or_else(|| anyhow!("slot {slot} end offset overflow"))?;
    Ok(Some(end))
}

#[derive(Clone)]
struct HttpRangeReader {
    client: Client,
    url: String,
}

impl HttpRangeReader {
    fn new(client: Client, url: String) -> Self {
        Self { client, url }
    }
}

impl RangeReader for HttpRangeReader {
    type ReadFuture<'a>
        = Ready<Result<()>>
    where
        Self: 'a;

    fn read_exact_at<'a>(&'a mut self, offset: u64, out: &'a mut [u8]) -> Self::ReadFuture<'a> {
        ready(
            http_range_get_len(&self.client, &self.url, offset, out.len())
                .and_then(|bytes| {
                    anyhow::ensure!(
                        bytes.len() == out.len(),
                        "{} range at {} returned {} bytes, expected {}",
                        self.url,
                        offset,
                        bytes.len(),
                        out.len()
                    );
                    out.copy_from_slice(&bytes);
                    Ok(())
                })
                .with_context(|| {
                    format!(
                        "range read {} bytes at {} from {}",
                        out.len(),
                        offset,
                        self.url
                    )
                }),
        )
    }
}

fn get_text(client: &Client, url: &str) -> Result<String> {
    let response = client.get(url).send()?;
    let status = response.status();
    anyhow::ensure!(status.is_success(), "{url} returned HTTP {status}");
    Ok(response.text()?)
}

fn http_range_get_len(client: &Client, url: &str, offset: u64, len: usize) -> Result<Vec<u8>> {
    let end = offset
        .checked_add(len as u64)
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(|| anyhow!("range overflow for {url}"))?;
    http_range_get(client, url, offset, end)
}

fn http_range_get(client: &Client, url: &str, start: u64, end: u64) -> Result<Vec<u8>> {
    let mut headers = HeaderMap::new();
    headers.insert(
        RANGE,
        HeaderValue::from_str(&format!("bytes={start}-{end}")).context("build Range header")?,
    );
    let mut response = client.get(url).headers(headers).send()?;
    let status = response.status();
    anyhow::ensure!(
        status.as_u16() == 206,
        "{url} range {start}-{end} returned HTTP {status}"
    );
    let mut bytes = Vec::new();
    response.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn fetch_range_to_writer<W: Write>(
    client: &Client,
    url: &str,
    start: u64,
    len: u64,
    writer: &mut W,
) -> Result<()> {
    let end = start
        .checked_add(len)
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(|| anyhow!("range overflow for {url}"))?;
    let mut headers = HeaderMap::new();
    headers.insert(
        RANGE,
        HeaderValue::from_str(&format!("bytes={start}-{end}")).context("build Range header")?,
    );
    let mut response = client.get(url).headers(headers).send()?;
    let status = response.status();
    anyhow::ensure!(
        status.as_u16() == 206,
        "{url} range {start}-{end} returned HTTP {status}"
    );
    std::io::copy(&mut response, writer)?;
    Ok(())
}
