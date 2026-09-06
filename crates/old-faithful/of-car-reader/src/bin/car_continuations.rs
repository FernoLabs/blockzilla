use std::{
    env,
    fs::File,
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    path::Path,
};

use of_car_reader::node::{CborArrayView, CborCidRef, DataFrame, Node, decode_node};

const CAR_BUF: usize = 128 << 20;
const CID_LEN: usize = 36;

fn main() -> io::Result<()> {
    let mut args = env::args().skip(1);
    let Some(input) = args.next() else {
        eprintln!("usage: car_continuations <input.car|input.car.zst> <output.tsv>");
        std::process::exit(1);
    };
    let Some(output) = args.next() else {
        eprintln!("usage: car_continuations <input.car|input.car.zst> <output.tsv>");
        std::process::exit(1);
    };
    if args.next().is_some() {
        eprintln!("usage: car_continuations <input.car|input.car.zst> <output.tsv>");
        std::process::exit(1);
    }

    let input_path = Path::new(&input);
    let out = File::create(&output)?;
    let mut out = BufWriter::with_capacity(8 << 20, out);
    writeln!(
        out,
        "entry_index\tcar_offset\tnode_kind\tframe_kind\tslot\tindex\tframe_hash\tframe_index\tframe_total\tdata_len\tnext_count\tnext_cids"
    )?;

    if input_path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("zst"))
    {
        let file = File::open(input_path)?;
        let file = BufReader::with_capacity(CAR_BUF, file);
        let zstd = zstd::Decoder::with_buffer(file)?;
        let reader = BufReader::with_capacity(CAR_BUF, zstd);
        scan_reader(reader, &mut out)?;
    } else {
        let file = File::open(input_path)?;
        let reader = BufReader::with_capacity(CAR_BUF, file);
        scan_reader(reader, &mut out)?;
    }

    out.flush()?;
    Ok(())
}

fn scan_reader<R: Read, W: Write>(mut reader: BufReader<R>, out: &mut W) -> io::Result<()> {
    let Some((header_len, header_varint_len)) = try_read_uvarint64(&mut reader)? else {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "empty car stream",
        ));
    };
    let header_len = header_len as usize;
    let mut header = vec![0u8; header_len];
    reader.read_exact(&mut header)?;

    let mut car_offset = (header_varint_len + header_len) as u64;
    let mut entry_index = 0u64;
    let mut cid = [0u8; CID_LEN];
    let mut payload = Vec::with_capacity(1 << 20);

    loop {
        let entry_offset = car_offset;
        let entry_len = match try_read_uvarint64(&mut reader)? {
            Some((value, consumed)) => {
                car_offset += consumed as u64;
                value as usize
            }
            None => break,
        };
        if entry_len < CID_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("entry {entry_index} length {entry_len} is smaller than cid size"),
            ));
        }

        reader.read_exact(&mut cid)?;
        car_offset += CID_LEN as u64;

        let payload_len = entry_len - CID_LEN;
        payload.resize(payload_len, 0);
        reader.read_exact(&mut payload)?;
        car_offset += payload_len as u64;

        let node = decode_node(&payload).map_err(invalid_data)?;
        match node {
            Node::Transaction(tx) => {
                log_dataframe(
                    out,
                    DataFrameLogContext {
                        entry_index,
                        entry_offset,
                        node_kind: "transaction",
                        frame_kind: "data",
                        slot: tx.slot,
                        index: tx.index,
                    },
                    &tx.data,
                )?;
                log_dataframe(
                    out,
                    DataFrameLogContext {
                        entry_index,
                        entry_offset,
                        node_kind: "transaction",
                        frame_kind: "metadata",
                        slot: tx.slot,
                        index: tx.index,
                    },
                    &tx.metadata,
                )?;
            }
            Node::Rewards(rewards) => {
                log_dataframe(
                    out,
                    DataFrameLogContext {
                        entry_index,
                        entry_offset,
                        node_kind: "rewards",
                        frame_kind: "data",
                        slot: rewards.slot,
                        index: None,
                    },
                    &rewards.data,
                )?;
            }
            Node::DataFrame(frame) => {
                log_dataframe(
                    out,
                    DataFrameLogContext {
                        entry_index,
                        entry_offset,
                        node_kind: "dataframe",
                        frame_kind: "standalone",
                        slot: 0,
                        index: None,
                    },
                    &frame,
                )?;
            }
            _ => {}
        }

        entry_index += 1;
    }

    Ok(())
}

struct DataFrameLogContext<'a> {
    entry_index: u64,
    entry_offset: u64,
    node_kind: &'a str,
    frame_kind: &'a str,
    slot: u64,
    index: Option<u64>,
}

fn log_dataframe<W: Write>(
    out: &mut W,
    ctx: DataFrameLogContext<'_>,
    frame: &DataFrame<'_>,
) -> io::Result<()> {
    let Some(next) = frame.next.as_ref() else {
        return Ok(());
    };

    let next_cids = cids_to_hex(next).map_err(invalid_data)?;
    let slot = if ctx.slot == 0 {
        String::new()
    } else {
        ctx.slot.to_string()
    };
    let index = ctx.index.map(|value| value.to_string()).unwrap_or_default();
    let frame_hash = frame
        .hash
        .map(|value| value.to_string())
        .unwrap_or_default();
    let frame_index = frame
        .index
        .map(|value| value.to_string())
        .unwrap_or_default();
    let frame_total = frame
        .total
        .map(|value| value.to_string())
        .unwrap_or_default();

    writeln!(
        out,
        "{}\t{}\t{}\t{}\t{slot}\t{index}\t{frame_hash}\t{frame_index}\t{frame_total}\t{}\t{}\t{}",
        ctx.entry_index,
        ctx.entry_offset,
        ctx.node_kind,
        ctx.frame_kind,
        frame.data.len(),
        next_cids.len(),
        next_cids.join(",")
    )
}

fn cids_to_hex(
    next: &CborArrayView<'_, CborCidRef<'_>>,
) -> Result<Vec<String>, minicbor::decode::Error> {
    next.iter()
        .map(|item| item.map(|cid| hex_encode(cid.bytes)))
        .collect()
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn try_read_uvarint64<R: BufRead>(reader: &mut R) -> io::Result<Option<(u64, usize)>> {
    const MAX_UVARINT_LEN_64: usize = 10;

    let mut value = 0u64;
    let mut shift = 0u32;
    let mut total = 0usize;
    let mut started = false;

    loop {
        if total >= MAX_UVARINT_LEN_64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "uvarint overflow",
            ));
        }

        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            if started {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected eof while reading uvarint",
                ));
            }
            return Ok(None);
        }

        let mut consumed = 0usize;
        for &byte in buf {
            started = true;
            total += 1;
            consumed += 1;

            if byte < 0x80 {
                if total == MAX_UVARINT_LEN_64 && byte > 1 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "uvarint overflow",
                    ));
                }
                value |= (byte as u64) << shift;
                reader.consume(consumed);
                return Ok(Some((value, total)));
            }

            value |= ((byte & 0x7f) as u64) << shift;
            shift += 7;
            if shift > 63 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "uvarint too long",
                ));
            }
        }

        reader.consume(consumed);
    }
}

fn invalid_data(err: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err.to_string())
}
