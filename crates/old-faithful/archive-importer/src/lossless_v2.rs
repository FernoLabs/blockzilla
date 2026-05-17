use anyhow::{Context, Result, ensure};
use blockzilla_format::{
    HistoricalArchiveRecord, HistoricalBlockRecord, HistoricalTransactionRecord, LosslessV2Header,
    PostcardFramedReader, PostcardFramedWriter, ReconstructionArchiveRecord,
    ReconstructionNodeKind, ReconstructionNodeRecord, RuntimeArchiveRecord, RuntimeBlockRecord,
    RuntimeTransactionRecord,
};
use of_car_reader::{
    CarBlockReader,
    reconstruct::{
        Cid36, RawEntryNode, RawEpochNode, RawNode, RawRewardsNode, RawSubsetNode,
        StandaloneDataFrame,
    },
};
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
};

use crate::BUFFER_SIZE;

const HISTORICAL_FILE: &str = "historical.bin";
const RUNTIME_FILE: &str = "runtime.bin";
const RECONSTRUCT_FILE: &str = "reconstruct.bin";

pub(crate) fn build(input: &Path, output_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;

    let historical_path = output_dir.join(HISTORICAL_FILE);
    let runtime_path = output_dir.join(RUNTIME_FILE);
    let reconstruct_path = output_dir.join(RECONSTRUCT_FILE);

    let input = open_car_input(input)?;
    let mut reader = CarBlockReader::with_capacity(input, BUFFER_SIZE);
    let encoded_car_header = reader.read_header_bytes()?;

    let historical_file = File::create(&historical_path)
        .with_context(|| format!("create {}", historical_path.display()))?;
    let runtime_file = File::create(&runtime_path)
        .with_context(|| format!("create {}", runtime_path.display()))?;
    let reconstruct_file = File::create(&reconstruct_path)
        .with_context(|| format!("create {}", reconstruct_path.display()))?;

    let historical_file = BufWriter::with_capacity(BUFFER_SIZE, historical_file);
    let runtime_file = BufWriter::with_capacity(BUFFER_SIZE, runtime_file);
    let reconstruct_file = BufWriter::with_capacity(BUFFER_SIZE, reconstruct_file);

    let mut historical = PostcardFramedWriter::new(historical_file);
    let mut runtime = PostcardFramedWriter::new(runtime_file);
    let mut reconstruct = PostcardFramedWriter::new(reconstruct_file);

    reconstruct.write(&ReconstructionArchiveRecord::Header(LosslessV2Header {
        encoded_car_header,
    }))?;

    let mut scratch = Vec::new();
    while let Some(node) = reader.read_lossless_node_with_scratch(&mut scratch)? {
        reconstruct.write(&ReconstructionArchiveRecord::Node(
            ReconstructionNodeRecord::from_raw(&node),
        ))?;

        match node {
            RawNode::Transaction(tx) => {
                historical.write(&HistoricalArchiveRecord::Transaction(
                    HistoricalTransactionRecord::from_raw(&tx),
                ))?;
                runtime.write(&RuntimeArchiveRecord::Transaction(
                    RuntimeTransactionRecord::from_raw(&tx),
                ))?;
            }
            RawNode::Entry(entry) => historical.write(&HistoricalArchiveRecord::Entry(entry))?,
            RawNode::Rewards(rewards) => runtime.write(&RuntimeArchiveRecord::Rewards(rewards))?,
            RawNode::Block(block) => {
                historical.write(&HistoricalArchiveRecord::Block(
                    HistoricalBlockRecord::from_raw(&block),
                ))?;
                runtime.write(&RuntimeArchiveRecord::Block(RuntimeBlockRecord::from_raw(
                    &block,
                )))?;
            }
            RawNode::Subset(subset) => {
                historical.write(&HistoricalArchiveRecord::Subset(subset))?
            }
            RawNode::Epoch(epoch) => historical.write(&HistoricalArchiveRecord::Epoch(epoch))?,
            RawNode::DataFrame(frame) => {
                historical.write(&HistoricalArchiveRecord::DataFrame(frame))?
            }
        }
    }

    historical.flush()?;
    runtime.flush()?;
    reconstruct.flush()?;
    Ok(())
}

pub(crate) fn extract(input_dir: &Path, output: &Path) -> Result<()> {
    let historical_path = input_dir.join(HISTORICAL_FILE);
    let runtime_path = input_dir.join(RUNTIME_FILE);
    let reconstruct_path = input_dir.join(RECONSTRUCT_FILE);

    let historical_file = File::open(&historical_path)
        .with_context(|| format!("open {}", historical_path.display()))?;
    let historical_file = BufReader::with_capacity(BUFFER_SIZE, historical_file);
    let mut historical = HistoricalStream::new(historical_file);

    let runtime_file =
        File::open(&runtime_path).with_context(|| format!("open {}", runtime_path.display()))?;
    let runtime_file = BufReader::with_capacity(BUFFER_SIZE, runtime_file);
    let mut runtime = RuntimeStream::new(runtime_file);

    let reconstruct_file = File::open(&reconstruct_path)
        .with_context(|| format!("open {}", reconstruct_path.display()))?;
    let reconstruct_file = BufReader::with_capacity(BUFFER_SIZE, reconstruct_file);
    let mut reconstruct = PostcardFramedReader::new(reconstruct_file);

    let output_file =
        File::create(output).with_context(|| format!("create {}", output.display()))?;
    let mut output_file = BufWriter::with_capacity(BUFFER_SIZE, output_file);

    let Some(ReconstructionArchiveRecord::Header(header)) =
        reconstruct.read::<ReconstructionArchiveRecord>()?
    else {
        anyhow::bail!(
            "reconstruction file {} did not start with a header record",
            reconstruct_path.display()
        );
    };
    output_file
        .write_all(&header.encoded_car_header)
        .with_context(|| format!("write CAR header to {}", output.display()))?;

    while let Some(record) = reconstruct.read::<ReconstructionArchiveRecord>()? {
        let ReconstructionArchiveRecord::Node(record) = record else {
            anyhow::bail!(
                "unexpected extra reconstruction header in {}",
                reconstruct_path.display()
            );
        };

        let node = match record.kind {
            ReconstructionNodeKind::Transaction => {
                let tx = historical.next_transaction()?;
                let meta = runtime.next_transaction()?;
                ensure_matching_location_and_cid(
                    "transaction",
                    &record,
                    tx.location,
                    tx.cid,
                    meta.location,
                    meta.cid,
                )?;
                RawNode::Transaction(tx.with_runtime(meta))
            }
            ReconstructionNodeKind::Entry => {
                let entry = historical.next_entry()?;
                ensure_matching_node("entry", &record, entry.location, entry.cid)?;
                RawNode::Entry(entry)
            }
            ReconstructionNodeKind::Rewards => {
                let rewards = runtime.next_rewards()?;
                ensure_matching_node("rewards", &record, rewards.location, rewards.cid)?;
                RawNode::Rewards(rewards)
            }
            ReconstructionNodeKind::Block => {
                let block = historical.next_block()?;
                let runtime_block = runtime.next_block()?;
                ensure_matching_location_and_cid(
                    "block",
                    &record,
                    block.location,
                    block.cid,
                    runtime_block.location,
                    runtime_block.cid,
                )?;
                RawNode::Block(block.with_runtime(runtime_block))
            }
            ReconstructionNodeKind::Subset => {
                let subset = historical.next_subset()?;
                ensure_matching_node("subset", &record, subset.location, subset.cid)?;
                RawNode::Subset(subset)
            }
            ReconstructionNodeKind::Epoch => {
                let epoch = historical.next_epoch()?;
                ensure_matching_node("epoch", &record, epoch.location, epoch.cid)?;
                RawNode::Epoch(epoch)
            }
            ReconstructionNodeKind::DataFrame => {
                let frame = historical.next_dataframe()?;
                ensure_matching_node("dataframe", &record, frame.location, frame.cid)?;
                RawNode::DataFrame(frame)
            }
        };

        node.validate_cid()?;
        let payload = node.encode_payload();
        write_car_entry(&mut output_file, node.cid(), &payload)?;
    }

    output_file.flush().context("flush reconstructed car")?;

    historical.ensure_empty()?;
    runtime.ensure_empty()?;
    Ok(())
}

fn open_car_input(path: &Path) -> Result<Box<dyn Read>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let file = BufReader::with_capacity(BUFFER_SIZE, file);

    if path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("zst"))
    {
        let decoder = zstd::Decoder::with_buffer(file)
            .with_context(|| format!("init zstd decoder for {}", path.display()))?;
        Ok(Box::new(decoder))
    } else {
        Ok(Box::new(file))
    }
}

fn write_car_entry<W: Write>(writer: &mut W, cid: Cid36, payload: &[u8]) -> Result<()> {
    write_uvarint(writer, (cid.car_bytes().len() + payload.len()) as u64)?;
    writer.write_all(cid.car_bytes())?;
    writer.write_all(payload)?;
    Ok(())
}

fn write_uvarint<W: Write>(writer: &mut W, mut value: u64) -> Result<()> {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        writer.write_all(&[byte])?;
        if value == 0 {
            return Ok(());
        }
    }
}

fn ensure_matching_node(
    kind: &str,
    record: &ReconstructionNodeRecord,
    location: of_car_reader::reconstruct::NodeLocation,
    cid: Cid36,
) -> Result<()> {
    ensure!(
        record.location == location,
        "{kind} location mismatch: reconstruct={:?} stored={:?}",
        record.location,
        location
    );
    ensure!(
        record.cid == cid,
        "{kind} cid mismatch: reconstruct={} stored={}",
        record.cid,
        cid
    );
    Ok(())
}

fn ensure_matching_location_and_cid(
    kind: &str,
    record: &ReconstructionNodeRecord,
    lhs_location: of_car_reader::reconstruct::NodeLocation,
    lhs_cid: Cid36,
    rhs_location: of_car_reader::reconstruct::NodeLocation,
    rhs_cid: Cid36,
) -> Result<()> {
    ensure_matching_node(kind, record, lhs_location, lhs_cid)?;
    ensure!(
        lhs_location == rhs_location,
        "{kind} split location mismatch: left={lhs_location:?} right={rhs_location:?}"
    );
    ensure!(
        lhs_cid == rhs_cid,
        "{kind} split cid mismatch: left={} right={}",
        lhs_cid,
        rhs_cid
    );
    Ok(())
}

struct HistoricalStream<R> {
    reader: PostcardFramedReader<R>,
}

impl<R: Read> HistoricalStream<R> {
    fn new(reader: R) -> Self {
        Self {
            reader: PostcardFramedReader::new(reader),
        }
    }

    fn next_transaction(&mut self) -> Result<HistoricalTransactionRecord> {
        match self.reader.read::<HistoricalArchiveRecord>()? {
            Some(HistoricalArchiveRecord::Transaction(tx)) => Ok(tx),
            Some(other) => anyhow::bail!(
                "expected historical transaction, found {}",
                historical_record_kind(&other)
            ),
            None => anyhow::bail!("unexpected EOF while reading historical transaction"),
        }
    }

    fn next_entry(&mut self) -> Result<RawEntryNode> {
        match self.reader.read::<HistoricalArchiveRecord>()? {
            Some(HistoricalArchiveRecord::Entry(entry)) => Ok(entry),
            Some(other) => anyhow::bail!(
                "expected historical entry, found {}",
                historical_record_kind(&other)
            ),
            None => anyhow::bail!("unexpected EOF while reading historical entry"),
        }
    }

    fn next_block(&mut self) -> Result<HistoricalBlockRecord> {
        match self.reader.read::<HistoricalArchiveRecord>()? {
            Some(HistoricalArchiveRecord::Block(block)) => Ok(block),
            Some(other) => anyhow::bail!(
                "expected historical block, found {}",
                historical_record_kind(&other)
            ),
            None => anyhow::bail!("unexpected EOF while reading historical block"),
        }
    }

    fn next_dataframe(&mut self) -> Result<StandaloneDataFrame> {
        match self.reader.read::<HistoricalArchiveRecord>()? {
            Some(HistoricalArchiveRecord::DataFrame(frame)) => Ok(frame),
            Some(other) => anyhow::bail!(
                "expected historical dataframe, found {}",
                historical_record_kind(&other)
            ),
            None => anyhow::bail!("unexpected EOF while reading historical dataframe"),
        }
    }

    fn next_subset(&mut self) -> Result<RawSubsetNode> {
        match self.reader.read::<HistoricalArchiveRecord>()? {
            Some(HistoricalArchiveRecord::Subset(subset)) => Ok(subset),
            Some(other) => anyhow::bail!(
                "expected historical subset, found {}",
                historical_record_kind(&other)
            ),
            None => anyhow::bail!("unexpected EOF while reading historical subset"),
        }
    }

    fn next_epoch(&mut self) -> Result<RawEpochNode> {
        match self.reader.read::<HistoricalArchiveRecord>()? {
            Some(HistoricalArchiveRecord::Epoch(epoch)) => Ok(epoch),
            Some(other) => anyhow::bail!(
                "expected historical epoch, found {}",
                historical_record_kind(&other)
            ),
            None => anyhow::bail!("unexpected EOF while reading historical epoch"),
        }
    }

    fn ensure_empty(&mut self) -> Result<()> {
        if let Some(record) = self.reader.read::<HistoricalArchiveRecord>()? {
            anyhow::bail!(
                "unused historical record remained after extraction: {}",
                historical_record_kind(&record)
            );
        }
        Ok(())
    }
}

struct RuntimeStream<R> {
    reader: PostcardFramedReader<R>,
}

impl<R: Read> RuntimeStream<R> {
    fn new(reader: R) -> Self {
        Self {
            reader: PostcardFramedReader::new(reader),
        }
    }

    fn next_transaction(&mut self) -> Result<RuntimeTransactionRecord> {
        match self.reader.read::<RuntimeArchiveRecord>()? {
            Some(RuntimeArchiveRecord::Transaction(tx)) => Ok(tx),
            Some(other) => anyhow::bail!(
                "expected runtime transaction, found {}",
                runtime_record_kind(&other)
            ),
            None => anyhow::bail!("unexpected EOF while reading runtime transaction"),
        }
    }

    fn next_rewards(&mut self) -> Result<RawRewardsNode> {
        match self.reader.read::<RuntimeArchiveRecord>()? {
            Some(RuntimeArchiveRecord::Rewards(rewards)) => Ok(rewards),
            Some(other) => anyhow::bail!(
                "expected runtime rewards, found {}",
                runtime_record_kind(&other)
            ),
            None => anyhow::bail!("unexpected EOF while reading runtime rewards"),
        }
    }

    fn next_block(&mut self) -> Result<RuntimeBlockRecord> {
        match self.reader.read::<RuntimeArchiveRecord>()? {
            Some(RuntimeArchiveRecord::Block(block)) => Ok(block),
            Some(other) => anyhow::bail!(
                "expected runtime block, found {}",
                runtime_record_kind(&other)
            ),
            None => anyhow::bail!("unexpected EOF while reading runtime block"),
        }
    }

    fn ensure_empty(&mut self) -> Result<()> {
        if let Some(record) = self.reader.read::<RuntimeArchiveRecord>()? {
            anyhow::bail!(
                "unused runtime record remained after extraction: {}",
                runtime_record_kind(&record)
            );
        }
        Ok(())
    }
}

fn historical_record_kind(record: &HistoricalArchiveRecord) -> &'static str {
    match record {
        HistoricalArchiveRecord::Transaction(_) => "transaction",
        HistoricalArchiveRecord::Entry(_) => "entry",
        HistoricalArchiveRecord::Block(_) => "block",
        HistoricalArchiveRecord::DataFrame(_) => "dataframe",
        HistoricalArchiveRecord::Subset(_) => "subset",
        HistoricalArchiveRecord::Epoch(_) => "epoch",
    }
}

fn runtime_record_kind(record: &RuntimeArchiveRecord) -> &'static str {
    match record {
        RuntimeArchiveRecord::Transaction(_) => "transaction",
        RuntimeArchiveRecord::Rewards(_) => "rewards",
        RuntimeArchiveRecord::Block(_) => "block",
    }
}

#[cfg(test)]
mod tests {
    use super::{build, extract};
    use blockzilla_format::ReconstructionArchiveRecord;
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn round_trips_synthetic_car_through_lossless_v2() {
        let tmp = unique_temp_dir();
        fs::create_dir_all(&tmp).expect("create temp dir");

        let input = tmp.join("input.car");
        let bundle = tmp.join("bundle");
        let output = tmp.join("output.car");
        let original = build_synthetic_car();
        fs::write(&input, &original).expect("write input car");

        build(&input, &bundle).expect("build bundle");
        extract(&bundle, &output).expect("extract bundle");

        let rebuilt = fs::read(&output).expect("read rebuilt car");
        assert_eq!(original, rebuilt);

        let reconstruct = bundle.join(super::RECONSTRUCT_FILE);
        let reconstruct = fs::File::open(reconstruct).expect("open reconstruct");
        let reconstruct = std::io::BufReader::new(reconstruct);
        let mut reconstruct = blockzilla_format::PostcardFramedReader::new(reconstruct);
        let first = reconstruct
            .read::<ReconstructionArchiveRecord>()
            .expect("read reconstruct record")
            .expect("reconstruct header");
        assert!(matches!(first, ReconstructionArchiveRecord::Header(_)));
    }

    fn unique_temp_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "blockzilla-lossless-v2-{}-{}",
            std::process::id(),
            nanos
        ))
    }

    fn build_synthetic_car() -> Vec<u8> {
        use of_car_reader::confirmed_block;
        use of_car_reader::reconstruct::Cid36;
        use prost::Message;

        let tx_payload = encode_transaction_node(42, Some(0), &minimal_legacy_transaction());
        let tx_cid = Cid36::compute(&tx_payload);

        let rewards_bytes = confirmed_block::Rewards {
            rewards: Vec::new(),
            num_partitions: None,
        }
        .encode_to_vec();
        let rewards_payload = encode_rewards_node(42, &rewards_bytes);
        let rewards_cid = Cid36::compute(&rewards_payload);

        let entry_ref = cid_ref(tx_cid);
        let entry_payload = encode_entry_node(1, [0x44; 32], &[entry_ref]);
        let entry_cid = Cid36::compute(&entry_payload);

        let block_payload = encode_block_node(42, entry_cid, rewards_cid);
        let block_cid = Cid36::compute(&block_payload);

        let subset_payload = encode_subset_node(42, 42, &[cid_ref(block_cid)]);
        let subset_cid = Cid36::compute(&subset_payload);

        let epoch_payload = encode_epoch_node(0, &[cid_ref(subset_cid)]);

        let mut out = Vec::new();
        out.push(0);
        push_car_entry(&mut out, tx_cid, &tx_payload);
        push_car_entry(&mut out, entry_cid, &entry_payload);
        push_car_entry(&mut out, rewards_cid, &rewards_payload);
        push_car_entry(&mut out, block_cid, &block_payload);
        push_car_entry(&mut out, subset_cid, &subset_payload);
        push_car_entry(&mut out, Cid36::compute(&epoch_payload), &epoch_payload);
        out
    }

    fn push_car_entry(out: &mut Vec<u8>, cid: of_car_reader::reconstruct::Cid36, payload: &[u8]) {
        push_uvarint(out, (cid.car_bytes().len() + payload.len()) as u64);
        out.extend_from_slice(cid.car_bytes());
        out.extend_from_slice(payload);
    }

    fn push_uvarint(out: &mut Vec<u8>, mut value: u64) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if value == 0 {
                return;
            }
        }
    }

    fn cid_ref(cid: of_car_reader::reconstruct::Cid36) -> of_car_reader::reconstruct::RawCidRef {
        let cbor_bytes = cid.cbor_bytes();
        of_car_reader::reconstruct::RawCidRef {
            cid: Some(cid),
            normalized_bytes: cid.car_bytes().to_vec(),
            cbor_bytes: cbor_bytes.to_vec(),
            tagged: true,
        }
    }

    fn encode_dataframe(data: &[u8]) -> Vec<u8> {
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(5).expect("vec encoder is infallible");
        e.u64(6).expect("vec encoder is infallible");
        e.null().expect("vec encoder is infallible");
        e.null().expect("vec encoder is infallible");
        e.null().expect("vec encoder is infallible");
        e.bytes(data).expect("vec encoder is infallible");
        e.into_writer()
    }

    fn encode_transaction_node(slot: u64, index: Option<u64>, tx_bytes: &[u8]) -> Vec<u8> {
        let data_frame = encode_dataframe(tx_bytes);
        let meta_frame = encode_dataframe(&[]);
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(5).expect("vec encoder is infallible");
        e.u64(0).expect("vec encoder is infallible");
        e.writer_mut().extend_from_slice(&data_frame);
        e.writer_mut().extend_from_slice(&meta_frame);
        e.u64(slot).expect("vec encoder is infallible");
        if let Some(index) = index {
            e.u64(index).expect("vec encoder is infallible");
        } else {
            e.null().expect("vec encoder is infallible");
        }
        e.into_writer()
    }

    fn encode_rewards_node(slot: u64, rewards_bytes: &[u8]) -> Vec<u8> {
        let frame = encode_dataframe(rewards_bytes);
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(3).expect("vec encoder is infallible");
        e.u64(5).expect("vec encoder is infallible");
        e.u64(slot).expect("vec encoder is infallible");
        e.writer_mut().extend_from_slice(&frame);
        e.into_writer()
    }

    fn encode_entry_node(
        num_hashes: u64,
        hash: [u8; 32],
        txs: &[of_car_reader::reconstruct::RawCidRef],
    ) -> Vec<u8> {
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(4).expect("vec encoder is infallible");
        e.u64(1).expect("vec encoder is infallible");
        e.u64(num_hashes).expect("vec encoder is infallible");
        e.bytes(&hash).expect("vec encoder is infallible");
        e.array(txs.len() as u64)
            .expect("vec encoder is infallible");
        for tx in txs {
            encode_cid_ref(&mut e, tx);
        }
        e.into_writer()
    }

    fn encode_block_node(
        slot: u64,
        entry_cid: of_car_reader::reconstruct::Cid36,
        rewards_cid: of_car_reader::reconstruct::Cid36,
    ) -> Vec<u8> {
        let entry_ref = cid_ref(entry_cid);
        let rewards_ref = cid_ref(rewards_cid);
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(6).expect("vec encoder is infallible");
        e.u64(2).expect("vec encoder is infallible");
        e.u64(slot).expect("vec encoder is infallible");
        e.array(1).expect("vec encoder is infallible");
        e.array(2).expect("vec encoder is infallible");
        e.i64(1).expect("vec encoder is infallible");
        e.i64(2).expect("vec encoder is infallible");
        e.array(1).expect("vec encoder is infallible");
        encode_cid_ref(&mut e, &entry_ref);
        e.array(1).expect("vec encoder is infallible");
        e.u64(slot - 1).expect("vec encoder is infallible");
        encode_cid_ref(&mut e, &rewards_ref);
        e.into_writer()
    }

    fn encode_subset_node(
        first: u64,
        last: u64,
        blocks: &[of_car_reader::reconstruct::RawCidRef],
    ) -> Vec<u8> {
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(4).expect("vec encoder is infallible");
        e.u64(3).expect("vec encoder is infallible");
        e.u64(first).expect("vec encoder is infallible");
        e.u64(last).expect("vec encoder is infallible");
        e.array(blocks.len() as u64)
            .expect("vec encoder is infallible");
        for block in blocks {
            encode_cid_ref(&mut e, block);
        }
        e.into_writer()
    }

    fn encode_epoch_node(epoch: u64, subsets: &[of_car_reader::reconstruct::RawCidRef]) -> Vec<u8> {
        let mut e = minicbor::Encoder::new(Vec::new());
        e.array(3).expect("vec encoder is infallible");
        e.u64(4).expect("vec encoder is infallible");
        e.u64(epoch).expect("vec encoder is infallible");
        e.array(subsets.len() as u64)
            .expect("vec encoder is infallible");
        for subset in subsets {
            encode_cid_ref(&mut e, subset);
        }
        e.into_writer()
    }

    fn encode_cid_ref(
        e: &mut minicbor::Encoder<Vec<u8>>,
        cid: &of_car_reader::reconstruct::RawCidRef,
    ) {
        if cid.tagged {
            e.tag(minicbor::data::Tag::new(42))
                .expect("vec encoder is infallible");
        }
        e.bytes(&cid.cbor_bytes).expect("vec encoder is infallible");
    }

    fn minimal_legacy_transaction() -> Vec<u8> {
        let mut out = Vec::with_capacity(38);
        out.push(0);
        out.extend_from_slice(&[0, 0, 0]);
        out.push(0);
        out.extend_from_slice(&[0; 32]);
        out.push(0);
        out
    }
}
