//! Local test journal. FrameV1 provides the prefix chain; the outer file is
//! explicitly a hook capture, not a registered or deletion-authorizing stream.

use crate::wire;
use anyhow::{Context, Result, ensure};
use hivezilla_protocol::{CursorV1, FrameV1, PrefixHash};
use hivezilla_shred_hook_abi::{END_KIND, IPC_HEADER_BYTES, MAX_PACKET_BYTES};
use serde::Serialize;
use std::{
    fs::{self, File, OpenOptions},
    io::{BufWriter, Read, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

pub const WAL_MAGIC: &[u8; 8] = b"HZSHWAL1";
pub const WAL_END: &[u8; 8] = b"HZSHEND1";

#[derive(Default, Clone, Serialize)]
pub struct Counts {
    pub packets: u64,
    pub payload_bytes: u64,
    pub turbine_received: u64,
    pub repair_received: u64,
    pub repair_accepted: u64,
    pub data_shreds: u64,
    pub coding_shreds: u64,
    pub other_datagrams: u64,
    pub first_header_slot: Option<u64>,
    pub max_header_slot: Option<u64>,
}

#[derive(Serialize)]
pub struct Status<'a> {
    pub schema: &'static str,
    pub state: &'a str,
    pub reason: &'a str,
    pub updated_unix_ms: u128,
    pub durable_next_sequence: u64,
    pub durable_prefix_hash: String,
    pub journal_bytes: u64,
    pub counts: &'a Counts,
    pub custody_authority: bool,
    pub canonical_coverage_verified: bool,
}

pub struct Writer {
    root: PathBuf,
    file: BufWriter<File>,
    active: PathBuf,
    segment: u64,
    segment_bytes: u64,
    total_bytes: u64,
    max_bytes: u64,
    segment_target: u64,
    capture_id: [u8; 16],
    cursor: CursorV1,
    expected_packet_sequence: u64,
    pub counts: Counts,
    pub terminal_reason: Option<u32>,
}

fn private_file(path: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?)
}

pub fn atomic_json(path: &Path, data: &impl Serialize) -> Result<()> {
    let mut nonce = [0u8; 16];
    File::open("/dev/urandom")?.read_exact(&mut nonce)?;
    let temp = path.with_extension(format!("tmp.{}", hex(&nonce)));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temp)?;
    serde_json::to_writer(&mut file, data)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(temp, path)?;
    File::open(path.parent().context("JSON has no parent")?)?.sync_all()?;
    Ok(())
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

impl Writer {
    pub fn new(
        root: PathBuf,
        capture_id: [u8; 16],
        segment_target: u64,
        max_bytes: u64,
    ) -> Result<Self> {
        ensure!(
            segment_target >= 4096 && segment_target <= max_bytes,
            "invalid journal size limits"
        );
        fs::create_dir(&root)?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        let cursor = CursorV1::new(0, PrefixHash::new([0; 32]));
        let active = root.join("0000000000000000.active");
        let mut file = BufWriter::with_capacity(1024 * 1024, private_file(&active)?);
        file.write_all(WAL_MAGIC)?;
        file.write_all(&capture_id)?;
        file.write_all(&cursor.fixed_encode())?;
        let header_bytes = 24 + cursor.fixed_encode().len() as u64;
        file.flush()?;
        file.get_ref().sync_all()?;
        File::open(&root)?.sync_all()?;
        Ok(Self {
            root,
            file,
            active,
            segment: 0,
            segment_bytes: header_bytes,
            total_bytes: header_bytes,
            max_bytes,
            segment_target,
            capture_id,
            cursor,
            expected_packet_sequence: 0,
            counts: Counts::default(),
            terminal_reason: None,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn bytes(&self) -> u64 {
        self.total_bytes
    }

    pub fn append(&mut self, ipc: Vec<u8>) -> Result<()> {
        ensure!(
            self.terminal_reason.is_none(),
            "packet after terminal frame"
        );
        ensure!(
            wire::sequence(&ipc) == self.expected_packet_sequence,
            "hook sequence gap"
        );
        let kind = ipc[8];
        let frame = FrameV1::new(self.cursor, ipc.clone())?;
        let encoded = frame.encode();
        let footer_bytes = 48u64;
        let rotate = self.segment_bytes + encoded.len() as u64 + footer_bytes > self.segment_target;
        let additional = encoded.len() as u64 + if rotate { footer_bytes + 64 } else { 0 };
        ensure!(
            self.total_bytes + additional + footer_bytes <= self.max_bytes,
            "hook journal quota reached"
        );
        if rotate {
            self.rotate()?;
        }
        self.file.write_all(&encoded)?;
        self.segment_bytes += encoded.len() as u64;
        self.total_bytes += encoded.len() as u64;
        self.cursor = self.cursor.advance(&ipc)?;
        if kind == END_KIND {
            self.terminal_reason = Some(u32::from_be_bytes(ipc[IPC_HEADER_BYTES..].try_into()?));
        } else {
            self.expected_packet_sequence = self
                .expected_packet_sequence
                .checked_add(1)
                .context("packet sequence overflow")?;
            self.counts.packets += 1;
            self.counts.payload_bytes += (ipc.len() - IPC_HEADER_BYTES) as u64;
            match kind {
                0 => self.counts.turbine_received += 1,
                1 => self.counts.repair_received += 1,
                2 => self.counts.repair_accepted += 1,
                _ => unreachable!(),
            }
            let payload = &ipc[IPC_HEADER_BYTES..];
            let variant = payload.get(64).copied().unwrap_or(0) & 0xf0;
            // Pinned Agave 4.3.0 sizes. Other TVU messages can coincidentally
            // share a variant nibble; do not use their arbitrary bytes as slots.
            // These are header observations, not signature-verified shreds.
            let nonce_bytes = if kind == 0 { 0 } else { 4 };
            let shape_matches = match variant {
                0x60 | 0x70 if payload.len() == 1228 + nonce_bytes => {
                    self.counts.coding_shreds += 1;
                    true
                }
                0x90 | 0xb0 if payload.len() == 1203 + nonce_bytes => {
                    self.counts.data_shreds += 1;
                    true
                }
                _ => {
                    self.counts.other_datagrams += 1;
                    false
                }
            };
            if shape_matches {
                let slot = u64::from_le_bytes(payload[65..73].try_into()?);
                self.counts.first_header_slot.get_or_insert(slot);
                self.counts.max_header_slot =
                    Some(self.counts.max_header_slot.unwrap_or(slot).max(slot));
            }
        }
        Ok(())
    }

    fn seal(&mut self) -> Result<()> {
        self.file.write_all(WAL_END)?;
        self.file.write_all(&self.cursor.fixed_encode())?;
        self.segment_bytes += 48;
        self.total_bytes += 48;
        self.file.flush()?;
        self.file.get_ref().sync_all()?;
        fs::rename(&self.active, self.active.with_extension("wal"))?;
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }

    fn rotate(&mut self) -> Result<()> {
        self.seal()?;
        self.segment += 1;
        self.active = self.root.join(format!("{:016x}.active", self.segment));
        self.file = BufWriter::with_capacity(1024 * 1024, private_file(&self.active)?);
        self.file.write_all(WAL_MAGIC)?;
        self.file.write_all(&self.capture_id)?;
        self.file.write_all(&self.cursor.fixed_encode())?;
        self.segment_bytes = 64;
        self.total_bytes += 64;
        self.file.flush()?;
        self.file.get_ref().sync_all()?;
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }

    pub fn commit(&mut self, state: &str, reason: &str) -> Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_data()?;
        self.status(state, reason)
    }

    pub fn finish(&mut self, state: &str, reason: &str) -> Result<()> {
        self.seal()?;
        self.status(state, reason)
    }

    fn status(&self, state: &str, reason: &str) -> Result<()> {
        atomic_json(
            &self.root.join("STATUS.json"),
            &Status {
                schema: "hivezilla-shred-hook-status-v1",
                state,
                reason,
                updated_unix_ms: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
                durable_next_sequence: self.cursor.next_sequence(),
                durable_prefix_hash: hex(self.cursor.prefix_hash().as_bytes()),
                journal_bytes: self.total_bytes,
                counts: &self.counts,
                custody_authority: false,
                canonical_coverage_verified: false,
            },
        )
    }
}

/// Test/operations reader for one sealed local segment. No archive or custody authority.
pub fn read_segment(path: &Path, mut visit: impl FnMut(&[u8]) -> Result<()>) -> Result<u64> {
    let mut reader = std::io::BufReader::new(File::open(path)?);
    let mut header = [0u8; 64];
    reader.read_exact(&mut header)?;
    ensure!(&header[..8] == WAL_MAGIC, "invalid hook WAL header");
    let mut cursor = CursorV1::decode(&header[24..])?;
    let mut count = 0;
    loop {
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if magic == WAL_END[..4] {
            let mut trailer = [0u8; 44];
            reader.read_exact(&mut trailer)?;
            ensure!(trailer[..4] == WAL_END[4..], "invalid hook WAL footer");
            ensure!(
                CursorV1::decode(&trailer[4..])? == cursor,
                "hook WAL footer cursor differs"
            );
            let mut end = [0u8; 1];
            ensure!(reader.read(&mut end)? == 0, "trailing hook WAL bytes");
            break;
        }
        let mut length = [0u8; 8];
        reader.read_exact(&mut length)?;
        let length = u64::from_be_bytes(length);
        ensure!(
            length <= (IPC_HEADER_BYTES + MAX_PACKET_BYTES) as u64,
            "oversized hook WAL frame"
        );
        let mut encoded = Vec::with_capacity(44 + length as usize);
        encoded.extend_from_slice(&magic);
        encoded.extend_from_slice(&length.to_be_bytes());
        encoded.resize(44 + length as usize, 0);
        reader.read_exact(&mut encoded[12..])?;
        let frame = FrameV1::decode_after(&encoded, cursor)?;
        let mut payload = frame.payload();
        ensure!(
            wire::read_frame(&mut payload)?.is_some() && payload.is_empty(),
            "invalid hook IPC payload"
        );
        visit(frame.payload())?;
        cursor = cursor.advance(frame.payload())?;
        count += 1;
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hivezilla_shred_hook_abi::*;
    #[test]
    fn rotates_preserves_duplicates_and_detects_prefix_corruption() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("capture");
        let payload = [7u8; MAX_PACKET_BYTES];
        let packet = PacketV1 {
            struct_bytes: size_of::<PacketV1>() as u32,
            kind: 0,
            address_family: 4,
            local_port: 8000,
            remote_port: 8001,
            reserved: 0,
            remote_address: [0; 16],
            observed_unix_ns: 1,
            payload: payload.as_ptr(),
            payload_bytes: payload.len(),
        };
        let mut writer = Writer::new(root.clone(), [1; 16], 4096, 16384).unwrap();
        for seq in 0..6u64 {
            let mut frame = wire::packet_frame(&packet, &payload);
            frame[16..24].copy_from_slice(&seq.to_be_bytes());
            writer.append(frame).unwrap();
        }
        writer.append(wire::end_frame(6, END_CLEAN)).unwrap();
        writer.finish("CLOSED", "clean").unwrap();
        let mut files: Vec<_> = fs::read_dir(&root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "wal"))
            .collect();
        files.sort();
        assert!(files.len() > 1);
        let mut received = 0;
        for file in &files {
            read_segment(file, |frame| {
                if frame[8] != END_KIND {
                    assert_eq!(&frame[IPC_HEADER_BYTES..], &payload);
                    received += 1;
                }
                Ok(())
            })
            .unwrap();
        }
        assert_eq!(received, 6);
        let mut bytes = fs::read(&files[0]).unwrap();
        bytes[100] ^= 1;
        fs::write(&files[0], bytes).unwrap();
        assert!(read_segment(&files[0], |_| Ok(())).is_err());
        assert!(Writer::new(root, [1; 16], 4096, 16384).is_err());
    }
    #[test]
    fn sequence_gap_and_quota_fail_before_append() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("capture");
        let mut writer = Writer::new(root.clone(), [1; 16], 4096, 4096).unwrap();
        assert!(writer.append(wire::end_frame(1, END_CLEAN)).is_err());
        assert_eq!(writer.bytes(), 64);
        let payload = [7u8; MAX_PACKET_BYTES];
        let packet = PacketV1 {
            struct_bytes: size_of::<PacketV1>() as u32,
            kind: 0,
            address_family: 4,
            local_port: 8000,
            remote_port: 8001,
            reserved: 0,
            remote_address: [0; 16],
            observed_unix_ns: 1,
            payload: payload.as_ptr(),
            payload_bytes: payload.len(),
        };
        for seq in 0..3u64 {
            let mut frame = wire::packet_frame(&packet, &payload);
            frame[16..24].copy_from_slice(&seq.to_be_bytes());
            writer.append(frame).unwrap();
        }
        let before = writer.bytes();
        let mut frame = wire::packet_frame(&packet, &payload);
        frame[16..24].copy_from_slice(&3u64.to_be_bytes());
        assert!(writer.append(frame).is_err());
        assert_eq!(writer.bytes(), before);
    }
}
