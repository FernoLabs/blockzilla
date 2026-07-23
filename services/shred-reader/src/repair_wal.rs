//! Dedicated append-only WAL for accepted repair shreds.
//!
//! This format is intentionally unrelated to the raw-shred WAL and replication ACK state. A
//! writer will only open a path ending in `.repair.wal`, takes an exclusive file lock, enforces a
//! hard byte cap, and refuses to append to a non-empty file unless it starts with this module's
//! file header. A configuration mistake therefore fails closed instead of corrupting an ingest or
//! ACK file.

use std::{
    fs::{File, OpenOptions, TryLockError},
    io::{self, ErrorKind, Read, Seek, SeekFrom, Write},
    num::NonZeroU64,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use solana_hash::Hash;
use solana_keypair::Signature;
use solana_pubkey::Pubkey;

use crate::repair_wire::{RepairNonce, ShredRepairRequest};

const FILE_HEADER: &[u8; 16] = b"SHRED-REPAIR\0\0\x02\0";
const FRAME_PREFIX_BYTES: usize = 4;
const FRAME_CHECKSUM_BYTES: usize = 4;
const MIN_BODY_BYTES: usize = 8;
const MAX_FRAME_BODY_BYTES: usize = 16 * 1024;
const MAX_FRAME_BYTES: usize = FRAME_PREFIX_BYTES + MAX_FRAME_BODY_BYTES + FRAME_CHECKSUM_BYTES;
pub const MIN_REPAIR_WAL_FILE_BYTES: u64 = (FILE_HEADER.len() + MAX_FRAME_BYTES) as u64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepairWalFsyncPolicy {
    EveryRecord,
    Batch {
        max_unsynced_records: NonZeroU64,
        max_unsynced_age: Duration,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairWalConfig {
    pub path: PathBuf,
    pub fsync: RepairWalFsyncPolicy,
    /// Hard cap for this repair-only file. Append fails before writing any frame that would cross
    /// the cap; the caller should disable repair without disturbing raw capture.
    pub max_file_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairProvenance {
    pub received_at_unix_ms: u64,
    pub nonce: RepairNonce,
    pub request: ShredRepairRequest,
    pub peer_addr: String,
    pub peer_pubkey: Pubkey,
    pub shred_slot: u64,
    pub shred_index: u32,
    pub fec_set_index: u32,
    pub shred_version: u16,
    pub expected_slot_leader: Pubkey,
    pub fec_merkle_root: Hash,
    pub trust_anchor_fec_set_index: u32,
    pub learned_chained_merkle_root: bool,
    pub chained_merkle_root: Option<Hash>,
    pub leader_signature: Signature,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RepairWalAppend {
    pub sequence: u64,
    pub frame_bytes: usize,
    pub synced: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairWalEntry {
    pub sequence: u64,
    pub provenance: RepairProvenance,
    pub shred_payload: Vec<u8>,
}

#[derive(Debug)]
pub struct RepairWal {
    file: File,
    path: PathBuf,
    fsync: RepairWalFsyncPolicy,
    max_file_bytes: u64,
    next_sequence: u64,
    file_len: u64,
    unsynced_records: u64,
    last_sync_at: Instant,
}

impl RepairWal {
    pub fn open(config: RepairWalConfig, now: Instant) -> io::Result<Self> {
        validate_repair_wal_path(&config.path)?;
        if config.max_file_bytes < MIN_REPAIR_WAL_FILE_BYTES {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!("repair WAL max_file_bytes must be at least {MIN_REPAIR_WAL_FILE_BYTES}"),
            ));
        }
        if config.path.exists() && config.path.symlink_metadata()?.file_type().is_symlink() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "repair WAL must not be a symbolic link: {}",
                    config.path.display()
                ),
            ));
        }
        if let Some(parent) = config.path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&config.path)?;
        file.try_lock().map_err(|error| match error {
            TryLockError::Error(error) => io::Error::new(
                error.kind(),
                format!(
                    "cannot lock repair WAL ({}): {error}",
                    config.path.display()
                ),
            ),
            TryLockError::WouldBlock => io::Error::new(
                ErrorKind::WouldBlock,
                format!(
                    "repair WAL is already open by another writer ({})",
                    config.path.display()
                ),
            ),
        })?;

        let len = file.metadata()?.len();
        if len > config.max_file_bytes {
            return Err(io::Error::new(
                ErrorKind::StorageFull,
                format!(
                    "repair WAL is already {len} bytes, above its {}-byte cap",
                    config.max_file_bytes
                ),
            ));
        }
        let (valid_len, next_sequence) = if len == 0 {
            file.write_all(FILE_HEADER)?;
            file.sync_data()?;
            (FILE_HEADER.len() as u64, 0)
        } else {
            validate_header(&mut file, &config.path)?;
            scan_valid_frames(&mut file, len)?
        };
        // A crash during an unsynced append may leave only the final frame truncated. It is safe to
        // remove that tail because this is the isolated repair WAL and every accepted frame before
        // it passed its checksum. Interior corruption is rejected by scan_valid_frames.
        if valid_len < len {
            file.set_len(valid_len)?;
            file.sync_data()?;
        }
        file.seek(SeekFrom::End(0))?;

        Ok(Self {
            file,
            path: config.path,
            fsync: config.fsync,
            max_file_bytes: config.max_file_bytes,
            next_sequence,
            file_len: valid_len,
            unsynced_records: 0,
            last_sync_at: now,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    pub fn file_len(&self) -> u64 {
        self.file_len
    }

    pub fn max_file_bytes(&self) -> u64 {
        self.max_file_bytes
    }

    pub fn remaining_bytes(&self) -> u64 {
        self.max_file_bytes.saturating_sub(self.file_len)
    }

    pub fn append(
        &mut self,
        provenance: &RepairProvenance,
        shred_payload: &[u8],
        now: Instant,
    ) -> io::Result<RepairWalAppend> {
        let sequence = self.next_sequence;
        let body = encode_body(sequence, provenance, shred_payload)?;
        if body.len() > MAX_FRAME_BODY_BYTES {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "repair WAL frame body is {} bytes; maximum is {MAX_FRAME_BODY_BYTES}",
                    body.len()
                ),
            ));
        }
        let body_len = u32::try_from(body.len()).map_err(|_| {
            io::Error::new(ErrorKind::InvalidData, "repair WAL frame length overflow")
        })?;
        let checksum = crc32(&body);
        let frame_bytes = FRAME_PREFIX_BYTES + body.len() + FRAME_CHECKSUM_BYTES;
        let next_file_len = self
            .file_len
            .checked_add(frame_bytes as u64)
            .ok_or_else(|| io::Error::new(ErrorKind::StorageFull, "repair WAL size overflow"))?;
        if next_file_len > self.max_file_bytes {
            return Err(io::Error::new(
                ErrorKind::StorageFull,
                format!(
                    "repair WAL cap reached: {} + {frame_bytes} would exceed {} bytes",
                    self.file_len, self.max_file_bytes
                ),
            ));
        }

        self.file.write_all(&body_len.to_le_bytes())?;
        self.file.write_all(&body)?;
        self.file.write_all(&checksum.to_le_bytes())?;
        self.file_len = next_file_len;
        self.next_sequence = self.next_sequence.checked_add(1).ok_or_else(|| {
            io::Error::new(ErrorKind::InvalidData, "repair WAL sequence exhausted")
        })?;
        self.unsynced_records = self.unsynced_records.saturating_add(1);

        let synced = self.sync_due(now)?;
        Ok(RepairWalAppend {
            sequence,
            frame_bytes,
            synced,
        })
    }

    pub fn flush_and_sync(&mut self, now: Instant) -> io::Result<()> {
        self.file.flush()?;
        self.file.sync_data()?;
        self.unsynced_records = 0;
        self.last_sync_at = now;
        Ok(())
    }

    /// Enforces the time side of a batch fsync policy even when no newer record arrives.
    pub fn sync_if_due(&mut self, now: Instant) -> io::Result<bool> {
        if self.unsynced_records == 0 {
            return Ok(false);
        }
        self.sync_due(now)
    }

    pub fn read_all(path: &Path) -> io::Result<Vec<RepairWalEntry>> {
        validate_repair_wal_path(path)?;
        let mut file = File::open(path)?;
        validate_header(&mut file, path)?;
        let mut entries = Vec::new();
        while let Some(body) = read_frame(&mut file, false)? {
            let entry = decode_body(&body)?;
            if entry.sequence != entries.len() as u64 {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "repair WAL sequence discontinuity: expected {}, got {}",
                        entries.len(),
                        entry.sequence
                    ),
                ));
            }
            entries.push(entry);
        }
        Ok(entries)
    }

    fn sync_due(&mut self, now: Instant) -> io::Result<bool> {
        let due = match self.fsync {
            RepairWalFsyncPolicy::EveryRecord => true,
            RepairWalFsyncPolicy::Batch {
                max_unsynced_records,
                max_unsynced_age,
            } => {
                self.unsynced_records >= max_unsynced_records.get()
                    || elapsed(now, self.last_sync_at) >= max_unsynced_age
            }
        };
        if due {
            self.flush_and_sync(now)?;
        }
        Ok(due)
    }
}

impl Drop for RepairWal {
    fn drop(&mut self) {
        if self.unsynced_records != 0 {
            let _ = self.file.flush();
            let _ = self.file.sync_data();
        }
        let _ = File::unlock(&self.file);
    }
}

fn validate_repair_wal_path(path: &Path) -> io::Result<()> {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "repair WAL path has no UTF-8 file name",
        ));
    };
    if !name.ends_with(".repair.wal") {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            format!(
                "repair WAL path must end in .repair.wal (refusing {})",
                path.display()
            ),
        ));
    }
    Ok(())
}

fn validate_header(file: &mut File, path: &Path) -> io::Result<()> {
    file.seek(SeekFrom::Start(0))?;
    let mut header = [0u8; FILE_HEADER.len()];
    file.read_exact(&mut header).map_err(|error| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "{} is not a complete repair WAL header: {error}",
                path.display()
            ),
        )
    })?;
    if &header != FILE_HEADER {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("{} is not a repair WAL; refusing to append", path.display()),
        ));
    }
    Ok(())
}

/// Returns the last valid byte and next sequence. Only a physically truncated final frame is
/// recoverable; checksum errors, malformed lengths, and non-contiguous sequences fail closed.
fn scan_valid_frames(file: &mut File, file_len: u64) -> io::Result<(u64, u64)> {
    file.seek(SeekFrom::Start(FILE_HEADER.len() as u64))?;
    let mut valid_len = FILE_HEADER.len() as u64;
    let mut expected_sequence = 0u64;
    while valid_len < file_len {
        let frame_start = valid_len;
        let body = match read_frame(file, true)? {
            Some(body) => body,
            None => return Ok((frame_start, expected_sequence)),
        };
        let sequence = u64::from_le_bytes(body[..8].try_into().expect("minimum body checked"));
        if sequence != expected_sequence {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "repair WAL sequence discontinuity: expected {expected_sequence}, got {sequence}"
                ),
            ));
        }
        expected_sequence = expected_sequence.checked_add(1).ok_or_else(|| {
            io::Error::new(ErrorKind::InvalidData, "repair WAL sequence overflow")
        })?;
        valid_len = file.stream_position()?;
    }
    Ok((valid_len, expected_sequence))
}

fn read_frame(file: &mut File, tolerate_truncated_tail: bool) -> io::Result<Option<Vec<u8>>> {
    let frame_start = file.stream_position()?;
    let mut prefix = [0u8; FRAME_PREFIX_BYTES];
    match file.read_exact(&mut prefix) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::UnexpectedEof && tolerate_truncated_tail => {
            return Ok(None);
        }
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
            if file.stream_position()? == frame_start {
                return Ok(None);
            }
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "truncated repair WAL frame prefix",
            ));
        }
        Err(error) => return Err(error),
    }
    let body_len = u32::from_le_bytes(prefix) as usize;
    if !(MIN_BODY_BYTES..=MAX_FRAME_BODY_BYTES).contains(&body_len) {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("invalid repair WAL frame body length {body_len}"),
        ));
    }
    let mut body = vec![0u8; body_len];
    if let Err(error) = file.read_exact(&mut body) {
        if error.kind() == ErrorKind::UnexpectedEof && tolerate_truncated_tail {
            return Ok(None);
        }
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("truncated repair WAL frame body: {error}"),
        ));
    }
    let mut checksum_bytes = [0u8; FRAME_CHECKSUM_BYTES];
    if let Err(error) = file.read_exact(&mut checksum_bytes) {
        if error.kind() == ErrorKind::UnexpectedEof && tolerate_truncated_tail {
            return Ok(None);
        }
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("truncated repair WAL checksum: {error}"),
        ));
    }
    let expected = u32::from_le_bytes(checksum_bytes);
    let actual = crc32(&body);
    if actual != expected {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("repair WAL checksum mismatch: expected {expected:#010x}, got {actual:#010x}"),
        ));
    }
    Ok(Some(body))
}

fn encode_body(
    sequence: u64,
    provenance: &RepairProvenance,
    shred_payload: &[u8],
) -> io::Result<Vec<u8>> {
    let peer_addr = provenance.peer_addr.as_bytes();
    let peer_addr_len = u16::try_from(peer_addr.len())
        .map_err(|_| io::Error::new(ErrorKind::InvalidData, "repair peer address is too long"))?;
    let payload_len = u32::try_from(shred_payload.len())
        .map_err(|_| io::Error::new(ErrorKind::InvalidData, "repair shred payload is too long"))?;
    let (request_kind, request_slot, request_index) = encode_request(provenance.request);
    let mut body = Vec::with_capacity(256 + peer_addr.len() + shred_payload.len());
    body.extend_from_slice(&sequence.to_le_bytes());
    body.extend_from_slice(&provenance.received_at_unix_ms.to_le_bytes());
    body.extend_from_slice(&provenance.nonce.to_le_bytes());
    body.push(request_kind);
    body.extend_from_slice(&request_slot.to_le_bytes());
    body.extend_from_slice(&request_index.to_le_bytes());
    body.extend_from_slice(&peer_addr_len.to_le_bytes());
    body.extend_from_slice(peer_addr);
    body.extend_from_slice(provenance.peer_pubkey.as_ref());
    body.extend_from_slice(&provenance.shred_slot.to_le_bytes());
    body.extend_from_slice(&provenance.shred_index.to_le_bytes());
    body.extend_from_slice(&provenance.fec_set_index.to_le_bytes());
    body.extend_from_slice(&provenance.shred_version.to_le_bytes());
    body.extend_from_slice(provenance.expected_slot_leader.as_ref());
    body.extend_from_slice(provenance.fec_merkle_root.as_ref());
    body.extend_from_slice(&provenance.trust_anchor_fec_set_index.to_le_bytes());
    body.push(u8::from(provenance.learned_chained_merkle_root));
    match provenance.chained_merkle_root {
        Some(root) => {
            body.push(1);
            body.extend_from_slice(root.as_ref());
        }
        None => body.push(0),
    }
    body.extend_from_slice(provenance.leader_signature.as_ref());
    body.extend_from_slice(&payload_len.to_le_bytes());
    body.extend_from_slice(shred_payload);
    Ok(body)
}

fn decode_body(body: &[u8]) -> io::Result<RepairWalEntry> {
    let mut decoder = Decoder::new(body);
    let sequence = decoder.u64()?;
    let received_at_unix_ms = decoder.u64()?;
    let nonce = decoder.u32()?;
    let request_kind = decoder.u8()?;
    let request_slot = decoder.u64()?;
    let request_index = decoder.u64()?;
    let request = decode_request(request_kind, request_slot, request_index)?;
    let peer_addr_len = decoder.u16()? as usize;
    let peer_addr = String::from_utf8(decoder.bytes(peer_addr_len)?.to_vec()).map_err(|_| {
        io::Error::new(
            ErrorKind::InvalidData,
            "repair WAL peer address is not UTF-8",
        )
    })?;
    let peer_pubkey = Pubkey::new_from_array(decoder.array()?);
    let shred_slot = decoder.u64()?;
    let shred_index = decoder.u32()?;
    let fec_set_index = decoder.u32()?;
    let shred_version = decoder.u16()?;
    let expected_slot_leader = Pubkey::new_from_array(decoder.array()?);
    let fec_merkle_root = Hash::new_from_array(decoder.array()?);
    let trust_anchor_fec_set_index = decoder.u32()?;
    let learned_chained_merkle_root = match decoder.u8()? {
        0 => false,
        1 => true,
        value => {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("invalid repair WAL learned-chain flag {value}"),
            ));
        }
    };
    let chained_merkle_root = match decoder.u8()? {
        0 => None,
        1 => Some(Hash::new_from_array(decoder.array()?)),
        value => {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("invalid repair WAL chained-root flag {value}"),
            ));
        }
    };
    let leader_signature = Signature::from(decoder.array::<64>()?);
    let payload_len = decoder.u32()? as usize;
    let shred_payload = decoder.bytes(payload_len)?.to_vec();
    if !decoder.is_empty() {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "repair WAL frame has trailing bytes",
        ));
    }
    Ok(RepairWalEntry {
        sequence,
        provenance: RepairProvenance {
            received_at_unix_ms,
            nonce,
            request,
            peer_addr,
            peer_pubkey,
            shred_slot,
            shred_index,
            fec_set_index,
            shred_version,
            expected_slot_leader,
            fec_merkle_root,
            trust_anchor_fec_set_index,
            learned_chained_merkle_root,
            chained_merkle_root,
            leader_signature,
        },
        shred_payload,
    })
}

fn encode_request(request: ShredRepairRequest) -> (u8, u64, u64) {
    match request {
        ShredRepairRequest::Shred { slot, shred_index } => (0, slot, shred_index),
        ShredRepairRequest::HighestShred { slot, shred_index } => (1, slot, shred_index),
        ShredRepairRequest::Orphan { slot } => (2, slot, 0),
    }
}

fn decode_request(kind: u8, slot: u64, index: u64) -> io::Result<ShredRepairRequest> {
    match kind {
        0 => Ok(ShredRepairRequest::Shred {
            slot,
            shred_index: index,
        }),
        1 => Ok(ShredRepairRequest::HighestShred {
            slot,
            shred_index: index,
        }),
        2 if index == 0 => Ok(ShredRepairRequest::Orphan { slot }),
        _ => Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("invalid repair WAL request kind/index {kind}/{index}"),
        )),
    }
}

struct Decoder<'a> {
    remaining: &'a [u8],
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    fn bytes(&mut self, len: usize) -> io::Result<&'a [u8]> {
        let Some((head, tail)) = self.remaining.split_at_checked(len) else {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "truncated repair WAL record",
            ));
        };
        self.remaining = tail;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        Ok(self
            .bytes(N)?
            .try_into()
            .expect("decoder requested a fixed checked length"))
    }

    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.bytes(1)?[0])
    }

    fn u16(&mut self) -> io::Result<u16> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }
}

fn elapsed(now: Instant, earlier: Instant) -> Duration {
    now.checked_duration_since(earlier)
        .unwrap_or(Duration::ZERO)
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320u32 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn provenance() -> RepairProvenance {
        RepairProvenance {
            received_at_unix_ms: 1_723_456_789_012,
            nonce: 42,
            request: ShredRepairRequest::Shred {
                slot: 123,
                shred_index: 7,
            },
            peer_addr: "127.0.0.1:8000".into(),
            peer_pubkey: Pubkey::new_from_array([1; 32]),
            shred_slot: 123,
            shred_index: 7,
            fec_set_index: 0,
            shred_version: 50093,
            expected_slot_leader: Pubkey::new_from_array([2; 32]),
            fec_merkle_root: Hash::new_from_array([3; 32]),
            trust_anchor_fec_set_index: 0,
            learned_chained_merkle_root: false,
            chained_merkle_root: Some(Hash::new_from_array([4; 32])),
            leader_signature: Signature::from([5; 64]),
        }
    }

    #[test]
    fn round_trips_provenance_and_payload() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("accepted.repair.wal");
        let now = Instant::now();
        {
            let mut wal = RepairWal::open(
                RepairWalConfig {
                    path: path.clone(),
                    fsync: RepairWalFsyncPolicy::EveryRecord,
                    max_file_bytes: 1024 * 1024,
                },
                now,
            )
            .unwrap();
            let append = wal.append(&provenance(), &[9, 8, 7], now).unwrap();
            assert_eq!(append.sequence, 0);
            assert!(append.synced);
        }

        let entries = RepairWal::read_all(&path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence, 0);
        assert_eq!(entries[0].provenance, provenance());
        assert_eq!(entries[0].shred_payload, [9, 8, 7]);
    }

    #[test]
    fn refuses_non_repair_file_and_wrong_suffix() {
        let directory = tempdir().unwrap();
        let wrong_suffix = directory.path().join("raw.wal");
        assert!(
            RepairWal::open(
                RepairWalConfig {
                    path: wrong_suffix,
                    fsync: RepairWalFsyncPolicy::EveryRecord,
                    max_file_bytes: 1024 * 1024,
                },
                Instant::now(),
            )
            .is_err()
        );

        let foreign = directory.path().join("raw-disguised.repair.wal");
        std::fs::write(&foreign, b"not a repair wal").unwrap();
        assert!(
            RepairWal::open(
                RepairWalConfig {
                    path: foreign.clone(),
                    fsync: RepairWalFsyncPolicy::EveryRecord,
                    max_file_bytes: 1024 * 1024,
                },
                Instant::now(),
            )
            .is_err()
        );
        assert_eq!(std::fs::read(foreign).unwrap(), b"not a repair wal");
    }

    #[test]
    fn recovers_only_a_truncated_final_frame() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("accepted.repair.wal");
        let now = Instant::now();
        let valid_len;
        {
            let mut wal = RepairWal::open(
                RepairWalConfig {
                    path: path.clone(),
                    fsync: RepairWalFsyncPolicy::EveryRecord,
                    max_file_bytes: 1024 * 1024,
                },
                now,
            )
            .unwrap();
            wal.append(&provenance(), &[1, 2, 3], now).unwrap();
            valid_len = wal.file.metadata().unwrap().len();
        }
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[10, 0, 0, 0, 1, 2])
            .unwrap();

        let wal = RepairWal::open(
            RepairWalConfig {
                path: path.clone(),
                fsync: RepairWalFsyncPolicy::EveryRecord,
                max_file_bytes: 1024 * 1024,
            },
            now,
        )
        .unwrap();
        assert_eq!(wal.next_sequence(), 1);
        assert_eq!(std::fs::metadata(path).unwrap().len(), valid_len);
    }

    #[test]
    fn batch_policy_syncs_on_age_without_a_new_append() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("batched.repair.wal");
        let now = Instant::now();
        let mut wal = RepairWal::open(
            RepairWalConfig {
                path,
                fsync: RepairWalFsyncPolicy::Batch {
                    max_unsynced_records: NonZeroU64::new(10).unwrap(),
                    max_unsynced_age: Duration::from_millis(100),
                },
                max_file_bytes: 1024 * 1024,
            },
            now,
        )
        .unwrap();
        assert!(!wal.append(&provenance(), &[1], now).unwrap().synced);
        assert!(!wal.sync_if_due(now + Duration::from_millis(99)).unwrap());
        assert!(wal.sync_if_due(now + Duration::from_millis(100)).unwrap());
        assert!(!wal.sync_if_due(now + Duration::from_millis(200)).unwrap());
    }

    #[test]
    fn cap_refuses_append_before_any_partial_write() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("capped.repair.wal");
        let now = Instant::now();
        let mut wal = RepairWal::open(
            RepairWalConfig {
                path: path.clone(),
                fsync: RepairWalFsyncPolicy::EveryRecord,
                max_file_bytes: MIN_REPAIR_WAL_FILE_BYTES,
            },
            now,
        )
        .unwrap();
        let overhead = encode_body(0, &provenance(), &[]).unwrap().len();
        let payload = vec![7; MAX_FRAME_BODY_BYTES - overhead];
        wal.append(&provenance(), &payload, now).unwrap();
        assert_eq!(wal.file_len(), MIN_REPAIR_WAL_FILE_BYTES);
        let before = std::fs::read(&path).unwrap();

        let error = wal.append(&provenance(), &[1], now).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::StorageFull);
        assert_eq!(wal.next_sequence(), 1);
        assert_eq!(wal.file_len(), MIN_REPAIR_WAL_FILE_BYTES);
        assert_eq!(std::fs::read(path).unwrap(), before);
    }

    #[test]
    fn cap_must_hold_header_and_one_maximum_frame() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("too-small.repair.wal");
        let error = RepairWal::open(
            RepairWalConfig {
                path: path.clone(),
                fsync: RepairWalFsyncPolicy::EveryRecord,
                max_file_bytes: MIN_REPAIR_WAL_FILE_BYTES - 1,
            },
            Instant::now(),
        )
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(
            !path.exists(),
            "invalid cap must fail before creating a file"
        );
    }
}
