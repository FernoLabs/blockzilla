use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};

use crate::{
    BlockzillaAuthorityId, ProtocolError, Result, StreamId, StreamManifestSha256,
    StreamRegistrySnapshotSha256,
};

pub const MAX_REGISTRY_LOGICAL_NAME_BYTES: usize = 128;
pub const MAX_REGISTRY_ENTRIES_V1: usize = 65_536;
pub const MAX_STREAM_REGISTRY_SNAPSHOT_V1_ENCODED_LEN: usize = 33_554_432;
pub const STREAM_REGISTRY_ENTRY_V1_FIXED_ENCODED_LEN: usize = 8 + 8 + 16 + 32 + 1 + 1;
pub const STREAM_REGISTRY_SNAPSHOT_V1_FIXED_ENCODED_LEN: usize = 16 + 8 + 32 + 4 + 32;
pub const STREAM_REGISTRY_HEAD_V1_ENCODED_LEN: usize = 8 + 32;

const STREAM_REGISTRY_DOMAIN: &[u8] = b"hive/v1/stream-registry";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum StreamRegistryStatusV1 {
    Active = 1,
    Closed = 2,
    Quarantined = 3,
}

impl TryFrom<u8> for StreamRegistryStatusV1 {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Active),
            2 => Ok(Self::Closed),
            3 => Ok(Self::Quarantined),
            value => Err(ProtocolError::UnknownRegistryStatus { value }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamRegistryEntryV1 {
    logical_name: Vec<u8>,
    stream_generation: u64,
    stream_id: StreamId,
    stream_manifest_sha256: StreamManifestSha256,
    status: StreamRegistryStatusV1,
    successor_stream_id: Option<StreamId>,
}

impl StreamRegistryEntryV1 {
    pub fn new(
        logical_name: Vec<u8>,
        stream_generation: u64,
        stream_id: StreamId,
        stream_manifest_sha256: StreamManifestSha256,
        status: StreamRegistryStatusV1,
        successor_stream_id: Option<StreamId>,
    ) -> Result<Self> {
        validate_logical_name(&logical_name)?;
        if status == StreamRegistryStatusV1::Active && successor_stream_id.is_some() {
            return Err(ProtocolError::InvalidRegistrySnapshot {
                reason: "an ACTIVE entry cannot name a successor",
            });
        }
        if successor_stream_id == Some(stream_id) {
            return Err(ProtocolError::InvalidRegistrySnapshot {
                reason: "a stream cannot name itself as successor",
            });
        }
        Ok(Self {
            logical_name,
            stream_generation,
            stream_id,
            stream_manifest_sha256,
            status,
            successor_stream_id,
        })
    }

    #[must_use]
    pub fn logical_name(&self) -> &[u8] {
        &self.logical_name
    }

    #[must_use]
    pub const fn stream_generation(&self) -> u64 {
        self.stream_generation
    }

    #[must_use]
    pub const fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    #[must_use]
    pub const fn stream_manifest_sha256(&self) -> StreamManifestSha256 {
        self.stream_manifest_sha256
    }

    #[must_use]
    pub const fn status(&self) -> StreamRegistryStatusV1 {
        self.status
    }

    #[must_use]
    pub const fn successor_stream_id(&self) -> Option<StreamId> {
        self.successor_stream_id
    }

    #[must_use]
    pub fn encoded_len(&self) -> usize {
        STREAM_REGISTRY_ENTRY_V1_FIXED_ENCODED_LEN
            + self.logical_name.len()
            + usize::from(self.successor_stream_id.is_some()) * StreamId::LENGTH
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(self.encoded_len());
        self.encode_into(&mut encoded);
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(encoded);
        let entry = Self::decode_from(&mut reader)?;
        reader.finish("StreamRegistryEntryV1")?;
        Ok(entry)
    }

    fn encode_into(&self, encoded: &mut Vec<u8>) {
        encoded.extend_from_slice(&(self.logical_name.len() as u64).to_be_bytes());
        encoded.extend_from_slice(&self.logical_name);
        encoded.extend_from_slice(&self.stream_generation.to_be_bytes());
        encoded.extend_from_slice(self.stream_id.as_bytes());
        encoded.extend_from_slice(self.stream_manifest_sha256.as_bytes());
        encoded.push(self.status as u8);
        match self.successor_stream_id {
            None => encoded.push(0),
            Some(successor) => {
                encoded.push(1);
                encoded.extend_from_slice(successor.as_bytes());
            }
        }
    }

    fn decode_from(reader: &mut Reader<'_>) -> Result<Self> {
        let logical_name_len = reader.u64("registry_logical_name_len")?;
        if logical_name_len == 0 || logical_name_len > MAX_REGISTRY_LOGICAL_NAME_BYTES as u64 {
            return Err(ProtocolError::InvalidRegistryLogicalName {
                reason: "length must be in 1..=128 bytes",
            });
        }
        let logical_name_len =
            usize::try_from(logical_name_len).map_err(|_| ProtocolError::IntegerOverflow {
                field: "registry_logical_name_len",
            })?;
        let logical_name = reader
            .take(logical_name_len, "registry_logical_name")?
            .to_vec();
        let stream_generation = reader.u64("registry_stream_generation")?;
        let stream_id = StreamId::try_from(reader.take(StreamId::LENGTH, "registry_stream_id")?)?;
        let stream_manifest_sha256 = StreamManifestSha256::try_from(reader.take(
            StreamManifestSha256::LENGTH,
            "registry_stream_manifest_sha256",
        )?)?;
        let status = StreamRegistryStatusV1::try_from(reader.u8("registry_status")?)?;
        let successor_stream_id = match reader.u8("registry_successor_stream_id")? {
            0 => None,
            1 => Some(StreamId::try_from(
                reader.take(StreamId::LENGTH, "registry_successor_stream_id")?,
            )?),
            value => {
                return Err(ProtocolError::InvalidOptionTag {
                    field: "registry_successor_stream_id",
                    value,
                });
            }
        };
        Self::new(
            logical_name,
            stream_generation,
            stream_id,
            stream_manifest_sha256,
            status,
            successor_stream_id,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamRegistrySnapshotV1 {
    blockzilla_authority_id: BlockzillaAuthorityId,
    registry_generation: u64,
    previous_snapshot_sha256: StreamRegistrySnapshotSha256,
    entries: Vec<StreamRegistryEntryV1>,
    snapshot_sha256: StreamRegistrySnapshotSha256,
}

impl StreamRegistrySnapshotV1 {
    pub fn new(
        blockzilla_authority_id: BlockzillaAuthorityId,
        registry_generation: u64,
        previous_snapshot_sha256: StreamRegistrySnapshotSha256,
        mut entries: Vec<StreamRegistryEntryV1>,
    ) -> Result<Self> {
        if registry_generation == 0
            && previous_snapshot_sha256 != StreamRegistrySnapshotSha256::new([0; 32])
        {
            return Err(ProtocolError::InvalidRegistryGeneration {
                reason: "generation zero requires an all-zero predecessor digest",
            });
        }
        if entries.len() > MAX_REGISTRY_ENTRIES_V1 {
            return Err(ProtocolError::RegistryEntryLimitExceeded {
                actual: entries.len() as u64,
                max: MAX_REGISTRY_ENTRIES_V1 as u64,
            });
        }
        entries.sort_by(|left, right| entry_key(left).cmp(&entry_key(right)));
        validate_entries(&entries)?;

        let mut snapshot = Self {
            blockzilla_authority_id,
            registry_generation,
            previous_snapshot_sha256,
            entries,
            snapshot_sha256: StreamRegistrySnapshotSha256::new([0; 32]),
        };
        let body = snapshot.encode_without_snapshot_hash();
        let stored_len = body
            .len()
            .checked_add(StreamRegistrySnapshotSha256::LENGTH)
            .ok_or(ProtocolError::IntegerOverflow {
                field: "StreamRegistrySnapshotV1.encoded_len",
            })?;
        if stored_len > MAX_STREAM_REGISTRY_SNAPSHOT_V1_ENCODED_LEN {
            return Err(ProtocolError::RegistrySnapshotTooLarge {
                actual: stored_len as u64,
                max: MAX_STREAM_REGISTRY_SNAPSHOT_V1_ENCODED_LEN as u64,
            });
        }
        snapshot.snapshot_sha256 = stream_registry_snapshot_sha256(&body);
        Ok(snapshot)
    }

    #[must_use]
    pub const fn blockzilla_authority_id(&self) -> BlockzillaAuthorityId {
        self.blockzilla_authority_id
    }

    #[must_use]
    pub const fn registry_generation(&self) -> u64 {
        self.registry_generation
    }

    #[must_use]
    pub const fn previous_snapshot_sha256(&self) -> StreamRegistrySnapshotSha256 {
        self.previous_snapshot_sha256
    }

    #[must_use]
    pub fn entries(&self) -> &[StreamRegistryEntryV1] {
        &self.entries
    }

    #[must_use]
    pub const fn snapshot_sha256(&self) -> StreamRegistrySnapshotSha256 {
        self.snapshot_sha256
    }

    #[must_use]
    pub fn encode_without_snapshot_hash(&self) -> Vec<u8> {
        let entries_len = self
            .entries
            .iter()
            .map(StreamRegistryEntryV1::encoded_len)
            .sum::<usize>();
        let mut encoded = Vec::with_capacity(
            STREAM_REGISTRY_SNAPSHOT_V1_FIXED_ENCODED_LEN - StreamRegistrySnapshotSha256::LENGTH
                + entries_len,
        );
        encoded.extend_from_slice(self.blockzilla_authority_id.as_bytes());
        encoded.extend_from_slice(&self.registry_generation.to_be_bytes());
        encoded.extend_from_slice(self.previous_snapshot_sha256.as_bytes());
        encoded.extend_from_slice(&(self.entries.len() as u32).to_be_bytes());
        for entry in &self.entries {
            entry.encode_into(&mut encoded);
        }
        encoded
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = self.encode_without_snapshot_hash();
        encoded.extend_from_slice(self.snapshot_sha256.as_bytes());
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        if encoded.len() > MAX_STREAM_REGISTRY_SNAPSHOT_V1_ENCODED_LEN {
            return Err(ProtocolError::RegistrySnapshotTooLarge {
                actual: encoded.len() as u64,
                max: MAX_STREAM_REGISTRY_SNAPSHOT_V1_ENCODED_LEN as u64,
            });
        }
        let mut reader = Reader::new(encoded);
        let blockzilla_authority_id = BlockzillaAuthorityId::try_from(reader.take(
            BlockzillaAuthorityId::LENGTH,
            "registry_blockzilla_authority_id",
        )?)?;
        let registry_generation = reader.u64("registry_generation")?;
        let previous_snapshot_sha256 = StreamRegistrySnapshotSha256::try_from(reader.take(
            StreamRegistrySnapshotSha256::LENGTH,
            "registry_previous_snapshot_sha256",
        )?)?;
        let entry_count = reader.u32("registry_entry_count")? as u64;
        if entry_count > MAX_REGISTRY_ENTRIES_V1 as u64 {
            return Err(ProtocolError::RegistryEntryLimitExceeded {
                actual: entry_count,
                max: MAX_REGISTRY_ENTRIES_V1 as u64,
            });
        }
        let entry_count =
            usize::try_from(entry_count).map_err(|_| ProtocolError::IntegerOverflow {
                field: "registry_entry_count",
            })?;
        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            entries.push(StreamRegistryEntryV1::decode_from(&mut reader)?);
        }
        let snapshot_sha256 = StreamRegistrySnapshotSha256::try_from(reader.take(
            StreamRegistrySnapshotSha256::LENGTH,
            "registry_snapshot_sha256",
        )?)?;
        reader.finish("StreamRegistrySnapshotV1")?;

        if entries
            .windows(2)
            .any(|pair| entry_key(&pair[0]) >= entry_key(&pair[1]))
        {
            return Err(ProtocolError::NonCanonicalOrder {
                context: "StreamRegistrySnapshotV1.entries",
            });
        }
        let decoded = Self::new(
            blockzilla_authority_id,
            registry_generation,
            previous_snapshot_sha256,
            entries,
        )?;
        if snapshot_sha256 != decoded.snapshot_sha256 {
            return Err(ProtocolError::RegistrySnapshotHashMismatch);
        }
        if decoded.encode() != encoded {
            return Err(ProtocolError::NonCanonicalOrder {
                context: "StreamRegistrySnapshotV1",
            });
        }
        Ok(decoded)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamRegistryHeadV1 {
    registry_generation: u64,
    snapshot_sha256: StreamRegistrySnapshotSha256,
}

impl StreamRegistryHeadV1 {
    #[must_use]
    pub const fn new(
        registry_generation: u64,
        snapshot_sha256: StreamRegistrySnapshotSha256,
    ) -> Self {
        Self {
            registry_generation,
            snapshot_sha256,
        }
    }

    #[must_use]
    pub const fn from_snapshot(snapshot: &StreamRegistrySnapshotV1) -> Self {
        Self::new(snapshot.registry_generation, snapshot.snapshot_sha256)
    }

    #[must_use]
    pub const fn registry_generation(&self) -> u64 {
        self.registry_generation
    }

    #[must_use]
    pub const fn snapshot_sha256(&self) -> StreamRegistrySnapshotSha256 {
        self.snapshot_sha256
    }

    #[must_use]
    pub fn encode(&self) -> [u8; STREAM_REGISTRY_HEAD_V1_ENCODED_LEN] {
        let mut encoded = [0; STREAM_REGISTRY_HEAD_V1_ENCODED_LEN];
        encoded[..8].copy_from_slice(&self.registry_generation.to_be_bytes());
        encoded[8..].copy_from_slice(self.snapshot_sha256.as_bytes());
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        if encoded.len() < STREAM_REGISTRY_HEAD_V1_ENCODED_LEN {
            return Err(ProtocolError::Truncated {
                context: "StreamRegistryHeadV1",
                expected: STREAM_REGISTRY_HEAD_V1_ENCODED_LEN,
                actual: encoded.len(),
            });
        }
        if encoded.len() > STREAM_REGISTRY_HEAD_V1_ENCODED_LEN {
            return Err(ProtocolError::TrailingBytes {
                context: "StreamRegistryHeadV1",
                count: encoded.len() - STREAM_REGISTRY_HEAD_V1_ENCODED_LEN,
            });
        }
        let registry_generation = u64::from_be_bytes(encoded[..8].try_into().expect("fixed slice"));
        let snapshot_sha256 = StreamRegistrySnapshotSha256::try_from(&encoded[8..])?;
        Ok(Self::new(registry_generation, snapshot_sha256))
    }

    pub fn validate_snapshot(&self, snapshot: &StreamRegistrySnapshotV1) -> Result<()> {
        if self.registry_generation != snapshot.registry_generation
            || self.snapshot_sha256 != snapshot.snapshot_sha256
        {
            return Err(ProtocolError::RegistryHeadMismatch);
        }
        Ok(())
    }
}

#[must_use]
pub fn stream_registry_snapshot_sha256(
    canonical_snapshot_without_hash: &[u8],
) -> StreamRegistrySnapshotSha256 {
    let mut hasher = Sha256::new();
    hasher.update(STREAM_REGISTRY_DOMAIN);
    hasher.update(canonical_snapshot_without_hash);
    StreamRegistrySnapshotSha256::new(hasher.finalize().into())
}

pub fn validate_stream_registry_transition(
    previous: Option<&StreamRegistrySnapshotV1>,
    next: &StreamRegistrySnapshotV1,
) -> Result<()> {
    let Some(previous) = previous else {
        if next.registry_generation != 0
            || next.previous_snapshot_sha256 != StreamRegistrySnapshotSha256::new([0; 32])
        {
            return Err(ProtocolError::InvalidRegistryTransition {
                reason: "the first snapshot must be generation zero with a zero predecessor",
            });
        }
        return Ok(());
    };

    if next.blockzilla_authority_id != previous.blockzilla_authority_id {
        return Err(ProtocolError::RegistryAuthorityMismatch);
    }
    let expected_generation =
        previous
            .registry_generation
            .checked_add(1)
            .ok_or(ProtocolError::IntegerOverflow {
                field: "registry_generation",
            })?;
    if next.registry_generation != expected_generation
        || next.previous_snapshot_sha256 != previous.snapshot_sha256
    {
        return Err(ProtocolError::InvalidRegistryTransition {
            reason: "generation or predecessor digest does not extend the exact prior snapshot",
        });
    }

    let old_entries = previous
        .entries
        .iter()
        .map(|entry| (entry_key(entry), entry))
        .collect::<BTreeMap<_, _>>();
    let new_entries = next
        .entries
        .iter()
        .map(|entry| (entry_key(entry), entry))
        .collect::<BTreeMap<_, _>>();
    for (key, old) in old_entries {
        let new = new_entries
            .get(&key)
            .ok_or(ProtocolError::InvalidRegistryTransition {
                reason: "a complete successor snapshot cannot remove a retained entry",
            })?;
        if old.stream_id != new.stream_id
            || old.stream_manifest_sha256 != new.stream_manifest_sha256
        {
            return Err(ProtocolError::InvalidRegistryTransition {
                reason: "a logical-name generation mapping is immutable",
            });
        }
        if !valid_status_transition(old.status, new.status) {
            return Err(ProtocolError::InvalidRegistryTransition {
                reason: "stream status moved backward",
            });
        }
        if old.successor_stream_id.is_some() && old.successor_stream_id != new.successor_stream_id {
            return Err(ProtocolError::InvalidRegistryTransition {
                reason: "a published successor link is immutable",
            });
        }
    }
    Ok(())
}

fn validate_entries(entries: &[StreamRegistryEntryV1]) -> Result<()> {
    if entries
        .windows(2)
        .any(|pair| entry_key(&pair[0]) >= entry_key(&pair[1]))
    {
        return Err(ProtocolError::InvalidRegistrySnapshot {
            reason: "logical-name/generation pairs must be unique",
        });
    }

    let mut stream_ids = BTreeSet::new();
    for entry in entries {
        if !stream_ids.insert(entry.stream_id) {
            return Err(ProtocolError::InvalidRegistrySnapshot {
                reason: "one stream ID may appear in only one registry entry",
            });
        }
    }

    let by_stream_id = entries
        .iter()
        .map(|entry| (entry.stream_id, entry))
        .collect::<BTreeMap<_, _>>();
    let mut offset = 0;
    while offset < entries.len() {
        let logical_name = entries[offset].logical_name.as_slice();
        let mut expected_generation = 0u64;
        let mut active_count = 0usize;
        while offset < entries.len() && entries[offset].logical_name == logical_name {
            let entry = &entries[offset];
            if entry.stream_generation != expected_generation {
                return Err(ProtocolError::InvalidRegistrySnapshot {
                    reason: "generations for one logical name must start at zero and be contiguous",
                });
            }
            expected_generation =
                expected_generation
                    .checked_add(1)
                    .ok_or(ProtocolError::IntegerOverflow {
                        field: "stream_generation",
                    })?;
            active_count += usize::from(entry.status == StreamRegistryStatusV1::Active);
            if let Some(successor_stream_id) = entry.successor_stream_id {
                let successor = by_stream_id.get(&successor_stream_id).ok_or(
                    ProtocolError::InvalidRegistrySnapshot {
                        reason: "a successor link cannot dangle",
                    },
                )?;
                if successor.logical_name != entry.logical_name
                    || successor.stream_generation <= entry.stream_generation
                {
                    return Err(ProtocolError::InvalidRegistrySnapshot {
                        reason: "a successor must be a higher generation of the same logical name",
                    });
                }
            }
            offset += 1;
        }
        if active_count > 1 {
            return Err(ProtocolError::InvalidRegistrySnapshot {
                reason: "at most one generation per logical name may be ACTIVE",
            });
        }
    }
    Ok(())
}

fn validate_logical_name(logical_name: &[u8]) -> Result<()> {
    if logical_name.is_empty() || logical_name.len() > MAX_REGISTRY_LOGICAL_NAME_BYTES {
        return Err(ProtocolError::InvalidRegistryLogicalName {
            reason: "length must be in 1..=128 bytes",
        });
    }
    if !logical_name[0].is_ascii_lowercase() && !logical_name[0].is_ascii_digit() {
        return Err(ProtocolError::InvalidRegistryLogicalName {
            reason: "the first byte must be lowercase ASCII or a digit",
        });
    }
    if !logical_name
        .iter()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._/-".contains(byte))
    {
        return Err(ProtocolError::InvalidRegistryLogicalName {
            reason: "bytes must match [a-z0-9][a-z0-9._/-]{0,127}",
        });
    }
    Ok(())
}

fn valid_status_transition(previous: StreamRegistryStatusV1, next: StreamRegistryStatusV1) -> bool {
    matches!(
        (previous, next),
        (StreamRegistryStatusV1::Active, _)
            | (
                StreamRegistryStatusV1::Closed,
                StreamRegistryStatusV1::Closed
            )
            | (
                StreamRegistryStatusV1::Closed,
                StreamRegistryStatusV1::Quarantined
            )
            | (
                StreamRegistryStatusV1::Quarantined,
                StreamRegistryStatusV1::Quarantined
            )
    )
}

fn entry_key(entry: &StreamRegistryEntryV1) -> (&[u8], u64) {
    (&entry.logical_name, entry.stream_generation)
}

struct Reader<'a> {
    encoded: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(encoded: &'a [u8]) -> Self {
        Self { encoded, offset: 0 }
    }

    fn take(&mut self, length: usize, context: &'static str) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(ProtocolError::IntegerOverflow { field: context })?;
        if end > self.encoded.len() {
            return Err(ProtocolError::Truncated {
                context,
                expected: end,
                actual: self.encoded.len(),
            });
        }
        let bytes = &self.encoded[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    fn u8(&mut self, context: &'static str) -> Result<u8> {
        Ok(self.take(1, context)?[0])
    }

    fn u32(&mut self, context: &'static str) -> Result<u32> {
        Ok(u32::from_be_bytes(
            self.take(4, context)?.try_into().expect("fixed slice"),
        ))
    }

    fn u64(&mut self, context: &'static str) -> Result<u64> {
        Ok(u64::from_be_bytes(
            self.take(8, context)?.try_into().expect("fixed slice"),
        ))
    }

    fn finish(self, context: &'static str) -> Result<()> {
        if self.offset == self.encoded.len() {
            Ok(())
        } else {
            Err(ProtocolError::TrailingBytes {
                context,
                count: self.encoded.len() - self.offset,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(
        generation: u64,
        id: u8,
        status: StreamRegistryStatusV1,
        successor_stream_id: Option<StreamId>,
    ) -> StreamRegistryEntryV1 {
        StreamRegistryEntryV1::new(
            b"solana.mainnet/shreds".to_vec(),
            generation,
            StreamId::new([id; 16]),
            StreamManifestSha256::new([id.wrapping_add(0x40); 32]),
            status,
            successor_stream_id,
        )
        .unwrap()
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn entry_and_head_have_exact_golden_bytes() {
        let entry = entry(0, 1, StreamRegistryStatusV1::Active, None);
        assert_eq!(
            hex(&entry.encode()),
            concat!(
                "0000000000000015",
                "736f6c616e612e6d61696e6e65742f736872656473",
                "0000000000000000",
                "01010101010101010101010101010101",
                "4141414141414141414141414141414141414141414141414141414141414141",
                "01",
                "00"
            )
        );
        assert_eq!(StreamRegistryEntryV1::decode(&entry.encode()), Ok(entry));

        let head = StreamRegistryHeadV1::new(7, StreamRegistrySnapshotSha256::new([0xab; 32]));
        assert_eq!(
            hex(&head.encode()),
            concat!(
                "0000000000000007",
                "abababababababababababababababababababababababababababababababab"
            )
        );
        assert_eq!(StreamRegistryHeadV1::decode(&head.encode()), Ok(head));
    }

    #[test]
    fn snapshots_sort_hash_round_trip_and_match_their_head() {
        let snapshot = StreamRegistrySnapshotV1::new(
            BlockzillaAuthorityId::new([0xa0; 16]),
            0,
            StreamRegistrySnapshotSha256::new([0; 32]),
            vec![entry(0, 1, StreamRegistryStatusV1::Active, None)],
        )
        .unwrap();
        assert_eq!(snapshot.encode().len(), 179);
        assert_eq!(
            hex(snapshot.snapshot_sha256().as_bytes()),
            "5e82d53f909ab730f413e5b074fb7f9dab23f925067fd4f32e861841afa1d61b"
        );
        assert_eq!(
            StreamRegistrySnapshotV1::decode(&snapshot.encode()),
            Ok(snapshot.clone())
        );
        StreamRegistryHeadV1::from_snapshot(&snapshot)
            .validate_snapshot(&snapshot)
            .unwrap();

        let empty = StreamRegistrySnapshotV1::new(
            BlockzillaAuthorityId::new([0; 16]),
            0,
            StreamRegistrySnapshotSha256::new([0; 32]),
            Vec::new(),
        )
        .unwrap();
        assert_eq!(
            empty.encode().len(),
            STREAM_REGISTRY_SNAPSHOT_V1_FIXED_ENCODED_LEN
        );
        assert_eq!(
            hex(empty.snapshot_sha256().as_bytes()),
            "223c5f9464756449b8cc6026ab9972c95b9e47efe4d61d70fd5f9ce51744a472"
        );
    }

    #[test]
    fn transition_is_linear_and_mappings_never_change() {
        let authority = BlockzillaAuthorityId::new([9; 16]);
        let first = StreamRegistrySnapshotV1::new(
            authority,
            0,
            StreamRegistrySnapshotSha256::new([0; 32]),
            vec![entry(0, 1, StreamRegistryStatusV1::Active, None)],
        )
        .unwrap();
        let second = StreamRegistrySnapshotV1::new(
            authority,
            1,
            first.snapshot_sha256(),
            vec![
                entry(
                    0,
                    1,
                    StreamRegistryStatusV1::Closed,
                    Some(StreamId::new([2; 16])),
                ),
                entry(1, 2, StreamRegistryStatusV1::Active, None),
            ],
        )
        .unwrap();
        validate_stream_registry_transition(None, &first).unwrap();
        validate_stream_registry_transition(Some(&first), &second).unwrap();

        let changed = StreamRegistrySnapshotV1::new(
            authority,
            1,
            first.snapshot_sha256(),
            vec![
                StreamRegistryEntryV1::new(
                    b"solana.mainnet/shreds".to_vec(),
                    0,
                    StreamId::new([8; 16]),
                    StreamManifestSha256::new([0x41; 32]),
                    StreamRegistryStatusV1::Closed,
                    None,
                )
                .unwrap(),
            ],
        )
        .unwrap();
        assert!(matches!(
            validate_stream_registry_transition(Some(&first), &changed),
            Err(ProtocolError::InvalidRegistryTransition { .. })
        ));
    }

    #[test]
    fn snapshot_rejects_invalid_names_generations_successors_and_duplicates() {
        assert!(matches!(
            StreamRegistryEntryV1::new(
                b"Bad".to_vec(),
                0,
                StreamId::new([1; 16]),
                StreamManifestSha256::new([2; 32]),
                StreamRegistryStatusV1::Active,
                None,
            ),
            Err(ProtocolError::InvalidRegistryLogicalName { .. })
        ));

        let result = StreamRegistrySnapshotV1::new(
            BlockzillaAuthorityId::new([0; 16]),
            0,
            StreamRegistrySnapshotSha256::new([0; 32]),
            vec![entry(1, 1, StreamRegistryStatusV1::Closed, None)],
        );
        assert!(matches!(
            result,
            Err(ProtocolError::InvalidRegistrySnapshot { .. })
        ));

        let duplicate = entry(0, 1, StreamRegistryStatusV1::Closed, None);
        let result = StreamRegistrySnapshotV1::new(
            BlockzillaAuthorityId::new([0; 16]),
            0,
            StreamRegistrySnapshotSha256::new([0; 32]),
            vec![duplicate.clone(), duplicate],
        );
        assert!(matches!(
            result,
            Err(ProtocolError::InvalidRegistrySnapshot { .. })
        ));
    }

    #[test]
    fn decoders_reject_declared_limits_unknown_values_hashes_and_trailing_bytes() {
        let mut malicious_snapshot = vec![0; 16 + 8 + 32];
        malicious_snapshot.extend_from_slice(&((MAX_REGISTRY_ENTRIES_V1 as u32) + 1).to_be_bytes());
        assert_eq!(
            StreamRegistrySnapshotV1::decode(&malicious_snapshot),
            Err(ProtocolError::RegistryEntryLimitExceeded {
                actual: MAX_REGISTRY_ENTRIES_V1 as u64 + 1,
                max: MAX_REGISTRY_ENTRIES_V1 as u64,
            })
        );

        let mut unknown_status = entry(0, 1, StreamRegistryStatusV1::Active, None).encode();
        let status_offset = unknown_status.len() - 2;
        unknown_status[status_offset] = 9;
        assert_eq!(
            StreamRegistryEntryV1::decode(&unknown_status),
            Err(ProtocolError::UnknownRegistryStatus { value: 9 })
        );

        let snapshot = StreamRegistrySnapshotV1::new(
            BlockzillaAuthorityId::new([0; 16]),
            0,
            StreamRegistrySnapshotSha256::new([0; 32]),
            Vec::new(),
        )
        .unwrap();
        let mut wrong_hash = snapshot.encode();
        *wrong_hash.last_mut().unwrap() ^= 1;
        assert_eq!(
            StreamRegistrySnapshotV1::decode(&wrong_hash),
            Err(ProtocolError::RegistrySnapshotHashMismatch)
        );

        let mut trailing_head = StreamRegistryHeadV1::from_snapshot(&snapshot)
            .encode()
            .to_vec();
        trailing_head.push(0);
        assert!(matches!(
            StreamRegistryHeadV1::decode(&trailing_head),
            Err(ProtocolError::TrailingBytes { count: 1, .. })
        ));
    }
}
