use {
    blockzilla_shred_codec::parse_shred_header,
    core::{convert::TryFrom, error::Error as StdError, fmt},
    sha2::{Digest, Sha256},
    solana_pubkey::Pubkey,
    solana_signature::Signature,
};

pub const DATA_SHREDS_PER_FEC_BLOCK: usize = 32;
pub const MAX_DATA_SHREDS_PER_SLOT: usize = 32_768;
pub const MAX_CODE_SHREDS_PER_SLOT: usize = 32_768;

const SIZE_OF_SIGNATURE: usize = 64;
const SIZE_OF_NONCE: usize = 4;
const SIZE_OF_COMMON_SHRED_HEADER: usize = blockzilla_shred_codec::COMMON_SHRED_HEADER_BYTES;
const SIZE_OF_DATA_SHRED_HEADERS: usize = 88;
const SIZE_OF_CODING_SHRED_HEADERS: usize = 89;

const DATA_SHRED_PAYLOAD_SIZE: usize = 1203;
const CODE_SHRED_PAYLOAD_SIZE: usize = 1228;

const SIZE_OF_MERKLE_ROOT: usize = 32;
const SIZE_OF_MERKLE_PROOF_ENTRY: usize = 20;
const MERKLE_HASH_PREFIX_LEAF: &[u8] = b"\x00SOLANA_MERKLE_SHREDS_LEAF";
const MERKLE_HASH_PREFIX_NODE: &[u8] = b"\x01SOLANA_MERKLE_SHREDS_NODE";

const SHRED_DATA_PARENT_OFFSET_OFFSET: usize = 83;
const SHRED_DATA_FLAGS_OFFSET: usize = 85;
const SHRED_DATA_SIZE_OFFSET: usize = 86;
const CODING_NUM_DATA_OFFSET: usize = 83;
const CODING_NUM_CODING_OFFSET: usize = 85;
const CODING_POSITION_OFFSET: usize = 87;

const DATA_COMPLETE_SHRED: u8 = 0b0100_0000;
const LAST_SHRED_IN_SLOT: u8 = 0b1100_0000;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum ShredType {
    Data,
    Code,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct ShredId {
    slot: u64,
    index: u32,
    shred_type: ShredType,
}

impl ShredId {
    #[inline]
    pub const fn new(slot: u64, index: u32, shred_type: ShredType) -> Self {
        Self {
            slot,
            index,
            shred_type,
        }
    }

    #[inline]
    pub const fn slot(&self) -> u64 {
        self.slot
    }

    #[inline]
    pub const fn index(&self) -> u32 {
        self.index
    }

    #[inline]
    pub const fn shred_type(&self) -> ShredType {
        self.shred_type
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum ShredError {
    InvalidPayload,
    InvalidPayloadSize(usize),
    InvalidProofSize(u8),
    InvalidMerkleProof,
}

impl fmt::Display for ShredError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPayload => formatter.write_str("shred payload is malformed"),
            Self::InvalidPayloadSize(size) => {
                write!(formatter, "invalid shred payload size: {size}")
            }
            Self::InvalidProofSize(proof_size) => {
                write!(formatter, "invalid merkle proof size: {proof_size}")
            }
            Self::InvalidMerkleProof => formatter.write_str("invalid merkle proof"),
        }
    }
}

impl StdError for ShredError {}

#[derive(Clone, Debug)]
pub struct Shred {
    payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ShredVariant {
    MerkleCode { proof_size: u8, resigned: bool },
    MerkleData { proof_size: u8, resigned: bool },
}

impl Shred {
    pub fn new_from_serialized_shred<T>(shred: T) -> Result<Self, ShredError>
    where
        T: AsRef<[u8]> + Into<Vec<u8>>,
    {
        let mut payload = shred.into();
        let variant = parse_shred_variant(payload.get(SIZE_OF_SIGNATURE).copied())
            .ok_or(ShredError::InvalidPayload)?;
        let expected_size = expected_shred_size(Some(variant));
        if payload.len() < expected_size {
            return Err(ShredError::InvalidPayloadSize(payload.len()));
        }
        payload.truncate(expected_size);
        Ok(Self { payload })
    }

    #[inline]
    pub fn slot(&self) -> u64 {
        parse_shred_header(&self.payload).map_or(0, |header| header.slot)
    }

    #[inline]
    pub fn index(&self) -> u32 {
        parse_shred_header(&self.payload).map_or(0, |header| header.index)
    }

    #[inline]
    pub fn fec_set_index(&self) -> u32 {
        parse_shred_header(&self.payload).map_or(0, |header| header.fec_set_index)
    }

    #[inline]
    pub fn version(&self) -> u16 {
        parse_shred_header(&self.payload).map_or(0, |header| header.version)
    }

    #[inline]
    pub fn shred_type(&self) -> ShredType {
        match self.variant().unwrap_or(ShredVariant::MerkleData {
            proof_size: 0,
            resigned: false,
        }) {
            ShredVariant::MerkleCode { .. } => ShredType::Code,
            ShredVariant::MerkleData { .. } => ShredType::Data,
        }
    }

    #[inline]
    pub fn is_data(&self) -> bool {
        self.shred_type() == ShredType::Data
    }

    #[inline]
    pub fn is_code(&self) -> bool {
        self.shred_type() == ShredType::Code
    }

    #[inline]
    pub fn is_shred_duplicate(&self, other: &Self) -> bool {
        self.id() == other.id() && self.payload != other.payload
    }

    #[inline]
    pub fn id(&self) -> ShredId {
        ShredId::new(self.slot(), self.index(), self.shred_type())
    }

    #[inline]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn into_payload(self) -> Box<[u8]> {
        self.payload.into_boxed_slice()
    }

    #[inline]
    pub fn signature(&self) -> &[u8; 64] {
        self.payload
            .get(0..SIZE_OF_SIGNATURE)
            .and_then(|bytes| <&[u8; 64]>::try_from(bytes).ok())
            .unwrap_or(&ZERO_SIGNATURE)
    }

    pub fn parent(&self) -> Result<u64, ShredError> {
        if !self.is_data() {
            return Err(ShredError::InvalidPayload);
        }
        let parent_offset = u16::from_le_bytes(
            self.payload
                .get(SHRED_DATA_PARENT_OFFSET_OFFSET..SHRED_DATA_PARENT_OFFSET_OFFSET + 2)
                .and_then(|bytes| <[u8; 2]>::try_from(bytes).ok())
                .ok_or(ShredError::InvalidPayload)?,
        );
        if parent_offset == 0 && self.slot() != 0 {
            return Err(ShredError::InvalidPayload);
        }
        self.slot()
            .checked_sub(u64::from(parent_offset))
            .ok_or(ShredError::InvalidPayload)
    }

    pub fn data(&self) -> Result<&[u8], ShredError> {
        if !self.is_data() {
            return Err(ShredError::InvalidPayload);
        }
        let variant = self.variant().ok_or(ShredError::InvalidPayload)?;
        let (proof_size, resigned) = match variant {
            ShredVariant::MerkleData {
                proof_size,
                resigned,
            } => (proof_size, resigned),
            ShredVariant::MerkleCode { .. } => return Err(ShredError::InvalidPayload),
        };
        let capacity = data_capacity(proof_size, resigned)?;
        let data_size = usize::from(u16::from_le_bytes(
            self.payload
                .get(SHRED_DATA_SIZE_OFFSET..SHRED_DATA_SIZE_OFFSET + 2)
                .and_then(|bytes| <[u8; 2]>::try_from(bytes).ok())
                .ok_or(ShredError::InvalidPayload)?,
        ));
        if !(SIZE_OF_DATA_SHRED_HEADERS..=SIZE_OF_DATA_SHRED_HEADERS + capacity)
            .contains(&data_size)
        {
            return Err(ShredError::InvalidPayload);
        }
        Ok(&self.payload[SIZE_OF_DATA_SHRED_HEADERS..data_size])
    }

    pub fn last_in_slot(&self) -> bool {
        self.is_data()
            && self
                .payload
                .get(SHRED_DATA_FLAGS_OFFSET)
                .is_some_and(|flags| (flags & LAST_SHRED_IN_SLOT) == LAST_SHRED_IN_SLOT)
    }

    pub fn data_complete(&self) -> bool {
        self.is_data()
            && self
                .payload
                .get(SHRED_DATA_FLAGS_OFFSET)
                .is_some_and(|flags| (flags & DATA_COMPLETE_SHRED) == DATA_COMPLETE_SHRED)
    }

    pub fn sanitize(&self) -> Result<(), ShredError> {
        let variant = self.variant().ok_or(ShredError::InvalidPayload)?;
        match variant {
            ShredVariant::MerkleData { .. } => self.sanitize_data(),
            ShredVariant::MerkleCode { .. } => self.sanitize_code(),
        }?;
        Ok(())
    }

    pub fn merkle_root(&self) -> Result<[u8; SIZE_OF_MERKLE_ROOT], ShredError> {
        let variant = self.variant().ok_or(ShredError::InvalidPayload)?;
        let (proof_size, resigned) = match variant {
            ShredVariant::MerkleData {
                proof_size,
                resigned,
            }
            | ShredVariant::MerkleCode {
                proof_size,
                resigned,
            } => (proof_size, resigned),
        };
        let proof_offset = proof_offset(self.is_data(), proof_size, resigned)?;
        let proof = merkle_proof(&self.payload, proof_offset, proof_size)?;
        let node = merkle_node(&self.payload[SIZE_OF_SIGNATURE..proof_offset])?;
        let index = match variant {
            ShredVariant::MerkleData { .. } => self.erasure_shard_index_data()?,
            ShredVariant::MerkleCode { .. } => self.erasure_shard_index_code()?,
        };
        merkle_root(index, node, &proof)
    }

    pub fn chained_merkle_root(&self) -> Result<[u8; SIZE_OF_MERKLE_ROOT], ShredError> {
        let variant = self.variant().ok_or(ShredError::InvalidPayload)?;
        let (proof_size, resigned) = match variant {
            ShredVariant::MerkleData {
                proof_size,
                resigned,
            }
            | ShredVariant::MerkleCode {
                proof_size,
                resigned,
            } => (proof_size, resigned),
        };
        let offset = chained_merkle_root_offset(self.is_data(), proof_size, resigned)?;
        let bytes = self
            .payload
            .get(offset..offset + SIZE_OF_MERKLE_ROOT)
            .ok_or(ShredError::InvalidPayload)?;
        <[u8; SIZE_OF_MERKLE_ROOT]>::try_from(bytes).map_err(|_| ShredError::InvalidPayload)
    }

    pub fn verify(&self, leader: &Pubkey) -> bool {
        let merkle_root = match self.merkle_root() {
            Ok(merkle_root) => merkle_root,
            Err(_) => return false,
        };
        Signature::from(*self.signature()).verify(leader.as_ref(), merkle_root.as_ref())
    }

    fn variant(&self) -> Option<ShredVariant> {
        parse_shred_variant(self.payload.get(SIZE_OF_SIGNATURE).copied())
    }

    fn sanitize_data(&self) -> Result<(), ShredError> {
        if self.payload.len() != DATA_SHRED_PAYLOAD_SIZE {
            return Err(ShredError::InvalidPayloadSize(self.payload.len()));
        }
        if usize::try_from(self.index()).ok() >= Some(MAX_DATA_SHREDS_PER_SLOT) {
            return Err(ShredError::InvalidPayload);
        }
        let ShredVariant::MerkleData {
            proof_size,
            resigned,
        } = self.variant().ok_or(ShredError::InvalidPayload)?
        else {
            return Err(ShredError::InvalidPayload);
        };
        let flags = self
            .payload
            .get(SHRED_DATA_FLAGS_OFFSET)
            .ok_or(ShredError::InvalidPayload)?;
        if (flags & LAST_SHRED_IN_SLOT) == LAST_SHRED_IN_SLOT && !self.data_complete() {
            return Err(ShredError::InvalidPayload);
        }
        data_capacity(proof_size, resigned)?;
        self.erasure_shard_index_data()?;
        self.data()?;
        self.parent()?;
        Ok(())
    }

    fn sanitize_code(&self) -> Result<(), ShredError> {
        if self.payload.len() != CODE_SHRED_PAYLOAD_SIZE {
            return Err(ShredError::InvalidPayloadSize(self.payload.len()));
        }
        if usize::try_from(self.index()).ok() >= Some(MAX_CODE_SHREDS_PER_SLOT) {
            return Err(ShredError::InvalidPayload);
        }
        let ShredVariant::MerkleCode {
            proof_size,
            resigned,
        } = self.variant().ok_or(ShredError::InvalidPayload)?
        else {
            return Err(ShredError::InvalidPayload);
        };
        let num_data = self
            .u16_at(CODING_NUM_DATA_OFFSET)?
            .map(usize::from)
            .ok_or(ShredError::InvalidPayload)?;
        let num_coding = self
            .u16_at(CODING_NUM_CODING_OFFSET)?
            .map(usize::from)
            .ok_or(ShredError::InvalidPayload)?;
        if num_coding > 8 * DATA_SHREDS_PER_FEC_BLOCK || num_data == 0 {
            return Err(ShredError::InvalidPayload);
        }
        let fec_set_index =
            usize::try_from(self.fec_set_index()).map_err(|_| ShredError::InvalidPayload)?;
        let num_data_minus_one = num_data.checked_sub(1).ok_or(ShredError::InvalidPayload)?;
        if fec_set_index
            .checked_add(num_data_minus_one)
            .ok_or(ShredError::InvalidPayload)?
            >= MAX_DATA_SHREDS_PER_SLOT
        {
            return Err(ShredError::InvalidPayload);
        }
        let first_coding_index =
            usize::try_from(self.first_coding_index()?).map_err(|_| ShredError::InvalidPayload)?;
        let num_coding_minus_one = num_coding
            .checked_sub(1)
            .ok_or(ShredError::InvalidPayload)?;
        if first_coding_index
            .checked_add(num_coding_minus_one)
            .ok_or(ShredError::InvalidPayload)?
            >= MAX_CODE_SHREDS_PER_SLOT
        {
            return Err(ShredError::InvalidPayload);
        }
        data_capacity(proof_size, resigned)?;
        self.erasure_shard_index_code()?;
        Ok(())
    }

    fn first_coding_index(&self) -> Result<u32, ShredError> {
        let position = self
            .u16_at(CODING_POSITION_OFFSET)?
            .map(u32::from)
            .ok_or(ShredError::InvalidPayload)?;
        self.index()
            .checked_sub(position)
            .ok_or(ShredError::InvalidPayload)
    }

    fn erasure_shard_index_data(&self) -> Result<usize, ShredError> {
        self.index()
            .checked_sub(self.fec_set_index())
            .and_then(|index| usize::try_from(index).ok())
            .ok_or(ShredError::InvalidPayload)
    }

    fn erasure_shard_index_code(&self) -> Result<usize, ShredError> {
        let num_data = self
            .u16_at(CODING_NUM_DATA_OFFSET)?
            .map(usize::from)
            .ok_or(ShredError::InvalidPayload)?;
        let num_coding = self
            .u16_at(CODING_NUM_CODING_OFFSET)?
            .map(usize::from)
            .ok_or(ShredError::InvalidPayload)?;
        let position = self
            .u16_at(CODING_POSITION_OFFSET)?
            .map(usize::from)
            .ok_or(ShredError::InvalidPayload)?;
        let index = position
            .checked_add(num_data)
            .ok_or(ShredError::InvalidPayload)?;
        let fec_set_size = num_data
            .checked_add(num_coding)
            .ok_or(ShredError::InvalidPayload)?;
        (index < fec_set_size)
            .then_some(index)
            .ok_or(ShredError::InvalidPayload)
    }

    fn u16_at(&self, offset: usize) -> Result<Option<u16>, ShredError> {
        let bytes = self
            .payload
            .get(offset..offset + 2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes);
        Ok(bytes)
    }
}

fn parse_shred_variant(byte: Option<u8>) -> Option<ShredVariant> {
    let byte = byte?;
    let proof_size = byte & 0x0f;
    match byte & 0xf0 {
        0x60 => Some(ShredVariant::MerkleCode {
            proof_size,
            resigned: false,
        }),
        0x70 => Some(ShredVariant::MerkleCode {
            proof_size,
            resigned: true,
        }),
        0x90 => Some(ShredVariant::MerkleData {
            proof_size,
            resigned: false,
        }),
        0xb0 => Some(ShredVariant::MerkleData {
            proof_size,
            resigned: true,
        }),
        _ => None,
    }
}

fn expected_shred_size(variant: Option<ShredVariant>) -> usize {
    match variant {
        Some(ShredVariant::MerkleCode { .. }) => CODE_SHRED_PAYLOAD_SIZE,
        Some(ShredVariant::MerkleData { .. }) => DATA_SHRED_PAYLOAD_SIZE,
        None => DATA_SHRED_PAYLOAD_SIZE,
    }
}

fn data_capacity(proof_size: u8, resigned: bool) -> Result<usize, ShredError> {
    merkle_payload_capacity(SIZE_OF_DATA_SHRED_HEADERS, proof_size, resigned)
}

fn code_capacity(proof_size: u8, resigned: bool) -> Result<usize, ShredError> {
    merkle_payload_capacity(SIZE_OF_CODING_SHRED_HEADERS, proof_size, resigned)
}

fn merkle_payload_capacity(
    headers: usize,
    proof_size: u8,
    resigned: bool,
) -> Result<usize, ShredError> {
    let proof_size = usize::from(proof_size);
    headers
        .checked_add(SIZE_OF_MERKLE_ROOT)
        .and_then(|v| v.checked_add(proof_size.checked_mul(SIZE_OF_MERKLE_PROOF_ENTRY)?))
        .and_then(|v| {
            if resigned {
                v.checked_add(SIZE_OF_SIGNATURE)
            } else {
                Some(v)
            }
        })
        .and_then(|v| DATA_SHRED_PAYLOAD_SIZE.checked_sub(v))
        .ok_or(ShredError::InvalidProofSize(proof_size as u8))
}

fn proof_offset(is_data: bool, proof_size: u8, resigned: bool) -> Result<usize, ShredError> {
    let headers = if is_data {
        SIZE_OF_DATA_SHRED_HEADERS
    } else {
        SIZE_OF_CODING_SHRED_HEADERS
    };
    let payload_capacity = if is_data {
        data_capacity(proof_size, resigned)?
    } else {
        code_capacity(proof_size, resigned)?
    };
    headers
        .checked_add(payload_capacity)
        .and_then(|v| v.checked_add(SIZE_OF_MERKLE_ROOT))
        .ok_or(ShredError::InvalidProofSize(proof_size))
}

fn chained_merkle_root_offset(
    is_data: bool,
    proof_size: u8,
    resigned: bool,
) -> Result<usize, ShredError> {
    let headers = if is_data {
        SIZE_OF_DATA_SHRED_HEADERS
    } else {
        SIZE_OF_CODING_SHRED_HEADERS
    };
    let payload_capacity = if is_data {
        data_capacity(proof_size, resigned)?
    } else {
        code_capacity(proof_size, resigned)?
    };
    headers
        .checked_add(payload_capacity)
        .ok_or(ShredError::InvalidProofSize(proof_size))
}

fn merkle_proof(
    shred: &[u8],
    proof_offset: usize,
    proof_size: u8,
) -> Result<Vec<[u8; SIZE_OF_MERKLE_PROOF_ENTRY]>, ShredError> {
    let proof_len = usize::from(proof_size) * SIZE_OF_MERKLE_PROOF_ENTRY;
    let proof_slice = shred
        .get(proof_offset..proof_offset + proof_len)
        .ok_or(ShredError::InvalidPayload)?;
    if proof_slice.len() != proof_len
        || !proof_slice.len().is_multiple_of(SIZE_OF_MERKLE_PROOF_ENTRY)
    {
        return Err(ShredError::InvalidMerkleProof);
    }
    proof_slice
        .chunks_exact(SIZE_OF_MERKLE_PROOF_ENTRY)
        .map(|chunk| <[u8; SIZE_OF_MERKLE_PROOF_ENTRY]>::try_from(chunk))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ShredError::InvalidMerkleProof)
}

fn merkle_node(bytes: &[u8]) -> Result<[u8; SIZE_OF_MERKLE_ROOT], ShredError> {
    let mut hasher = Sha256::new();
    hasher.update(MERKLE_HASH_PREFIX_LEAF);
    hasher.update(bytes);
    let digest = hasher.finalize();
    <[u8; SIZE_OF_MERKLE_ROOT]>::try_from(digest.as_slice()).map_err(|_| ShredError::InvalidPayload)
}

fn merkle_root(
    mut index: usize,
    mut node: [u8; SIZE_OF_MERKLE_ROOT],
    proof: &[[u8; SIZE_OF_MERKLE_PROOF_ENTRY]],
) -> Result<[u8; SIZE_OF_MERKLE_ROOT], ShredError> {
    for sibling in proof {
        node = if index % 2 == 0 {
            hash_nodes(&node, sibling)
        } else {
            hash_nodes(sibling, &node)
        };
        index >>= 1;
    }
    (index == 0)
        .then_some(node)
        .ok_or(ShredError::InvalidMerkleProof)
}

fn hash_nodes<L: AsRef<[u8]>, R: AsRef<[u8]>>(left: L, right: R) -> [u8; SIZE_OF_MERKLE_ROOT] {
    let mut hasher = Sha256::new();
    hasher.update(MERKLE_HASH_PREFIX_NODE);
    hasher.update(&left.as_ref()[..SIZE_OF_MERKLE_PROOF_ENTRY]);
    hasher.update(&right.as_ref()[..SIZE_OF_MERKLE_PROOF_ENTRY]);
    let digest = hasher.finalize();
    <[u8; SIZE_OF_MERKLE_ROOT]>::try_from(digest.as_slice()).unwrap()
}

pub fn get_data_index(payload: &[u8]) -> Option<u32> {
    parse_shred_header(payload).map(|header| header.index)
}

const ZERO_SIGNATURE: [u8; SIZE_OF_SIGNATURE] = [0u8; SIZE_OF_SIGNATURE];

const _: usize = SIZE_OF_NONCE;
const _: usize = SIZE_OF_COMMON_SHRED_HEADER;
