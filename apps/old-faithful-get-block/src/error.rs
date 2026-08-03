use worker::Error;

pub(crate) const MAX_CAR_BLOCK_BYTES: u32 = 64 * 1024 * 1024;

pub type SourceResult<T> = std::result::Result<T, SourceError>;

#[derive(Debug)]
pub enum SourceError {
    SourceStatus {
        path: String,
        range: Option<String>,
        status: u16,
    },
    SourceBody {
        path: String,
        reason: String,
    },
    RangeOverflow {
        path: String,
        offset: u64,
        len: usize,
    },
    Worker(Error),
}

impl SourceError {
    pub fn client_message(&self) -> String {
        match self {
            Self::SourceStatus {
                path,
                range,
                status,
            } => match range {
                Some(range) => format!("{path} returned HTTP {status} for {range}"),
                None => format!("{path} returned HTTP {status}"),
            },
            Self::SourceBody { path, reason } => {
                format!("{path} returned an invalid body: {reason}")
            }
            Self::RangeOverflow { path, offset, len } => {
                format!("range offset {offset} plus length {len} overflows for {path}")
            }
            Self::Worker(err) => err.to_string(),
        }
    }
}

impl From<Error> for SourceError {
    fn from(err: Error) -> Self {
        Self::Worker(err)
    }
}

pub(crate) type FetchResult<T> = std::result::Result<T, FetchError>;

#[derive(Debug)]
pub(crate) enum FetchError {
    IndexUnavailable { keys: Vec<String> },
    MissingSlotIndex { key: String },
    PreviousBlockhashUnavailable { slot: u64 },
    MalformedSlotIndexEntry { key: String, reason: String },
    RangeTooLarge { slot: u64, len: u32 },
    MalformedCarSlice { reason: String },
    RewardLookup { reason: String },
    Source(SourceError),
    Worker(Error),
}

impl FetchError {
    pub(crate) fn status_code(&self) -> u16 {
        match self {
            Self::IndexUnavailable { .. } | Self::PreviousBlockhashUnavailable { .. } => 503,
            Self::MissingSlotIndex { .. } => 404,
            Self::MalformedSlotIndexEntry { .. }
            | Self::RangeTooLarge { .. }
            | Self::MalformedCarSlice { .. }
            | Self::RewardLookup { .. }
            | Self::Worker(_) => 500,
            Self::Source(SourceError::SourceStatus { .. } | SourceError::SourceBody { .. }) => 502,
            Self::Source(SourceError::RangeOverflow { .. } | SourceError::Worker(_)) => 500,
        }
    }

    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::IndexUnavailable { .. } => "index_unavailable",
            Self::MissingSlotIndex { .. } => "slot_index_missing",
            Self::PreviousBlockhashUnavailable { .. } => "previous_blockhash_unavailable",
            Self::MalformedSlotIndexEntry { .. } => "slot_index_malformed",
            Self::RangeTooLarge { .. } => "range_too_large",
            Self::MalformedCarSlice { .. } => "car_slice_malformed",
            Self::RewardLookup { .. } => "reward_lookup_failed",
            Self::Source(SourceError::SourceStatus { .. }) => "source_status",
            Self::Source(SourceError::SourceBody { .. }) => "source_body",
            Self::Source(SourceError::RangeOverflow { .. }) => "range_overflow",
            Self::Source(SourceError::Worker(_)) => "worker_error",
            Self::Worker(_) => "worker_error",
        }
    }

    pub(crate) fn client_message(&self) -> String {
        match self {
            Self::IndexUnavailable { keys } => {
                format!(
                    "compact index is missing from the serving index store: {}",
                    keys.join(", ")
                )
            }
            Self::MissingSlotIndex { key } => {
                format!("{key} is not available in the slot index source")
            }
            Self::PreviousBlockhashUnavailable { slot } => format!(
                "slot {slot} has no verified previousBlockhash; populate and validate the v2 slot-range index"
            ),
            Self::MalformedSlotIndexEntry { key, reason } => {
                format!("{key} has an invalid slot-range entry: {reason}")
            }
            Self::RangeTooLarge { slot, len } => {
                format!(
                    "slot {slot} CAR range is {len} bytes, above the {MAX_CAR_BLOCK_BYTES} byte limit"
                )
            }
            Self::MalformedCarSlice { reason } => {
                format!("slot CAR slice is malformed: {reason}")
            }
            Self::RewardLookup { reason } => {
                format!("failed to fetch block rewards by CID: {reason}")
            }
            Self::Source(err) => err.client_message(),
            Self::Worker(err) => err.to_string(),
        }
    }
}

impl From<Error> for FetchError {
    fn from(err: Error) -> Self {
        Self::Worker(err)
    }
}

impl From<SourceError> for FetchError {
    fn from(err: SourceError) -> Self {
        Self::Source(err)
    }
}
