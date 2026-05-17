use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct LookupResponse {
    pub found: bool,
    pub signature: String,
    pub slot: Option<u64>,
    pub offset: Option<u64>,
    pub size: Option<u32>,
    pub offset_kind: Option<String>,
    pub car_path: Option<String>,
    pub transaction: Option<TransactionView>,
    pub note: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TransactionView {
    pub slot: u64,
    pub index: Option<u64>,
    pub message_version: &'static str,
    pub signatures: Vec<String>,
    pub transaction_data_base64: String,
    pub metadata: Option<MetadataView>,
}

#[derive(Debug, Serialize)]
pub struct MetadataView {
    pub fee: u64,
    pub has_error: bool,
    pub log_count: usize,
    pub inner_instruction_count: usize,
}
