#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct KeyStats {
    pub id: u32,
    pub count: u32,
    pub first_fee_payer: u32,
    pub first_epoch: u16,
    pub last_epoch: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadMode {
    NoDownload,
    Stream,
    Cache,
}
