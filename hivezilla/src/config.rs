use std::path::PathBuf;

use blockzilla_format::LiveArchiveSource;
use clap::{Args, ValueEnum};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
pub struct ProducerConfig {
    #[arg(long, default_value = "blockzilla-live")]
    pub archive_dir: PathBuf,

    #[arg(long, value_enum, default_value_t = SourceKind::OwnGrpc)]
    pub source: SourceKind,

    #[arg(long)]
    pub endpoint: Option<String>,

    #[arg(long, default_value_t = 0)]
    pub source_rank: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
pub enum SourceKind {
    OwnGrpc,
    Fumarole,
    LaserStream,
    DoubleZeroShred,
    ShredStream,
    RpcGetBlock,
    TritonCar,
    LocalCar,
}

impl From<SourceKind> for LiveArchiveSource {
    fn from(value: SourceKind) -> Self {
        match value {
            SourceKind::OwnGrpc => Self::OwnGrpc,
            SourceKind::Fumarole => Self::Fumarole,
            SourceKind::LaserStream => Self::LaserStream,
            SourceKind::DoubleZeroShred => Self::DoubleZeroShred,
            SourceKind::ShredStream => Self::ShredStream,
            SourceKind::RpcGetBlock => Self::RpcGetBlock,
            SourceKind::TritonCar => Self::TritonCar,
            SourceKind::LocalCar => Self::LocalCar,
        }
    }
}
