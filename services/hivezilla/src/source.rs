use anyhow::Result;
use blockzilla_format::{LiveArchiveSource, LiveBlockDraft};

pub trait LiveBlockSource {
    fn source(&self) -> LiveArchiveSource;
    fn next_block(&mut self) -> Result<Option<LiveBlockDraft>>;
}

#[derive(Debug)]
pub struct PendingSource {
    source: LiveArchiveSource,
}

impl PendingSource {
    pub fn new(source: LiveArchiveSource) -> Self {
        Self { source }
    }
}

impl LiveBlockSource for PendingSource {
    fn source(&self) -> LiveArchiveSource {
        self.source
    }

    fn next_block(&mut self) -> Result<Option<LiveBlockDraft>> {
        anyhow::bail!(
            "live source adapter {:?} is not implemented yet",
            self.source
        )
    }
}
