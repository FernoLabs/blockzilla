use anyhow::Result;
use tracing::info;

use crate::{
    config::ProducerConfig,
    layout::ProducerLayout,
    source::{LiveBlockSource, PendingSource},
};

#[derive(Debug, Clone)]
pub struct LiveProducerApp {
    config: ProducerConfig,
}

impl LiveProducerApp {
    pub fn new(config: ProducerConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &ProducerConfig {
        &self.config
    }

    pub fn init(&self) -> Result<ProducerLayout> {
        ProducerLayout::create(&self.config.archive_dir)
    }

    pub fn dry_run(&self) -> Result<ProducerLayout> {
        let layout = self.init()?;
        info!(
            source = ?self.config.source,
            endpoint = ?self.config.endpoint,
            archive_dir = %layout.archive_dir.display(),
            "live producer dry run complete"
        );
        Ok(layout)
    }

    pub fn run(&self) -> Result<()> {
        let layout = self.init()?;
        info!(
            source = ?self.config.source,
            endpoint = ?self.config.endpoint,
            archive_dir = %layout.archive_dir.display(),
            "live producer starting"
        );

        let mut source = PendingSource::new(self.config.source.into());
        source.next_block()?;
        Ok(())
    }
}
