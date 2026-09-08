use std::fmt;

use blockzilla_model::ArchiveIoSnapshot;

/// Format cumulative setup/total counters and their scan interval for a reader
/// summary. GET counts include retries and sidecar downloads. They are not
/// logical read counts or counts of unique ranges. This adds no per-block work.
pub fn transport_metrics(setup: ArchiveIoSnapshot, total: ArchiveIoSnapshot) -> impl fmt::Display {
    TransportMetrics { setup, total }
}

struct TransportMetrics {
    setup: ArchiveIoSnapshot,
    total: ArchiveIoSnapshot,
}

impl fmt::Display for TransportMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let scan = self.total.saturating_sub(self.setup);
        let mut separator = "";
        for (phase, io) in [("setup", self.setup), ("scan", scan), ("total", self.total)] {
            for (name, value) in [
                ("head_requests", io.head_requests),
                ("get_requests", io.get_requests),
                ("incomplete_body_retries", io.incomplete_body_retries),
                ("server_error_retries", io.server_error_retries),
                ("cache_hits", io.cache_hits),
                ("cache_downloads", io.cache_downloads),
                ("cache_read_calls", io.cache_read_calls),
            ] {
                write!(f, "{separator}{phase}_{name}={value}")?;
                separator = " ";
            }
        }
        Ok(())
    }
}
