pub use blockzilla_source::{RangeSource, SourceResult};
pub use blockzilla_source_local::{
    LocalRangeSource, OverlayRangeSource, PinnedLocalObjectIdentity, PinnedLocalRangeSource,
    PinnedLocalRangeSourceStats,
};
use std::io::{self, Read};

pub(crate) struct RangeSourceReader<'a, S: RangeSource> {
    source: &'a S,
    object: &'a str,
    position: u64,
    end: u64,
    chunk_size: usize,
    chunk: Vec<u8>,
    chunk_position: usize,
}

impl<'a, S: RangeSource> RangeSourceReader<'a, S> {
    pub(crate) fn new(source: &'a S, object: &'a str, size: u64, chunk_size: usize) -> Self {
        Self {
            source,
            object,
            position: 0,
            end: size,
            chunk_size: chunk_size.max(1),
            chunk: Vec::new(),
            chunk_position: 0,
        }
    }

    fn refill(&mut self) -> io::Result<bool> {
        if self.position == self.end {
            return Ok(false);
        }
        let remaining = self.end - self.position;
        let length = usize::try_from(remaining.min(self.chunk_size as u64))
            .expect("chunk length is bounded by usize");
        self.source
            .read_range_into(self.object, self.position, length, &mut self.chunk)
            .map_err(io::Error::other)?;
        if self.chunk.len() != length {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "source returned {} bytes for requested {} byte range",
                    self.chunk.len(),
                    length
                ),
            ));
        }
        self.position += length as u64;
        self.chunk_position = 0;
        Ok(true)
    }
}

impl<S: RangeSource> Read for RangeSourceReader<'_, S> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if self.chunk_position == self.chunk.len() && !self.refill()? {
            return Ok(0);
        }
        let available = &self.chunk[self.chunk_position..];
        let count = available.len().min(output.len());
        output[..count].copy_from_slice(&available[..count]);
        self.chunk_position += count;
        Ok(count)
    }
}
