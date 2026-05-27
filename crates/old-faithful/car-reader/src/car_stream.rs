use std::{fs::File, io::BufReader, path::Path};

use crate::{
    CarBlockReader,
    car_block_group::CarBlockGroup,
    error::{CarReadError as CarError, CarReadResult as Result},
};

const CAR_BUF: usize = 128 << 20;

/// Convenience stream that yields one reusable [`CarBlockGroup`] per block.
///
/// `CarStream` handles the CAR header and repeatedly fills an internal
/// [`CarBlockGroup`]. It is the easiest API when you want to process a complete
/// archive block by block.
pub struct CarStream<R: std::io::Read> {
    car: CarBlockReader<R>,
    group: CarBlockGroup,
}

impl<R: std::io::Read> CarStream<R> {
    /// Build a stream from any reader containing an uncompressed CAR archive.
    ///
    /// The CAR header is read during construction. If your data is zstd
    /// compressed, wrap the reader in a decoder first or use
    /// [`CarStream::open_zstd`] for files when the `zstd-native` feature is
    /// enabled.
    pub fn from_reader(reader: R) -> Result<Self> {
        let mut car = CarBlockReader::with_capacity(reader, CAR_BUF);
        car.skip_header()?;

        Ok(Self {
            car,
            group: CarBlockGroup::new(),
        })
    }

    /// Read the next block into the stream's internal reusable group.
    ///
    /// Returns `Ok(None)` at clean EOF. Any references obtained from the
    /// returned group are invalidated by the next call to this method.
    #[inline(always)]
    pub fn next_group(&mut self) -> Result<Option<&mut CarBlockGroup>> {
        match self.car.read_until_block_into(&mut self.group) {
            Ok(true) => Ok(Some(&mut self.group)),
            Ok(false) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Read the next block into a caller-owned reusable group.
    ///
    /// Returns `Ok(false)` at clean EOF. This is useful when you want to choose
    /// a memory profile with constructors such as
    /// [`CarBlockGroup::without_rewards`] or
    /// [`CarBlockGroup::with_transaction_signature_prefixes`].
    #[inline(always)]
    pub fn next_group_into(&mut self, group: &mut CarBlockGroup) -> Result<bool> {
        self.car.read_until_block_into(group)
    }
}

impl CarStream<BufReader<File>> {
    /// Open an uncompressed `.car` file and stream it block by block.
    pub fn open(path: &Path) -> Result<Self> {
        let file =
            File::open(path).map_err(|e| CarError::Io(format!("open {}: {e}", path.display())))?;
        let file = BufReader::with_capacity(CAR_BUF, file);

        Self::from_reader(file)
    }
}

#[cfg(feature = "zstd-native")]
impl CarStream<zstd::Decoder<'static, BufReader<File>>> {
    /// Open a `.car.zst` file and stream it block by block.
    ///
    /// Requires the `zstd-native` feature, which is enabled by default.
    pub fn open_zstd(path: &Path) -> Result<Self> {
        let file =
            File::open(path).map_err(|e| CarError::Io(format!("open {}: {e}", path.display())))?;
        let file = BufReader::with_capacity(CAR_BUF, file);
        let zstd = zstd::Decoder::with_buffer(file)
            .map_err(|e| CarError::InvalidData(format!("zstd decoder init failed ({e})")))?;

        Self::from_reader(zstd)
    }
}
