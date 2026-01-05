use std::{fs::File, io::BufReader, path::Path};

use crate::{
    CarBlockReader,
    car_block_group::CarBlockGroup,
    error::{CarReadError as CarError, CarReadResult as Result},
};

const CAR_BUF: usize = 128 << 20;

pub struct CarStream<R: std::io::Read> {
    car: CarBlockReader<R>,
    group: CarBlockGroup,
}

impl<R:std::io::Read> CarStream<R> {
    #[inline(always)]
    pub fn next_group(&mut self) -> Result<Option<&CarBlockGroup>> {
        if self.car.read_until_block_into(&mut self.group).is_ok() {
            Ok(Some(&self.group))
        } else {
            Ok(None)
        }
    }
}

impl CarStream<BufReader<File>> {
    pub fn open(path: &Path) -> Result<Self> {
        let file =
            File::open(path).map_err(|e| CarError::Io(format!("open {}: {e}", path.display())))?;
        let file = BufReader::with_capacity(CAR_BUF, file);
        let mut car = CarBlockReader::with_capacity(file, CAR_BUF);
        car.skip_header()?;

        Ok(Self {
            car,
            group: CarBlockGroup::new(),
        })
    }
}

impl CarStream<zstd::Decoder<'static, BufReader<File>>> {
    pub fn open_zstd(path: &Path) -> Result<Self> {
        let file =
            File::open(path).map_err(|e| CarError::Io(format!("open {}: {e}", path.display())))?;
        let file = BufReader::with_capacity(CAR_BUF, file);

        let zstd = zstd::Decoder::with_buffer(file)
            .map_err(|e| CarError::InvalidData(format!("zstd decoder init failed: {e}")))?;

        let mut car = CarBlockReader::with_capacity(zstd, CAR_BUF);
        car.skip_header()?;

        Ok(Self {
            car,
            group: CarBlockGroup::new(),
        })
    }
}
