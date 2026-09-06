use std::{env, path::PathBuf};

use anyhow::{Context, Result};
use blockzilla_read_sdk::{ArchiveReader, PinnedLocalRangeSource};
use blockzilla_user_program_index::build::{DenseIndexBuildOptions, build_dense_index_from_reader};

fn main() -> Result<()> {
    let mut arguments = env::args_os().skip(1);
    let archive_root = PathBuf::from(
        arguments
            .next()
            .context("usage: build_from_reader <archive-root> <output-directory>")?,
    );
    let output = PathBuf::from(
        arguments
            .next()
            .context("usage: build_from_reader <archive-root> <output-directory>")?,
    );
    anyhow::ensure!(
        arguments.next().is_none(),
        "usage: build_from_reader <archive-root> <output-directory>"
    );

    let reader = ArchiveReader::open(PinnedLocalRangeSource::new(&archive_root))
        .context("open the published archive generation")?;
    build_dense_index_from_reader(
        &reader,
        &archive_root.join("registry.bin"),
        &archive_root.join("registry.mphf"),
        &output,
        DenseIndexBuildOptions::default(),
    )
}
