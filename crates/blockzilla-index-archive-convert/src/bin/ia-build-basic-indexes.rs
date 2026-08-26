//! Build the fixed-width slot index for one target candidate.

use std::{env, path::PathBuf};

use anyhow::{Context, Result, bail};
use blockzilla_index_archive_convert::basic_indexes::{
    BasicIndexBuildOptions, build_basic_indexes,
};

fn usage() -> &'static str {
    "usage: ia-build-basic-indexes <candidate-dir>"
}

fn main() -> Result<()> {
    let mut arguments = env::args_os().skip(1);
    let root = PathBuf::from(arguments.next().context(usage())?);
    if let Some(argument) = arguments.next() {
        let argument = argument
            .to_str()
            .with_context(|| format!("argument is not valid UTF-8; {}", usage()))?;
        match argument {
            "-h" | "--help" => {
                println!("{}", usage());
                return Ok(());
            }
            other => bail!("unknown argument {other:?}; {}", usage()),
        }
    }

    let report = build_basic_indexes(&root, BasicIndexBuildOptions)?;
    println!("archive_id={}", report.archive_id.to_hex());
    println!("blocks={}", report.blocks);
    println!("transactions={}", report.transactions);
    println!("slots_object_bytes={}", report.slots_object_bytes);
    Ok(())
}
