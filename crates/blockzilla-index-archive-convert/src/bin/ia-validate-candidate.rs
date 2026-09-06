//! Validate the complete physical layout of an unpublished candidate.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use blockzilla_index_archive_convert::candidate::validate_complete_candidate;

fn usage() -> &'static str {
    "usage: ia-validate-candidate <candidate-directory> <epoch>"
}

fn main() -> Result<()> {
    let mut arguments = std::env::args_os().skip(1);
    let root = PathBuf::from(arguments.next().context(usage())?);
    let epoch: u64 = arguments
        .next()
        .context(usage())?
        .to_str()
        .context("epoch is not valid UTF-8")?
        .parse()
        .context("epoch must be an unsigned integer")?;
    if arguments.next().is_some() {
        bail!(usage());
    }

    let result = validate_complete_candidate(&root, epoch)?;
    println!("archive_id={}", result.archive_id.to_hex());
    println!("required_objects={}", result.required_objects);
    println!("physical_layout=valid");
    println!("publication_ready=false");
    Ok(())
}
