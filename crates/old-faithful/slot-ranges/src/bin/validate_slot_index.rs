use anyhow::{Context, Result, bail};
use of_car_reader::slot_ranges::{SLOT_RANGE_ENTRY_SIZE, SLOTS_PER_EPOCH};
use std::{env, ffi::OsStr, path::Path, process::ExitCode};

fn main() -> ExitCode {
    let mut arguments = env::args_os();
    let _program = arguments.next();
    let Some(root) = arguments.next() else {
        eprintln!("usage: of-validate-slot-index <directory>");
        return ExitCode::from(2);
    };
    if arguments.next().is_some() {
        eprintln!("usage: of-validate-slot-index <directory>");
        return ExitCode::from(2);
    }
    match validate(Path::new(&root)) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(2),
        Err(error) => {
            eprintln!("slot-index validation failed: {error:#}");
            ExitCode::from(1)
        }
    }
}

fn validate(root: &Path) -> Result<bool> {
    if !root.is_dir() {
        bail!("missing directory: {}", root.display());
    }
    let expected_size = u64::from(SLOTS_PER_EPOCH) * SLOT_RANGE_ENTRY_SIZE as u64;
    let mut valid = Vec::new();
    let mut bad = Vec::new();
    for entry in root
        .read_dir()
        .with_context(|| format!("read {}", root.display()))?
    {
        let entry = entry.with_context(|| format!("read entry in {}", root.display()))?;
        let Some(epoch) = epoch_from_name(&entry.file_name()) else {
            continue;
        };
        let metadata = entry
            .path()
            .symlink_metadata()
            .with_context(|| format!("inspect {}", entry.path().display()))?;
        if metadata.is_file() && metadata.len() == expected_size {
            valid.push(epoch);
        } else {
            bad.push((epoch, metadata.len()));
        }
    }
    valid.sort_unstable();
    bad.sort_unstable();
    let missing_between = match (valid.first().copied(), valid.last().copied()) {
        (Some(first), Some(last)) => (first..=last)
            .filter(|epoch| valid.binary_search(epoch).is_err())
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };

    println!("slot_index_dir={}", root.display());
    println!("valid_count={}", valid.len());
    match (valid.first(), valid.last()) {
        (Some(first), Some(last)) => println!("first_last=({first}, {last})"),
        _ => println!("first_last=None"),
    }
    println!("bad_count={}", bad.len());
    println!("missing_between_count={}", missing_between.len());
    if !bad.is_empty() {
        eprintln!("bad_files={:?}", &bad[..bad.len().min(20)]);
    }
    if !missing_between.is_empty() {
        eprintln!(
            "missing_between={:?}",
            &missing_between[..missing_between.len().min(50)]
        );
    }
    Ok(bad.is_empty())
}

fn epoch_from_name(name: &OsStr) -> Option<u64> {
    let name = name.to_str()?;
    let epoch = name
        .strip_prefix("epoch-")?
        .strip_suffix("-slot-ranges.raw")?;
    if epoch.is_empty() || !epoch.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    epoch.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_strict() {
        assert_eq!(
            epoch_from_name(OsStr::new("epoch-73-slot-ranges.raw")),
            Some(73)
        );
        for name in [
            "epoch--slot-ranges.raw",
            "epoch-7x-slot-ranges.raw",
            "epoch-7-slot-ranges-v2.raw",
            "prefix-epoch-7-slot-ranges.raw",
        ] {
            assert_eq!(epoch_from_name(OsStr::new(name)), None, "{name}");
        }
    }

    #[test]
    fn size_validation_rejects_bad_files_but_not_epoch_gaps() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let expected_size = SLOTS_PER_EPOCH * SLOT_RANGE_ENTRY_SIZE as u64;
        std::fs::File::create(temporary.path().join("epoch-7-slot-ranges.raw"))
            .expect("create valid index")
            .set_len(expected_size)
            .expect("size valid index");
        std::fs::File::create(temporary.path().join("epoch-9-slot-ranges.raw"))
            .expect("create second valid index")
            .set_len(expected_size)
            .expect("size second valid index");
        std::fs::write(temporary.path().join("epoch-8-slot-ranges.raw"), b"short")
            .expect("write bad index");
        assert!(!validate(temporary.path()).expect("bad validation result"));
        std::fs::remove_file(temporary.path().join("epoch-8-slot-ranges.raw"))
            .expect("remove bad fixture");
        assert!(validate(temporary.path()).expect("valid indexes with a gap"));
    }
}
