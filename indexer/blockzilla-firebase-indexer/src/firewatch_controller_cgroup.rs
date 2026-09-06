use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Read,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Component, Path, PathBuf},
};

use anyhow::{Context, Result, ensure};

const CGROUP_V2_ROOT: &str = "/sys/fs/cgroup";
const PROC_SELF_CGROUP: &str = "/proc/self/cgroup";
const MAX_PROC_CGROUP_BYTES: u64 = 64 * 1024;
const MAX_SCALAR_BYTES: u64 = 128;
const MAX_MEMORY_STAT_BYTES: u64 = 64 * 1024;
const MAX_MEMORY_EVENTS_BYTES: u64 = 16 * 1024;
const MAX_MEMORY_PRESSURE_BYTES: u64 = 16 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CgroupMemoryEvents {
    pub(crate) high: u64,
    pub(crate) max: u64,
    pub(crate) oom: u64,
    pub(crate) oom_kill: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct CgroupMemorySnapshot {
    pub(crate) current_bytes: u64,
    pub(crate) high_bytes: Option<u64>,
    pub(crate) max_bytes: Option<u64>,
    pub(crate) anon_bytes: u64,
    pub(crate) file_bytes: u64,
    pub(crate) inactive_file_bytes: u64,
    pub(crate) pressure_some_avg10: Option<f64>,
    pub(crate) pressure_full_avg10: Option<f64>,
    pub(crate) swap_current_bytes: u64,
    pub(crate) events: CgroupMemoryEvents,
}

/// Resolve this process's unified cgroup-v2 directory.
///
/// The returned path is canonical and is guaranteed to remain below the
/// canonical `/sys/fs/cgroup` root. Hybrid or malformed `/proc/self/cgroup`
/// data is rejected instead of being guessed.
pub(crate) fn resolve_self_cgroup_v2() -> Result<PathBuf> {
    let text = read_bounded_utf8(Path::new(PROC_SELF_CGROUP), MAX_PROC_CGROUP_BYTES)
        .context("read unified process cgroup membership")?;
    let relative = parse_unified_cgroup(&text)?;
    let root = canonical_cgroup_root(Path::new(CGROUP_V2_ROOT))?;
    let candidate = root.join(relative.strip_prefix("/").expect("validated absolute path"));
    validate_cgroup_directory(&candidate, &root)
}

/// Read the memory-controller state for a canonical cgroup-v2 directory.
pub(crate) fn read_cgroup_memory(path: &Path) -> Result<CgroupMemorySnapshot> {
    let root = canonical_cgroup_root(Path::new(CGROUP_V2_ROOT))?;
    read_cgroup_memory_under_root(path, &root)
}

fn read_cgroup_memory_under_root(path: &Path, root: &Path) -> Result<CgroupMemorySnapshot> {
    let directory = validate_cgroup_directory(path, root)?;
    let current_bytes = read_scalar(&directory.join("memory.current"), false)?
        .expect("memory.current does not accept max");
    let high_bytes = read_scalar(&directory.join("memory.high"), true)?;
    let max_bytes = read_scalar(&directory.join("memory.max"), true)?;
    let swap_current_bytes = read_scalar(&directory.join("memory.swap.current"), false)?
        .expect("memory.swap.current does not accept max");

    let stat = parse_keyed_u64(
        &read_bounded_utf8(&directory.join("memory.stat"), MAX_MEMORY_STAT_BYTES)
            .context("read cgroup memory.stat")?,
        "memory.stat",
    )?;
    let anon_bytes = required_value(&stat, "anon", "memory.stat")?;
    let file_bytes = required_value(&stat, "file", "memory.stat")?;
    let inactive_file_bytes = required_value(&stat, "inactive_file", "memory.stat")?;

    let events = parse_memory_events(
        &read_bounded_utf8(
            &directory.join("memory.events.local"),
            MAX_MEMORY_EVENTS_BYTES,
        )
        .context("read cgroup memory.events.local")?,
    )?;

    let (pressure_some_avg10, pressure_full_avg10) = match read_optional_bounded_utf8(
        &directory.join("memory.pressure"),
        MAX_MEMORY_PRESSURE_BYTES,
    )? {
        Some(text) => parse_memory_pressure(&text)?,
        None => (None, None),
    };

    Ok(CgroupMemorySnapshot {
        current_bytes,
        high_bytes,
        max_bytes,
        anon_bytes,
        file_bytes,
        inactive_file_bytes,
        pressure_some_avg10,
        pressure_full_avg10,
        swap_current_bytes,
        events,
    })
}

fn canonical_cgroup_root(root: &Path) -> Result<PathBuf> {
    ensure!(root.is_absolute(), "cgroup-v2 root is not absolute");
    let canonical = fs::canonicalize(root)
        .with_context(|| format!("canonicalize cgroup-v2 root {}", root.display()))?;
    ensure!(
        canonical == root,
        "cgroup-v2 root {} is not canonical",
        root.display()
    );
    let metadata = fs::symlink_metadata(&canonical)
        .with_context(|| format!("inspect cgroup-v2 root {}", canonical.display()))?;
    ensure!(
        metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
        "cgroup-v2 root is not a real directory"
    );
    Ok(canonical)
}

fn validate_cgroup_directory(path: &Path, root: &Path) -> Result<PathBuf> {
    ensure!(path.is_absolute(), "cgroup-v2 path is not absolute");
    let canonical = fs::canonicalize(path)
        .with_context(|| format!("canonicalize cgroup-v2 path {}", path.display()))?;
    ensure!(
        canonical == path,
        "cgroup-v2 path {} is not canonical",
        path.display()
    );
    ensure!(
        canonical == root || canonical.starts_with(root),
        "cgroup-v2 path {} escapes {}",
        canonical.display(),
        root.display()
    );
    let metadata = fs::symlink_metadata(&canonical)
        .with_context(|| format!("inspect cgroup-v2 path {}", canonical.display()))?;
    ensure!(
        metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
        "cgroup-v2 path is not a real directory"
    );
    Ok(canonical)
}

fn parse_unified_cgroup(text: &str) -> Result<PathBuf> {
    let mut unified = None;
    for line in text.lines() {
        ensure!(!line.is_empty(), "empty /proc/self/cgroup line");
        let mut parts = line.splitn(3, ':');
        let hierarchy = parts.next().unwrap_or_default();
        let controllers = parts
            .next()
            .context("malformed /proc/self/cgroup controller field")?;
        let path = parts
            .next()
            .context("malformed /proc/self/cgroup path field")?;
        ensure!(
            !hierarchy.is_empty() && !path.is_empty(),
            "malformed /proc/self/cgroup entry"
        );
        if hierarchy == "0" && controllers.is_empty() {
            ensure!(unified.is_none(), "multiple unified cgroup-v2 entries");
            unified = Some(validate_kernel_cgroup_path(path)?);
        }
    }
    unified.context("no unified cgroup-v2 entry in /proc/self/cgroup")
}

fn validate_kernel_cgroup_path(value: &str) -> Result<PathBuf> {
    ensure!(
        value.starts_with('/'),
        "unified cgroup-v2 path is not absolute"
    );
    ensure!(
        !value.as_bytes().contains(&0),
        "unified cgroup-v2 path contains NUL"
    );
    let path = PathBuf::from(value);
    for component in path.components() {
        ensure!(
            matches!(component, Component::RootDir | Component::Normal(_)),
            "unified cgroup-v2 path contains a non-canonical component"
        );
    }
    Ok(path)
}

fn read_scalar(path: &Path, allow_max: bool) -> Result<Option<u64>> {
    let text = read_bounded_utf8(path, MAX_SCALAR_BYTES)
        .with_context(|| format!("read cgroup scalar {}", path.display()))?;
    parse_scalar(&text, allow_max)
        .with_context(|| format!("parse cgroup scalar {}", path.display()))
}

fn parse_scalar(text: &str, allow_max: bool) -> Result<Option<u64>> {
    let value = text.trim_ascii();
    ensure!(!value.is_empty(), "empty cgroup scalar");
    ensure!(
        !value.bytes().any(|byte| byte.is_ascii_whitespace()),
        "cgroup scalar contains multiple fields"
    );
    if value == "max" {
        ensure!(allow_max, "max is not valid for this cgroup scalar");
        return Ok(None);
    }
    ensure!(
        value.bytes().all(|byte| byte.is_ascii_digit()),
        "cgroup scalar is not an unsigned decimal integer"
    );
    Ok(Some(value.parse().context("cgroup scalar exceeds u64")?))
}

fn parse_keyed_u64(text: &str, label: &str) -> Result<BTreeMap<String, u64>> {
    let mut values = BTreeMap::new();
    for line in text.lines() {
        ensure!(!line.is_empty(), "{label} contains an empty line");
        let mut fields = line.split_ascii_whitespace();
        let key = fields.next().context("cgroup key is missing")?;
        let value = fields.next().context("cgroup value is missing")?;
        ensure!(fields.next().is_none(), "{label} line has extra fields");
        ensure!(
            !key.is_empty()
                && key
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
            "{label} contains an invalid key"
        );
        ensure!(
            value.bytes().all(|byte| byte.is_ascii_digit()),
            "{label} value for {key} is not an unsigned decimal integer"
        );
        let value = value
            .parse::<u64>()
            .with_context(|| format!("{label} value for {key} exceeds u64"))?;
        ensure!(
            values.insert(key.to_owned(), value).is_none(),
            "{label} contains duplicate key {key}"
        );
    }
    ensure!(!values.is_empty(), "{label} is empty");
    Ok(values)
}

fn required_value(values: &BTreeMap<String, u64>, key: &str, label: &str) -> Result<u64> {
    values
        .get(key)
        .copied()
        .with_context(|| format!("{label} is missing {key}"))
}

fn parse_memory_events(text: &str) -> Result<CgroupMemoryEvents> {
    let values = parse_keyed_u64(text, "memory.events.local")?;
    Ok(CgroupMemoryEvents {
        high: required_value(&values, "high", "memory.events.local")?,
        max: required_value(&values, "max", "memory.events.local")?,
        oom: required_value(&values, "oom", "memory.events.local")?,
        oom_kill: required_value(&values, "oom_kill", "memory.events.local")?,
    })
}

fn parse_memory_pressure(text: &str) -> Result<(Option<f64>, Option<f64>)> {
    let mut some = None;
    let mut full = None;
    for line in text.lines() {
        ensure!(!line.is_empty(), "memory.pressure contains an empty line");
        let mut fields = line.split_ascii_whitespace();
        let kind = fields.next().context("memory.pressure kind is missing")?;
        ensure!(
            matches!(kind, "some" | "full"),
            "memory.pressure contains an unknown pressure kind"
        );
        let mut avg10 = None;
        let mut field_count = 0usize;
        for field in fields {
            field_count = field_count
                .checked_add(1)
                .context("memory.pressure field count overflow")?;
            let (key, value) = field
                .split_once('=')
                .context("memory.pressure field has no value")?;
            ensure!(
                !key.is_empty() && !value.is_empty(),
                "invalid memory.pressure field"
            );
            if key == "avg10" {
                ensure!(avg10.is_none(), "duplicate memory.pressure avg10 field");
                let parsed = value
                    .parse::<f64>()
                    .context("memory.pressure avg10 is not a number")?;
                ensure!(
                    parsed.is_finite() && (0.0..=100.0).contains(&parsed),
                    "memory.pressure avg10 is outside 0..=100"
                );
                avg10 = Some(parsed);
            }
        }
        ensure!(field_count > 0, "memory.pressure line has no fields");
        let avg10 = avg10.context("memory.pressure line is missing avg10")?;
        match kind {
            "some" => {
                ensure!(
                    some.replace(avg10).is_none(),
                    "duplicate memory.pressure some line"
                )
            }
            "full" => {
                ensure!(
                    full.replace(avg10).is_none(),
                    "duplicate memory.pressure full line"
                )
            }
            _ => unreachable!(),
        }
    }
    ensure!(some.is_some() || full.is_some(), "memory.pressure is empty");
    Ok((some, full))
}

fn read_optional_bounded_utf8(path: &Path, limit: u64) -> Result<Option<String>> {
    match fs::symlink_metadata(path) {
        Ok(_) => read_bounded_utf8(path, limit).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => {
            Err(error).with_context(|| format!("inspect optional cgroup file {}", path.display()))
        }
    }
}

fn read_bounded_utf8(path: &Path, limit: u64) -> Result<String> {
    ensure!(limit < u64::MAX, "bounded read limit is invalid");
    let before = fs::symlink_metadata(path)
        .with_context(|| format!("inspect bounded cgroup file {}", path.display()))?;
    ensure!(
        before.file_type().is_file() && !before.file_type().is_symlink(),
        "bounded cgroup path {} is not a real file",
        path.display()
    );
    ensure!(
        before.len() <= limit,
        "bounded cgroup file {} exceeds {limit} bytes",
        path.display()
    );

    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .with_context(|| format!("open bounded cgroup file {}", path.display()))?;
    let opened = file
        .metadata()
        .with_context(|| format!("inspect opened cgroup file {}", path.display()))?;
    ensure!(
        before.dev() == opened.dev()
            && before.ino() == opened.ino()
            && opened.file_type().is_file(),
        "bounded cgroup file {} changed before open",
        path.display()
    );

    let mut bytes = Vec::new();
    file.by_ref()
        .take(limit + 1)
        .read_to_end(&mut bytes)
        .with_context(|| format!("read bounded cgroup file {}", path.display()))?;
    ensure!(
        bytes.len() as u64 <= limit,
        "bounded cgroup file {} exceeds {limit} bytes while reading",
        path.display()
    );
    String::from_utf8(bytes)
        .with_context(|| format!("bounded cgroup file {} is not UTF-8", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unified_cgroup_parser_accepts_one_cgroup_v2_entry() {
        assert_eq!(
            parse_unified_cgroup("11:memory:/legacy\n0::/user.slice/test.service\n7:cpu:/legacy\n")
                .unwrap(),
            PathBuf::from("/user.slice/test.service")
        );
    }

    #[test]
    fn unified_cgroup_parser_rejects_duplicates_and_traversal() {
        assert!(parse_unified_cgroup("0::/one\n0::/two\n").is_err());
        assert!(parse_unified_cgroup("0::/one/../two\n").is_err());
        assert!(parse_unified_cgroup("1:memory:/legacy\n").is_err());
    }

    #[test]
    fn scalar_parser_distinguishes_max_from_numbers() {
        assert_eq!(parse_scalar("123\n", false).unwrap(), Some(123));
        assert_eq!(parse_scalar("max\n", true).unwrap(), None);
        assert!(parse_scalar("max\n", false).is_err());
        assert!(parse_scalar("1 2\n", true).is_err());
        assert!(parse_scalar("-1\n", true).is_err());
    }

    #[test]
    fn pressure_parser_extracts_avg10_and_rejects_invalid_values() {
        assert_eq!(
            parse_memory_pressure(
                "some avg10=0.25 avg60=0.10 avg300=0.01 total=12\n\
                 full avg10=0.05 avg60=0.01 avg300=0.00 total=3\n"
            )
            .unwrap(),
            (Some(0.25), Some(0.05))
        );
        assert!(parse_memory_pressure("some avg60=0.10 total=12\n").is_err());
        assert!(parse_memory_pressure("some avg10=101 total=12\n").is_err());
    }

    #[test]
    fn reads_complete_snapshot_from_bounded_temp_files() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("cgroup");
        let group = root.join("test.service");
        fs::create_dir_all(&group).unwrap();
        fs::write(group.join("memory.current"), "1000\n").unwrap();
        fs::write(group.join("memory.high"), "max\n").unwrap();
        fs::write(group.join("memory.max"), "4096\n").unwrap();
        fs::write(group.join("memory.swap.current"), "17\n").unwrap();
        fs::write(
            group.join("memory.stat"),
            "anon 200\nfile 700\ninactive_file 300\nfuture_key 1\n",
        )
        .unwrap();
        fs::write(
            group.join("memory.events.local"),
            "low 0\nhigh 5\nmax 2\noom 1\noom_kill 0\noom_group_kill 0\n",
        )
        .unwrap();
        fs::write(
            group.join("memory.pressure"),
            "some avg10=0.20 avg60=0.10 avg300=0.00 total=10\n\
             full avg10=0.05 avg60=0.01 avg300=0.00 total=2\n",
        )
        .unwrap();

        let canonical_root = fs::canonicalize(root).unwrap();
        let canonical_group = fs::canonicalize(group).unwrap();
        let snapshot = read_cgroup_memory_under_root(&canonical_group, &canonical_root).unwrap();
        assert_eq!(
            snapshot,
            CgroupMemorySnapshot {
                current_bytes: 1000,
                high_bytes: None,
                max_bytes: Some(4096),
                anon_bytes: 200,
                file_bytes: 700,
                inactive_file_bytes: 300,
                pressure_some_avg10: Some(0.20),
                pressure_full_avg10: Some(0.05),
                swap_current_bytes: 17,
                events: CgroupMemoryEvents {
                    high: 5,
                    max: 2,
                    oom: 1,
                    oom_kill: 0,
                },
            }
        );
    }

    #[test]
    fn cgroup_directory_validation_rejects_symlink_escape() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("cgroup");
        let outside = temp.path().join("outside");
        fs::create_dir(&root).unwrap();
        fs::create_dir(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, root.join("escape")).unwrap();

        let canonical_root = fs::canonicalize(&root).unwrap();
        assert!(validate_cgroup_directory(&root.join("escape"), &canonical_root).is_err());
    }
}
