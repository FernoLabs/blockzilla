//! Prepare pinned validator tools and a private local canary configuration.
//!
//! This command does not create an identity, start a service, or change kernel settings.

use anyhow::{Context, Result, bail, ensure};
use futures::StreamExt;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::unix::{
        fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
        process::CommandExt,
    },
    path::{Component, Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};

const AGAVE: &str = "v4.3.0";
const YELLOWSTONE: &str = "v16.0.0+solana.4.3.0";
const GENESIS: &str = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d";
const ARCHIVE: &str = "solana-release-x86_64-unknown-linux-gnu.tar.bz2";
const PLUGIN_ASSETS: [&str; 5] = [
    "libyellowstone_grpc_geyser.so",
    "config-check-linux-amd64",
    "client-linux-amd64",
    "geyser.proto",
    "solana-storage.proto",
];
const KNOWN_VALIDATORS: [&str; 4] = [
    "7Np41oeYqPefeNQEHSv1UDhYrehxin3NStELsSKCT4K2",
    "GdnSyH3YtwcxFvQrVVJMm1JhTS4QVX7MFsX56uJLUfiZ",
    "DE1bawNcRJB9rVm3buyMVfr8mBEoyyu73NBovf2oXJsJ",
    "CakcnaRDHka2gXyfbEd2d3xsvkJkqsLw2akB3zsN1D2S",
];
const MAX_JSON_BYTES: u64 = 8 * 1024 * 1024;
const MAX_ASSET_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_TOOL_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Debug, clap::Args)]
pub struct PrepareValidatorCanaryArgs {
    /// Private directory for pinned tools, configuration, and local canary data.
    #[arg(long)]
    pub root: PathBuf,
}

#[derive(Debug, Deserialize)]
struct Release {
    tag_name: String,
    prerelease: bool,
    #[serde(default)]
    draft: bool,
    assets: Vec<Asset>,
}

#[derive(Clone, Debug, Deserialize)]
struct Asset {
    name: String,
    size: u64,
    digest: Option<String>,
    browser_download_url: String,
}

impl Asset {
    fn digest(&self) -> Result<&str> {
        let digest = self
            .digest
            .as_deref()
            .and_then(|s| s.strip_prefix("sha256:"))
            .context("release asset has no SHA-256 digest")?;
        ensure!(
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "release asset has an invalid SHA-256 digest"
        );
        ensure!(
            self.size > 0 && self.size <= MAX_ASSET_BYTES,
            "release asset exceeds the size bound"
        );
        Ok(digest)
    }

    fn receipt(&self) -> Result<Value> {
        Ok(json!({"name": self.name, "sha256": self.digest()?, "bytes": self.size}))
    }
}

fn private_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) => ensure!(
            meta.is_dir() && meta.uid() == unsafe { libc::geteuid() } && meta.mode() & 0o077 == 0,
            "{} must be a private directory owned by this user",
            path.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::DirBuilder::new().mode(0o700).create(path)?;
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn root_directory(path: &Path) -> Result<PathBuf> {
    let path = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    ensure!(path.to_str().is_some(), "root must be valid UTF-8");
    ensure!(
        path.components()
            .all(|c| matches!(c, Component::RootDir | Component::Normal(_))),
        "root must not contain dot or parent components"
    );
    if !path.exists() {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&path)?;
    }
    private_directory(&path)?;
    ensure!(
        fs::canonicalize(&path)? == path,
        "root must not traverse symbolic links"
    );
    Ok(path)
}

fn read_file(path: &Path, maximum: u64) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let meta = file.metadata()?;
    ensure!(
        meta.is_file() && meta.uid() == unsafe { libc::geteuid() } && meta.mode() & 0o022 == 0,
        "{} must be a regular file owned by this user without shared write access",
        path.display()
    );
    ensure!(
        meta.len() <= maximum,
        "{} exceeds its size bound",
        path.display()
    );
    Ok(file)
}

fn hash_reader(mut file: File) -> Result<(u64, String)> {
    file.seek(SeekFrom::Start(0))?;
    let before = file.metadata()?;
    let mut hash = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = [0u8; 1024 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        bytes = bytes
            .checked_add(count as u64)
            .context("file size overflow")?;
        ensure!(bytes <= before.len(), "file grew while hashing");
        hash.update(&buffer[..count]);
    }
    let after = file.metadata()?;
    ensure!(
        bytes == before.len()
            && before.len() == after.len()
            && before.mtime() == after.mtime()
            && before.mtime_nsec() == after.mtime_nsec()
            && before.ctime() == after.ctime()
            && before.ctime_nsec() == after.ctime_nsec(),
        "file changed while hashing"
    );
    Ok((
        bytes,
        hash.finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
    ))
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all().map_err(Into::into)
}

fn read_json(path: &Path) -> Result<Value> {
    Ok(serde_json::from_reader(read_file(path, MAX_JSON_BYTES)?)?)
}

fn publish_json(path: &Path, value: &Value, replace_report: bool) -> Result<()> {
    if fs::symlink_metadata(path).is_ok() && !replace_report {
        ensure!(
            read_json(path)? == *value,
            "existing {} differs; it was not replaced",
            path.display()
        );
        return Ok(());
    }
    let parent = path.parent().context("JSON path has no parent")?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary
        .as_file()
        .set_permissions(fs::Permissions::from_mode(0o600))?;
    serde_json::to_writer_pretty(&mut temporary, value)?;
    temporary.write_all(b"\n")?;
    temporary.as_file().sync_all()?;
    if replace_report {
        temporary.persist(path).map_err(|e| e.error)?;
    } else {
        match temporary.persist_noclobber(path) {
            Ok(_) => (),
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                ensure!(
                    read_json(path)? == *value,
                    "concurrent configuration differs"
                );
            }
            Err(error) => return Err(error.error.into()),
        }
    }
    sync_directory(parent)
}

fn select_asset(release: &Release, repo: &str, tag: &str, name: &str) -> Result<Asset> {
    ensure!(
        release.tag_name == tag && !release.prerelease && !release.draft,
        "release is not the pinned stable release"
    );
    let matches: Vec<_> = release
        .assets
        .iter()
        .filter(|asset| asset.name == name)
        .collect();
    ensure!(
        matches.len() == 1,
        "release must contain exactly one {name}"
    );
    let asset = matches[0];
    asset.digest()?;
    let url = reqwest::Url::parse(&asset.browser_download_url)?;
    ensure!(
        url.scheme() == "https"
            && url.host_str() == Some("github.com")
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && url.port().is_none(),
        "asset URL is not an official GitHub release URL"
    );
    let expected = format!("https://github.com/{repo}/releases/download/{tag}/{name}");
    let encoded = expected.replace('+', "%2B");
    ensure!(
        url.as_str() == expected || url.as_str() == encoded,
        "asset URL differs from the pinned release path"
    );
    Ok(asset.clone())
}

fn release_url(repo: &str, tag: &str) -> Result<reqwest::Url> {
    let mut url = reqwest::Url::parse(&format!(
        "https://api.github.com/repos/{repo}/releases/tags/"
    ))?;
    url.path_segments_mut()
        .map_err(|_| anyhow::anyhow!("invalid release URL"))?
        .pop_if_empty()
        .push(tag);
    Ok(url)
}

async fn fetch_release(client: &reqwest::Client, repo: &str, tag: &str) -> Result<Release> {
    let url = release_url(repo, tag)?;
    let response = client.get(url).send().await?.error_for_status()?;
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        ensure!(
            body.len() as u64 + chunk.len() as u64 <= MAX_JSON_BYTES,
            "release metadata exceeds its size bound"
        );
        body.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&body)?)
}

async fn download_asset(
    client: &reqwest::Client,
    asset: &Asset,
    directory: &Path,
) -> Result<PathBuf> {
    let expected = asset.digest()?;
    let output = directory.join(&asset.name);
    if fs::symlink_metadata(&output).is_ok() {
        let (bytes, digest) = hash_reader(read_file(&output, MAX_ASSET_BYTES)?)?;
        ensure!(
            bytes == asset.size && digest == expected,
            "existing release asset differs; it was not replaced"
        );
        return Ok(output);
    }
    let mut temporary = tempfile::NamedTempFile::new_in(directory)?;
    let mut stream = client
        .get(&asset.browser_download_url)
        .send()
        .await?
        .error_for_status()?
        .bytes_stream();
    let mut received = 0u64;
    let mut digest = Sha256::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        received = received
            .checked_add(chunk.len() as u64)
            .context("download size overflow")?;
        ensure!(
            received <= asset.size,
            "release asset exceeds the declared size"
        );
        digest.update(&chunk);
        temporary.write_all(&chunk)?;
    }
    ensure!(
        received == asset.size
            && digest
                .finalize()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
                == expected,
        "release asset failed size or SHA-256 verification"
    );
    temporary
        .as_file()
        .set_permissions(fs::Permissions::from_mode(
            if asset.name.ends_with("amd64") {
                0o700
            } else {
                0o600
            },
        ))?;
    temporary.as_file().sync_all()?;
    temporary.persist_noclobber(&output).map_err(|e| e.error)?;
    sync_directory(directory)?;
    Ok(output)
}

struct ProcessGroup(Option<u32>);
impl Drop for ProcessGroup {
    fn drop(&mut self) {
        if let Some(pid) = self.0 {
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
        }
    }
}

async fn checked_command(
    program: &Path,
    args: &[&std::ffi::OsStr],
    output: &File,
    maximum: u64,
    timeout: Duration,
) -> Result<()> {
    let error = tempfile::tempfile()?;
    let mut command = tokio::process::Command::new(program);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(output.try_clone()?)
        .stderr(error.try_clone()?)
        .kill_on_drop(true);
    command.as_std_mut().process_group(0);
    let mut child = command
        .spawn()
        .with_context(|| format!("cannot start {}", program.display()))?;
    let mut group = ProcessGroup(child.id());
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait()? {
            group.0 = None;
            ensure!(
                status.success(),
                "{} failed with {status}; child output is not printed",
                program.display()
            );
            ensure!(
                output.metadata()?.len() <= maximum && error.metadata()?.len() <= 64 * 1024,
                "child output exceeds its size bound"
            );
            return Ok(());
        }
        if started.elapsed() >= timeout
            || output.metadata()?.len() > maximum
            || error.metadata()?.len() > 64 * 1024
        {
            drop(group);
            let _ = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
            bail!("{} exceeded its time or output bound", program.display());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn extract_tool(archive: &Path, destination: &Path, tool: &str) -> Result<()> {
    ensure!(
        matches!(tool, "solana" | "solana-keygen"),
        "tool is outside the extraction allowlist"
    );
    let directory = destination.parent().context("tool path has no parent")?;
    let temporary = tempfile::NamedTempFile::new_in(directory)?;
    let member = format!("solana-release/bin/{tool}");
    checked_command(
        Path::new("tar"),
        &[
            "-xOjf".as_ref(),
            archive.as_os_str(),
            "--".as_ref(),
            member.as_ref(),
        ],
        temporary.as_file(),
        MAX_TOOL_BYTES,
        Duration::from_secs(180),
    )
    .await?;
    let mut magic = [0u8; 4];
    let mut source = temporary.reopen()?;
    source.read_exact(&mut magic)?;
    ensure!(
        &magic == b"\x7fELF",
        "release tool is not an ELF executable"
    );
    let expected = hash_reader(temporary.reopen()?)?;
    if fs::symlink_metadata(destination).is_ok() {
        ensure!(
            hash_reader(read_file(destination, MAX_TOOL_BYTES)?)? == expected,
            "existing release tool differs from the verified archive; it was not replaced"
        );
        return Ok(());
    }
    temporary
        .as_file()
        .set_permissions(fs::Permissions::from_mode(0o700))?;
    temporary.as_file().sync_all()?;
    temporary
        .persist_noclobber(destination)
        .map_err(|e| e.error)?;
    sync_directory(directory)
}

fn plugin_config(plugin_dir: &Path) -> Value {
    json!({
        "libpath": plugin_dir.join("libyellowstone_grpc_geyser.so"),
        "log": {"level": "info"}, "tokio": {"worker_threads": 8},
        "grpc": {
            "listen": [{"address": "127.0.0.1:10000"}],
            "compression": {"accept": ["gzip", "zstd"], "send": ["gzip", "zstd"]},
            "max_decoding_message_size": "134_217_728", "channel_capacity": "100_000",
            "replay_stored_slots": 512, "unary_concurrency_limit": 8, "unary_disabled": false,
        }, "prometheus": {"address": "127.0.0.1:8999"}
    })
}

fn validator_argv(root: &Path, validator: &Path) -> Value {
    let mut argv: Vec<String> = vec![validator.to_string_lossy().into_owned()];
    for value in [
        "--identity".into(),
        root.join("config/identity.json").display().to_string(),
        "--no-voting".into(),
        "--no-xdp".into(),
        "--ledger".into(),
        root.join("ledger").display().to_string(),
        "--accounts".into(),
        root.join("accounts").display().to_string(),
        "--snapshots".into(),
        root.join("snapshots").display().to_string(),
        "--rpc-bind-address".into(),
        "127.0.0.1".into(),
        "--rpc-port".into(),
        "8899".into(),
        "--private-rpc".into(),
        "--full-rpc-api".into(),
        "--enable-rpc-transaction-history".into(),
        "--enable-extended-tx-metadata-storage".into(),
        "--dynamic-port-range".into(),
        "8000-8030".into(),
        "--expected-genesis-hash".into(),
        GENESIS.into(),
        "--only-known-rpc".into(),
        "--limit-blockstore-size".into(),
        "100000000".into(),
        "--geyser-plugin-config".into(),
        root.join("config/yellowstone.json").display().to_string(),
        "--log".into(),
        "-".into(),
    ] {
        argv.push(value);
    }
    for validator in KNOWN_VALIDATORS {
        argv.extend(["--known-validator".into(), validator.into()]);
    }
    for number in 1..=5 {
        argv.extend([
            "--entrypoint".into(),
            format!(
                "entrypoint{}.mainnet-beta.solana.com:8001",
                if number == 1 {
                    String::new()
                } else {
                    number.to_string()
                }
            ),
        ]);
    }
    json!(argv)
}

fn validate_report_update(path: &Path, report: &Value) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => (),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    }
    let mut prior = read_json(path)?;
    let mut next = report.clone();
    prior
        .as_object_mut()
        .context("existing install report is not an object")?
        .remove("validator_ready");
    next.as_object_mut()
        .context("new install report is not an object")?
        .remove("validator_ready");
    // The Python oracle preserves the API asset order, which is not an identity.
    for value in [&mut prior, &mut next] {
        let assets = value
            .get_mut("assets")
            .and_then(Value::as_array_mut)
            .context("install assets are not an array")?;
        assets.sort_by(|left, right| left["name"].as_str().cmp(&right["name"].as_str()));
    }
    ensure!(
        prior == next,
        "existing install report differs from the pinned preparation; it was not replaced"
    );
    Ok(())
}

pub async fn run(args: PrepareValidatorCanaryArgs) -> Result<Value> {
    ensure!(
        cfg!(target_os = "linux") && cfg!(target_arch = "x86_64"),
        "preparation requires Linux x86_64"
    );
    let root = root_directory(&args.root)?;
    for name in [
        "downloads",
        "releases",
        "config",
        "ledger",
        "accounts",
        "snapshots",
        "captures",
        "reports",
    ] {
        private_directory(&root.join(name))?;
    }
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(root.join("reports/prepare.lock"))?;
    ensure!(
        lock.metadata()?.uid() == unsafe { libc::geteuid() } && lock.metadata()?.is_file(),
        "invalid preparation lock"
    );
    use std::os::fd::AsRawFd;
    ensure!(
        unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
        "another preparation command holds this root"
    );
    let client = reqwest::Client::builder()
        .user_agent("hivezilla-validator-canary")
        .timeout(Duration::from_secs(300))
        .connect_timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()?;
    let agave = fetch_release(&client, "anza-xyz/agave", AGAVE).await?;
    let yellowstone = fetch_release(&client, "rpcpool/yellowstone-grpc", YELLOWSTONE).await?;
    let archive_asset = select_asset(&agave, "anza-xyz/agave", AGAVE, ARCHIVE)?;
    let archive = download_asset(&client, &archive_asset, &root.join("downloads")).await?;
    let agave_dir = root.join("releases").join(AGAVE);
    private_directory(&agave_dir)?;
    let release_dir = agave_dir.join("solana-release");
    // The old oracle extracted these two directories with release permissions.
    for path in [&release_dir, &release_dir.join("bin")] {
        if fs::symlink_metadata(path).is_ok() {
            let meta = fs::symlink_metadata(path)?;
            ensure!(
                meta.is_dir()
                    && meta.uid() == unsafe { libc::geteuid() }
                    && meta.mode() & 0o022 == 0,
                "release directory is not owned and protected from shared writes"
            );
        } else {
            private_directory(path)?;
        }
    }
    let bin_dir = release_dir.join("bin");
    for tool in ["solana", "solana-keygen"] {
        extract_tool(&archive, &bin_dir.join(tool), tool).await?;
    }
    let plugin_dir = root.join("releases/yellowstone-16");
    private_directory(&plugin_dir)?;
    let mut assets = vec![archive_asset.receipt()?];
    for name in PLUGIN_ASSETS {
        let asset = select_asset(&yellowstone, "rpcpool/yellowstone-grpc", YELLOWSTONE, name)?;
        download_asset(&client, &asset, &plugin_dir).await?;
        assets.push(asset.receipt()?);
    }
    let config_path = root.join("config/yellowstone.json");
    publish_json(&config_path, &plugin_config(&plugin_dir), false)?;
    let version_output = tempfile::tempfile()?;
    checked_command(
        &bin_dir.join("solana"),
        &["--version".as_ref()],
        &version_output,
        64 * 1024,
        Duration::from_secs(30),
    )
    .await?;
    let mut version = String::new();
    let mut version_file = version_output;
    version_file.seek(SeekFrom::Start(0))?;
    version_file.read_to_string(&mut version)?;
    ensure!(
        version.split_whitespace().any(|part| part == "4.3.0"),
        "Solana tool version differs from the pinned release"
    );
    let check_output = tempfile::tempfile()?;
    checked_command(
        &plugin_dir.join("config-check-linux-amd64"),
        &["--config".as_ref(), config_path.as_os_str()],
        &check_output,
        64 * 1024,
        Duration::from_secs(30),
    )
    .await?;
    let packaged_validator = bin_dir.join("agave-validator");
    let validator = if packaged_validator.exists() {
        packaged_validator
    } else {
        root.join("releases/agave-source-v4.3.0/target/release/agave-validator")
    };
    let ready = match fs::symlink_metadata(&validator) {
        Ok(_) => {
            let file = read_file(&validator, MAX_TOOL_BYTES)?;
            ensure!(
                file.metadata()?.mode() & 0o100 != 0,
                "validator is not executable"
            );
            true
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(error.into()),
    };
    let report = json!({"schema": "hivezilla-validator-canary-install-v1", "root": root,
        "agave_tag": AGAVE, "yellowstone_tag": YELLOWSTONE, "genesis_hash": GENESIS,
        "validator_binary": validator, "validator_ready": ready, "assets": assets});
    let report_path = root.join("reports/install.json");
    validate_report_update(&report_path, &report)?;
    publish_json(
        &root.join("config/validator-argv.json"),
        &validator_argv(&root, &validator),
        false,
    )?;
    publish_json(&report_path, &report, true)?;
    Ok(
        json!({"prepared": true, "validator_ready": ready, "root": root, "plugin_config": config_path,
        "validator_started": false, "identity_created": false, "kernel_changed": false}),
    )
}

#[cfg(test)]
mod tests;
