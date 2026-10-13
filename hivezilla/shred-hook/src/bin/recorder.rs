use anyhow::{Context, Result, ensure};
use clap::Parser;
use hivezilla_shred_hook::{
    wire,
    writer::{self, Writer},
};
use hivezilla_shred_hook_abi::{END_CLEAN, END_KIND};
use serde_json::json;

fn private_directory(path: &Path) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    if path.exists() {
        let metadata = path.symlink_metadata()?;
        // SAFETY: geteuid has no memory preconditions.
        ensure!(
            metadata.is_dir()
                && !metadata.file_type().is_symlink()
                && metadata.uid() == unsafe { libc::geteuid() }
                && metadata.permissions().mode() & 0o077 == 0,
            "existing capture/socket directory must be private and owned by this user"
        );
    } else {
        fs::create_dir_all(path)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn notify_ready() -> Result<()> {
    use std::{
        ffi::OsStr,
        os::{
            linux::net::SocketAddrExt,
            unix::{
                ffi::OsStrExt,
                net::{SocketAddr, UnixDatagram},
            },
        },
    };
    if let Some(path) = std::env::var_os("NOTIFY_SOCKET") {
        let bytes = path.as_bytes();
        let address = if bytes.starts_with(b"@") {
            SocketAddr::from_abstract_name(&bytes[1..])?
        } else {
            SocketAddr::from_pathname(Path::new(OsStr::from_bytes(bytes)))?
        };
        UnixDatagram::unbound()?.send_to_addr(
            b"READY=1\nSTATUS=Waiting for validator shred hook",
            &address,
        )?;
    }
    Ok(())
}
#[cfg(not(target_os = "linux"))]
fn notify_ready() -> Result<()> {
    Ok(())
}
use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, Read},
    os::{
        fd::AsRawFd,
        unix::{
            fs::{OpenOptionsExt, PermissionsExt},
            net::{UnixListener, UnixStream},
        },
    },
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    socket: PathBuf,
    #[arg(long)]
    output_root: PathBuf,
    #[arg(long)]
    expected_executable: PathBuf,
    #[arg(long)]
    genesis_hash: String,
    #[arg(long)]
    validator_identity: String,
    #[arg(long,default_value_t=64*1024*1024)]
    segment_bytes: u64,
    #[arg(long,default_value_t=64*1024*1024*1024)]
    max_bytes: u64,
    #[arg(long,default_value_t=500*1024*1024*1024)]
    reserve_free_bytes: u64,
    /// Accept a single producer, then exit after it closes.
    #[arg(long)]
    once: bool,
}

// libc uses different statvfs field widths on the supported build hosts.
#[allow(clippy::unnecessary_cast)]
fn available(path: &Path) -> Result<u64> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes())?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: valid C path and writable statvfs storage.
    ensure!(
        unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } == 0,
        "statvfs failed"
    );
    // SAFETY: the successful call initialized the result.
    let stat = unsafe { stat.assume_init() };
    (stat.f_bavail as u64)
        .checked_mul(stat.f_frsize as u64)
        .context("filesystem size overflow")
}

fn total_bytes(path: &Path) -> Result<u64> {
    let mut total = 0u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let meta = entry.path().symlink_metadata()?;
        ensure!(
            !meta.file_type().is_symlink(),
            "symlink inside capture root"
        );
        total = total
            .checked_add(if meta.is_dir() {
                total_bytes(&entry.path())?
            } else {
                meta.len()
            })
            .context("capture size overflow")?;
    }
    Ok(total)
}

#[cfg(target_os = "linux")]
fn peer(stream: &UnixStream, expected: &Path) -> Result<u32> {
    let mut cred = std::mem::MaybeUninit::<libc::ucred>::uninit();
    let mut size = size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: valid socket and writable credential buffer and size.
    ensure!(
        unsafe {
            libc::getsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                cred.as_mut_ptr().cast(),
                &mut size,
            )
        } == 0,
        "cannot read producer credentials"
    );
    ensure!(
        size as usize == size_of::<libc::ucred>(),
        "invalid producer credentials"
    );
    // SAFETY: successful getsockopt initialized the exact-size ucred.
    let cred = unsafe { cred.assume_init() };
    // SAFETY: geteuid has no memory preconditions.
    ensure!(
        cred.uid == unsafe { libc::geteuid() },
        "producer UID differs from recorder UID"
    );
    ensure!(
        fs::read_link(format!("/proc/{}/exe", cred.pid))? == fs::canonicalize(expected)?,
        "unexpected producer executable"
    );
    Ok(cred.pid as u32)
}
#[cfg(not(target_os = "linux"))]
fn peer(_stream: &UnixStream, _expected: &Path) -> Result<u32> {
    anyhow::bail!("recorder requires Linux peer credentials")
}

fn capture(args: &Args, stream: UnixStream, pid: u32) -> Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    let mut random = [0u8; 16];
    File::open("/dev/urandom")?.read_exact(&mut random)?;
    let root = args.output_root.join(writer::hex(&random));
    let prior_bytes = total_bytes(&args.output_root)?;
    let budget = args
        .max_bytes
        .checked_sub(prior_bytes)
        .and_then(|budget| budget.checked_sub(64 * 1024))
        .context("capture root quota reached")?;
    ensure!(
        available(&args.output_root)? > args.reserve_free_bytes + args.segment_bytes,
        "filesystem reserve reached"
    );
    let mut journal = Writer::new(root, random, args.segment_bytes, budget)?;
    writer::atomic_json(
        &journal.root().join("SESSION.json"),
        &json!({
            "schema":"hivezilla-shred-hook-session-v1","capture_format":"hivezilla-shred-hook-ipc-v1",
            "capture_id":writer::hex(&random),"producer_pid":pid,"producer_executable":args.expected_executable,
            "genesis_hash":args.genesis_hash,"validator_identity":args.validator_identity,
            "source_boundary":"UDP receiver before fetch queue eviction, filtering, signature checks, deduplication, or resigning; accepted repair observations after nonce validation",
            "timestamp_boundary":"adapter observation time per received batch, not NIC arrival time",
            "global_order":"producer queue acceptance order; order across receive sockets is not physical network arrival order",
            "upstream_socket_loss_measured":false,"registered_hivesync_stream":false,"custody_authority":false,
            "raw_bytes_preserved":true,"max_packet_bytes":1232,"segment_bytes":args.segment_bytes,
            "max_root_bytes":args.max_bytes,"reserve_free_bytes":args.reserve_free_bytes
        }),
    )?;
    journal.commit("ACTIVE", "receiving")?;
    writer::atomic_json(
        &args.output_root.join("CURRENT.json"),
        &json!({"capture_id":writer::hex(&random),"path":journal.root(),"producer_pid":pid}),
    )?;
    let mut reader = BufReader::with_capacity(1024 * 1024, stream);
    let mut last_commit = Instant::now();
    let mut last_quota = Instant::now();
    let mut last_bytes = journal.bytes();
    let result = (|| -> Result<&'static str> {
        loop {
            if last_commit.elapsed() >= Duration::from_millis(100) {
                journal.commit("ACTIVE", "receiving")?;
                last_commit = Instant::now();
            }
            if last_quota.elapsed() >= Duration::from_secs(1) {
                ensure!(
                    available(&args.output_root)?
                        > args.reserve_free_bytes + 2 * args.segment_bytes,
                    "filesystem reserve reached"
                );
                last_quota = Instant::now();
            }
            if reader.buffer().is_empty() {
                let mut pollfd = libc::pollfd {
                    fd: reader.get_ref().as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                };
                // SAFETY: poll sees a valid one-element pollfd array for this call.
                let ready = unsafe { libc::poll(&mut pollfd, 1, 50) };
                if ready < 0 {
                    let err = std::io::Error::last_os_error();
                    if err.kind() == std::io::ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(err.into());
                }
                if ready == 0 {
                    continue;
                }
            }
            let Some(frame) = wire::read_frame(&mut reader)? else {
                anyhow::bail!("producer closed without a terminal frame");
            };
            let kind = frame[8];
            journal.append(frame)?;
            if journal.bytes() - last_bytes >= 4 * 1024 * 1024 {
                journal.commit("ACTIVE", "receiving")?;
                last_bytes = journal.bytes();
                last_commit = Instant::now();
            }
            if kind == END_KIND {
                ensure!(
                    journal.terminal_reason == Some(END_CLEAN),
                    "producer stopped capture, code {:?}",
                    journal.terminal_reason
                );
                break Ok("clean producer shutdown");
            }
        }
    })();
    match result {
        Ok(reason) => {
            journal.finish("CLOSED", reason)?;
            println!(
                "{}",
                serde_json::to_string(
                    &json!({"state":"CLOSED","capture":journal.root(),"counts":journal.counts})
                )?
            );
            Ok(())
        }
        Err(error) => {
            let reason = format!("{error:#}");
            journal.finish("FAILED", &reason)?;
            eprintln!(
                "{}",
                serde_json::to_string(
                    &json!({"state":"FAILED","capture":journal.root(),"reason":reason,"counts":journal.counts})
                )?
            );
            Err(error)
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        args.socket.is_absolute() && args.output_root.is_absolute(),
        "paths must be absolute"
    );
    ensure!(
        args.output_root != Path::new("/"),
        "output root cannot be /"
    );
    ensure!(
        args.genesis_hash.len() <= 128 && args.validator_identity.len() <= 128,
        "source identity label too long"
    );
    ensure!(
        args.max_bytes >= args.segment_bytes.saturating_add(64 * 1024),
        "quota must include segment and metadata reserve"
    );
    private_directory(&args.output_root)?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(args.output_root.join(".lock"))?;
    // SAFETY: valid open descriptor; nonblocking exclusive ownership is required.
    ensure!(
        unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
        "another recorder owns the output root"
    );
    ensure!(
        !args.socket.exists(),
        "socket already exists; check the previous recorder before removing it"
    );
    let parent = args.socket.parent().context("socket path has no parent")?;
    private_directory(parent)?;
    let listener = UnixListener::bind(&args.socket)?;
    fs::set_permissions(&args.socket, fs::Permissions::from_mode(0o600))?;
    notify_ready()?;
    println!(
        "{}",
        json!({"state":"LISTENING","socket":args.socket,"output_root":args.output_root})
    );
    for incoming in listener.incoming() {
        let stream = incoming?;
        let pid = match peer(&stream, &args.expected_executable) {
            Ok(pid) => pid,
            Err(error) => {
                eprintln!("rejected producer: {error:#}");
                continue;
            }
        };
        let result = capture(&args, stream, pid);
        if args.once {
            fs::remove_file(&args.socket)?;
            return result;
        }
        if let Err(error) = result {
            eprintln!("capture ended: {error:#}");
        }
    }
    Ok(())
}
