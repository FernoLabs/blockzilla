use crate::event::{Event, INTERFACE_VERSION, REVISION, SCHEMA, digest};
use anyhow::{Context, Result, bail, ensure};
use crossbeam_channel::{Sender, bounded};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Seek, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub bank_v2: bool,
    pub libpath: String,
    pub output_root: PathBuf,
    pub genesis_hash: String,
    pub node_identity: String,
    pub queue_events: usize,
    pub queue_bytes: u64,
    pub max_event_bytes: usize,
    pub max_journal_bytes: u64,
    pub reserve_disk_bytes: u64,
    pub commit_interval_ms: u64,
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.output_root.is_absolute(),
            "output_root must be absolute"
        );
        crate::event::hash(&self.genesis_hash)?;
        crate::event::hash(&self.node_identity)?;
        ensure!(
            (16..=65536).contains(&self.queue_events),
            "queue_events must be 16..65536"
        );
        ensure!(
            (4096..=16 * 1024 * 1024).contains(&self.max_event_bytes),
            "event limit must be 4 KiB..16 MiB"
        );
        ensure!(
            self.queue_bytes >= self.max_event_bytes as u64
                && self.queue_bytes <= 1024 * 1024 * 1024,
            "queue byte limit invalid"
        );
        ensure!(
            self.max_journal_bytes >= 1024 * 1024 && self.max_journal_bytes <= 16 * 1024u64.pow(4),
            "journal byte limit invalid"
        );
        ensure!(
            (1..=1000).contains(&self.commit_interval_ms),
            "commit interval must be 1..1000 ms"
        );
        Ok(())
    }
}
#[derive(Debug)]
struct Queued {
    slot: u64,
    bank: Option<u64>,
    kind: &'static str,
    bytes: Vec<u8>,
}
#[derive(Debug)]
struct Shared {
    failed: AtomicBool,
    queued: AtomicU64,
    accepted: AtomicU64,
    durable: AtomicU64,
    failure: Mutex<Option<String>>,
    // Reserved before capture. An in-place failure write needs no new allocation on the filesystem.
    failure_file: Mutex<File>,
}
impl Shared {
    fn fail(&self, message: String) {
        let mut state = self.failure.lock().unwrap_or_else(|p| p.into_inner());
        if state.is_some() {
            return;
        }
        self.failed.store(true, Ordering::Release);
        eprintln!("HIVEZILLA_CAPTURE_FAILED: {message}; source coverage is incomplete");
        *state = Some(message.clone());
        let bytes = serde_json::to_vec(&serde_json::json!({"state":"FAILED", "error":message,"accepted_events":self.accepted.load(Ordering::Acquire),"durable_sequence":self.durable.load(Ordering::Acquire),"coverage_complete":false})).unwrap_or_default();
        if let Ok(mut f) = self.failure_file.lock() {
            let mut reserved = [b' '; 4096];
            let n = bytes.len().min(4095);
            reserved[..n].copy_from_slice(&bytes[..n]);
            reserved[4095] = b'\n';
            if f.rewind()
                .and_then(|_| f.write_all(&reserved))
                .and_then(|_| f.sync_all())
                .is_err()
            {
                eprintln!("HIVEZILLA_CAPTURE_FAILURE_MARKER_WRITE_FAILED");
            }
        }
    }
}
#[derive(Debug)]
pub struct Journal {
    pub session_dir: PathBuf,
    pub session_id: String,
    _capture_lock: File,
    config: Config,
    sender: Option<Sender<Queued>>,
    shared: Arc<Shared>,
    writer: Option<JoinHandle<()>>,
}
impl Journal {
    pub fn start(config: Config) -> Result<Self> {
        config.validate()?;
        fs::create_dir_all(&config.output_root)?;
        let capture_lock = exclusive_lock(&config.output_root.join("CAPTURE.lock"))?;
        let previous_unclean = fs::read_dir(&config.output_root)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path().join("SESSION.json").is_file() && !e.path().join("CLOSED.json").exists()
            })
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let session_id = format!("{nanos:032x}-{:x}", std::process::id());
        let session_dir = config.output_root.join(&session_id);
        fs::create_dir(&session_dir)?;
        let session_lock = exclusive_lock(&session_dir.join("LIVE.lock"))?;
        fs::create_dir(session_dir.join("retire-pending"))?;
        let mut failure_file = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(session_dir.join("FAILURE.json"))?;
        let mut reserved = [b' '; 4096];
        reserved[..2].copy_from_slice(b"{}");
        reserved[4095] = b'\n';
        failure_file.write_all(&reserved)?;
        failure_file.sync_all()?;
        let session = serde_json::json!({"schema":SCHEMA,"payload_codec":"json-zstd-v1","retirement":"rooted-banks-v1","processed_custody":true,"bank_observations_v2":config.bank_v2,"agave_revision":REVISION,"interface_version":INTERFACE_VERSION,"session_id":session_id,"previous_unclean_sessions":previous_unclean,"config":config,"config_sha256":digest(&serde_json::to_vec(&config)?),"durability":"SQLite WAL synchronous=FULL; only committed rows are durable","unsealed_session_means_gap":true,"shreds":"separate receive hook; no completeness claim"});
        atomic_json(&session_dir.join("SESSION.json"), &session)?;
        let mut db = open_writer(&session_dir.join("events.sqlite"))?;
        db.execute_batch("CREATE TABLE events(sequence INTEGER PRIMARY KEY, slot INTEGER NOT NULL, bank INTEGER, kind TEXT NOT NULL, sha256 TEXT NOT NULL, payload BLOB NOT NULL); CREATE INDEX bank_events ON events(slot,bank,sequence); CREATE INDEX roots ON events(kind,slot); CREATE TABLE status(id INTEGER PRIMARY KEY CHECK(id=1),state TEXT NOT NULL,durable_sequence INTEGER NOT NULL,error TEXT,latest_rooted_slot INTEGER NOT NULL,retired_events INTEGER NOT NULL); INSERT INTO status VALUES(1,'CAPTURING',0,NULL,0,0); CREATE TABLE retire_jobs(id TEXT PRIMARY KEY,bank_index INTEGER NOT NULL,deleted_events INTEGER NOT NULL,marker_sha TEXT NOT NULL);")?;
        let (sender, receiver) = bounded::<Queued>(config.queue_events);
        let shared = Arc::new(Shared {
            failed: AtomicBool::new(false),
            queued: AtomicU64::new(0),
            accepted: AtomicU64::new(0),
            durable: AtomicU64::new(0),
            failure: Mutex::new(None),
            failure_file: Mutex::new(failure_file),
        });
        let writer_shared = Arc::clone(&shared);
        let writer_dir = session_dir.clone();
        let writer_config = config.clone();
        let writer_session = session_id.clone();
        let writer = thread::Builder::new()
            .name("hivezilla-journal".into())
            .spawn(move || {
                let _writer_live_lock = session_lock;
                let result = (|| -> Result<()> {
                    let mut sequence = 0u64;
                    let mut retirement_failed=false;
                    loop {
                        if let Some(error) = writer_shared
                            .failure
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .clone()
                        {
                            bail!("{error}");
                        }
                        let first = match receiver
                            .recv_timeout(Duration::from_millis(writer_config.commit_interval_ms))
                        {
                            Ok(v) => Some(v),
                            Err(crossbeam_channel::RecvTimeoutError::Timeout) => None,
                            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                        };
                        if let Some(first) = first {
                            let mut batch = vec![first];
                            let mut bytes = batch[0].bytes.len();
                            let deadline = std::time::Instant::now()
                                + Duration::from_millis(writer_config.commit_interval_ms);
                            while bytes < 4 * 1024 * 1024 && batch.len() < 2048 {
                                match receiver.recv_timeout(
                                    deadline.saturating_duration_since(std::time::Instant::now()),
                                ) {
                                    Ok(v) => {
                                        bytes += v.bytes.len();
                                        batch.push(v);
                                    }
                                    Err(_) => break,
                                }
                            }
                            check_space(&writer_dir, &writer_config, bytes as u64,&db)?;
                            let tx = db.transaction()?;
                            {
                                let mut insert = tx.prepare_cached(
                                    "INSERT INTO events VALUES(?1,?2,?3,?4,?5,?6)",
                                )?;
                                for event in &batch {
                                    sequence =
                                        sequence.checked_add(1).context("sequence overflow")?;
                                    insert.execute(params![
                                        sequence,
                                        event.slot,
                                        event.bank,
                                        event.kind,
                                        digest(&event.bytes),
                                        zstd::bulk::compress(&event.bytes, 1)?
                                    ])?;
                                }
                            }
                            tx.execute(
                                "UPDATE status SET durable_sequence=?1 WHERE id=1",
                                [sequence],
                            )?;
                            let root=batch.iter().filter(|e|e.kind=="rooted").map(|e|e.slot).max().unwrap_or(0);
                            tx.execute("UPDATE status SET latest_rooted_slot=max(latest_rooted_slot,?1) WHERE id=1",[root])?;
                            tx.commit()?; // FULL WAL commit completes before the durable watermark moves.
                            writer_shared.durable.store(sequence, Ordering::Release);
                            writer_shared
                                .queued
                                .fetch_sub(bytes as u64, Ordering::AcqRel);
                        }
                        // This is independent of the worker and uses PASSIVE, never a blocking checkpoint.
                        if !retirement_failed {
                            if let Err(error)=crate::retention::process_one(&writer_dir,&mut db,&writer_session){
                                retirement_failed=true;
                                eprintln!("HIVEZILLA_RETIREMENT_FAILED: {error:#}; capture continues and source records remain");
                                let _=atomic_json(&writer_dir.join("RETIREMENT-FAILED.json"),&serde_json::json!({"state":"FAILED","error":format!("{error:#}"),"capture_independent":true}));
                            }
                        }
                        db.execute_batch("PRAGMA wal_checkpoint(PASSIVE)")?;
                    }
                    if let Some(error) = writer_shared
                        .failure
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .clone()
                    {
                        bail!("{error}");
                    }
                    db.execute("UPDATE status SET state='CLOSED' WHERE id=1", [])?;
                    db.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
                    atomic_json(
                        &writer_dir.join("CLOSED.json"),
                        &serde_json::json!({"state":"CLOSED","durable_sequence":sequence}),
                    )?;
                    Ok(())
                })();
                if let Err(error) = result {
                    let message = format!("{error:#}");
                    writer_shared.fail(message.clone());
                    let _ = db.execute(
                        "UPDATE status SET state='FAILED',error=?1 WHERE id=1",
                        [message],
                    );
                }
            })?;
        Ok(Self {
            session_dir,
            session_id,
            _capture_lock: capture_lock,
            config,
            sender: Some(sender),
            shared,
            writer: Some(writer),
        })
    }
    pub fn is_failed(&self) -> bool {
        self.shared.failed.load(Ordering::Acquire)
    }
    pub fn append(&self, event: &Event) -> Result<()> {
        if let Some(error) = self
            .shared
            .failure
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
        {
            bail!("capture failed: {error}");
        }
        let bytes = serde_json::to_vec(event)?;
        if bytes.len() > self.config.max_event_bytes {
            return self.reject("event size exceeds capture bound");
        }
        let size = bytes.len() as u64;
        if self
            .shared
            .queued
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(size)
                    .filter(|total| *total <= self.config.queue_bytes)
            })
            .is_err()
        {
            return self.reject("callback queue byte limit reached");
        }
        let (slot, bank, kind) = event.key();
        if self
            .sender
            .as_ref()
            .context("journal closed")?
            .try_send(Queued {
                slot,
                bank,
                kind,
                bytes,
            })
            .is_err()
        {
            self.shared.queued.fetch_sub(size, Ordering::AcqRel);
            return self.reject("callback queue event limit reached or writer stopped");
        }
        self.shared.accepted.fetch_add(1, Ordering::Release);
        Ok(())
    }
    pub fn reject<T>(&self, message: &str) -> Result<T> {
        self.shared.fail(message.to_owned());
        bail!("{message}")
    }
    pub fn close(&mut self) -> Result<()> {
        self.sender.take();
        if let Some(writer) = self.writer.take() {
            if writer.join().is_err() {
                self.shared.fail("journal writer panicked".into());
            }
        }
        if let Some(error) = self
            .shared
            .failure
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
        {
            bail!("{error}");
        }
        Ok(())
    }
}
impl Drop for Journal {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
fn open_writer(path: &Path) -> Result<Connection> {
    let db = Connection::open(path)?;
    db.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=0;",
    )?;
    Ok(db)
}
pub fn open_reader(path: &Path) -> Result<Connection> {
    let db = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    db.busy_timeout(Duration::from_millis(1000))?;
    Ok(db)
}
fn check_space(dir: &Path, config: &Config, incoming: u64, db: &Connection) -> Result<()> {
    let bytes = fs::read_dir(dir)?.try_fold(0u64, |sum, e| -> Result<u64> {
        Ok(sum + e?.metadata()?.len())
    })?;
    let free_pages: u64 = db.query_row("PRAGMA freelist_count", [], |r| r.get(0))?;
    let page_size: u64 = db.query_row("PRAGMA page_size", [], |r| r.get(0))?;
    let additional = incoming
        .saturating_mul(2)
        .saturating_sub(free_pages.saturating_mul(page_size));
    ensure!(
        bytes.saturating_add(additional) < config.max_journal_bytes,
        "journal quota reached; preserve source and extend storage before a new capture session"
    );
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let path = std::ffi::CString::new(dir.as_os_str().as_bytes())?;
        let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let stat = unsafe { stat.assume_init() };
        let available = (stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64);
        ensure!(
            available >= config.reserve_disk_bytes.saturating_add(8 * 1024 * 1024),
            "disk reserve reached; capture stopped with explicit gap"
        );
    }
    Ok(())
}
pub fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let temp = path.with_extension("json.tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp)?;
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(temp, path)?;
    File::open(path.parent().context("path has no parent")?)?.sync_all()?;
    Ok(())
}

pub fn exclusive_lock(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            bail!("capture/worker already active: {}", path.display());
        }
    }
    Ok(file)
}

pub fn decode_payload(payload: &[u8], codec: &str, max_bytes: usize) -> Result<Vec<u8>> {
    ensure!(
        payload.len() <= max_bytes.saturating_add(1024 * 1024),
        "stored event exceeds bound"
    );
    let bytes = match codec {
        "json-v1" => payload.to_vec(),
        "json-zstd-v1" => zstd::bulk::decompress(payload, max_bytes)?,
        _ => bail!("unknown journal payload codec"),
    };
    ensure!(bytes.len() <= max_bytes, "decoded event exceeds bound");
    Ok(bytes)
}
