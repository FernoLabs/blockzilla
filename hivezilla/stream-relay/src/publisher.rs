//! Durable native projection publication. No source retention or canonical claims.
use crate::{
    frame::{self, Chunk},
    source::{Source, bounded_file},
    store::{atomic, now_ms},
};
use anyhow::{Context, Result, ensure};
use clap::Args;
use hivezilla_object_store::uploader::{Payload, S3Client, StorageSettings};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::Read,
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Args)]
pub struct Options {
    #[arg(long)]
    pub output: PathBuf,
    #[arg(long)]
    pub state: PathBuf,
    #[arg(long)]
    pub credentials_file: PathBuf,
    /// Must start with native-stream/. A source session is appended automatically.
    #[arg(long)]
    pub prefix: String,
    /// Initial position only. A saved state always takes precedence.
    #[arg(long)]
    pub start_slot: Option<u64>,
    #[arg(long, conflicts_with = "start_slot")]
    pub processed: bool,
    #[arg(long, requires = "processed")]
    pub start_sequence: Option<u64>,
    #[arg(long, default_value_t = 256)]
    pub tail_slots: u64,
    /// Exit after this many new commits, for a bounded deployment check.
    #[arg(long)]
    pub max_chunks: Option<u64>,
    #[arg(long, default_value_t = 16)]
    pub upload_workers: usize,
    /// Maximum ordered events per durable batch; byte limits still apply.
    #[arg(long, default_value_t = 64)]
    pub batch_events: usize,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    schema: String,
    source: PathBuf,
    namespace: String,
    prefix: String,
    session: String,
    publisher_id: String,
    start_slot: u64,
    sequence: u64,
    last_first_slot: Option<u64>,
    cursor: String,
    blocks: u64,
    bytes: u64,
}

trait Remote: Sync {
    fn immutable(&self, key: &str, bytes: &[u8], content_type: &str) -> Result<()>;
    fn head(&self, key: &str, bytes: &[u8]) -> Result<()>;
}
struct R2(S3Client);
impl R2 {
    fn checked_metadata(&self, key: &str, bytes: &[u8]) -> Result<bool> {
        let response = self.0.head(key)?;
        if response.status == 404 {
            return Ok(false);
        }
        ensure!(response.status == 200, "R2_metadata_status");
        ensure!(
            response.exact_content_length("native stream object")? == bytes.len() as u64,
            "R2_object_length_conflict"
        );
        // This is uploader metadata, not a provider-attested checksum.
        ensure!(
            response.exact_header("x-amz-meta-sha256", "native stream object")?
                == frame::digest(bytes),
            "R2_object_digest_metadata_conflict"
        );
        Ok(true)
    }
    fn small_exact(&self, key: &str, expected: &[u8]) -> Result<()> {
        let mut body = self.0.get(key)?.into_reader();
        let mut bytes = Vec::new();
        body.by_ref()
            .take(expected.len() as u64 + 1)
            .read_to_end(&mut bytes)?;
        ensure!(bytes == expected, "R2_manifest_readback_conflict");
        Ok(())
    }
}
impl Remote for R2 {
    fn immutable(&self, key: &str, bytes: &[u8], content_type: &str) -> Result<()> {
        if !self.checked_metadata(key, bytes)? {
            let response = self.0.put(
                key,
                content_type,
                &frame::digest(bytes),
                None,
                true,
                &Payload::Bytes(bytes.to_vec()),
            )?;
            ensure!(matches!(response.status, 200 | 412), "R2_create_status");
            ensure!(
                self.checked_metadata(key, bytes)?,
                "R2_created_object_missing"
            );
        }
        if bytes.len() <= frame::MAX_META {
            self.small_exact(key, bytes)?;
        }
        Ok(())
    }
    fn head(&self, key: &str, bytes: &[u8]) -> Result<()> {
        if self.0.head(key)?.status == 200 {
            let mut prior = Vec::new();
            self.0
                .get(key)?
                .into_reader()
                .take(frame::MAX_META as u64 + 1)
                .read_to_end(&mut prior)?;
            ensure!(prior.len() <= frame::MAX_META, "R2_head_too_large");
            let old: Value = serde_json::from_slice(&prior)?;
            let next: Value = serde_json::from_slice(bytes)?;
            ensure!(
                old["session"] == next["session"] && old["schema"] == next["schema"],
                "R2_head_identity_conflict"
            );
            let old_sequence = old["sequence"]
                .as_u64()
                .context("invalid_R2_head_sequence")?;
            let sequence = next["sequence"]
                .as_u64()
                .context("invalid_new_head_sequence")?;
            ensure!(sequence >= old_sequence, "R2_head_would_regress");
            if sequence == old_sequence {
                ensure!(old == next, "R2_head_sequence_conflict");
            }
        }
        let response = self.0.put(
            key,
            "application/json",
            &frame::digest(bytes),
            None,
            false,
            &Payload::Bytes(bytes.to_vec()),
        )?;
        ensure!(response.status == 200, "R2_head_status");
        self.small_exact(key, bytes)
    }
}

fn save(path: &Path, value: &impl Serialize) -> Result<()> {
    atomic(path, &serde_json::to_vec(value)?)
}
fn object_key(cp: &Checkpoint, c: &Chunk) -> String {
    format!("{}/chunks/{}-{}.frame", cp.prefix, c.last_slot, c.digest)
}
fn manifest_key(cp: &Checkpoint, sequence: u64) -> String {
    format!("{}/commits/{sequence:020}.json", cp.prefix)
}
const MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;
const MAX_BATCH_EVENTS: usize = 256;
const MAX_INTENT: usize = 8 * 1024 * 1024;

fn intents(state: &Path) -> Result<Option<Vec<Value>>> {
    let path = state.join("INTENT.json");
    if !path.exists() {
        return Ok(None);
    }
    let v: Value = serde_json::from_slice(&bounded_file(&path, MAX_INTENT)?)?;
    // Accept the already deployed serial publisher's single pending intent.
    if v["schema"] == "blockzilla-native-stream-commit-v1" {
        return Ok(Some(vec![v]));
    }
    ensure!(
        v["schema"] == "blockzilla-native-publisher-intent-v1",
        "invalid_publisher_intent"
    );
    let commits = v["commits"]
        .as_array()
        .context("missing_intent_commits")?
        .clone();
    ensure!(
        !commits.is_empty() && commits.len() <= MAX_BATCH_EVENTS,
        "invalid_intent_batch"
    );
    Ok(Some(commits))
}

#[cfg(test)]
fn commit(
    remote: &dyn Remote,
    state: &Path,
    cp: &mut Checkpoint,
    previous: Option<&Chunk>,
    c: &Chunk,
) -> Result<()> {
    commit_batch(remote, state, cp, previous, std::slice::from_ref(c), 1)
}

fn commit_batch(
    remote: &dyn Remote,
    state: &Path,
    cp: &mut Checkpoint,
    previous: Option<&Chunk>,
    chunks: &[Chunk],
    workers: usize,
) -> Result<()> {
    ensure!((1..=64).contains(&workers), "invalid_upload_worker_limit");
    ensure!(
        !chunks.is_empty() && chunks.len() <= MAX_BATCH_EVENTS,
        "invalid_publish_batch"
    );
    ensure!(
        chunks.iter().map(|c| c.wire.len()).sum::<usize>() <= MAX_BATCH_BYTES,
        "publish_batch_too_large"
    );
    let mut next = cp.clone();
    let mut prior = previous;
    let mut manifests = Vec::new();
    for c in chunks {
        if let Some(p) = prior {
            frame::follows(p, c)?;
        }
        ensure!(
            c.session == cp.session
                && frame::processed_cursor(&next.cursor)
                    == (c.envelope.profile == frame::PROCESSED)
                && (if c.envelope.profile == frame::PROCESSED {
                    c.position() == frame::parse_cursor(&next.cursor)?.1 + 1
                } else {
                    c.position() > frame::parse_cursor(&next.cursor)?.1
                }),
            "publisher_order_conflict"
        );
        next.sequence = next.sequence.checked_add(1).context("sequence_overflow")?;
        let manifest = json!({"schema":"blockzilla-native-stream-commit-v1",
            "profile":c.envelope.profile, "event":c.envelope.event,"sequence":next.sequence,
            "session":c.session, "after":next.cursor, "cursor":c.cursor,
            "first_slot":c.first_slot,"last_slot":c.last_slot,
            "object":object_key(cp,c),"sha256":c.digest,"bytes":c.wire.len(),
            "produced_blocks":c.envelope.marker["produced_blocks"],
            "genesis_hash":c.envelope.marker["genesis_hash"],
            "node_identity":c.envelope.marker["node_identity"],
            "agave_revision":c.envelope.marker["agave_revision"],
            "canonical":false,"publication_ready":false,"evidence_included":false});
        ensure!(
            serde_json::to_vec(&manifest)?.len() <= frame::MAX_META,
            "commit_manifest_too_large"
        );
        manifests.push(manifest);
        next.last_first_slot = Some(if c.envelope.profile == frame::PROCESSED {
            c.position()
        } else {
            c.first_slot
        });
        next.cursor = c.cursor.clone();
        next.blocks += if c.envelope.event.as_ref().is_some_and(|e| e.kind != "block") {
            0
        } else {
            c.envelope.marker["produced_blocks"]
                .as_array()
                .map_or(0, |v| v.len()) as u64
        };
        next.bytes += c.wire.len() as u64;
        prior = Some(c);
    }
    let intent = state.join("INTENT.json");
    if let Some(old) = intents(state)? {
        ensure!(old == manifests, "publisher_pending_intent_changed");
    } else {
        save(
            &intent,
            &json!({"schema":"blockzilla-native-publisher-intent-v1","commits":manifests}),
        )?;
    }
    // Individual immutable objects are independent. Only the ordered catalog
    // head becomes visible after every object and receipt in this batch completes.
    let index = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for _ in 0..workers.min(chunks.len()) {
            handles.push(scope.spawn(|| -> Result<()> {
                loop {
                    let i = index.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let Some(c) = chunks.get(i) else {
                        return Ok(());
                    };
                    remote.immutable(
                        &object_key(cp, c),
                        &c.wire,
                        "application/vnd.blockzilla.stream.v1",
                    )?;
                    remote.immutable(
                        &manifest_key(cp, cp.sequence + i as u64 + 1),
                        &serde_json::to_vec(&manifests[i])?,
                        "application/json",
                    )?;
                }
            }));
        }
        let mut first_error = None;
        for h in handles {
            let result = h
                .join()
                .unwrap_or_else(|_| Err(anyhow::anyhow!("publisher_upload_worker_panicked")));
            if let Err(e) = result {
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
        if let Some(e) = first_error {
            return Err(e);
        }
        Ok(())
    })?;
    let c = chunks.last().unwrap();
    let head = json!({"schema":"blockzilla-native-stream-catalog-v1",
        "profile":c.envelope.profile,"event":c.envelope.event,"session":cp.session,
        "start_slot":cp.start_slot,"sequence":next.sequence,"latest_cursor":c.cursor,
        "first_available_after":if frame::processed_cursor(&cp.cursor){frame::processed_anchor(&cp.session,cp.start_slot-1)}else{frame::anchor(&cp.session,cp.start_slot-1)},
        "last_slot":c.last_slot,"last_commit":manifest_key(cp,next.sequence),
        "commit_pattern":format!("{}/commits/SEQUENCE_20_DIGITS.json",cp.prefix),
        "blocks":next.blocks,"bytes":next.bytes,
        "canonical":false,"publication_ready":false,"evidence_included":false,
        "coverage":if frame::processed_cursor(&cp.cursor){"from_explicit_event_start_with_sequence_checks"}else{"from_explicit_start_with_parent_chain_checks"}});
    remote.head(
        &format!("{}/HEAD.json", cp.prefix),
        &serde_json::to_vec(&head)?,
    )?;
    save(&state.join("CHECKPOINT.json"), &next)?;
    fs::remove_file(intent)?;
    File::open(state)?.sync_all()?;
    *cp = next;
    Ok(())
}

fn stale_intent(state: &Path, cp: &Checkpoint) -> Result<()> {
    if let Some(commits) = intents(state)? {
        let last = commits.last().unwrap();
        if last["sequence"].as_u64() == Some(cp.sequence) {
            ensure!(
                last["cursor"] == cp.cursor,
                "publisher_stale_intent_conflict"
            );
            fs::remove_file(state.join("INTENT.json"))?;
            File::open(state)?.sync_all()?;
        } else {
            ensure!(
                commits[0]["sequence"].as_u64() == cp.sequence.checked_add(1),
                "publisher_intent_sequence_conflict"
            );
        }
    }
    Ok(())
}

pub fn run(options: Options) -> Result<()> {
    ensure!(
        options.prefix.starts_with("native-stream/")
            && options.prefix.len() <= 256
            && options.prefix.split('/').all(frame::session_name),
        "invalid_native_stream_prefix"
    );
    ensure!(
        (1..=4096).contains(&options.tail_slots),
        "invalid_tail_window"
    );
    ensure!(options.max_chunks != Some(0), "invalid_chunk_limit");
    ensure!(
        (1..=MAX_BATCH_EVENTS).contains(&options.batch_events),
        "invalid_batch_event_limit"
    );
    ensure!(
        (1..=64).contains(&options.upload_workers),
        "invalid_upload_worker_limit"
    );
    let source = options.output.canonicalize()?;
    fs::create_dir_all(&options.state)?;
    let state = options.state.canonicalize()?;
    ensure!(!state.starts_with(&source), "publisher_state_inside_source");
    let lock = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(state.join("LOCK"))?;
    ensure!(
        unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
        "publisher_state_in_use"
    );
    let settings = StorageSettings::load(Some(&options.credentials_file), Some("r2"))?;
    let namespace = frame::digest(
        format!(
            "{}\0{}\0{}",
            settings.endpoint, settings.bucket, options.prefix
        )
        .as_bytes(),
    );
    let remote = R2(S3Client::new(settings, 3)?);
    let cursor: Value = serde_json::from_slice(&bounded_file(
        &source.join(if options.processed {
            "FEED.json"
        } else {
            "CURSOR.json"
        }),
        65536,
    )?)?;
    if options.processed {
        ensure!(
            options.prefix.starts_with("native-stream/processed/"),
            "processed_requires_separate_namespace"
        );
    }
    let session = cursor["session_id"]
        .as_str()
        .context("missing_source_session")?
        .to_owned();
    ensure!(frame::session_name(&session), "invalid_source_session");
    let prefix = format!("{}/{}", options.prefix, session);
    let mut cp: Checkpoint = if state.join("CHECKPOINT.json").exists() {
        serde_json::from_slice(&bounded_file(&state.join("CHECKPOINT.json"), 65536)?)?
    } else {
        let start = if options.processed {
            options.start_sequence.unwrap_or(1)
        } else {
            options.start_slot.unwrap_or(
                cursor["last_slot"]
                    .as_u64()
                    .context("missing_source_slot")?
                    .saturating_sub(options.tail_slots)
                    .max(1),
            )
        };
        ensure!(start > 0, "invalid_start_slot");
        let mut entropy = [0u8; 32];
        File::open("/dev/urandom")?.read_exact(&mut entropy)?;
        let initial = Checkpoint {
            schema: "blockzilla-native-publisher-checkpoint-v1".into(),
            source: source.clone(),
            namespace: namespace.clone(),
            prefix: prefix.clone(),
            session: session.clone(),
            publisher_id: frame::digest(&entropy),
            start_slot: start,
            sequence: 0,
            last_first_slot: None,
            cursor: if options.processed {
                frame::processed_anchor(&session, start - 1)
            } else {
                frame::anchor(&session, start - 1)
            },
            blocks: 0,
            bytes: 0,
        };
        save(&state.join("CHECKPOINT.json"), &initial)?;
        initial
    };
    ensure!(
        cp.schema == "blockzilla-native-publisher-checkpoint-v1"
            && cp.source == source
            && cp.namespace == namespace
            && cp.prefix == prefix
            && cp.session == session
            && cp.start_slot > 0
            && frame::processed_cursor(&cp.cursor) == options.processed,
        "publisher_destination_or_source_changed"
    );
    stale_intent(&state, &cp)?;
    let owner = json!({"schema":"blockzilla-native-stream-owner-v1",
        "namespace":cp.namespace,"session":cp.session,"publisher_id":cp.publisher_id,"start_slot":cp.start_slot,
        "source_path":cp.source,"canonical":false,"publication_ready":false});
    remote.immutable(
        &format!("{prefix}/OWNER.json"),
        &serde_json::to_vec(&owner)?,
        "application/json",
    )?;
    let mut previous = if let Some(first) = cp.last_first_slot {
        let mut previous_source = if options.processed {
            Source::open_processed(&source, Some(first), 256)?
        } else {
            Source::open(&source, first)?
        };
        let chunk = previous_source
            .next_chunk()?
            .context("publisher_previous_source_missing")?;
        ensure!(
            chunk.cursor == cp.cursor,
            "publisher_previous_source_changed"
        );
        Some(chunk)
    } else {
        None
    };
    let next_position = frame::parse_cursor(&cp.cursor)?
        .1
        .checked_add(1)
        .context("position_overflow")?;
    let mut reader = if options.processed {
        Source::open_processed(&source, Some(next_position), 256)?
    } else {
        Source::open(&source, next_position)?
    };
    let initial_sequence = cp.sequence;
    let mut last_status = 0;
    let mut last_batch = Value::Null;
    loop {
        let result = (|| -> Result<bool> {
            let read_started = now_ms();
            let pending = intents(&state)?;
            let count = pending.as_ref().map_or(options.batch_events, Vec::len);
            let count = if pending.is_some() {
                count
            } else {
                options.max_chunks.map_or(count, |n| {
                    (n - (cp.sequence - initial_sequence)).min(count as u64) as usize
                })
            };
            let mut chunks = Vec::new();
            let mut bytes = 0;
            while chunks.len() < count
                && (pending.is_some() || bytes < MAX_BATCH_BYTES - frame::MAX_FRAME)
            {
                let Some(c) = reader.next_chunk()? else {
                    break;
                };
                bytes += c.wire.len();
                chunks.push(c);
            }
            if chunks.is_empty() {
                return Ok(false);
            }
            ensure!(
                pending.is_none() || chunks.len() == count,
                "publisher_pending_source_missing"
            );
            let upload_started = now_ms();
            commit_batch(
                &remote,
                &state,
                &mut cp,
                previous.as_ref(),
                &chunks,
                options.upload_workers,
            )?;
            last_batch = json!({"events":chunks.len(),"bytes":bytes,
                "source_read_ms":upload_started.saturating_sub(read_started),
                "upload_commit_ms":now_ms().saturating_sub(upload_started),
                "upload_workers":options.upload_workers});
            previous = chunks.pop();
            Ok(true)
        })();
        if let Err(error) = result {
            save(
                &state.join("STATUS.json"),
                &json!({"state":"FAILED","updated_unix_ms":now_ms(),
                "error":error.to_string(),"checkpoint":cp}),
            )?;
            return Err(error);
        }
        if now_ms() - last_status >= 5000 || options.max_chunks.is_some() {
            save(
                &state.join("STATUS.json"),
                &json!({"state":"RUNNING","updated_unix_ms":now_ms(),
                "checkpoint":cp,"last_batch":last_batch}),
            )?;
            last_status = now_ms();
        }
        if options
            .max_chunks
            .is_some_and(|n| cp.sequence - initial_sequence >= n)
        {
            return Ok(());
        }
        if !result? {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::BTreeMap,
        sync::{
            Mutex,
            atomic::{AtomicBool, Ordering},
        },
    };
    #[derive(Default)]
    struct Fake {
        objects: Mutex<BTreeMap<String, Vec<u8>>>,
        lose_reply: AtomicBool,
    }
    impl Remote for Fake {
        fn immutable(&self, key: &str, bytes: &[u8], _: &str) -> Result<()> {
            let mut objects = self.objects.lock().unwrap();
            if let Some(old) = objects.get(key) {
                ensure!(old == bytes, "object_conflict");
            } else {
                objects.insert(key.into(), bytes.into());
            }
            if self.lose_reply.swap(false, Ordering::SeqCst) {
                anyhow::bail!("lost_reply");
            }
            Ok(())
        }
        fn head(&self, key: &str, bytes: &[u8]) -> Result<()> {
            self.objects
                .lock()
                .unwrap()
                .insert(key.into(), bytes.into());
            Ok(())
        }
    }
    fn checkpoint() -> Checkpoint {
        Checkpoint {
            schema: "blockzilla-native-publisher-checkpoint-v1".into(),
            source: "/source".into(),
            namespace: "bound".into(),
            prefix: "native-stream/test/session1".into(),
            session: "session1".into(),
            publisher_id: "test-owner".into(),
            start_slot: 100,
            sequence: 0,
            last_first_slot: None,
            cursor: frame::anchor("session1", 99),
            blocks: 0,
            bytes: 0,
        }
    }
    #[test]
    fn parallel_batch_is_atomic_and_resumes_partial_uploads() {
        let t = tempfile::tempdir().unwrap();
        let remote = Fake::default();
        let mut cp = checkpoint();
        let chunks = (100..164)
            .map(|slot| frame::fixture(slot, slot - 1))
            .collect::<Vec<_>>();
        remote.lose_reply.store(true, Ordering::SeqCst);
        assert!(commit_batch(&remote, t.path(), &mut cp, None, &chunks, 64).is_err());
        assert_eq!(cp.sequence, 0);
        assert!(
            !remote
                .objects
                .lock()
                .unwrap()
                .contains_key(&format!("{}/HEAD.json", cp.prefix))
        );
        let pending = intents(t.path()).unwrap().unwrap();
        assert_eq!(pending.len(), 64);
        commit_batch(&remote, t.path(), &mut cp, None, &chunks, 64).unwrap();
        assert_eq!(cp.sequence, 64);
        assert_eq!(cp.blocks, 64);
        assert_eq!(remote.objects.lock().unwrap().len(), 129);
        for i in 1..pending.len() {
            assert_eq!(pending[i]["after"], pending[i - 1]["cursor"]);
        }
        assert!(!t.path().join("INTENT.json").exists());
    }
    #[test]
    fn larger_batch_keeps_the_durable_head_and_restart_boundaries() {
        let t = tempfile::tempdir().unwrap();
        let remote = Fake::default();
        let mut cp = checkpoint();
        let chunks = (100..228)
            .map(|slot| frame::fixture(slot, slot - 1))
            .collect::<Vec<_>>();
        remote.lose_reply.store(true, Ordering::SeqCst);
        assert!(commit_batch(&remote, t.path(), &mut cp, None, &chunks, 64).is_err());
        assert_eq!(cp.sequence, 0);
        assert_eq!(intents(t.path()).unwrap().unwrap().len(), 128);
        commit_batch(&remote, t.path(), &mut cp, None, &chunks, 64).unwrap();
        assert_eq!(cp.sequence, 128);
        assert_eq!(cp.blocks, 128);
        assert!(!t.path().join("INTENT.json").exists());
        let head: Value = serde_json::from_slice(
            &remote.objects.lock().unwrap()[&format!("{}/HEAD.json", cp.prefix)],
        )
        .unwrap();
        assert_eq!(head["sequence"], 128);
    }
    #[test]
    fn processed_publication_reuses_payload_and_saves_ordered_controls() {
        let t = tempfile::tempdir().unwrap();
        let remote = Fake::default();
        let mut cp = checkpoint();
        cp.cursor = frame::processed_anchor("session1", 0);
        cp.start_slot = 1;
        let chunks = vec![
            frame::processed_fixture(1, 100, 7, "block"),
            frame::processed_fixture(2, 100, 8, "block"),
            frame::processed_fixture(3, 100, 7, "invalidate"),
            frame::processed_fixture(4, 100, 8, "root"),
        ];
        commit_batch(&remote, t.path(), &mut cp, None, &chunks, 2).unwrap();
        assert_eq!(cp.blocks, 2);
        assert_eq!(cp.sequence, 4);
        assert_eq!(cp.cursor, chunks[3].cursor);
        let objects = remote.objects.lock().unwrap();
        let head: Value =
            serde_json::from_slice(objects.get(&format!("{}/HEAD.json", cp.prefix)).unwrap())
                .unwrap();
        assert_eq!(head["profile"], frame::PROCESSED);
        assert_eq!(head["event"]["kind"], "root");
        assert_eq!(head["first_available_after"], "session1:p0:-");
    }
    #[test]
    fn lost_reply_reuses_bytes_without_advancing_checkpoint() {
        let t = tempfile::tempdir().unwrap();
        let remote = Fake::default();
        let mut cp = checkpoint();
        let chunk = frame::fixture(100, 99);
        remote.lose_reply.store(true, Ordering::SeqCst);
        assert!(commit(&remote, t.path(), &mut cp, None, &chunk).is_err());
        assert_eq!(cp.sequence, 0);
        assert!(!t.path().join("CHECKPOINT.json").exists());
        commit(&remote, t.path(), &mut cp, None, &chunk).unwrap();
        assert_eq!(cp.sequence, 1);
        assert_eq!(remote.objects.lock().unwrap().len(), 3);
        let restored: Checkpoint =
            serde_json::from_slice(&fs::read(t.path().join("CHECKPOINT.json")).unwrap()).unwrap();
        assert_eq!(restored.cursor, chunk.cursor);
    }
    #[test]
    fn gaps_and_remote_conflicts_do_not_publish_a_head() {
        let t = tempfile::tempdir().unwrap();
        let remote = Fake::default();
        let mut cp = checkpoint();
        let chunk = frame::fixture(100, 99);
        remote
            .objects
            .lock()
            .unwrap()
            .insert(object_key(&cp, &chunk), vec![1]);
        assert!(commit(&remote, t.path(), &mut cp, None, &chunk).is_err());
        assert_eq!(cp.sequence, 0);
        assert!(
            !remote
                .objects
                .lock()
                .unwrap()
                .contains_key(&format!("{}/HEAD.json", cp.prefix))
        );
        remote.objects.lock().unwrap().clear();
        commit(&remote, t.path(), &mut cp, None, &chunk).unwrap();
        assert!(
            commit(
                &remote,
                t.path(),
                &mut cp,
                Some(&chunk),
                &frame::fixture(103, 102)
            )
            .is_err()
        );
        assert_eq!(cp.sequence, 1);
    }
    #[test]
    fn changed_pending_intent_is_rejected_and_committed_intent_is_reclaimed() {
        let t = tempfile::tempdir().unwrap();
        let remote = Fake::default();
        let mut cp = checkpoint();
        remote.lose_reply.store(true, Ordering::SeqCst);
        assert!(commit(&remote, t.path(), &mut cp, None, &frame::fixture(100, 99)).is_err());
        assert!(commit(&remote, t.path(), &mut cp, None, &frame::fixture(101, 99)).is_err());
        commit(&remote, t.path(), &mut cp, None, &frame::fixture(100, 99)).unwrap();
        save(
            &t.path().join("INTENT.json"),
            &json!({"schema":"blockzilla-native-stream-commit-v1","sequence":cp.sequence,"cursor":cp.cursor}),
        )
        .unwrap();
        stale_intent(t.path(), &cp).unwrap();
        assert!(!t.path().join("INTENT.json").exists());
    }
}
