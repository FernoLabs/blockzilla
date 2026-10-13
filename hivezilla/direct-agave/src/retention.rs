//! Writer-owned retirement of exact rooted bank records after compact custody commits.
use crate::{event::digest, journal::atomic_json};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::Read,
    path::Path,
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bank {
    pub slot: u64,
    pub bank_id: u64,
    pub through_sequence: u64,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub schema: String,
    pub session_id: String,
    pub marker_path: String,
    pub marker_sha256: String,
    pub banks: Vec<Bank>,
}
pub fn enqueue(session: &Path, request: &Request) -> Result<()> {
    ensure!(
        !request.banks.is_empty() && request.banks.len() <= 128,
        "invalid retirement bank count"
    );
    let path = session.join("retire-pending").join(format!(
        "{:020}-{:020}.json",
        request.banks[0].slot,
        request.banks.last().unwrap().slot
    ));
    if path.exists() {
        ensure!(
            fs::read(&path)? == {
                let mut v = serde_json::to_vec(request)?;
                v.push(b'\n');
                v
            },
            "retirement request changed"
        );
        return Ok(());
    }
    atomic_json(&path, request)
}
fn read_file(path: &Path, max: u64) -> Result<Vec<u8>> {
    ensure!(
        fs::symlink_metadata(path)?.file_type().is_file(),
        "retirement proof is not a regular file"
    );
    let mut bytes = Vec::new();
    File::open(path)?.take(max + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= max, "retirement proof exceeds bound");
    Ok(bytes)
}
fn verify_source(marker: &serde_json::Value, base: &Path) -> Result<()> {
    use sha2::{Digest, Sha256};
    let files = marker["files"].as_array().context("marker has no files")?;
    let source = files
        .iter()
        .find(|f| {
            matches!(
                f["path"].as_str(),
                Some("source-events.json" | "source-events.json.zst")
            )
        })
        .context("marker has no source evidence")?;
    let path = base.join(source["path"].as_str().unwrap());
    ensure!(
        fs::symlink_metadata(&path)?.file_type().is_file(),
        "source evidence is not a regular file"
    );
    let mut file = File::open(path)?;
    let mut sha = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = [0; 65536];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        bytes += n as u64;
        ensure!(bytes <= 512 * 1024 * 1024, "source evidence exceeds bound");
        sha.update(&buffer[..n]);
    }
    let actual = sha
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    ensure!(
        source["bytes"] == bytes && source["sha256"] == actual,
        "retained source evidence differs from compact custody receipt"
    );
    Ok(())
}
/// Performs at most one 2048-record DELETE transaction. Capture owns this connection.
pub fn process_one(session: &Path, db: &mut Connection, session_id: &str) -> Result<bool> {
    let mut pending = fs::read_dir(session.join("retire-pending"))?
        .filter_map(|r| r.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "json"))
        .collect::<Vec<_>>();
    ensure!(pending.len() <= 4096, "retirement queue exceeds bound");
    pending.sort();
    let Some(path) = pending.first() else {
        return Ok(false);
    };
    let id = path
        .file_name()
        .context("retirement name missing")?
        .to_str()
        .context("retirement name invalid")?;
    let request: Request = serde_json::from_slice(&read_file(path, 1024 * 1024)?)?;
    ensure!(
        request.schema == "hivezilla-direct-agave-retire-v1"
            && request.session_id == session_id
            && !request.banks.is_empty()
            && request.banks.len() <= 128,
        "invalid retirement session or bank count"
    );
    let job: Option<(usize, u64, String)> = db
        .query_row(
            "SELECT bank_index,deleted_events,marker_sha FROM retire_jobs WHERE id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    let (mut bank_index, mut deleted) = if let Some((index, count, sha)) = job {
        ensure!(sha == request.marker_sha256, "retirement proof changed");
        (index, count)
    } else {
        let marker_path = Path::new(&request.marker_path);
        ensure!(
            marker_path.is_absolute(),
            "retirement proof path must be absolute"
        );
        let raw = read_file(marker_path, 16 * 1024 * 1024)?;
        ensure!(
            digest(&raw) == request.marker_sha256,
            "retirement marker digest differs"
        );
        let marker: serde_json::Value = serde_json::from_slice(&raw)?;
        ensure!(
            marker["state"] == "COMPLETE"
                && matches!(
                    marker["schema"].as_str(),
                    Some(
                        "hivezilla-direct-agave-compact-v1"
                            | "hivezilla-direct-agave-processed-custody-v1"
                    )
                )
                && marker["session_id"] == session_id
                && marker["finalized"] == true
                && marker["canonical"] == false,
            "not a committed direct compact output"
        );
        verify_source(
            &marker,
            marker_path.parent().context("marker has no directory")?,
        )?;
        if marker["schema"] == "hivezilla-direct-agave-processed-custody-v1" {
            let payload_path = Path::new(
                marker["payload_marker_path"]
                    .as_str()
                    .context("missing processed payload path")?,
            );
            ensure!(
                payload_path.is_absolute(),
                "processed payload path is not absolute"
            );
            let raw = read_file(payload_path, 1024 * 1024)?;
            ensure!(
                digest(&raw) == marker["payload_marker_sha256"],
                "processed payload marker changed"
            );
            let payload: serde_json::Value = serde_json::from_slice(&raw)?;
            ensure!(
                payload["schema"] == "hivezilla-direct-agave-processed-compact-v1"
                    && payload["state"] == "COMPLETE"
                    && payload["finalized"] == false
                    && payload["canonical"] == false
                    && payload["publication_ready"] == false
                    && payload["session_id"] == session_id
                    && payload["produced_blocks"] == marker["produced_blocks"],
                "processed payload identity differs from rooted custody"
            );
            let base = payload_path
                .parent()
                .context("missing processed payload directory")?;
            let files = payload["files"]
                .as_array()
                .context("missing processed files")?;
            ensure!(files.len() <= 32, "too many processed files");
            for f in files {
                let name = f["path"].as_str().context("missing processed file path")?;
                ensure!(
                    Path::new(name).components().count() == 1 && !matches!(name, "." | ".."),
                    "unsafe processed file path"
                );
                let bytes = read_file(&base.join(name), 256 * 1024 * 1024)?;
                ensure!(
                    f["bytes"] == bytes.len() && f["sha256"] == digest(&bytes),
                    "processed object changed before retirement"
                );
            }
            ensure!(
                request.banks.len() == 1
                    && marker["through_sequence"] == request.banks[0].through_sequence,
                "processed custody sequence mismatch"
            );
        }
        let rows = marker["produced_blocks"]
            .as_array()
            .context("marker lacks bank rows")?;
        let durable: u64 =
            db.query_row("SELECT durable_sequence FROM status WHERE id=1", [], |r| {
                r.get(0)
            })?;
        for bank in &request.banks {
            ensure!(
                bank.through_sequence <= durable
                    && rows
                        .iter()
                        .any(|r| r["slot"] == bank.slot && r["bank_id"] == bank.bank_id),
                "retirement bank is absent from committed output"
            );
            let rooted:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM events WHERE slot=?1 AND bank=?2 AND kind='rooted' AND sequence<=?3)",params![bank.slot,bank.bank_id,bank.through_sequence],|r|r.get(0))?;
            ensure!(rooted, "retirement bank has no captured root");
        }
        db.execute(
            "INSERT INTO retire_jobs VALUES(?1,0,0,?2)",
            params![id, request.marker_sha256],
        )?;
        (0, 0)
    };
    ensure!(
        bank_index <= request.banks.len(),
        "invalid retirement progress"
    );
    if bank_index < request.banks.len() {
        let bank = &request.banks[bank_index];
        let tx = db.transaction()?;
        let count=tx.execute("DELETE FROM events WHERE sequence IN (SELECT sequence FROM events WHERE slot=?1 AND bank=?2 AND sequence<=?3 ORDER BY sequence LIMIT 2048)",params![bank.slot,bank.bank_id,bank.through_sequence])?;
        let more: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM events WHERE slot=?1 AND bank=?2 AND sequence<=?3)",
            params![bank.slot, bank.bank_id, bank.through_sequence],
            |r| r.get(0),
        )?;
        if !more {
            bank_index += 1;
        }
        deleted += count as u64;
        tx.execute(
            "UPDATE retire_jobs SET bank_index=?1,deleted_events=?2 WHERE id=?3",
            params![bank_index, deleted, id],
        )?;
        tx.execute(
            "UPDATE status SET retired_events=retired_events+?1 WHERE id=1",
            [count],
        )?;
        tx.commit()?; // The acknowledgement below is strictly after durable deletion.
    }
    if bank_index == request.banks.len() {
        atomic_json(
            &session.join("RETIREMENT.json"),
            &serde_json::json!({"schema":"hivezilla-direct-agave-retired-v1","session_id":session_id,"request":id,"marker_path":request.marker_path,"marker_sha256":request.marker_sha256,"deleted_events":deleted,"last_retired_slot":request.banks.last().unwrap().slot,"acknowledgement":"SQLite FULL delete commit completed"}),
        )?;
        fs::remove_file(path)?;
        File::open(session.join("retire-pending"))?.sync_all()?;
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (tempfile::TempDir, Connection, Request) {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir(tmp.path().join("retire-pending")).unwrap();
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE events(sequence INTEGER PRIMARY KEY,slot INTEGER,bank INTEGER,kind TEXT);CREATE TABLE status(id INTEGER PRIMARY KEY,durable_sequence INTEGER,latest_rooted_slot INTEGER,retired_events INTEGER);INSERT INTO status VALUES(1,4000,10,0);CREATE TABLE retire_jobs(id TEXT PRIMARY KEY,bank_index INTEGER,deleted_events INTEGER,marker_sha TEXT);").unwrap();
        for seq in 1..=3000 {
            db.execute("INSERT INTO events VALUES(?1,10,7,'entry')", [seq])
                .unwrap();
        }
        db.execute_batch("INSERT INTO events VALUES(3001,10,7,'rooted');INSERT INTO events VALUES(3002,10,8,'entry');INSERT INTO events VALUES(3003,10,NULL,'status');INSERT INTO events VALUES(3004,10,7,'entry');").unwrap();
        let source = br#"[{"full_source":"fixture"}]"#;
        fs::write(tmp.path().join("source-events.json"), source).unwrap();
        let marker = serde_json::json!({"schema":"hivezilla-direct-agave-compact-v1","state":"COMPLETE","session_id":"test","finalized":true,"canonical":false,"produced_blocks":[{"slot":10,"bank_id":7}],"files":[{"path":"source-events.json","bytes":source.len(),"sha256":digest(source)}]});
        let path = tmp.path().join("DIRECT-AGAVE-COMPACT.v1.json");
        atomic_json(&path, &marker).unwrap();
        let request = Request {
            schema: "hivezilla-direct-agave-retire-v1".into(),
            session_id: "test".into(),
            marker_path: path.to_string_lossy().into_owned(),
            marker_sha256: digest(&fs::read(&path).unwrap()),
            banks: vec![Bank {
                slot: 10,
                bank_id: 7,
                through_sequence: 3001,
            }],
        };
        (tmp, db, request)
    }
    #[test]
    fn retirement_is_bounded_and_ack_follows_durable_delete() {
        let (tmp, mut db, request) = fixture();
        enqueue(tmp.path(), &request).unwrap();
        assert_eq!(
            db.query_row("SELECT count(*) FROM events", [], |r| r.get::<_, u64>(0))
                .unwrap(),
            3004
        );
        process_one(tmp.path(), &mut db, "test").unwrap();
        assert!(!tmp.path().join("RETIREMENT.json").exists());
        assert_eq!(
            db.query_row("SELECT retired_events FROM status", [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            2048
        );
        process_one(tmp.path(), &mut db, "test").unwrap();
        assert!(tmp.path().join("RETIREMENT.json").exists());
        assert_eq!(
            db.query_row("SELECT count(*) FROM events", [], |r| r.get::<_, u64>(0))
                .unwrap(),
            3
        );
        assert_eq!(
            db.query_row("SELECT latest_rooted_slot FROM status", [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            10
        );
        assert!(!process_one(tmp.path(), &mut db, "test").unwrap());
    }
    #[test]
    fn retirement_rejects_tampered_source_without_deletion() {
        let (tmp, mut db, request) = fixture();
        enqueue(tmp.path(), &request).unwrap();
        fs::write(tmp.path().join("source-events.json"), b"changed").unwrap();
        assert!(process_one(tmp.path(), &mut db, "test").is_err());
        assert_eq!(
            db.query_row("SELECT count(*) FROM events", [], |r| r.get::<_, u64>(0))
                .unwrap(),
            3004
        );
    }
}
