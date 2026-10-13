//! Durable upload intents and verified range lookup for one private overflow namespace.
//!
//! This is transport storage, not Blockzilla reconciliation. An uploaded range never advances a
//! consumer ACK and this module cannot delete local or remote data. The trusted caller binds the
//! supplied object store to `namespace`; neither reader names nor this catalog are authentication.
//! Legacy ingress WAL bytes must first pass an explicit V1 adapter, not be relabelled as HIVESEG1.

use std::{fs, io::Read, path::Path, time::Duration};

use anyhow::{Context, Result, ensure};
use hivezilla_object_store::{CreateRequest, ImmutableObjectStore, ObjectKey};
use hivezilla_protocol::{
    CursorV1, MAX_OVERFLOW_OBJECT_V1_ENCODED_LEN, OverflowNamespaceSha256, OverflowObjectV1,
    SealedSegmentV1, StreamHeaderV1, StreamId, StreamManifestV1, overflow_manifest_key,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use serde::Serialize;
use sha2::{Digest, Sha256};

const APPLICATION_ID: i64 = 0x484f5631; // HOV1
const SCHEMA_VERSION: i64 = 1;
const DATABASE_FILE: &str = "overflow.sqlite3";

pub mod commands;
pub mod grpc;

#[derive(Clone, Copy, Debug)]
pub struct OverflowLimits {
    pub max_segment_bytes: u64,
    pub max_streams: usize,
    pub max_ranges_per_stream: usize,
}

impl Default for OverflowLimits {
    fn default() -> Self {
        Self {
            max_segment_bytes: 256 * 1024 * 1024,
            max_streams: 128,
            max_ranges_per_stream: 100_000,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedOverflowRange {
    pub object: OverflowObjectV1,
    /// Verification of an overflow copy, not terminal protection or an ACK.
    pub verified: bool,
}

#[derive(Debug, Serialize)]
pub struct OverflowStreamStatus {
    pub reader_id: String,
    pub stream_id: String,
    pub pending_ranges: u64,
    pub verified_ranges: u64,
    pub verified_bytes: u64,
    /// Index coverage from P(0). It is not a fresh remote availability check or consumer progress.
    pub contiguous_verified_next_sequence: u64,
}

pub struct OverflowCatalog {
    connection: Connection,
    limits: OverflowLimits,
}

impl OverflowCatalog {
    /// Publish the exact manifest before adapter records. This binds transport identity only;
    /// declaring a durability policy does not prove its copies exist or permit an ACK.
    pub fn register_manifest(
        &mut self,
        reader_id: &str,
        manifest: &StreamManifestV1,
        store: &dyn ImmutableObjectStore,
    ) -> Result<()> {
        validate_reader_id(reader_id)?;
        let namespace: Vec<u8> =
            self.connection
                .query_row("SELECT namespace FROM catalog WHERE id=1", [], |row| {
                    row.get(0)
                })?;
        ensure!(
            manifest
                .overflow_namespace_sha256()
                .is_some_and(|value| value.as_bytes().as_slice() == namespace),
            "manifest does not name this overflow namespace"
        );
        let bytes = manifest.encode();
        ensure!(
            bytes.len() <= 1024 * 1024,
            "overflow manifest exceeds byte limit"
        );
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        bind_stream(&transaction, self.limits, reader_id, manifest.stream())?;
        transaction.commit()?;
        let request = CreateRequest {
            key: ObjectKey::new(overflow_manifest_key(
                manifest.stream().stream_id(),
                manifest.stream().stream_manifest_sha256(),
            ))?,
            encoded_len: bytes.len() as u64,
            encoded_sha256: digest(&bytes),
        };
        store.create_if_absent(&request, &mut std::io::Cursor::new(bytes))?;
        store.verify_exact(&request)?;
        Ok(())
    }

    pub fn open(
        state_dir: &Path,
        namespace: OverflowNamespaceSha256,
        limits: OverflowLimits,
    ) -> Result<Self> {
        ensure!(
            limits.max_segment_bytes > 0,
            "segment byte limit must be positive"
        );
        ensure!(limits.max_streams > 0, "stream limit must be positive");
        ensure!(
            limits.max_ranges_per_stream > 0,
            "range limit must be positive"
        );
        prepare_directory(state_dir)?;
        check_database_paths(state_dir)?;
        // Resolve trusted parent aliases (for example macOS /var -> /private/var), after
        // rejecting a symlink at the state directory and database paths themselves.
        let state_dir = state_dir.canonicalize()?;
        let mut connection = Connection::open_with_flags(
            state_dir.join(DATABASE_FILE),
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )?;
        connection.busy_timeout(Duration::from_secs(5))?;
        let app: i64 = connection.pragma_query_value(None, "application_id", |row| row.get(0))?;
        let version: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if app == 0 && version == 0 {
            let tables: i64 = connection.query_row(
                "SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
                [],
                |row| row.get(0),
            )?;
            ensure!(tables == 0, "refuse an unrelated database");
        } else {
            ensure!(
                app == APPLICATION_ID && version == SCHEMA_VERSION,
                "unsupported overflow catalog"
            );
        }
        let mode: String = connection.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
        ensure!(
            mode.eq_ignore_ascii_case("wal"),
            "overflow catalog requires WAL mode"
        );
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.pragma_update(None, "foreign_keys", true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute_batch(
            "CREATE TABLE IF NOT EXISTS catalog (
                id INTEGER PRIMARY KEY CHECK (id=1), namespace BLOB NOT NULL CHECK(length(namespace)=32)
             ) STRICT;
             CREATE TABLE IF NOT EXISTS streams (
                stream_id BLOB PRIMARY KEY CHECK(length(stream_id)=16),
                reader_id TEXT NOT NULL, header BLOB NOT NULL,
                range_count INTEGER NOT NULL DEFAULT 0 CHECK(range_count>=0)
             ) STRICT;
             CREATE TABLE IF NOT EXISTS ranges (
                stream_id BLOB NOT NULL REFERENCES streams(stream_id),
                first_sequence BLOB NOT NULL CHECK(length(first_sequence)=8),
                next_sequence BLOB NOT NULL CHECK(length(next_sequence)=8),
                object BLOB NOT NULL, object_digest BLOB NOT NULL CHECK(length(object_digest)=32),
                verified INTEGER NOT NULL CHECK(verified IN (0,1)),
                PRIMARY KEY(stream_id, first_sequence)
             ) STRICT;",
        )?;
        transaction.execute(
            "INSERT OR IGNORE INTO catalog VALUES (1, ?1)",
            [namespace.as_bytes().as_slice()],
        )?;
        let stored: Vec<u8> =
            transaction.query_row("SELECT namespace FROM catalog WHERE id=1", [], |row| {
                row.get(0)
            })?;
        ensure!(
            stored == namespace.as_bytes(),
            "overflow namespace cannot change"
        );
        transaction.pragma_update(None, "application_id", APPLICATION_ID)?;
        transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        transaction.commit()?;
        fs::File::open(state_dir)?.sync_all()?;
        Ok(Self { connection, limits })
    }

    /// Persist an intent before any remote write. Repeating exact bytes is idempotent.
    /// Disjoint ranges may arrive out of order; overlaps and inconsistent chain joins fail.
    pub fn begin_upload(
        &mut self,
        reader_id: &str,
        segment: &SealedSegmentV1,
    ) -> Result<IndexedOverflowRange> {
        validate_reader_id(reader_id)?;
        ensure!(
            !segment.frames().is_empty(),
            "overflow segment must contain records"
        );
        ensure!(
            segment.encoded_len() as u64 <= self.limits.max_segment_bytes,
            "overflow segment exceeds byte limit"
        );
        let stream = segment.header().stream();
        let encoded = segment.encode();
        let object = OverflowObjectV1::new(
            stream.stream_id(),
            segment.start(),
            segment.end(),
            encoded.len() as u64,
            Sha256::digest(&encoded).into(),
            None,
        )?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let range_count = bind_stream(&transaction, self.limits, reader_id, stream)?;
        let previous = neighbor(
            &transaction,
            object.stream_id(),
            object.start().next_sequence(),
            true,
        )?;
        if let Some(previous) = previous {
            if previous.object.start().next_sequence() == object.start().next_sequence() {
                ensure!(
                    same_object(&previous.object, &object),
                    "conflicting overflow range"
                );
                transaction.commit()?;
                return Ok(previous);
            }
            check_join(&previous.object, &object)?;
        }
        if let Some(next) = neighbor(
            &transaction,
            object.stream_id(),
            object.start().next_sequence(),
            false,
        )? {
            check_join(&object, &next.object)?;
        }
        ensure!(
            (range_count as u64) < self.limits.max_ranges_per_stream as u64,
            "overflow range limit reached"
        );
        let metadata = object.encode();
        transaction.execute(
            "INSERT INTO ranges VALUES (?1,?2,?3,?4,?5,0)",
            params![
                object.stream_id().as_bytes().as_slice(),
                object.start().next_sequence().to_be_bytes().as_slice(),
                object.end().next_sequence().to_be_bytes().as_slice(),
                &metadata,
                digest(&metadata).as_slice()
            ],
        )?;
        transaction.execute(
            "UPDATE streams SET range_count=range_count+1 WHERE stream_id=?1",
            [object.stream_id().as_bytes().as_slice()],
        )?;
        transaction.commit()?;
        Ok(IndexedOverflowRange {
            object,
            verified: false,
        })
    }

    /// Upload exact sealed bytes, verify the stored copy, then commit its receipt.
    /// An error leaves the intent pending and the caller must retain the source bytes.
    pub fn upload(
        &mut self,
        reader_id: &str,
        segment: &SealedSegmentV1,
        store: &dyn ImmutableObjectStore,
    ) -> Result<OverflowObjectV1> {
        let intent = self.begin_upload(reader_id, segment)?;
        let request = object_request(&intent.object)?;
        store.create_if_absent(&request, &mut std::io::Cursor::new(segment.encode()))?;
        self.verify_upload(reader_id, &intent.object, store)
    }

    /// Resume an upload that reached storage but did not commit its local receipt.
    /// A supplied metadata value alone never changes range status.
    pub fn verify_upload(
        &mut self,
        reader_id: &str,
        expected: &OverflowObjectV1,
        store: &dyn ImmutableObjectStore,
    ) -> Result<OverflowObjectV1> {
        self.stream(reader_id, expected.stream_id())?;
        let indexed = exact_range(
            &self.connection,
            expected.stream_id(),
            expected.start().next_sequence(),
        )?
        .context("overflow upload has no durable intent")?;
        ensure!(
            same_object(&indexed.object, expected),
            "upload differs from its durable intent"
        );
        let metadata = store.verify_exact(&object_request(&indexed.object)?)?;
        let version = metadata.version.map(|version| version.as_bytes().to_vec());
        if indexed.verified {
            ensure!(
                indexed.object.object_version() == version.as_deref(),
                "verified object version changed"
            );
            return Ok(indexed.object);
        }
        let verified = OverflowObjectV1::new(
            expected.stream_id(),
            expected.start(),
            expected.end(),
            expected.encoded_len(),
            *expected.encoded_sha256(),
            version,
        )?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = exact_range(
            &transaction,
            expected.stream_id(),
            expected.start().next_sequence(),
        )?
        .context("upload intent disappeared")?;
        ensure!(
            same_object(&current.object, expected),
            "upload intent changed"
        );
        if current.verified {
            ensure!(
                current.object == verified,
                "verified object receipt changed"
            );
        } else {
            let encoded = verified.encode();
            transaction.execute(
                "UPDATE ranges SET object=?1,object_digest=?2,verified=1 WHERE stream_id=?3 AND first_sequence=?4",
                params![&encoded, digest(&encoded).as_slice(), expected.stream_id().as_bytes().as_slice(),
                    expected.start().next_sequence().to_be_bytes().as_slice()],
            )?;
        }
        transaction.commit()?;
        Ok(verified)
    }

    /// Return one indexed range containing a record sequence. Pending intents are visible for
    /// upload recovery, but `read_segment` refuses to expose them as recovered data.
    pub fn containing(
        &self,
        reader_id: &str,
        stream_id: StreamId,
        sequence: u64,
    ) -> Result<Option<IndexedOverflowRange>> {
        self.stream(reader_id, stream_id)?;
        Ok(neighbor(&self.connection, stream_id, sequence, true)?
            .filter(|range| sequence < range.object.end().next_sequence()))
    }

    /// Enumerate bounded recovery work, including pending uploads, in a consistent snapshot.
    /// Includes a range containing `from_sequence`. Continue at the last returned range's end.
    /// Missing ranges remain gaps; callers must not interpret a page as contiguous coverage.
    pub fn ranges_from(
        &self,
        reader_id: &str,
        stream_id: StreamId,
        from_sequence: u64,
        limit: usize,
    ) -> Result<Vec<IndexedOverflowRange>> {
        ensure!(
            (1..=1024).contains(&limit),
            "range page limit must be 1..=1024"
        );
        let transaction = self.connection.unchecked_transaction()?;
        self.stream(reader_id, stream_id)?;
        let first = neighbor(&transaction, stream_id, from_sequence, true)?
            .filter(|range| from_sequence < range.object.end().next_sequence())
            .map_or(from_sequence, |range| range.object.start().next_sequence());
        let result = {
            let mut statement = transaction.prepare(
                "SELECT first_sequence,next_sequence,object,object_digest,verified FROM ranges
                 WHERE stream_id=?1 AND first_sequence>=?2 ORDER BY first_sequence LIMIT ?3",
            )?;
            let mut rows = statement.query(params![
                stream_id.as_bytes().as_slice(),
                first.to_be_bytes().as_slice(),
                limit as i64
            ])?;
            let mut ranges = Vec::new();
            while let Some(row) = rows.next()? {
                ranges.push(decode_row(row, stream_id)?);
            }
            ranges
        };
        transaction.commit()?;
        Ok(result)
    }

    /// Read and revalidate one complete segment. No record escapes before its full digest and
    /// prefix chain pass. Memory is bounded by the configured encoded segment limit plus decoding.
    pub fn read_segment(
        &self,
        reader_id: &str,
        stream_id: StreamId,
        sequence: u64,
        store: &dyn ImmutableObjectStore,
    ) -> Result<SealedSegmentV1> {
        let stream = self.stream(reader_id, stream_id)?;
        let range = self
            .containing(reader_id, stream_id, sequence)?
            .context("overflow range is missing")?;
        ensure!(range.verified, "overflow range is not verified");
        ensure!(
            range.object.encoded_len() <= self.limits.max_segment_bytes,
            "overflow segment exceeds byte limit"
        );
        let read = store.open(&ObjectKey::new(range.object.object_key())?)?;
        ensure!(
            read.metadata.key.as_str() == range.object.object_key(),
            "object key mismatch"
        );
        ensure!(
            read.metadata.encoded_len == range.object.encoded_len(),
            "object length mismatch"
        );
        ensure!(
            read.metadata
                .version
                .as_ref()
                .map(|version| version.as_bytes())
                == range.object.object_version(),
            "object version mismatch"
        );
        let mut bytes = Vec::new();
        read.body
            .take(
                range
                    .object
                    .encoded_len()
                    .checked_add(1)
                    .context("segment length overflow")?,
            )
            .read_to_end(&mut bytes)?;
        let segment = range.object.validate_segment(&bytes)?;
        ensure!(
            segment.header().stream() == stream,
            "segment stream header mismatch"
        );
        Ok(segment)
    }

    /// Contiguous *indexed overflow* coverage. This is deliberately not named an ACK.
    pub fn contiguous_verified_end(
        &self,
        reader_id: &str,
        stream_id: StreamId,
    ) -> Result<CursorV1> {
        let stream = self.stream(reader_id, stream_id)?;
        let mut cursor = stream.initial_cursor();
        loop {
            let Some(range) = exact_range(&self.connection, stream_id, cursor.next_sequence())?
            else {
                break;
            };
            if !range.verified {
                break;
            }
            ensure!(range.object.start() == cursor, "overflow prefix mismatch");
            cursor = range.object.end();
        }
        Ok(cursor)
    }

    fn stream(&self, reader_id: &str, stream_id: StreamId) -> Result<StreamHeaderV1> {
        let (owner, header): (String, Vec<u8>) = self
            .connection
            .query_row(
                "SELECT reader_id,header FROM streams WHERE stream_id=?1",
                [stream_id.as_bytes().as_slice()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .context("unknown overflow stream")?;
        ensure!(owner == reader_id, "reader does not own this stream");
        let stream = StreamHeaderV1::decode(&header)?;
        ensure!(
            stream.stream_id() == stream_id,
            "catalog stream identity mismatch"
        );
        Ok(stream)
    }

    /// Read-only local inspection; never initializes an absent catalog or contacts storage.
    pub fn inspect(state_dir: &Path) -> Result<Vec<OverflowStreamStatus>> {
        let catalog = Self::read_only(state_dir, OverflowLimits::default())?;
        catalog.connection.execute_batch("BEGIN DEFERRED")?;
        catalog.statuses()
    }

    /// Open existing recovery metadata without creating or changing catalog records.
    pub fn open_read_only(
        state_dir: &Path,
        namespace: OverflowNamespaceSha256,
        limits: OverflowLimits,
    ) -> Result<Self> {
        let catalog = Self::read_only(state_dir, limits)?;
        let stored: Vec<u8> = catalog.connection.query_row(
            "SELECT namespace FROM catalog WHERE id=1",
            [],
            |row| row.get(0),
        )?;
        ensure!(
            stored == namespace.as_bytes(),
            "overflow namespace cannot change"
        );
        Ok(catalog)
    }

    fn read_only(state_dir: &Path, limits: OverflowLimits) -> Result<Self> {
        check_database_paths(state_dir)?;
        let state_dir = state_dir.canonicalize()?;
        let connection = Connection::open_with_flags(
            state_dir.join(DATABASE_FILE),
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )?;
        connection.busy_timeout(Duration::from_secs(5))?;
        let app: i64 = connection.pragma_query_value(None, "application_id", |row| row.get(0))?;
        let version: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
        ensure!(
            app == APPLICATION_ID && version == SCHEMA_VERSION,
            "unsupported overflow catalog"
        );
        Ok(Self { connection, limits })
    }

    fn statuses(&self) -> Result<Vec<OverflowStreamStatus>> {
        let mut statement = self
            .connection
            .prepare("SELECT reader_id,stream_id FROM streams ORDER BY reader_id,stream_id")?;
        let bindings = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
        })?;
        let mut statuses = Vec::new();
        for binding in bindings {
            let (reader_id, id) = binding?;
            let stream_id = StreamId::try_from(id.as_slice())?;
            let mut status = OverflowStreamStatus {
                reader_id: reader_id.clone(),
                stream_id: hex(&id),
                pending_ranges: 0,
                verified_ranges: 0,
                verified_bytes: 0,
                contiguous_verified_next_sequence: self
                    .contiguous_verified_end(&reader_id, stream_id)?
                    .next_sequence(),
            };
            let mut ranges = self.connection.prepare("SELECT first_sequence,next_sequence,object,object_digest,verified FROM ranges WHERE stream_id=?1 ORDER BY first_sequence")?;
            let mut rows = ranges.query([id])?;
            while let Some(row) = rows.next()? {
                let range = decode_row(row, stream_id)?;
                if range.verified {
                    status.verified_ranges += 1;
                    status.verified_bytes = status
                        .verified_bytes
                        .checked_add(range.object.encoded_len())
                        .context("verified byte count overflow")?;
                } else {
                    status.pending_ranges += 1;
                }
            }
            statuses.push(status);
        }
        Ok(statuses)
    }
}

fn bind_stream(
    connection: &Connection,
    limits: OverflowLimits,
    reader_id: &str,
    stream: StreamHeaderV1,
) -> Result<i64> {
    let binding: Option<(String, Vec<u8>, i64)> = connection
        .query_row(
            "SELECT reader_id, header, range_count FROM streams WHERE stream_id=?1",
            [stream.stream_id().as_bytes().as_slice()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    if let Some((owner, header, count)) = binding {
        ensure!(
            owner == reader_id && header == stream.fixed_encode(),
            "stream binding cannot change"
        );
        return Ok(count);
    }
    let count: i64 = connection.query_row("SELECT count(*) FROM streams", [], |row| row.get(0))?;
    ensure!(
        (count as u64) < limits.max_streams as u64,
        "overflow stream limit reached"
    );
    connection.execute(
        "INSERT INTO streams(stream_id,reader_id,header) VALUES (?1,?2,?3)",
        params![
            stream.stream_id().as_bytes().as_slice(),
            reader_id,
            stream.fixed_encode().as_slice()
        ],
    )?;
    Ok(0)
}

fn neighbor(
    connection: &Connection,
    stream: StreamId,
    sequence: u64,
    previous: bool,
) -> Result<Option<IndexedOverflowRange>> {
    let sql = if previous {
        "SELECT first_sequence,next_sequence,object,object_digest,verified FROM ranges WHERE stream_id=?1 AND first_sequence<=?2 ORDER BY first_sequence DESC LIMIT 1"
    } else {
        "SELECT first_sequence,next_sequence,object,object_digest,verified FROM ranges WHERE stream_id=?1 AND first_sequence>?2 ORDER BY first_sequence LIMIT 1"
    };
    let mut statement = connection.prepare(sql)?;
    let mut rows = statement.query(params![
        stream.as_bytes().as_slice(),
        sequence.to_be_bytes().as_slice()
    ])?;
    rows.next()?.map(|row| decode_row(row, stream)).transpose()
}

fn exact_range(
    connection: &Connection,
    stream: StreamId,
    first: u64,
) -> Result<Option<IndexedOverflowRange>> {
    Ok(neighbor(connection, stream, first, true)?
        .filter(|range| range.object.start().next_sequence() == first))
}

fn decode_row(row: &rusqlite::Row<'_>, stream: StreamId) -> Result<IndexedOverflowRange> {
    let encoded = row.get_ref(2)?.as_blob()?;
    ensure!(
        encoded.len() <= MAX_OVERFLOW_OBJECT_V1_ENCODED_LEN,
        "oversized overflow metadata"
    );
    ensure!(
        digest(encoded).as_slice() == row.get_ref(3)?.as_blob()?,
        "overflow metadata checksum mismatch"
    );
    let object = OverflowObjectV1::decode(encoded)?;
    ensure!(
        object.stream_id() == stream,
        "overflow metadata stream mismatch"
    );
    ensure!(
        object.start().next_sequence().to_be_bytes().as_slice() == row.get_ref(0)?.as_blob()?
            && object.end().next_sequence().to_be_bytes().as_slice()
                == row.get_ref(1)?.as_blob()?,
        "overflow range columns mismatch"
    );
    ensure!(
        object.start().next_sequence() < object.end().next_sequence(),
        "empty indexed range"
    );
    let verified: i64 = row.get(4)?;
    ensure!(
        verified == 0 || verified == 1,
        "invalid overflow verification state"
    );
    Ok(IndexedOverflowRange {
        object,
        verified: verified == 1,
    })
}

fn check_join(left: &OverflowObjectV1, right: &OverflowObjectV1) -> Result<()> {
    ensure!(
        left.end().next_sequence() <= right.start().next_sequence(),
        "overlapping overflow ranges"
    );
    if left.end().next_sequence() == right.start().next_sequence() {
        ensure!(
            left.end() == right.start(),
            "overflow range prefix mismatch"
        );
    }
    Ok(())
}

fn same_object(left: &OverflowObjectV1, right: &OverflowObjectV1) -> bool {
    left.stream_id() == right.stream_id()
        && left.start() == right.start()
        && left.end() == right.end()
        && left.encoded_len() == right.encoded_len()
        && left.encoded_sha256() == right.encoded_sha256()
}

fn object_request(object: &OverflowObjectV1) -> Result<CreateRequest> {
    Ok(CreateRequest {
        key: ObjectKey::new(object.object_key())?,
        encoded_len: object.encoded_len(),
        encoded_sha256: *object.encoded_sha256(),
    })
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn validate_reader_id(reader: &str) -> Result<()> {
    ensure!(
        !reader.is_empty()
            && reader.len() <= 128
            && reader
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "invalid reader ID"
    );
    Ok(())
}

fn prepare_directory(path: &Path) -> Result<()> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut missing = Vec::new();
    let mut cursor = absolute.as_path();
    while !cursor.try_exists()? {
        missing.push(cursor.to_path_buf());
        cursor = cursor
            .parent()
            .context("catalog directory has no existing ancestor")?;
    }
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)?;
    check_state_directory(path)?;
    // Persist each newly created ancestor's directory entry as well as the state directory.
    for directory in missing.iter().rev() {
        fs::File::open(directory.parent().context("catalog parent missing")?)?.sync_all()?;
    }
    fs::File::open(path)?.sync_all()?;
    Ok(())
}

fn check_state_directory(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "catalog state must be a directory, not a symlink"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // SAFETY: geteuid has no arguments or memory effects.
        ensure!(
            metadata.uid() == unsafe { libc::geteuid() } && metadata.mode() & 0o022 == 0,
            "catalog directory must be owned by this user and not writable by other users"
        );
    }
    Ok(())
}

fn check_database_paths(path: &Path) -> Result<()> {
    check_state_directory(path)?;
    for name in [
        DATABASE_FILE,
        "overflow.sqlite3-wal",
        "overflow.sqlite3-shm",
        "overflow.sqlite3-journal",
    ] {
        match fs::symlink_metadata(path.join(name)) {
            Ok(metadata) => ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "catalog file must be regular, not a symlink"
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
