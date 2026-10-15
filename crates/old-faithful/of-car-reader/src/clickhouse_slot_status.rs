//! Insert [`SlotStatus`] rows into a named ClickHouse database.
//!
//! The table is `jetstreamer_slot_status`, with the same columns and engine as
//! Jetstreamer's `--no-plugins` writer. The database comes from the caller.
//! Jetstreamer's DSN does not select one.
//!
//! A full batch is spawned onto the current Tokio runtime and the caller
//! continues without waiting for ClickHouse. Each insert still uses
//! `async_insert=1` and `wait_for_async_insert=1`, so an acknowledgement means
//! the row was flushed. [`SlotStatusSink::finish`] waits until every spawned
//! insert has succeeded or exhausted its retry horizon.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use clickhouse::Client;
use serde::Serialize;
use url::Url;

use crate::slot_status::SlotStatus;

const RETRY_INITIAL: Duration = Duration::from_millis(500);
const RETRY_MAX: Duration = Duration::from_secs(15);
const RETRY_HORIZON: Duration = Duration::from_secs(600);

const CREATE_SLOT_STATUS: &str = r#"
CREATE TABLE IF NOT EXISTS jetstreamer_slot_status (
    slot UInt64,
    transaction_count UInt32 DEFAULT 0,
    vote_transaction_count UInt32 DEFAULT 0,
    non_vote_transaction_count UInt32 DEFAULT 0,
    thread_id UInt8 DEFAULT 0,
    block_time DateTime('UTC') DEFAULT toDateTime(0),
    indexed_at DateTime('UTC') DEFAULT now()
) ENGINE = ReplacingMergeTree(indexed_at)
ORDER BY slot
"#;

#[derive(clickhouse::Row, Serialize)]
struct SlotStatusRow {
    slot: u64,
    transaction_count: u32,
    vote_transaction_count: u32,
    non_vote_transaction_count: u32,
    thread_id: u8,
    block_time: u32,
}

/// ClickHouse connection and insert batch for `jetstreamer_slot_status`.
pub struct SlotStatusSink {
    client: Client,
    pending: Vec<SlotStatusRow>,
    batch_rows: usize,
    writes: Arc<WriteState>,
}

struct WriteState {
    in_flight: AtomicU64,
    written: AtomicU64,
    failed: Mutex<Option<String>>,
}

struct InFlightGuard(Arc<WriteState>);

impl InFlightGuard {
    fn begin(state: &Arc<WriteState>) -> Self {
        state.in_flight.fetch_add(1, Ordering::SeqCst);
        Self(Arc::clone(state))
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

impl SlotStatusSink {
    /// Open a client for `database`.
    ///
    /// `dsn` is an HTTP URL. User and password may be embedded:
    /// `http://default:secret@127.0.0.1:8123`.
    pub fn connect(dsn: &str, database: &str, batch_rows: usize) -> Result<Self, SlotStatusError> {
        check_database_name(database)?;
        let endpoint = split_clickhouse_dsn(dsn)?;
        let mut client = Client::default()
            .with_url(endpoint.url)
            .with_database(database)
            .with_setting("async_insert", "1")
            .with_setting("wait_for_async_insert", "1");
        if let Some(user) = endpoint.user {
            client = client.with_user(user);
        }
        if let Some(password) = endpoint.password {
            client = client.with_password(password);
        }
        Ok(Self {
            client,
            pending: Vec::with_capacity(batch_rows.max(1)),
            batch_rows: batch_rows.max(1),
            writes: Arc::new(WriteState {
                in_flight: AtomicU64::new(0),
                written: AtomicU64::new(0),
                failed: Mutex::new(None),
            }),
        })
    }

    /// Create `jetstreamer_slot_status` in the selected database when it is absent.
    pub async fn ensure_schema(&self) -> Result<(), SlotStatusError> {
        self.client
            .query(CREATE_SLOT_STATUS)
            .execute()
            .await
            .map_err(SlotStatusError::ClickHouse)
    }

    /// Queue one block.
    ///
    /// A full batch is handed to a background task. This method does not wait
    /// for ClickHouse. The caller must run on a multi-thread Tokio runtime so
    /// those tasks progress while the caller blocks on the CAR reader.
    pub async fn push(&mut self, row: SlotStatus) -> Result<(), SlotStatusError> {
        self.raise_background_error()?;
        self.pending.push(SlotStatusRow {
            slot: row.slot,
            transaction_count: row.transaction_count,
            vote_transaction_count: row.vote_transaction_count,
            non_vote_transaction_count: row.non_vote_transaction_count,
            thread_id: row.thread_id,
            block_time: row.block_time,
        });
        if self.pending.len() >= self.batch_rows {
            self.spawn_batch()?;
        }
        Ok(())
    }

    /// Send any queued rows, wait for every background insert, and return the
    /// number of rows ClickHouse acknowledged.
    pub async fn finish(&mut self) -> Result<u64, SlotStatusError> {
        if !self.pending.is_empty() {
            self.spawn_batch()?;
        }
        loop {
            if self.writes.in_flight.load(Ordering::SeqCst) == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        self.raise_background_error()?;
        Ok(self.writes.written.load(Ordering::SeqCst))
    }

    fn spawn_batch(&mut self) -> Result<(), SlotStatusError> {
        self.raise_background_error()?;
        if self.pending.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.pending);
        self.pending.reserve(self.batch_rows);
        let row_count = rows.len() as u64;
        let client = self.client.clone();
        let writes = Arc::clone(&self.writes);
        let guard = InFlightGuard::begin(&writes);
        tokio::spawn(async move {
            let _guard = guard;
            match retry_slot_status_insert(client, rows).await {
                Ok(()) => {
                    writes.written.fetch_add(row_count, Ordering::SeqCst);
                }
                Err(error) => {
                    eprintln!("{error}");
                    let mut failed = writes.failed.lock().expect("slot status error lock");
                    if failed.is_none() {
                        *failed = Some(error);
                    }
                }
            }
        });
        Ok(())
    }

    fn raise_background_error(&self) -> Result<(), SlotStatusError> {
        let failed = self.writes.failed.lock().expect("slot status error lock");
        match failed.as_ref() {
            Some(message) => Err(SlotStatusError::Background(message.clone())),
            None => Ok(()),
        }
    }
}

async fn retry_slot_status_insert(client: Client, rows: Vec<SlotStatusRow>) -> Result<(), String> {
    let started = Instant::now();
    let mut delay = RETRY_INITIAL;
    let mut attempt = 1u32;
    loop {
        match insert_slot_status(&client, &rows).await {
            Ok(()) => {
                if attempt > 1 {
                    eprintln!("ClickHouse slot status insert succeeded on attempt {attempt}");
                }
                return Ok(());
            }
            Err(error) => {
                if started.elapsed() >= RETRY_HORIZON {
                    return Err(format!(
                        "ClickHouse slot status insert still failing after {attempt} attempts: {error}"
                    ));
                }
                eprintln!(
                    "ClickHouse slot status insert failed (attempt {attempt}); retrying in {:.1}s: {error}",
                    delay.as_secs_f64()
                );
                tokio::time::sleep(delay).await;
                delay = next_retry_delay(delay);
                attempt += 1;
            }
        }
    }
}

fn next_retry_delay(current: Duration) -> Duration {
    current.saturating_mul(2).min(RETRY_MAX)
}

async fn insert_slot_status(
    client: &Client,
    rows: &[SlotStatusRow],
) -> Result<(), clickhouse::error::Error> {
    let mut insert = client
        .insert::<SlotStatusRow>("jetstreamer_slot_status")
        .await?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}

#[derive(Debug)]
pub enum SlotStatusError {
    Dsn(String),
    Database(String),
    ClickHouse(clickhouse::error::Error),
    Background(String),
}

impl std::fmt::Display for SlotStatusError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dsn(message) => write!(formatter, "ClickHouse DSN: {message}"),
            Self::Database(message) => write!(formatter, "ClickHouse database: {message}"),
            Self::ClickHouse(error) => write!(formatter, "ClickHouse: {error}"),
            Self::Background(message) => write!(formatter, "{message}"),
        }
    }
}

impl std::error::Error for SlotStatusError {}

struct ClickHouseEndpoint {
    url: String,
    user: Option<String>,
    password: Option<String>,
}

fn split_clickhouse_dsn(dsn: &str) -> Result<ClickHouseEndpoint, SlotStatusError> {
    if dsn.trim().is_empty() {
        return Err(SlotStatusError::Dsn("DSN is empty".to_string()));
    }
    let Ok(mut url) = Url::parse(dsn) else {
        return Ok(ClickHouseEndpoint {
            url: dsn.to_string(),
            user: None,
            password: None,
        });
    };
    let user = {
        let name = url.username();
        if name.is_empty() {
            None
        } else {
            Some(name.to_string())
        }
    };
    let password = url.password().map(str::to_string);
    if user.is_some() || password.is_some() {
        let _ = url.set_username("");
        let _ = url.set_password(None);
    }
    Ok(ClickHouseEndpoint {
        url: url.as_str().to_string(),
        user,
        password,
    })
}

fn check_database_name(name: &str) -> Result<(), SlotStatusError> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err(SlotStatusError::Database(
            "database name is empty".to_string(),
        ));
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(SlotStatusError::Database(format!(
            "database name '{name}' must start with a letter or underscore"
        )));
    }
    if !chars.all(|character| character.is_ascii_alphanumeric() || character == '_') {
        return Err(SlotStatusError::Database(format!(
            "database name '{name}' may contain only letters, digits, and underscores"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dsn_keeps_user_and_password_out_of_the_url() {
        let endpoint = split_clickhouse_dsn("http://default:secret@127.0.0.1:8123").unwrap();
        assert_eq!(endpoint.url, "http://127.0.0.1:8123/");
        assert_eq!(endpoint.user.as_deref(), Some("default"));
        assert_eq!(endpoint.password.as_deref(), Some("secret"));
    }

    #[test]
    fn database_name_rejects_a_path() {
        assert!(check_database_name("default").is_ok());
        assert!(check_database_name("jetstreamer").is_ok());
        assert!(check_database_name("").is_err());
        assert!(check_database_name("default.jetstreamer").is_err());
    }

    #[test]
    fn retry_delay_doubles_until_fifteen_seconds() {
        assert_eq!(
            next_retry_delay(Duration::from_millis(500)),
            Duration::from_secs(1)
        );
        assert_eq!(
            next_retry_delay(Duration::from_secs(8)),
            Duration::from_secs(15)
        );
        assert_eq!(
            next_retry_delay(Duration::from_secs(15)),
            Duration::from_secs(15)
        );
    }
}
