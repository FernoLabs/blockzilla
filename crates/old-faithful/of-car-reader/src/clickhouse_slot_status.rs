//! Insert [`SlotStatus`] rows into a named ClickHouse database.
//!
//! The table is `jetstreamer_slot_status`, with the same columns and engine as
//! Jetstreamer's `--no-plugins` writer. The database comes from the caller.
//! Jetstreamer's DSN does not select one.

use clickhouse::Client;
use serde::Serialize;
use url::Url;

use crate::slot_status::SlotStatus;

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
    written: u64,
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
            written: 0,
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

    /// Queue one block. A full batch is inserted immediately.
    pub async fn push(&mut self, row: SlotStatus) -> Result<(), SlotStatusError> {
        self.pending.push(SlotStatusRow {
            slot: row.slot,
            transaction_count: row.transaction_count,
            vote_transaction_count: row.vote_transaction_count,
            non_vote_transaction_count: row.non_vote_transaction_count,
            thread_id: row.thread_id,
            block_time: row.block_time,
        });
        if self.pending.len() >= self.batch_rows {
            self.flush().await?;
        }
        Ok(())
    }

    /// Insert every queued row and return the number of rows sent.
    pub async fn finish(&mut self) -> Result<u64, SlotStatusError> {
        self.flush().await?;
        Ok(self.written)
    }

    async fn flush(&mut self) -> Result<(), SlotStatusError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let mut insert = self
            .client
            .insert::<SlotStatusRow>("jetstreamer_slot_status")
            .await
            .map_err(SlotStatusError::ClickHouse)?;
        for row in &self.pending {
            insert.write(row).await.map_err(SlotStatusError::ClickHouse)?;
        }
        insert.end().await.map_err(SlotStatusError::ClickHouse)?;
        self.written += self.pending.len() as u64;
        self.pending.clear();
        Ok(())
    }
}

#[derive(Debug)]
pub enum SlotStatusError {
    Dsn(String),
    Database(String),
    ClickHouse(clickhouse::error::Error),
}

impl std::fmt::Display for SlotStatusError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dsn(message) => write!(formatter, "ClickHouse DSN: {message}"),
            Self::Database(message) => write!(formatter, "ClickHouse database: {message}"),
            Self::ClickHouse(error) => write!(formatter, "ClickHouse: {error}"),
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
}
