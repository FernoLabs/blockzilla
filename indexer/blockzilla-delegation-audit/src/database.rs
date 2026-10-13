use std::{
    cell::Cell,
    fs,
    io::Write,
    path::Path,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;

use crate::scan::{Approval, BlockProjection, STATE_ADDRESS};

pub struct Database {
    connection: Connection,
    candidate_export: Cell<Option<(Instant, i64)>>,
}

impl Database {
    pub fn open(path: &Path, start_slot: u64) -> Result<Self> {
        let connection = Connection::open(path)?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;
             CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS epochs(
               epoch INTEGER PRIMARY KEY, binding_json TEXT NOT NULL, first_row INTEGER NOT NULL,
               next_row INTEGER NOT NULL, total_rows INTEGER NOT NULL, state TEXT NOT NULL,
               error TEXT, committed_transactions INTEGER NOT NULL DEFAULT 0,
               approval_events INTEGER NOT NULL DEFAULT 0, candidate_accounts INTEGER NOT NULL DEFAULT 0);
             CREATE TABLE IF NOT EXISTS block_commits(
               epoch INTEGER NOT NULL REFERENCES epochs(epoch), block_row INTEGER NOT NULL,
               slot INTEGER NOT NULL, block_id INTEGER NOT NULL, transactions INTEGER NOT NULL,
               approvals INTEGER NOT NULL, PRIMARY KEY(epoch,block_row));
             CREATE TABLE IF NOT EXISTS approvals(
               epoch INTEGER NOT NULL, slot INTEGER NOT NULL, tx_index INTEGER NOT NULL,
               outer_ix INTEGER NOT NULL, inner_ix INTEGER NOT NULL, batch_ix INTEGER NOT NULL,
               token_account TEXT NOT NULL, token_program TEXT NOT NULL, delegate TEXT NOT NULL,
               authority TEXT NOT NULL, instruction_tag INTEGER NOT NULL, amount TEXT NOT NULL,
               mint TEXT, decimals INTEGER, first_signature TEXT NOT NULL,
               instruction_data BLOB NOT NULL, account_indices BLOB NOT NULL,
               parent_instruction_data BLOB, parent_account_indices BLOB,
               PRIMARY KEY(epoch,slot,tx_index,outer_ix,inner_ix,batch_ix));
             CREATE TABLE IF NOT EXISTS candidates(token_account TEXT PRIMARY KEY);
             CREATE INDEX IF NOT EXISTS approvals_account ON approvals(token_account);"
        )?;
        let spec = serde_json::to_string(&serde_json::json!({
            "version": 1, "delegate": STATE_ADDRESS, "start_slot": start_slot,
            "programs": [crate::scan::CLASSIC_ADDRESS, crate::scan::TOKEN_2022_ADDRESS],
            "instruction_tags": [4,13], "token_batch_expansion":true, "successful_only": true,
        }))?;
        connection.execute("INSERT OR IGNORE INTO settings VALUES('spec',?1)", [&spec])?;
        let stored: String =
            connection.query_row("SELECT value FROM settings WHERE key='spec'", [], |r| {
                r.get(0)
            })?;
        ensure!(stored == spec, "database audit specification changed");
        Ok(Self {
            connection,
            candidate_export: Cell::new(None),
        })
    }

    pub fn begin_epoch(
        &mut self,
        epoch: u64,
        binding: &str,
        first_row: usize,
        total_rows: usize,
    ) -> Result<usize> {
        let tx = self.connection.transaction()?;
        let existing: Option<(String, i64, i64, i64)> = tx
            .query_row(
                "SELECT binding_json,first_row,next_row,total_rows FROM epochs WHERE epoch=?1",
                [sql(epoch)?],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        let next = if let Some((saved_binding, saved_first, next, saved_total)) = existing {
            let saved_first = usize::try_from(saved_first)?;
            let next = usize::try_from(next)?;
            let saved_total = usize::try_from(saved_total)?;
            ensure!(
                saved_binding == binding && saved_first == first_row && saved_total == total_rows,
                "epoch {epoch} source binding or scan range changed; use a new database"
            );
            ensure!(
                next >= first_row && next <= total_rows,
                "invalid epoch checkpoint"
            );
            tx.execute(
                "UPDATE epochs SET state='scanning',error=NULL WHERE epoch=?1",
                [sql(epoch)?],
            )?;
            next
        } else {
            tx.execute("INSERT INTO epochs(epoch,binding_json,first_row,next_row,total_rows,state,error) VALUES(?1,?2,?3,?3,?4,'scanning',NULL)",
                       params![sql(epoch)?, binding, sql(first_row as u64)?, sql(total_rows as u64)?])?;
            first_row
        };
        tx.commit()?;
        Ok(next)
    }

    /// Events, unique candidates, block receipts and the durable next row are one commit.
    pub fn commit(&mut self, epoch: u64, blocks: &[BlockProjection]) -> Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }
        let tx = self.connection.transaction()?;
        let saved_next: i64 = tx.query_row(
            "SELECT next_row FROM epochs WHERE epoch=?1",
            [sql(epoch)?],
            |r| r.get(0),
        )?;
        let mut next = usize::try_from(saved_next)?;
        let mut transactions = 0u64;
        let mut approvals = 0u64;
        let mut candidates = 0u64;
        for block in blocks {
            if block.row < next {
                // Idempotent retry: compare the complete receipt and all exact event bytes.
                let existing: (i64, u32, i64, i64) = tx.query_row(
                    "SELECT slot,block_id,transactions,approvals FROM block_commits WHERE epoch=?1 AND block_row=?2",
                    params![sql(epoch)?, sql(block.row as u64)?], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)),
                )?;
                ensure!(
                    existing
                        == (
                            sql(block.slot)?,
                            block.block_id,
                            sql(block.transactions)?,
                            sql(block.events.len() as u64)?
                        ),
                    "retry block receipt changed"
                );
                for event in &block.events {
                    verify_event(&tx, event)?;
                }
                continue;
            }
            ensure!(
                block.row == next,
                "non-contiguous block checkpoint: expected {next}, received {}",
                block.row
            );
            tx.execute(
                "INSERT INTO block_commits VALUES(?1,?2,?3,?4,?5,?6)",
                params![
                    sql(epoch)?,
                    sql(block.row as u64)?,
                    sql(block.slot)?,
                    block.block_id,
                    sql(block.transactions)?,
                    sql(block.events.len() as u64)?
                ],
            )?;
            for event in &block.events {
                ensure!(
                    event.epoch == epoch && event.slot == block.slot,
                    "event does not match block receipt"
                );
                tx.execute("INSERT INTO approvals VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)",
                    params![sql(event.epoch)?,sql(event.slot)?,event.tx_index,event.outer_ix,event.inner_ix,event.batch_ix,
                        event.token_account,event.token_program,STATE_ADDRESS,event.authority,event.tag,
                        event.amount,event.mint,event.decimals,event.signature,event.data,event.accounts,event.parent_data,event.parent_accounts])?;
                candidates += tx.execute(
                    "INSERT OR IGNORE INTO candidates VALUES(?1)",
                    [&event.token_account],
                )? as u64;
            }
            next += 1;
            transactions = transactions
                .checked_add(block.transactions)
                .context("transaction counter overflow")?;
            approvals = approvals
                .checked_add(block.events.len() as u64)
                .context("approval counter overflow")?;
        }
        tx.execute("UPDATE epochs SET next_row=?2,committed_transactions=committed_transactions+?3,approval_events=approval_events+?4,candidate_accounts=candidate_accounts+?5 WHERE epoch=?1",
            params![sql(epoch)?,sql(next as u64)?,sql(transactions)?,sql(approvals)?,sql(candidates)?])?;
        tx.commit()?;
        Ok(())
    }

    pub fn finish_epoch(&mut self, epoch: u64) -> Result<()> {
        ensure!(self.connection.execute(
            "UPDATE epochs SET state='complete',error=NULL WHERE epoch=?1 AND next_row=total_rows", [sql(epoch)?])? == 1,
            "epoch is not fully committed");
        Ok(())
    }

    pub fn fail_epoch(&mut self, epoch: u64, error: &str) -> Result<()> {
        self.connection.execute(
            "UPDATE epochs SET state='coverage_unknown',error=?2 WHERE epoch=?1",
            params![sql(epoch)?, error],
        )?;
        Ok(())
    }

    pub fn export(
        &self,
        candidates: &Path,
        status: &Path,
        first: u64,
        last: u64,
        run_state: &str,
        error: Option<&str>,
    ) -> Result<()> {
        // O(number of epochs): no full receipt/event-table scan at each checkpoint.
        let (blocks,transactions,approvals,candidate_count): (i64,i64,i64,i64) = self.connection.query_row(
            "SELECT coalesce(sum(next_row-first_row),0),coalesce(sum(committed_transactions),0),coalesce(sum(approval_events),0),coalesce(sum(candidate_accounts),0) FROM epochs",
            [],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
        let previous_export = self.candidate_export.get();
        let refresh = run_state != "scanning"
            || !candidates.is_file()
            || previous_export.is_none()
            || previous_export.is_some_and(|(time, count)| {
                count != candidate_count && time.elapsed() >= Duration::from_secs(60)
            });
        if refresh {
            atomic_write(candidates, |file| {
                let mut query = self
                    .connection
                    .prepare("SELECT token_account FROM candidates ORDER BY token_account")?;
                for account in query.query_map([], |r| r.get::<_, String>(0))? {
                    writeln!(file, "{}", account?)?;
                }
                Ok(())
            })?;
            self.candidate_export
                .set(Some((Instant::now(), candidate_count)));
        }
        #[derive(Serialize)]
        struct EpochStatus {
            epoch: i64,
            first_row: i64,
            next_row: i64,
            total_rows: i64,
            state: String,
            error: Option<String>,
        }
        let mut query = self.connection.prepare("SELECT epoch,first_row,next_row,total_rows,state,error FROM epochs WHERE epoch BETWEEN ?1 AND ?2 ORDER BY epoch")?;
        let epochs: Vec<EpochStatus> = query
            .query_map(params![sql(first)?, sql(last)?], |r| {
                Ok(EpochStatus {
                    epoch: r.get(0)?,
                    first_row: r.get(1)?,
                    next_row: r.get(2)?,
                    total_rows: r.get(3)?,
                    state: r.get(4)?,
                    error: r.get(5)?,
                })
            })?
            .collect::<rusqlite::Result<_>>()?;
        let complete =
            epochs.len() as u64 == last - first + 1 && epochs.iter().all(|e| e.state == "complete");
        let json = serde_json::json!({
            "kind":"blockzilla-delegation-candidates-v1", "run_state":run_state,
            "history_complete":complete, "epoch_start":first, "epoch_end":last,
            "delegate":STATE_ADDRESS, "candidate_token_accounts":candidate_count,
            "candidate_export_count":self.candidate_export.get().map(|(_,count)|count),
            "approval_events":approvals, "committed_blocks":blocks,
            "committed_transactions":transactions, "epochs":epochs, "error":error,
            "current_delegations_verified":false,
            "cpi_execution_success_verified":false,
            "approval_record_semantics":"Approval instruction records in successful transactions; CPI success is not independently established.",
            "candidate_set_semantics":"Historical candidate superset; current token account reads are required.",
            "coverage_scope":"All available archive block rows in requested epochs from start slot; missing ledger blocks are not independently verified.",
        });
        atomic_write(status, |file| {
            serde_json::to_writer_pretty(file, &json)?;
            Ok(())
        })
    }
}

fn verify_event(tx: &rusqlite::Transaction<'_>, event: &Approval) -> Result<()> {
    let stored: (String,String,String,String,u8,Option<String>,Option<u8>,String,Vec<u8>,Vec<u8>,Option<Vec<u8>>,Option<Vec<u8>>) = tx.query_row(
        "SELECT token_account,token_program,authority,amount,instruction_tag,mint,decimals,first_signature,instruction_data,account_indices,parent_instruction_data,parent_account_indices FROM approvals WHERE epoch=?1 AND slot=?2 AND tx_index=?3 AND outer_ix=?4 AND inner_ix=?5 AND batch_ix=?6",
        params![sql(event.epoch)?,sql(event.slot)?,event.tx_index,event.outer_ix,event.inner_ix,event.batch_ix],
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?,r.get(9)?,r.get(10)?,r.get(11)?)),
    )?;
    ensure!(
        stored
            == (
                event.token_account.clone(),
                event.token_program.clone(),
                event.authority.clone(),
                event.amount.clone(),
                event.tag,
                event.mint.clone(),
                event.decimals,
                event.signature.clone(),
                event.data.clone(),
                event.accounts.clone(),
                event.parent_data.clone(),
                event.parent_accounts.clone()
            ),
        "retry approval bytes changed"
    );
    Ok(())
}

fn sql(value: u64) -> Result<i64> {
    i64::try_from(value).context("coordinate or counter exceeds SQLite signed integer range")
}

fn atomic_write(path: &Path, write: impl FnOnce(&mut fs::File) -> Result<()>) -> Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
    let mut file =
        fs::File::create(&temporary).with_context(|| format!("create {}", temporary.display()))?;
    write(&mut file)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn event() -> Approval {
        Approval {
            epoch: 1049,
            slot: 453_169_000,
            tx_index: 1,
            outer_ix: 0,
            inner_ix: -1,
            batch_ix: -1,
            token_account: "account".into(),
            token_program: crate::scan::CLASSIC_ADDRESS.into(),
            authority: "authority".into(),
            tag: 4,
            amount: u64::MAX.to_string(),
            mint: None,
            decimals: None,
            signature: "signature".into(),
            data: vec![4, 255, 255, 255, 255, 255, 255, 255, 255],
            accounts: vec![1, 2, 0],
            parent_data: None,
            parent_accounts: None,
        }
    }
    fn block(row: usize, events: Vec<Approval>) -> BlockProjection {
        BlockProjection {
            row,
            slot: 453_169_000 + row as u64,
            block_id: row as u32,
            transactions: 4,
            events,
        }
    }
    #[test]
    fn atomic_duplicate_commit_and_restart_preserve_exact_amount() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.sqlite");
        let mut db = Database::open(&path, 386_160_031).unwrap();
        assert_eq!(db.begin_epoch(1049, "binding", 0, 2).unwrap(), 0);
        let blocks = vec![block(0, vec![event()])];
        db.commit(1049, &blocks).unwrap();
        db.commit(1049, &blocks).unwrap();
        let counts: (i64, i64, i64) = db
            .connection
            .query_row(
                "SELECT committed_transactions,approval_events,candidate_accounts FROM epochs",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(counts, (4, 1, 1));
        let amount: String = db
            .connection
            .query_row("SELECT amount FROM approvals", [], |r| r.get(0))
            .unwrap();
        assert_eq!(amount, "18446744073709551615");
        drop(db);
        let mut db = Database::open(&path, 386_160_031).unwrap();
        assert_eq!(db.begin_epoch(1049, "binding", 0, 2).unwrap(), 1);
        assert!(db.begin_epoch(1049, "different binding", 0, 2).is_err());
        let mut next_event = event();
        next_event.slot += 1;
        next_event.amount = "0".into();
        db.commit(1049, &[block(1, vec![next_event])]).unwrap();
        db.finish_epoch(1049).unwrap();
        let candidates = dir.path().join("candidates.txt");
        let status = dir.path().join("status.json");
        db.export(&candidates, &status, 1049, 1049, "complete", None)
            .unwrap();
        assert_eq!(fs::read_to_string(candidates).unwrap(), "account\n");
        let json: serde_json::Value = serde_json::from_slice(&fs::read(status).unwrap()).unwrap();
        assert_eq!(json["history_complete"], true);
        assert_eq!(json["candidate_token_accounts"], 1);
        assert_eq!(json["approval_events"], 2);
        assert_eq!(json["committed_transactions"], 8);
    }
    #[test]
    fn bad_later_event_rolls_back_entire_batch_and_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Database::open(&dir.path().join("audit.sqlite"), 386_160_031).unwrap();
        db.begin_epoch(1049, "binding", 0, 2).unwrap();
        let mut bad = event();
        bad.epoch = 1048;
        assert!(
            db.commit(1049, &[block(0, vec![event()]), block(1, vec![bad])])
                .is_err()
        );
        let rows: i64 = db
            .connection
            .query_row("SELECT count(*) FROM approvals", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 0);
        assert_eq!(db.begin_epoch(1049, "binding", 0, 2).unwrap(), 0);
    }
}
