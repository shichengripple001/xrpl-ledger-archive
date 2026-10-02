//! Writer for xrpld's `transaction.db` (`Transactions` + `AccountTransactions`), the tables
//! xrpld answers `account_tx` and `tx` from. Schema is `kTxDbInit` (`include/xrpl/rdb/DBInit.h`),
//! row format is `STTx::getMetaSQL` / `saveValidatedLedger` (`Node.cpp`).
//!
//! The file is built as `<path>.tmp` with no indexes (bulk insert is far faster that way), the
//! indexes are created last, and only then is it renamed into place — a crashed import never
//! leaves a half-written `transaction.db` that xrpld would trust.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use rusqlite::{params, Connection};

use xrla_common::chunk::TxMap;
use xrla_common::meta_decode::{account_id_to_classic_address, parse_tx_fields, summarize_meta};
use xrla_common::tx_types::tx_type_name;

const SCHEMA_TABLES: &str = "
CREATE TABLE IF NOT EXISTS Transactions (
    TransID     CHARACTER(64) PRIMARY KEY,
    TransType   CHARACTER(24),
    FromAcct    CHARACTER(35),
    FromSeq     BIGINT UNSIGNED,
    LedgerSeq   BIGINT UNSIGNED,
    Status      CHARACTER(1),
    RawTxn      BLOB,
    TxnMeta     BLOB
);
CREATE TABLE IF NOT EXISTS AccountTransactions (
    TransID     CHARACTER(64),
    Account     CHARACTER(64),
    LedgerSeq   BIGINT UNSIGNED,
    TxnSeq      INTEGER
);";

const SCHEMA_INDEXES: &str = "
CREATE INDEX IF NOT EXISTS TxLgrIndex ON Transactions(LedgerSeq);
CREATE INDEX IF NOT EXISTS AcctTxIDIndex ON AccountTransactions(TransID);
CREATE INDEX IF NOT EXISTS AcctTxIndex ON AccountTransactions(Account, LedgerSeq, TxnSeq, TransID);
CREATE INDEX IF NOT EXISTS AcctLgrIndex ON AccountTransactions(LedgerSeq, Account, TransID);";

pub struct TxDbSink {
    conn: Option<Connection>,
    tmp: PathBuf,
    dest: PathBuf,
    rows_tx: u64,
    rows_acct: u64,
}

impl TxDbSink {
    /// Refuses an existing destination: xrpld owns that file if it is running, and merging into
    /// it would mix our rows with its own.
    pub fn create(dest: &Path) -> Result<Self> {
        if dest.exists() {
            bail!("{} already exists; refusing to overwrite or merge", dest.display());
        }
        let mut tmp = dest.as_os_str().to_owned();
        tmp.push(".tmp");
        let tmp = PathBuf::from(tmp);
        let _ = std::fs::remove_file(&tmp);
        let conn = Connection::open(&tmp).with_context(|| format!("create {}", tmp.display()))?;
        conn.execute_batch(
            "PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA page_size=4096; \
             PRAGMA cache_size=-262144; BEGIN;",
        )?;
        conn.execute_batch(SCHEMA_TABLES)?;
        Ok(Self { conn: Some(conn), tmp, dest: dest.to_owned(), rows_tx: 0, rows_acct: 0 })
    }

    /// Writes one ledger's transactions. Any transaction that cannot be decoded is an error:
    /// a silently skipped row would be missing history that nothing reports.
    pub fn write_ledger(&mut self, m: &TxMap) -> Result<()> {
        let conn = self.conn.as_ref().expect("write after finish");
        let mut ins_tx = conn.prepare_cached(
            "INSERT OR REPLACE INTO Transactions \
             (TransID, TransType, FromAcct, FromSeq, LedgerSeq, Status, RawTxn, TxnMeta) \
             VALUES (?1,?2,?3,?4,?5,'V',?6,?7)",
        )?;
        let mut ins_acct = conn.prepare_cached(
            "INSERT INTO AccountTransactions (TransID, Account, LedgerSeq, TxnSeq) VALUES (?1,?2,?3,?4)",
        )?;
        for tx in &m.txns {
            let id = hex::encode_upper(tx.tx_hash);
            let f = parse_tx_fields(&tx.tx_blob)
                .with_context(|| format!("ledger {} tx {id}: transaction fields", m.ledger_seq))?;
            let name = tx_type_name(f.tx_type).with_context(|| {
                format!("ledger {} tx {id}: unknown transaction type {}", m.ledger_seq, f.tx_type)
            })?;
            let s = summarize_meta(&tx.meta_blob)
                .with_context(|| format!("ledger {} tx {id}: metadata", m.ledger_seq))?;
            ins_tx.execute(params![
                id,
                name,
                account_id_to_classic_address(&f.account),
                f.sequence,
                m.ledger_seq,
                tx.tx_blob,
                tx.meta_blob
            ])?;
            self.rows_tx += 1;
            for a in &s.affected_accounts {
                ins_acct.execute(params![
                    id,
                    account_id_to_classic_address(a),
                    m.ledger_seq,
                    s.transaction_index
                ])?;
                self.rows_acct += 1;
            }
        }
        Ok(())
    }

    pub fn counts(&self) -> (u64, u64) {
        (self.rows_tx, self.rows_acct)
    }

    pub fn finish(mut self) -> Result<()> {
        let conn = self.conn.take().expect("finish twice");
        conn.execute_batch("COMMIT;")?;
        conn.execute_batch(SCHEMA_INDEXES)?;
        conn.close().map_err(|(_, e)| e)?;
        std::fs::rename(&self.tmp, &self.dest)
            .with_context(|| format!("publish {}", self.dest.display()))?;
        Ok(())
    }
}

impl Drop for TxDbSink {
    fn drop(&mut self) {
        // Still holding the connection means finish() never ran: remove the unpublished file.
        if self.conn.take().is_some() {
            let _ = std::fs::remove_file(&self.tmp);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xrla_common::chunk::TxRecord;

    #[test]
    fn refuses_existing_destination_and_cleans_up_when_unfinished() {
        let dir = std::env::temp_dir().join(format!("xrla-txdb-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let dest = dir.join("transaction.db");
        {
            let _s = TxDbSink::create(&dest).unwrap();
        }
        assert!(!dest.exists());
        assert!(!dir.join("transaction.db.tmp").exists());
        std::fs::write(&dest, b"x").unwrap();
        assert!(TxDbSink::create(&dest).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn undecodable_transaction_is_an_error_not_a_skipped_row() {
        let dir = std::env::temp_dir().join(format!("xrla-txdb-bad-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut s = TxDbSink::create(&dir.join("transaction.db")).unwrap();
        let m = TxMap {
            ledger_seq: 1,
            ledger_hash: [0; 32],
            account_hash: [0; 32],
            drops: 0,
            parent_close_time: 0,
            close_time: 0,
            close_time_resolution: 10,
            close_flags: 0,
            txns: vec![TxRecord { tx_hash: [1; 32], tx_blob: vec![0xff], meta_blob: vec![] }],
        };
        assert!(s.write_ledger(&m).is_err());
        drop(s);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
