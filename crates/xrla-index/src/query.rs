//! Read side of the per-chunk index: `account_tx` and `tx`.
use std::collections::HashMap;
use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};
use rusqlite::{params, Connection, OpenFlags, Row};

use xrla_common::shamap::Hash256;

use crate::schema::{RULE_ID, SCHEMA_VERSION};

#[derive(Debug, Clone)]
pub struct IndexMeta {
    pub network_id: u32,
    pub start_ledger: u32,
    pub end_ledger: u32,
    /// Hex of the `chunk_hash` this index was built from.
    pub chunk_hash: String,
    pub ledgers: u64,
    pub transactions: u64,
    pub rows: u64,
    pub rule_id: String,
}

/// Where the previous page stopped, in xrpld's `account_tx` marker terms: `{ledger, seq}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Marker {
    pub ledger_seq: u32,
    pub txn_seq: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountTxRow {
    pub ledger_seq: u32,
    pub txn_seq: u32,
    pub tx_hash: Hash256,
}

pub struct Index {
    conn: Connection,
    pub meta: IndexMeta,
}

impl Index {
    /// Open an index read-only, refusing one that is incomplete, from a different schema, or
    /// built by a different account rule. A wrong rule means silently incomplete histories.
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .with_context(|| format!("opening index {}", path.display()))?;

        let mut kv: HashMap<String, String> = HashMap::new();
        let mut stmt = conn.prepare("SELECT key, value FROM meta")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
        for row in rows {
            let (k, v) = row?;
            kv.insert(k, v);
        }
        drop(stmt);

        let get = |k: &str| -> Result<&String> {
            kv.get(k).ok_or_else(|| anyhow!("index {} is missing `{k}`", path.display()))
        };
        if get("complete")? != "1" {
            bail!("index {} is not complete", path.display());
        }
        if get("schema_version")? != &SCHEMA_VERSION.to_string() {
            bail!("index {} has schema version {}, expected {SCHEMA_VERSION}", path.display(), get("schema_version")?);
        }
        if get("rule_id")? != RULE_ID {
            bail!(
                "index {} was built with account rule {:?}, this build uses {RULE_ID:?}; rebuild it",
                path.display(),
                get("rule_id")?
            );
        }
        let num = |k: &str| -> Result<u64> {
            get(k)?.parse().with_context(|| format!("index meta `{k}` is not a number"))
        };
        let meta = IndexMeta {
            network_id: num("network_id")? as u32,
            start_ledger: num("start_ledger")? as u32,
            end_ledger: num("end_ledger")? as u32,
            chunk_hash: get("chunk_hash")?.clone(),
            ledgers: num("ledgers")?,
            transactions: num("transactions")?,
            rows: num("rows")?,
            rule_id: get("rule_id")?.clone(),
        };
        Ok(Self { conn, meta })
    }

    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    /// Transactions that affected `account` in ledgers `min..=max`, ordered newest-first
    /// (`forward = false`, the `account_tx` default) or oldest-first, at most `limit` of them.
    /// Pass the last row of the previous page as `marker` to continue exactly after it.
    pub fn account_tx(
        &self,
        account: &[u8; 20],
        min: u32,
        max: u32,
        limit: usize,
        forward: bool,
        marker: Option<Marker>,
    ) -> Result<Vec<AccountTxRow>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let (cmp, order) = if forward { (">", "ASC") } else { ("<", "DESC") };
        let limit = limit as i64;
        let out: Result<Vec<AccountTxRow>, rusqlite::Error> = match marker {
            None => {
                let sql = format!(
                    "SELECT ledger_seq, txn_seq, tx_hash FROM account_tx \
                     WHERE account_id = ?1 AND ledger_seq BETWEEN ?2 AND ?3 \
                     ORDER BY ledger_seq {order}, txn_seq {order} LIMIT ?4"
                );
                let mut stmt = self.conn.prepare(&sql)?;
                let rows = stmt.query_map(params![&account[..], min, max, limit], map_row)?;
                rows.collect()
            }
            Some(m) => {
                let sql = format!(
                    "SELECT ledger_seq, txn_seq, tx_hash FROM account_tx \
                     WHERE account_id = ?1 AND ledger_seq BETWEEN ?2 AND ?3 \
                       AND (ledger_seq, txn_seq) {cmp} (?4, ?5) \
                     ORDER BY ledger_seq {order}, txn_seq {order} LIMIT ?6"
                );
                let mut stmt = self.conn.prepare(&sql)?;
                let rows = stmt.query_map(
                    params![&account[..], min, max, m.ledger_seq, m.txn_seq, limit],
                    map_row,
                )?;
                rows.collect()
            }
        };
        Ok(out?)
    }

    /// `(ledger_seq, txn_seq)` of a transaction, if it is in this chunk.
    pub fn locate_tx(&self, hash: &Hash256) -> Result<Option<(u32, u32)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT ledger_seq, txn_seq FROM tx_locator WHERE tx_hash = ?1")?;
        let mut rows = stmt.query(params![&hash[..]])?;
        match rows.next()? {
            Some(r) => Ok(Some((r.get(0)?, r.get(1)?))),
            None => Ok(None),
        }
    }
}

fn map_row(r: &Row) -> rusqlite::Result<AccountTxRow> {
    let blob: Vec<u8> = r.get(2)?;
    let tx_hash = <Hash256>::try_from(blob.as_slice()).map_err(|_| {
        rusqlite::Error::InvalidColumnType(2, "tx_hash".into(), rusqlite::types::Type::Blob)
    })?;
    Ok(AccountTxRow { ledger_seq: r.get(0)?, txn_seq: r.get(1)?, tx_hash })
}
