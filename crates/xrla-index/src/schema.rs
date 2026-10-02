//! On-disk layout of a per-chunk index (SQLite, version 1).
//!
//! Everything is keyed by logical coordinates — `(ledger_seq, txn_seq)` — never byte offsets, so
//! the index stays valid across any change to how a chunk is laid out on disk.

/// Bump when the table layout changes.
pub const SCHEMA_VERSION: u32 = 1;

/// Names the account rule that produced the rows (`xrla_common::meta_decode`). An index built
/// under a different rule must be rejected, not trusted: a wrong rule yields silently incomplete
/// account histories.
pub const RULE_ID: &str = "xrpld-getAffectedAccounts/1";

pub const SCHEMA_SQL: &str = "
CREATE TABLE meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;

-- One row per (transaction, affected account). The key is the query order: an account's rows are
-- contiguous and newest-first, so `account_tx` is one seek plus a forward scan.
CREATE TABLE account_tx (
    account_id BLOB    NOT NULL,   -- raw 20-byte AccountID, not the r-address
    ledger_seq INTEGER NOT NULL,
    txn_seq    INTEGER NOT NULL,   -- meta.TransactionIndex: apply order within the ledger
    tx_hash    BLOB    NOT NULL,   -- raw 32 bytes
    PRIMARY KEY (account_id, ledger_seq DESC, txn_seq DESC)
) WITHOUT ROWID;

CREATE TABLE tx_locator (
    tx_hash    BLOB PRIMARY KEY,   -- raw 32 bytes
    ledger_seq INTEGER NOT NULL,
    txn_seq    INTEGER NOT NULL
) WITHOUT ROWID;
";
