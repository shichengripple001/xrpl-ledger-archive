//! Build a per-chunk index by streaming the chunk once.
//!
//! The chunk's `chunk_hash` is only checkable after the whole file has been read, so rows are
//! written to `<out>.tmp` while streaming and the index only reaches its final name after that
//! check passes. A bad chunk, a decode failure, or a kill at any point leaves nothing at the
//! final path — and a previously built index there is left exactly as it was.
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use rusqlite::{params, Connection, Statement, Transaction};

use xrla_common::chunk::TxMap;
use xrla_common::meta_decode::summarize_meta;
use xrla_common::serialize::ChunkReader;
use xrla_common::shamap::Hash256;
use xrla_common::tx_tree::calculate_tx_id;

use crate::schema::{RULE_ID, SCHEMA_SQL, SCHEMA_VERSION};

#[derive(Debug, Clone)]
pub struct BuildReport {
    pub network_id: u32,
    pub start_ledger: u32,
    pub end_ledger: u32,
    pub chunk_hash: Hash256,
    pub ledgers: u32,
    pub transactions: u64,
    pub rows: u64,
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut t = path.as_os_str().to_owned();
    t.push(".tmp");
    PathBuf::from(t)
}

/// Removes the temp file when dropped unless disarmed, so an abandoned build never leaves one.
struct TmpGuard {
    path: PathBuf,
    armed: bool,
}

impl Drop for TmpGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// Stream `chunk_path` and write its index to `out_path`.
///
/// Strict by design: any transaction whose id does not match its blob, whose metadata cannot be
/// decoded, or whose `TransactionIndex` values do not form exactly `0..n` for its ledger aborts
/// the build. An index that silently skipped something would present as "no matches".
///
/// `progress(ledgers_done, ledgers_total)` is called as the stream advances.
pub fn build_index(
    chunk_path: &Path,
    out_path: &Path,
    force: bool,
    mut progress: impl FnMut(u32, u32),
) -> Result<BuildReport> {
    if out_path.exists() && !force {
        bail!("{} already exists (use --force to replace it)", out_path.display());
    }

    let mut reader = ChunkReader::open(chunk_path)
        .with_context(|| format!("opening chunk {}", chunk_path.display()))?;
    let (network_id, start_ledger, end_ledger) =
        (reader.network_id, reader.start_ledger, reader.end_ledger);
    let total_ledgers = end_ledger - start_ledger + 1;

    let tmp = tmp_path(out_path);
    let _ = fs::remove_file(&tmp); // a stale one from a killed run
    // Declared before `conn` so the connection is closed before the file is removed.
    let mut guard = TmpGuard { path: tmp.clone(), armed: true };

    let mut conn = Connection::open(&tmp)?;
    conn.execute_batch(
        "PRAGMA page_size = 8192;
         PRAGMA journal_mode = OFF;
         PRAGMA synchronous = OFF;
         PRAGMA cache_size = -1048576;
         PRAGMA temp_store = MEMORY;",
    )?;
    conn.execute_batch(SCHEMA_SQL)?;

    let tx = conn.transaction()?;
    let (ledgers, transactions, rows);
    {
        let mut sink = Sink::new(&tx, start_ledger)?;

        // The checkpoint is the whole account state — none of it is needed here.
        reader.read_checkpoint(|_node| {})?;
        sink.index_ledger(&reader.read_checkpoint_tx_map()?)?;
        progress(sink.ledgers, total_ledgers);

        while let Some((_delta, tx_map)) = reader.next_delta_tx_map()? {
            sink.index_ledger(&tx_map)?;
            if sink.ledgers % 1000 == 0 {
                progress(sink.ledgers, total_ledgers);
            }
        }
        if sink.ledgers != total_ledgers {
            bail!("chunk header says {total_ledgers} ledgers but {} were present", sink.ledgers);
        }
        (ledgers, transactions, rows) = (sink.ledgers, sink.txs, sink.rows);
    }

    let chunk_hash = reader.chunk_hash();
    // The only place the chunk hash is verified. Nothing has reached the final name yet.
    reader
        .finish()
        .context("chunk hash verification failed — no index was written")?;

    let meta: [(&str, String); 10] = [
        ("schema_version", SCHEMA_VERSION.to_string()),
        ("rule_id", RULE_ID.to_string()),
        ("network_id", network_id.to_string()),
        ("start_ledger", start_ledger.to_string()),
        ("end_ledger", end_ledger.to_string()),
        ("chunk_hash", hex::encode_upper(chunk_hash)),
        ("ledgers", ledgers.to_string()),
        ("transactions", transactions.to_string()),
        ("rows", rows.to_string()),
        // Last: a reader refuses any index without it.
        ("complete", "1".to_string()),
    ];
    for (k, v) in meta {
        tx.execute("INSERT INTO meta (key, value) VALUES (?1, ?2)", params![k, v])?;
    }
    tx.commit()?;
    conn.close().map_err(|(_, e)| anyhow!(e))?;

    File::open(&tmp)?.sync_all()?;
    fs::rename(&tmp, out_path)?;
    guard.armed = false;
    let dir = out_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(dir)?.sync_all()?;

    Ok(BuildReport { network_id, start_ledger, end_ledger, chunk_hash, ledgers, transactions, rows })
}

struct Sink<'a> {
    acct: Statement<'a>,
    loc: Statement<'a>,
    start_ledger: u32,
    ledgers: u32,
    txs: u64,
    rows: u64,
    indices: Vec<u32>,
}

impl<'a> Sink<'a> {
    fn new(tx: &'a Transaction, start_ledger: u32) -> Result<Self> {
        Ok(Self {
            acct: tx.prepare(
                "INSERT INTO account_tx (account_id, ledger_seq, txn_seq, tx_hash) \
                 VALUES (?1, ?2, ?3, ?4)",
            )?,
            loc: tx.prepare(
                "INSERT INTO tx_locator (tx_hash, ledger_seq, txn_seq) VALUES (?1, ?2, ?3)",
            )?,
            start_ledger,
            ledgers: 0,
            txs: 0,
            rows: 0,
            indices: Vec::new(),
        })
    }

    fn index_ledger(&mut self, tx_map: &TxMap) -> Result<()> {
        let seq = tx_map.ledger_seq;
        let expected = self.start_ledger + self.ledgers;
        if seq != expected {
            bail!("ledgers out of order: expected {expected}, found {seq}");
        }

        self.indices.clear();
        for t in &tx_map.txns {
            let id = || hex::encode_upper(t.tx_hash);
            if calculate_tx_id(&t.tx_blob) != t.tx_hash {
                bail!("ledger {seq}: transaction {} does not hash to its recorded id — corrupt chunk", id());
            }
            let summary = summarize_meta(&t.meta_blob)
                .with_context(|| format!("ledger {seq}, transaction {}", id()))?;
            let n = summary.transaction_index.ok_or_else(|| {
                anyhow!("ledger {seq}, transaction {}: metadata has no TransactionIndex", id())
            })?;
            self.indices.push(n);

            for account in &summary.affected_accounts {
                self.acct
                    .execute(params![&account[..], seq, n, &t.tx_hash[..]])
                    .with_context(|| {
                        format!("ledger {seq}, transaction {}: duplicate (account, ledger, TransactionIndex)", id())
                    })?;
                self.rows += 1;
            }
            self.loc.execute(params![&t.tx_hash[..], seq, n])?;
            self.txs += 1;
        }

        // The index orders results by TransactionIndex, so it must be exactly 0..n within a
        // ledger. This is also a tripwire for a wrong field code: it cannot pass by accident.
        self.indices.sort_unstable();
        for (i, &n) in self.indices.iter().enumerate() {
            if n as usize != i {
                bail!(
                    "ledger {seq}: TransactionIndex values are not exactly 0..{} (found {n} at \
                     position {i})",
                    self.indices.len()
                );
            }
        }
        self.ledgers += 1;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::query::{Index, Marker};
    use xrla_common::chunk::{LedgerDelta, TxRecord};
    use xrla_common::meta_decode::{account_id_to_classic_address, classic_address_to_account_id};
    use xrla_common::serialize::ChunkWriter;
    use xrla_common::shamap::SHAMapDiff;

    pub fn a(n: u8) -> [u8; 20] {
        [n; 20]
    }

    /// Metadata with the given TransactionIndex whose single ModifiedNode names `accounts` as
    /// AccountID fields and `issuers` as the issuers of `LowLimit` amounts.
    pub fn meta(txn_index: u32, accounts: &[[u8; 20]], issuers: &[[u8; 20]]) -> Vec<u8> {
        let mut final_fields = Vec::new();
        for acct in accounts {
            final_fields.extend([0x81, 20]); // sfAccount: ACCOUNT(8), field 1
            final_fields.extend(acct);
        }
        for issuer in issuers {
            final_fields.push(0x66); // sfLowLimit: AMOUNT(6), field 6
            final_fields.extend([0xC0, 0, 0, 0, 0, 0, 0, 1]); // issued-currency amount
            final_fields.extend([7u8; 20]); // currency
            final_fields.extend(issuer);
        }
        let mut m = vec![0x20, 28];
        m.extend(txn_index.to_be_bytes()); // sfTransactionIndex: UINT32(2), field 28
        m.push(0xF8); // sfAffectedNodes
        m.push(0xE5); // ModifiedNode
        m.push(0xE7); // FinalFields
        m.extend(final_fields);
        m.extend([0xE1, 0xE1, 0xF1]);
        m
    }

    pub fn tx(seq: u32, txn_index: u32, accounts: &[[u8; 20]], issuers: &[[u8; 20]]) -> TxRecord {
        let mut blob = seq.to_be_bytes().to_vec();
        blob.extend(txn_index.to_be_bytes());
        TxRecord {
            tx_hash: calculate_tx_id(&blob),
            tx_blob: blob,
            meta_blob: meta(txn_index, accounts, issuers),
        }
    }

    /// Writes a real v3 chunk starting at ledger 100, one entry of `ledgers` per ledger.
    pub fn write_chunk(path: &Path, ledgers: &[Vec<TxRecord>], last_drops: u64) -> Hash256 {
        let start = 100u32;
        let end = start + ledgers.len() as u32 - 1;
        let mut w = ChunkWriter::create(path.to_path_buf(), 1, start, end, [0u8; 32]).unwrap();
        w.write_checkpoint(&mut []).unwrap();
        for (i, txns) in ledgers.iter().enumerate() {
            let seq = start + i as u32;
            if i > 0 {
                w.write_delta(&LedgerDelta { ledger_seq: seq, diff: SHAMapDiff::default() }).unwrap();
            }
            let drops = if i + 1 == ledgers.len() { last_drops } else { 0 };
            w.write_tx_map(&TxMap {
                ledger_seq: seq,
                ledger_hash: [0u8; 32],
                account_hash: [0u8; 32],
                drops,
                parent_close_time: 0,
                close_time: 0,
                close_time_resolution: 10,
                close_flags: 0,
                txns: txns.clone(),
            })
            .unwrap();
        }
        w.finish().unwrap()
    }

    pub fn scratch(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("xrla-index-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        d
    }

    /// Three ledgers: 100 (two txs), 101 (none), 102 (one tx, with an issuer-only account).
    fn sample() -> Vec<Vec<TxRecord>> {
        vec![
            vec![tx(100, 1, &[a(1), a(2)], &[]), tx(100, 0, &[a(1)], &[])],
            vec![],
            vec![tx(102, 0, &[a(1)], &[a(3)])],
        ]
    }

    fn rows(idx: &Index, acct: [u8; 20], min: u32, max: u32, limit: usize, forward: bool) -> Vec<(u32, u32)> {
        idx.account_tx(&acct, min, max, limit, forward, None)
            .unwrap()
            .into_iter()
            .map(|r| (r.ledger_seq, r.txn_seq))
            .collect()
    }

    #[test]
    fn builds_an_index_that_answers_account_tx_and_tx() {
        let d = scratch("ok");
        let chunk = d.join("c.xrla");
        let chunk_hash = write_chunk(&chunk, &sample(), 0);
        let out = d.join("c.xidx");

        let report = build_index(&chunk, &out, false, |_, _| {}).unwrap();
        assert_eq!(report.chunk_hash, chunk_hash);
        assert_eq!((report.ledgers, report.transactions, report.rows), (3, 3, 5));
        assert!(!tmp_path(&out).exists());

        let idx = Index::open(&out).unwrap();
        assert_eq!(idx.meta.chunk_hash, hex::encode_upper(chunk_hash));
        assert_eq!((idx.meta.start_ledger, idx.meta.end_ledger), (100, 102));

        // Newest first; within a ledger, highest TransactionIndex first.
        assert_eq!(rows(&idx, a(1), 0, u32::MAX, 100, false), vec![(102, 0), (100, 1), (100, 0)]);
        assert_eq!(rows(&idx, a(1), 0, u32::MAX, 100, true), vec![(100, 0), (100, 1), (102, 0)]);
        assert_eq!(rows(&idx, a(2), 0, u32::MAX, 100, false), vec![(100, 1)]);
        // a(3) appears only as an issuer inside an amount — the case that was once dropped.
        assert_eq!(rows(&idx, a(3), 0, u32::MAX, 100, false), vec![(102, 0)]);
        assert!(rows(&idx, a(9), 0, u32::MAX, 100, false).is_empty());

        // Ledger range bounds.
        assert_eq!(rows(&idx, a(1), 101, 102, 100, false), vec![(102, 0)]);
        assert_eq!(rows(&idx, a(1), 100, 100, 100, false), vec![(100, 1), (100, 0)]);
        assert!(rows(&idx, a(1), 0, 99, 100, false).is_empty());
        assert!(rows(&idx, a(1), 0, u32::MAX, 0, false).is_empty());

        // tx lookup.
        let hash_a = tx(100, 1, &[a(1), a(2)], &[]).tx_hash;
        assert_eq!(idx.locate_tx(&hash_a).unwrap(), Some((100, 1)));
        assert_eq!(idx.locate_tx(&[0xAB; 32]).unwrap(), None);

        // Addresses survive the round trip the CLI uses.
        let addr = account_id_to_classic_address(&a(1));
        assert_eq!(classic_address_to_account_id(&addr).unwrap(), a(1));
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn paging_with_a_marker_reproduces_the_unpaged_result_in_both_directions() {
        let d = scratch("paging");
        let chunk = d.join("c.xrla");
        write_chunk(&chunk, &sample(), 0);
        let out = d.join("c.xidx");
        build_index(&chunk, &out, false, |_, _| {}).unwrap();
        let idx = Index::open(&out).unwrap();

        for forward in [false, true] {
            let whole = idx.account_tx(&a(1), 0, u32::MAX, 100, forward, None).unwrap();
            for page_size in [1usize, 2] {
                let mut got = Vec::new();
                let mut marker: Option<Marker> = None;
                loop {
                    let page = idx.account_tx(&a(1), 0, u32::MAX, page_size, forward, marker).unwrap();
                    if page.is_empty() {
                        break;
                    }
                    let last = page.last().unwrap();
                    marker = Some(Marker { ledger_seq: last.ledger_seq, txn_seq: last.txn_seq });
                    got.extend(page);
                }
                assert_eq!(got, whole, "forward={forward} page_size={page_size}");
            }
        }
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn a_tampered_chunk_writes_nothing_and_leaves_an_existing_index_untouched() {
        let d = scratch("tamper");
        let good = d.join("good.xrla");
        write_chunk(&good, &sample(), 0x1122334455667788);
        let out = d.join("c.xidx");
        build_index(&good, &out, false, |_, _| {}).unwrap();
        let before = fs::read(&out).unwrap();

        // Flip one byte of a field that parses fine (the last ledger's `drops`) so the failure
        // is the chunk hash check and nothing else.
        let mut bytes = fs::read(&good).unwrap();
        let needle = 0x1122334455667788u64.to_be_bytes();
        let pos = bytes.windows(8).position(|w| w == needle).expect("drops field present");
        bytes[pos + 7] ^= 0x01;
        let bad = d.join("bad.xrla");
        fs::write(&bad, &bytes).unwrap();

        let err = build_index(&bad, &out, true, |_, _| {}).unwrap_err();
        assert!(format!("{err:#}").contains("hash"), "unexpected error: {err:#}");
        assert_eq!(fs::read(&out).unwrap(), before, "an existing index must survive a failed rebuild");
        assert!(!tmp_path(&out).exists(), "no temp file may be left behind");

        // And with nothing there beforehand, nothing appears.
        let fresh = d.join("fresh.xidx");
        assert!(build_index(&bad, &fresh, false, |_, _| {}).is_err());
        assert!(!fresh.exists() && !tmp_path(&fresh).exists());
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn refuses_to_replace_an_index_without_force() {
        let d = scratch("force");
        let chunk = d.join("c.xrla");
        write_chunk(&chunk, &sample(), 0);
        let out = d.join("c.xidx");
        build_index(&chunk, &out, false, |_, _| {}).unwrap();
        assert!(build_index(&chunk, &out, false, |_, _| {}).is_err());
        assert!(build_index(&chunk, &out, true, |_, _| {}).is_ok());
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn a_gap_in_transaction_index_aborts_the_build() {
        let d = scratch("gap");
        let chunk = d.join("c.xrla");
        write_chunk(&chunk, &[vec![tx(100, 0, &[a(1)], &[]), tx(100, 2, &[a(1)], &[])]], 0);
        let out = d.join("c.xidx");
        let err = build_index(&chunk, &out, false, |_, _| {}).unwrap_err();
        assert!(format!("{err:#}").contains("TransactionIndex"), "unexpected: {err:#}");
        assert!(!out.exists() && !tmp_path(&out).exists());
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn a_transaction_that_does_not_hash_to_its_id_aborts_the_build() {
        let d = scratch("badid");
        let chunk = d.join("c.xrla");
        let mut t = tx(100, 0, &[a(1)], &[]);
        t.tx_hash[0] ^= 0xFF;
        write_chunk(&chunk, &[vec![t]], 0);
        let out = d.join("c.xidx");
        let err = build_index(&chunk, &out, false, |_, _| {}).unwrap_err();
        assert!(format!("{err:#}").contains("does not hash"), "unexpected: {err:#}");
        assert!(!out.exists());
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn metadata_that_cannot_be_decoded_aborts_the_build_rather_than_being_skipped() {
        let d = scratch("baddecode");
        let chunk = d.join("c.xrla");
        let mut t = tx(100, 0, &[a(1)], &[]);
        t.meta_blob = Vec::new(); // an empty blob must never become "touched nobody"
        write_chunk(&chunk, &[vec![t]], 0);
        let out = d.join("c.xidx");
        assert!(build_index(&chunk, &out, false, |_, _| {}).is_err());
        assert!(!out.exists());
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn a_reader_rejects_an_incomplete_index_or_one_built_by_a_different_rule() {
        let d = scratch("gate");

        let incomplete = d.join("incomplete.xidx");
        let c = Connection::open(&incomplete).unwrap();
        c.execute_batch(SCHEMA_SQL).unwrap();
        c.execute("INSERT INTO meta VALUES ('schema_version', '1')", []).unwrap();
        drop(c);
        assert!(Index::open(&incomplete).is_err(), "no `complete` marker");

        let wrong_rule = d.join("wrong.xidx");
        let chunk = d.join("c.xrla");
        write_chunk(&chunk, &sample(), 0);
        build_index(&chunk, &wrong_rule, false, |_, _| {}).unwrap();
        let c = Connection::open(&wrong_rule).unwrap();
        c.execute("UPDATE meta SET value = 'old-rule/0' WHERE key = 'rule_id'", []).unwrap();
        drop(c);
        assert!(Index::open(&wrong_rule).is_err(), "an index built by a different rule is untrusted");
        let _ = fs::remove_dir_all(&d);
    }
}
