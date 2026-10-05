/// xrla-difftest — check our affected-account rule against xrpld's own `transaction.db`.
///
/// xrpld writes one `AccountTransactions` row per (transaction, affected account) for every
/// ledger it saves. That is exactly what `account_tx` answers from, so it is the oracle for
/// whether our index can serve the same queries. For every transaction in a ledger range this
/// re-derives the account set from the stored metadata with `xrla_common::meta_decode` and
/// compares it to xrpld's rows.
///
/// Reported separately, per transaction, never as a single total (one missing account and one
/// extra account cancel in a count but are two different bugs):
///   MISSING — in xrpld's rows, not in ours.   Any single one is a ship-blocker: it is a
///             silently incomplete account history.
///   EXTRA   — in ours, not in xrpld's rows.
///   TXNSEQ  — same transaction, different apply-order position.
///
/// Usage:
///   xrla-difftest --txdb /space/xrpld/db/transaction.db --from 107368107 --to 107373106
///
/// Exits non-zero unless every transaction matches exactly with no decode errors.
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::Parser;
use rusqlite::{Connection, OpenFlags};

use xrla_common::meta_decode::{account_id_to_classic_address, summarize_meta};
use xrla_index::query::Index;

/// Transaction types a comparison corpus should contain, otherwise a clean run says little
/// about the rule's handling of trustlines, offers, AMM, NFTs, regular keys and account deletion.
const REQUIRED_TYPES: &[&str] = &[
    "Payment",
    "TrustSet",
    "OfferCreate",
    "OfferCancel",
    "AMMDeposit",
    "NFTokenAcceptOffer",
    "SetRegularKey",
    "AccountDelete",
];

#[derive(Parser, Debug)]
#[command(name = "xrla-difftest", about = "Diff our affected-account rule against xrpld's transaction.db")]
struct Args {
    /// Path to xrpld's transaction.db (opened read-only)
    #[arg(long)]
    txdb: PathBuf,
    /// First ledger sequence to compare (inclusive)
    #[arg(long)]
    from: u32,
    /// Last ledger sequence to compare (inclusive)
    #[arg(long)]
    to: u32,
    /// How many example transactions to print per mismatch bucket
    #[arg(long, default_value_t = 8)]
    examples: usize,
    /// How many ledgers in the range may legitimately have no transactions. Default 0: on recent
    /// mainnet a ledger with no transactions essentially never happens, so a ledger with no rows
    /// means xrpld has not populated it and the run would silently under-cover the range. Raise
    /// this for early history, where empty ledgers are common.
    #[arg(long, default_value_t = 0)]
    max_empty_ledgers: i64,
    /// Also check a built `.xidx` index against xrpld's rows: the same account sets per
    /// transaction (what `account_tx` answers from) and the `tx` locator. This tests the real
    /// path — chunk -> index — not just the decoder.
    #[arg(long)]
    index: Option<PathBuf>,
}

/// Pseudo-transactions are the only transactions that may legitimately affect no account.
const PSEUDO_TYPES: &[&str] = &["EnableAmendment", "SetFee", "UNLModify"];

#[derive(Default)]
struct TypeStats {
    txs: u64,
    with_missing: u64,
    with_extra: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.from > args.to {
        bail!("--from must be <= --to");
    }

    let conn = Connection::open_with_flags(&args.txdb, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("opening {}", args.txdb.display()))?;
    conn.busy_timeout(Duration::from_secs(60))?;

    let expected_ledgers = (args.to - args.from + 1) as i64;
    let ledgers_with_rows: i64 = conn.query_row(
        "SELECT COUNT(DISTINCT LedgerSeq) FROM Transactions WHERE LedgerSeq BETWEEN ?1 AND ?2",
        [args.from, args.to],
        |r| r.get(0),
    )?;
    println!("range            : {}..={} ({} ledgers)", args.from, args.to, expected_ledgers);
    let empty_ledgers = expected_ledgers - ledgers_with_rows;
    println!("ledgers w/ rows  : {ledgers_with_rows} ({empty_ledgers} without; {} allowed)", args.max_empty_ledgers);
    let incomplete_range = empty_ledgers > args.max_empty_ledgers;
    if incomplete_range {
        println!(
            "WARNING: {empty_ledgers} of {expected_ledgers} ledgers have no rows in this database, so only \
             part of the requested range is being compared"
        );
    }

    let mut txs = conn.prepare(
        "SELECT TransID, LedgerSeq, TransType, TxnMeta FROM Transactions \
         WHERE LedgerSeq BETWEEN ?1 AND ?2 ORDER BY LedgerSeq, TransID",
    )?;
    let mut acct_rows =
        conn.prepare("SELECT Account, TxnSeq FROM AccountTransactions WHERE TransID = ?1")?;

    let mut total = 0u64;
    let mut exact = 0u64;
    let mut decode_errors = 0u64;
    let mut missing_txs = 0u64;
    let mut extra_txs = 0u64;
    let mut missing_rows = 0u64;
    let mut extra_rows = 0u64;
    let mut txnseq_mismatch = 0u64;
    let mut no_xrpld_rows = 0u64;
    let mut unexplained_empty = 0u64;
    let mut per_type: BTreeMap<String, TypeStats> = BTreeMap::new();

    let mut ex_unexplained: Vec<String> = Vec::new();
    let mut ex_missing: Vec<String> = Vec::new();
    let mut ex_extra: Vec<String> = Vec::new();
    let mut ex_errors: Vec<String> = Vec::new();
    let mut ex_txnseq: Vec<String> = Vec::new();

    let mut rows = txs.query([args.from, args.to])?;
    while let Some(row) = rows.next()? {
        let trans_id: String = row.get(0)?;
        let ledger: u32 = row.get(1)?;
        let trans_type: String = row.get::<_, Option<String>>(2)?.unwrap_or_default();
        let meta: Vec<u8> = row.get::<_, Option<Vec<u8>>>(3)?.unwrap_or_default();

        total += 1;
        let stats = per_type.entry(trans_type.clone()).or_default();
        stats.txs += 1;

        // xrpld's answer
        let mut truth: BTreeSet<String> = BTreeSet::new();
        let mut truth_txnseq: Option<i64> = None;
        let mut truth_txnseq_consistent = true;
        let mut it = acct_rows.query([&trans_id])?;
        while let Some(r) = it.next()? {
            truth.insert(r.get::<_, String>(0)?);
            let seq: i64 = r.get(1)?;
            match truth_txnseq {
                None => truth_txnseq = Some(seq),
                Some(prev) if prev != seq => truth_txnseq_consistent = false,
                _ => {}
            }
        }
        drop(it);
        if truth.is_empty() {
            no_xrpld_rows += 1;
        }

        // ours
        let summary = match summarize_meta(&meta) {
            Ok(s) => s,
            Err(e) => {
                decode_errors += 1;
                if ex_errors.len() < args.examples {
                    ex_errors.push(format!("ledger {ledger} {trans_type} {trans_id}: {e}"));
                }
                continue;
            }
        };
        let ours: BTreeSet<String> =
            summary.affected_accounts.iter().map(account_id_to_classic_address).collect();

        let missing: Vec<&String> = truth.difference(&ours).collect();
        let extra: Vec<&String> = ours.difference(&truth).collect();

        let mut ok = true;
        // Both sides empty looks like a perfect match, but only a pseudo-transaction may affect
        // no account. Anything else means neither side found anyone, which is a bug (or a
        // missing row/metadata) agreeing with itself.
        if truth.is_empty() && ours.is_empty() && !PSEUDO_TYPES.contains(&trans_type.as_str()) {
            ok = false;
            unexplained_empty += 1;
            if ex_unexplained.len() < args.examples {
                ex_unexplained.push(format!("ledger {ledger} {trans_type} {trans_id}"));
            }
        }
        if !missing.is_empty() {
            ok = false;
            missing_txs += 1;
            missing_rows += missing.len() as u64;
            stats.with_missing += 1;
            if ex_missing.len() < args.examples {
                ex_missing.push(format!(
                    "ledger {ledger} {trans_type} {trans_id}\n      missing {missing:?}\n      ours    {:?}",
                    ours
                ));
            }
        }
        if !extra.is_empty() {
            ok = false;
            extra_txs += 1;
            extra_rows += extra.len() as u64;
            stats.with_extra += 1;
            if ex_extra.len() < args.examples {
                ex_extra.push(format!("ledger {ledger} {trans_type} {trans_id}\n      extra {extra:?}"));
            }
        }
        // Only comparable when xrpld wrote at least one row for the transaction.
        if let Some(seq) = truth_txnseq {
            let ours_seq = summary.transaction_index.map(|v| v as i64);
            if !truth_txnseq_consistent || ours_seq != Some(seq) {
                ok = false;
                txnseq_mismatch += 1;
                if ex_txnseq.len() < args.examples {
                    ex_txnseq.push(format!(
                        "ledger {ledger} {trans_type} {trans_id}: xrpld TxnSeq {seq}, ours {ours_seq:?}"
                    ));
                }
            }
        }
        if ok {
            exact += 1;
        }
        if total % 100_000 == 0 {
            eprintln!("  ... {total} transactions compared");
        }
    }

    println!();
    println!("transactions compared : {total}");
    println!("exact match           : {exact}");
    println!("decode errors         : {decode_errors}");
    println!("MISSING (ours lacks)  : {missing_txs} txs, {missing_rows} account rows   <- ship-blocker if non-zero");
    println!("EXTRA   (ours adds)   : {extra_txs} txs, {extra_rows} account rows");
    println!("TXNSEQ mismatches     : {txnseq_mismatch}");
    println!("txs xrpld has no rows : {no_xrpld_rows} (only pseudo-transactions may have none)");
    println!("NOBODY (both empty)   : {unexplained_empty} non-pseudo txs where neither side found any account");

    println!();
    println!("{:<22} {:>9} {:>14} {:>12}", "tx type", "count", "with MISSING", "with EXTRA");
    for (t, s) in &per_type {
        println!("{:<22} {:>9} {:>14} {:>12}", t, s.txs, s.with_missing, s.with_extra);
    }

    let absent: Vec<&&str> =
        REQUIRED_TYPES.iter().filter(|t| !per_type.contains_key(**t)).collect();
    println!();
    if absent.is_empty() {
        println!("COVERAGE: every required transaction type was exercised");
    } else {
        println!("COVERAGE: NOT EXERCISED (a clean result says nothing about these): {absent:?}");
    }

    for (title, list) in [
        ("NOBODY examples", &ex_unexplained),
        ("MISSING examples", &ex_missing),
        ("EXTRA examples", &ex_extra),
        ("TXNSEQ examples", &ex_txnseq),
        ("DECODE ERROR examples", &ex_errors),
    ] {
        if !list.is_empty() {
            println!("\n--- {title} ---");
            for e in list {
                println!("  {e}");
            }
        }
    }

    println!();
    if incomplete_range {
        println!("RESULT: FAIL — the requested range is not fully populated in this database");
        bail!("{empty_ledgers} of {expected_ledgers} ledgers have no rows; refusing to report PASS on a partial range");
    }
    let rule_ok = decode_errors == 0
        && missing_txs == 0
        && extra_txs == 0
        && txnseq_mismatch == 0
        && unexplained_empty == 0;
    if rule_ok {
        println!("RESULT (decoder vs xrpld): PASS — every transaction matches xrpld exactly");
    } else {
        println!("RESULT (decoder vs xrpld): FAIL");
    }

    let index_ok = match &args.index {
        Some(path) => compare_index(&conn, path, args.from, args.to, args.examples)?,
        None => true,
    };

    if rule_ok && index_ok {
        Ok(())
    } else {
        bail!("our results do not match xrpld's");
    }
}

/// Compare a built index against xrpld's `AccountTransactions` / `Transactions`, per
/// transaction and in both directions: the account set, the apply-order position, and the `tx`
/// locator. Anything the index holds that xrpld does not is reported too.
fn compare_index(conn: &Connection, path: &PathBuf, from: u32, to: u32, examples: usize) -> Result<bool> {
    let idx = Index::open(path)?;
    println!();
    println!("=== index {} ===", path.display());
    println!(
        "index covers     : ledgers {}..={}, {} transactions, {} account rows, chunk {}",
        idx.meta.start_ledger, idx.meta.end_ledger, idx.meta.transactions, idx.meta.rows, idx.meta.chunk_hash
    );
    if from < idx.meta.start_ledger || to > idx.meta.end_ledger {
        println!(
            "WARNING: requested {from}..={to} extends beyond the index's {}..={}; the part outside \
             would be reported as missing",
            idx.meta.start_ledger, idx.meta.end_ledger
        );
    }

    // Everything the index says about the range, keyed by transaction id (uppercase hex, the
    // form xrpld stores).
    struct Entry {
        ledger: u32,
        txn_seq: u32,
        accounts: BTreeSet<String>,
    }
    let mut by_tx: std::collections::HashMap<String, Entry> = std::collections::HashMap::new();
    {
        let mut stmt = idx.connection().prepare(
            "SELECT account_id, ledger_seq, txn_seq, tx_hash FROM account_tx \
             WHERE ledger_seq BETWEEN ?1 AND ?2",
        )?;
        let mut rows = stmt.query([from, to])?;
        while let Some(r) = rows.next()? {
            let id: Vec<u8> = r.get(0)?;
            let id = <[u8; 20]>::try_from(id.as_slice()).context("index account_id is not 20 bytes")?;
            let hash: Vec<u8> = r.get(3)?;
            let e = by_tx.entry(hex_upper(&hash)).or_insert_with(|| Entry {
                ledger: 0,
                txn_seq: 0,
                accounts: BTreeSet::new(),
            });
            e.ledger = r.get(1)?;
            e.txn_seq = r.get(2)?;
            e.accounts.insert(account_id_to_classic_address(&id));
        }
    }

    let mut txs = conn.prepare(
        "SELECT TransID, LedgerSeq FROM Transactions WHERE LedgerSeq BETWEEN ?1 AND ?2 \
         ORDER BY LedgerSeq, TransID",
    )?;
    let mut acct_rows = conn.prepare("SELECT Account, TxnSeq FROM AccountTransactions WHERE TransID = ?1")?;

    let (mut total, mut exact) = (0u64, 0u64);
    let (mut idx_missing_tx, mut idx_missing_rows) = (0u64, 0u64);
    let (mut idx_extra_tx, mut idx_extra_rows) = (0u64, 0u64);
    let (mut seq_bad, mut ledger_bad, mut locator_bad, mut not_in_index) = (0u64, 0u64, 0u64, 0u64);
    let mut ex: Vec<String> = Vec::new();
    let note = |ex: &mut Vec<String>, s: String| {
        if ex.len() < examples {
            ex.push(s);
        }
    };

    let mut rows = txs.query([from, to])?;
    while let Some(row) = rows.next()? {
        let trans_id: String = row.get(0)?;
        let ledger: u32 = row.get(1)?;
        total += 1;

        let mut truth: BTreeSet<String> = BTreeSet::new();
        let mut truth_seq: Option<i64> = None;
        let mut it = acct_rows.query([&trans_id])?;
        while let Some(r) = it.next()? {
            truth.insert(r.get::<_, String>(0)?);
            truth_seq = Some(r.get(1)?);
        }
        drop(it);

        let mut ok = true;
        match by_tx.remove(&trans_id) {
            None => {
                if !truth.is_empty() {
                    ok = false;
                    not_in_index += 1;
                    note(&mut ex, format!("NOT IN INDEX ledger {ledger} {trans_id}"));
                }
            }
            Some(e) => {
                let missing: Vec<&String> = truth.difference(&e.accounts).collect();
                let extra: Vec<&String> = e.accounts.difference(&truth).collect();
                if !missing.is_empty() {
                    ok = false;
                    idx_missing_tx += 1;
                    idx_missing_rows += missing.len() as u64;
                    note(&mut ex, format!("INDEX MISSING ledger {ledger} {trans_id}: {missing:?}"));
                }
                if !extra.is_empty() {
                    ok = false;
                    idx_extra_tx += 1;
                    idx_extra_rows += extra.len() as u64;
                    note(&mut ex, format!("INDEX EXTRA ledger {ledger} {trans_id}: {extra:?}"));
                }
                if let Some(s) = truth_seq {
                    if s != e.txn_seq as i64 {
                        ok = false;
                        seq_bad += 1;
                        note(&mut ex, format!("TXNSEQ ledger {ledger} {trans_id}: xrpld {s}, index {}", e.txn_seq));
                    }
                }
                if e.ledger != ledger {
                    ok = false;
                    ledger_bad += 1;
                    note(&mut ex, format!("LEDGER {trans_id}: xrpld {ledger}, index {}", e.ledger));
                }
            }
        }

        // The `tx` lookup: where the index says this transaction is.
        let hash = hex_decode(&trans_id)?;
        let located: Option<(u32, u32)> = idx
            .connection()
            .query_row(
                "SELECT ledger_seq, txn_seq FROM tx_locator WHERE tx_hash = ?1",
                [&hash[..]],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .ok();
        match (located, truth_seq) {
            (Some((l, s)), Some(ts)) if l == ledger && s as i64 == ts => {}
            (Some((l, s)), None) if l == ledger => {
                let _ = s; // xrpld wrote no account rows (pseudo-transaction): ledger match is all we can check
            }
            other => {
                ok = false;
                locator_bad += 1;
                note(&mut ex, format!("LOCATOR ledger {ledger} {trans_id}: index {other:?}, xrpld seq {truth_seq:?}"));
            }
        }

        if ok {
            exact += 1;
        }
    }

    // Anything left in the map is in the index but is not an xrpld transaction in this range.
    let index_only = by_tx.len() as u64;
    for (h, e) in by_tx.iter().take(examples) {
        ex.push(format!("INDEX-ONLY ledger {} {h}", e.ledger));
    }

    println!("transactions     : {total} compared");
    println!("exact match      : {exact}");
    println!("INDEX MISSING    : {idx_missing_tx} txs, {idx_missing_rows} account rows   <- silently incomplete histories");
    println!("INDEX EXTRA      : {idx_extra_tx} txs, {idx_extra_rows} account rows");
    println!("not in index     : {not_in_index} (xrpld has account rows, the index has none)");
    println!("TXNSEQ mismatch  : {seq_bad}");
    println!("LEDGER mismatch  : {ledger_bad}");
    println!("tx LOCATOR bad   : {locator_bad}");
    println!("index-only txs   : {index_only} (in the index, not an xrpld transaction in this range)");
    if !ex.is_empty() {
        println!("\n--- index mismatch examples ---");
        for e in &ex {
            println!("  {e}");
        }
    }

    let ok = idx_missing_tx == 0
        && idx_extra_tx == 0
        && not_in_index == 0
        && seq_bad == 0
        && ledger_bad == 0
        && locator_bad == 0
        && index_only == 0;
    println!();
    println!("RESULT (index vs xrpld): {}", if ok { "PASS — the index matches xrpld exactly" } else { "FAIL" });
    Ok(ok)
}

fn hex_upper(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02X}")).collect()
}

fn hex_decode(s: &str) -> Result<Vec<u8>> {
    if s.len() % 2 != 0 {
        bail!("odd-length hex string {s:?}");
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).with_context(|| format!("bad hex in {s:?}")))
        .collect()
}
