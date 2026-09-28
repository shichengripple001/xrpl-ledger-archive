/// xrla-import — import one or more XRLA chunk files into a rippled-compatible NuDB store.
///
/// Usage:
///   xrla-import --chunk ./chunks/xrla_1_01000000_01001000.xrla \
///               --dat /var/lib/rippled/db/nudb.dat
///
/// Multiple --chunk arguments (e.g. every range file from a full-history export) are
/// combined into a single write.
///
/// Verifies the chunk hash, then replays checkpoint+deltas and rebuilds each ledger's
/// transaction tree, independently recomputing and asserting:
///   - each transaction's own tx_hash (SHA512half(HashPrefix::transactionID + tx_blob))
///   - the replayed account-state root against the ledger's stored account_hash
///   - the full LedgerHash, chained via parent_hash to the previous ledger in the chunk
///
/// The very first ledger in a chunk cannot have its LedgerHash fully verified this way —
/// its parent_hash is external to the chunk (see spec/chunk-format.md "Verification
/// without full history"). Everything from the second ledger onward chains internally.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::rc::Rc;

use anyhow::{bail, Result};
use clap::Parser;

use xrla_common::chunk::{Chunk, TxMap};
use xrla_common::serialize::{calculate_ledger_hash, deserialize_chunk, LedgerHashInput};
use xrla_common::shamap::{Hash256, InnerNode, NodeType, SHAMapNode};
use xrla_common::state_tree::verify_state_nodes;
use xrla_common::tx_tree::{build_tx_tree, calculate_tx_id};

#[derive(Parser, Debug)]
#[command(name = "xrla-import", about = "Import an XRLA chunk file into rippled NuDB")]
struct Args {
    /// Path(s) to .xrla chunk file(s). Multiple chunks (e.g. every range file from a
    /// full-history export) are combined into a single write: every chunk's checkpoint
    /// and delta nodes are unioned before one NuDB store / ledger.db is written, so
    /// nodes shared across chunk boundaries are naturally deduped, not written twice.
    #[arg(long, required = true, num_args = 1..)]
    chunk: Vec<PathBuf>,

    /// Path to the NuDB .dat file to write (a sibling .key file is written alongside it)
    #[arg(long)]
    dat: PathBuf,

    /// Skip hash verification (faster, not recommended)
    #[arg(long, default_value_t = false)]
    skip_verify: bool,

    /// Path to write rippled's ledger.db (Ledgers index) alongside the NuDB store. The
    /// chunk's checkpoint ledger has no in-chunk PrevHash (its parent is external to this
    /// chunk), so it is not written; every ledger from the first delta onward chains
    /// internally and is written.
    #[arg(long)]
    ledger_db: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let mut all_state_nodes: HashMap<Hash256, Rc<SHAMapNode>> = HashMap::new();
    let mut tx_nodes: Vec<SHAMapNode> = Vec::new();
    let mut ledger_rows: Vec<LedgerDbRow> = Vec::new();

    for chunk_path in &args.chunk {
        println!("Reading chunk: {}", chunk_path.display());
        let data = fs::read(chunk_path)?;

        println!("Deserializing and verifying chunk...");
        let chunk = deserialize_chunk(&data).map_err(|e| anyhow::anyhow!("{e}"))?;

        println!(
            "Chunk: network={} ledgers={}..{} ({} ledgers)",
            chunk.network_id,
            chunk.start_ledger,
            chunk.end_ledger,
            chunk.ledger_count()
        );
        println!("Chunk hash OK: {}", hex::encode(chunk.chunk_hash));

        let replay = replay_chunk(&chunk, !args.skip_verify)?;
        println!(
            "Replayed {} live state nodes, {} tx-tree nodes across {} ledgers",
            replay.state.len(),
            replay.tx_nodes.len(),
            chunk.ledger_count()
        );

        // Union across chunks: a node shared across chunk boundaries (e.g. every
        // later chunk's checkpoint duplicates unchanged nodes from earlier chunks)
        // is kept once, not once per chunk.
        for (hash, node) in replay.all_state_nodes {
            all_state_nodes.entry(hash).or_insert(node);
        }
        tx_nodes.extend(replay.tx_nodes);
        ledger_rows.extend(replay.ledger_rows);
    }

    let combined = ReplayResult {
        state: HashMap::new(),
        all_state_nodes,
        tx_nodes,
        ledger_rows,
    };

    let key_path = args.dat.with_extension("key");
    println!("Writing NuDB store: {} / {}", args.dat.display(), key_path.display());
    write_to_nudb(&combined, &args.dat, &key_path)?;

    if let Some(ledger_db_path) = &args.ledger_db {
        println!("Writing ledger.db: {}", ledger_db_path.display());
        write_ledger_db(&combined.ledger_rows, ledger_db_path)?;
        println!("  {} rows written", combined.ledger_rows.len());
    }

    println!("Import complete.");
    Ok(())
}

/// One row for rippled's `Ledgers` table (`ledger.db`), matching `kLgrDbInit`
/// (`include/xrpl/rdb/DBInit.h`) exactly.
#[derive(Debug)]
struct LedgerDbRow {
    ledger_hash: Hash256,
    ledger_seq: u32,
    prev_hash: Hash256,
    total_coins: u64,
    closing_time: u32,
    prev_closing_time: u32,
    close_time_resolution: u8,
    close_flags: u8,
    account_hash: Hash256,
    trans_hash: Hash256,
}

#[derive(Debug)]
struct ReplayResult {
    /// Final live account-state SHAMap nodes (checkpoint replayed through all deltas) —
    /// used for root-tracking/verification during replay, not for the NuDB write-set (a
    /// real full-history node retains every node any served ledger ever referenced, not
    /// just the range's last live set; see `all_state_nodes`). Shares node storage with
    /// `all_state_nodes` via `Rc` — a node present in both maps is one allocation, not two;
    /// a naive `.clone()` of a 27M-node checkpoint doubled resident memory and OOM'd a real
    /// run (2026-09-28), which is why this isn't a plain `HashMap<Hash256, SHAMapNode>`.
    state: HashMap<Hash256, Rc<SHAMapNode>>,
    /// Every state node the chunk contains: the checkpoint plus every delta's
    /// `diff.added`, regardless of whether a later delta's `diff.deleted` superseded it.
    /// This — not `state` — is what gets written to the NuDB store, so every ledger in
    /// the chunk's range stays servable, not just the last one.
    all_state_nodes: HashMap<Hash256, Rc<SHAMapNode>>,
    /// Every inner/leaf node of every ledger's rebuilt transaction tree.
    tx_nodes: Vec<SHAMapNode>,
    /// One row per ledger from the first delta onward (see `Args::ledger_db` doc comment
    /// for why the checkpoint ledger itself is excluded).
    ledger_rows: Vec<LedgerDbRow>,
}

/// Replay checkpoint + deltas, rebuilding each ledger's transaction tree along the way.
/// When `verify` is true, independently recomputes and asserts (bailing on the first
/// mismatch): per-transaction authenticity, the account-state root, and the full
/// LedgerHash chained to the previous ledger.
fn replay_chunk(chunk: &Chunk, verify: bool) -> Result<ReplayResult> {
    let mut state: HashMap<Hash256, Rc<SHAMapNode>> = chunk
        .checkpoint
        .iter()
        .map(|n| (n.hash, Rc::new(n.clone())))
        .collect();
    // Shares the same Rc<SHAMapNode> as `state`, not a second copy of the node bytes.
    let mut all_state_nodes: HashMap<Hash256, Rc<SHAMapNode>> = state.clone();
    let mut tx_nodes = Vec::new();

    if chunk.tx_maps.is_empty() {
        bail!("chunk has no TX_MAPS entries");
    }

    // Ledger 0 is the checkpoint. Its account_hash must be a node we actually have; its
    // LedgerHash can't be fully verified here since parent_hash is external to this chunk.
    let cp = &chunk.tx_maps[0];
    if !state.contains_key(&cp.account_hash) {
        bail!(
            "checkpoint account_hash {} not found among checkpoint nodes",
            hex::encode(cp.account_hash)
        );
    }
    if verify {
        verify_txns_authentic(cp)?;
        if let Err(bad_hash) = verify_state_nodes(&chunk.checkpoint) {
            bail!(
                "checkpoint: node {} does not hash to its own claimed content \
                 (source data corruption or a decode bug)",
                hex::encode(bad_hash)
            );
        }
    }
    let (_, nodes) = build_tx_tree(&cp.txns);
    tx_nodes.extend(nodes);
    if verify {
        println!(
            "  ledger {} (checkpoint): account_hash OK, {} txns authentic, {} state nodes \
             self-consistent (LedgerHash needs an external parent_hash anchor — not verified here)",
            cp.ledger_seq,
            cp.txns.len(),
            chunk.checkpoint.len()
        );
    }

    let mut current_root = cp.account_hash;
    let mut prev_ledger_hash = cp.ledger_hash;
    let mut ledger_rows = Vec::new();

    for (i, delta) in chunk.deltas.iter().enumerate() {
        for node in &delta.diff.added {
            let shared = Rc::new(node.clone());
            state.insert(node.hash, Rc::clone(&shared));
            all_state_nodes.insert(node.hash, shared);
        }
        for hash in &delta.diff.deleted {
            state.remove(hash);
        }

        let tx_map = chunk
            .tx_maps
            .get(i + 1)
            .ok_or_else(|| anyhow::anyhow!("missing TX_MAPS entry for delta index {i}"))?;
        if tx_map.ledger_seq != delta.ledger_seq {
            bail!(
                "delta/tx_map sequence mismatch: delta.ledger_seq={} tx_map.ledger_seq={}",
                delta.ledger_seq, tx_map.ledger_seq
            );
        }

        let new_root = find_new_root(&delta.diff.added, &current_root)?;
        let (tx_hash, nodes) = build_tx_tree(&tx_map.txns);
        tx_nodes.extend(nodes);

        if verify {
            verify_txns_authentic(tx_map)?;

            if let Err(bad_hash) = verify_state_nodes(&delta.diff.added) {
                bail!(
                    "ledger {}: node {} does not hash to its own claimed content \
                     (source data corruption or a decode bug)",
                    tx_map.ledger_seq,
                    hex::encode(bad_hash)
                );
            }

            if new_root != tx_map.account_hash {
                bail!(
                    "ledger {}: replayed account root {} != stored account_hash {}",
                    tx_map.ledger_seq,
                    hex::encode(new_root),
                    hex::encode(tx_map.account_hash)
                );
            }

            let recomputed = calculate_ledger_hash(&LedgerHashInput {
                seq: tx_map.ledger_seq,
                drops: tx_map.drops,
                parent_hash: prev_ledger_hash,
                tx_hash,
                account_hash: new_root,
                parent_close_time: tx_map.parent_close_time,
                close_time: tx_map.close_time,
                close_time_resolution: tx_map.close_time_resolution,
                close_flags: tx_map.close_flags,
            });
            if recomputed != tx_map.ledger_hash {
                bail!(
                    "ledger {}: recomputed LedgerHash {} != stored {}",
                    tx_map.ledger_seq,
                    hex::encode(recomputed),
                    hex::encode(tx_map.ledger_hash)
                );
            }
            println!(
                "  ledger {}: account_hash OK, {} txns authentic, {} state nodes self-consistent, \
                 LedgerHash OK (chained to parent)",
                tx_map.ledger_seq,
                tx_map.txns.len(),
                delta.diff.added.len()
            );
        }

        ledger_rows.push(LedgerDbRow {
            ledger_hash: tx_map.ledger_hash,
            ledger_seq: tx_map.ledger_seq,
            prev_hash: prev_ledger_hash,
            total_coins: tx_map.drops,
            closing_time: tx_map.close_time,
            prev_closing_time: tx_map.parent_close_time,
            close_time_resolution: tx_map.close_time_resolution,
            close_flags: tx_map.close_flags,
            account_hash: new_root,
            trans_hash: tx_hash,
        });

        current_root = new_root;
        prev_ledger_hash = tx_map.ledger_hash;
    }

    Ok(ReplayResult { state, all_state_nodes, tx_nodes, ledger_rows })
}

fn verify_txns_authentic(tx_map: &TxMap) -> Result<()> {
    for tx in &tx_map.txns {
        let expected = calculate_tx_id(&tx.tx_blob);
        if expected != tx.tx_hash {
            bail!(
                "ledger {}: tx_hash mismatch — stored {}, recomputed {}",
                tx_map.ledger_seq,
                hex::encode(tx.tx_hash),
                hex::encode(expected)
            );
        }
    }
    Ok(())
}

/// Find the new account-state root hash after applying a delta.
/// The root is the added inner node that is not referenced as a child by any other added
/// inner node. If nothing was added (delta is empty), the root is unchanged.
fn find_new_root(added: &[SHAMapNode], prev_root: &Hash256) -> Result<Hash256> {
    if added.is_empty() {
        return Ok(*prev_root);
    }

    let mut referenced = std::collections::HashSet::new();
    for node in added {
        if matches!(node.node_type, NodeType::Inner) {
            if let Ok(inner) = InnerNode::from_full_bytes(&node.content) {
                for child_hash in inner.child_hashes() {
                    referenced.insert(*child_hash);
                }
            }
        }
    }

    let roots: Vec<Hash256> = added
        .iter()
        .filter(|n| matches!(n.node_type, NodeType::Inner))
        .filter(|n| !referenced.contains(&n.hash))
        .map(|n| n.hash)
        .collect();

    match roots.len() {
        0 => Ok(*prev_root),
        1 => Ok(roots[0]),
        _ => bail!("multiple candidate root nodes in delta — unexpected"),
    }
}

/// Write the final live account-state nodes plus every rebuilt transaction-tree node into
/// a fresh NuDB store (nodes deduped by hash across the two sets).
/// Write rippled's `ledger.db` `Ledgers` table. Schema matches `kLgrDbInit`
/// (`include/xrpl/rdb/DBInit.h`) exactly; hashes are stored as uppercase hex, matching
/// what a real rippled node writes (and what `xrla-export`'s `parse_hash` reads back).
///
/// Opens (creating if absent) rather than truncating: pointing this at an *existing*,
/// already-populated `ledger.db` (e.g. a running instance that already holds a different
/// ledger range) merges this chunk's rows in rather than destroying the existing ones.
/// `INSERT OR IGNORE` makes re-running against the same chunk idempotent — a duplicate
/// `LedgerHash` primary key can only mean the identical row (the hash is content-derived),
/// never conflicting data under the same key.
fn write_ledger_db(rows: &[LedgerDbRow], path: &std::path::Path) -> Result<()> {
    let conn = rusqlite::Connection::open(path)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS Ledgers (
            LedgerHash CHARACTER(64) PRIMARY KEY,
            LedgerSeq BIGINT UNSIGNED,
            PrevHash CHARACTER(64),
            TotalCoins BIGINT UNSIGNED,
            ClosingTime BIGINT UNSIGNED,
            PrevClosingTime BIGINT UNSIGNED,
            CloseTimeRes BIGINT UNSIGNED,
            CloseFlags BIGINT UNSIGNED,
            AccountSetHash CHARACTER(64),
            TransSetHash CHARACTER(64)
         );
         CREATE INDEX IF NOT EXISTS SeqLedger ON Ledgers(LedgerSeq);",
    )?;

    let mut stmt = conn.prepare(
        "INSERT OR IGNORE INTO Ledgers (LedgerHash, LedgerSeq, PrevHash, TotalCoins, ClosingTime, \
         PrevClosingTime, CloseTimeRes, CloseFlags, AccountSetHash, TransSetHash) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
    )?;
    for row in rows {
        stmt.execute(rusqlite::params![
            hex::encode_upper(row.ledger_hash),
            row.ledger_seq,
            hex::encode_upper(row.prev_hash),
            row.total_coins,
            row.closing_time,
            row.prev_closing_time,
            row.close_time_resolution,
            row.close_flags,
            hex::encode_upper(row.account_hash),
            hex::encode_upper(row.trans_hash),
        ])?;
    }
    Ok(())
}

fn write_to_nudb(replay: &ReplayResult, dat_path: &std::path::Path, key_path: &std::path::Path) -> Result<()> {
    // Write every state node the chunk ever contained (checkpoint ∪ all diff.added), not
    // just replay.state's post-replay live set — otherwise only the chunk's last ledger
    // would be servable. See ReplayResult::all_state_nodes.
    let mut all: HashMap<Hash256, Vec<u8>> =
        HashMap::with_capacity(replay.all_state_nodes.len() + replay.tx_nodes.len());
    for node in replay.all_state_nodes.values() {
        all.entry(node.hash)
            .or_insert_with(|| xrla_nudb::dat::encode_wire_to_value(&node.content, &node.node_type));
    }
    for node in &replay.tx_nodes {
        all.entry(node.hash)
            .or_insert_with(|| xrla_nudb::dat::encode_wire_to_value(&node.content, &node.node_type));
    }
    let entries: Vec<(Hash256, Vec<u8>)> = all.into_iter().collect();
    println!(
        "  {} unique nodes ({} state + {} tx-tree, before dedup)",
        entries.len(),
        replay.all_state_nodes.len(),
        replay.tx_nodes.len()
    );
    xrla_nudb::writer::write_nudb_store(&entries, dat_path, key_path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use xrla_common::chunk::{LedgerDelta, TxRecord};
    use xrla_common::serialize::sha512half;
    use xrla_common::shamap::SHAMapDiff;

    /// A leaf node with its own hash correctly derived from content (`SHA512half(content)`,
    /// no prefix — see `state_tree` module docs), so it survives `verify_state_nodes`.
    fn leaf(tag: u8) -> SHAMapNode {
        let content = vec![tag; 16];
        let hash = sha512half(&content);
        SHAMapNode { hash, node_type: NodeType::AccountState, content }
    }

    /// An inner node with a single child at `slot`, with its own hash correctly derived
    /// from content (so `find_new_root`'s structural check has a real hash to key off).
    fn inner_with_child(slot: usize, child: Hash256) -> SHAMapNode {
        let mut content = vec![0u8; 512];
        content[slot * 32..(slot + 1) * 32].copy_from_slice(&child);
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MIN\0");
        buf.extend_from_slice(&content);
        SHAMapNode { hash: sha512half(&buf), node_type: NodeType::Inner, content }
    }

    /// Two-ledger synthetic chunk exercising the full wiring: checkpoint replay, delta
    /// application, root-finding, tx tree rebuild, and parent_hash-chained LedgerHash
    /// verification. Unlike the unit tests for individual pieces (build_tx_tree,
    /// write_nudb_store), this catches "wired the fields in the wrong order" bugs.
    #[test]
    fn two_ledger_chunk_replays_and_verifies() {
        let leaf_a = leaf(0xAA);
        let root_a = inner_with_child(3, leaf_a.hash);

        let tx_a = TxRecord {
            tx_hash: [0; 32],
            tx_blob: b"txA".to_vec(),
            meta_blob: b"metaA".to_vec(),
        };
        let tx_a = TxRecord { tx_hash: calculate_tx_id(&tx_a.tx_blob), ..tx_a };

        // Ledger A is the checkpoint — its ledger_hash is an external anchor, not
        // chain-verified here, so any value is fine for this synthetic test.
        let ledger_hash_a = [0x99; 32];

        let tx_map_a = TxMap {
            ledger_seq: 100,
            ledger_hash: ledger_hash_a,
            account_hash: root_a.hash,
            drops: 100_000_000_000,
            parent_close_time: 1000,
            close_time: 1010,
            close_time_resolution: 10,
            close_flags: 0,
            txns: vec![tx_a],
        };

        let leaf_b = leaf(0xBB);
        let root_b = inner_with_child(3, leaf_b.hash);

        let tx_b = TxRecord {
            tx_hash: [0; 32],
            tx_blob: b"txB".to_vec(),
            meta_blob: b"metaB".to_vec(),
        };
        let tx_b = TxRecord { tx_hash: calculate_tx_id(&tx_b.tx_blob), ..tx_b };
        let (tx_hash_b, _) = build_tx_tree(&[tx_b.clone()]);

        let ledger_hash_b = calculate_ledger_hash(&LedgerHashInput {
            seq: 101,
            drops: 100_000_005_000,
            parent_hash: ledger_hash_a,
            tx_hash: tx_hash_b,
            account_hash: root_b.hash,
            parent_close_time: 1010,
            close_time: 1020,
            close_time_resolution: 10,
            close_flags: 0,
        });

        let tx_map_b = TxMap {
            ledger_seq: 101,
            ledger_hash: ledger_hash_b,
            account_hash: root_b.hash,
            drops: 100_000_005_000,
            parent_close_time: 1010,
            close_time: 1020,
            close_time_resolution: 10,
            close_flags: 0,
            txns: vec![tx_b],
        };

        let chunk = Chunk {
            network_id: 1,
            start_ledger: 100,
            end_ledger: 101,
            checkpoint_hash: ledger_hash_a,
            chunk_hash: [0; 32],
            checkpoint: vec![leaf_a.clone(), root_a.clone()],
            deltas: vec![LedgerDelta {
                ledger_seq: 101,
                diff: SHAMapDiff {
                    added: vec![leaf_b.clone(), root_b.clone()],
                    deleted: vec![leaf_a.hash, root_a.hash],
                },
            }],
            tx_maps: vec![tx_map_a, tx_map_b],
        };

        let replay = replay_chunk(&chunk, true).expect("replay + verify should succeed");
        assert_eq!(replay.state.len(), 2, "final live state should be exactly ledger B's nodes");
        assert!(replay.state.contains_key(&leaf_b.hash));
        assert!(replay.state.contains_key(&root_b.hash));
        assert!(!replay.state.contains_key(&leaf_a.hash), "superseded ledger-A leaf must not survive");

        // A tampered stored LedgerHash must be caught, not silently accepted.
        let mut bad_chunk = chunk;
        bad_chunk.tx_maps[1].ledger_hash[0] ^= 0xFF;
        let err = replay_chunk(&bad_chunk, true).unwrap_err();
        assert!(
            err.to_string().contains("LedgerHash"),
            "expected a LedgerHash mismatch error, got: {err}"
        );
    }

    /// `tag` distinguishes the two synthetic ranges; `seq` must also be folded into the
    /// hash so every row within a range gets a distinct `LedgerHash` primary key —
    /// otherwise `INSERT OR IGNORE` collapses same-hash rows exactly as it's meant to.
    fn ledger_row(seq: u32, tag: u8) -> LedgerDbRow {
        let mut h = [tag; 32];
        h[28..32].copy_from_slice(&seq.to_be_bytes());
        LedgerDbRow {
            ledger_hash: h,
            ledger_seq: seq,
            prev_hash: h,
            total_coins: 100_000_000_000,
            closing_time: 1000 + seq,
            prev_closing_time: 999 + seq,
            close_time_resolution: 10,
            close_flags: 0,
            account_hash: h,
            trans_hash: h,
        }
    }

    /// Writing a chunk's ledger range into a `ledger.db` that already holds a *different*
    /// range (e.g. a running instance's own current data) must merge, not destroy the
    /// existing rows — see the delete-then-recreate bug this replaced.
    #[test]
    fn write_ledger_db_merges_into_an_existing_populated_file() {
        let dir = std::env::temp_dir().join(format!("xrla-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("ledger.db");
        let _ = std::fs::remove_file(&path);

        // Simulate a running instance's existing ledger.db: ledgers 100-200.
        write_ledger_db(&(100..=200).map(|s| ledger_row(s, 1)).collect::<Vec<_>>(), &path)
            .expect("initial write");

        // Now import an older, disjoint range: 50-99.
        write_ledger_db(&(50..=99).map(|s| ledger_row(s, 2)).collect::<Vec<_>>(), &path)
            .expect("merge write");

        let conn = rusqlite::Connection::open(&path).unwrap();
        let count: u32 = conn.query_row("SELECT COUNT(*) FROM Ledgers", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 151, "both ranges (50-99 and 100-200) must be present, not one replacing the other");

        let has_75: bool = conn
            .query_row("SELECT 1 FROM Ledgers WHERE LedgerSeq = 75", [], |r| r.get(0))
            .unwrap_or(false);
        assert!(has_75, "the newly-merged range must be queryable");
        let has_150: bool = conn
            .query_row("SELECT 1 FROM Ledgers WHERE LedgerSeq = 150", [], |r| r.get(0))
            .unwrap_or(false);
        assert!(has_150, "the pre-existing range must survive the merge, not be wiped");

        std::fs::remove_dir_all(&dir).ok();
    }
}
