/// xrla-import — import one or more XRLA chunk files into a xrpld-compatible NuDB store.
///
/// Usage:
///   xrla-import --chunk ./chunks/xrla_1_01000000_01001000.xrla \
///               --dat /var/lib/xrpld/db/nudb.dat
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

use xrla_common::chunk::{Chunk, TxMap, FORMAT_VERSION_STREAMED};
use xrla_common::serialize::{calculate_ledger_hash, deserialize_chunk, ChunkReader, LedgerHashInput};
use xrla_common::shamap::{Hash256, InnerNode, NodeType, SHAMapNode};
use xrla_common::state_tree::verify_state_nodes;
use xrla_common::tx_tree::{build_tx_tree, calculate_tx_id};
use xrla_nudb::writer::NuDbSink;

mod txdb;
use txdb::TxDbSink;

#[derive(Parser, Debug)]
#[command(name = "xrla-import", about = "Import an XRLA chunk file into xrpld NuDB")]
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

    /// Path to write xrpld's ledger.db (Ledgers index) alongside the NuDB store. The
    /// chunk's checkpoint ledger has no in-chunk PrevHash (its parent is external to this
    /// chunk), so it is not written; every ledger from the first delta onward chains
    /// internally and is written.
    #[arg(long)]
    ledger_db: Option<PathBuf>,

    /// Path to write xrpld's transaction.db (`Transactions` + `AccountTransactions`), which
    /// xrpld answers `account_tx` and `tx` from. Also writes each ledger's header into the
    /// NuDB store so xrpld can load imported ledgers by hash. Refuses an existing file. Roughly
    /// 40 GB per 150k-ledger chunk.
    #[arg(long)]
    txdb: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let key_path = args.dat.with_extension("key");
    println!("Writing NuDB store: {} / {}", args.dat.display(), key_path.display());

    // One sink for the whole invocation: every node any chunk produces is written to the
    // .dat file the moment replay produces it, then dropped. Sharing a single sink across
    // all --chunk files also gives cross-chunk dedup for free (a node duplicated in a later
    // chunk's checkpoint is written once), which is what the old
    // `all_state_nodes.entry().or_insert()` union used to do — without holding that union.
    let mut sink = NuDbSink::create(&args.dat)?;
    let mut ledger_rows: Vec<LedgerDbRow> = Vec::new();
    let mut txdb: Option<TxDbSink> = match &args.txdb {
        Some(p) => Some(TxDbSink::create(p)?),
        None => None,
    };

    for chunk_path in &args.chunk {
        println!("Reading chunk: {}", chunk_path.display());

        // v3 files are streamed straight from disk (see replay_chunk_streaming). v2 files
        // can't be streamed (see spec/chunk-format.md) and fall back to the original
        // read-whole-file-then-parse path.
        let replay = if detect_version(chunk_path)? == FORMAT_VERSION_STREAMED {
            replay_chunk_streaming(chunk_path, !args.skip_verify, &mut sink, &mut txdb)?
        } else {
            let data = fs::read(chunk_path)?;
            println!("Deserializing and verifying chunk (v2, buffered)...");
            let chunk = deserialize_chunk(&data).map_err(|e| anyhow::anyhow!("{e}"))?;
            println!(
                "Chunk: network={} ledgers={}..{} ({} ledgers)",
                chunk.network_id, chunk.start_ledger, chunk.end_ledger, chunk.ledger_count()
            );
            println!("Chunk hash OK: {}", hex::encode(chunk.chunk_hash));
            replay_chunk(&chunk, !args.skip_verify, &mut sink, &mut txdb)?
        };

        println!(
            "Replayed {} live state nodes; {} unique nodes written so far",
            replay.state.len(),
            sink.node_count(),
        );
        ledger_rows.extend(replay.ledger_rows);
    }

    println!("  {} unique nodes total", sink.node_count());
    if let Some(t) = txdb.take() {
        let (n_tx, n_acct) = t.counts();
        println!("Building transaction.db indexes ({n_tx} transactions, {n_acct} account rows)...");
        t.finish()?;
    }
    sink.finish(&key_path)?;

    if let Some(ledger_db_path) = &args.ledger_db {
        println!("Writing ledger.db: {}", ledger_db_path.display());
        write_ledger_db(&ledger_rows, ledger_db_path)?;
        println!("  {} rows written", ledger_rows.len());
    }

    println!("Import complete.");
    Ok(())
}

/// One row for xrpld's `Ledgers` table (`ledger.db`), matching `kLgrDbInit`
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
    /// used for the checkpoint root-presence check and as the live-set invariant (a
    /// superseded node must not survive here). **Bounded**: each delta inserts its added
    /// nodes and removes its deleted ones, so this tracks the account state's current size
    /// (~28M nodes on mainnet today) and does *not* grow with the chunk's ledger range.
    ///
    /// Every node the chunk contains — checkpoint plus every delta's `diff.added`, whether
    /// or not a later delta superseded it, plus every ledger's rebuilt tx-tree nodes — is
    /// written straight to the `NuDbSink` as replay produces it and then dropped. A real
    /// full-history node retains every node any served ledger ever referenced, so all of
    /// them must reach the store, but none of them need to be *retained in memory* to get
    /// there. Accumulating them (as `all_state_nodes` / `tx_nodes` fields used to) is what
    /// made import memory scale with range length: 64 GB for 20,000 real mainnet ledgers,
    /// extrapolating past 480 GB at 150,000. See STATUS.md.
    state: HashMap<Hash256, Rc<SHAMapNode>>,
    /// One row per ledger from the first delta onward (see `Args::ledger_db` doc comment
    /// for why the checkpoint ledger itself is excluded). Small: ~130 bytes/ledger.
    ledger_rows: Vec<LedgerDbRow>,
}

/// Encode a node into its NuDB on-disk value and hand it to the sink, which writes it to
/// the `.dat` file immediately. The encoded `Vec<u8>` lives only for this call.
/// With `--txdb`: store the ledger header as the NuDB object xrpld looks up by LedgerHash, and
/// write the ledger's transaction rows. A no-op without `--txdb`.
fn write_ledger_outputs(
    sink: &mut NuDbSink,
    txdb: &mut Option<TxDbSink>,
    tx_map: &TxMap,
    parent_hash: Hash256,
    tx_hash: Hash256,
    account_hash: Hash256,
) -> Result<()> {
    let Some(t) = txdb.as_mut() else { return Ok(()) };
    let header = xrla_common::serialize::ledger_header_object(&LedgerHashInput {
        seq: tx_map.ledger_seq,
        drops: tx_map.drops,
        parent_hash,
        tx_hash,
        account_hash,
        parent_close_time: tx_map.parent_close_time,
        close_time: tx_map.close_time,
        close_time_resolution: tx_map.close_time_resolution,
        close_flags: tx_map.close_flags,
    });
    let value = xrla_nudb::dat::encode_object_to_value(xrla_nudb::dat::NOTYPE_LEDGER, &header);
    sink.write_node(tx_map.ledger_hash, &value)?;
    t.write_ledger(tx_map)
}

fn sink_node(sink: &mut NuDbSink, node: &SHAMapNode) -> Result<()> {
    // Check before encoding: on a multi-chunk import every later chunk's checkpoint repeats
    // ~28M nodes the previous chunk already wrote, and encoding just to discard is wasted work.
    if sink.contains(&node.hash) {
        return Ok(());
    }
    let value = xrla_nudb::dat::encode_wire_to_value(&node.content, &node.node_type);
    sink.write_node(node.hash, &value)
}

/// Replay checkpoint + deltas, rebuilding each ledger's transaction tree along the way.
/// When `verify` is true, independently recomputes and asserts (bailing on the first
/// mismatch): per-transaction authenticity, the account-state root, and the full
/// LedgerHash chained to the previous ledger.
fn replay_chunk(
    chunk: &Chunk,
    verify: bool,
    sink: &mut NuDbSink,
    txdb: &mut Option<TxDbSink>,
) -> Result<ReplayResult> {
    let mut state: HashMap<Hash256, Rc<SHAMapNode>> = chunk
        .checkpoint
        .iter()
        .map(|n| (n.hash, Rc::new(n.clone())))
        .collect();
    // Checkpoint nodes go to the store immediately; only the live `state` map is retained.
    for node in &chunk.checkpoint {
        sink_node(sink, node)?;
    }

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
    if let Some(t) = txdb.as_mut() {
        t.write_ledger(cp)?;
    }
    let (_, nodes) = build_tx_tree(&cp.txns);
    for node in &nodes {
        sink_node(sink, node)?;
    }
    drop(nodes);
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
            state.insert(node.hash, Rc::new(node.clone()));
            sink_node(sink, node)?;
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
        for node in &nodes {
            sink_node(sink, node)?;
        }
        drop(nodes);

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

        write_ledger_outputs(sink, txdb, &tx_map, prev_ledger_hash, tx_hash, new_root)?;
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

    Ok(ReplayResult { state, ledger_rows })
}

/// Peeks a chunk file's format-version byte (5 bytes read, not the whole file) so `main`
/// can decide whether to stream it (v3) or fall back to the buffered path (v2).
fn detect_version(path: &std::path::Path) -> Result<u8> {
    use std::io::Read;
    let mut f = fs::File::open(path)?;
    let mut buf = [0u8; 5]; // magic(4) + version(1)
    f.read_exact(&mut buf)?;
    Ok(buf[4])
}

/// Same contract and same verification as `replay_chunk`, but for a v3 file read via
/// `ChunkReader` instead of a fully-materialized `Chunk` — see the call site in `main`.
/// Each checkpoint node is verified against its own claimed hash as it streams in
/// (`state_tree::recompute_node_hash`), not batched afterward — batching would mean
/// cloning the entire checkpoint a second time just to hand it to `verify_state_nodes`,
/// defeating the point of streaming.
fn replay_chunk_streaming(
    path: &std::path::Path,
    verify: bool,
    sink: &mut NuDbSink,
    txdb: &mut Option<TxDbSink>,
) -> Result<ReplayResult> {
    let mut reader = ChunkReader::open(path)?;
    println!(
        "Chunk: network={} ledgers={}..{} ({} ledgers, streamed)",
        reader.network_id,
        reader.start_ledger,
        reader.end_ledger,
        reader.end_ledger - reader.start_ledger + 1
    );

    let mut state: HashMap<Hash256, Rc<SHAMapNode>> = HashMap::new();
    let mut bad_checkpoint_hash: Option<Hash256> = None;
    let mut sink_err: Option<anyhow::Error> = None;
    reader.read_checkpoint(|node| {
        if verify && bad_checkpoint_hash.is_none() {
            if xrla_common::state_tree::recompute_node_hash(&node) != node.hash {
                bad_checkpoint_hash = Some(node.hash);
            }
        }
        // Straight to the .dat file as it is parsed; only the live map is retained.
        if sink_err.is_none() {
            if let Err(e) = sink_node(sink, &node) {
                sink_err = Some(e);
            }
        }
        state.insert(node.hash, Rc::new(node));
    })?;
    if let Some(e) = sink_err {
        return Err(e);
    }
    if let Some(bad_hash) = bad_checkpoint_hash {
        bail!(
            "checkpoint: node {} does not hash to its own claimed content \
             (source data corruption or a decode bug)",
            hex::encode(bad_hash)
        );
    }

    let cp = reader.read_checkpoint_tx_map()?;
    if !state.contains_key(&cp.account_hash) {
        bail!(
            "checkpoint account_hash {} not found among checkpoint nodes",
            hex::encode(cp.account_hash)
        );
    }
    if verify {
        verify_txns_authentic(&cp)?;
    }
    if let Some(t) = txdb.as_mut() {
        t.write_ledger(&cp)?;
    }
    let (_, nodes) = build_tx_tree(&cp.txns);
    for node in &nodes {
        sink_node(sink, node)?;
    }
    drop(nodes);
    if verify {
        println!(
            "  ledger {} (checkpoint): account_hash OK, {} txns authentic, {} state nodes \
             self-consistent (LedgerHash needs an external parent_hash anchor — not verified here)",
            cp.ledger_seq,
            cp.txns.len(),
            state.len()
        );
    }

    let mut current_root = cp.account_hash;
    let mut prev_ledger_hash = cp.ledger_hash;
    let mut ledger_rows = Vec::new();

    while let Some((delta, tx_map)) = reader.next_delta_tx_map()? {
        for node in &delta.diff.added {
            state.insert(node.hash, Rc::new(node.clone()));
            sink_node(sink, node)?;
        }
        for hash in &delta.diff.deleted {
            state.remove(hash);
        }

        if tx_map.ledger_seq != delta.ledger_seq {
            bail!(
                "delta/tx_map sequence mismatch: delta.ledger_seq={} tx_map.ledger_seq={}",
                delta.ledger_seq, tx_map.ledger_seq
            );
        }

        let new_root = find_new_root(&delta.diff.added, &current_root)?;
        let (tx_hash, nodes) = build_tx_tree(&tx_map.txns);
        for node in &nodes {
            sink_node(sink, node)?;
        }
        drop(nodes);

        if verify {
            verify_txns_authentic(&tx_map)?;

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

        write_ledger_outputs(sink, txdb, &tx_map, prev_ledger_hash, tx_hash, new_root)?;
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

    reader.finish()?;
    Ok(ReplayResult { state, ledger_rows })
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
/// Write xrpld's `ledger.db` `Ledgers` table. Schema matches `kLgrDbInit`
/// (`include/xrpl/rdb/DBInit.h`) exactly; hashes are stored as uppercase hex, matching
/// what a real xrpld node writes (and what `xrla-export`'s `parse_hash` reads back).
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

#[cfg(test)]
mod tests {
    use super::*;
    use xrla_common::chunk::{LedgerDelta, TxRecord};
    use xrla_common::serialize::sha512half;
    use xrla_common::shamap::SHAMapDiff;

    /// A sink writing into a throwaway temp dir, for tests that care about replay results
    /// rather than the store bytes. Returns the sink plus its dir so the caller can drop it.
    fn temp_sink(tag: &str) -> (NuDbSink, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("xrla_sink_{tag}_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let sink = NuDbSink::create(&dir.join("nudb.dat")).unwrap();
        (sink, dir)
    }

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

        let (mut sink, sink_dir) = temp_sink("two_ledger");
        let replay = replay_chunk(&chunk, true, &mut sink, &mut None).expect("replay + verify should succeed");
        assert_eq!(replay.state.len(), 2, "final live state should be exactly ledger B's nodes");
        assert!(replay.state.contains_key(&leaf_b.hash));
        assert!(replay.state.contains_key(&root_b.hash));
        assert!(!replay.state.contains_key(&leaf_a.hash), "superseded ledger-A leaf must not survive");

        // A tampered stored LedgerHash must be caught, not silently accepted.
        let mut bad_chunk = chunk;
        bad_chunk.tx_maps[1].ledger_hash[0] ^= 0xFF;
        let (mut bad_sink, bad_sink_dir) = temp_sink("two_ledger_bad");
        let err = replay_chunk(&bad_chunk, true, &mut bad_sink, &mut None).unwrap_err();
        assert!(
            err.to_string().contains("LedgerHash"),
            "expected a LedgerHash mismatch error, got: {err}"
        );
        fs::remove_dir_all(&sink_dir).ok();
        fs::remove_dir_all(&bad_sink_dir).ok();
    }

    /// The same two-ledger chunk as above, but written to a real v3 file via `ChunkWriter`
    /// and replayed via `replay_chunk_streaming` — the path a real mainnet import now takes
    /// (see `detect_version` dispatch in `main`). Asserts it produces the identical result
    /// as the buffered `replay_chunk` path on the same data, so the streaming rewrite (done
    /// to fix a real 127 GB OOM importing a real 20k-ledger mainnet chunk) didn't silently
    /// change what actually gets verified or written.
    #[test]
    fn streaming_replay_matches_buffered_replay_on_the_same_chunk() {
        use xrla_common::serialize::ChunkWriter;

        let leaf_a = leaf(0xAA);
        let root_a = inner_with_child(3, leaf_a.hash);
        let tx_a = TxRecord {
            tx_hash: [0; 32],
            tx_blob: b"txA".to_vec(),
            meta_blob: b"metaA".to_vec(),
        };
        let tx_a = TxRecord { tx_hash: calculate_tx_id(&tx_a.tx_blob), ..tx_a };
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
            tx_maps: vec![tx_map_a.clone(), tx_map_b.clone()],
        };
        let (mut buf_sink, buf_dir) = temp_sink("cmp_buffered");
        let buffered =
            replay_chunk(&chunk, true, &mut buf_sink, &mut None).expect("buffered replay should succeed");
        let buffered_nodes = buf_sink.node_count();

        let dir = std::env::temp_dir()
            .join(format!("xrla_import_streaming_test_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.xrla");
        let mut writer =
            ChunkWriter::create(path.clone(), 1, 100, 101, ledger_hash_a).unwrap();
        {
            let mut refs = vec![&root_a, &leaf_a];
            writer.write_checkpoint(&mut refs).unwrap();
        }
        writer.write_tx_map(&tx_map_a).unwrap();
        writer
            .write_delta(&LedgerDelta {
                ledger_seq: 101,
                diff: SHAMapDiff {
                    added: vec![leaf_b.clone(), root_b.clone()],
                    deleted: vec![leaf_a.hash, root_a.hash],
                },
            })
            .unwrap();
        writer.write_tx_map(&tx_map_b).unwrap();
        writer.finish().unwrap();

        let (mut str_sink, str_dir) = temp_sink("cmp_streamed");
        let streamed = replay_chunk_streaming(&path, true, &mut str_sink, &mut None)
            .expect("streaming replay should succeed");
        let streamed_nodes = str_sink.node_count();

        assert_eq!(streamed.state.len(), buffered.state.len());
        assert!(streamed.state.contains_key(&leaf_b.hash));
        assert!(streamed.state.contains_key(&root_b.hash));
        assert!(!streamed.state.contains_key(&leaf_a.hash));
        // Both paths must write the identical set of nodes to the store.
        assert_eq!(streamed_nodes, buffered_nodes, "both paths must write the same node count");
        assert_eq!(streamed.ledger_rows.len(), buffered.ledger_rows.len());
        assert_eq!(streamed.ledger_rows[0].ledger_seq, buffered.ledger_rows[0].ledger_seq);
        assert_eq!(streamed.ledger_rows[0].account_hash, buffered.ledger_rows[0].account_hash);

        // A tampered stored LedgerHash must be caught here too, not just in the buffered path.
        let mut bad_tx_map_b = tx_map_b.clone();
        bad_tx_map_b.ledger_hash[0] ^= 0xFF;
        let bad_path = dir.join("bad.xrla");
        let mut bad_writer =
            ChunkWriter::create(bad_path.clone(), 1, 100, 101, ledger_hash_a).unwrap();
        {
            let mut refs = vec![&root_a, &leaf_a];
            bad_writer.write_checkpoint(&mut refs).unwrap();
        }
        bad_writer.write_tx_map(&tx_map_a).unwrap();
        bad_writer
            .write_delta(&LedgerDelta {
                ledger_seq: 101,
                diff: SHAMapDiff {
                    added: vec![leaf_b.clone(), root_b.clone()],
                    deleted: vec![leaf_a.hash, root_a.hash],
                },
            })
            .unwrap();
        bad_writer.write_tx_map(&bad_tx_map_b).unwrap();
        bad_writer.finish().unwrap();
        let (mut bad2_sink, bad2_dir) = temp_sink("cmp_bad");
        let err = replay_chunk_streaming(&bad_path, true, &mut bad2_sink, &mut None).unwrap_err();
        assert!(
            err.to_string().contains("LedgerHash"),
            "expected a LedgerHash mismatch error, got: {err}"
        );

        fs::remove_dir_all(&dir).ok();
        for d in [&buf_dir, &str_dir, &bad2_dir] {
            fs::remove_dir_all(d).ok();
        }
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
