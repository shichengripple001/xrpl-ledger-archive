/// xrla-export — export a range of ledgers from a rippled NuDB store
/// into an XRLA chunk file.
///
/// Usage:
///   xrla-export --dat /var/lib/rippled/db/nudb.dat \
///               --ledgers /var/lib/rippled/db/ledger.db \
///               --start 1000000 --end 1001000 \
///               --out ./chunks/

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::Parser;
use rusqlite::{Connection, params};

use xrla_common::chunk::{chunk_filename, Chunk, LedgerDelta, TxMap, NETWORK_MAINNET};
use xrla_common::serialize::{calculate_ledger_hash, serialize_chunk, LedgerHashInput};
use xrla_common::shamap::{Hash256, SHAMapNode};
use xrla_nudb::NuDBReader;

#[derive(Parser, Debug)]
#[command(name = "xrla-export", about = "Export XRPL ledger history to chunk files")]
struct Args {
    /// Path to a rippled NuDB .dat file (sibling nudb.key must exist). Repeat for each
    /// shard — online_delete keeps two databases live and the state spans both.
    #[arg(long, required = true, num_args = 1..)]
    dat: Vec<PathBuf>,

    /// Path to rippled ledger SQLite database (ledger.db)
    #[arg(long)]
    ledgers: PathBuf,

    /// Start ledger sequence (inclusive)
    #[arg(long)]
    start: u32,

    /// End ledger sequence (inclusive)
    #[arg(long)]
    end: u32,

    /// Output directory for chunk files
    #[arg(long, default_value = ".")]
    out: PathBuf,

    /// Network ID (1=mainnet, 2=testnet, 3=devnet)
    #[arg(long, default_value_t = NETWORK_MAINNET)]
    network_id: u32,

    /// Ledgers per chunk (1 checkpoint + chunk_size-1 deltas). Only the very first chunk's
    /// checkpoint costs a full NuDB trie walk — every chunk after that gets its checkpoint
    /// by snapshotting the running in-memory state, not by re-reading NuDB. See PLAN.md
    /// Phase 2 item 1.
    #[arg(long, default_value_t = 10_000)]
    chunk_size: u32,
}

/// Maintain-state-across-chunks export. Only the very first chunk's checkpoint costs a real
/// NuDB trie walk (`collect_reachable_adaptive`); every chunk after that gets its checkpoint
/// by snapshotting the running in-memory `state` map, which is kept alive and updated by each
/// ledger's delta as the whole range is scanned forward once. See PLAN.md Phase 2 item 1 —
/// this is what turns "one full trie walk per chunk" into "one full trie walk, ever."
///
/// Memory note: `state` holds every live SHAMap node for the entire export run — at today's
/// mainnet scale that's ~27M nodes (~8 GB, see PLAN.md Storage Estimate). This is a known,
/// accepted tradeoff of this design, not addressed here.
fn main() -> Result<()> {
    let args = Args::parse();

    if args.end <= args.start {
        bail!("--end must be greater than --start");
    }
    if args.chunk_size == 0 {
        bail!("--chunk-size must be at least 1");
    }

    fs::create_dir_all(&args.out)?;

    let nudb = NuDBReader::open(&args.dat)?;

    println!("Opening ledger index: {}", args.ledgers.display());
    let ledger_db = LedgerIndex::open(&args.ledgers)?;

    println!(
        "Exporting ledgers {}..{} in chunks of {} ledgers (one trie walk total, not one per chunk)",
        args.start, args.end, args.chunk_size
    );

    // The one and only full trie walk for the whole export run.
    let first_info = ledger_db.get(args.start)?;
    let first_ledger_hash = first_info.verify_ledger_hash(args.start)?;
    println!(
        "Building initial checkpoint at ledger {} (account_hash={})...",
        args.start,
        hex::encode(first_info.account_hash)
    );
    let walk_start = std::time::Instant::now();
    // Calibrate once and reuse the same concurrency level for both the initial walk and
    // batching delta computation below (diff_batch_concurrent) — one calibration, not two.
    // See PLAN.md Immediate TODOs item 10b.
    let concurrency = nudb.calibrate_concurrency(&first_info.account_hash)?;
    let initial_nodes = nudb.collect_reachable_concurrent(&first_info.account_hash, concurrency)?;
    println!(
        "Initial checkpoint: {} nodes ({:.1}s, concurrency={})",
        initial_nodes.len(),
        walk_start.elapsed().as_secs_f64(),
        concurrency
    );

    let mut state: HashMap<Hash256, SHAMapNode> =
        initial_nodes.iter().map(|n| (n.hash, n.clone())).collect();

    let first_txns = nudb
        .collect_transactions(&first_info.tx_hash)
        .with_context(|| format!("tx collect failed at ledger {}", args.start))?;

    let mut chunk_start = args.start;
    let mut chunk_checkpoint_hash = first_ledger_hash;
    let mut chunk_checkpoint_nodes = initial_nodes;
    let mut chunk_deltas: Vec<LedgerDelta> = Vec::new();
    let mut chunk_tx_maps: Vec<TxMap> = vec![TxMap {
        ledger_seq: args.start,
        ledger_hash: first_ledger_hash,
        account_hash: first_info.account_hash,
        drops: first_info.total_coins,
        parent_close_time: first_info.prev_closing_time,
        close_time: first_info.closing_time,
        close_time_resolution: first_info.close_time_resolution,
        close_flags: first_info.close_flags,
        txns: first_txns,
    }];

    let mut prev_account_hash = first_info.account_hash;
    let mut chunks_written = 0usize;
    let mut total_added = 0usize;
    let mut total_deleted = 0usize;
    let mut total_txns = chunk_tx_maps[0].txns.len();

    // Process ledgers in batches of up to `concurrency` at a time: fetch the batch's ledger
    // info (cheap, local SQLite — unchanged), compute all their diffs CONCURRENTLY against
    // NuDB (the actual bottleneck across a full-history export — see PLAN.md Immediate TODOs
    // item 10b), then apply each diff to the running state IN ORDER. Discovery of what
    // changed is parallel; applying it to `state` and writing chunks stays strictly
    // sequential, since each ledger's starting state depends on the previous one already
    // being applied.
    let batch_size = concurrency.max(1) as u32;
    let mut seq = args.start + 1;
    while seq <= args.end {
        let batch_end = (seq + batch_size - 1).min(args.end);
        let batch_seqs: Vec<u32> = (seq..=batch_end).collect();

        let batch_infos: Vec<LedgerInfo> = batch_seqs
            .iter()
            .map(|&s| ledger_db.get(s))
            .collect::<Result<Vec<_>>>()?;
        let batch_ledger_hashes: Vec<Hash256> = batch_infos
            .iter()
            .zip(&batch_seqs)
            .map(|(info, &s)| info.verify_ledger_hash(s))
            .collect::<Result<Vec<_>>>()?;

        let mut pairs = Vec::with_capacity(batch_infos.len());
        let mut running_prev = prev_account_hash;
        for info in &batch_infos {
            pairs.push((running_prev, info.account_hash));
            running_prev = info.account_hash;
        }

        let diffs = nudb
            .diff_batch_concurrent(&pairs, concurrency)
            .with_context(|| format!("batch diff failed for ledgers {seq}..={batch_end}"))?;

        for (i, diff) in diffs.into_iter().enumerate() {
            let s = batch_seqs[i];
            let curr_info = &batch_infos[i];
            let curr_ledger_hash = batch_ledger_hashes[i];

            for node in &diff.added {
                state.insert(node.hash, node.clone());
            }
            for hash in &diff.deleted {
                state.remove(hash);
            }

            let txns = nudb
                .collect_transactions(&curr_info.tx_hash)
                .with_context(|| format!("tx collect failed at ledger {s}"))?;

            println!(
                "  ledger {s}: +{} -{} nodes ({} bytes), {} txns",
                diff.added.len(),
                diff.deleted.len(),
                diff.added.iter().map(|n| n.content.len() + 33).sum::<usize>(),
                txns.len()
            );
            total_added += diff.added.len();
            total_deleted += diff.deleted.len();
            total_txns += txns.len();

            if s - chunk_start == args.chunk_size {
                // Close out the just-finished chunk (its checkpoint + deltas accumulated so far).
                write_chunk(
                    &args,
                    chunk_start,
                    s - 1,
                    chunk_checkpoint_hash,
                    std::mem::take(&mut chunk_checkpoint_nodes),
                    std::mem::take(&mut chunk_deltas),
                    std::mem::take(&mut chunk_tx_maps),
                )?;
                chunks_written += 1;

                // This ledger becomes the NEXT chunk's checkpoint ledger — its state is
                // already in `state` (we just applied its diff above), so no NuDB walk needed.
                chunk_start = s;
                chunk_checkpoint_hash = curr_ledger_hash;
                chunk_checkpoint_nodes = state.values().cloned().collect();
                chunk_tx_maps.push(TxMap {
                    ledger_seq: s,
                    ledger_hash: curr_ledger_hash,
                    account_hash: curr_info.account_hash,
                    drops: curr_info.total_coins,
                    parent_close_time: curr_info.prev_closing_time,
                    close_time: curr_info.closing_time,
                    close_time_resolution: curr_info.close_time_resolution,
                    close_flags: curr_info.close_flags,
                    txns,
                });
            } else {
                chunk_deltas.push(LedgerDelta { ledger_seq: s, diff });
                chunk_tx_maps.push(TxMap {
                    ledger_seq: s,
                    ledger_hash: curr_ledger_hash,
                    account_hash: curr_info.account_hash,
                    drops: curr_info.total_coins,
                    parent_close_time: curr_info.prev_closing_time,
                    close_time: curr_info.closing_time,
                    close_time_resolution: curr_info.close_time_resolution,
                    close_flags: curr_info.close_flags,
                    txns,
                });
            }

            prev_account_hash = curr_info.account_hash;
        }

        seq = batch_end + 1;
    }

    // Final (possibly partial) chunk.
    write_chunk(
        &args,
        chunk_start,
        args.end,
        chunk_checkpoint_hash,
        chunk_checkpoint_nodes,
        chunk_deltas,
        chunk_tx_maps,
    )?;
    chunks_written += 1;

    let ledger_count = args.end - args.start;
    println!(
        "\nTotals: {chunks_written} chunk(s), +{total_added} -{total_deleted} nodes, \
         {total_txns} txns across {ledger_count} ledgers (avg +{}/ledger)",
        if ledger_count > 0 { total_added / ledger_count as usize } else { 0 }
    );

    Ok(())
}

fn write_chunk(
    args: &Args,
    start_ledger: u32,
    end_ledger: u32,
    checkpoint_hash: Hash256,
    checkpoint: Vec<SHAMapNode>,
    deltas: Vec<LedgerDelta>,
    tx_maps: Vec<TxMap>,
) -> Result<()> {
    let checkpoint_len = checkpoint.len();
    let delta_count = deltas.len();

    let chunk = Chunk {
        network_id: args.network_id,
        start_ledger,
        end_ledger,
        checkpoint_hash,
        chunk_hash: [0u8; 32], // computed by serialize_chunk
        checkpoint,
        deltas,
        tx_maps,
    };

    let bytes = serialize_chunk(&chunk)?;
    let filename = chunk_filename(args.network_id, start_ledger, end_ledger);
    let out_path = args.out.join(&filename);
    fs::write(&out_path, &bytes)?;

    // chunk_hash is at byte offset 77 in the header (4+1+4+4+4+32+32 = 81 bytes header,
    // chunk_hash starts at offset 4+1+4+4+4+32 = 49)
    let chunk_hash_hex = hex::encode(&bytes[49..81]);

    println!(
        "Wrote {} ({} bytes, {} checkpoint nodes, {} deltas)\n  chunk_hash: {}",
        out_path.display(),
        bytes.len(),
        checkpoint_len,
        delta_count,
        chunk_hash_hex
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// LedgerIndex: reads rippled's ledger SQLite database
//
// Table: Ledgers
//   LedgerHash      TEXT — hex-encoded ledger hash
//   LedgerSeq       INT
//   PrevHash        TEXT — hex-encoded parent ledger hash
//   TotalCoins      INT  — drops in circulation
//   ClosingTime     INT
//   PrevClosingTime INT
//   CloseTimeRes    INT
//   CloseFlags      INT
//   AccountSetHash  TEXT — hex-encoded state SHAMap root hash
//   TransSetHash    TEXT — hex-encoded tx SHAMap root hash
//
// Source: src/xrpld/app/rdb/backend/detail/Node.cpp
// ---------------------------------------------------------------------------

struct LedgerInfo {
    ledger_hash:  Hash256,
    account_hash: Hash256, // state SHAMap root
    tx_hash:      Hash256, // transaction SHAMap root (TransSetHash)
    parent_hash:  Hash256,
    total_coins:  u64,
    closing_time: u32,
    prev_closing_time: u32,
    close_time_resolution: u8,
    close_flags: u8,
}

impl LedgerInfo {
    /// Independently recompute this ledger's LedgerHash and verify it matches
    /// what the source database claims. Returns the verified hash.
    fn verify_ledger_hash(&self, seq: u32) -> Result<Hash256> {
        let recomputed = calculate_ledger_hash(&LedgerHashInput {
            seq,
            drops: self.total_coins,
            parent_hash: self.parent_hash,
            tx_hash: self.tx_hash,
            account_hash: self.account_hash,
            parent_close_time: self.prev_closing_time,
            close_time: self.closing_time,
            close_time_resolution: self.close_time_resolution,
            close_flags: self.close_flags,
        });
        if recomputed != self.ledger_hash {
            bail!(
                "LedgerHash mismatch at ledger {seq}: db says {}, recomputed {}",
                hex::encode(self.ledger_hash),
                hex::encode(recomputed)
            );
        }
        Ok(recomputed)
    }
}

struct LedgerIndex {
    conn: Connection,
}

impl LedgerIndex {
    fn open(path: &Path) -> Result<Self> {
        if !path.exists() {
            bail!("ledger database not found: {}", path.display());
        }
        let conn = Connection::open(path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        Ok(Self { conn })
    }

    fn get(&self, seq: u32) -> Result<LedgerInfo> {
        let row: (String, String, String, String, u64, u32, u32, u8, u8) = self
            .conn
            .query_row(
                "SELECT LedgerHash, AccountSetHash, TransSetHash, PrevHash, \
                        TotalCoins, ClosingTime, PrevClosingTime, CloseTimeRes, CloseFlags \
                 FROM Ledgers WHERE LedgerSeq = ?1",
                params![seq],
                |row| {
                    Ok((
                        row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?,
                        row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?, row.get(8)?,
                    ))
                },
            )
            .with_context(|| format!("ledger {seq} not found in database"))?;
        let (ledger_hash_hex, account_hash_hex, tx_hash_hex, parent_hash_hex,
             total_coins, closing_time, prev_closing_time, close_time_resolution, close_flags) = row;

        Ok(LedgerInfo {
            ledger_hash:  parse_hash(&ledger_hash_hex)
                .with_context(|| format!("invalid LedgerHash for seq {seq}"))?,
            account_hash: parse_hash(&account_hash_hex)
                .with_context(|| format!("invalid AccountSetHash for seq {seq}"))?,
            tx_hash:      parse_hash(&tx_hash_hex)
                .with_context(|| format!("invalid TransSetHash for seq {seq}"))?,
            parent_hash:  parse_hash(&parent_hash_hex)
                .with_context(|| format!("invalid PrevHash for seq {seq}"))?,
            total_coins,
            closing_time,
            prev_closing_time,
            close_time_resolution,
            close_flags,
        })
    }
}

fn parse_hash(s: &str) -> Result<Hash256> {
    let bytes = hex::decode(s.trim())?;
    if bytes.len() != 32 {
        bail!("expected 32-byte hash, got {} bytes from '{}'", bytes.len(), s);
    }
    let mut h = [0u8; 32];
    h.copy_from_slice(&bytes);
    Ok(h)
}
