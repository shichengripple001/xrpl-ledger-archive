/// xrla-export — export a range of ledgers from a rippled NuDB store
/// into an XRLA chunk file.
///
/// Usage:
///   xrla-export --dat /var/lib/rippled/db/nudb.dat \
///               --ledgers /var/lib/rippled/db/ledger.db \
///               --start 1000000 --end 1001000 \
///               --out ./chunks/

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::Parser;
use rusqlite::{Connection, params};

use xrla_common::chunk::{chunk_filename, Chunk, LedgerDelta, TxMap, NETWORK_MAINNET};
use xrla_common::serialize::{calculate_ledger_hash, serialize_chunk, LedgerHashInput};
use xrla_common::shamap::Hash256;
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
}

fn main() -> Result<()> {
    let args = Args::parse();

    if args.end <= args.start {
        bail!("--end must be greater than --start");
    }

    fs::create_dir_all(&args.out)?;

    let nudb = NuDBReader::open(&args.dat)?;

    println!("Opening ledger index: {}", args.ledgers.display());
    let ledger_db = LedgerIndex::open(&args.ledgers)?;

    println!("Exporting ledgers {}..{}", args.start, args.end);

    // Checkpoint: full state at start_ledger
    let start_info = ledger_db.get(args.start)?;
    let start_ledger_hash = start_info.verify_ledger_hash(args.start)?;
    println!(
        "Building checkpoint at ledger {} (state_hash={}, ledger_hash verified)...",
        args.start,
        hex::encode(start_info.account_hash)
    );
    let checkpoint_nodes = nudb.collect_reachable(&start_info.account_hash)?;
    println!("Checkpoint: {} nodes", checkpoint_nodes.len());

    // TX map for start ledger (no delta, just transactions)
    let start_txns = nudb
        .collect_transactions(&start_info.tx_hash)
        .with_context(|| format!("tx collect failed at ledger {}", args.start))?;
    let mut total_txns = start_txns.len();
    let mut tx_maps = vec![TxMap {
        ledger_seq: args.start,
        ledger_hash: start_ledger_hash,
        account_hash: start_info.account_hash,
        drops: start_info.total_coins,
        parent_close_time: start_info.prev_closing_time,
        close_time: start_info.closing_time,
        close_time_resolution: start_info.close_time_resolution,
        close_flags: start_info.close_flags,
        txns: start_txns,
    }];

    // Compute deltas
    let mut deltas = Vec::new();
    let mut total_added = 0usize;
    let mut total_deleted = 0usize;

    for seq in (args.start + 1)..=args.end {
        let prev_info = ledger_db.get(seq - 1)?;
        let curr_info = ledger_db.get(seq)?;
        let curr_ledger_hash = curr_info.verify_ledger_hash(seq)?;

        let diff = nudb
            .diff(&prev_info.account_hash, &curr_info.account_hash)
            .with_context(|| format!("diff failed at ledger {seq}"))?;

        let txns = nudb
            .collect_transactions(&curr_info.tx_hash)
            .with_context(|| format!("tx collect failed at ledger {seq}"))?;

        println!(
            "  ledger {seq}: +{} -{} nodes ({} bytes), {} txns",
            diff.added.len(),
            diff.deleted.len(),
            diff.added.iter().map(|n| n.content.len() + 33).sum::<usize>(),
            txns.len()
        );

        total_added += diff.added.len();
        total_deleted += diff.deleted.len();
        total_txns += txns.len();

        deltas.push(LedgerDelta { ledger_seq: seq, diff });
        tx_maps.push(TxMap {
            ledger_seq: seq,
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

    let ledger_count = args.end - args.start;
    println!(
        "Totals: +{total_added} -{total_deleted} nodes, {total_txns} txns across {ledger_count} ledgers \
         (avg +{}/ledger)",
        if ledger_count > 0 { total_added / ledger_count as usize } else { 0 }
    );

    let chunk = Chunk {
        network_id:      args.network_id,
        start_ledger:    args.start,
        end_ledger:      args.end,
        checkpoint_hash: start_info.ledger_hash,
        chunk_hash:      [0u8; 32], // computed by serialize_chunk
        checkpoint:      checkpoint_nodes,
        deltas,
        tx_maps,
    };

    let bytes = serialize_chunk(&chunk)?;
    let filename = chunk_filename(args.network_id, args.start, args.end);
    let out_path = args.out.join(&filename);
    fs::write(&out_path, &bytes)?;

    // chunk_hash is at byte offset 77 in the header (4+1+4+4+4+32+32 = 81 bytes header,
    // chunk_hash starts at offset 4+1+4+4+4+32 = 49)
    let chunk_hash_hex = hex::encode(&bytes[49..81]);

    println!(
        "\nWrote {} ({} bytes)\nchunk_hash: {}",
        out_path.display(),
        bytes.len(),
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
