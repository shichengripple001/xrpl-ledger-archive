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
use xrla_common::serialize::serialize_chunk;
use xrla_common::shamap::{Hash256, NodeType, SHAMapNode};
use xrla_nudb::NuDBReader;

#[derive(Parser, Debug)]
#[command(name = "xrla-export", about = "Export XRPL ledger history to chunk files")]
struct Args {
    /// Path to rippled NuDB .dat file
    #[arg(long)]
    dat: PathBuf,

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

    println!("Opening NuDB: {}", args.dat.display());
    let nudb = NuDBReader::open(&args.dat)?;

    println!("Opening ledger index: {}", args.ledgers.display());
    let ledger_db = LedgerIndex::open(&args.ledgers)?;

    println!("Exporting ledgers {}..{}", args.start, args.end);

    // Checkpoint: full state at start_ledger
    let start_info = ledger_db.get(args.start)?;
    println!(
        "Building checkpoint at ledger {} (state_hash={})...",
        args.start,
        hex::encode(start_info.account_hash)
    );
    let checkpoint_nodes = nudb.collect_reachable(&start_info.account_hash)?;
    println!("Checkpoint: {} nodes", checkpoint_nodes.len());

    // TX map for start ledger (no delta, just transactions)
    let mut tx_maps = vec![TxMap { ledger_seq: args.start, txns: vec![] }];

    // Compute deltas
    let mut deltas = Vec::new();
    let mut total_added = 0usize;
    let mut total_deleted = 0usize;

    for seq in (args.start + 1)..=args.end {
        let prev_info = ledger_db.get(seq - 1)?;
        let curr_info = ledger_db.get(seq)?;

        let diff = nudb
            .diff(&prev_info.account_hash, &curr_info.account_hash)
            .with_context(|| format!("diff failed at ledger {seq}"))?;

        println!(
            "  ledger {seq}: +{} -{} nodes ({} bytes)",
            diff.added.len(),
            diff.deleted.len(),
            diff.added.iter().map(|n| n.content.len() + 33).sum::<usize>()
        );

        total_added += diff.added.len();
        total_deleted += diff.deleted.len();

        deltas.push(LedgerDelta { ledger_seq: seq, diff });
        tx_maps.push(TxMap { ledger_seq: seq, txns: vec![] });
    }

    let ledger_count = args.end - args.start;
    println!(
        "Totals: +{total_added} -{total_deleted} nodes across {ledger_count} ledgers \
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
//   LedgerHash     TEXT  — hex-encoded ledger hash
//   LedgerSeq      INT
//   AccountSetHash TEXT  — hex-encoded state SHAMap root hash
//   TransSetHash   TEXT  — hex-encoded tx SHAMap root hash
//
// Source: src/xrpld/app/rdb/backend/detail/Node.cpp
// ---------------------------------------------------------------------------

struct LedgerInfo {
    ledger_hash:  Hash256,
    account_hash: Hash256, // state SHAMap root
    #[allow(dead_code)]
    tx_hash:      Hash256,
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
        let (ledger_hash_hex, account_hash_hex, tx_hash_hex): (String, String, String) = self
            .conn
            .query_row(
                "SELECT LedgerHash, AccountSetHash, TransSetHash \
                 FROM Ledgers WHERE LedgerSeq = ?1",
                params![seq],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .with_context(|| format!("ledger {seq} not found in database"))?;

        Ok(LedgerInfo {
            ledger_hash:  parse_hash(&ledger_hash_hex)
                .with_context(|| format!("invalid LedgerHash for seq {seq}"))?,
            account_hash: parse_hash(&account_hash_hex)
                .with_context(|| format!("invalid AccountSetHash for seq {seq}"))?,
            tx_hash:      parse_hash(&tx_hash_hex)
                .with_context(|| format!("invalid TransSetHash for seq {seq}"))?,
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
