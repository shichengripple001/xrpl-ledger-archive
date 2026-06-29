/// xrla-export — export a range of ledgers from a rippled NuDB store
/// into an XRLA chunk file.
///
/// Usage:
///   xrla-export --dat /var/lib/rippled/db/nudb.dat \
///               --ledgers /var/lib/rippled/db/ledger.db \
///               --start 1000000 --end 1001000 \
///               --out ./chunks/
///
/// The --ledgers file is rippled's SQLite ledger index, used to look up
/// the state hash and transaction list for each ledger sequence number.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use clap::Parser;

use xrla_common::chunk::{chunk_filename, Chunk, LedgerDelta, TxMap, TxRecord, NETWORK_MAINNET};
use xrla_common::serialize::serialize_chunk;
use xrla_common::shamap::SHAMapNode;
use xrla_nudb::NuDBReader;

#[derive(Parser, Debug)]
#[command(name = "xrla-export", about = "Export XRPL ledger history to chunk files")]
struct Args {
    /// Path to rippled NuDB .dat file
    #[arg(long)]
    dat: PathBuf,

    /// Path to rippled ledger SQLite database (for state hashes + tx lists)
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

    // Fetch checkpoint (full state at start_ledger)
    let start_info = ledger_db.get(args.start)?;
    println!("Building checkpoint at ledger {}...", args.start);
    let checkpoint_nodes = nudb.collect_reachable(&start_info.state_hash)?;
    let checkpoint: Vec<SHAMapNode> = checkpoint_nodes
        .into_iter()
        .map(|(hash, content)| {
            let node_type = if content.first().copied().unwrap_or(1) == 0 {
                xrla_common::shamap::NodeType::Inner
            } else {
                xrla_common::shamap::NodeType::Leaf
            };
            SHAMapNode { hash, node_type, content }
        })
        .collect();
    println!("Checkpoint: {} nodes", checkpoint.len());

    // Fetch tx map for start ledger
    let mut tx_maps = vec![ledger_db.get_tx_map(args.start)?];

    // Compute deltas
    let mut deltas = Vec::new();
    let mut total_delta_nodes = 0usize;

    for seq in (args.start + 1)..=args.end {
        let prev_info = ledger_db.get(seq - 1)?;
        let curr_info = ledger_db.get(seq)?;

        print!("  ledger {seq}... ");
        let diff = nudb.diff(&prev_info.state_hash, &curr_info.state_hash)?;
        let added = diff.added.len();
        let deleted = diff.deleted.len();
        total_delta_nodes += added + deleted;
        println!("+{added} -{deleted} nodes");

        deltas.push(LedgerDelta { ledger_seq: seq, diff });
        tx_maps.push(ledger_db.get_tx_map(seq)?);
    }

    println!(
        "Total delta nodes: {} across {} ledgers",
        total_delta_nodes,
        args.end - args.start
    );

    let chunk = Chunk {
        network_id:      args.network_id,
        start_ledger:    args.start,
        end_ledger:      args.end,
        checkpoint_hash: start_info.ledger_hash,
        chunk_hash:      [0u8; 32], // computed by serialize_chunk
        checkpoint,
        deltas,
        tx_maps,
    };

    let bytes = serialize_chunk(&chunk)?;
    let filename = chunk_filename(args.network_id, args.start, args.end);
    let out_path = args.out.join(&filename);
    fs::write(&out_path, &bytes)?;

    println!(
        "Wrote {} ({} bytes, chunk_hash={})",
        out_path.display(),
        bytes.len(),
        hex::encode(&bytes[77..109]) // chunk_hash offset in header
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// LedgerIndex: reads rippled's ledger SQLite database
//
// rippled stores a SQLite database (Ledgers table) with:
//   LedgerSeq      INTEGER
//   LedgerHash     BLOB
//   PrevHash       BLOB
//   AccountSetHash BLOB   <- this is the state hash
//   TransSetHash   BLOB   <- this is the tx map hash
//   ...
//
// TODO: Verify exact table/column names against rippled source.
// ---------------------------------------------------------------------------

struct LedgerInfo {
    ledger_hash: [u8; 32],
    state_hash:  [u8; 32],
    tx_hash:     [u8; 32],
}

struct LedgerIndex {
    // TODO: use rusqlite crate for real implementation
    // Placeholder for now — real implementation reads rippled's Ledgers table
    _path: PathBuf,
}

impl LedgerIndex {
    fn open(path: &Path) -> Result<Self> {
        if !path.exists() {
            bail!("ledger database not found: {}", path.display());
        }
        // TODO: open SQLite connection
        Ok(Self { _path: path.to_path_buf() })
    }

    fn get(&self, _seq: u32) -> Result<LedgerInfo> {
        // TODO: SELECT LedgerHash, AccountSetHash, TransSetHash FROM Ledgers WHERE LedgerSeq = ?
        bail!("LedgerIndex::get not yet implemented — add rusqlite dependency")
    }

    fn get_tx_map(&self, seq: u32) -> Result<TxMap> {
        // TODO: fetch transactions for this ledger from rippled's tx database
        Ok(TxMap {
            ledger_seq: seq,
            txns: vec![],
        })
    }
}
