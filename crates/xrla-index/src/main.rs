/// xrla-index — build a per-chunk account index, then answer `account_tx` / `tx` from it.
///
/// Usage:
///   xrla-index build --chunk xrla_1_0107368107_0107373106.xrla
///   xrla-index query --index xrla_1_0107368107_0107373106.xidx --account rHb9CJAWy...
///   xrla-index query --index ... --account r... --from 107370000 --limit 20 --forward
///   xrla-index query --index ... --account r... --marker 107371500:12     (next page)
///   xrla-index tx    --index ... --hash 46C1CECE...
///   xrla-index stats --index ...
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};

use xrla_common::meta_decode::classic_address_to_account_id;
use xrla_index::build::build_index;
use xrla_index::query::{Index, Marker};

#[derive(Parser, Debug)]
#[command(name = "xrla-index", about = "Per-chunk account index: build it, then answer account_tx / tx from it")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Stream a chunk once and write its index. Nothing is written unless the chunk's hash
    /// verifies.
    Build {
        /// The .xrla chunk (format v3)
        #[arg(long)]
        chunk: PathBuf,
        /// Where to write the index (default: next to the chunk, extension .xidx)
        #[arg(long)]
        out: Option<PathBuf>,
        /// Replace an existing index
        #[arg(long)]
        force: bool,
    },
    /// account_tx: the transactions that affected an account, newest first
    Query {
        #[arg(long)]
        index: PathBuf,
        /// Classic r-address
        #[arg(long)]
        account: String,
        /// First ledger to include (default: the index's first)
        #[arg(long)]
        from: Option<u32>,
        /// Last ledger to include (default: the index's last)
        #[arg(long)]
        to: Option<u32>,
        #[arg(long, default_value_t = 200)]
        limit: usize,
        /// Oldest first instead of newest first
        #[arg(long)]
        forward: bool,
        /// Continue after this `ledger:txn_seq` (printed at the end of a full page)
        #[arg(long)]
        marker: Option<String>,
    },
    /// Find a transaction by hash
    Tx {
        #[arg(long)]
        index: PathBuf,
        #[arg(long)]
        hash: String,
    },
    /// Show what an index covers
    Stats {
        #[arg(long)]
        index: PathBuf,
    },
}

fn parse_marker(s: &str) -> Result<Marker> {
    let (l, t) = s
        .split_once(':')
        .with_context(|| format!("marker {s:?} must look like LEDGER:TXN_SEQ"))?;
    Ok(Marker { ledger_seq: l.parse()?, txn_seq: t.parse()? })
}

fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Build { chunk, out, force } => {
            let out = out.unwrap_or_else(|| chunk.with_extension("xidx"));
            eprintln!("indexing {} -> {}", chunk.display(), out.display());
            let r = build_index(&chunk, &out, force, |done, total| {
                eprintln!("  {done}/{total} ledgers");
            })?;
            println!("chunk:        network {} ledgers {}..={}", r.network_id, r.start_ledger, r.end_ledger);
            println!("chunk_hash:   {} (verified)", hex::encode_upper(r.chunk_hash));
            println!("transactions: {}", r.transactions);
            println!("account rows: {}", r.rows);
            println!("wrote {}", out.display());
        }
        Cmd::Query { index, account, from, to, limit, forward, marker } => {
            let idx = Index::open(&index)?;
            let id = classic_address_to_account_id(&account)?;
            let (min, max) = (from.unwrap_or(idx.meta.start_ledger), to.unwrap_or(idx.meta.end_ledger));
            let marker = marker.as_deref().map(parse_marker).transpose()?;
            let rows = idx.account_tx(&id, min, max, limit, forward, marker)?;

            println!("{:>12}  {:>7}  {:<64}", "ledger", "txn_seq", "tx_hash");
            for r in &rows {
                println!("{:>12}  {:>7}  {}", r.ledger_seq, r.txn_seq, hex::encode_upper(r.tx_hash));
            }
            println!();
            println!("{} transaction(s) affected {account} in ledgers {min}..={max}", rows.len());
            if rows.len() == limit {
                let last = rows.last().unwrap();
                println!("more may follow: --marker {}:{}", last.ledger_seq, last.txn_seq);
            }
        }
        Cmd::Tx { index, hash } => {
            let idx = Index::open(&index)?;
            let bytes = hex::decode(hash.trim()).with_context(|| format!("invalid hex: {hash}"))?;
            let Ok(h) = <[u8; 32]>::try_from(bytes.as_slice()) else {
                bail!("a transaction hash is 32 bytes (64 hex characters), got {}", bytes.len());
            };
            match idx.locate_tx(&h)? {
                Some((ledger, txn_seq)) => println!("ledger {ledger}, txn_seq {txn_seq}"),
                None => {
                    println!("not in this index (ledgers {}..={})", idx.meta.start_ledger, idx.meta.end_ledger);
                    std::process::exit(1);
                }
            }
        }
        Cmd::Stats { index } => {
            let m = Index::open(&index)?.meta;
            println!("network:      {}", m.network_id);
            println!("ledgers:      {}..={} ({})", m.start_ledger, m.end_ledger, m.ledgers);
            println!("transactions: {}", m.transactions);
            println!("account rows: {}", m.rows);
            println!("chunk_hash:   {}", m.chunk_hash);
            println!("account rule: {}", m.rule_id);
        }
    }
    Ok(())
}
