/// xrla-inspect — show the contents of an .xrla chunk file without importing it.
///
/// Usage:
///   xrla-inspect --chunk ./chunks/xrla_1_0105277428_0105277478.xrla
///   xrla-inspect --chunk ... --ledger 105277430
///   xrla-inspect --chunk ... --ledger 105277430 --tx 0
///   xrla-inspect --chunk ... --tx-hash 010D3CA6...   (no --ledger needed — searches the whole chunk)
///   xrla-inspect --chunk ... --account rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh   (account_tx-style scan)

use std::fs;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::Parser;

use xrla_common::chunk::TxRecord;
use xrla_common::meta_decode::{account_id_to_classic_address, affected_accounts};
use xrla_common::serialize::deserialize_chunk;

#[derive(Parser, Debug)]
#[command(name = "xrla-inspect", about = "Show the contents of an XRLA chunk file")]
struct Args {
    /// Path to the .xrla chunk file
    #[arg(long)]
    chunk: PathBuf,

    /// Show detail for one ledger sequence instead of the whole-chunk summary
    #[arg(long)]
    ledger: Option<u32>,

    /// With --ledger, show the raw blob/meta hex for one transaction by index
    #[arg(long)]
    tx: Option<usize>,

    /// Look up one transaction anywhere in the chunk by its hash (hex, case-insensitive) —
    /// searches every ledger in the chunk, no --ledger needed
    #[arg(long)]
    tx_hash: Option<String>,

    /// account_tx-style scan: list every transaction in this chunk that touched the given
    /// account (classic r-address), each tagged with its ledger — derived on demand from the
    /// existing meta_blob data, no separate index required
    #[arg(long)]
    account: Option<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let data = fs::read(&args.chunk)
        .with_context(|| format!("reading {}", args.chunk.display()))?;
    println!("File: {} ({} bytes)", args.chunk.display(), data.len());

    let chunk = deserialize_chunk(&data).context("parsing chunk")?;

    if let Some(account) = &args.account {
        print_account_tx(&chunk, account)?;
    } else if let Some(hash_hex) = &args.tx_hash {
        print_tx_by_hash(&chunk, hash_hex)?;
    } else {
        match (args.ledger, args.tx) {
            (None, None) => print_summary(&chunk),
            (Some(seq), None) => print_ledger(&chunk, seq)?,
            (Some(seq), Some(tx_idx)) => print_tx(&chunk, seq, tx_idx)?,
            (None, Some(_)) => bail!("--tx requires --ledger (or use --tx-hash to look up by hash directly)"),
        }
    }

    Ok(())
}

fn print_summary(chunk: &xrla_common::chunk::Chunk) {
    println!("network_id:      {}", chunk.network_id);
    println!("ledger range:    {}..={}", chunk.start_ledger, chunk.end_ledger);
    println!("checkpoint_hash: {}", hex::encode_upper(chunk.checkpoint_hash));
    println!("chunk_hash:      {}", hex::encode_upper(chunk.chunk_hash));
    println!("checkpoint:      {} state nodes", chunk.checkpoint.len());
    println!("deltas:          {} ledgers", chunk.deltas.len());
    println!();
    println!("{:>12}  {:<64}  {:>8}  {:>8}  {:>6}  {:>10}", "ledger", "ledger_hash", "+nodes", "-nodes", "txns", "drops");
    for tx_map in &chunk.tx_maps {
        let delta = chunk.deltas.iter().find(|d| d.ledger_seq == tx_map.ledger_seq);
        let (added, deleted) = delta
            .map(|d| (d.diff.added.len(), d.diff.deleted.len()))
            .unwrap_or((0, 0)); // the checkpoint ledger itself has no delta entry
        println!(
            "{:>12}  {:<64}  {:>8}  {:>8}  {:>6}  {:>10}",
            tx_map.ledger_seq,
            hex::encode_upper(tx_map.ledger_hash),
            added,
            deleted,
            tx_map.txns.len(),
            tx_map.drops,
        );
    }
}

fn print_ledger(chunk: &xrla_common::chunk::Chunk, seq: u32) -> Result<()> {
    let tx_map = chunk
        .tx_maps
        .iter()
        .find(|t| t.ledger_seq == seq)
        .with_context(|| format!("ledger {seq} not in this chunk"))?;

    println!("ledger_seq:            {}", tx_map.ledger_seq);
    println!("ledger_hash:           {}", hex::encode_upper(tx_map.ledger_hash));
    println!("account_hash:          {}", hex::encode_upper(tx_map.account_hash));
    println!("drops:                 {}", tx_map.drops);
    println!("parent_close_time:     {}", tx_map.parent_close_time);
    println!("close_time:            {}", tx_map.close_time);
    println!("close_time_resolution: {}", tx_map.close_time_resolution);
    println!("close_flags:           {}", tx_map.close_flags);
    println!("txns:                  {}", tx_map.txns.len());

    if let Some(delta) = chunk.deltas.iter().find(|d| d.ledger_seq == seq) {
        println!("delta: +{} -{} state nodes", delta.diff.added.len(), delta.diff.deleted.len());
    } else if seq == chunk.start_ledger {
        println!("(checkpoint ledger — full state, no delta entry)");
    }

    println!();
    println!("{:>6}  {:<64}  {:>10}  {:>10}", "idx", "tx_hash", "blob_bytes", "meta_bytes");
    for (i, tx) in tx_map.txns.iter().enumerate() {
        println!(
            "{:>6}  {:<64}  {:>10}  {:>10}",
            i,
            hex::encode_upper(tx.tx_hash),
            tx.tx_blob.len(),
            tx.meta_blob.len(),
        );
    }
    Ok(())
}

fn print_tx(chunk: &xrla_common::chunk::Chunk, seq: u32, tx_idx: usize) -> Result<()> {
    let tx_map = chunk
        .tx_maps
        .iter()
        .find(|t| t.ledger_seq == seq)
        .with_context(|| format!("ledger {seq} not in this chunk"))?;
    let tx = tx_map
        .txns
        .get(tx_idx)
        .with_context(|| format!("ledger {seq} has no transaction at index {tx_idx}"))?;

    print_tx_detail(tx);
    Ok(())
}

/// Search every ledger in the chunk for a transaction matching `hash_hex`, independent of
/// which ledger it happens to be in — useful when you have a tx hash but not its ledger.
fn print_tx_by_hash(chunk: &xrla_common::chunk::Chunk, hash_hex: &str) -> Result<()> {
    let target = parse_hash(hash_hex)?;
    for tx_map in &chunk.tx_maps {
        if let Some(tx) = tx_map.txns.iter().find(|t| t.tx_hash == target) {
            println!("found in ledger: {}", tx_map.ledger_seq);
            print_tx_detail(tx);
            return Ok(());
        }
    }
    bail!(
        "transaction {} not found in this chunk (ledgers {}..={})",
        hash_hex.to_uppercase(),
        chunk.start_ledger,
        chunk.end_ledger
    )
}

/// account_tx-style scan over this chunk only: decode every transaction's meta_blob and
/// report the ones that touched `account_r_address`. Full-archive coverage would mean running
/// this same scan over every chunk in the range you care about — no separate index needed.
fn print_account_tx(chunk: &xrla_common::chunk::Chunk, account_r_address: &str) -> Result<()> {
    let mut found = 0usize;
    let mut undecodable = 0usize;
    println!("{:>12}  {:<64}", "ledger", "tx_hash");
    for tx_map in &chunk.tx_maps {
        for tx in &tx_map.txns {
            let touched = match affected_accounts(&tx.meta_blob) {
                Ok(v) => v,
                Err(e) => {
                    // A decode failure must never look like "no matches": count it, and fail
                    // the command at the end so the result can't be mistaken for complete.
                    eprintln!("error: tx {} could not be decoded: {e}", hex::encode_upper(tx.tx_hash));
                    undecodable += 1;
                    continue;
                }
            };
            let matches = touched
                .iter()
                .any(|a| account_id_to_classic_address(a) == account_r_address);
            if matches {
                found += 1;
                println!("{:>12}  {:<64}", tx_map.ledger_seq, hex::encode_upper(tx.tx_hash));
            }
        }
    }
    println!();
    println!("{found} transaction(s) touched {account_r_address} in ledgers {}..={}", chunk.start_ledger, chunk.end_ledger);
    if undecodable > 0 {
        bail!("{undecodable} transaction(s) could not be decoded — the list above is INCOMPLETE");
    }
    Ok(())
}

fn print_tx_detail(tx: &TxRecord) {
    println!("tx_hash:   {}", hex::encode_upper(tx.tx_hash));
    println!("tx_blob ({} bytes, xrpld binary serialization):", tx.tx_blob.len());
    println!("{}", hex::encode(&tx.tx_blob));
    println!();
    println!("meta_blob ({} bytes, xrpld binary serialization):", tx.meta_blob.len());
    println!("{}", hex::encode(&tx.meta_blob));
}

fn parse_hash(s: &str) -> Result<[u8; 32]> {
    let bytes = hex::decode(s.trim()).with_context(|| format!("invalid hex string: {s}"))?;
    if bytes.len() != 32 {
        bail!("expected 32-byte hash, got {} bytes from '{}'", bytes.len(), s);
    }
    let mut h = [0u8; 32];
    h.copy_from_slice(&bytes);
    Ok(h)
}
