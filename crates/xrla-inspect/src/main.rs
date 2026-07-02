/// xrla-inspect — show the contents of an .xrla chunk file without importing it.
///
/// Usage:
///   xrla-inspect --chunk ./chunks/xrla_1_0105277428_0105277478.xrla
///   xrla-inspect --chunk ... --ledger 105277430
///   xrla-inspect --chunk ... --ledger 105277430 --tx 0

use std::fs;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::Parser;

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
}

fn main() -> Result<()> {
    let args = Args::parse();

    let data = fs::read(&args.chunk)
        .with_context(|| format!("reading {}", args.chunk.display()))?;
    println!("File: {} ({} bytes)", args.chunk.display(), data.len());

    let chunk = deserialize_chunk(&data).context("parsing chunk")?;

    match (args.ledger, args.tx) {
        (None, None) => print_summary(&chunk),
        (Some(seq), None) => print_ledger(&chunk, seq)?,
        (Some(seq), Some(tx_idx)) => print_tx(&chunk, seq, tx_idx)?,
        (None, Some(_)) => bail!("--tx requires --ledger"),
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

    println!("tx_hash:   {}", hex::encode_upper(tx.tx_hash));
    println!("tx_blob ({} bytes, rippled binary serialization):", tx.tx_blob.len());
    println!("{}", hex::encode(&tx.tx_blob));
    println!();
    println!("meta_blob ({} bytes, rippled binary serialization):", tx.meta_blob.len());
    println!("{}", hex::encode(&tx.meta_blob));
    Ok(())
}
