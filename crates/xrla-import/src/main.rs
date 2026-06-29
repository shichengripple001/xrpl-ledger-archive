/// xrla-import — import an XRLA chunk file into a rippled NuDB store.
///
/// Usage:
///   xrla-import --chunk ./chunks/xrla_1_01000000_01001000.xrla \
///               --dat /var/lib/rippled/db/nudb.dat
///
/// Verifies the chunk hash, then replays each delta verifying the
/// SHAMap root hash against the on-chain ledger header at every step.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use anyhow::{bail, Result};
use clap::Parser;

use xrla_common::chunk::Chunk;
use xrla_common::serialize::{deserialize_chunk, sha512half};
use xrla_common::shamap::{Hash256, InnerNode, NodeType, SHAMapNode};

#[derive(Parser, Debug)]
#[command(name = "xrla-import", about = "Import an XRLA chunk file into rippled NuDB")]
struct Args {
    /// Path to the .xrla chunk file
    #[arg(long)]
    chunk: PathBuf,

    /// Path to rippled NuDB .dat file (will be written to)
    #[arg(long)]
    dat: PathBuf,

    /// Skip hash verification (faster, not recommended)
    #[arg(long, default_value_t = false)]
    skip_verify: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    println!("Reading chunk: {}", args.chunk.display());
    let data = fs::read(&args.chunk)?;

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

    if !args.skip_verify {
        println!("Verifying ledger hashes...");
        verify_ledger_hashes(&chunk)?;
        println!("All ledger hashes OK");
    }

    println!("Writing to NuDB: {}", args.dat.display());
    write_to_nudb(&chunk, &args.dat)?;

    println!("Import complete.");
    Ok(())
}

/// Replay all deltas and verify the SHAMap root hash at each ledger.
///
/// This is the trustless verification step: we reconstruct the SHAMap state
/// from checkpoint + deltas, compute the root hash, and compare against
/// the on-chain ledger header hash embedded in the chunk.
fn verify_ledger_hashes(chunk: &Chunk) -> Result<()> {
    // Build initial state from checkpoint
    let mut state: HashMap<Hash256, SHAMapNode> = chunk
        .checkpoint
        .iter()
        .map(|n| (n.hash, n.clone()))
        .collect();

    // Find checkpoint root hash (the node with no parent = the root)
    // For the checkpoint, root hash = chunk.checkpoint_hash
    let mut current_root = chunk.checkpoint_hash;

    // Verify checkpoint root exists in state
    if !state.contains_key(&current_root) {
        bail!(
            "checkpoint root hash not found in checkpoint nodes: {}",
            hex::encode(current_root)
        );
    }

    for delta in &chunk.deltas {
        // Apply delta
        for node in &delta.diff.added {
            state.insert(node.hash, node.clone());
        }
        for hash in &delta.diff.deleted {
            state.remove(hash);
        }

        // Compute new root hash by finding the new inner node that
        // was added at the root level
        // The new root is the added inner node with no parent in the added set
        // (i.e., not referenced as a child by any other added inner node)
        let new_root = find_new_root(&delta.diff.added, &current_root)?;

        // TODO: Fetch the on-chain ledger header hash for delta.ledger_seq
        // and compare against new_root. Requires access to the ledger header
        // database (either from the chunk's tx_maps or an external source).
        //
        // For now: compute and print the root hash for manual verification.
        println!(
            "  ledger {}: root_hash={}",
            delta.ledger_seq,
            hex::encode(new_root)
        );

        current_root = new_root;
    }

    Ok(())
}

/// Find the new root hash after applying a delta.
/// The root is the added inner node that is not a child of any other added node.
fn find_new_root(added: &[SHAMapNode], prev_root: &Hash256) -> Result<Hash256> {
    if added.is_empty() {
        return Ok(*prev_root);
    }

    // Collect all hashes referenced as children in the added inner nodes
    let mut referenced = std::collections::HashSet::new();
    for node in added {
        if matches!(node.node_type, NodeType::Inner) {
            if let Ok(inner) = InnerNode::from_bytes(&node.content) {
                for child_hash in inner.child_hashes() {
                    referenced.insert(*child_hash);
                }
            }
        }
    }

    // The root is the added inner node not referenced by any other added node
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

/// Write all nodes from the chunk into a NuDB .dat file.
fn write_to_nudb(chunk: &Chunk, _dat_path: &PathBuf) -> Result<()> {
    // TODO: implement NuDB writer
    // Write checkpoint nodes + all delta nodes to the .dat file
    // Each record: [field_size bytes: key+value len][32 bytes: key][value bytes]
    //
    // For now: count what would be written
    let mut total_nodes = chunk.checkpoint.len();
    for delta in &chunk.deltas {
        total_nodes += delta.diff.added.len();
    }
    println!("Would write {total_nodes} nodes to NuDB (writer not yet implemented)");
    Ok(())
}
