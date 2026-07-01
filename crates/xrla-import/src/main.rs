/// xrla-import — import an XRLA chunk file into a rippled-compatible NuDB store.
///
/// Usage:
///   xrla-import --chunk ./chunks/xrla_1_01000000_01001000.xrla \
///               --dat /var/lib/rippled/db/nudb.dat
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

use anyhow::{bail, Result};
use clap::Parser;

use xrla_common::chunk::{Chunk, TxMap};
use xrla_common::serialize::{calculate_ledger_hash, deserialize_chunk, LedgerHashInput};
use xrla_common::shamap::{Hash256, InnerNode, NodeType, SHAMapNode};
use xrla_common::tx_tree::{build_tx_tree, calculate_tx_id};

#[derive(Parser, Debug)]
#[command(name = "xrla-import", about = "Import an XRLA chunk file into rippled NuDB")]
struct Args {
    /// Path to the .xrla chunk file
    #[arg(long)]
    chunk: PathBuf,

    /// Path to the NuDB .dat file to write (a sibling .key file is written alongside it)
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

    let replay = replay_chunk(&chunk, !args.skip_verify)?;
    println!(
        "Replayed {} live state nodes, {} tx-tree nodes across {} ledgers",
        replay.state.len(),
        replay.tx_nodes.len(),
        chunk.ledger_count()
    );

    let key_path = args.dat.with_extension("key");
    println!("Writing NuDB store: {} / {}", args.dat.display(), key_path.display());
    write_to_nudb(&replay, &args.dat, &key_path)?;

    println!("Import complete.");
    Ok(())
}

#[derive(Debug)]
struct ReplayResult {
    /// Final live account-state SHAMap nodes (checkpoint replayed through all deltas).
    state: HashMap<Hash256, SHAMapNode>,
    /// Every inner/leaf node of every ledger's rebuilt transaction tree.
    tx_nodes: Vec<SHAMapNode>,
}

/// Replay checkpoint + deltas, rebuilding each ledger's transaction tree along the way.
/// When `verify` is true, independently recomputes and asserts (bailing on the first
/// mismatch): per-transaction authenticity, the account-state root, and the full
/// LedgerHash chained to the previous ledger.
fn replay_chunk(chunk: &Chunk, verify: bool) -> Result<ReplayResult> {
    let mut state: HashMap<Hash256, SHAMapNode> = chunk
        .checkpoint
        .iter()
        .map(|n| (n.hash, n.clone()))
        .collect();
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
    }
    let (_, nodes) = build_tx_tree(&cp.txns);
    tx_nodes.extend(nodes);
    if verify {
        println!(
            "  ledger {} (checkpoint): account_hash OK, {} txns authentic \
             (LedgerHash needs an external parent_hash anchor — not verified here)",
            cp.ledger_seq,
            cp.txns.len()
        );
    }

    let mut current_root = cp.account_hash;
    let mut prev_ledger_hash = cp.ledger_hash;

    for (i, delta) in chunk.deltas.iter().enumerate() {
        for node in &delta.diff.added {
            state.insert(node.hash, node.clone());
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
                "  ledger {}: account_hash OK, {} txns authentic, LedgerHash OK (chained to parent)",
                tx_map.ledger_seq,
                tx_map.txns.len()
            );
        }

        current_root = new_root;
        prev_ledger_hash = tx_map.ledger_hash;
    }

    Ok(ReplayResult { state, tx_nodes })
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
fn write_to_nudb(replay: &ReplayResult, dat_path: &std::path::Path, key_path: &std::path::Path) -> Result<()> {
    let mut all: HashMap<Hash256, Vec<u8>> = HashMap::with_capacity(replay.state.len() + replay.tx_nodes.len());
    for node in replay.state.values() {
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
        replay.state.len(),
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

    fn leaf(tag: u8) -> SHAMapNode {
        SHAMapNode { hash: [tag; 32], node_type: NodeType::AccountState, content: vec![tag; 16] }
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
}
