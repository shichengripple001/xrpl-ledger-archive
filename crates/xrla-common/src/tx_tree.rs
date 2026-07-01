/// Rebuild a ledger's transaction SHAMap from its transaction records.
///
/// Needed for two things: (1) independently recomputing `tx_hash` so a ledger's full
/// `LedgerHash` can be verified without trusting anything beyond the raw transactions
/// (see `serialize::calculate_ledger_hash`), and (2) as a byproduct, producing every
/// inner/leaf node of that tree so it can be written into a NuDB store on import.
///
/// Verified against real mainnet data (51/51 ledgers, 4,500 txns): leaf hash =
/// `SHA512half(HashPrefix::txNode "SND\0" + VL(tx_blob) + VL(meta_blob) + tx_hash)`,
/// inner hash = `SHA512half(HashPrefix::innerNode "MIN\0" + 16×32-byte children)`.
///
/// Tree placement uses the transaction's own `tx_hash` as the SHAMap item key (nibble
/// path), which is a *different* value from the leaf node's own content hash used as
/// the child reference — mixing these up silently produces a self-consistent but wrong
/// tree (the same class of bug as the sparse-inner-node mask order mistake).
use crate::chunk::TxRecord;
use crate::serialize::sha512half;
use crate::shamap::{Hash256, NodeType, SHAMapNode, ZERO_HASH};

const HASH_PREFIX_TX_NODE: &[u8; 4] = b"SND\0";
const HASH_PREFIX_INNER_NODE: &[u8; 4] = b"MIN\0";
const HASH_PREFIX_TRANSACTION_ID: &[u8; 4] = b"TXN\0";

/// Independently recompute a transaction's own ID from its blob.
pub fn calculate_tx_id(tx_blob: &[u8]) -> Hash256 {
    let mut buf = Vec::with_capacity(4 + tx_blob.len());
    buf.extend_from_slice(HASH_PREFIX_TRANSACTION_ID);
    buf.extend_from_slice(tx_blob);
    sha512half(&buf)
}

/// Rippled variable-length encoding: 1-byte (<=192), 2-byte (193..=12480),
/// 3-byte (12481..) length prefixes. Inverse of the `read_vl` used when parsing leaves.
fn write_vl(buf: &mut Vec<u8>, data: &[u8]) {
    let len = data.len();
    if len <= 192 {
        buf.push(len as u8);
    } else if len <= 12_480 {
        let n = len - 193;
        buf.push((193 + n / 256) as u8);
        buf.push((n % 256) as u8);
    } else {
        let n = len - 12_481;
        buf.push((241 + n / 65_536) as u8);
        buf.push(((n / 256) % 256) as u8);
        buf.push((n % 256) as u8);
    }
    buf.extend_from_slice(data);
}

struct Item {
    key:       Hash256, // tx_hash — determines tree placement
    leaf_hash: Hash256, // this leaf node's own content hash — the child reference
    node:      SHAMapNode,
}

fn nibble(key: &Hash256, depth: usize) -> usize {
    let byte = key[depth / 2];
    if depth.is_multiple_of(2) {
        (byte >> 4) as usize
    } else {
        (byte & 0x0F) as usize
    }
}

/// Rebuild the transaction SHAMap from a ledger's transaction records.
/// Returns (root_hash, all_nodes). An empty ledger has root_hash == ZERO_HASH and no nodes
/// (matches rippled: an empty tx tree's TransSetHash is the zero hash, not a hashed empty inner).
pub fn build_tx_tree(txns: &[TxRecord]) -> (Hash256, Vec<SHAMapNode>) {
    if txns.is_empty() {
        return (ZERO_HASH, Vec::new());
    }

    let items: Vec<Item> = txns
        .iter()
        .map(|tx| {
            let mut content = Vec::with_capacity(4 + tx.tx_blob.len() + tx.meta_blob.len() + 32);
            content.extend_from_slice(HASH_PREFIX_TX_NODE);
            write_vl(&mut content, &tx.tx_blob);
            write_vl(&mut content, &tx.meta_blob);
            content.extend_from_slice(&tx.tx_hash);
            let leaf_hash = sha512half(&content);
            let node = SHAMapNode {
                hash: leaf_hash,
                node_type: NodeType::TransactionWithMeta,
                content,
            };
            Item { key: tx.tx_hash, leaf_hash, node }
        })
        .collect();

    let mut all_nodes: Vec<SHAMapNode> = items.iter().map(|i| i.node.clone()).collect();
    let refs: Vec<&Item> = items.iter().collect();
    let root_hash = build_level(&refs, 0, &mut all_nodes);
    (root_hash, all_nodes)
}

fn build_level(items: &[&Item], depth: usize, all_nodes: &mut Vec<SHAMapNode>) -> Hash256 {
    let mut buckets: [Vec<&Item>; 16] = std::array::from_fn(|_| Vec::new());
    for &item in items {
        buckets[nibble(&item.key, depth)].push(item);
    }

    let mut children = [ZERO_HASH; 16];
    for (slot, bucket) in buckets.iter().enumerate() {
        match bucket.len() {
            0 => {}
            1 => children[slot] = bucket[0].leaf_hash,
            _ => children[slot] = build_level(bucket, depth + 1, all_nodes),
        }
    }

    let mut content = Vec::with_capacity(512);
    for c in &children {
        content.extend_from_slice(c);
    }
    let mut buf = Vec::with_capacity(4 + 512);
    buf.extend_from_slice(HASH_PREFIX_INNER_NODE);
    buf.extend_from_slice(&content);
    let hash = sha512half(&buf);
    all_nodes.push(SHAMapNode { hash, node_type: NodeType::Inner, content });
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx(tx_hash: Hash256, blob: &[u8], meta: &[u8]) -> TxRecord {
        TxRecord { tx_hash, tx_blob: blob.to_vec(), meta_blob: meta.to_vec() }
    }

    #[test]
    fn empty_tree_is_zero_hash() {
        let (root, nodes) = build_tx_tree(&[]);
        assert_eq!(root, ZERO_HASH);
        assert!(nodes.is_empty());
    }

    #[test]
    fn single_tx_root_is_inner_with_one_child() {
        let key = [0x42u8; 32];
        let txns = vec![tx(key, b"blob", b"meta")];
        let (root, nodes) = build_tx_tree(&txns);

        // Exactly one leaf + one inner (the root).
        assert_eq!(nodes.len(), 2);
        let leaf = nodes.iter().find(|n| n.node_type == NodeType::TransactionWithMeta).unwrap();
        let inner = nodes.iter().find(|n| n.node_type == NodeType::Inner).unwrap();
        assert_eq!(inner.hash, root);

        // Root content is 512 bytes; the nibble-0 slot (key[0]=0x42 -> nibble 4) holds the
        // leaf hash, every other slot is zero.
        assert_eq!(inner.content.len(), 512);
        let slot = 4;
        assert_eq!(&inner.content[slot * 32..(slot + 1) * 32], &leaf.hash[..]);
        for s in 0..16 {
            if s != slot {
                assert_eq!(&inner.content[s * 32..(s + 1) * 32], &ZERO_HASH[..]);
            }
        }

        // Leaf hash matches the documented formula directly.
        let mut expect = Vec::new();
        expect.extend_from_slice(b"SND\0");
        expect.push(4); // VL(len=4) single-byte form
        expect.extend_from_slice(b"blob");
        expect.push(4);
        expect.extend_from_slice(b"meta");
        expect.extend_from_slice(&key);
        assert_eq!(leaf.hash, sha512half(&expect));
    }

    #[test]
    fn two_txns_sharing_first_nibble_split_at_second_level() {
        let mut a = [0u8; 32];
        a[0] = 0x10; // nibbles: 1, 0
        let mut b = [0u8; 32];
        b[0] = 0x11; // nibbles: 1, 1 — shares first nibble with `a`, diverges at depth 1
        let txns = vec![tx(a, b"a", b""), tx(b, b"b", b"")];
        let (root, nodes) = build_tx_tree(&txns);

        // Two leaves + two inner nodes (root at depth 0, one child inner at depth 1).
        assert_eq!(nodes.iter().filter(|n| n.node_type == NodeType::Inner).count(), 2);
        let root_node = nodes.iter().find(|n| n.hash == root).unwrap();
        // Root's slot 1 (nibble 0x1) should point at the depth-1 inner node, not a leaf.
        let slot1 = &root_node.content[32..64];
        assert!(nodes.iter().any(|n| n.node_type == NodeType::Inner && n.hash.as_slice() == slot1));
    }

    #[test]
    fn write_vl_matches_read_vl_boundaries() {
        // Mirrors reader.rs::read_vl exactly; round-trip a few boundary lengths.
        for len in [0usize, 1, 192, 193, 240, 12480, 12481, 70000] {
            let data = vec![0xABu8; len];
            let mut buf = Vec::new();
            write_vl(&mut buf, &data);
            let (parsed_len, n) = read_vl_for_test(&buf);
            assert_eq!(parsed_len, len, "length mismatch for len={len}");
            assert_eq!(&buf[n..], &data[..], "payload mismatch for len={len}");
        }
    }

    // Local copy of reader.rs::read_vl for the round-trip test (xrla-nudb depends on
    // xrla-common, not the other way around, so it can't be imported directly).
    fn read_vl_for_test(b: &[u8]) -> (usize, usize) {
        let b0 = b[0] as usize;
        if b0 <= 192 {
            (b0, 1)
        } else if b0 <= 240 {
            (193 + (b0 - 193) * 256 + b[1] as usize, 2)
        } else {
            (12481 + (b0 - 241) * 65536 + b[1] as usize * 256 + b[2] as usize, 3)
        }
    }
}
