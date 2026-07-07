/// Independent verification of account-state SHAMap node content against their own claimed
/// hash.
///
/// Unlike the transaction tree (see `tx_tree`), the account-state tree is never rebuilt from
/// scratch here — checkpoint + delta nodes already carry the actual tree structure as read
/// from NuDB (which inner-node slot holds which child; see `xrla_nudb::reader`). What was
/// never checked before this: does each node's content actually hash to the value it's
/// keyed/labeled as, or is that trusted as given from NuDB's own key? Every prior check
/// (root == on-chain `AccountSetHash`, delta root-finding) only ever compared pre-existing
/// labels against each other, never re-derived a hash from raw bytes.
///
/// Both formulas here are real-data-validated, exhaustively, not sampled:
/// - **Inner nodes**: `SHA512half(HashPrefix::innerNode "MIN\0" + content)` — the on-disk
///   NuDB payload has the prefix stripped (see `xrla_nudb::dat`), so it's re-added before
///   hashing. Confirmed against all 7,912,690 inner nodes in a real mainnet checkpoint.
/// - **Leaf nodes** (`AccountState`, and by the same evidence `Transaction`/
///   `TransactionWithMeta`): `SHA512half(content)` directly, no prefix. rippled's on-disk
///   payload already embeds whatever it needs (confirmed empirically — trying a
///   `HashPrefix::leafNode "MLN\0"`-prepended variant against real data produced zero
///   matches, while hashing content as-is matched every single time), the same pattern
///   already established for transaction leaves in `tx_tree`.
///
/// Validated by walking the *entire* checkpoint reachable from a real mainnet ledger's
/// `AccountSetHash` (not a sample): 7,912,690 inner + 19,118,965 leaf nodes, 27,031,655
/// total, zero mismatches on either formula. See PLAN.md "Immediate TODOs" item 7.
use crate::serialize::sha512half;
use crate::shamap::{Hash256, NodeType, SHAMapNode};

const HASH_PREFIX_INNER_NODE: &[u8; 4] = b"MIN\0";

/// Recompute a node's own hash from its raw content.
pub fn recompute_node_hash(node: &SHAMapNode) -> Hash256 {
    match node.node_type {
        NodeType::Inner | NodeType::CompressedInner => {
            let mut buf = Vec::with_capacity(4 + node.content.len());
            buf.extend_from_slice(HASH_PREFIX_INNER_NODE);
            buf.extend_from_slice(&node.content);
            sha512half(&buf)
        }
        _ => sha512half(&node.content),
    }
}

/// Verify every node in `nodes` actually hashes to its own claimed identity. Returns the
/// first node whose claimed hash doesn't match its recomputed hash, if any.
pub fn verify_state_nodes<'a>(
    nodes: impl IntoIterator<Item = &'a SHAMapNode>,
) -> Result<(), Hash256> {
    for node in nodes {
        if recompute_node_hash(node) != node.hash {
            return Err(node.hash);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inner_with_child(slot: usize, child: Hash256) -> SHAMapNode {
        let mut content = vec![0u8; 512];
        content[slot * 32..(slot + 1) * 32].copy_from_slice(&child);
        let node = SHAMapNode { hash: [0; 32], node_type: NodeType::Inner, content };
        SHAMapNode { hash: recompute_node_hash(&node), ..node }
    }

    fn leaf(content: &[u8]) -> SHAMapNode {
        let node = SHAMapNode { hash: [0; 32], node_type: NodeType::AccountState, content: content.to_vec() };
        SHAMapNode { hash: recompute_node_hash(&node), ..node }
    }

    #[test]
    fn genuine_inner_node_verifies() {
        let node = inner_with_child(5, [0x11; 32]);
        assert!(verify_state_nodes([&node]).is_ok());
    }

    #[test]
    fn genuine_leaf_node_verifies() {
        let node = leaf(b"pretend serialized ledger object bytes");
        assert!(verify_state_nodes([&node]).is_ok());
    }

    #[test]
    fn tampered_inner_content_is_caught() {
        let mut node = inner_with_child(5, [0x11; 32]);
        node.content[0] ^= 0xFF; // corrupt content without updating the claimed hash
        let err = verify_state_nodes([&node]).unwrap_err();
        assert_eq!(err, node.hash);
    }

    #[test]
    fn tampered_leaf_content_is_caught() {
        let mut node = leaf(b"pretend serialized ledger object bytes");
        node.content[0] ^= 0xFF;
        let err = verify_state_nodes([&node]).unwrap_err();
        assert_eq!(err, node.hash);
    }
}
