/// Independent verification of account-state SHAMap inner nodes.
///
/// Unlike the transaction tree (see `tx_tree`), the account-state tree is never rebuilt
/// from scratch here — checkpoint + delta nodes already carry the actual tree structure as
/// read from NuDB (which inner-node slot holds which child; see `xrla_nudb::reader`). What
/// was never checked before this: does each node's content actually hash to the value it's
/// keyed/labeled as, or is that trusted as given from NuDB's own key? Every prior check
/// (root == on-chain `AccountSetHash`, delta root-finding) only ever compared pre-existing
/// labels against each other, never re-derived a hash from raw bytes.
///
/// This covers inner nodes only. The formula here (`HashPrefix::innerNode` + full 512-byte
/// children) was already independently validated in Phase 0 against real mainnet data
/// (7.9M checkpoint inner nodes re-hashed, root matched on-chain `AccountSetHash`) — see
/// PLAN.md Phase 0.
///
/// Leaf nodes (`AccountState`) are deliberately NOT covered yet. Getting this right needs
/// the same real-data validation treatment `tx_tree`'s leaf formula got (confirmed against
/// 4,500 real transactions before being trusted) — a real NuDB snapshot to test candidate
/// formulas against was not available when this was written. Guessing the formula without
/// that check would risk shipping a "verification" that's silently wrong in either
/// direction (false passes or false failures). See PLAN.md "Immediate TODOs".
use crate::serialize::sha512half;
use crate::shamap::{Hash256, NodeType, SHAMapNode};

const HASH_PREFIX_INNER_NODE: &[u8; 4] = b"MIN\0";

/// Recompute an inner node's own hash from its raw content. The on-disk NuDB payload has
/// `HashPrefix::innerNode` stripped (see `xrla_nudb::dat`), so it's re-added here before
/// hashing — mirrors `tx_tree::build_level`'s inner-node formula exactly.
pub fn recompute_inner_hash(content: &[u8]) -> Hash256 {
    let mut buf = Vec::with_capacity(4 + content.len());
    buf.extend_from_slice(HASH_PREFIX_INNER_NODE);
    buf.extend_from_slice(content);
    sha512half(&buf)
}

/// Verify every inner node in `nodes` actually hashes to its own claimed identity. Returns
/// the first node whose claimed hash doesn't match its recomputed hash, if any. Non-inner
/// nodes (leaves) are skipped — see module docs for why leaf verification isn't implemented.
pub fn verify_inner_nodes<'a>(
    nodes: impl IntoIterator<Item = &'a SHAMapNode>,
) -> Result<(), Hash256> {
    for node in nodes {
        if node.node_type == NodeType::Inner && recompute_inner_hash(&node.content) != node.hash {
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
        SHAMapNode { hash: recompute_inner_hash(&content), node_type: NodeType::Inner, content }
    }

    #[test]
    fn genuine_inner_node_verifies() {
        let node = inner_with_child(5, [0x11; 32]);
        assert!(verify_inner_nodes([&node]).is_ok());
    }

    #[test]
    fn tampered_content_is_caught() {
        let mut node = inner_with_child(5, [0x11; 32]);
        node.content[0] ^= 0xFF; // corrupt content without updating the claimed hash
        let err = verify_inner_nodes([&node]).unwrap_err();
        assert_eq!(err, node.hash);
    }

    #[test]
    fn leaf_nodes_are_skipped_not_flagged() {
        let leaf = SHAMapNode { hash: [0xAA; 32], node_type: NodeType::AccountState, content: vec![1, 2, 3] };
        assert!(verify_inner_nodes([&leaf]).is_ok());
    }
}
