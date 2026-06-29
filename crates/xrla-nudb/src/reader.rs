use std::collections::HashMap;
use std::path::Path;

use anyhow::{bail, Result};

use xrla_common::shamap::{Hash256, InnerNode, SHAMapDiff, SHAMapNode, ZERO_HASH};

use crate::dat::scan_dat;

/// Reads SHAMap nodes from a rippled NuDB store.
///
/// PoC implementation: loads the .dat file into memory on open.
/// For production: replace scan_dat() with .key file lookups (O(1), no full load).
pub struct NuDBReader {
    /// key = node hash, value = raw wire bytes (content + trailing type byte)
    store: HashMap<Hash256, Vec<u8>>,
}

impl NuDBReader {
    pub fn open(dat_path: &Path) -> Result<Self> {
        println!("Scanning NuDB: {}", dat_path.display());
        let store = scan_dat(dat_path)?;
        println!("Loaded {} nodes", store.len());
        Ok(Self { store })
    }

    /// Look up raw wire bytes for a node hash.
    pub fn get_wire(&self, hash: &Hash256) -> Option<&[u8]> {
        self.store.get(hash).map(|v| v.as_slice())
    }

    /// Parse a node by hash.
    pub fn get_node(&self, hash: &Hash256) -> Result<SHAMapNode> {
        let wire = self.get_wire(hash)
            .ok_or_else(|| anyhow::anyhow!("node not found: {}", hex::encode(hash)))?;
        SHAMapNode::from_wire_bytes(*hash, wire)
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Collect all nodes reachable from root_hash by traversing the SHAMap tree.
    pub fn collect_reachable(&self, root_hash: &Hash256) -> Result<Vec<SHAMapNode>> {
        let mut result = Vec::new();
        let mut visited = std::collections::HashSet::new();
        let mut stack = vec![*root_hash];

        while let Some(hash) = stack.pop() {
            if visited.contains(&hash) {
                continue;
            }
            visited.insert(hash);

            let node = self.get_node(&hash)?;

            if node.node_type.is_inner() {
                let inner = InnerNode::from_node(&node)
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                for child_hash in inner.child_hashes() {
                    if !visited.contains(child_hash) {
                        stack.push(*child_hash);
                    }
                }
            }

            result.push(node);
        }

        Ok(result)
    }

    /// Compute the SHAMap diff between two ledger state roots.
    ///
    /// Walks both trees simultaneously, short-circuiting on equal hashes
    /// (same hash = identical subtree = skip entirely).
    /// This is the core primitive: O(changed nodes), not O(total nodes).
    pub fn diff(&self, old_root: &Hash256, new_root: &Hash256) -> Result<SHAMapDiff> {
        let mut diff = SHAMapDiff::default();
        self.diff_nodes(old_root, new_root, &mut diff)?;

        // Deterministic ordering: sort by hash ascending
        diff.added.sort_by(|a, b| a.hash.cmp(&b.hash));
        diff.deleted.sort();

        Ok(diff)
    }

    fn diff_nodes(
        &self,
        old_hash: &Hash256,
        new_hash: &Hash256,
        diff: &mut SHAMapDiff,
    ) -> Result<()> {
        // Same hash = identical subtree — skip entirely (the key optimization)
        if old_hash == new_hash {
            return Ok(());
        }

        // New hash is zero = subtree deleted entirely
        if new_hash == &ZERO_HASH {
            self.collect_deleted(old_hash, diff)?;
            return Ok(());
        }

        // Old hash is zero = subtree entirely new
        if old_hash == &ZERO_HASH {
            self.collect_added(new_hash, diff)?;
            return Ok(());
        }

        let new_node = self.get_node(new_hash)?;
        let old_node = self.get_node(old_hash)?;

        if new_node.node_type.is_inner() && old_node.node_type.is_inner() {
            // Both inner: recurse into each child slot
            let new_inner = InnerNode::from_node(&new_node)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let old_inner = InnerNode::from_node(&old_node)
                .map_err(|e| anyhow::anyhow!("{e}"))?;

            for i in 0..16 {
                let old_child = old_inner.children[i].unwrap_or(ZERO_HASH);
                let new_child = new_inner.children[i].unwrap_or(ZERO_HASH);
                if old_child != new_child {
                    self.diff_nodes(&old_child, &new_child, diff)?;
                }
            }

            // The inner node itself has a new hash — add new, delete old
            diff.added.push(new_node);
            diff.deleted.push(*old_hash);
        } else {
            // One or both are leaves, or type changed — replace entirely
            self.collect_added(new_hash, diff)?;
            self.collect_deleted(old_hash, diff)?;
        }

        Ok(())
    }

    fn collect_added(&self, hash: &Hash256, diff: &mut SHAMapDiff) -> Result<()> {
        if hash == &ZERO_HASH {
            return Ok(());
        }
        let node = self.get_node(hash)?;
        if node.node_type.is_inner() {
            let inner = InnerNode::from_node(&node)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            for child_hash in inner.child_hashes().cloned().collect::<Vec<_>>() {
                self.collect_added(&child_hash, diff)?;
            }
        }
        diff.added.push(node);
        Ok(())
    }

    fn collect_deleted(&self, hash: &Hash256, diff: &mut SHAMapDiff) -> Result<()> {
        if hash == &ZERO_HASH {
            return Ok(());
        }
        // Node may already be gone from store — that's OK
        let node = match self.get_node(hash) {
            Ok(n) => n,
            Err(_) => { diff.deleted.push(*hash); return Ok(()); }
        };
        if node.node_type.is_inner() {
            let inner = InnerNode::from_node(&node)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            for child_hash in inner.child_hashes().cloned().collect::<Vec<_>>() {
                self.collect_deleted(&child_hash, diff)?;
            }
        }
        diff.deleted.push(*hash);
        Ok(())
    }
}
