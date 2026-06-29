use std::collections::HashMap;
use std::path::Path;

use anyhow::{bail, Result};

use xrla_common::shamap::{Hash256, InnerNode, NodeType, SHAMapDiff, SHAMapNode};

use crate::dat::scan_dat;

/// Reads SHAMap nodes from a rippled NuDB store.
///
/// PoC implementation: loads the .dat file into memory on open.
/// Production implementation: use .key file for O(1) lookups without
/// loading the full database into memory.
pub struct NuDBReader {
    store: HashMap<Hash256, Vec<u8>>,
}

impl NuDBReader {
    /// Open a NuDB store by scanning the .dat file.
    /// `dat_path` should point to the .dat file (e.g. /var/lib/rippled/db/nudb.dat).
    pub fn open(dat_path: &Path) -> Result<Self> {
        println!("Scanning NuDB: {}", dat_path.display());
        let store = scan_dat(dat_path)?;
        println!("Loaded {} nodes", store.len());
        Ok(Self { store })
    }

    /// Look up a node by hash. Returns raw content bytes.
    pub fn get(&self, hash: &Hash256) -> Option<&[u8]> {
        self.store.get(hash).map(|v| v.as_slice())
    }

    /// Collect all node hashes reachable from `root_hash` by traversing the SHAMap.
    pub fn collect_reachable(&self, root_hash: &Hash256) -> Result<HashMap<Hash256, Vec<u8>>> {
        let mut result = HashMap::new();
        let mut stack = vec![*root_hash];

        while let Some(hash) = stack.pop() {
            if result.contains_key(&hash) {
                continue;
            }
            let content = match self.get(&hash) {
                Some(c) => c.to_vec(),
                None => bail!("node not found: {}", hex::encode(hash)),
            };

            // Determine node type: rippled prefixes the value with a type byte.
            // 0 = inner, 1 = leaf (verify against rippled source for exact encoding)
            let node_type = if content.first().copied().unwrap_or(1) == 0 {
                NodeType::Inner
            } else {
                NodeType::Leaf
            };

            if matches!(node_type, NodeType::Inner) {
                // Parse children and push to stack
                if let Ok(inner) = InnerNode::from_bytes(&content[1..]) {
                    for child_hash in inner.child_hashes() {
                        if !result.contains_key(child_hash) {
                            stack.push(*child_hash);
                        }
                    }
                }
            }

            result.insert(hash, content);
        }

        Ok(result)
    }

    /// Compute the SHAMap diff between two ledger state roots.
    ///
    /// This is the core of the exporter: walk both trees simultaneously,
    /// short-circuiting on equal hashes (same subtree = no diff needed).
    pub fn diff(
        &self,
        old_root: &Hash256,
        new_root: &Hash256,
    ) -> Result<SHAMapDiff> {
        let mut diff = SHAMapDiff::default();
        self.diff_recursive(old_root, new_root, &mut diff)?;

        // Sort for deterministic serialization
        diff.added.sort_by(|a, b| a.hash.cmp(&b.hash));
        diff.deleted.sort();

        Ok(diff)
    }

    fn diff_recursive(
        &self,
        old_hash: &Hash256,
        new_hash: &Hash256,
        diff: &mut SHAMapDiff,
    ) -> Result<()> {
        // Same hash = identical subtree, nothing to do
        if old_hash == new_hash {
            return Ok(());
        }

        let new_content = match self.get(new_hash) {
            Some(c) => c.to_vec(),
            None => bail!("node not found: {}", hex::encode(new_hash)),
        };

        let new_type = if new_content.first().copied().unwrap_or(1) == 0 {
            NodeType::Inner
        } else {
            NodeType::Leaf
        };

        // If old_hash is zero (null), the entire new subtree is added
        if old_hash == &[0u8; 32] {
            self.collect_added(new_hash, diff)?;
            return Ok(());
        }

        let old_content = match self.get(old_hash) {
            Some(c) => c.to_vec(),
            None => bail!("old node not found: {}", hex::encode(old_hash)),
        };

        match new_type {
            NodeType::Leaf => {
                // Leaf changed or replaced
                diff.added.push(SHAMapNode {
                    hash:      *new_hash,
                    node_type: NodeType::Leaf,
                    content:   new_content,
                });
                diff.deleted.push(*old_hash);
            }
            NodeType::Inner => {
                let new_inner = InnerNode::from_bytes(&new_content[1..])?;
                let old_type = if old_content.first().copied().unwrap_or(1) == 0 {
                    NodeType::Inner
                } else {
                    NodeType::Leaf
                };

                if matches!(old_type, NodeType::Inner) {
                    let old_inner = InnerNode::from_bytes(&old_content[1..])?;

                    // Recurse into each child slot
                    for i in 0..16 {
                        let old_child = old_inner.children[i].unwrap_or([0u8; 32]);
                        let new_child = new_inner.children[i].unwrap_or([0u8; 32]);

                        if old_child == new_child {
                            continue;
                        }

                        match (old_inner.children[i], new_inner.children[i]) {
                            (None, Some(nc)) => self.collect_added(&nc, diff)?,
                            (Some(oc), None) => self.collect_deleted(&oc, diff)?,
                            (Some(_), Some(_)) => self.diff_recursive(&old_child, &new_child, diff)?,
                            (None, None) => {}
                        }
                    }

                    // The inner node itself changed (new hash)
                    diff.added.push(SHAMapNode {
                        hash:      *new_hash,
                        node_type: NodeType::Inner,
                        content:   new_content,
                    });
                    diff.deleted.push(*old_hash);
                } else {
                    // Old was leaf, new is inner — add all of new subtree, delete old leaf
                    self.collect_added(new_hash, diff)?;
                    diff.deleted.push(*old_hash);
                }
            }
        }

        Ok(())
    }

    fn collect_added(&self, hash: &Hash256, diff: &mut SHAMapDiff) -> Result<()> {
        let content = match self.get(hash) {
            Some(c) => c.to_vec(),
            None => bail!("node not found: {}", hex::encode(hash)),
        };
        let node_type = if content.first().copied().unwrap_or(1) == 0 {
            NodeType::Inner
        } else {
            NodeType::Leaf
        };

        if matches!(node_type, NodeType::Inner) {
            if let Ok(inner) = InnerNode::from_bytes(&content[1..]) {
                for child_hash in inner.child_hashes().cloned().collect::<Vec<_>>() {
                    self.collect_added(&child_hash, diff)?;
                }
            }
        }

        diff.added.push(SHAMapNode {
            hash:      *hash,
            node_type,
            content,
        });
        Ok(())
    }

    fn collect_deleted(&self, hash: &Hash256, diff: &mut SHAMapDiff) -> Result<()> {
        let content = match self.get(hash) {
            Some(c) => c.to_vec(),
            None => return Ok(()), // already gone, skip
        };
        let node_type = if content.first().copied().unwrap_or(1) == 0 {
            NodeType::Inner
        } else {
            NodeType::Leaf
        };

        if matches!(node_type, NodeType::Inner) {
            if let Ok(inner) = InnerNode::from_bytes(&content[1..]) {
                for child_hash in inner.child_hashes().cloned().collect::<Vec<_>>() {
                    self.collect_deleted(&child_hash, diff)?;
                }
            }
        }

        diff.deleted.push(*hash);
        Ok(())
    }
}
