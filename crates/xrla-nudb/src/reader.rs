use std::path::{Path, PathBuf};

use anyhow::{bail, Result};

use xrla_common::chunk::TxRecord;
use xrla_common::shamap::{Hash256, InnerNode, SHAMapDiff, SHAMapNode, ZERO_HASH};

use crate::dat::decode_value_to_wire;
use crate::keyfile::Shard;

/// Reads SHAMap nodes from a rippled NuDB store via O(1) .key file lookups.
///
/// rippled's online_delete keeps two NuDB databases live at once during rotation, and the
/// complete state tree spans both. Each `--dat` path is paired with its sibling `nudb.key`
/// and tried in order on every lookup.
pub struct NuDBReader {
    shards: Vec<Shard>,
}

impl NuDBReader {
    /// Open one or more NuDB shards. Each `dat_path` must have a sibling `<dir>/nudb.key`.
    pub fn open(dat_paths: &[PathBuf]) -> Result<Self> {
        if dat_paths.is_empty() {
            bail!("no NuDB .dat paths provided");
        }
        let mut shards = Vec::with_capacity(dat_paths.len());
        for dat_path in dat_paths {
            let key_path = dat_path.with_extension("key");
            println!("Opening NuDB shard: {} + {}", dat_path.display(), key_path.display());
            shards.push(Shard::open(dat_path, &key_path)?);
        }
        Ok(Self { shards })
    }

    pub fn open_single(dat_path: &Path) -> Result<Self> {
        Self::open(std::slice::from_ref(&dat_path.to_path_buf()))
    }

    /// Look up decoded wire bytes for a node hash, trying each shard in turn.
    pub fn get_wire(&self, hash: &Hash256) -> Result<Option<Vec<u8>>> {
        for shard in &self.shards {
            if let Some(value) = shard.fetch(hash)? {
                // decode_value_to_wire returns None for ledger objects / unknown codecs,
                // which are not part of the account SHAMap — treat as "not this node".
                if let Some(wire) = decode_value_to_wire(&value) {
                    return Ok(Some(wire));
                }
            }
        }
        Ok(None)
    }

    /// Parse a node by hash.
    pub fn get_node(&self, hash: &Hash256) -> Result<SHAMapNode> {
        let wire = self.get_wire(hash)?
            .ok_or_else(|| anyhow::anyhow!("node not found: {}", hex::encode(hash)))?;
        SHAMapNode::from_wire_bytes(*hash, &wire)
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

    /// Collect all transactions (with metadata) from a transaction SHAMap root
    /// (the ledger's `TransSetHash`). Returns records sorted by tx_hash.
    /// Empty if `tx_root` is the zero hash (a ledger with no transactions).
    ///
    /// Transaction-with-metadata leaf content is `['SND\0'][VL(tx)][VL(meta)][32-byte txid]`
    /// (rippled SHAMapTreeNode::serializeWithPrefix for the tx-with-meta map). The txid is
    /// the SHAMap key and equals SHA512half(HashPrefix::transactionID + tx).
    pub fn collect_transactions(&self, tx_root: &Hash256) -> Result<Vec<TxRecord>> {
        let mut out = Vec::new();
        if tx_root == &ZERO_HASH {
            return Ok(out);
        }
        let mut visited = std::collections::HashSet::new();
        let mut stack = vec![*tx_root];
        while let Some(hash) = stack.pop() {
            if !visited.insert(hash) {
                continue;
            }
            let node = self.get_node(&hash)?;
            if node.node_type.is_inner() {
                let inner = InnerNode::from_node(&node).map_err(|e| anyhow::anyhow!("{e}"))?;
                for child in inner.child_hashes() {
                    if !visited.contains(child) {
                        stack.push(*child);
                    }
                }
            } else {
                out.push(parse_tx_leaf(&node.content)?);
            }
        }
        out.sort_by(|a, b| a.tx_hash.cmp(&b.tx_hash));
        Ok(out)
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

/// Parse a transaction-with-metadata SHAMap leaf's content into a TxRecord.
/// Content layout: ['SND\0' (4)][VL(tx)][VL(meta)][32-byte txid].
fn parse_tx_leaf(content: &[u8]) -> Result<TxRecord> {
    if content.len() < 4 + 32 || &content[0..4] != b"SND\0" {
        anyhow::bail!(
            "unexpected tx leaf: len={} prefix={:02x?}",
            content.len(),
            &content[..content.len().min(4)]
        );
    }
    let mut p = 4;
    let (tx_len, n) = read_vl(&content[p..])?;
    p += n;
    let tx_blob = content
        .get(p..p + tx_len)
        .ok_or_else(|| anyhow::anyhow!("tx leaf: tx blob truncated"))?
        .to_vec();
    p += tx_len;

    let (meta_len, n) = read_vl(&content[p..])?;
    p += n;
    let meta_blob = content
        .get(p..p + meta_len)
        .ok_or_else(|| anyhow::anyhow!("tx leaf: meta blob truncated"))?
        .to_vec();
    p += meta_len;

    if content.len() - p != 32 {
        anyhow::bail!("tx leaf: expected 32-byte txid, found {} bytes", content.len() - p);
    }
    let mut tx_hash = [0u8; 32];
    tx_hash.copy_from_slice(&content[p..p + 32]);

    Ok(TxRecord { tx_hash, tx_blob, meta_blob })
}

/// rippled variable-length (VL) length prefix decoder.
/// Returns (length, bytes_consumed). See Serializer::addVL / ripple protocol.
fn read_vl(b: &[u8]) -> Result<(usize, usize)> {
    let b0 = *b.first().ok_or_else(|| anyhow::anyhow!("vl: truncated"))? as usize;
    if b0 <= 192 {
        Ok((b0, 1))
    } else if b0 <= 240 {
        let b1 = *b.get(1).ok_or_else(|| anyhow::anyhow!("vl: truncated (2)"))? as usize;
        Ok((193 + (b0 - 193) * 256 + b1, 2))
    } else if b0 <= 254 {
        let b1 = *b.get(1).ok_or_else(|| anyhow::anyhow!("vl: truncated (3a)"))? as usize;
        let b2 = *b.get(2).ok_or_else(|| anyhow::anyhow!("vl: truncated (3b)"))? as usize;
        Ok((12481 + (b0 - 241) * 65536 + b1 * 256 + b2, 3))
    } else {
        anyhow::bail!("vl: invalid length byte {b0}")
    }
}
