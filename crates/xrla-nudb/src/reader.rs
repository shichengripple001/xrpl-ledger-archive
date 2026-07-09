use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use rayon::prelude::*;

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

    /// Collect all nodes reachable from root_hash, fetching each BFS level's nodes
    /// concurrently via a dedicated thread pool of `concurrency` workers. Falls back to the
    /// plain serial `collect_reachable` when `concurrency <= 1`.
    ///
    /// Correctness is identical to `collect_reachable`: a hash is only ever added to a
    /// frontier once (deduped via `visited` before insertion), so every unique node is
    /// fetched exactly once and appears exactly once in the result — only the fetch
    /// order/parallelism differs. See PLAN.md Phase 2 item 2.
    pub fn collect_reachable_concurrent(
        &self,
        root_hash: &Hash256,
        concurrency: usize,
    ) -> Result<Vec<SHAMapNode>> {
        if concurrency <= 1 {
            return self.collect_reachable(root_hash);
        }

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(concurrency)
            .build()
            .context("build concurrent NuDB fetch thread pool")?;

        let mut result = Vec::new();
        let mut visited: std::collections::HashSet<Hash256> = std::collections::HashSet::new();
        visited.insert(*root_hash);
        let mut frontier = vec![*root_hash];

        while !frontier.is_empty() {
            let fetched: Vec<Result<SHAMapNode>> =
                pool.install(|| frontier.par_iter().map(|h| self.get_node(h)).collect());

            let mut next_frontier = Vec::new();
            for node_result in fetched {
                let node = node_result?;
                if node.node_type.is_inner() {
                    let inner = InnerNode::from_node(&node).map_err(|e| anyhow::anyhow!("{e}"))?;
                    for child_hash in inner.child_hashes() {
                        if visited.insert(*child_hash) {
                            next_frontier.push(*child_hash);
                        }
                    }
                }
                result.push(node);
            }
            frontier = next_frontier;
        }

        Ok(result)
    }

    /// True if any open shard's `.dat` file shares a physical device with the OS root
    /// filesystem (e.g. a laptop's single boot disk), vs. genuinely dedicated storage (real
    /// full-history server instance-store NVMe). Fails safe: if device IDs can't be read for
    /// any reason, treated as shared.
    ///
    /// **2026-07-08 incident**: `calibrate_concurrency` climbed to 256 concurrent threads
    /// against two real ~6 GB files sitting on a laptop's single shared disk. The resulting
    /// I/O saturation made the whole machine unresponsive badly enough to require a hard
    /// restart — the calibration logic optimized purely for *this benchmark's own*
    /// throughput and had no concept of the cost imposed on every other process sharing that
    /// disk. This check exists so a shared-disk environment gets a hard, low concurrency
    /// ceiling regardless of what calibration would otherwise pick.
    fn shares_device_with_os_root(&self) -> bool {
        use std::os::unix::fs::MetadataExt;
        let root_dev = match std::fs::metadata("/") {
            Ok(m) => m.dev(),
            Err(_) => return true, // can't tell — fail safe
        };
        self.shards.iter().any(|s| match s.dat_device() {
            Ok(d) => d == root_dev,
            Err(_) => true, // can't tell this shard — fail safe
        })
    }

    /// Probe the real store to pick a concurrency level for `collect_reachable_concurrent`,
    /// instead of hardcoding one that may be wrong for the actual hardware (see PLAN.md
    /// Phase 2 item 2 — the right level depends on real, unmeasured-per-box NVMe
    /// characteristics). Samples a bounded set of real hashes near `root_hash`, then times
    /// fetching them at increasing concurrency levels, stopping once additional concurrency
    /// stops meaningfully improving throughput OR once latency indicates real queueing (see
    /// `calibrate_concurrency_with_levels`). Safe by default: the concurrency ceiling is far
    /// lower, and reached far more cautiously, when the target store shares a disk with the
    /// OS — see `shares_device_with_os_root` and its incident notes.
    pub fn calibrate_concurrency(&self, root_hash: &Hash256) -> Result<usize> {
        // Ladders start small and climb one step at a time — unlike the pre-incident version,
        // which jumped straight to hardcoded [1, 4, 16, 64, 256] regardless of hardware, a
        // dangerous level got a live trial run before anything could rule it out. Max ceiling
        // is now 32 (dedicated) or 4 (shared) — nowhere near the old 256.
        const DEDICATED_LEVELS: &[usize] = &[1, 2, 4, 8, 16, 32];
        const SHARED_DISK_LEVELS: &[usize] = &[1, 2, 4];

        let levels = if self.shares_device_with_os_root() {
            SHARED_DISK_LEVELS
        } else {
            DEDICATED_LEVELS
        };
        self.calibrate_concurrency_with_levels(root_hash, levels)
    }

    /// Core calibration logic, parameterized on the ladder to climb — split out from
    /// `calibrate_concurrency` so tests can exercise it deterministically without depending
    /// on real device detection.
    fn calibrate_concurrency_with_levels(&self, root_hash: &Hash256, levels: &[usize]) -> Result<usize> {
        const SAMPLE_SIZE: usize = 2048;
        const MIN_GAIN: f64 = 1.15; // require >=15% throughput improvement to keep climbing
        // Absolute per-request latency ceiling: real NVMe/SSD random reads are sub-millisecond.
        // 50ms average means genuine queueing/contention, not just "a bit slower" — abort the
        // ladder immediately rather than only reacting once the *relative* throughput gain
        // stops looking good, since by then damage to the rest of the machine may already be
        // underway (see `shares_device_with_os_root` incident notes).
        const ABORT_LATENCY_SECS: f64 = 0.05;

        let min_sample = levels.get(1).copied().unwrap_or(1);
        let sample = self.sample_hashes(root_hash, SAMPLE_SIZE)?;
        if sample.len() < min_sample {
            // Not enough real data to calibrate meaningfully (e.g. a tiny tree) — serial
            // is a safe, correct default.
            return Ok(1);
        }

        let mut best_level = 1;
        let mut best_throughput = 0.0f64;

        for &level in levels {
            if level > sample.len() {
                break;
            }
            // Bounded probe size regardless of level — even the top of the ladder only ever
            // issues a small, short-lived burst of reads during calibration.
            let probe_size = (level * 4).clamp(1, 128).min(sample.len());
            let probe: Vec<Hash256> = sample.iter().take(probe_size).cloned().collect();
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(level)
                .build()
                .context("build calibration thread pool")?;

            let start = std::time::Instant::now();
            pool.install(|| {
                probe.par_iter().for_each(|h| {
                    let _ = self.get_node(h);
                });
            });
            let elapsed = start.elapsed().as_secs_f64().max(1e-9);
            let avg_latency = elapsed / probe.len() as f64;
            let throughput = probe.len() as f64 / elapsed;

            if avg_latency > ABORT_LATENCY_SECS {
                // Real queueing detected at this level — don't trust its throughput number
                // (it can look fine in isolation while still hurting the rest of the
                // machine) and don't climb any further. Keep the last known-safe level.
                break;
            }

            if level == levels[0] || throughput > best_throughput * MIN_GAIN {
                best_throughput = throughput;
                best_level = level;
            } else {
                break; // marginal gain — past the knee, stop climbing
            }
        }

        Ok(best_level)
    }

    /// Walk the full reachable set from `root_hash`, self-tuning concurrency by probing the
    /// real store first (`calibrate_concurrency`) rather than using a fixed, hardcoded
    /// in-flight count. Safe by default: calibration climbs cautiously and is hard-capped
    /// much lower when the target shares a disk with the OS (see `shares_device_with_os_root`).
    /// This is the entry point production callers (e.g. `xrla-export`) should use.
    pub fn collect_reachable_adaptive(&self, root_hash: &Hash256) -> Result<Vec<SHAMapNode>> {
        let concurrency = self.calibrate_concurrency(root_hash)?;
        self.collect_reachable_concurrent(root_hash, concurrency)
    }

    /// Gather up to `target` real, distinct node hashes reachable from `root_hash` via a
    /// bounded sequential walk — used to build a representative probe set for
    /// `calibrate_concurrency` without paying for a full walk first.
    fn sample_hashes(&self, root_hash: &Hash256, target: usize) -> Result<Vec<Hash256>> {
        let mut result = Vec::new();
        let mut visited = std::collections::HashSet::new();
        visited.insert(*root_hash);
        let mut stack = vec![*root_hash];

        while let Some(hash) = stack.pop() {
            if result.len() >= target {
                break;
            }
            let node = self.get_node(&hash)?;
            result.push(hash);
            if node.node_type.is_inner() {
                let inner = InnerNode::from_node(&node).map_err(|e| anyhow::anyhow!("{e}"))?;
                for child in inner.child_hashes() {
                    if visited.insert(*child) {
                        stack.push(*child);
                    }
                }
            }
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

    /// Compute `diff()` for many `(old_root, new_root)` pairs concurrently, instead of one
    /// ledger transition at a time. Results are returned in the same order as `pairs` —
    /// callers must still apply them to a running state map in that order, since each
    /// ledger's starting state depends on the previous one already being applied; only the
    /// *discovery* of what changed is parallelized here, not the bookkeeping. Falls back to
    /// a plain serial loop when `concurrency <= 1`. Uses the same `rayon` thread-pool
    /// approach as `collect_reachable_concurrent` — callers should pass a concurrency level
    /// already produced by `calibrate_concurrency` rather than an unbounded/hardcoded one.
    /// See PLAN.md Immediate TODOs item 10b.
    pub fn diff_batch_concurrent(
        &self,
        pairs: &[(Hash256, Hash256)],
        concurrency: usize,
    ) -> Result<Vec<SHAMapDiff>> {
        if concurrency <= 1 || pairs.len() <= 1 {
            return pairs.iter().map(|(old, new)| self.diff(old, new)).collect();
        }

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(concurrency)
            .build()
            .context("build concurrent diff-batch thread pool")?;

        pool.install(|| pairs.par_iter().map(|(old, new)| self.diff(old, new)).collect())
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

#[cfg(test)]
mod tests {
    use super::*;
    use xrla_common::serialize::sha512half;
    use xrla_common::shamap::NodeType;
    use xrla_common::state_tree::verify_state_nodes;

    use crate::dat::encode_wire_to_value;
    use crate::writer::write_nudb_store;

    fn leaf_node(tag: u8) -> SHAMapNode {
        let content = vec![tag; 16];
        let hash = sha512half(&content);
        SHAMapNode { hash, node_type: NodeType::AccountState, content }
    }

    fn inner_node(children: &[(usize, Hash256)]) -> SHAMapNode {
        let mut content = vec![0u8; 512];
        for &(slot, hash) in children {
            content[slot * 32..(slot + 1) * 32].copy_from_slice(&hash);
        }
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MIN\0");
        buf.extend_from_slice(&content);
        let hash = sha512half(&buf);
        SHAMapNode { hash, node_type: NodeType::Inner, content }
    }

    /// Build a small but genuinely multi-level tree (root -> 4 branches -> 16 leaves, 21
    /// nodes) into a real NuDB store at `dir`, written through the same NuDB writer/codec
    /// path production code uses (not hand-rolled bytes). Returns the opened reader and the
    /// root node.
    fn build_test_tree(dir: &std::path::Path) -> (NuDBReader, SHAMapNode) {
        std::fs::create_dir_all(dir).unwrap();
        let dat_path = dir.join("nudb.dat");
        let key_path = dir.join("nudb.key");

        let leaves: Vec<SHAMapNode> = (0u8..16).map(leaf_node).collect();
        let branches: Vec<SHAMapNode> = (0..4usize)
            .map(|b| {
                let children: Vec<(usize, Hash256)> =
                    (0..4usize).map(|i| (i, leaves[b * 4 + i].hash)).collect();
                inner_node(&children)
            })
            .collect();
        let root = inner_node(
            &branches.iter().enumerate().map(|(i, n)| (i, n.hash)).collect::<Vec<_>>(),
        );

        let entries: Vec<(Hash256, Vec<u8>)> = leaves
            .iter()
            .chain(branches.iter())
            .chain(std::iter::once(&root))
            .map(|n| (n.hash, encode_wire_to_value(&n.content, &n.node_type)))
            .collect();

        write_nudb_store(&entries, &dat_path, &key_path).unwrap();
        let nudb = NuDBReader::open_single(&dat_path).unwrap();
        (nudb, root)
    }

    /// `collect_reachable_concurrent` at any concurrency level (including the adaptive,
    /// self-tuned path) must return exactly the same node set as the plain serial walk —
    /// only fetch order/parallelism should differ, never correctness.
    #[test]
    fn concurrent_walk_matches_serial_walk() {
        let dir = std::env::temp_dir()
            .join(format!("xrla_nudb_reader_concurrent_test_{}", std::process::id()));
        let (nudb, root) = build_test_tree(&dir);

        let serial = nudb.collect_reachable(&root.hash).unwrap();
        let mut serial_hashes: Vec<Hash256> = serial.iter().map(|n| n.hash).collect();
        serial_hashes.sort();
        assert_eq!(serial_hashes.len(), 21, "1 root + 4 branches + 16 leaves");

        for &level in &[1usize, 2, 4, 8] {
            let concurrent = nudb.collect_reachable_concurrent(&root.hash, level).unwrap();
            let mut concurrent_hashes: Vec<Hash256> = concurrent.iter().map(|n| n.hash).collect();
            concurrent_hashes.sort();
            assert_eq!(
                concurrent_hashes, serial_hashes,
                "concurrency={level} produced a different node set than the serial walk"
            );
        }

        let adaptive = nudb.collect_reachable_adaptive(&root.hash).unwrap();
        let mut adaptive_hashes: Vec<Hash256> = adaptive.iter().map(|n| n.hash).collect();
        adaptive_hashes.sort();
        assert_eq!(adaptive_hashes, serial_hashes);

        let level = nudb.calibrate_concurrency(&root.hash).unwrap();
        assert!(level >= 1, "calibration must always return a usable concurrency level");

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Regression test for the 2026-07-08 I/O-saturation incident: calibration must always
    /// return a level from the exact ladder it was given, and must never silently pick
    /// something outside it (which is how a future edit could reintroduce an unbounded
    /// climb like the pre-incident `[1, 4, 16, 64, 256]` ladder). Exercises the
    /// device-detection-independent core (`calibrate_concurrency_with_levels`) directly so
    /// this is deterministic regardless of what disk the test happens to run on.
    #[test]
    fn calibration_never_exceeds_the_given_ladder() {
        let dir = std::env::temp_dir()
            .join(format!("xrla_nudb_reader_calibration_ladder_test_{}", std::process::id()));
        let (nudb, root) = build_test_tree(&dir);

        for ladder in [&[1usize][..], &[1, 2, 4][..], &[1, 2, 4, 8, 16, 32][..]] {
            let level = nudb.calibrate_concurrency_with_levels(&root.hash, ladder).unwrap();
            assert!(
                ladder.contains(&level),
                "calibrated level {level} is not in the given ladder {ladder:?}"
            );
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    /// `diff_batch_concurrent` must produce exactly the same set of added/deleted nodes as
    /// calling `diff()` serially, one pair at a time — only the discovery order/parallelism
    /// should differ. Builds a short chain of 4 states (root_0..root_3), each differing from
    /// the previous by exactly one leaf, all versions written into the same real NuDB store
    /// (mirroring how a real archive keeps every historical version reachable).
    #[test]
    fn diff_batch_concurrent_matches_serial() {
        let dir = std::env::temp_dir()
            .join(format!("xrla_nudb_reader_diffbatch_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let dat_path = dir.join("nudb.dat");
        let key_path = dir.join("nudb.key");

        // leaves[0..4] = originals (slots 0..4 at version 0), leaves[4..8] = their
        // replacements, phased in one slot per version so each consecutive root pair
        // differs by exactly one leaf.
        let leaves: Vec<SHAMapNode> = (0u8..8).map(leaf_node).collect();
        let roots: Vec<SHAMapNode> = (0..4usize)
            .map(|version| {
                let children: Vec<(usize, Hash256)> = (0..4usize)
                    .map(|slot| {
                        let leaf = if slot < version { &leaves[4 + slot] } else { &leaves[slot] };
                        (slot, leaf.hash)
                    })
                    .collect();
                inner_node(&children)
            })
            .collect();

        let mut entries: Vec<(Hash256, Vec<u8>)> = leaves
            .iter()
            .map(|n| (n.hash, encode_wire_to_value(&n.content, &n.node_type)))
            .collect();
        entries.extend(roots.iter().map(|n| (n.hash, encode_wire_to_value(&n.content, &n.node_type))));

        write_nudb_store(&entries, &dat_path, &key_path).unwrap();
        let nudb = NuDBReader::open_single(&dat_path).unwrap();

        let pairs: Vec<(Hash256, Hash256)> =
            (0..roots.len() - 1).map(|i| (roots[i].hash, roots[i + 1].hash)).collect();

        let serial: Vec<SHAMapDiff> = pairs.iter().map(|(o, n)| nudb.diff(o, n).unwrap()).collect();
        assert!(
            serial.iter().all(|d| !d.added.is_empty()),
            "each consecutive pair should have at least one added node (sanity check on the test tree)"
        );

        for &level in &[1usize, 2, 4] {
            let concurrent = nudb.diff_batch_concurrent(&pairs, level).unwrap();
            assert_eq!(concurrent.len(), serial.len());
            for (i, (c, s)) in concurrent.iter().zip(serial.iter()).enumerate() {
                let mut c_added: Vec<Hash256> = c.added.iter().map(|n| n.hash).collect();
                let mut s_added: Vec<Hash256> = s.added.iter().map(|n| n.hash).collect();
                c_added.sort();
                s_added.sort();
                assert_eq!(c_added, s_added, "concurrency={level} pair {i}: added set mismatch");

                let mut c_deleted = c.deleted.clone();
                let mut s_deleted = s.deleted.clone();
                c_deleted.sort();
                s_deleted.sort();
                assert_eq!(c_deleted, s_deleted, "concurrency={level} pair {i}: deleted set mismatch");
            }
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Validates `xrla_common::state_tree`'s node-hash formulas against a real mainnet
    /// checkpoint — the entire reachable state tree from a real ledger's `AccountSetHash`,
    /// not a sample. This is how the leaf-node formula (`SHA512half(content)`, no prefix)
    /// was originally derived and confirmed: tried candidate formulas against real nodes
    /// until one matched every single one.
    ///
    /// Requires real rippled NuDB shards on disk:
    ///   RIPPLED_DAT_PATHS=/path/shard0/nudb.dat,/path/shard1/nudb.dat \
    ///   RIPPLED_ACCOUNT_HASH=<hex AccountSetHash for that checkpoint ledger> \
    ///   cargo test --package xrla-nudb --lib -- --ignored real_snapshot_state_nodes --nocapture
    ///
    /// Last run against a real mainnet checkpoint (ledger 105277428): 7,912,690 inner +
    /// 19,118,965 leaf nodes, 27,031,655 total, zero mismatches.
    #[test]
    #[ignore]
    fn real_snapshot_state_nodes_self_verify() {
        let dat_paths: Vec<PathBuf> = std::env::var("RIPPLED_DAT_PATHS")
            .expect("set RIPPLED_DAT_PATHS (comma-separated .dat paths)")
            .split(',')
            .map(PathBuf::from)
            .collect();
        let root_hex = std::env::var("RIPPLED_ACCOUNT_HASH").expect("set RIPPLED_ACCOUNT_HASH");
        let root_bytes = hex::decode(root_hex.trim()).expect("valid hex");
        let root: Hash256 = root_bytes.try_into().expect("32 bytes");

        let nudb = NuDBReader::open(&dat_paths).expect("open real NuDB shards");
        let nodes = nudb.collect_reachable(&root).expect("walk real checkpoint");
        assert!(nodes.len() > 1_000_000, "expected a real full checkpoint, got {} nodes", nodes.len());

        match verify_state_nodes(&nodes) {
            Ok(()) => println!(
                "real_snapshot_state_nodes_self_verify: {} nodes, 0 mismatches",
                nodes.len()
            ),
            Err(bad_hash) => panic!("node {} does not hash to its own claimed content", hex::encode(bad_hash)),
        }
    }
}
