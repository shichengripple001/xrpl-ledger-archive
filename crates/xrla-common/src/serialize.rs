use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use anyhow::Result;
use sha2::{Digest, Sha256, Sha512};

use crate::chunk::{
    Chunk, ChunkError, LedgerDelta, TxMap, TxRecord,
    MAGIC_FOOTER, MAGIC_HEADER, FORMAT_VERSION, FORMAT_VERSION_STREAMED,
};
use crate::shamap::{Hash256, NodeType, SHAMapDiff, SHAMapNode};

// ---------------------------------------------------------------------------
// SHA-512/half
// ---------------------------------------------------------------------------

pub fn sha512half(data: &[u8]) -> Hash256 {
    let digest = Sha512::digest(data);
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest[..32]);
    out
}

pub fn sha256(data: &[u8]) -> [u8; 32] {
    let digest = Sha256::digest(data);
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}

/// Fields needed to independently recompute a ledger's LedgerHash.
/// Mirrors xrpld's LedgerHeader; source: libxrpl/protocol/LedgerHeader.cpp
/// `calculateLedgerHash()`, verified against real mainnet data.
pub struct LedgerHashInput {
    pub seq: u32,
    pub drops: u64,
    pub parent_hash: Hash256,
    pub tx_hash: Hash256,
    pub account_hash: Hash256,
    pub parent_close_time: u32,
    pub close_time: u32,
    pub close_time_resolution: u8,
    pub close_flags: u8,
}

/// The bytes that hash to the LedgerHash: `HashPrefix::LedgerMaster` ("LWR\0") followed by the
/// header fields. xrpld also stores exactly these bytes, keyed by the ledger hash, as the
/// ledger's `hotLEDGER` NodeObject (`saveValidatedLedger`, `Node.cpp`).
pub fn ledger_header_object(h: &LedgerHashInput) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + 4 + 8 + 32 + 32 + 32 + 4 + 4 + 1 + 1);
    buf.extend_from_slice(&[0x4C, 0x57, 0x52, 0x00]); // HashPrefix::LedgerMaster
    buf.extend_from_slice(&h.seq.to_be_bytes());
    buf.extend_from_slice(&h.drops.to_be_bytes());
    buf.extend_from_slice(&h.parent_hash);
    buf.extend_from_slice(&h.tx_hash);
    buf.extend_from_slice(&h.account_hash);
    buf.extend_from_slice(&h.parent_close_time.to_be_bytes());
    buf.extend_from_slice(&h.close_time.to_be_bytes());
    buf.push(h.close_time_resolution);
    buf.push(h.close_flags);
    buf
}

/// Recompute the LedgerHash: sha512half of `ledger_header_object`.
pub fn calculate_ledger_hash(h: &LedgerHashInput) -> Hash256 {
    sha512half(&ledger_header_object(h))
}

// ---------------------------------------------------------------------------
// Write helpers
// ---------------------------------------------------------------------------

fn write_u8(w: &mut impl Write, v: u8) -> Result<()> {
    w.write_all(&[v])?;
    Ok(())
}

fn write_u16be(w: &mut impl Write, v: u16) -> Result<()> {
    w.write_all(&v.to_be_bytes())?;
    Ok(())
}

fn write_u32be(w: &mut impl Write, v: u32) -> Result<()> {
    w.write_all(&v.to_be_bytes())?;
    Ok(())
}

fn write_u64be(w: &mut impl Write, v: u64) -> Result<()> {
    w.write_all(&v.to_be_bytes())?;
    Ok(())
}

fn write_bytes(w: &mut impl Write, b: &[u8]) -> Result<()> {
    w.write_all(b)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Node serialization
// ---------------------------------------------------------------------------

fn write_node(w: &mut impl Write, node: &SHAMapNode) -> Result<()> {
    write_bytes(w, &node.hash)?;
    write_u8(w, u8::from(&node.node_type))?;
    write_u16be(w, node.content.len() as u16)?;
    write_bytes(w, &node.content)?;
    Ok(())
}

fn write_node_list(w: &mut impl Write, mut nodes: Vec<SHAMapNode>) -> Result<()> {
    nodes.sort_by(|a, b| a.hash.cmp(&b.hash));
    write_u32be(w, nodes.len() as u32)?;
    for node in &nodes {
        write_node(w, node)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Streaming chunk writer
// ---------------------------------------------------------------------------

/// Writes a chunk directly to disk piece by piece (header, then checkpoint, then each
/// delta/tx_map as it's produced) instead of buffering the whole chunk body in memory
/// and writing it in one shot. Bytes are hashed as they're written so the running
/// SHA-512/half never needs the full body materialized either.
///
/// Writes to `<final_path>` with a `.tmp` suffix and renames to the real name only in
/// `finish()`, so a mid-write crash (OOM, kill, disk full) never leaves a corrupt file
/// at the final name — `Drop` cleans up the temp file if `finish()` was never reached.
///
/// Emits format v3 (`FORMAT_VERSION_STREAMED`): same fields as v2, but each ledger's delta
/// is followed immediately by its tx_map instead of all deltas preceding all tx_maps. That
/// reordering is what makes streaming possible at all — under v2 you cannot finish the delta
/// block until the last ledger's tx_map has also been computed, so one side always has to be
/// buffered. `serialize_chunk` still emits v2; `deserialize_chunk` reads both.
pub struct ChunkWriter {
    inner: BufWriter<File>,
    tmp_path: PathBuf,
    final_path: PathBuf,
    hasher: Option<Sha512>,
    chunk_hash_offset: u64,
    finished: bool,
}

impl ChunkWriter {
    pub fn create(
        final_path: PathBuf,
        network_id: u32,
        start_ledger: u32,
        end_ledger: u32,
        checkpoint_hash: Hash256,
    ) -> Result<Self> {
        let mut tmp_path = final_path.clone();
        tmp_path.as_mut_os_string().push(".tmp");
        let file = File::create(&tmp_path)?;
        let mut inner = BufWriter::new(file);

        write_bytes(&mut inner, MAGIC_HEADER)?;
        write_u8(&mut inner, FORMAT_VERSION_STREAMED)?;
        write_u32be(&mut inner, network_id)?;
        write_u32be(&mut inner, start_ledger)?;
        write_u32be(&mut inner, end_ledger)?;
        write_bytes(&mut inner, &checkpoint_hash)?;
        let chunk_hash_offset = inner.stream_position()?;
        write_bytes(&mut inner, &[0u8; 32])?; // placeholder, filled in by finish()

        Ok(Self {
            inner,
            tmp_path,
            final_path,
            hasher: Some(Sha512::new()),
            chunk_hash_offset,
            finished: false,
        })
    }

    fn body(&mut self, buf: &[u8]) -> Result<()> {
        self.hasher.as_mut().expect("hasher taken before finish").update(buf);
        self.inner.write_all(buf)?;
        Ok(())
    }

    /// Write the checkpoint: full SHAMap state at start_ledger, sorted by hash (the
    /// on-disk order the format requires). Takes references so the caller doesn't need
    /// a second, cloned copy of every node just to hand it to this call.
    pub fn write_checkpoint(&mut self, nodes: &mut [&SHAMapNode]) -> Result<()> {
        nodes.sort_by(|a, b| a.hash.cmp(&b.hash));
        let mut buf = Vec::new();
        write_u32be(&mut buf, nodes.len() as u32)?;
        self.body(&buf)?;
        for node in nodes.iter() {
            buf.clear();
            write_node(&mut buf, node)?;
            self.body(&buf)?;
        }
        Ok(())
    }

    pub fn write_delta(&mut self, delta: &LedgerDelta) -> Result<()> {
        let mut buf = Vec::new();
        write_delta(&mut buf, delta)?;
        self.body(&buf)
    }

    pub fn write_tx_map(&mut self, tx_map: &TxMap) -> Result<()> {
        let mut buf = Vec::new();
        write_tx_map(&mut buf, tx_map)?;
        self.body(&buf)
    }

    /// Write the footer, compute the real chunk_hash over everything written so far,
    /// seek back to fill in the header's placeholder, then atomically rename the temp
    /// file to its final name. Returns the computed chunk_hash.
    pub fn finish(mut self) -> Result<Hash256> {
        self.body(MAGIC_FOOTER)?;
        let digest = self.hasher.take().expect("finish called twice").finalize();
        let mut chunk_hash = [0u8; 32];
        chunk_hash.copy_from_slice(&digest[..32]);

        self.inner.flush()?;
        self.inner.seek(SeekFrom::Start(self.chunk_hash_offset))?;
        self.inner.write_all(&chunk_hash)?;
        self.inner.flush()?;

        fs::rename(&self.tmp_path, &self.final_path)?;
        self.finished = true;
        Ok(chunk_hash)
    }
}

impl Drop for ChunkWriter {
    fn drop(&mut self) {
        if !self.finished {
            let _ = fs::remove_file(&self.tmp_path);
        }
    }
}

// ---------------------------------------------------------------------------
// Streaming chunk reader
// ---------------------------------------------------------------------------

/// Wraps a `Read` and feeds every byte actually consumed through a running SHA-512, so the
/// existing `read_node`/`read_delta`/`read_tx_map` helpers (unchanged, still generic over
/// `impl Read`) can be reused for streaming without duplicating their parsing logic.
struct HashingReader<R> {
    inner: R,
    hasher: Sha512,
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
}

/// Reads a v3 chunk one piece at a time — checkpoint node by node, then (delta, tx_map) pair
/// by pair — instead of `deserialize_chunk`'s approach of reading the whole file into one
/// `Vec<u8>` and materializing every node/delta/tx_map into a second, fully-owned `Chunk`
/// struct before a caller sees any of it. That approach held the raw file bytes, the parsed
/// `Chunk`, and the replay's own `state`/`all_state_nodes` maps all alive simultaneously —
/// three-plus copies of the same content — and OOM-killed a real 20,000-ledger mainnet
/// import at 127 GB RSS (2026-09-29). `ChunkReader` never holds more than the current
/// node/delta/tx_map being parsed, so peak memory is whatever the caller chooses to keep
/// (for `xrla-import`, that's just the final `state`/`all_state_nodes`/`tx_nodes`).
///
/// v3 only, not v2: use `deserialize_chunk` for v2 — the v2 block layout (all deltas, then
/// all tx_maps) can't be streamed for the same reason `ChunkWriter` can't write it streamed;
/// see `spec/chunk-format.md`.
pub struct ChunkReader {
    inner: HashingReader<BufReader<File>>,
    pub network_id: u32,
    pub start_ledger: u32,
    pub end_ledger: u32,
    pub checkpoint_hash: Hash256,
    stored_chunk_hash: Hash256,
    deltas_remaining: u32,
}

impl ChunkReader {
    /// Opens the file and reads its header. Errors if the file is v2 — callers should fall
    /// back to `deserialize_chunk` for those.
    pub fn open(path: &std::path::Path) -> Result<Self> {
        let mut header_reader = BufReader::new(File::open(path)?);

        let magic = read_exact(&mut header_reader, 4)?;
        if magic != MAGIC_HEADER {
            anyhow::bail!("{}", ChunkError::InvalidMagic);
        }
        let version = read_u8(&mut header_reader)?;
        if version != FORMAT_VERSION_STREAMED {
            anyhow::bail!(
                "ChunkReader only streams format v{FORMAT_VERSION_STREAMED} chunks; this file \
                 is v{version} — use deserialize_chunk instead"
            );
        }
        let network_id = read_u32be(&mut header_reader)?;
        let start_ledger = read_u32be(&mut header_reader)?;
        let end_ledger = read_u32be(&mut header_reader)?;
        let checkpoint_hash = read_hash(&mut header_reader)?;
        let stored_chunk_hash = read_hash(&mut header_reader)?;

        Ok(Self {
            inner: HashingReader { inner: header_reader, hasher: Sha512::new() },
            network_id,
            start_ledger,
            end_ledger,
            checkpoint_hash,
            stored_chunk_hash,
            deltas_remaining: end_ledger - start_ledger,
        })
    }

    /// The `chunk_hash` the header *claims*. It is only proven correct once `finish()` has
    /// returned `Ok`; anything derived from this chunk must not be published before then.
    pub fn chunk_hash(&self) -> Hash256 {
        self.stored_chunk_hash
    }

    /// Streams the checkpoint, calling `f` once per node instead of collecting a `Vec`.
    pub fn read_checkpoint(&mut self, mut f: impl FnMut(SHAMapNode)) -> Result<u32> {
        let count = read_u32be(&mut self.inner)?;
        for _ in 0..count {
            f(read_node(&mut self.inner)?);
        }
        Ok(count)
    }

    /// Reads the checkpoint ledger's own TX Map Entry — call once, right after
    /// `read_checkpoint`.
    pub fn read_checkpoint_tx_map(&mut self) -> Result<TxMap> {
        Ok(read_tx_map(&mut self.inner)?)
    }

    /// Reads the next (delta, tx_map) pair, or `None` once every ledger from
    /// `start_ledger + 1` to `end_ledger` has been read.
    pub fn next_delta_tx_map(&mut self) -> Result<Option<(LedgerDelta, TxMap)>> {
        if self.deltas_remaining == 0 {
            return Ok(None);
        }
        let delta = read_delta(&mut self.inner)?;
        let tx_map = read_tx_map(&mut self.inner)?;
        self.deltas_remaining -= 1;
        Ok(Some((delta, tx_map)))
    }

    /// Reads the footer and verifies the running chunk_hash matches the header's claim.
    /// Call only after `next_delta_tx_map` has returned `None`.
    pub fn finish(mut self) -> Result<()> {
        let footer = read_exact(&mut self.inner, 4)?;
        if footer != MAGIC_FOOTER {
            anyhow::bail!("{}", ChunkError::InvalidMagic);
        }
        let digest = self.inner.hasher.finalize();
        let mut actual = [0u8; 32];
        actual.copy_from_slice(&digest[..32]);
        if actual != self.stored_chunk_hash {
            anyhow::bail!(
                "{}",
                ChunkError::HashMismatch {
                    expected: hex::encode(self.stored_chunk_hash),
                    got: hex::encode(actual),
                }
            );
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Chunk serialization
// ---------------------------------------------------------------------------

/// Serialize a chunk to bytes. Computes and sets chunk_hash.
pub fn serialize_chunk(chunk: &Chunk) -> Result<Vec<u8>> {
    let body = serialize_body(chunk)?;
    let chunk_hash = sha512half(&body);

    let mut out = Vec::new();
    write_bytes(&mut out, MAGIC_HEADER)?;
    write_u8(&mut out, FORMAT_VERSION)?;
    write_u32be(&mut out, chunk.network_id)?;
    write_u32be(&mut out, chunk.start_ledger)?;
    write_u32be(&mut out, chunk.end_ledger)?;
    write_bytes(&mut out, &chunk.checkpoint_hash)?;
    write_bytes(&mut out, &chunk_hash)?;
    out.extend_from_slice(&body);
    Ok(out)
}

fn serialize_body(chunk: &Chunk) -> Result<Vec<u8>> {
    let mut body = Vec::new();

    // Checkpoint
    write_node_list(&mut body, chunk.checkpoint.clone())?;

    // Deltas
    for delta in &chunk.deltas {
        write_delta(&mut body, delta)?;
    }

    // TX maps
    for tx_map in &chunk.tx_maps {
        write_tx_map(&mut body, tx_map)?;
    }

    write_bytes(&mut body, MAGIC_FOOTER)?;
    Ok(body)
}

fn write_delta(w: &mut impl Write, delta: &LedgerDelta) -> Result<()> {
    write_u32be(w, delta.ledger_seq)?;

    let mut added = delta.diff.added.clone();
    added.sort_by(|a, b| a.hash.cmp(&b.hash));
    write_u32be(w, added.len() as u32)?;
    for node in &added {
        write_node(w, node)?;
    }

    let mut deleted = delta.diff.deleted.clone();
    deleted.sort();
    write_u32be(w, deleted.len() as u32)?;
    for hash in &deleted {
        write_bytes(w, hash)?;
    }
    Ok(())
}

fn write_tx_map(w: &mut impl Write, tx_map: &TxMap) -> Result<()> {
    write_u32be(w, tx_map.ledger_seq)?;
    write_bytes(w, &tx_map.ledger_hash)?;
    write_bytes(w, &tx_map.account_hash)?;
    write_u64be(w, tx_map.drops)?;
    write_u32be(w, tx_map.parent_close_time)?;
    write_u32be(w, tx_map.close_time)?;
    write_u8(w, tx_map.close_time_resolution)?;
    write_u8(w, tx_map.close_flags)?;
    write_u16be(w, tx_map.txns.len() as u16)?;

    let mut txns = tx_map.txns.clone();
    txns.sort_by(|a, b| a.tx_hash.cmp(&b.tx_hash));

    for tx in &txns {
        write_bytes(w, &tx.tx_hash)?;
        write_u32be(w, tx.tx_blob.len() as u32)?;
        write_bytes(w, &tx.tx_blob)?;
        write_u32be(w, tx.meta_blob.len() as u32)?;
        write_bytes(w, &tx.meta_blob)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Read helpers
// ---------------------------------------------------------------------------

fn read_exact(r: &mut impl Read, n: usize) -> Result<Vec<u8>, ChunkError> {
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf).map_err(|_| ChunkError::UnexpectedEof)?;
    Ok(buf)
}

fn read_u8(r: &mut impl Read) -> Result<u8, ChunkError> {
    Ok(read_exact(r, 1)?[0])
}

fn read_u16be(r: &mut impl Read) -> Result<u16, ChunkError> {
    let b = read_exact(r, 2)?;
    Ok(u16::from_be_bytes([b[0], b[1]]))
}

fn read_u32be(r: &mut impl Read) -> Result<u32, ChunkError> {
    let b = read_exact(r, 4)?;
    Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
}

fn read_hash(r: &mut impl Read) -> Result<Hash256, ChunkError> {
    let b = read_exact(r, 32)?;
    let mut h = [0u8; 32];
    h.copy_from_slice(&b);
    Ok(h)
}

// ---------------------------------------------------------------------------
// Chunk deserialization
// ---------------------------------------------------------------------------

pub fn deserialize_chunk(data: &[u8]) -> Result<Chunk, ChunkError> {
    let mut r = std::io::Cursor::new(data);

    // Header
    let magic = read_exact(&mut r, 4)?;
    if magic != MAGIC_HEADER {
        return Err(ChunkError::InvalidMagic);
    }
    let version = read_u8(&mut r)?;
    if version != FORMAT_VERSION && version != FORMAT_VERSION_STREAMED {
        return Err(ChunkError::UnsupportedVersion(version));
    }
    let network_id   = read_u32be(&mut r)?;
    let start_ledger = read_u32be(&mut r)?;
    let end_ledger   = read_u32be(&mut r)?;
    let checkpoint_hash = read_hash(&mut r)?;
    let stored_chunk_hash = read_hash(&mut r)?;

    // Verify chunk hash covers everything from current position to end
    let body_start = r.position() as usize;
    let actual_hash = sha512half(&data[body_start..]);
    if actual_hash != stored_chunk_hash {
        return Err(ChunkError::HashMismatch {
            expected: hex::encode(stored_chunk_hash),
            got: hex::encode(actual_hash),
        });
    }

    // Checkpoint
    let checkpoint = read_node_list(&mut r)?;

    let delta_count = (end_ledger - start_ledger) as usize;
    let tx_map_count = delta_count + 1;
    let (deltas, tx_maps) = if version == FORMAT_VERSION_STREAMED {
        // v3: checkpoint, tx_map[start], then (delta, tx_map) pairs for start+1..=end —
        // written this way so the exporter never has to buffer a whole chunk in memory.
        let mut deltas = Vec::with_capacity(delta_count);
        let mut tx_maps = Vec::with_capacity(tx_map_count);
        tx_maps.push(read_tx_map(&mut r)?);
        for _ in 0..delta_count {
            deltas.push(read_delta(&mut r)?);
            tx_maps.push(read_tx_map(&mut r)?);
        }
        (deltas, tx_maps)
    } else {
        // v2: all deltas as one block, then all tx_maps as a second block.
        let mut deltas = Vec::with_capacity(delta_count);
        for _ in 0..delta_count {
            deltas.push(read_delta(&mut r)?);
        }
        let mut tx_maps = Vec::with_capacity(tx_map_count);
        for _ in 0..tx_map_count {
            tx_maps.push(read_tx_map(&mut r)?);
        }
        (deltas, tx_maps)
    };

    // Footer
    let footer = read_exact(&mut r, 4)?;
    if footer != MAGIC_FOOTER {
        return Err(ChunkError::InvalidMagic);
    }

    Ok(Chunk {
        network_id,
        start_ledger,
        end_ledger,
        checkpoint_hash,
        chunk_hash: stored_chunk_hash,
        checkpoint,
        deltas,
        tx_maps,
    })
}

fn read_node(r: &mut impl Read) -> Result<SHAMapNode, ChunkError> {
    let hash      = read_hash(r)?;
    let type_byte = read_u8(r)?;
    let node_type = NodeType::try_from(type_byte)
        .map_err(|_| ChunkError::UnsupportedVersion(type_byte))?;
    let len     = read_u16be(r)? as usize;
    let content = read_exact(r, len)?;
    Ok(SHAMapNode { hash, node_type, content })
}

fn read_node_list(r: &mut impl Read) -> Result<Vec<SHAMapNode>, ChunkError> {
    let count = read_u32be(r)? as usize;
    let mut nodes = Vec::with_capacity(count);
    for _ in 0..count {
        nodes.push(read_node(r)?);
    }
    Ok(nodes)
}

fn read_delta(r: &mut impl Read) -> Result<LedgerDelta, ChunkError> {
    let ledger_seq   = read_u32be(r)?;
    let added_count  = read_u32be(r)? as usize;
    let mut added    = Vec::with_capacity(added_count);
    for _ in 0..added_count {
        added.push(read_node(r)?);
    }
    let deleted_count = read_u32be(r)? as usize;
    let mut deleted   = Vec::with_capacity(deleted_count);
    for _ in 0..deleted_count {
        deleted.push(read_hash(r)?);
    }
    Ok(LedgerDelta {
        ledger_seq,
        diff: SHAMapDiff { added, deleted },
    })
}

fn read_u64be(r: &mut impl Read) -> Result<u64, ChunkError> {
    let b = read_exact(r, 8)?;
    Ok(u64::from_be_bytes(b.try_into().unwrap()))
}

fn read_tx_map(r: &mut impl Read) -> Result<TxMap, ChunkError> {
    let ledger_seq            = read_u32be(r)?;
    let ledger_hash           = read_hash(r)?;
    let account_hash          = read_hash(r)?;
    let drops                 = read_u64be(r)?;
    let parent_close_time     = read_u32be(r)?;
    let close_time            = read_u32be(r)?;
    let close_time_resolution = read_u8(r)?;
    let close_flags           = read_u8(r)?;
    let tx_count    = read_u16be(r)? as usize;
    let mut txns    = Vec::with_capacity(tx_count);
    for _ in 0..tx_count {
        let tx_hash   = read_hash(r)?;
        let tx_len    = read_u32be(r)? as usize;
        let tx_blob   = read_exact(r, tx_len)?;
        let meta_len  = read_u32be(r)? as usize;
        let meta_blob = read_exact(r, meta_len)?;
        txns.push(TxRecord { tx_hash, tx_blob, meta_blob });
    }
    Ok(TxMap {
        ledger_seq, ledger_hash, account_hash, drops,
        parent_close_time, close_time, close_time_resolution, close_flags,
        txns,
    })
}

#[cfg(test)]
mod chunk_writer_tests {
    use super::*;
    use crate::chunk::NETWORK_MAINNET;

    fn node(tag: u8) -> SHAMapNode {
        let mut hash = [0u8; 32];
        hash[31] = tag;
        SHAMapNode {
            hash,
            node_type: NodeType::AccountState,
            content: vec![tag; 5],
        }
    }

    fn tx_map(seq: u32) -> TxMap {
        TxMap {
            ledger_seq: seq,
            ledger_hash: [seq as u8; 32],
            account_hash: [(seq + 1) as u8; 32],
            drops: 100_000_000,
            parent_close_time: 1000,
            close_time: 1004,
            close_time_resolution: 10,
            close_flags: 0,
            txns: vec![TxRecord {
                tx_hash: [(seq + 2) as u8; 32],
                tx_blob: vec![0xAA, 0xBB],
                meta_blob: vec![0xCC],
            }],
        }
    }

    #[test]
    fn streamed_chunk_round_trips_through_deserialize_chunk() {
        let dir = std::env::temp_dir().join(format!(
            "xrla_chunkwriter_test_{}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        let final_path = dir.join("test_chunk.xrla");

        let n0 = node(1);
        let n1 = node(2);
        let n2 = node(3);
        let checkpoint_hash = [0x11u8; 32];

        let mut writer =
            ChunkWriter::create(final_path.clone(), NETWORK_MAINNET, 100, 102, checkpoint_hash)
                .unwrap();
        {
            let mut refs = vec![&n1, &n0];
            writer.write_checkpoint(&mut refs).unwrap();
        }
        writer.write_tx_map(&tx_map(100)).unwrap();
        writer
            .write_delta(&LedgerDelta {
                ledger_seq: 101,
                diff: SHAMapDiff {
                    added: vec![n2.clone()],
                    deleted: vec![n0.hash],
                },
            })
            .unwrap();
        writer.write_tx_map(&tx_map(101)).unwrap();
        writer
            .write_delta(&LedgerDelta {
                ledger_seq: 102,
                diff: SHAMapDiff { added: vec![], deleted: vec![] },
            })
            .unwrap();
        writer.write_tx_map(&tx_map(102)).unwrap();
        let returned_hash = writer.finish().unwrap();

        assert!(!final_path.with_extension("xrla.tmp").exists());
        assert!(final_path.exists());

        let bytes = fs::read(&final_path).unwrap();
        let chunk = deserialize_chunk(&bytes).unwrap();

        assert_eq!(chunk.network_id, NETWORK_MAINNET);
        assert_eq!(chunk.start_ledger, 100);
        assert_eq!(chunk.end_ledger, 102);
        assert_eq!(chunk.checkpoint_hash, checkpoint_hash);
        assert_eq!(chunk.chunk_hash, returned_hash);
        assert_eq!(chunk.checkpoint.len(), 2);
        // deserialize_chunk verifies chunk_hash internally already; sanity-check it matches.
        assert_eq!(chunk.deltas.len(), 2);
        assert_eq!(chunk.tx_maps.len(), 3);
        assert_eq!(chunk.deltas[0].ledger_seq, 101);
        assert_eq!(chunk.deltas[0].diff.added[0].hash, n2.hash);
        assert_eq!(chunk.deltas[0].diff.deleted[0], n0.hash);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn dropping_writer_without_finish_removes_tmp_file() {
        let dir = std::env::temp_dir().join(format!(
            "xrla_chunkwriter_droptest_{}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        let final_path = dir.join("abandoned.xrla");

        {
            let _writer =
                ChunkWriter::create(final_path.clone(), NETWORK_MAINNET, 1, 1, [0u8; 32]).unwrap();
            // dropped without calling finish()
        }

        assert!(!final_path.exists());
        assert!(!final_path.with_extension("xrla.tmp").exists());

        fs::remove_dir_all(&dir).ok();
    }
}
