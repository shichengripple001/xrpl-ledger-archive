use std::io::{Read, Write};

use anyhow::Result;
use sha2::{Digest, Sha512};

use crate::chunk::{
    Chunk, ChunkError, LedgerDelta, TxMap, TxRecord,
    MAGIC_FOOTER, MAGIC_HEADER, FORMAT_VERSION,
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

fn write_bytes(w: &mut impl Write, b: &[u8]) -> Result<()> {
    w.write_all(b)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Node serialization
// ---------------------------------------------------------------------------

fn write_node(w: &mut impl Write, node: &SHAMapNode) -> Result<()> {
    write_bytes(w, &node.hash)?;
    write_u8(w, node.node_type.clone() as u8)?;
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
    if version != FORMAT_VERSION {
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

    // Deltas
    let delta_count = (end_ledger - start_ledger) as usize;
    let mut deltas = Vec::with_capacity(delta_count);
    for _ in 0..delta_count {
        deltas.push(read_delta(&mut r)?);
    }

    // TX maps
    let tx_map_count = (end_ledger - start_ledger + 1) as usize;
    let mut tx_maps = Vec::with_capacity(tx_map_count);
    for _ in 0..tx_map_count {
        tx_maps.push(read_tx_map(&mut r)?);
    }

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
        .map_err(|_| ChunkError::InvalidMagic)?;
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

fn read_tx_map(r: &mut impl Read) -> Result<TxMap, ChunkError> {
    let ledger_seq = read_u32be(r)?;
    let tx_count   = read_u16be(r)? as usize;
    let mut txns   = Vec::with_capacity(tx_count);
    for _ in 0..tx_count {
        let tx_hash   = read_hash(r)?;
        let tx_len    = read_u32be(r)? as usize;
        let tx_blob   = read_exact(r, tx_len)?;
        let meta_len  = read_u32be(r)? as usize;
        let meta_blob = read_exact(r, meta_len)?;
        txns.push(TxRecord { tx_hash, tx_blob, meta_blob });
    }
    Ok(TxMap { ledger_seq, txns })
}
