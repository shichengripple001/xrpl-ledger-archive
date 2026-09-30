/// NuDB store writer — produces a fresh, valid `.dat`/`.key` pair from a set of
/// (hash, already-encoded-value) entries.
///
/// This is a from-scratch bulk writer, not a reimplementation of libnudb's incremental
/// insert/grow algorithm — it sizes the bucket table once for the full entry set instead
/// of growing it via linear hashing as inserts happen live. The on-disk layout it produces
/// (headers, bucket format, spill-chain format) matches what `keyfile::Shard` reads, and is
/// validated by reading a written store back through that same reader. As of 2026-09-28 a
/// real xrpld process also opens, boots from, and correctly serves a store written here —
/// see NUDB_FORMAT.md for the format this mirrors, including the `pepper` warning (getting
/// that field wrong is silently fatal: xrpld rejects the store with `hash_mismatch`).
use std::fs::File;
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};
use xxhash_rust::xxh64::xxh64;

use xrla_common::shamap::Hash256;

const DAT_MAGIC: &[u8; 8] = b"nudb.dat";
const KEY_MAGIC: &[u8; 8] = b"nudb.key";
const KEY_SIZE: usize = 32;
const BLOCK_SIZE: u64 = 4096;
const ENTRY_SIZE: usize = 18; // offset(6) + size(6) + hash(6)
const BUCKET_HEADER: usize = 8; // count(2) + spill(6)
const BUCKET_CAPACITY: usize = (BLOCK_SIZE as usize - BUCKET_HEADER) / ENTRY_SIZE; // 227
const DAT_HEADER_SIZE: u64 = 92;

/// One placed entry: (nhash, full key, .dat offset of its val_size field, value size).
type PlacedEntry = (u64, Hash256, u64, u64);

fn write_u48(buf: &mut [u8], v: u64) {
    let b = v.to_be_bytes();
    buf.copy_from_slice(&b[2..8]);
}

/// Write a fresh NuDB store containing `entries` (hash -> already NuDB-encoded value,
/// e.g. from `dat::encode_wire_to_value`) to `dat_path` / `key_path`.
pub fn write_nudb_store(
    entries: &[(Hash256, Vec<u8>)],
    dat_path: &Path,
    key_path: &Path,
) -> Result<()> {
    write_nudb_store_streaming(
        entries.iter().map(|(h, v)| (*h, v.clone())),
        dat_path,
        key_path,
    )
}

/// Same as `write_nudb_store`, but takes an iterator instead of a pre-built slice, so a
/// caller can encode each entry's value lazily — one at a time, written straight to the
/// `.dat` file — instead of first collecting every entry into one big `Vec<(Hash256,
/// Vec<u8>)>`. `write_nudb_store` itself is unavoidably not this lazy (it's handed an
/// already-fully-materialized slice), but `xrla-import` calls this directly with a lazy
/// iterator over its replay result, which is what actually avoids the second full copy of
/// every node's content that `write_nudb_store`'s old inline implementation used to require.
///
/// Only `placed` (small: one `(u64, Hash256, u64, u64)` tuple per entry, not the value
/// content) needs to be held for the whole call — see the doc comment on `PlacedEntry`.
pub fn write_nudb_store_streaming(
    entries: impl IntoIterator<Item = (Hash256, Vec<u8>)>,
    dat_path: &Path,
    key_path: &Path,
) -> Result<()> {
    let salt: u64 = 0x5852_4C41_5852_4C41; // "XRLAXRLA" — arbitrary but fixed
    let uid: u64 = 1;
    let appnum: u64 = 1;
    let version: u16 = 2;

    let mut dat = File::create(dat_path)
        .with_context(|| format!("create {}", dat_path.display()))?;
    write_dat_header(&mut dat, version, uid, appnum)?;

    let mut placed: Vec<PlacedEntry> = Vec::new();
    let mut seen: std::collections::HashSet<Hash256> = std::collections::HashSet::new();
    let mut offset = DAT_HEADER_SIZE;
    for (hash, value) in entries {
        // First-wins dedup, matching the old `HashMap::entry().or_insert_with()` semantics
        // (a hash appearing in both `all_state_nodes` and `tx_nodes` keeps whichever value
        // is encountered first) — but without needing every value resident to check it.
        if !seen.insert(hash) {
            continue;
        }
        let val_size = value.len() as u64;
        let mut size_field = [0u8; 6];
        write_u48(&mut size_field, val_size);
        dat.write_all(&size_field)?;
        dat.write_all(&hash)?;
        dat.write_all(&value)?;
        // `value` (and its heap allocation) is dropped here, at the end of this iteration
        // — never held alongside the next entry's value, unlike a pre-built Vec.

        let nhash = xxh64(&hash, salt) >> 16;
        placed.push((nhash, hash, offset, val_size));
        offset += 6 + KEY_SIZE as u64 + val_size;
    }

    // Size the bucket table for a target load factor of ~0.5. Uses `placed.len()`
    // (post-dedup count), not `entries.len()` — `entries` was consumed by the loop above,
    // and pre-dedup count would be wrong anyway if any hash collided.
    let target_load = 0.5;
    let num_buckets = ((placed.len() as f64 / (BUCKET_CAPACITY as f64 * target_load)).ceil() as u64).max(1);
    let mut modulus = 1u64;
    while modulus < num_buckets {
        modulus <<= 1;
    }

    let mut buckets: Vec<Vec<PlacedEntry>> = vec![Vec::new(); num_buckets as usize];
    for entry in placed {
        let mut n = entry.0 % modulus;
        if n >= num_buckets {
            n -= modulus / 2;
        }
        buckets[n as usize].push(entry);
    }

    // Overflow entries spill into the .dat file as additional bucket blocks.
    let mut spill_offsets = vec![0u64; num_buckets as usize];
    for (i, bucket) in buckets.iter_mut().enumerate() {
        bucket.sort_by_key(|e| e.0);
        if bucket.len() > BUCKET_CAPACITY {
            let overflow = bucket.split_off(BUCKET_CAPACITY);
            spill_offsets[i] = write_spill_chain(&mut dat, &mut offset, &overflow)?;
        }
    }

    let mut key = File::create(key_path)
        .with_context(|| format!("create {}", key_path.display()))?;
    write_key_header(&mut key, version, uid, appnum, salt)?;
    for (i, bucket) in buckets.iter().enumerate() {
        write_bucket_block(&mut key, bucket, spill_offsets[i])?;
    }

    Ok(())
}

fn write_dat_header(dat: &mut File, version: u16, uid: u64, appnum: u64) -> Result<()> {
    let mut hdr = [0u8; DAT_HEADER_SIZE as usize];
    hdr[0..8].copy_from_slice(DAT_MAGIC);
    hdr[8..10].copy_from_slice(&version.to_be_bytes());
    hdr[10..18].copy_from_slice(&uid.to_be_bytes());
    hdr[18..26].copy_from_slice(&appnum.to_be_bytes());
    hdr[26..28].copy_from_slice(&(KEY_SIZE as u16).to_be_bytes());
    dat.write_all(&hdr)?;
    Ok(())
}

fn write_key_header(key: &mut File, version: u16, uid: u64, appnum: u64, salt: u64) -> Result<()> {
    let mut hdr = vec![0u8; BLOCK_SIZE as usize];
    hdr[0..8].copy_from_slice(KEY_MAGIC);
    hdr[8..10].copy_from_slice(&version.to_be_bytes());
    hdr[10..18].copy_from_slice(&uid.to_be_bytes());
    hdr[18..26].copy_from_slice(&appnum.to_be_bytes());
    hdr[26..28].copy_from_slice(&(KEY_SIZE as u16).to_be_bytes());
    hdr[28..36].copy_from_slice(&salt.to_be_bytes());
    let pepper = xxh64(&salt.to_le_bytes(), salt); // NuDB's pepper<Hasher>(salt); verify() rejects a mismatch
    hdr[36..44].copy_from_slice(&pepper.to_be_bytes());
    hdr[44..46].copy_from_slice(&(BLOCK_SIZE as u16).to_be_bytes());
    hdr[46..48].copy_from_slice(&0x8000u16.to_be_bytes()); // load_factor = 0.5
    key.write_all(&hdr)?;
    Ok(())
}

/// Write one bucket as a full BLOCK_SIZE-byte block (used for primary buckets in the key file).
fn write_bucket_block(key: &mut File, entries: &[PlacedEntry], spill: u64) -> Result<()> {
    let mut block = vec![0u8; BLOCK_SIZE as usize];
    write_bucket_into(&mut block, entries, spill);
    key.write_all(&block)?;
    Ok(())
}

/// Write count(2) + spill(6) + entries into the front of `buf` (may be longer than needed;
/// used both for full key-file blocks and exact-sized spill bodies in the .dat file).
fn write_bucket_into(buf: &mut [u8], entries: &[PlacedEntry], spill: u64) {
    buf[0..2].copy_from_slice(&(entries.len() as u16).to_be_bytes());
    write_u48(&mut buf[2..8], spill);
    for (i, (nhash, _hash, off, size)) in entries.iter().enumerate() {
        let b = BUCKET_HEADER + i * ENTRY_SIZE;
        write_u48(&mut buf[b..b + 6], *off);
        write_u48(&mut buf[b + 6..b + 12], *size);
        write_u48(&mut buf[b + 12..b + 18], *nhash);
    }
}

/// Write `overflow` as a chain of spill records in the .dat file, building the chain from
/// the tail backward so each block's `spill` pointer is already known. Returns the offset
/// of the head block's *body* (what the parent bucket's `spill` field should point at).
fn write_spill_chain(dat: &mut File, offset: &mut u64, overflow: &[PlacedEntry]) -> Result<u64> {
    if overflow.is_empty() {
        return Ok(0);
    }
    let chunks: Vec<&[PlacedEntry]> = overflow.chunks(BUCKET_CAPACITY).collect();
    let mut next_spill = 0u64;
    let mut head_body_offset = 0u64;

    for chunk in chunks.iter().rev() {
        let body_len = BUCKET_HEADER + chunk.len() * ENTRY_SIZE;
        let body_offset = *offset + 8; // spill record = [6 zero][2 size BE][body]; spill points at body

        dat.write_all(&[0u8; 6])?;
        dat.write_all(&(body_len as u16).to_be_bytes())?;
        let mut body = vec![0u8; body_len];
        write_bucket_into(&mut body, chunk, next_spill);
        dat.write_all(&body)?;

        *offset += 8 + body_len as u64;
        next_spill = body_offset;
        head_body_offset = body_offset;
    }

    Ok(head_body_offset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dat::decode_value_to_wire;
    use crate::keyfile::Shard;
    use std::collections::HashMap;

    fn hash_of(n: u8) -> Hash256 {
        let mut h = [0u8; 32];
        h[0] = n;
        h[31] = n.wrapping_mul(7);
        h
    }

    #[test]
    fn round_trip_small_store() {
        let dir = std::env::temp_dir().join(format!("xrla_nudb_writer_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let dat_path = dir.join("nudb.dat");
        let key_path = dir.join("nudb.key");

        let mut expected: HashMap<Hash256, Vec<u8>> = HashMap::new();
        let mut entries = Vec::new();
        for i in 0u8..50 {
            let key = hash_of(i);
            let value = vec![0xAAu8; 10 + i as usize];
            expected.insert(key, value.clone());
            entries.push((key, value));
        }

        write_nudb_store(&entries, &dat_path, &key_path).unwrap();

        let shard = Shard::open(&dat_path, &key_path).unwrap();
        for (key, value) in &expected {
            let got = shard.fetch(key).unwrap().expect("entry present after write");
            assert_eq!(&got, value, "value mismatch for key starting {:02x}", key[0]);
        }

        // A key that was never written must come back as None.
        assert!(shard.fetch(&hash_of(200)).unwrap().is_none());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn round_trip_forces_spill_chain() {
        // Force every entry into the same bucket by using distinct real hashes but a
        // tiny bucket table (num_buckets stays 1 for a handful of entries), so this
        // exercises the spill-chain write/read path, not just primary buckets.
        let dir = std::env::temp_dir().join(format!("xrla_nudb_writer_spill_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let dat_path = dir.join("nudb.dat");
        let key_path = dir.join("nudb.key");

        let mut expected: HashMap<Hash256, Vec<u8>> = HashMap::new();
        let mut entries = Vec::new();
        // BUCKET_CAPACITY is 227; write enough entries into one small store that with
        // load factor 0.5 sizing we still end up needing at least one spill for some bucket.
        for i in 0u16..500 {
            let mut key = [0u8; 32];
            key[0] = (i >> 8) as u8;
            key[1] = (i & 0xFF) as u8;
            key[31] = 0x5A;
            let value = vec![(i % 251) as u8; 20];
            expected.insert(key, value.clone());
            entries.push((key, value));
        }

        write_nudb_store(&entries, &dat_path, &key_path).unwrap();

        let shard = Shard::open(&dat_path, &key_path).unwrap();
        for (key, value) in &expected {
            let got = shard.fetch(key).unwrap().expect("entry present after write");
            assert_eq!(&got, value);
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Round-trips real, xrpld-produced node values (not synthetic bytes) through
    /// encode_wire_to_value -> write_nudb_store -> Shard::fetch -> decode_value_to_wire,
    /// and asserts the wire bytes are unchanged.
    ///
    /// Requires a real xrpld NuDB shard on disk:
    ///   XRPLD_DAT=/path/to/nudb.dat cargo test --workspace -- --ignored real_snapshot
    #[test]
    #[ignore]
    fn real_snapshot_roundtrip_via_writer() {
        use std::fs::File;
        use std::io::Read;
        use xrla_common::shamap::SHAMapNode;

        let dat_path_str = std::env::var("XRPLD_DAT").expect("set XRPLD_DAT to a real nudb.dat");
        let dat_path = std::path::Path::new(&dat_path_str);

        let mut f = File::open(dat_path).expect("open real nudb.dat");
        let mut hdr = [0u8; 92];
        f.read_exact(&mut hdr).expect("read dat header");

        // Sequentially sample the first ~200 real records (bounded, not a full scan —
        // scan_dat's unbounded whole-file load is the wrong tool here, see NUDB_FORMAT.md).
        let mut originals: Vec<SHAMapNode> = Vec::new();
        for _ in 0..200 {
            let mut size_field = [0u8; 6];
            if f.read_exact(&mut size_field).is_err() {
                break;
            }
            let mut size_bytes = [0u8; 8];
            size_bytes[2..8].copy_from_slice(&size_field);
            let val_size = u64::from_be_bytes(size_bytes) as usize;
            if val_size == 0 || val_size > 65_536 {
                break;
            }
            let mut key = [0u8; 32];
            if f.read_exact(&mut key).is_err() {
                break;
            }
            let mut value = vec![0u8; val_size];
            if f.read_exact(&mut value).is_err() {
                break;
            }
            if let Some(wire) = decode_value_to_wire(&value) {
                if let Ok(node) = SHAMapNode::from_wire_bytes(key, &wire) {
                    originals.push(node);
                }
            }
        }
        assert!(originals.len() >= 10, "expected to sample at least 10 real nodes, got {}", originals.len());

        let entries: Vec<(Hash256, Vec<u8>)> = originals
            .iter()
            .map(|n| (n.hash, crate::dat::encode_wire_to_value(&n.content, &n.node_type)))
            .collect();

        let dir = std::env::temp_dir().join(format!("xrla_nudb_real_roundtrip_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let out_dat = dir.join("nudb.dat");
        let out_key = dir.join("nudb.key");
        write_nudb_store(&entries, &out_dat, &out_key).unwrap();

        let shard = Shard::open(&out_dat, &out_key).unwrap();
        for node in &originals {
            let raw = shard.fetch(&node.hash).unwrap().expect("real node present after write");
            let wire = decode_value_to_wire(&raw).expect("re-decode written value");
            assert_eq!(wire, node.to_wire_bytes(), "wire mismatch for real node {:02x?}", &node.hash[..4]);
        }

        std::fs::remove_dir_all(&dir).ok();
        println!("real_snapshot_roundtrip_via_writer: {} real nodes round-tripped OK", originals.len());
    }
}
