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
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

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

/// One placed entry: (nhash, .dat offset of its val_size field, value size).
///
/// The full 32-byte key is deliberately NOT kept: bucket blocks and spill records store only
/// the 48-bit `nhash` prefix, offset and size (see `write_bucket_into`), so the key was dead
/// weight — 32 of 56 bytes per entry, ~13 GB across a 150,000-ledger chunk's ~400M nodes.
type PlacedEntry = (u64, u64, u64);

/// Buffer size for the `.dat`/`.key` writers. Unbuffered, `write_node` issued three
/// `write(2)` calls per node; on the 20,000-ledger mainnet import kernel time (502 s) already
/// exceeded user time (469 s).
const IO_BUF: usize = 4 << 20;

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
    let mut sink = NuDbSink::create(dat_path)?;
    for (hash, value) in entries {
        sink.write_node(*hash, value)?;
    }
    sink.finish(key_path)
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
    let mut sink = NuDbSink::create(dat_path)?;
    for (hash, value) in entries {
        sink.write_node(hash, &value)?;
    }
    sink.finish(key_path)
}

/// Incremental NuDB store writer: accepts nodes **one at a time** via `write_node`, writing
/// each straight to the `.dat` file, then `finish` builds the bucket table and writes the
/// `.key` file.
///
/// `write_nudb_store_streaming` pulls an iterator to completion in a single call, which
/// only helps if the caller can *produce* entries lazily. `xrla-import` can't: it discovers
/// nodes progressively as it replays ledgers, and previously had to accumulate them all
/// (`all_state_nodes` + `tx_nodes`) just to hand the writer one iterator at the end — which
/// is content that scales linearly with the chunk's ledger range and was the dominant term
/// in a real 20,000-ledger import's 64 GB peak. Driving this sink directly lets the importer
/// write and then **drop** each node as replay produces it, so nothing proportional to the
/// range length stays resident.
///
/// What this holds for the whole run (all small, none of it node content):
///   - `seen`: one 32-byte hash per unique node, for dedup
///   - `placed`: one `PlacedEntry` (24 bytes) per unique node, needed to size and fill the
///     bucket table in `finish`
/// The `.key` file genuinely cannot be written incrementally — bucket assignment needs the
/// final entry count — but it only needs this metadata, never the values.
pub struct NuDbSink {
    /// `None` only after `finish` has taken and closed it.
    dat: Option<BufWriter<File>>,
    dat_tmp: PathBuf,
    dat_final: PathBuf,
    finished: bool,
    offset: u64,
    salt: u64,
    version: u16,
    uid: u64,
    appnum: u64,
    seen: std::collections::HashSet<Hash256>,
    placed: Vec<PlacedEntry>,
}

/// `<path>.tmp`, in the same directory so the final `rename` never crosses a filesystem.
fn tmp_path(path: &Path) -> PathBuf {
    let mut os = path.as_os_str().to_owned();
    os.push(".tmp");
    PathBuf::from(os)
}

impl NuDbSink {
    /// Starts a new store. **Nothing at `dat_path` is touched until `finish` succeeds**: data
    /// goes to `<dat_path>.tmp`, and dropping the sink early (verification failure, bad chunk,
    /// crash) just deletes the temp file. Creating straight over `dat_path` would truncate an
    /// existing store before a single chunk had been read — the same delete-then-recreate
    /// mistake `write_ledger_db` had — and leave its `.key` orphaned against an empty `.dat`.
    pub fn create(dat_path: &Path) -> Result<Self> {
        let salt: u64 = 0x5852_4C41_5852_4C41; // "XRLAXRLA" — arbitrary but fixed
        let uid: u64 = 1;
        let appnum: u64 = 1;
        let version: u16 = 2;

        let dat_tmp = tmp_path(dat_path);
        let file = File::create(&dat_tmp)
            .with_context(|| format!("create {}", dat_tmp.display()))?;
        let mut dat = BufWriter::with_capacity(IO_BUF, file);
        write_dat_header(&mut dat, version, uid, appnum)?;

        Ok(Self {
            dat: Some(dat),
            dat_tmp,
            dat_final: dat_path.to_path_buf(),
            finished: false,
            offset: DAT_HEADER_SIZE,
            salt,
            version,
            uid,
            appnum,
            seen: std::collections::HashSet::new(),
            placed: Vec::new(),
        })
    }

    /// True if a node with this hash has already been written. Lets a caller skip *encoding*
    /// a duplicate — worthwhile on multi-chunk imports, where every later chunk's checkpoint
    /// repeats ~28M nodes already written by the previous one.
    pub fn contains(&self, hash: &Hash256) -> bool {
        self.seen.contains(hash)
    }

    /// Write one node's already-NuDB-encoded value (e.g. from `dat::encode_wire_to_value`)
    /// to the `.dat` file. Duplicate hashes are skipped (first write wins, matching the
    /// `HashMap::entry().or_insert_with()` semantics the old buffered path had), so a caller
    /// may pass the same node more than once — across ledgers, or across separate chunk
    /// files in one invocation — without pre-deduping.
    pub fn write_node(&mut self, hash: Hash256, value: &[u8]) -> Result<()> {
        if !self.seen.insert(hash) {
            return Ok(());
        }
        let dat = self.dat.as_mut().expect("write_node after finish");
        let val_size = value.len() as u64;
        let mut size_field = [0u8; 6];
        write_u48(&mut size_field, val_size);
        dat.write_all(&size_field)?;
        dat.write_all(&hash)?;
        dat.write_all(value)?;

        let nhash = xxh64(&hash, self.salt) >> 16;
        self.placed.push((nhash, self.offset, val_size));
        self.offset += 6 + KEY_SIZE as u64 + val_size;
        Ok(())
    }

    /// Number of unique nodes written so far.
    pub fn node_count(&self) -> usize {
        self.placed.len()
    }

    /// Build the bucket table from the accumulated metadata, write any spill chains into the
    /// `.dat` file, write the `.key` file, then move both into place.
    ///
    /// Both files are written to `.tmp` names and only renamed over the real paths at the very
    /// end (renames are instant metadata swaps, not copies). Two renames can't be jointly
    /// atomic, so the old `.key` is moved aside first: a crash mid-swap leaves no `.key`
    /// (fails loudly on open, old key kept as `.bak`) rather than a new `.dat` silently paired
    /// with an old `.key`.
    pub fn finish(mut self, key_path: &Path) -> Result<()> {
        // Size the bucket table for a target load factor of ~0.5, from the post-dedup count.
        let target_load = 0.5;
        let num_buckets =
            ((self.placed.len() as f64 / (BUCKET_CAPACITY as f64 * target_load)).ceil() as u64).max(1);
        let mut modulus = 1u64;
        while modulus < num_buckets {
            modulus <<= 1;
        }

        let mut buckets: Vec<Vec<PlacedEntry>> = vec![Vec::new(); num_buckets as usize];
        for entry in std::mem::take(&mut self.placed) {
            let mut n = entry.0 % modulus;
            if n >= num_buckets {
                n -= modulus / 2;
            }
            buckets[n as usize].push(entry);
        }

        // Overflow entries spill into the .dat file as additional bucket blocks.
        let mut dat = self.dat.take().expect("finish called twice");
        let mut spill_offsets = vec![0u64; num_buckets as usize];
        for (i, bucket) in buckets.iter_mut().enumerate() {
            bucket.sort_by_key(|e| e.0);
            if bucket.len() > BUCKET_CAPACITY {
                let overflow = bucket.split_off(BUCKET_CAPACITY);
                spill_offsets[i] = write_spill_chain(&mut dat, &mut self.offset, &overflow)?;
            }
        }
        dat.flush()?;
        drop(dat); // close before rename (required on Windows, harmless elsewhere)

        let key_tmp = tmp_path(key_path);
        let result = (|| -> Result<()> {
            let key_file = File::create(&key_tmp)
                .with_context(|| format!("create {}", key_tmp.display()))?;
            let mut key = BufWriter::with_capacity(IO_BUF, key_file);
            write_key_header(&mut key, self.version, self.uid, self.appnum, self.salt)?;
            for (i, bucket) in buckets.iter().enumerate() {
                write_bucket_block(&mut key, bucket, spill_offsets[i])?;
            }
            key.flush()?;
            drop(key);

            // Swapping two files can't be one atomic step. Order it so that no crash can leave
            // a NEW .dat paired with an OLD .key (bucket offsets pointing into the wrong data —
            // wrong answers served silently). Instead, move the old key aside first: a crash
            // then leaves "no .key", which xrpld refuses to open loudly, and the old key is
            // still on disk as `.bak` (and the new one as `.tmp`), so nothing is lost.
            let key_bak = {
                let mut os = key_path.as_os_str().to_owned();
                os.push(".bak");
                PathBuf::from(os)
            };
            let had_old_key = key_path.exists();
            if had_old_key {
                fs::rename(key_path, &key_bak)
                    .with_context(|| format!("move old key aside to {}", key_bak.display()))?;
            }
            let swapped = fs::rename(&self.dat_tmp, &self.dat_final)
                .with_context(|| format!("rename {} -> {}", self.dat_tmp.display(), self.dat_final.display()))
                .and_then(|_| {
                    fs::rename(&key_tmp, key_path)
                        .with_context(|| format!("rename {} -> {}", key_tmp.display(), key_path.display()))
                });
            if let Err(e) = swapped {
                if had_old_key && !key_path.exists() {
                    let _ = fs::rename(&key_bak, key_path); // put the old key back
                }
                return Err(e);
            }
            if had_old_key {
                let _ = fs::remove_file(&key_bak);
            }
            Ok(())
        })();

        if result.is_err() {
            let _ = fs::remove_file(&key_tmp);
        }
        self.finished = result.is_ok();
        result
    }
}

impl Drop for NuDbSink {
    fn drop(&mut self) {
        if !self.finished {
            self.dat.take(); // close the handle first
            let _ = fs::remove_file(&self.dat_tmp);
        }
    }
}

fn write_dat_header(dat: &mut impl Write, version: u16, uid: u64, appnum: u64) -> Result<()> {
    let mut hdr = [0u8; DAT_HEADER_SIZE as usize];
    hdr[0..8].copy_from_slice(DAT_MAGIC);
    hdr[8..10].copy_from_slice(&version.to_be_bytes());
    hdr[10..18].copy_from_slice(&uid.to_be_bytes());
    hdr[18..26].copy_from_slice(&appnum.to_be_bytes());
    hdr[26..28].copy_from_slice(&(KEY_SIZE as u16).to_be_bytes());
    dat.write_all(&hdr)?;
    Ok(())
}

fn write_key_header(key: &mut impl Write, version: u16, uid: u64, appnum: u64, salt: u64) -> Result<()> {
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
fn write_bucket_block(key: &mut impl Write, entries: &[PlacedEntry], spill: u64) -> Result<()> {
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
    for (i, (nhash, off, size)) in entries.iter().enumerate() {
        let b = BUCKET_HEADER + i * ENTRY_SIZE;
        write_u48(&mut buf[b..b + 6], *off);
        write_u48(&mut buf[b + 6..b + 12], *size);
        write_u48(&mut buf[b + 12..b + 18], *nhash);
    }
}

/// Write `overflow` as a chain of spill records in the .dat file, building the chain from
/// the tail backward so each block's `spill` pointer is already known. Returns the offset
/// of the head block's *body* (what the parent bucket's `spill` field should point at).
fn write_spill_chain(dat: &mut impl Write, offset: &mut u64, overflow: &[PlacedEntry]) -> Result<u64> {
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

    /// An aborted import must leave an existing store exactly as it was. The first version of
    /// `NuDbSink` truncated the target `.dat` inside `create`, i.e. before any chunk was even
    /// read, so a bad path or a tampered chunk destroyed a good store (45 bytes -> an empty
    /// 92-byte header, `.key` orphaned). Dropping the sink un-finished must now be a no-op.
    #[test]
    fn aborted_sink_leaves_existing_store_untouched() {
        let dir = std::env::temp_dir().join(format!("xrla_sink_abort_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let dat = dir.join("nudb.dat");
        let key = dir.join("nudb.key");
        std::fs::write(&dat, b"EXISTING DAT").unwrap();
        std::fs::write(&key, b"EXISTING KEY").unwrap();

        {
            let mut sink = NuDbSink::create(&dat).unwrap();
            sink.write_node(hash_of(1), b"some value").unwrap();
            // dropped without finish(): simulates verification failing mid-import
        }

        assert_eq!(std::fs::read(&dat).unwrap(), b"EXISTING DAT");
        assert_eq!(std::fs::read(&key).unwrap(), b"EXISTING KEY");
        assert!(!tmp_path(&dat).exists(), "temp .dat must be cleaned up");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The other half: a sink that *is* finished replaces the old store with a readable one
    /// and leaves no temp files behind.
    #[test]
    fn finished_sink_replaces_existing_store_and_is_readable() {
        let dir = std::env::temp_dir().join(format!("xrla_sink_finish_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let dat = dir.join("nudb.dat");
        let key = dir.join("nudb.key");
        std::fs::write(&dat, b"OLD").unwrap();
        std::fs::write(&key, b"OLD").unwrap();

        let mut sink = NuDbSink::create(&dat).unwrap();
        let payload: Vec<u8> = (0..40).map(|i| i as u8).collect();
        let value = crate::dat::encode_wire_to_value(&payload, &xrla_common::shamap::NodeType::AccountState);
        assert!(!sink.contains(&hash_of(7)));
        sink.write_node(hash_of(7), &value).unwrap();
        assert!(sink.contains(&hash_of(7)));
        sink.write_node(hash_of(7), &value).unwrap(); // duplicate: skipped
        assert_eq!(sink.node_count(), 1);
        sink.finish(&key).unwrap();

        assert!(!tmp_path(&dat).exists() && !tmp_path(&key).exists(), "no temp files left");
        let shard = Shard::open(&dat, &key).unwrap();
        let raw = shard.fetch(&hash_of(7)).unwrap().expect("node must be readable");
        let wire = decode_value_to_wire(&raw).expect("value must decode");
        assert!(!wire.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }
}
