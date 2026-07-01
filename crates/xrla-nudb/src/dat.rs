/// NuDB .dat file scanner.
///
/// Format details: see NUDB_FORMAT.md
///
/// NuDB source: https://github.com/cppalliance/NuDB
///
/// dat file header (92 bytes):
///   [8]  magic    = "nudb.dat"
///   [2]  version
///   [8]  uid
///   [8]  appnum
///   [2]  key_size = 32
///   [64] reserved (zeros)
///
/// Records start at offset 92 (immediately after header, no padding scan needed).
/// Each record: [6-byte val_size BE (uint48_t)][32-byte key][val_size bytes value]
///
/// FIELD_SIZE=6 is a NuDB library constant (uint48_t) — not a per-DB setting,
/// not stored in the header. All NuDB databases use 6-byte size/offset fields.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use anyhow::{bail, Result};

use xrla_common::shamap::{wire_type, Hash256, NodeType};

const NUDB_MAGIC: &[u8; 8] = b"nudb.dat";
const KEY_SIZE: usize = 32;
const FIELD_SIZE: usize = 6; // uint48_t — NuDB library constant, same for all NuDB databases
const HEADER_SIZE: u64 = 92; // 8+2+8+8+2+64 — records start immediately after

// Value codec types (value[0]) — see codec.h
const CODEC_RAW: u8 = 0x00;
const CODEC_LZ4: u8 = 0x01;
const CODEC_SPARSE_INNER: u8 = 0x02;
const CODEC_FULL_INNER: u8 = 0x03;

// NodeObjectType values stored in EncodedBlob at decoded[8] — see NodeObject.h
const NOTYPE_UNKNOWN: u8 = 0; // inner nodes
const NOTYPE_ACCOUNT: u8 = 3;
const NOTYPE_TRANSACTION: u8 = 4;

/// Scan the entire NuDB .dat file and return a map of hash → wire bytes.
///
/// Wire bytes = content + trailing XRLA type byte (same format as SHAMapNode::to_wire_bytes()).
/// This is the PoC approach: O(file size) scan, loads all nodes into memory.
/// For production, use key-file lookups instead.
pub fn scan_dat(path: &Path) -> Result<HashMap<Hash256, Vec<u8>>> {
    let file = File::open(path)?;
    let file_size = file.metadata()?.len();
    let mut r = BufReader::with_capacity(64 * 1024 * 1024, file);

    // Validate magic (8 bytes)
    let mut magic = [0u8; 8];
    r.read_exact(&mut magic)?;
    if &magic != NUDB_MAGIC {
        bail!(
            "invalid NuDB magic: expected {:?}, got {:?}",
            String::from_utf8_lossy(NUDB_MAGIC),
            String::from_utf8_lossy(&magic)
        );
    }

    // dat header: version(2) + uid(8) + appnum(8) + key_size(2) + reserved(64) = 84 bytes
    let mut hdr = [0u8; 84];
    r.read_exact(&mut hdr)?;
    // key_size at header offset 18-19 (file offset 26-27)
    let key_size = u16::from_be_bytes([hdr[18], hdr[19]]);
    if key_size != KEY_SIZE as u16 {
        bail!("expected key_size=32, got {}", key_size);
    }

    // Records start immediately at HEADER_SIZE (92) — no padding scan needed
    r.seek(SeekFrom::Start(HEADER_SIZE))?;

    let mut map: HashMap<Hash256, Vec<u8>> = HashMap::new();
    let mut n_decoded = 0u64;
    let mut n_skipped = 0u64;
    let mut last_report = 0u64;
    let mut record_pos = HEADER_SIZE;

    loop {
        // Read 6-byte val_size (big-endian u48)
        let mut field = [0u8; FIELD_SIZE];
        match r.read_exact(&mut field) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }

        let mut size_bytes = [0u8; 8];
        size_bytes[8 - FIELD_SIZE..].copy_from_slice(&field);
        let val_size = u64::from_be_bytes(size_bytes) as usize;

        if val_size == 0 {
            // Zero val_size — either end-of-block padding or a misalignment.
            // NuDB dat file records are sequential; zeros only appear at EOF.
            eprintln!("warn: val_size=0 at file offset {record_pos} (field bytes: {:02x?}) — stopping scan", field);
            break;
        }
        if val_size > 65_536 {
            eprintln!(
                "warn: large val_size={val_size} at file offset {record_pos} \
                 (field bytes: {:02x?}) — stopping scan",
                field
            );
            break;
        }

        // Read key (32 bytes)
        let mut key = [0u8; KEY_SIZE];
        match r.read_exact(&mut key) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }

        // Read value (val_size bytes)
        let mut value = vec![0u8; val_size];
        match r.read_exact(&mut value) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }

        // Decode NuDB value → wire bytes, insert into map
        match nudb_value_to_wire_at(&value, record_pos) {
            Some(wire) => {
                map.insert(key, wire);
                n_decoded += 1;
            }
            None => {
                n_skipped += 1;
            }
        }
        record_pos = record_pos + FIELD_SIZE as u64 + KEY_SIZE as u64 + val_size as u64;

        // Progress report every 1M records
        let total = n_decoded + n_skipped;
        if total - last_report >= 1_000_000 {
            let pos = r.stream_position().unwrap_or(0);
            eprintln!(
                "  scan_dat: {n_decoded} nodes, {n_skipped} skipped \
                 ({:.1}% of {:.1} GB)",
                100.0 * pos as f64 / file_size as f64,
                file_size as f64 / 1e9
            );
            last_report = total;
        }
    }

    eprintln!(
        "scan_dat: loaded {} nodes ({} skipped)",
        n_decoded, n_skipped
    );
    Ok(map)
}

/// Decode a NuDB stored value into SHAMap wire bytes (content + trailing type byte).
///
/// This is the rippled-side codec: the NuDB *value* layout (codec byte + EncodedBlob).
/// Used by key-file lookups (see keyfile.rs) which read the raw value from the .dat file.
/// Returns None for ledger objects / unrecognised codecs (not part of the account SHAMap).
pub fn decode_value_to_wire(value: &[u8]) -> Option<Vec<u8>> {
    nudb_value_to_wire_at(value, 0)
}

/// Decode a NuDB compressed value into SHAMap wire bytes (content + trailing type byte).
/// Returns None for unrecognised codecs or malformed records (logged to stderr).
fn nudb_value_to_wire_at(value: &[u8], pos: u64) -> Option<Vec<u8>> {
    if value.is_empty() {
        return None;
    }
    match value[0] {
        CODEC_FULL_INNER => decode_full_inner(value),
        CODEC_SPARSE_INNER => decode_sparse_inner(value),
        CODEC_LZ4 => decode_lz4(value),
        CODEC_RAW => decode_raw(value),
        other => {
            eprintln!("warn: unknown NuDB codec byte 0x{other:02x} at file offset {pos} (val_size={})", value.len());
            None
        }
    }
}

/// Codec 0x03: full inner node — value = [0x03][512 child hashes]
fn decode_full_inner(value: &[u8]) -> Option<Vec<u8>> {
    if value.len() != 1 + 512 {
        eprintln!(
            "warn: codec 3 wrong length {} (expected 513)",
            value.len()
        );
        return None;
    }
    let mut wire = value[1..].to_vec();
    wire.push(wire_type::INNER);
    Some(wire)
}

/// Codec 0x02: sparse inner node — value = [0x02][u16 mask BE][N×32 hashes]
///
/// Branch mask uses big-endian bit numbering: branch slot `s` (0..15) is present iff
/// `mask & (0x8000 >> s)`, and present hashes are packed in ascending slot order.
/// This matches rippled NodeStore codec.h nodeobject_decompress (`bit = 0x8000; bit >>= 1`).
/// NOTE: NOT `mask & (1 << s)` — the bits are reversed relative to slot index.
fn decode_sparse_inner(value: &[u8]) -> Option<Vec<u8>> {
    if value.len() < 3 {
        return None;
    }
    let mask = u16::from_be_bytes([value[1], value[2]]);
    let n_children = mask.count_ones() as usize;
    let expected_len = 3 + n_children * 32;
    if value.len() != expected_len {
        eprintln!(
            "warn: codec 2 length {} expected {} (mask=0x{mask:04x})",
            value.len(),
            expected_len
        );
        return None;
    }

    // Expand to full inner: 512 bytes, slot s at offset s*32.
    // Big-endian bit order: slot 0 is the most-significant bit (0x8000).
    let mut content = vec![0u8; 512];
    let mut src = 0usize;
    for slot in 0..16usize {
        if mask & (0x8000u16 >> slot) != 0 {
            let src_off = 3 + src * 32;
            content[slot * 32..(slot + 1) * 32].copy_from_slice(&value[src_off..src_off + 32]);
            src += 1;
        }
    }
    content.push(wire_type::INNER);
    Some(content)
}

/// Codec 0x01: LZ4 — value = [0x01][varint orig_size][lz4 block data]
fn decode_lz4(value: &[u8]) -> Option<Vec<u8>> {
    let (orig_size, vlen) = read_varint(&value[1..])?;
    let lz4_data = &value[1 + vlen..];
    let decoded = lz4_flex::decompress(lz4_data, orig_size as usize)
        .map_err(|e| eprintln!("warn: lz4 decompress failed: {e}"))
        .ok()?;
    encoded_blob_to_wire(&decoded)
}

/// Codec 0x00: raw — value = [0x00][EncodedBlob data]
fn decode_raw(value: &[u8]) -> Option<Vec<u8>> {
    encoded_blob_to_wire(&value[1..])
}

/// Convert an EncodedBlob (from rippled) to XRLA wire bytes.
///
/// EncodedBlob layout: [8 zeros][NodeObjectType (1 byte)][payload...]
/// Wire bytes: [payload][XRLA type byte]
fn encoded_blob_to_wire(data: &[u8]) -> Option<Vec<u8>> {
    if data.len() < 9 {
        eprintln!("warn: encoded blob too short: {} bytes", data.len());
        return None;
    }
    let notype = data[8];
    match notype {
        NOTYPE_UNKNOWN => {
            // Inner node in raw/lz4 form. The decompressed inner node EncodedBlob is
            // 525 bytes: [8 zeros][0][HashPrefix::InnerNode (4 bytes)][512 hashes]
            // XRLA content = data[13..525]
            if data.len() < 525 {
                eprintln!(
                    "warn: inner encoded blob too short: {} bytes",
                    data.len()
                );
                return None;
            }
            let mut wire = data[13..525].to_vec();
            wire.push(wire_type::INNER);
            Some(wire)
        }
        NOTYPE_ACCOUNT => {
            let mut wire = data[9..].to_vec();
            wire.push(wire_type::ACCOUNT_STATE);
            Some(wire)
        }
        NOTYPE_TRANSACTION => {
            let mut wire = data[9..].to_vec();
            wire.push(wire_type::TRANSACTION_WITH_META);
            Some(wire)
        }
        1 => {
            // Ledger object — not part of account SHAMap, skip silently
            None
        }
        other => {
            eprintln!("warn: unknown NodeObjectType {other} in encoded blob");
            None
        }
    }
}

/// HashPrefix::InnerNode ("MIN\0") — embedded in the decompressed EncodedBlob for inner nodes.
const HASH_PREFIX_INNER_NODE: [u8; 4] = [0x4D, 0x49, 0x4E, 0x00];

/// Encode XRLA wire content (content bytes, WITHOUT the trailing type byte) back into a
/// valid NuDB stored value. Inverse of `decode_value_to_wire` / `nudb_value_to_wire_at`.
///
/// Always uses codec 0x00 (raw/uncompressed) rather than LZ4 or the sparse-inner codec —
/// simpler, and it reuses the already-verified `decode_raw`/`encoded_blob_to_wire` read
/// path, so a value written here round-trips through `decode_value_to_wire` byte-for-byte.
/// This has been validated by reading written stores back with `keyfile::Shard::fetch`;
/// it has NOT been tested against a real rippled process.
pub fn encode_wire_to_value(content: &[u8], node_type: &NodeType) -> Vec<u8> {
    let mut value = Vec::with_capacity(1 + 8 + 1 + content.len() + 4);
    value.push(CODEC_RAW);
    value.extend_from_slice(&[0u8; 8]); // EncodedBlob: 8 zero bytes

    if node_type.is_inner() {
        value.push(NOTYPE_UNKNOWN);
        value.extend_from_slice(&HASH_PREFIX_INNER_NODE);
        value.extend_from_slice(content); // 512 bytes
    } else {
        let notype = match node_type {
            NodeType::AccountState => NOTYPE_ACCOUNT,
            NodeType::TransactionWithMeta | NodeType::Transaction => NOTYPE_TRANSACTION,
            _ => unreachable!("is_inner() covered Inner/CompressedInner above"),
        };
        value.push(notype);
        value.extend_from_slice(content);
    }
    value
}

/// LEB128 unsigned varint decoder. Returns (value, bytes_consumed) or None on truncation.
fn read_varint(data: &[u8]) -> Option<(u64, usize)> {
    let mut result = 0u64;
    let mut shift = 0u32;
    for (i, &byte) in data.iter().enumerate() {
        result |= ((byte & 0x7F) as u64) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            return Some((result, i + 1));
        }
        if shift >= 64 {
            return None; // overflow
        }
    }
    None // truncated
}
