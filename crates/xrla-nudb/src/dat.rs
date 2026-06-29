/// NuDB .dat file parser.
///
/// NuDB .dat file layout:
///   [file header: 1 block]
///   [data records: variable]
///
/// File header fields (all big-endian):
///   magic:      u64  = 0x6e75_4442_0000_0000 ("nuDB\0\0\0\0")
///   version:    u16
///   uid:        u64
///   appnum:     u64
///   key_size:   u16  (32 for SHAMap nodes)
///   salt:       u64
///   pepper:     u64
///   block_size: u32
///   field_size: u8
///
/// Data records (within data blocks, after the file header block):
///   size:  u48 big-endian  (combined key+value size, encoded in field_size bytes)
///   key:   bytes[key_size]
///   value: bytes[size - key_size]
///
/// NOTE: The exact record layout depends on block_size and field_size from the header.
/// This implementation reads the header to determine those values, then scans records.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use anyhow::{bail, Result};
use thiserror::Error;

use xrla_common::shamap::Hash256;

pub const NUDB_MAGIC: u64 = 0x6e75_4442_0000_0000;

#[derive(Debug)]
pub struct DatHeader {
    pub version:    u16,
    pub uid:        u64,
    pub appnum:     u64,
    pub key_size:   u16,
    pub block_size: u32,
    pub field_size: u8,
}

#[derive(Debug, Error)]
pub enum DatError {
    #[error("invalid NuDB magic")]
    InvalidMagic,
    #[error("unsupported NuDB version: {0}")]
    UnsupportedVersion(u16),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

fn read_u8(r: &mut impl Read) -> Result<u8> {
    let mut b = [0u8; 1];
    r.read_exact(&mut b)?;
    Ok(b[0])
}

fn read_u16be(r: &mut impl Read) -> Result<u16> {
    let mut b = [0u8; 2];
    r.read_exact(&mut b)?;
    Ok(u16::from_be_bytes(b))
}

fn read_u32be(r: &mut impl Read) -> Result<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_be_bytes(b))
}

fn read_u64be(r: &mut impl Read) -> Result<u64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(u64::from_be_bytes(b))
}

/// Read field_size bytes as a big-endian u64 (NuDB uses variable-width size fields).
fn read_field(r: &mut impl Read, field_size: u8) -> Result<u64> {
    let mut buf = [0u8; 8];
    let start = 8 - field_size as usize;
    r.read_exact(&mut buf[start..])?;
    Ok(u64::from_be_bytes(buf))
}

pub fn read_header(r: &mut (impl Read + Seek)) -> Result<DatHeader, DatError> {
    let magic = read_u64be(r)?;
    if magic & 0xFFFF_FFFF_0000_0000 != NUDB_MAGIC & 0xFFFF_FFFF_0000_0000 {
        return Err(DatError::InvalidMagic);
    }
    let version    = read_u16be(r)?;
    let uid        = read_u64be(r)?;
    let appnum     = read_u64be(r)?;
    let key_size   = read_u16be(r)?;
    let _salt      = read_u64be(r)?;
    let _pepper    = read_u64be(r)?;
    let block_size = read_u32be(r)?;
    let field_size = read_u8(r)?;

    Ok(DatHeader { version, uid, appnum, key_size, block_size, field_size })
}

/// Scan the entire .dat file and build an in-memory map: hash → value bytes.
///
/// This is the PoC approach — suitable for small ledger ranges where the
/// relevant portion fits in memory. For production, use NuDB key-file lookups.
pub fn scan_dat(path: &Path) -> Result<HashMap<Hash256, Vec<u8>>> {
    let file = File::open(path)?;
    let mut r = BufReader::new(file);

    let header = read_header(&mut r).map_err(|e| anyhow::anyhow!("{e}"))?;
    if header.key_size != 32 {
        bail!("expected key_size=32, got {}", header.key_size);
    }

    // Skip to start of first data block (after the header block)
    r.seek(SeekFrom::Start(header.block_size as u64))?;

    let mut map = HashMap::new();
    let key_size = header.key_size as usize;

    loop {
        // Read record size (field_size bytes, big-endian)
        let record_size = match read_field(&mut r, header.field_size) {
            Ok(0) => break, // end of data or padding
            Ok(s) => s as usize,
            Err(_) => break,
        };

        if record_size < key_size {
            break;
        }

        let mut key = [0u8; 32];
        if r.read_exact(&mut key).is_err() {
            break;
        }

        let value_size = record_size - key_size;
        let mut value = vec![0u8; value_size];
        if r.read_exact(&mut value).is_err() {
            break;
        }

        map.insert(key, value);
    }

    Ok(map)
}
