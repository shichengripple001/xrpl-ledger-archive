/// NuDB .key file reader — O(1) random-access lookup of a node by hash.
///
/// This replaces the sequential .dat scan (see dat.rs scan_dat) which is fundamentally
/// wrong: in a live mainnet store the valid records are scattered across the entire
/// multi-GB .dat file (record offsets reach 4+ GB), so a front-to-back scan stops at the
/// first zero gap and recovers only a tiny fraction of the tree.
///
/// Format reverse-engineered against rippled 3.2.0 and the NuDB library source
/// (https://github.com/cppalliance/NuDB, detail/bucket.hpp). See NUDB_FORMAT.md.
///
/// Key file header (first block, block_size bytes):
///   [8]  magic = "nudb.key"
///   [2]  version
///   [8]  uid
///   [8]  appnum
///   [2]  key_size = 32
///   [8]  salt        (offset 28)
///   [8]  pepper      (offset 36)
///   [2]  block_size  (offset 44)
///   [2]  load_factor (offset 46)
///   ... zero padding to block_size ...
///
/// Buckets follow the header, one per block_size-byte block:
///   bucket N is at file offset block_size * (N + 1)
///   num_buckets = (key_file_size - block_size) / block_size
///
/// Bucket layout (from NuDB bucket.hpp):
///   [2]  count  (u16 BE)  — number of entries
///   [6]  spill  (u48 BE)  — .dat offset of next spill bucket, or 0
///   count × entry, each 18 bytes, sorted ascending by hash:
///     [6] offset (u48 BE) — .dat offset of the record
///     [6] size   (u48 BE) — value size of the record
///     [6] hash   (u48 BE) — hash prefix (see below)
///
/// Hashing (empirically verified, both bucket placement and stored prefix use it):
///   nhash  = xxh64(key, seed=salt) >> 16        (NuDB's effective 48-bit hash)
///   bucket = nhash % modulus; if bucket >= num_buckets { bucket -= modulus/2 }
///   where modulus is the smallest power of two >= num_buckets (linear hashing).
///   The stored 6-byte entry hash equals nhash; full 32-byte key is verified from .dat.
///
/// Spill buckets live in the .dat file: [6 zero][2 size BE][bucket body of `size` bytes].

use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;

use anyhow::{bail, Context, Result};

use xrla_common::shamap::Hash256;
use xxhash_rust::xxh64::xxh64;

const KEY_MAGIC: &[u8; 8] = b"nudb.key";
const ENTRY_SIZE: usize = 18; // offset(6) + size(6) + hash(6)
const BUCKET_HEADER: usize = 8; // count(2) + spill(6)
const REC_HEADER: u64 = 6 + 32; // val_size(6) + key(32)

/// Read a 6-byte big-endian u48 from the start of `b`.
fn read_u48(b: &[u8]) -> u64 {
    let mut x = [0u8; 8];
    x[2..8].copy_from_slice(&b[0..6]);
    u64::from_be_bytes(x)
}

/// A single NuDB database (one .dat + .key pair). rippled's online_delete keeps two
/// of these live at once during rotation; the full state spans both, so callers should
/// try each shard in turn.
pub struct Shard {
    dat: File,
    key: File,
    salt: u64,
    block_size: u64,
    num_buckets: u64,
    modulus: u64,
}

impl Shard {
    pub fn open(dat_path: &Path, key_path: &Path) -> Result<Self> {
        let dat = File::open(dat_path)
            .with_context(|| format!("open dat {}", dat_path.display()))?;
        let key = File::open(key_path)
            .with_context(|| format!("open key {}", key_path.display()))?;

        let mut hdr = [0u8; 64];
        key.read_exact_at(&mut hdr, 0)
            .with_context(|| format!("read key header {}", key_path.display()))?;
        if &hdr[0..8] != KEY_MAGIC {
            bail!("invalid NuDB key magic in {}", key_path.display());
        }
        let key_size = u16::from_be_bytes([hdr[26], hdr[27]]);
        if key_size != 32 {
            bail!("expected key_size=32, got {key_size} in {}", key_path.display());
        }
        let salt = u64::from_be_bytes(hdr[28..36].try_into().unwrap());
        let block_size = u16::from_be_bytes([hdr[44], hdr[45]]) as u64;
        if block_size == 0 {
            bail!("zero block_size in {}", key_path.display());
        }

        let key_file_size = key.metadata()?.len();
        let num_buckets = (key_file_size - block_size) / block_size;
        let mut modulus = 1u64;
        while modulus < num_buckets {
            modulus <<= 1;
        }

        Ok(Self { dat, key, salt, block_size, num_buckets, modulus })
    }

    fn bucket_index(&self, nhash: u64) -> u64 {
        let mut n = nhash % self.modulus;
        if n >= self.num_buckets {
            n -= self.modulus / 2;
        }
        n
    }

    /// Fetch the raw NuDB stored value for `key`, following spill chains.
    /// Returns Ok(None) if the key is not present in this shard.
    pub fn fetch(&self, key: &Hash256) -> Result<Option<Vec<u8>>> {
        let nhash = xxh64(key, self.salt) >> 16;
        let bucket = self.bucket_index(nhash);

        // Read the primary bucket from the key file.
        let mut block = vec![0u8; self.block_size as usize];
        self.key
            .read_exact_at(&mut block, self.block_size * (bucket + 1))?;

        loop {
            let count = u16::from_be_bytes([block[0], block[1]]) as usize;
            let spill = read_u48(&block[2..8]);

            for i in 0..count {
                let b = BUCKET_HEADER + i * ENTRY_SIZE;
                let entry_hash = read_u48(&block[b + 12..b + 18]);
                // Entries are sorted by hash; we could binary-search, but a bucket holds
                // ~100 entries so a linear scan is cheap and robust against duplicates.
                if entry_hash != nhash {
                    continue;
                }
                let offset = read_u48(&block[b..b + 6]);
                let val_size = read_u48(&block[b + 6..b + 12]) as usize;

                // Verify the full 32-byte key from the .dat record (prefix collisions exist).
                let mut head = [0u8; REC_HEADER as usize];
                self.dat.read_exact_at(&mut head, offset)?;
                if &head[6..38] == &key[..] {
                    let mut val = vec![0u8; val_size];
                    self.dat.read_exact_at(&mut val, offset + REC_HEADER)?;
                    return Ok(Some(val));
                }
            }

            if spill == 0 {
                return Ok(None);
            }
            // Follow the spill bucket, stored in the .dat file. The stored `spill` offset
            // points directly at the bucket data (count + spill + entries) — the on-disk
            // spill record is [zero(6)][size(2)][bucket], and `spill` already skips the
            // 8-byte header. Read the header to learn the entry count, then the entries.
            let mut head = [0u8; BUCKET_HEADER];
            self.dat.read_exact_at(&mut head, spill)?;
            let scount = u16::from_be_bytes([head[0], head[1]]) as usize;
            let mut body = vec![0u8; BUCKET_HEADER + scount * ENTRY_SIZE];
            self.dat.read_exact_at(&mut body, spill)?;
            block = body;
        }
    }
}
