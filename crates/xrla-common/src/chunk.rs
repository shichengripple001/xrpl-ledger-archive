use crate::shamap::{Hash256, SHAMapDiff, SHAMapNode};
use thiserror::Error;

pub const MAGIC_HEADER: &[u8; 4] = b"XRLA";
pub const MAGIC_FOOTER: &[u8; 4] = b"ENDX";
/// Version 2: TX_MAPS gained account_hash + drops + close-time header fields, making
/// LedgerHash independently reconstructable from chunk contents alone (see
/// "Verification without full history" in spec/chunk-format.md). Version 1 chunks are
/// not readable by this version — never shipped past this repo, so no compat shim needed.
pub const FORMAT_VERSION: u8 = 2;

pub const NETWORK_MAINNET: u32 = 1;
pub const NETWORK_TESTNET: u32 = 2;
pub const NETWORK_DEVNET:  u32 = 3;

/// Transaction with raw blobs as stored in a chunk.
#[derive(Debug, Clone)]
pub struct TxRecord {
    pub tx_hash:   Hash256,
    pub tx_blob:   Vec<u8>,
    pub meta_blob: Vec<u8>,
}

/// All transactions for one ledger, plus the header fields needed to independently
/// reconstruct this ledger's full LedgerHash from chunk contents alone:
///   - `tx_hash` is not stored — rebuild the tx SHAMap from `txns` (see `tx_tree`).
///   - `parent_hash` is not stored — it's the previous entry's `ledger_hash` (or, for the
///     first ledger in a chunk, an externally obtained anchor; see spec "Verification
///     without full history").
///   - `account_hash`, `drops`, and the close-time fields ARE stored here because nothing
///     else in the chunk derives them.
#[derive(Debug, Clone)]
pub struct TxMap {
    pub ledger_seq: u32,
    /// This ledger's full LedgerHash, independently recomputed by the exporter from
    /// (seq, drops, parent_hash, tx_hash, account_hash, close-time fields) and verified
    /// against the source database. Lets a buyer walk the parent_hash chain-of-custody
    /// through the chunk without needing per-ledger network access.
    pub ledger_hash: Hash256,
    /// AccountSetHash — this ledger's state SHAMap root. Compared directly against the
    /// root produced by replaying checkpoint+deltas on import.
    pub account_hash: Hash256,
    pub drops: u64,
    pub parent_close_time: u32,
    pub close_time: u32,
    pub close_time_resolution: u8,
    pub close_flags: u8,
    pub txns: Vec<TxRecord>,
}

/// One delta entry covering a single ledger transition.
#[derive(Debug)]
pub struct LedgerDelta {
    pub ledger_seq: u32,
    pub diff: SHAMapDiff,
}

/// A complete chunk covering ledgers [start_ledger..=end_ledger].
#[derive(Debug)]
pub struct Chunk {
    pub network_id:      u32,
    pub start_ledger:    u32,
    pub end_ledger:      u32,
    /// Ledger hash at start_ledger — verifiable on-chain.
    pub checkpoint_hash: Hash256,
    /// SHA-512/half of serialized body (checkpoint + deltas + tx_maps + footer).
    pub chunk_hash:      Hash256,
    /// Full SHAMap state at start_ledger.
    pub checkpoint:      Vec<SHAMapNode>,
    /// One delta per ledger from start_ledger+1 to end_ledger.
    pub deltas:          Vec<LedgerDelta>,
    /// Transactions for every ledger from start_ledger to end_ledger.
    pub tx_maps:         Vec<TxMap>,
}

impl Chunk {
    pub fn ledger_count(&self) -> u32 {
        self.end_ledger - self.start_ledger + 1
    }
}

/// Canonical filename for a chunk.
pub fn chunk_filename(network_id: u32, start: u32, end: u32) -> String {
    format!("xrla_{network_id}_{start:010}_{end:010}.xrla")
}

#[derive(Debug, Error)]
pub enum ChunkError {
    #[error("invalid magic bytes")]
    InvalidMagic,
    #[error("unsupported format version: {0}")]
    UnsupportedVersion(u8),
    #[error("chunk hash mismatch: expected {expected}, got {got}")]
    HashMismatch { expected: String, got: String },
    #[error("ledger hash mismatch at ledger {ledger_seq}")]
    LedgerHashMismatch { ledger_seq: u32 },
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("unexpected end of file")]
    UnexpectedEof,
}
