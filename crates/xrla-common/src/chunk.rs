use crate::shamap::{Hash256, SHAMapDiff, SHAMapNode};
use thiserror::Error;

pub const MAGIC_HEADER: &[u8; 4] = b"XRLA";
pub const MAGIC_FOOTER: &[u8; 4] = b"ENDX";
pub const FORMAT_VERSION: u8 = 1;

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

/// All transactions for one ledger.
#[derive(Debug, Clone)]
pub struct TxMap {
    pub ledger_seq: u32,
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
