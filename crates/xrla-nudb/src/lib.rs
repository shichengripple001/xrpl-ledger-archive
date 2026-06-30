/// NuDB reader — reads rippled's on-disk NuDB store directly.
///
/// NuDB is a key-value store where:
///   key   = 32-byte SHA-512/half node hash
///   value = raw serialized SHAMap node content
///
/// Files on disk:
///   <path>.dat  — data file, contains all key-value records
///   <path>.key  — key file, hash table mapping key → offset in .dat
///
/// This module implements:
///   1. DatScanner  — sequential scan of .dat for building an in-memory index
///                    (used for PoC / small ledger ranges)
///   2. NuDBReader  — full reader with .key file lookups
///                    (TODO: implement for production)

pub mod dat;
pub mod keyfile;
pub mod reader;

pub use reader::NuDBReader;
