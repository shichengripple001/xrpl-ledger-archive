//! A per-chunk account index: for every transaction in a verified chunk, which accounts it
//! affected and where it sits in its ledger. It answers `account_tx` ("what did this account do
//! in this ledger range?") and `tx` ("where is this transaction?") without reading the chunk.
//!
//! The index is a derived, rebuildable artifact that sits *outside* the chunk's `chunk_hash`, so
//! it carries the hash of the chunk it was built from and the version of the account rule that
//! produced it; a reader refuses an index that is incomplete or built by a different rule.
pub mod build;
pub mod query;
pub mod schema;
