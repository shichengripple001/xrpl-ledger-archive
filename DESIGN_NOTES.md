# XRPL Ledger Archive — Design Notes & Decisions

This document captures the reasoning behind key design decisions, derived from the
initial design discussion.

---

## Why This Exists

Running a full-history XRP Ledger node requires ~39 TB of NVMe SSD (as of Jan 2026),
growing at ~12 GB/day. Getting that history from scratch via P2P takes several months
because backfilling is the lowest-priority task and is constrained to direct peers only.

**There is no existing mechanism to share full history easily between operators.**

History sharding (rippled v0.90.0, 2018) was the official attempt. It was removed in
rippled v2.3.0 (2024) because the SHAMap structure caused every shard to duplicate
unchanged InnerNodes across ledger ranges — aggregate shard storage across all shard
holders exceeded a single full-history node.

---

## Why Not Clio

Clio is not real full history. It stores a transformed/normalized subset of ledger data
in Cassandra optimized for API queries. It does NOT store raw SHAMap nodes, so:

- Cannot reconstruct full ledger state at an arbitrary historical ledger
- Cannot serve `ledger_data` (full state dump at any ledger)
- Cannot cryptographically prove the state of any object at any point in time

Clio was designed for a different goal: cheap API serving. It throws away the
cryptographic source data to achieve that. This project preserves it.

**However:** because the chunk format preserves full SHAMap state, a query layer
built on top of it can serve everything Clio serves, plus what Clio cannot. Clio
requires Cassandra; our approach only needs disk + a lightweight index.

---

## Why Delta Encoding

The SHAMap is a Merkle Patricia trie. Between ledger N and N+1, only the nodes along
paths to changed leaves are modified. The rest is identical (same hash = same content).

History sharding failed because it stored ALL nodes for every ledger range, including
unchanged InnerNodes. This caused massive duplication.

Delta encoding stores only what changed: the diff between consecutive ledger states.
This is the same principle as git object storage — content is stored once, identified
by hash, referenced everywhere it's needed.

**Why the duplication problem doesn't apply here:**
- NuDB already deduplicates by hash across the full database
- The 12 GB/day growth in a full-history node = genuinely new unique nodes
- Our delta format captures exactly those new nodes, nothing more

---

## Why Determinism Matters

If two operators independently export the same ledger range and get different bytes,
the format cannot be distributed via BitTorrent, IPFS, or any content-addressed system.
Operators would have to trust the source.

**How determinism is achieved:**
The SHAMap is itself deterministic — the same ledger state always produces the same
tree with the same node hashes. The diff of two identical trees always produces the
same set of changed nodes. We sort those nodes by hash (ascending) before serializing.
Result: byte-identical output from any two independent exporters on the same data.

This enables trustless distribution: recipients verify by chunk_hash, not by trusting
the sender. The chunk_hash is reproducible by anyone with the same ledger data.

---

## Why No rippled Dependency

The exporter reads NuDB `.dat` files directly from disk. rippled does not need to be
running. Reasons:

1. **Speed**: direct disk reads are faster than RPC roundtrips
2. **Stability**: rippled's internal C++ APIs (like `visitDifferences()`) can change
   with any release. NuDB's file format is stable — changing it would corrupt existing
   databases, so it never changes without a migration path.
3. **Portability**: the exporter works on any machine with the NuDB files mounted,
   regardless of rippled version

**On amendment safety:**
XRPL data format changes only happen through amendments, activated at a specific ledger
sequence. Chunks already exported before an amendment are permanently valid — their
format is frozen at that ledger sequence. The exporter only needs updating for new
ledgers after the amendment activates. This is a predictable, versioned change — not
a surprise API breakage.

---

## Why Rust

- No garbage collector: predictable I/O performance for large file operations
- Memory safety: critical for a tool handling tens of terabytes
- Single static binary: operators just download and run, no runtime dependencies
- No coupling to rippled's C++ build system

Go was considered. For this workload (disk I/O bound, not CPU bound) the performance
difference is small. Rust was chosen for long-term reliability and binary distribution.

---

## Wire Format Discovery

The SHAMap node wire format was verified against rippled source:
`include/xrpl/shamap/SHAMapTreeNode.h` and
`src/libxrpl/shamap/SHAMapInnerNode.cpp`

Key findings:
- **Type byte is at the END of the serialized node**, not the beginning
- Wire type constants: `Transaction=0, AccountState=1, Inner=2, CompressedInner=3, TxWithMeta=4`
- **Full inner node**: 16 × 32-byte hashes back to back (512 bytes), zero hash = empty slot
- **Compressed inner node**: N × (32-byte hash + 1-byte position) pairs, used when < 12 children

The LedgerIndex reads rippled's SQLite `Ledgers` table directly:
- `AccountSetHash` column = state SHAMap root hash for each ledger sequence
- Verified in `src/xrpld/app/rdb/backend/detail/Node.cpp`

---

## Storage Estimate

```
NuDB growth:  12 GB/day / ~350,000 ledgers/day ≈ 35 KB/ledger delta

Per chunk (100k ledgers):
  checkpoint (full state at one ledger):  ~15 GB
  100k deltas × 35 KB:                   ~3.5 GB
  total:                                  ~18 GB

All history (90M ledgers, 900 chunks):
  deltas:       ~3 TB
  checkpoints:  ~13 TB
  total:        ~16 TB  (vs 39 TB today)
```

These are estimates. The PoC measures actual delta sizes.

---

## What Remains Unsolved

1. **NuDB writer** (`xrla-import`): writing nodes back into NuDB format for rippled to consume
2. **TX maps**: transaction blobs are not yet fetched (requires reading rippled's transaction DB)
3. **Ledger hash verification**: `xrla-import` needs on-chain ledger header hashes to verify
   each delta step — currently prints root hashes for manual verification only
4. **Production NuDB reader**: `scan_dat()` loads the full `.dat` into memory. For production,
   use `.key` file for O(1) lookups without loading 39 TB into RAM
5. **Parallel export**: exporting 900 chunks in parallel across non-overlapping ledger ranges
