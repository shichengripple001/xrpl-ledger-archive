# XRPL Ledger Archive — Implementation Plan

## Problem

Running a full-history XRP Ledger node requires ~39 TB of NVMe SSD, growing 12 GB/day.
Getting that history from scratch via P2P takes several months — backfilling is the
lowest-priority task and only works from direct peers.

There is no existing mechanism to share full history between operators.

History sharding (2018–2024) was the official attempt. Removed in rippled v2.3.0 because
the SHAMap structure caused every shard to duplicate unchanged InnerNodes — aggregate
shard storage exceeded a single full-history node.

## Solution

Canonical chunked archive format for XRPL ledger history.

Each chunk covers a range of ledgers and encodes only the **delta** — the SHAMap nodes
that actually changed between consecutive ledgers. Unchanged nodes are not repeated.
Chunks are deterministic, content-addressed, and self-verifying against on-chain hashes.

A new operator downloads chunks in parallel from any source, verifies each chunk against
on-chain hashes, imports into rippled NuDB, bootstrapped in hours not months.

**No protocol changes. No XLS amendment. No rippled dependency at runtime.**

---

## Why Rust

- No GC: predictable performance for large file I/O
- Single static binary: operators just download and run
- Memory safety: critical for a tool handling tens of terabytes
- No dependency on rippled process or source — reads NuDB files directly

---

## Key Design Decisions

### No rippled dependency

The exporter reads NuDB `.dat` files directly from disk. rippled does not need to be
running. This means:
- Works on any machine with the NuDB files mounted
- No version coupling to rippled releases
- Can run on a cold copy/snapshot of the database

### Determinism via hash-sort

SHAMap nodes are identified by their content hash (SHA-512/half). Serialization order
= ascending hash sort. Two independent exporters on identical NuDB data produce
identical bytes. This enables trustless P2P distribution — recipients verify by hash,
not by trusting the sender.

### Amendment safety

XRPL data format changes only happen through amendments, activated at a specific ledger
sequence. Chunks already exported before an amendment are permanently valid — they
contain exactly the data that existed at those ledger sequences, frozen forever.
The exporter only needs updating for new ledgers after the amendment activates.

---

## Storage Estimate

NuDB deduplicates nodes by hash. The 12 GB/day growth = genuinely new unique nodes.

```
12 GB/day / ~350,000 ledgers/day ≈ 35 KB/ledger delta

Per chunk (100k ledgers):
  checkpoint (full state at one ledger):  ~15 GB
  100k deltas x 35 KB:                   ~3.5 GB
  total:                                  ~18 GB

All history (90M ledgers, 900 chunks):
  deltas:      ~3 TB
  checkpoints: ~13 TB
  total:       ~16 TB  (vs 39 TB today)
```

PoC measures actual delta sizes to validate this estimate.

---

## Project Structure

```
xrpl-ledger-archive/
├── Cargo.toml                  workspace
├── crates/
│   ├── xrla-common/            shared types: chunk format, SHAMap types, serialization
│   │   └── src/
│   │       ├── chunk.rs        Chunk, LedgerDelta, TxMap structs + chunk_filename()
│   │       ├── shamap.rs       SHAMapNode, InnerNode, SHAMapDiff, NodeType
│   │       └── serialize.rs    serialize_chunk(), deserialize_chunk(), sha512half()
│   ├── xrla-nudb/              NuDB reader (no rippled dependency)
│   │   └── src/
│   │       ├── dat.rs          .dat file parser, DatHeader, scan_dat()
│   │       └── reader.rs       NuDBReader: get(), collect_reachable(), diff()
│   ├── xrla-export/            exporter binary
│   │   └── src/main.rs         CLI: nudb + ledger index → chunk files
│   └── xrla-import/            importer binary
│       └── src/main.rs         CLI: chunk files → NuDB
├── spec/
│   └── chunk-format.md         binary format specification
├── PLAN.md
└── TEST_PLAN.md
```

---

## Implementation Phases

### Phase 0: PoC — prove the design (current phase)

Goal: export 1000 consecutive ledgers, verify determinism, measure delta sizes.

What needs to be completed for PoC:
1. **NuDB reader** (`xrla-nudb`): verify `.dat` file parsing against a real rippled NuDB
2. **LedgerIndex** (`xrla-export`): read rippled's SQLite ledger database for state hashes
   - Add `rusqlite` dependency
   - Read `Ledgers` table: `LedgerSeq`, `LedgerHash`, `AccountSetHash` (state hash)
3. **SHAMap diff** (`xrla-nudb/reader.rs`): verify InnerNode encoding against rippled source
   - Confirm type byte prefix format (0=inner, 1=leaf) in `SHAMapTreeNode.cpp`
   - Confirm bitmask + child hash layout in `SHAMap::addRaw()`
4. **Run exporter**: `xrla-export --dat /path/nudb.dat --ledgers /path/ledger.db --start N --end N+1000`
5. **Verify determinism**: run twice, `diff` the output files — must be identical
6. **Measure**: print delta sizes per ledger, compare to 35 KB/ledger estimate

**Success criteria:**
- Two independent exports of the same range produce byte-identical chunk files
- SHAMap root hash after replaying deltas matches ledger header hash
- Delta sizes measured and logged

### Phase 1: Complete importer

- Implement NuDB writer in `xrla-import` (write nodes to `.dat` + rebuild `.key` index)
- Implement `verify_ledger_hashes()` against actual on-chain ledger header hashes
- Test: export range → import to fresh NuDB → rippled opens and serves from it

### Phase 2: Full history export

- Scale to all 90M ledgers (parallel workers per non-overlapping range)
- Measure actual total size vs 16 TB estimate
- Performance target: export full history in < 48 hours

### Phase 3: Distribution

- XRPLF S3 public bucket with all chunks
- `manifest.json` listing chunks with hashes + URLs
- Operators: `aria2c -i manifest.json` → parallel download → `xrla-import`

### Phase 4: Query layer (future)

The chunk format preserves full SHAMap state. A query server can be built on top:
- Lightweight index (SQLite or RocksDB): tx_hash/account_id/ledger_seq → chunk + offset
- Serve all Clio queries + ones Clio can't (`ledger_data`, full state at any ledger)
- No Cassandra required

---

## Immediate TODOs

1. Verify NuDB InnerNode encoding in rippled source:
   - `src/ripple/shamap/SHAMapTreeNode.cpp` → `addRaw()`
   - Confirm: type byte (0=inner, 1=leaf) + bitmask u16 + child hashes

2. Add `rusqlite` to `xrla-export` for LedgerIndex implementation

3. Run PoC against a local rippled node (testnet is fine, just needs consecutive ledgers)

4. Measure actual delta sizes and update storage estimate
