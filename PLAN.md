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

The exporter reads NuDB `.dat`/`.key` files directly from disk via O(1) key-file lookups
(see `crates/xrla-nudb/NUDB_FORMAT.md`). rippled does not need to be running. This means:
- Works on any machine with the NuDB files mounted
- No version coupling to rippled releases
- Can run on a cold copy/snapshot of the database

**The database must be quiesced for a consistent snapshot.** rippled's `online_delete`
rotates between two live NuDB databases ("shards"); copying while it runs yields a torn
snapshot. Stop the rippled service, copy *both* shard directories (each has `nudb.dat` +
`nudb.key`) plus `ledger.db`, then restart. Pass every shard's `.dat` to the exporter.

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

### Measured (PoC, 2026-06-30)

50-ledger export from a mainnet snapshot (ledgers 105277428–105277478):

```
checkpoint (full state @ 105277428):  27,031,655 nodes, ~9.0 GB  (~333 B/node, uncompressed)
                                       verified: root hashes to on-chain AccountSetHash
delta per ledger:                      ~1,966 changed nodes, ~1.02 MB raw (~0.51 MB zstd)
                                       +98,314 / -98,150 nodes over 50 deltas
transactions per ledger:               ~90 txns (4,500 over 51 ledgers); verified vs TransSetHash
```

### The original 35 KB/ledger estimate was wrong by ~33×

The estimate below assumed **350,000 ledgers/day**. XRPL actually closes a ledger every
~4 s ≈ **21,600 ledgers/day** — ~16× fewer. Re-deriving from the 12 GB/day node growth:

```
12 GB/day ÷ 21,600 ledgers/day ≈ 555 KB/ledger of net new on-disk (LZ4-compressed) nodes
```

Our measured **1.16 MB/ledger is uncompressed** wire bytes; NuDB stores values LZ4-compressed
(~2× on these nodes), so 1.16 MB ÷ 2 ≈ 555 KB reconciles with the disk-growth figure. The
PoC measurement and rippled's growth rate agree once the ledgers/day error is fixed.

### The size model (the dedup point)

The sum of all deltas = **every unique SHAMap node ever created, stored once** (content-addressed,
sorted by hash). This is the information floor. A full-history node stores that *same* node set,
plus the `.key` hash index, `ledger.db` + `transaction.db`, and NuDB pre-allocation slack. We ship
only the nodes (the `.key` index is rebuilt at import time), compressed.

Measured compression on real chunk data: **~1.9–2.2×** (zstd-3 ≈ 1.95×, lz4 ≈ 1.87×).

```
Compression:                  ~2×
Per-ledger delta:             1.02 MB raw → ~0.51 MB compressed  (≈ 12 GB/day ÷ 21,600 ledgers/day)
Checkpoint (current state):   9.0 GB raw  → ~4.6 GB compressed   (grows with account count)

Full archive (~105M ledgers), deduped + compressed:
  all unique state nodes:     ≈ the full node's .dat portion  (the floor; est. ~25–30 TB)
  sparse checkpoints:         ~0.3 TB  (one per 1M ledgers)  — negligible
  → vs 39 TB for a running full node (we shed .key index + SQLite + slack)
```

**Why this is *not* the failed-sharding blowup.** 2018 history sharding re-stored the unchanged
upper-trie inner nodes in every shard, so aggregate storage exceeded a single full node. Here each
unique node appears exactly once across the whole archive, so the aggregate is bounded *below* a
full node. The only thing that repeats is the checkpoint, and that is a tunable knob, not inner-node
duplication.

**Two distinct wins:**
1. *vs old sharding* — dedup makes it actually work (aggregate ≤ full node, not >).
2. *vs all-or-nothing* — a full node is 39 TB to participate at all; with chunks an operator
   downloads only the ledger ranges it needs, in parallel, in hours.

**Checkpoint spacing is a design parameter, not a blocker.** Per-chunk full checkpoints would add
~2.6 TB of duplication; one checkpoint per ~1M ledgers (a chunk referencing the nearest preceding
one) drops that to ~0.3 TB while keeping reconstruction bounded. Decide spacing in Phase 1.

**To validate at scale (Phase 2):** sample checkpoint sizes at older sequences (state was much
smaller historically), sum real deltas over a multi-million-ledger range, and confirm the floor
against a full node's actual `.dat` size.

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
│   │   ├── NUDB_FORMAT.md      on-disk .dat/.key format (reverse-engineered)
│   │   └── src/
│   │       ├── dat.rs          .dat value codecs (LZ4/inner) + EncodedBlob → wire bytes
│   │       ├── keyfile.rs      Shard: .key bucket hash-table, O(1) fetch() by hash
│   │       └── reader.rs       NuDBReader: multi-shard get_node(), collect_reachable(), diff()
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

### Phase 0: PoC — prove the design  ✅ (export side proven 2026-06-30)

Goal: export consecutive mainnet ledgers, verify determinism, measure delta sizes.

Status:
1. ✅ **NuDB reader** (`xrla-nudb`): `.dat`/`.key` format reverse-engineered and verified
   against rippled 3.2.0 + NuDB library source. O(1) key-file lookups, multi-shard,
   spill-chain aware. Full 27M-node mainnet state tree reads correctly. See NUDB_FORMAT.md.
2. ✅ **LedgerIndex** (`xrla-export`): reads `Ledgers` table (`LedgerSeq`, `LedgerHash`,
   `AccountSetHash`) via `rusqlite`.
3. ✅ **SHAMap diff** (`xrla-nudb/reader.rs`): inner/leaf wire encoding verified; diff
   short-circuits on equal subtree hashes (O(changed nodes)).
4. ✅ **Run exporter**: exported ledgers 105277428–105277478 (50 ledgers) from a stopped-node
   snapshot. Command:
   `xrla-export --dat shard0/nudb.dat shard1/nudb.dat --ledgers ledger.db --start N --end M --out ./`
5. ✅ **Determinism**: two independent runs → byte-identical `chunk_hash`
   `6573245dbdf149597d4be1cf575df9f994d94c3752f017aff6af9ca342549daf`.
6. ✅ **Correctness (state)**: parsed the chunk back and recomputed hashes — all 7,912,690
   checkpoint inner nodes hash to their key, and the root node hashes to the ledger's on-chain
   `AccountSetHash`. This is the test that determinism alone does NOT give you (see bug below).
7. ✅ **Transactions**: tx-with-meta SHAMap (`TransSetHash` tree) now exported into `tx_maps`
   (4,500 txns over 51 ledgers). Verified: every txid == `SHA512half(HashPrefix::transactionID
   + tx)` (4500/4500), and each ledger's reconstructed tx-tree root == on-chain `TransSetHash`
   (51/51) — which proves both completeness (all txns present) and metadata correctness.
8. ✅ **Measured** delta sizes (see Storage Estimate): ~1.02 MB/ledger uncompressed (~0.51 MB
   zstd), ~1,966 changed nodes/ledger; ~90 txns/ledger.

**Bug found and fixed by the correctness check:** sparse inner nodes (codec 0x02) were decoded
with the branch mask bit-reversed (`mask & (1<<s)` instead of `mask & (0x8000>>s)` — rippled uses
big-endian bit order, branch 0 = MSB). ~93% of sparse inners decoded wrong. It was **deterministic**,
so two runs matched and the first "success" claim was premature. Only recomputing the root against
the on-chain hash caught it. Fixed in `dat.rs::decode_sparse_inner`; chunk_hash changed from the
buggy `54e2226a…` through `91e49841…` (state fix) to the verified `6573245d…` (with txns).
Lesson: determinism ≠ correctness — always verify against on-chain hashes.

**Success criteria:**
- ✅ Two independent exports → byte-identical chunk files
- ✅ Checkpoint root hash == on-chain `AccountSetHash` (state tree fully verified)
- ✅ Transactions: txid authenticity + per-ledger tx-tree root == on-chain `TransSetHash`
- ⬜ Per-delta replay: reconstructed `AccountSetHash` matches at every ledger
      *(needs `xrla-import` replay path — Phase 1)*
- ✅ Delta sizes measured and logged

Still open in Phase 0:
- Round-trip verification: replay checkpoint + deltas and confirm each reconstructed
  `AccountSetHash` matches the on-chain value (needs `xrla-import`).
- State leaf-content verification: recompute account-state leaf hashes (not just inner/root)
  for full state coverage. (Transaction leaves are already fully verified via the tx-tree root.)
- Full ledger-hash verification: now feasible — combine the verified state root + tx root
  (+ ledger header fields) to check the complete ledger hash.

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

### Phase 4: Query layer — "download a range, query what you want locally"

The chunk store doubles as the backend for a query layer — no Cassandra. Two deployment modes
over the same format:

- **Local tool** — an operator pulls only the ledger range they care about and queries it from
  their own disk.
- **Hosted service** — a Clio-style API server over a chunk store, serving everything Clio serves
  plus what it can't (`ledger_data`, full historical state, balance-at-ledger proofs), because we
  retain the cryptographic SHAMap source data Clio discards.

Both share the same index + extraction logic. Two distinct query needs, with very different
download sizes (see "Stream separation" below):

- **Inspect transactions in N–M** — wants only the tx data for those ledgers (~KB/ledger). Must
  NOT require downloading the multi-GB state checkpoint.
- **Reconstruct full state / serve a node for N–M** — wants checkpoint + state-deltas (heavy).

The query tool:
- Builds a local index (SQLite/RocksDB) from downloaded chunks/streams:
  `tx_hash → (chunk, offset)`, `account → ledgers touched`, `ledger_seq → chunk`.
- Answers from tx-maps alone (cheap): a specific transaction; all transactions for an account in a
  range; everything in a ledger.
- Answers from the state stream (if also pulled): full ledger state at any sequence, balance-at-
  ledger — queries Clio cannot serve because it discards the SHAMap source data.

**Transaction data:** ✅ done — the exporter populates `tx_maps` from the transaction SHAMap
(`TransSetHash` tree), verified against on-chain roots. Each record is `(txid, tx_blob, meta_blob)`.
The query tool can index these directly; the remaining work is the index + extraction CLI itself.

### Stream separation (format consideration for partial fetch)

A chunk currently bundles checkpoint + state-deltas + tx-maps. To let a transaction-querier avoid
the heavy checkpoint, the three should be independently fetchable — either as separate sidecar
files per range or via a manifest with per-section byte ranges (HTTP range requests):

```
xrla_1_<start>_<end>.ckpt    full-state checkpoint  (heavy; only for state reconstruction)
xrla_1_<start>_<end>.delta   per-ledger state deltas
xrla_1_<start>_<end>.tx      per-ledger transactions + metadata  (cheap; for tx queries)
```

Each stays content-addressed and independently verifiable. Decide the exact mechanism (sidecars
vs. range index) in Phase 1 alongside checkpoint spacing.

---

## Immediate TODOs

1. **Round-trip verification** — build the replay path in `xrla-import`: apply checkpoint +
   deltas in order, recompute each ledger's state root, assert it equals `AccountSetHash`
   from `ledger.db`. This is the last unchecked Phase 0 success criterion.

2. **Resolve the storage premise** (blocks Phase 2): quantify compressed delta size and
   decide the checkpoint strategy (per-chunk full state is too expensive). See Storage Estimate.

3. **TX maps**: fetch transaction blobs (rippled `transaction.db` / tx SHAMap) so chunks carry
   transactions, not just state deltas. Currently `tx_maps` is populated with empty `txns`.

4. **Remove dead code**: `dat::scan_dat()` (the original sequential-scan PoC) is no longer used
   by `NuDBReader`; keep only if useful as a recovery tool, otherwise delete.

5. Clarify how snapshots are taken in production (stop-copy-restart vs. NuDB's own consistent
   snapshot, vs. reading a live DB safely).
