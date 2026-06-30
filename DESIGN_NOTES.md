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

Concretely (PLAN.md Phase 4): a user downloads only the ledger range they care about and runs a
**local** query tool over it. Transaction lookups read the cheap per-ledger tx stream (~KB/ledger,
no checkpoint needed); full-state and balance-at-ledger queries additionally use the
checkpoint + delta streams. The three streams are fetched independently so a transaction-querier
never downloads a multi-GB state checkpoint. Transaction data is now exported and verified; the
remaining work is the query index + extraction tool itself.

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

**Determinism is necessary but NOT sufficient — it does not imply correctness.** A deterministic
*decode* bug produces stable, reproducible, byte-identical — and wrong — output. We hit exactly
this: sparse inner nodes were decoded with the branch mask bit-reversed (`mask & (1<<s)` instead
of the big-endian `mask & (0x8000>>s)`), corrupting ~93% of them. Two runs matched perfectly; the
chunk_hash was stable; it looked "successful." The only thing that caught it was recomputing the
SHAMap root from the emitted nodes and comparing against the on-chain `AccountSetHash`. **Rule:
every export must be validated against on-chain hashes, never just against a second run.**

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

## NuDB On-Disk Format Discovery

Full format spec: `crates/xrla-nudb/NUDB_FORMAT.md`. Implementation: `xrla-nudb/src/keyfile.rs`
(`.key` lookups) and `xrla-nudb/src/dat.rs` (`.dat` value decoding). Summary of how it works
and the dead ends that were ruled out.

### Two codebases define the format

- **rippled** defines the *value* encoding: a codec byte (`0x00` raw, `0x01` LZ4, `0x02`
  sparse inner, `0x03` full inner) wrapping an `EncodedBlob` (`[8 zeros][NodeObjectType][payload]`).
  This is in rippled's `NodeStore` / `codec.h`.
- **NuDB** (the storage library rippled links against) defines the *container*: the `.dat`
  record framing and the `.key` hash-table bucket layout. rippled never touches this — it just
  calls `NuDB::fetch(key)`. So the bucket layout lives only in NuDB's `detail/bucket.hpp`.
  Reverse-engineering the reader required *both* sources.

### Why the `.dat` sequential scan is the wrong approach

The first attempt scanned the `.dat` file front-to-back. This recovers almost nothing: in a
live mainnet store, valid records are scattered across the entire multi-GB file (offsets reach
4+ GB) interleaved with spill buckets and zero gaps. A forward scan stops at the first zero gap
(~1 MB in) and recovers ~13 K of 27 M nodes. **The `.key` file is mandatory** — it's the hash
table giving O(1) random access. `dat::scan_dat()` remains in the tree but is unused.

### `.key` lookup algorithm (empirically verified)

```
nhash   = xxh64(key, seed=salt) >> 16            # NuDB's effective 48-bit hash
modulus = smallest power of two >= num_buckets    # linear hashing
bucket  = nhash % modulus;  if bucket >= num_buckets { bucket -= modulus/2 }
```

- Bucket N is at `.key` offset `block_size * (N+1)` (header occupies block 0). `block_size`=4096,
  `num_buckets = (key_file_size - block_size) / block_size`, `salt` at header offset 28.
- Bucket = `count(u16)` + `spill(u48)` + `count` × 18-byte entries
  (`offset(u48)` + `size(u48)` + `hash(u48)`), sorted ascending by hash.
- Match candidates by the 48-bit `nhash`, then **verify the full 32-byte key** from the `.dat`
  record (48-bit prefix collisions occur).
- Both bucket placement and the stored entry hash use `nhash`. The `>> 16` and the 18-byte
  entry size (the 6-byte `size` field is easy to miss) were the two non-obvious bugs.

### Spill chains

When a bucket overflows it spills to the `.dat` file: `[6 zero][2 size][bucket body]`. The
parent's `spill` field points *directly at the bucket body* (the 8-byte header exists only so a
dat-recovery scan can skip it). Read the body in place and follow the chain. Spilled entries are
usually superseded (old) nodes, so a current-state walk rarely needs them — but correctness
requires following the chain.

### online_delete keeps two live shards

rippled's `online_delete` rotates between **two** NuDB databases at once during deletion (e.g.
`rippledb.f380/` and `rippledb.fccd/`). The complete state spans both. The reader takes a list
of shards and tries each in turn; `NuDBReader::open(&[dat_paths])`. A consistent snapshot
requires stopping rippled and copying *both* shard dirs together — copying a live or
mid-rotation DB yields a torn snapshot (this cost significant debugging time).

---

## Storage Estimate

**The original estimate was wrong by ~33×** — it assumed 350,000 ledgers/day. XRPL closes a
ledger every ~4 s ≈ **21,600 ledgers/day**. See `PLAN.md` for the full revised numbers.

PoC measurement (50 mainnet ledgers, 2026-06-30, after the sparse-inner fix below):
```
checkpoint (full state):  27,031,655 nodes, ~9.0 GB uncompressed (~333 B/node)
                          verified: root hashes to the on-chain AccountSetHash
delta per ledger:         ~1.02 MB uncompressed (~0.51 MB zstd), ~1,966 changed nodes
transactions per ledger:  ~90 txns; verified: per-ledger tx-tree root == on-chain TransSetHash
```

Reconciliation: `12 GB/day ÷ 21,600 ledgers/day ≈ 555 KB/ledger` of net new compressed nodes on
disk. Our 1.16 MB is *uncompressed* wire bytes; measured compression on real chunk data is ~2×
(zstd-3 1.95×, lz4 1.87×), so 1.16 MB ÷ 2 ≈ 555 KB — consistent.

### The dedup point (why this isn't the old sharding blowup)

The sum of all deltas = **every unique SHAMap node, stored once** (content-addressed). That is the
information floor — the same node set a full-history node holds. History sharding (2018) failed
because it re-stored the unchanged upper-trie inner nodes in *every* shard, so aggregate exceeded a
single full node. Here a node appears exactly once across the whole archive, so the aggregate is
bounded *below* a full node. We ship only nodes (compressed); the `.key` index is rebuilt at import,
and we drop SQLite + NuDB slack — so the archive is the full node's `.dat` floor (est. ~25–30 TB)
rather than its full 39 TB.

The only thing that repeats across chunks is the **checkpoint**, and that is a tunable knob: one
checkpoint per ~1M ledgers adds ~0.3 TB; per-chunk would add ~2.6 TB. Decide spacing in Phase 1 —
it is not a blocker, and it never causes inner-node duplication.

The other win is granularity: a full node is all-or-nothing (39 TB to participate); chunks let an
operator pull only the ledger ranges it needs, in parallel. See PLAN.md for the full size model.

---

## What Remains Unsolved

1. **NuDB writer** (`xrla-import`): writing nodes back into NuDB format for rippled to consume
   (must rebuild the `.key` hash table, not just append `.dat` records)
2. **Ledger hash verification**: replay checkpoint + deltas and assert each reconstructed
   `AccountSetHash` matches `ledger.db`; combine with the verified tx root for the full ledger hash
4. **Checkpoint spacing + compression**: choose checkpoint cadence (~1 per 1M ledgers) and the
   on-disk compression codec, then validate the size floor at scale (see Storage Estimate).
   Design parameters, not a blocker.
5. **Parallel export**: exporting chunks in parallel across non-overlapping ledger ranges

### Solved since initial notes

- ✅ **Production NuDB reader**: replaced the in-memory `scan_dat()` with O(1) `.key` lookups
  (`keyfile.rs`), multi-shard and spill-aware. No full load into RAM. Reads the entire 27M-node
  mainnet state tree correctly; export is deterministic.
- ✅ **TX maps**: transactions + metadata are read from the `TransSetHash` SHAMap (same NuDB
  store) and verified — per-tx authenticity and per-ledger tx-tree root vs on-chain `TransSetHash`.
