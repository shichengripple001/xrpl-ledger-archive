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

**Core archive vs. the optional decoder — different exposure (2026-07-09).** The core
export/import/verify path treats all ledger state and transactions as opaque, content-addressed
bytes — a new ledger object type (new fields, new amendments, e.g. rippled 3.3.0's lending
protocol) is just a new kind of leaf blob to it, zero code changes needed, ever. Only the optional
`meta_decode.rs`-style decoder work has any exposure at all, and only to a genuinely new *binary
wire type* (a new `STI_*`, not a new field on an existing type) — rare (roughly once a year or two;
`Issue` for AMM, `XChainBridge` for bridges are the precedents), and it fails soft (skips that one
transaction, doesn't break the tool) even when it happens. See STATUS.md "External changes
assessed" for the live example of this (`STI_ISSUE`, hit and fixed in ~2 minutes this session).
**rippled 3.3.0 / lending protocol specifics not yet reviewed** — expected no-impact on the core
archive by this rule; revisit for the decoder once the ledger-entry format is published.

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

### Full-history server evidence (2026-07-08)

Found two real rippled full-history nodes (`devnet-fh-usw2-01`, `livenet-fh-usw2-01`) and inspected
their on-disk layout directly:

- **No `shard_db`, no `online_delete`.** Both run a single, permanently-growing `node_db` — one
  `nudb.dat`/`nudb.key` pair covering the node's entire retained range, nothing ever purged. The
  rippled shard store (fixed ~16,384-ledger immutable shards) is a separate, opt-in config these
  full-history boxes aren't using. This invalidates any plan that assumes "just open the shard for
  the ledger range you want" on mainnet full history — there is no shard boundary to target.
- **Mainnet (`livenet-fh-usw2-01`) real sizes:** `nudb.dat` = 29,487,676,646,008 bytes (~29.5 TB),
  `nudb.key` = 4,019,696,713,728 bytes (~4.0 TB). Combined ~33.5 TB, consistent with (and validating)
  the ~25–30 TB state-node floor estimated above once transaction-tree nodes and NuDB overhead are
  accounted for, and comfortably under the 39 TB full-node figure once `ledger.db`/`transaction.db`
  are added on top.
- **Derived total record count from the real key-file size** (no need to guess): NuDB's key file is
  a hash table of fixed `block_size` (4096 B) buckets, capacity 227 entries/bucket at our
  reverse-engineered format, target load factor 0.5 (see `xrla-nudb/src/keyfile.rs`,
  `xrla-nudb/src/writer.rs`):
  ```
  num_buckets  = 4,019,696,713,728 / 4096              ≈ 981.4 million
  total_entries ≈ num_buckets × 227 × load_factor       ≈ 55–111 billion   (range reflects
                                                           uncertainty in average vs. target
                                                           load factor under linear-hashing growth)
  ```
  This is the **total unique (state + transaction-tree) node count across all of mainnet history**,
  measured indirectly from real on-disk evidence, not extrapolated from a per-ledger rate. It is a
  more trustworthy total-workload number than multiplying today's measured per-ledger delta rate
  (~1,966 changed state nodes/ledger, see above) across all 105M+ ledgers, because both state size
  and transaction volume grew dramatically over the network's lifetime — a recent-ledger rate applied
  uniformly to early history overestimates old ledgers' cost the same way today's 27M-node checkpoint
  size overestimates early-history checkpoint size.
- **NuDB lookup is genuinely O(1) w.r.t. store size** — benchmarked, not assumed. See
  `crates/xrla-nudb/examples/bench_lookup.rs`: builds real, structurally valid NuDB stores (via
  `write_nudb_store`) from 1,000 to 5,000,000 entries (5000× range, key file 40 KB → 180 MB) and
  times `Shard::fetch` for a fixed probe set against each. Result: flat ~1.0–1.4 µs/lookup across
  the entire range, no growth trend — confirms the bucket-hash-then-single-read mechanism doesn't
  degrade with size. **Caveat:** this ran cache-warm on local SSD; it doesn't measure real cold-read
  latency against an actual multi-TB key file that can't fit in RAM (that requires running against
  a real full-history box — not yet done).

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
- ✅ Full `LedgerHash` per ledger: exporter now captures `PrevHash`/`TotalCoins`/close-time
  fields from `ledger.db`, independently recomputes `LedgerHash` (verified formula — see
  `serialize.rs::calculate_ledger_hash`), aborts on mismatch, and stores it in every `TxMap`
  entry. Verified on all 51 ledgers of the test range against real chain data.
- ✅ Per-delta replay: `xrla-import` replays checkpoint + deltas, reconstructs each ledger's
  `AccountSetHash`, and asserts it against the stored value. Run against a real 51-ledger
  mainnet export/import round trip (4,500 txns, 27M+ state nodes) — every ledger passed.
- ✅ Delta sizes measured and logged
- ✅ State leaf-content verification: every node in the state tree — inner *and*
  `AccountState` leaves — is now independently recomputed from raw content and checked
  against its own claimed hash on import (`xrla_common::state_tree`), not just the root.
  Validated exhaustively against a real mainnet checkpoint: all 27,031,655 nodes
  (7,912,690 inner + 19,118,965 leaves), zero mismatches. See Immediate TODOs item 7.

Phase 0 is now fully closed.

**Why the per-ledger `LedgerHash` matters — chain-of-custody without full history.** Because
`LedgerHash` embeds `parent_hash` (the previous ledger's hash), storing it for every ledger makes
each chunk a self-contained hash chain. A verifier needs only **one** independently obtained
anchor hash (genesis, a skip-list flag-ledger hash from any live node, or a single RPC call) to
prove an entire multi-terabyte archive wasn't tampered with anywhere — the same trust model as a
blockchain. See `spec/chunk-format.md` "Verification without full history."

### Phase 1: Complete importer

- ✅ NuDB writer in `xrla-import` (writes nodes to `.dat` + rebuilds `.key` index) —
  `xrla-nudb/src/writer.rs::write_nudb_store`. Round-trip validated by reading written stores
  back through `keyfile::Shard::fetch` (including a forced spill-chain case).
- ✅ `verify_ledger_hashes()` against actual on-chain ledger header hashes — `xrla-import`
  independently recomputes each ledger's `account_hash`, full chained `LedgerHash`, per-tx
  `TransactionID`, and every state node's own hash on replay.
- ⬜ **Test: export range → import to fresh NuDB → rippled opens and serves from it.** Still the
  one unclosed item in Phase 1, and the only remaining claim in this project that rests on
  format reasoning rather than observed behavior. `writer.rs` says so itself: the layout it
  produces matches what our own reader expects, but **has never been opened by a real rippled
  process**.

**What a rippled cold-start from a reconstructed store actually requires (researched 2026-08-24,
from rippled source — not yet exercised):**

- **NuDB nodestore (`.dat` + `.key`)** — ✅ we can write this today.
- **`ledger.db`** — ⬜ **the one missing piece.** rippled reads it to learn which ledger
  sequences/hashes it holds and where to resume. `xrla-import` does not write it yet. This is a
  small, well-understood SQLite insert (`LedgerSeq`, `LedgerHash`, `PrevHash`, `AccountSetHash`,
  `TransSetHash`, close-time fields) — every value is already present and verified in the chunk,
  so this is wiring, not research.
- **`wallet.db`** — not needed. Despite the name it holds **no XRPL account data**: only this
  server's own P2P node identity keypair, peer reservations, and the validator-manifest cache.
  All XRPL accounts live in the account-state SHAMap inside the nodestore like any other ledger
  entry.
- **`transaction.db`** — not needed for startup; it is a convenience tx-lookup index.

  All three SQLite DBs are opened through `DatabaseCon` (`include/xrpl/rdb/DatabaseCon.h`), which
  lets SQLite create the file if absent and then runs `CREATE TABLE IF NOT EXISTS` from the
  `*DbInit` arrays in `include/xrpl/rdb/DBInit.h`. No startup path checks for a pre-existing file
  or refuses to start. If `wallet.db`'s `NodeIdentity` table is empty, `getNodeIdentity()`
  (`src/libxrpl/server/Wallet.cpp`) mints a fresh random keypair — fine for a non-validating
  server that only serves ledger data.

  So the remaining work to attempt a real cold start is: **write `ledger.db`, then launch rippled
  against the reconstructed directory and see what happens.** Everything else can be left for
  rippled to create.

### Phase 2: Full history export

**The `< 48 hours` target below is not yet validated, though the two blocking architectural
changes are now implemented.** `xrla-export` used to re-walk the full account-state trie from
scratch (`NuDBReader::collect_reachable`) for every chunk's checkpoint. Against a real
full-history node's total workload (~55–111 billion node touches across history — see
"Full-history server evidence" above) that was years, not hours, even before accounting for
cold-storage I/O. Two architectural changes were required before Phase 2 is realistic, plus a
third for operational safety over a multi-day run:

1. **Maintain live state across the whole export run, not per chunk.** ✅ done and
   **validated end-to-end against real data (2026-07-08)**. `xrla-export` keeps one running
   `HashMap<Hash256, SHAMapNode>` alive for the entire export (see `--chunk-size`, default
   10,000 ledgers). Each ledger's delta is applied to it as the range is scanned forward; at a
   chunk boundary, the current in-memory map is serialized as that chunk's checkpoint
   (`write_chunk` helper) instead of calling `collect_reachable` again. This reduces the number
   of full trie walks for the entire export from ~1 per chunk to **one, total**.

   Validation run: real mainnet NuDB snapshot (ledgers 105277428–105277528, 100 ledgers),
   `--chunk-size 30` → 4 chunks (30/30/30/11 ledgers). Log confirms exactly one
   `"Initial checkpoint"` line for the whole run (27,031,655 nodes, 77.5s — matching the earlier
   serial-walk baseline, now under the safety-capped concurrency from item 2). All three later
   chunks' checkpoints came from the in-memory snapshot with no further NuDB walk. Verified with
   `xrla-import` against both the first chunk (whose checkpoint came from the real walk) and the
   last, partial chunk (whose checkpoint came from the in-memory snapshot, and which also
   exercises the shorter-final-chunk edge case): every ledger's account_hash, chained LedgerHash,
   transaction authenticity, and full state-tree self-consistency (27M+ nodes each) passed with
   no failures. This is the first real-data validation of the multi-chunk architecture — the
   prior attempt was the run that caused the item-2 I/O incident and never completed.

2. **Concurrent/batched NuDB reads — adaptive, not a fixed constant.** ✅ done for
   `collect_reachable` (2026-07-08); ✅ also done for the per-ledger delta computation
   (2026-07-09, `feat/concurrent-diff-batching` branch, not yet merged to `main`) — see
   Immediate TODOs item 10b for full detail, including an honest **no speedup observed
   locally** finding.
   `NuDBReader::collect_reachable` used to fetch one node at a time, fully serially — each blocked
   on disk before the next was issued, leaving most of an NVMe drive's real IOPS capacity unused
   (single-threaded ~16K IOPS from ~60µs latency vs. ~100–200K+ IOPS achievable at real queue
   depth). Fixed by `collect_reachable_concurrent` (level-by-level BFS, fetches an entire frontier
   concurrently via a `rayon` thread pool before computing the next) plus `calibrate_concurrency` /
   `collect_reachable_adaptive`, which self-tune the in-flight count at runtime instead of using a
   hardcoded constant. `xrla-export` now calls `collect_reachable_adaptive` for its checkpoint
   walk. Correctness verified: `collect_reachable_concurrent` at concurrency 1/2/4/8, plus the
   adaptive path, all produce the identical node set as the original serial walk against a real
   multi-level test tree (`xrla-nudb/src/reader.rs::concurrent_walk_matches_serial_walk`).

   **2026-07-08 incident and fix.** The first version of this calibrated by climbing a hardcoded
   `[1, 4, 16, 64, 256]` ladder against the real target store, optimizing purely for the
   benchmark's own throughput. Run against two real ~6 GB files sitting on a laptop's single
   shared disk, 256 concurrent threads saturated I/O badly enough to make the whole machine
   unresponsive, requiring a hard restart. Root cause: "adaptive" meant "tuned for maximum
   throughput," not "safe to run here" — the calibration climbed by *live-testing* each level
   (so a dangerous level got a real trial run before anything could rule it out), used a relative
   "did throughput plateau" stopping rule with no absolute latency ceiling, and had no concept of
   whether the target disk was shared with the OS or genuinely dedicated. Fixed in
   `NuDBReader::calibrate_concurrency` / `shares_device_with_os_root`
   (`crates/xrla-nudb/src/reader.rs`, `crates/xrla-nudb/src/keyfile.rs::Shard::dat_device`):
   - Device detection (`st_dev` of the target `.dat` file vs. the OS root filesystem, fail-safe
     to "shared" if undetectable) selects the ladder: `[1, 2, 4, 8, 16, 32]` on dedicated storage,
     `[1, 2, 4]` when sharing a disk with the OS — nowhere near the old flat `256` ceiling either way.
   - An absolute per-request latency ceiling (50ms) aborts the climb immediately on real queueing,
     not just once *relative* throughput gain drops off — by the time a relative metric notices,
     damage to the rest of the machine may already be underway.
   - Regression test `calibration_never_exceeds_the_given_ladder` locks this in: calibration must
     always return a level from the exact ladder it was given.

   **Not yet done**: a continuous AIMD feedback loop that re-adapts mid-run (the current design
   only calibrates once, at the start of a walk); real-world speedup and the safe ceiling on real
   full-history *dedicated* hardware are still unmeasured (depends on item 8) — this incident was
   on a shared laptop disk, not the target production environment, so item 8 must run somewhere
   dedicated, never on anyone's daily-driver machine.

3. **Resumability**, a natural consequence of (1): a completed chunk file already contains its own
   full checkpoint (`Chunk.checkpoint`), and `deserialize_chunk` already verifies `chunk_hash` on
   read. On restart after a crash/interruption, load the *last complete, hash-verified* chunk's own
   checkpoint back into the in-memory state map (a local file read, no NuDB access) and resume delta
   processing forward from there — discarding only the in-progress chunk. This bounds lost work on
   a mid-export failure to one chunk's worth of (cheap) delta replay, never a repeat of the
   expensive one-time trie walk. Not implemented.

**Paper estimates** (real key-file-derived total of ~55–111 billion node touches, 3 physical reads
per node — bucket + dat header + dat value — instance-store NVMe, unmeasured against the real box):

| Configuration | Estimated total time |
|---|---|
| Current (re-walk per chunk, serial reads) | years — not viable |
| + maintain-state fix only (serial reads) | ~4–8 months |
| + maintain-state fix + concurrency (both fixes) | ~10–40 days |

These are order-of-magnitude estimates from known hardware classes and small-scale benchmarks, not
measurements. The only way to firm them up is running the real workload (or a timed raw-lookup loop)
against an actual full-history box.

- Scale to all 90M+ ledgers (parallel workers per non-overlapping range remains viable *in addition*
  to the above — e.g. one maintain-state worker per large sub-range)
- Measure actual total size vs the ~25–30 TB state-node floor estimate (partially validated above
  via real `nudb.dat`+`nudb.key` sizes; still need the tx-tree-node share broken out)
- Revised performance target: get a real, measured number for one maintain-state + concurrent
  worker against a real full-history box before committing to a multi-day full-history export plan

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

**Two distinct query shapes, different mechanisms — do not conflate (2026-07-09 discussion):**

1. **State-snapshot queries** (`AccountRoot` balance, `account_lines`, `account_nfts`, DEX order
   books) — "what did the account/book hold *at ledger N*." Answered by: reconstruct state at N
   (checkpoint + replay deltas forward, already implemented in `xrla-export`'s maintain-state loop,
   reused read-side), then walk/decode the relevant object(s) — direct leaf lookup for
   `AccountRoot`, an owned-object/directory walk for lines/NFTs/order books. **No new export or
   storage needed** — everything required is already in existing chunks. The only missing piece is
   a raw-STObject binary decoder (rippled's field-code binary format), which does not exist
   anywhere in this codebase yet. Same as how rippled itself answers these — a live on-demand
   SHAMap walk, not a persistent index.

2. **History-index queries** (`account_tx` — "what did this account do, and when"). Different
   mechanism entirely: no state reconstruction involved. Needs (a) a decoder for `tx_blob`/
   `meta_blob` (specifically `meta.AffectedNodes`, to know which accounts a transaction touched),
   already partially enabled by `tx_tree.rs`'s independent `TransactionHash` verification, and (b)
   a **persistent index** — `account → [(ledger_seq, tx_hash), ...]`, sorted — built once by
   scanning every transaction, since a live per-query scan across the whole archive would be far
   slower than rippled's/Clio's own indexed lookup. This is exactly how rippled (local SQLite tx-DB)
   and Clio (Cassandra table) answer it too — never a live re-scan.
   - Estimated cost, corrected 2026-07-09 after checking the real measured PoC rate (~90 tx/ledger
     recent, not the earlier ~4 tx/ledger guess — that guess was wrong, off by ~22x): full mainnet
     history is ~117M ledgers. Using the recent measured rate uniformly (upper bound, overestimates
     early history, which had far less activity): 117M × 90 × ~2.5 accounts/tx ≈ 26B entries ×
     ~40–60 bytes ≈ **~1–1.6 TB**. A blended estimate accounting for much lower early-years activity
     (rough, unmeasured): more like **~100–300 GB**. Honest range: **~100 GB to 1.5 TB** — still a
     small fraction (a few percent, not tens of a percent) of the ~25–30 TB archive floor either
     way, but not the "rounding error"/"tens of GB" this section originally said.
   - Should be a **separate rebuildable sidecar artifact**, not baked into the `.xrla` chunk format
     — it's a deterministic, fully re-derivable function of already-verified chunk data (rebuild
     any time from the immutable chunks with zero data loss), not part of the archive's core
     preservation contract. Build it in the same pass as export to avoid a second full re-read
     later, but keep it a separate file.

**Priority conclusion (2026-07-09): build `account_tx` before the state-snapshot queries.** It
answers what people actually ask ("what did this account do, and when"), not a supporting fact.
It's the direct fit for the anchor use case (a market maker reconciling their own account's full
history against ground truth — see `[[clio-full-history-vs-rippled]]` reasoning: they wanted
full-history rippled, not Clio, precisely because they needed to trust the completeness of an
account's history). It's also provably better than Clio's/rippled's own opaque side-index, because
the underlying tx+meta blobs are already cryptographically anchored (`tx_tree.rs`) — the index is a
deterministic, re-derivable, auditable computation over verified data, not just rows you have to
trust. AccountRoot/lines/NFTs/order-book decoding remain real, valuable, and reuse the same
STObject-parser foundation, but are secondary to this.

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

6. **Automated checkpoint anchoring** (`--verify-checkpoint-rpc`): a chunk's checkpoint
   ledger is currently only self-consistency-checked (recomputed from `ledger.db`'s own
   fields) — this can't catch a single diverged/amendment-blocked source serving an
   internally-consistent-but-wrong fork (the exact failure mode discussed in a Clio incident
   where "clio trusts rippled data" with no cross-check led to corrupted ETL state from a
   diverging source). Close this by querying independent nodes' `ledger` RPC for the
   checkpoint's `ledger_hash` and requiring quorum agreement:
   - **Recent ledgers** (within any public node's retention window): query 2-3 independent
     public endpoints (e.g. `s1.ripple.com`, `s2.ripple.com`, `xrplcluster.com`), require
     `validated: true` + matching `ledger_hash`. Already proven manually once (see
     `spec/chunk-format.md` "Verification without full history"); just needs wiring into
     `xrla-export`/`xrla-import` as an automated flag.
   - **Old ledgers** (outside most nodes' retention): skip multi-node quorum, source directly
     from a full-history node already trusted as the export source. Note Ripple's own FH
     nodes are IP-allowlisted (`secure_gateway`), not open like `s1`/`s2` — a genuinely
     independent second FH source for cross-checks would need a self-hosted FH node or a
     non-Ripple-operated public FH provider.

7. **Leaf-node (`AccountState`) hash verification** — ✅ done. A real rippled Docker
   snapshot (`xrpl-sensor` container, stopped but its volumes intact) turned out to still be
   available and was used to derive and confirm the formula: leaf nodes hash as
   `SHA512half(content)` directly, no `HashPrefix` needed — rippled's on-disk payload
   already embeds whatever it needs, the same pattern already known for transaction leaves.
   Confirmed against the *entire* real checkpoint (27,031,655 nodes: 7,912,690 inner +
   19,118,965 leaves), zero mismatches — not a sample. Implemented in
   `xrla_common::state_tree::verify_state_nodes` (covers both inner and leaf nodes now) and
   wired into `xrla-import`'s replay path; real-data-gated regression test at
   `xrla-nudb/src/reader.rs::real_snapshot_state_nodes_self_verify`.

8. **Real cold-read latency benchmark against an actual full-history NuDB.** The synthetic
   benchmark (`crates/xrla-nudb/examples/bench_lookup.rs`) proved the lookup *algorithm* is
   O(1) up to a 180 MB key file, cache-warm on local SSD. It does not measure real latency
   against a genuinely multi-TB key file that can't fit in RAM (a real full-history box, e.g.
   `livenet-fh-usw2-01`'s 4.0 TB `nudb.key`). Needs a timed raw `NuDBReader::get_node` loop run
   directly against real full-history hardware (read-only, no export/write side effects) to
   replace the current 50–150µs/read paper estimate with a measured number. **Must run on
   dedicated hardware only** — see the 2026-07-08 incident under Phase 2 item 2: a concurrency
   experiment on a shared laptop disk saturated I/O badly enough to require a hard restart.
   Never run concurrency/throughput experiments against a daily-driver machine's disk again.
   **Now also the sole blocker for validating item 10b's speed benefit** — the local test there
   showed no speedup (page-cache-warm data has no I/O latency for concurrency to hide), so this
   benchmark is the only way to find out whether the concurrent-diff-batching work helps at all
   on real, cold, dedicated storage.

9. **Maintain-state-across-chunks export architecture** — ✅ done and validated end-to-end
   against real data (2026-07-08). See Phase 2 item 1 for the full validation run detail:
   `xrla-export` now performs exactly one full trie walk per invocation (via
   `collect_reachable_adaptive`), regardless of how many chunks the requested range spans.
   Added `--chunk-size` (default 10,000 ledgers); the export loop maintains a running
   `HashMap<Hash256, SHAMapNode>` across the whole run, applies each ledger's delta to it, and
   at every chunk boundary snapshots the current map as that chunk's checkpoint (`write_chunk`
   helper) instead of re-walking NuDB. Only the very first chunk's checkpoint costs a real walk.
   Real-data run: 100 real mainnet ledgers, `--chunk-size 30` → 4 chunks, exactly one trie walk
   logged, all 4 chunks written; `xrla-import` verified the first chunk (real-walk checkpoint)
   and the last, partial chunk (in-memory-snapshot checkpoint, shorter-final-chunk edge case) —
   every account_hash, chained LedgerHash, and state-tree self-consistency check passed.

10a. **Concurrent/batched NuDB reads for `collect_reachable`, adaptive concurrency** — ✅ done
    (2026-07-08), **hardened same day after an I/O-saturation incident** — see Phase 2 item 2 for
    full detail. Added `NuDBReader::collect_reachable_concurrent` (level-by-level BFS walk,
    fetches an entire frontier via a `rayon` thread pool instead of one node at a time),
    `calibrate_concurrency` / `calibrate_concurrency_with_levels` (startup probe against the real
    store; ladder and absolute-latency circuit breaker chosen based on whether the target shares
    a disk with the OS — see `shares_device_with_os_root`), and `collect_reachable_adaptive`
    (calibrate then walk) as the entry point `xrla-export` now calls for its checkpoint walk.
    Correctness tested against the original serial walk at multiple concurrency levels on a real
    multi-level tree — identical node sets; a dedicated regression test
    (`calibration_never_exceeds_the_given_ladder`) locks in that calibration can never exceed
    the ladder it's given. Added `rayon` as a workspace dependency. Real-world speedup on real
    (dedicated) full-history hardware is still unmeasured (depends on item 8); the calibration
    is one-shot at walk start, not a continuous feedback loop (see item 11).

10b. **Concurrent/batched delta computation across many ledgers** — ✅ implemented and
    correctness-validated (2026-07-09, branch `feat/concurrent-diff-batching`, **not yet
    merged to `main`**). Took approach (b) from the original plan below: rather than
    parallelizing inside one ledger's `diff_nodes` recursion (which mutates a shared
    `&mut SHAMapDiff` and doesn't fit 10a's level-by-level shape), added
    `NuDBReader::diff_batch_concurrent(pairs, concurrency)` — since every ledger's
    `(old_root, new_root)` pair is known upfront from `ledger.db`, it runs `diff()` for many
    ledgers concurrently via the same `rayon` thread-pool pattern as `collect_reachable_concurrent`,
    reusing the *same* calibrated concurrency level (no new calibration, no new safety surface).
    `xrla-export`'s main loop now processes ledgers in batches of `concurrency` size: discovery
    (the diffs) happens in parallel, application to the running `state` map stays strictly
    sequential (each ledger's starting state depends on the previous one already being applied).

    **Correctness**: strong evidence, not just a unit test. Synthetic test
    (`diff_batch_concurrent_matches_serial`) confirms batched and serial results match at
    concurrency 1/2/4 on a small multi-version tree. More importantly, a real 500-ledger export
    (105277428–105277928) run twice — once batched, once with concurrency forced to 1 — produced
    **byte-identical `chunk_hash`** (`de734d235a4acf...`) and identical totals (1,043,162 added
    nodes, 46,064 real transactions) either way.

    **Honest speed finding: no local speedup observed — likely correct, not a red flag.**
    Three real timed runs on the same 500-ledger range (checkpoint-walk time excluded, only the
    delta-processing loop compared): batched-cold ~56.6s, serial-warm ~26.3s, batched-warm
    ~48.2s. Batched was *slower* even under the most cache-favorable conditions tested. Read as:
    concurrency only pays off when there's real disk latency to hide (the whole premise from the
    `bench_lookup` synthetic benchmark); on this small, page-cache-warm local dataset each
    `diff()` call is already fast (low milliseconds), so thread-pool coordination overhead
    exceeds any latency saved. This does not indicate a design flaw — it reinforces that the
    real validation can only come from item 8's genuinely-cold, dedicated-hardware benchmark,
    not a local warm-cache test. Do not claim a speed benefit until that measurement exists.

    Original plan for reference: (a) restructure `diff_nodes`' recursion into a fork-join over
    the 16 child slots with independent per-branch diffs merged at the end, or (b) the coarser,
    higher-leverage win of running `diff()` for many *ledgers* concurrently instead of
    parallelizing inside one ledger's (typically small, ~2,000-node) diff — (b) is what got built.

11. **Continuous AIMD feedback loop for concurrency** — not started, follow-up to 10a. The
    current calibration is one-shot at the start of a walk; a continuous loop (track rolling
    p50/p99 latency over a sliding window, increase in-flight count while throughput climbs, back
    off multiplicatively when latency spikes — same idea as TCP congestion control) would also
    re-adapt if conditions change mid-run (e.g. other processes start competing for I/O on the
    box), which a one-time calibration pass misses.

12. **Export resumability** — see Phase 2 item 3. Depends on (9). On startup, detect the last
    complete, `chunk_hash`-verified chunk file, load its own checkpoint back into the in-memory
    state map (local read, no NuDB access), and resume delta processing forward from there,
    discarding any partially-written chunk. Bounds lost work on a mid-export crash/interruption
    to one chunk's delta-replay cost, not a repeat of the full trie walk. Not started.

13. **`account_tx` decoder + index (2026-07-09; decoder ✅ 2026-07-28, index ⬜)**. See Phase 4
    "Two distinct query shapes" above for the full architecture and reasoning.

    ✅ **Decoder done** — `xrla-common/src/meta_decode.rs`. A generic decoder for rippled's
    canonical binary STObject format that finds every `AccountID`-typed field at any nesting
    depth inside a `meta_blob`, plus `account_id_to_classic_address` (base58check r-address
    encoding). Deliberately type-driven rather than field-name-driven: it implements the
    type-level wire rules only, avoiding a hand-copied field-code table (the same class of
    silent, hard-to-spot error as the 2026-07-08 sparse-inner-node bit-order bug). Type codes
    confirmed against rippled `SField.h` `SerializedTypeID` and `ripple-binary-codec`
    `definitions.json`.

    ✅ **Live query works, unindexed** — `xrla-inspect --account <r-address>` scans a chunk's
    `meta_blob`s on demand and lists every transaction touching that account, tagged with its
    ledger. This already delivers the demoable end state *for a single chunk*, cross-checkable
    against the verified `TransactionHash`/`LedgerHash` chain.

    ⬜ **Remaining**: the persistent index — a one-time read-only pass over existing chunks
    producing `account → [(ledger_seq, tx_hash), ...]` sorted by `ledger_seq`, written as a
    separate rebuildable sidecar file per archive range (not embedded in `.xrla` chunks). Needed
    because a live scan across a full-history archive is far slower than an indexed lookup — the
    same reason rippled and Clio both maintain their own tx indexes rather than re-scanning.

    ✅ **Cross-checked against a live independent source, not just self-consistent.** Ran clean
    across all 1,236 real transactions in a real 10-ledger export (105277428–105277438) — zero
    decode errors, after fixing one real gap it exposed live (`STI_ISSUE`, type 24, added for AMM —
    not originally handled; fixed in ~2 minutes once hit). For one real account
    (`rf7QoGcRk2aFMSQNY3zt6FsADavoVucLni`), found 3 matching transactions in that range; all 3
    independently confirmed against a live public API (xrpscan) — exact match on `Account`,
    `Destination`, and `ledger_index` for every one, zero false positives.

    ⚠️ **Remaining verification gap**: still **no automated unit tests**, and completeness (false
    negatives — does it ever *miss* an account reference) is unverified, not just untested. A real
    `account_tx` RPC comparison would settle this but wasn't achieved this session: xrpscan's
    account-transactions endpoint ignored the ledger-range filter and only returned its most recent
    page. Still needs: known-vector tests (cross-checked against an independent implementation),
    malformed/truncated-input cases, and a real range-scoped `account_tx` comparison against a node
    whose retention covers the test range. The current `--account` path silently warns and skips on
    decode failure, so a systematically broken decode would look like "no matches" rather than a
    failure — this is exactly why the completeness gap matters more than it might otherwise.

14. **AccountRoot/lines/NFTs/order-book decoder (secondary, after 13)**. Shared foundation: a raw
    STObject binary parser (rippled's field-code binary format — does not exist anywhere in this
    codebase yet). `AccountRoot` balance-at-ledger-N is the simplest case (single direct leaf
    lookup after replay-to-N). `account_lines`/`account_nfts` add an owned-object/directory walk
    (same replay-to-N, different object type + a linked-list walk). DEX order-book reconstruction
    additionally needs the book-base hash computation + directory-page walk. All three read
    existing chunk data only — no new export. Not started.
