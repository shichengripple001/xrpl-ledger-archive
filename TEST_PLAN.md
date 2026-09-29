# XRPL Ledger Archive — Test Plan

> **Status as of 2026-09-29**: most of this document is still aspirational — it describes
> tests to write, not tests that exist. The tests marked ✅ **IMPLEMENTED** are real,
> committed, and passing (`cargo test --workspace`). Everything else in this document is
> still a plan, not code — do not assume a described test exists just because it's listed
> here.
>
> Actual inventory: **19 tests** across 6 files —
> `xrla-common/serialize.rs` (2: `streamed_chunk_round_trips_through_deserialize_chunk`,
> `dropping_writer_without_finish_removes_tmp_file`),
> `xrla-common/state_tree.rs` (4), `xrla-common/tx_tree.rs` (4),
> `xrla-nudb/reader.rs` (4: `concurrent_walk_matches_serial_walk`,
> `calibration_never_exceeds_the_given_ladder`, `diff_batch_concurrent_matches_serial`,
> `real_snapshot_state_nodes_self_verify`),
> `xrla-nudb/writer.rs` (3: `round_trip_small_store`, `round_trip_forces_spill_chain`,
> `real_snapshot_roundtrip_via_writer`),
> `xrla-import/main.rs` (2: `two_ledger_chunk_replays_and_verifies`,
> `write_ledger_db_merges_into_an_existing_populated_file`).
> `xrla-export` and `xrla-inspect` have no unit tests at all.
>
> **Not covered by any test, and worth adding**: v2-vs-v3 format dispatch in
> `deserialize_chunk` (only v3 is round-trip tested), multi-chunk `xrla-import --chunk`,
> and a peak-RSS regression guard for the streaming exporter (the bug it fixed was an OOM,
> which no correctness test would have caught).

---

## Test Levels

### 1. Unit Tests (per crate)

This section used to list ~35 named unit tests for `serialize.rs`, `shamap.rs`, `dat.rs`,
`keyfile.rs`, and `reader.rs` that were never written — they sat here unimplemented from
2026-07-01 to 2026-09-29. They have been removed rather than carried indefinitely as a wish
list; what follows is the real inventory. Add tests here *when they exist*.

**`xrla-common`**
- `state_tree.rs` (4) — `genuine_inner_node_verifies`, `genuine_leaf_node_verifies`,
  `tampered_inner_content_is_caught`, `tampered_leaf_content_is_caught`
- `tx_tree.rs` (4) — `empty_tree_is_zero_hash`, `single_tx_root_is_inner_with_one_child`,
  `two_txns_sharing_first_nibble_split_at_second_level`, `write_vl_matches_read_vl_boundaries`
- `serialize.rs` (2) — `streamed_chunk_round_trips_through_deserialize_chunk` (v3 write →
  `deserialize_chunk`), `dropping_writer_without_finish_removes_tmp_file`

**`xrla-nudb`**
- `reader.rs` (4) — `concurrent_walk_matches_serial_walk`,
  `calibration_never_exceeds_the_given_ladder`, `diff_batch_concurrent_matches_serial`,
  `real_snapshot_state_nodes_self_verify`
- `writer.rs` (3) — `round_trip_small_store`, `round_trip_forces_spill_chain`,
  `real_snapshot_roundtrip_via_writer`

**`xrla-import`**
- `main.rs` (2) — `two_ledger_chunk_replays_and_verifies`,
  `write_ledger_db_merges_into_an_existing_populated_file`

**`xrla-export`, `xrla-inspect`** — no unit tests.

**19 total.** Real gaps worth closing, in priority order:
1. **v2/v3 format dispatch** — only v3 is round-trip tested; nothing exercises reading a v2
   chunk, even though v2 files exist on disk and `deserialize_chunk` still supports them.
2. **Multi-chunk `xrla-import --chunk`** — the union-across-chunks path is untested.
3. **Peak-RSS regression guard for the exporter** — the bug that forced format v3 was an OOM,
   which no correctness test would have caught.
4. **Deliberate tamper detection** — `deserialize_chunk` verifies `chunk_hash` on every read,
   but no test flips a byte and asserts the failure.


### 2. Determinism Tests (critical)

These are the most important tests. They prove the format is suitable for P2P distribution.

**test_determinism_same_process**
- Build a known SHAMap tree in memory
- Export to chunk twice with identical inputs
- Assert output bytes are identical

**test_determinism_two_nudb_copies**  ✅ verified 2026-06-30
- Run xrla-export twice on the same shard snapshot for the same ledger range
- Assert chunk files are byte-identical
- Result: ledgers 105277428–105277478 → identical `chunk_hash`
  `91e4984187ec676801c56d34174f6acaaa62714a3eab1f247d06fb4566ecf2a2`
- NOTE: determinism is necessary but NOT sufficient — a deterministic decode bug (sparse-inner
  bit order) produced a stable but *wrong* `54e2226a…` before the correctness check below caught
  it. Always pair determinism with hash verification.

**test_correctness_checkpoint_root**  ✅ verified 2026-06-30
- Parse the exported chunk, recompute SHA-512/half(innerNode-prefix + content) for every
  checkpoint inner node, assert it equals the node's stored hash
- Assert the root node hashes to the ledger's on-chain `AccountSetHash`
- Result: 7,912,690 inner nodes, 0 mismatches; root == `ca718659…` ✅

**test_correctness_transactions**  ✅ verified 2026-06-30
- For each TX_MAP record assert `tx_hash == SHA512half(HashPrefix::transactionID + tx_blob)`
- For each ledger, rebuild the transaction SHAMap from its records (leaf =
  `SHA512half('SND\0' + VL(tx) + VL(meta) + tx_hash)`, inner = `SHA512half('MIN\0' + 16 children)`)
  and assert the root equals the on-chain `TransSetHash`
- Result: 4,500/4,500 txids authentic; 51/51 ledger tx-tree roots match ✅
  (proves completeness + metadata correctness, not just per-tx authenticity)

**test_correctness_ledger_hash**  ✅ verified 2026-06-30
- Verified `calculate_ledger_hash()` formula (seq, drops, parent_hash, tx_hash, account_hash,
  parent_close_time, close_time, close_time_resolution, close_flags, HashPrefix::LedgerMaster
  "LWR\0") against a real ledger.db row — recomputed hash matched the DB's `LedgerHash` exactly
- Exporter now recomputes + asserts this for every ledger in the range (aborts on mismatch) and
  stores it in each `TxMap.ledger_hash`
- Result: 51/51 ledgers verified during export; extracted `ledger_hash` for ledger 105277428
  from the output chunk matches the hand-verified value `1E0805A3…` ✅
- Regression test to add: a synthetic ledger row with a deliberately wrong field (e.g. flipped
  `CloseFlags`) must make the exporter `bail!`, not silently accept it

**test_determinism_different_node_insertion_order**
- Build same SHAMap tree by inserting nodes in two different orders
- Export to chunk from each
- Assert output bytes are identical
- (This verifies that hash-sorting produces the same result regardless of how nodes were inserted into the store)

---

### 3. Integration Tests

**two_ledger_chunk_replays_and_verifies** ✅ IMPLEMENTED (`crates/xrla-import/src/main.rs`)
- Synthetic 2-ledger chunk (checkpoint + 1 delta), built with the *real* hash formulas
  (`build_tx_tree`, `calculate_ledger_hash`), fed through `replay_chunk` end-to-end
- Asserts: final live state is exactly ledger B's nodes (ledger A's superseded leaf/root are
  gone); a tampered stored `ledger_hash` is caught and rejected, not silently accepted
- This is the wiring test the unit tests above can't be: it wouldn't have caught the
  original `verify_ledger_hashes` bug (comparing against `checkpoint_hash`, a LedgerHash,
  instead of the checkpoint's actual `account_hash`) — building this test is what surfaced
  and fixed that bug
- **Known gap**: uses hand-built synthetic nodes, not a real multi-ledger mainnet range —
  see `test_export_import_roundtrip` below for what's still missing

**test_export_import_roundtrip** — ✅ **DONE 2026-09-28**, as a manual run rather than a
committed test
- Ran as described: exported a real range, imported into a reconstructed store, pointed a
  real xrpld at it, and queried — every `account_hash`, account balance, and transaction
  checked matched the untouched ground-truth node.
- Stronger than originally specified: the range was first deleted from **all 7 nodes** of the
  PoC network, so no peer could have supplied the data the node served back.
- Three real bugs surfaced only at this level, none catchable by our own reader: the NuDB
  `pepper` bug (real xrpld rejects the store with `hash_mismatch`), a missing `ledger.db`
  writer, and a destructive-overwrite bug in the first version of that writer.
- **Still not automated.** This was a hand-driven run against live infrastructure; nothing in
  `cargo test` covers it, so it will not catch a regression. Automating even a scaled-down
  version remains open.

**test_hash_verification** — ✅ closed. `two_ledger_chunk_replays_and_verifies` covers the
wiring, `test_correctness_ledger_hash` (below) the formula, and the 2026-09-28 run above
closed the *real multi-ledger, real xrpld process* level.

**test_chunk_tamper_detection**
- Export a valid chunk
- Flip one byte in the body
- Attempt to deserialize → expect `ChunkError::HashMismatch`
- `deserialize_chunk` (`crates/xrla-common/src/serialize.rs`) already implements this check
  on every read; no dedicated test exists yet exercising a deliberately-flipped byte

**test_import_rejects_corrupt_chunk**
- Export a valid chunk
- Flip one byte in a delta
- Run xrla-import → expect failure with clear error message
- Partially covered by `two_ledger_chunk_replays_and_verifies`'s tampered-`ledger_hash`
  case; a dedicated test flipping bytes in an *added node* (not just the stored hash)
  would exercise a different failure path and is still open

---

### 4. PoC Validation Tests

Run against a real xrpld node (testnet or devnet sufficient).

**test_poc_delta_sizes**
- Export consecutive ledgers, print per-ledger delta size
- Expected range (mainnet, uncompressed wire bytes): **~1.0–1.6 MB/ledger**, ~2,000–2,700
  changed nodes/ledger. Measured at the live tip 2026-09-29: 1.4 MB and ~2,620 nodes; the
  2026-06-30 snapshot gave 1.02 MB and ~1,966. *(The earlier "~35 KB" target was wrong — it
  assumed 350K ledgers/day; XRPL is ~21,600/day. See PLAN.md Storage Estimate.)*
- Assert no single delta is 0 bytes (every ledger has some state change)

**test_poc_checkpoint_size**
- Export checkpoint for one ledger
- Print size
- Baseline for estimating full-history chunk overhead

**test_poc_determinism**
- Export same 1000-ledger range twice from same NuDB
- `diff` the two output files
- Assert: no differences

---

### 5. Performance Benchmarks

Not pass/fail — baseline measurements to track over time.

| Benchmark | What it measures |
|---|---|
| `bench_diff_1_ledger` | Time to compute diff between 2 consecutive ledgers |
| `bench_serialize_checkpoint` | Time to serialize full state at one ledger |
| `bench_export_1000_ledgers` | End-to-end export throughput (ledgers/sec) |
| `bench_import_1000_ledgers` | End-to-end import throughput (ledgers/sec) |
| `bench_keyfile_fetch` | `.key` lookup latency (single + full-tree traversal) |

PoC baseline (50 ledgers + 27M-node checkpoint, mainnet snapshot, 2026-06-30): **~1m45s**,
dominated by the full-state checkpoint traversal (27M key-file lookups). Per-ledger delta
diffs are O(changed nodes) and fast; the checkpoint is the cost.

---

## Test Data

For unit tests: construct synthetic NuDB `.dat` files and SHAMap trees in memory.
No real xrpld data needed.

For integration tests: use a local testnet or devnet xrpld node.
A non-full-history node is sufficient as long as the target ledger range is still on disk.

For performance benchmarks: use mainnet data if available, testnet otherwise.

---

## Running Tests

```bash
# Unit tests
cargo test --workspace

# Integration tests (requires local xrpld node)
XRPLD_DAT=/var/lib/xrpld/db/nudb.dat \
XRPLD_LEDGERS=/var/lib/xrpld/db/ledger.db \
cargo test --workspace -- --include-ignored

# Determinism test (export same range twice, diff output).
# Pass every online_delete shard's .dat (each needs a sibling nudb.key); the state spans both.
SHARDS="--dat /snap/shard0/nudb.dat /snap/shard1/nudb.dat"
cargo run --release --bin xrla-export -- $SHARDS --ledgers $LEDGERS --start 1000000 --end 1001000 --out /tmp/run1
cargo run --release --bin xrla-export -- $SHARDS --ledgers $LEDGERS --start 1000000 --end 1001000 --out /tmp/run2
diff /tmp/run1/xrla_1_01000000_01001000.xrla /tmp/run2/xrla_1_01000000_01001000.xrla && echo PASS

# Benchmarks
cargo bench --workspace
```
