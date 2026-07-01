# XRPL Ledger Archive — Test Plan

> **Status as of 2026-07-01**: most of this document was aspirational — it described tests
> to write, not tests that existed (`grep -rn "#\[test\]" crates` returned zero matches
> before the import/round-trip work below). The tests marked ✅ **IMPLEMENTED** are real,
> committed, and passing (`cargo test --workspace`). Everything else in this document is
> still a plan, not code — do not assume a described test exists just because it's listed
> here.

---

## Test Levels

### 1. Unit Tests (per crate)

#### xrla-common

**serialize.rs**
- `test_sha512half`: known input → verify SHA-512/half output matches reference vector
- `test_node_roundtrip`: serialize a SHAMapNode, deserialize, assert fields equal
- `test_node_list_sorted`: write unsorted nodes, read back, assert sorted by hash
- `test_delta_roundtrip`: serialize LedgerDelta with adds + deletes, deserialize, assert equal
- `test_tx_map_roundtrip`: serialize TxMap with multiple txns, deserialize, assert equal
- `test_chunk_hash_coverage`: chunk_hash must cover body bytes, not header — modify one body byte, assert hash changes
- `test_magic_validation`: deserialize with wrong magic → expect `ChunkError::InvalidMagic`
- `test_version_validation`: deserialize with version=99 → expect `ChunkError::UnsupportedVersion`

**shamap.rs**
- `test_inner_node_parse`: construct known bitmask + child hashes, parse with `InnerNode::from_bytes`, assert children match
- `test_inner_node_empty`: bitmask=0 → all children None
- `test_inner_node_all_children`: bitmask=0xFFFF → 16 children, assert all present
- `test_inner_node_truncated`: content too short → expect error

**tx_tree.rs** ✅ IMPLEMENTED (`crates/xrla-common/src/tx_tree.rs`)
- `empty_tree_is_zero_hash`: no transactions → root is `ZERO_HASH`, no nodes
- `single_tx_root_is_inner_with_one_child`: one tx → root inner has exactly one non-zero
  slot (matching the tx_hash's first nibble), leaf hash matches the documented formula
- `two_txns_sharing_first_nibble_split_at_second_level`: two keys sharing nibble 0 produce
  a second-level inner node, not a collision
- `write_vl_matches_read_vl_boundaries`: round-trips VL encoding across all three length
  classes (1/2/3-byte prefixes) against a standalone copy of reader.rs's `read_vl`

#### xrla-nudb

**writer.rs** ✅ IMPLEMENTED (`crates/xrla-nudb/src/writer.rs`)
- `round_trip_small_store`: write 50 synthetic entries, read every one back via
  `keyfile::Shard::fetch`, assert byte-identical; assert an absent key returns `None`
- `round_trip_forces_spill_chain`: 500 entries into a small bucket table, forcing the
  spill-chain write/read path (not just primary buckets)
- `real_snapshot_roundtrip_via_writer` *(ignored — needs a real snapshot)*: samples ~200
  real, rippled-produced records directly from a live mainnet `nudb.dat`, round-trips them
  through `encode_wire_to_value` → `write_nudb_store` → `Shard::fetch` →
  `decode_value_to_wire`, and asserts wire-byte equality. Run with:
  `RIPPLED_DAT=/path/to/nudb.dat cargo test --package xrla-nudb --lib -- --ignored real_snapshot`
  — passed against the 5.8 GB mainnet shard captured earlier in this project (196/196 nodes
  round-tripped exactly).

**dat.rs** (value decoding)
- `test_decode_full_inner`: codec `0x03` + 512 bytes → 512-byte content + Inner type byte
- `test_decode_sparse_inner`: codec `0x02` + mask + N hashes → expanded 512-byte inner
- `test_decode_sparse_inner_bit_order` *(regression)*: a known node whose `SHA512half(MIN\0 +
  expanded)` equals its hash — pins the big-endian branch order (`mask & (0x8000>>s)`); the
  reversed `1<<s` mapping must fail this
- `test_decode_lz4_leaf`: codec `0x01` LZ4 EncodedBlob → AccountState/TxWithMeta wire bytes
- `test_decode_ledger_object`: NodeObjectType=1 → None (not part of account SHAMap)

**keyfile.rs** (`.key` hash-table lookup)
- `test_header_parse`: write a minimal `nudb.key` header → assert salt, block_size, num_buckets
- `test_bucket_index`: known nhash + modulus/num_buckets → expected bucket (incl. the
  `>= num_buckets → -= modulus/2` linear-hashing fixup)
- `test_fetch_present`: synthetic 1-bucket shard with one entry → `fetch(key)` returns its value
- `test_fetch_absent`: `fetch` of a key not in the shard → `Ok(None)`
- `test_fetch_prefix_collision`: two entries sharing the 48-bit nhash → full-key verify picks right one
- `test_fetch_spill_chain`: bucket with a `.dat` spill record → entry in spill is found
- *(regression)* `test_real_shard_state_root`: against a captured shard fixture, `fetch` a known
  `AccountSetHash` → returns a 513-byte full-inner value

**reader.rs**
- `test_multishard_fallback`: node present only in shard1 → `get_node` finds it after shard0 miss
- `test_get_missing`: get non-existent hash → `get_wire` returns `Ok(None)`
- `test_parse_tx_leaf`: content `['SND\0'][VL(tx)][VL(meta)][txid]` → `TxRecord` with correct
  blobs and tx_hash; also exercises 2- and 3-byte VL length prefixes
- `test_collect_transactions_empty`: `collect_transactions(ZERO_HASH)` → empty vec
- `test_collect_reachable_single_leaf`: root = one leaf node → collect returns just that node
- `test_collect_reachable_tree`: build a 3-level tree, collect from root → all nodes returned
- `test_diff_identical_roots`: old_root == new_root → diff returns empty added + deleted
- `test_diff_leaf_changed`: swap one leaf → diff returns old leaf in deleted, new in added
- `test_diff_leaf_added`: add one new leaf → diff returns new leaf in added, nothing deleted
- `test_diff_leaf_deleted`: remove one leaf → nothing in added, old leaf in deleted
- `test_diff_inner_node_updated`: change one leaf deep in tree → only changed path nodes in diff

---

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

**test_export_import_roundtrip** — NOT YET RUN end-to-end
- Export ledger range [N, N+100] from a real rippled NuDB, import into a fresh NuDB, open
  with a real rippled process, query ledger N+50 — assert state matches original node
- Blocked in this environment: the populated `ledger.db` used for earlier verification
  work is gone (only an empty one remains); a real multi-GB `nudb.dat`/`nudb.key` shard is
  still present and was used for `real_snapshot_roundtrip_via_writer` above, but that test
  only proves node *content* round-trips, not a full ledger range with real header data,
  and nothing here has been tested against an actual rippled process opening the result
- To close this gap: re-run `xrla-export` against a fresh rippled snapshot with a populated
  `ledger.db`, then `xrla-import` the result, then point a real rippled at the output and
  query it

**test_hash_verification** — superseded by `two_ledger_chunk_replays_and_verifies` for the
wiring, and by `test_correctness_ledger_hash` (below) for the formula itself; still open at
the *real multi-ledger, real rippled process* level described in `test_export_import_roundtrip`.

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

Run against a real rippled node (testnet or devnet sufficient).

**test_poc_delta_sizes**
- Export consecutive ledgers, print per-ledger delta size
- Expected range (mainnet, uncompressed wire bytes): **~0.6–1.6 MB/ledger**, ~2,400 changed
  nodes/ledger. *(The earlier "~35 KB" target was wrong — it assumed 350K ledgers/day; XRPL is
  ~21,600/day. See PLAN.md Storage Estimate.)*
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
No real rippled data needed.

For integration tests: use a local testnet or devnet rippled node.
A non-full-history node is sufficient as long as the target ledger range is still on disk.

For performance benchmarks: use mainnet data if available, testnet otherwise.

---

## Running Tests

```bash
# Unit tests
cargo test --workspace

# Integration tests (requires local rippled node)
RIPPLED_DAT=/var/lib/rippled/db/nudb.dat \
RIPPLED_LEDGERS=/var/lib/rippled/db/ledger.db \
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
