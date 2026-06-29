# XRPL Ledger Archive — Test Plan

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

#### xrla-nudb

**dat.rs**
- `test_header_parse`: write a minimal valid NuDB header, parse with `read_header`, assert fields
- `test_scan_empty_dat`: dat file with header only → scan returns empty map
- `test_scan_single_record`: dat file with one key-value → scan returns that entry
- `test_scan_multiple_records`: dat file with N known entries → scan returns all

**reader.rs**
- `test_get_existing`: build NuDBReader with known store, get existing hash → returns content
- `test_get_missing`: get non-existent hash → returns None
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

**test_determinism_two_nудб_copies**
- Copy a real NuDB .dat file to two paths
- Run xrla-export on both copies for the same ledger range
- Assert chunk files are byte-identical

**test_determinism_different_node_insertion_order**
- Build same SHAMap tree by inserting nodes in two different orders
- Export to chunk from each
- Assert output bytes are identical
- (This verifies that hash-sorting produces the same result regardless of how nodes were inserted into the store)

---

### 3. Integration Tests

**test_export_import_roundtrip**
- Export ledger range [N, N+100] from a real rippled NuDB
- Import into a fresh NuDB
- Open fresh NuDB with rippled
- Query ledger N+50 — assert state matches original node

**test_hash_verification**
- Export chunk for ledger range [N, N+100]
- For each ledger in range: replay delta, compute root hash
- Fetch ledger header from rippled node, extract state hash
- Assert computed root hash == on-chain state hash
- All 100 ledgers must pass

**test_chunk_tamper_detection**
- Export a valid chunk
- Flip one byte in the body
- Attempt to deserialize → expect `ChunkError::HashMismatch`

**test_import_rejects_corrupt_chunk**
- Export a valid chunk
- Flip one byte in a delta
- Run xrla-import → expect failure with clear error message

---

### 4. PoC Validation Tests

Run against a real rippled node (testnet or devnet sufficient).

**test_poc_delta_sizes**
- Export 1000 consecutive ledgers
- Print per-ledger delta size
- Assert average delta size < 1 MB/ledger (sanity check — real value expected ~35 KB)
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
| `bench_nudb_scan` | Time to scan N GB .dat file into memory |

Target for PoC: export 1000 ledgers in < 60 seconds on a machine with NVMe SSD.

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

# Determinism test (export same range twice, diff output)
cargo run --bin xrla-export -- --dat $DAT --ledgers $LEDGERS --start 1000000 --end 1001000 --out /tmp/run1
cargo run --bin xrla-export -- --dat $DAT --ledgers $LEDGERS --start 1000000 --end 1001000 --out /tmp/run2
diff /tmp/run1/xrla_1_01000000_01001000.xrla /tmp/run2/xrla_1_01000000_01001000.xrla && echo PASS

# Benchmarks
cargo bench --workspace
```
