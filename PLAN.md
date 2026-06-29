# XRPL Ledger Archive — Implementation Plan

## Problem

Running a full-history XRP Ledger node requires ~39 TB of NVMe SSD, growing 12 GB/day.
Getting that history from scratch via P2P takes several months because backfilling is the
lowest-priority task and is constrained to direct peers only.

There is no existing mechanism to share full history easily between operators.

History sharding (2018–2024) was the official attempt. It was removed in rippled v2.3.0
because the SHAMap structure caused every shard to duplicate unchanged InnerNodes across
ledger ranges — aggregate shard storage exceeded a single full-history node.

## Solution

Define a canonical chunked archive format for XRPL ledger history.

Each chunk covers a range of ledgers and encodes only the **delta** between consecutive
ledger states — the SHAMap nodes that actually changed. Unchanged nodes are not repeated.
Chunks are deterministic, content-addressed, and self-verifying against on-chain ledger hashes.

A new operator downloads chunks in parallel from any source (S3, torrent, IPFS, peer),
verifies each chunk against on-chain hashes, imports into rippled NuDB, and is fully
bootstrapped in hours instead of months.

No protocol changes. No XLS amendment. Pure tooling on top of rippled.

---

## Core Concepts

### SHAMap Delta

The XRPL ledger state is a SHAMap — a Merkle Patricia trie where:
- Leaf nodes = ledger objects (accounts, trust lines, offers, escrows, ...)
- Inner nodes = branching nodes (up to 16 children, keyed by hash prefix)
- Every node is identified by SHA-512/half of its content
- Root hash = state hash committed in the ledger header (on-chain)

Between ledger N and N+1, only nodes along paths to changed leaves are modified.
The rest of the trie is identical (same hash = same content = no need to store again).

rippled already implements `SHAMap::visitDifferences()` which walks two SHAMap trees
and emits only the changed nodes. This is the core primitive we use.

### Determinism

Serialization order = sort all emitted nodes by their hash (ascending).
Two independent exporters running against identical ledger data will:
1. Find the same set of changed nodes (SHAMap is deterministic)
2. Sort by hash (deterministic)
3. Produce identical bytes

The chunk hash is then reproducible and verifiable by anyone.

### Verification

For any chunk:
1. Load checkpoint (full state at chunk start ledger)
2. Apply delta[1], compute root hash, compare against ledger header hash (on-chain)
3. Apply delta[2], compute root hash, compare
4. ... repeat for every ledger in the chunk
5. If any hash mismatches — chunk is corrupt or tampered, abort

Zero trust in the source required.

---

## Storage Estimate

NuDB already deduplicates nodes by hash. The 12 GB/day growth = genuinely new unique
nodes being added = the delta.

```
12 GB/day / ~350,000 ledgers/day = ~35 KB per ledger delta

Per chunk (100k ledgers):
  checkpoint (full state at one ledger): ~15 GB
  100k deltas x 35 KB:                  ~3.5 GB
  total:                                 ~18 GB

All history (90M ledgers, 900 chunks):
  deltas:      ~3 TB
  checkpoints: ~13 TB
  total:       ~16 TB  (vs 39 TB today)
```

These are estimates. The PoC measures actual delta sizes.

---

## Chunk Format

```
[HEADER]
  magic:            bytes[4]   = 0x58524C41  ("XRLA")
  version:          u8         = 1
  network_id:       u32        (1 = mainnet)
  start_ledger:     u32
  end_ledger:       u32
  checkpoint_hash:  bytes[32]  ledger hash at start_ledger (on-chain verifiable)
  chunk_hash:       bytes[32]  SHA-512/half of everything below this field

[CHECKPOINT]
  node_count:       u32
  nodes[]:
    hash:           bytes[32]
    type:           u8         (0 = inner, 1 = leaf)
    content:        u16 + bytes[]
  (sorted ascending by hash)

[DELTAS]
  for each ledger seq in [start_ledger+1 .. end_ledger]:
    ledger_seq:     u32
    added_count:    u32
    added[]:
      hash:         bytes[32]
      type:         u8
      content:      u16 + bytes[]
    deleted_count:  u32
    deleted[]:
      hash:         bytes[32]
    (added[] sorted ascending by hash)

[TX_MAPS]
  for each ledger seq in [start_ledger .. end_ledger]:
    ledger_seq:     u32
    tx_count:       u16
    txs[]:
      tx_hash:      bytes[32]
      tx_blob:      u32 + bytes[]
      meta_blob:    u32 + bytes[]

[FOOTER]
  end_magic:        bytes[4]   = 0x454E4458  ("ENDX")
```

---

## Project Structure

```
xrpl-ledger-archive/
├── spec/
│   └── chunk-format.md        full format specification
├── poc/
│   └── exporter.py            PoC: connect to rippled WebSocket, export 1000 ledgers
├── src/
│   ├── common/
│   │   ├── ChunkFormat.h      chunk format structs and constants
│   │   └── Serialization.cpp  deterministic serialization (hash-sorted)
│   ├── exporter/
│   │   ├── main.cpp           CLI: rippled node + ledger range → chunk files
│   │   ├── SHAMapDiff.cpp     wraps visitDifferences(), emits sorted node list
│   │   ├── NuDBReader.cpp     reads rippled NuDB directly
│   │   └── ChunkWriter.cpp    writes chunk files, computes chunk_hash
│   └── importer/
│       ├── main.cpp           CLI: chunk files → rippled NuDB
│       ├── ChunkReader.cpp    reads + verifies chunk files
│       ├── HashVerifier.cpp   replays deltas, checks root hash per ledger
│       └── NuDBWriter.cpp     writes nodes into NuDB
├── tests/
│   ├── determinism_test.cpp   export same range twice, assert byte-identical
│   ├── roundtrip_test.cpp     export range, import to fresh NuDB, verify hashes
│   └── delta_size_test.cpp    measure actual delta sizes across ledger ranges
├── CMakeLists.txt
└── README.md
```

---

## Implementation Phases

### Phase 0: PoC (poc/exporter.py)

Target: prove the design works in the simplest possible way.

- Connect to a live rippled node via WebSocket
- Fetch ledger state for ledgers N and N+1 using `ledger_data` pagination
- Compute delta at the object level (not SHAMap node level)
- Serialize deterministically (sort by object ID)
- Verify: apply delta to state N, check resulting state matches state N+1
- Measure: actual delta size for 1000 consecutive ledgers

**Success criteria:**
1. Delta serialization is deterministic (run twice, same bytes)
2. Delta correctly reconstructs next ledger state
3. Measured delta sizes match the ~35 KB/ledger estimate

Language: Python (fast iteration, no build system)
Input: any running rippled node (mainnet, testnet, devnet)

---

### Phase 1: Chunk Format Spec

- Write `spec/chunk-format.md` with full binary layout
- Define all type codes, field sizes, endianness (big-endian throughout)
- Define the chunk_hash computation (what's included, what's excluded)
- Review + lock the spec before writing C++

---

### Phase 2: C++ Exporter

- `NuDBReader`: open rippled's NuDB files directly, iterate nodes by ledger
- `SHAMapDiff`: call `visitDifferences()` between consecutive ledger SHAMaps,
  collect added/modified/deleted node lists, sort by hash
- `ChunkWriter`: assemble chunk binary, compute chunk_hash, write to disk
- CLI: `xrla-export --db /path/to/nudb --start 80000000 --end 80100000 --out ./chunks/`

Links against rippled's SHAMap library (from ~/git/rippled).

---

### Phase 3: C++ Importer

- `ChunkReader`: parse chunk binary, verify chunk_hash
- `HashVerifier`: for each ledger in chunk, apply delta to in-memory SHAMap,
  compute root hash, compare against on-chain ledger header hash
- `NuDBWriter`: write verified nodes into a fresh NuDB instance
- CLI: `xrla-import --chunks ./chunks/ --db /path/to/new-nudb`

---

### Phase 4: Determinism Tests

- Export same 1000-ledger range from two different full-history nodes
- Assert chunk files are byte-identical
- This is the critical proof that the format works for P2P distribution

---

### Phase 5: Distribution (out of scope for now)

Once exporter + importer work and determinism is proven:
- XRPLF hosts official chunks on S3 public bucket
- `manifest.json`: ledger range → chunk file hash + download URL
- Operators download with aria2c (parallel), verify by chunk_hash, import

No new infrastructure required beyond S3 + the manifest file.

---

## Open Questions

1. Does `visitDifferences()` expose both state map and transaction map diffs,
   or only state map? Need to check rippled source.
2. What is the on-disk NuDB schema for ledger state — keyed by ledger seq,
   or by node hash only? Need to verify before writing NuDBReader.
3. Checkpoint size for a recent mainnet ledger — need to measure.
   If >15 GB per chunk, chunk size may need to increase to amortize better.
4. Compression: apply LZ4 or zstd per-chunk after serialization?
   Early ledgers compress dramatically. Measure first, decide after PoC.

---

## Starting Point

Run the PoC against a local rippled node (testnet or mainnet):

```bash
cd poc/
python3 exporter.py --url ws://localhost:6006 --start 1000000 --end 1001000
```

Output:
- `chunk_1000000_1001000.bin`
- delta size stats per ledger
- determinism verification (run twice, diff the outputs)
