# Glossary

Plain-language definitions for the terms used across this project's docs, code, and
`xrla-inspect` output. Cross-references point at the file that defines the term
authoritatively — this file is a map, not the source of truth.

## Project-specific concepts

**Chunk (`.xrla` file)**
A single flat binary file covering a contiguous range of ledgers: one checkpoint, one
delta per ledger after the first, and one transaction map per ledger. See
[spec/chunk-format.md](spec/chunk-format.md) for the exact byte layout.

**Checkpoint**
The *full* ledger state (every SHAMap node) at the first ledger in a chunk's range.
Every other ledger in the chunk is expressed as a delta against this baseline instead of
storing its own full state — this is what keeps chunks small. A chunk's checkpoint currently
has no way to prove it's authentic on its own (see **checkpoint sparsity** below and
[spec/chunk-format.md](spec/chunk-format.md) "Verification without full history").

**Delta**
The set of state-tree nodes that changed between one ledger and the next: nodes *added*
(new or modified objects) and nodes *deleted* (objects removed or replaced). Storing only
the delta, not the whole state, per ledger is what makes a chunk far smaller than
`(number of ledgers) × (full state size)`.

**Checkpoint sparsity**
The open design gap where every `.xrla` chunk bundles its own full checkpoint, so
downloading many small chunks still means downloading many full-state copies. See
[DESIGN_NOTES.md](DESIGN_NOTES.md) and the README "Open" section. Not yet solved —
intentionally out of scope for the current import/verification work.

**Transaction map (TX Map)**
Per-ledger record holding every transaction + its metadata for that ledger, plus the
header fields (`account_hash`, `drops`, close-time fields) needed to independently rebuild
that ledger's full `LedgerHash`. See "TX Map Entry" in
[spec/chunk-format.md](spec/chunk-format.md).

**Chunk hash**
`SHA512half` of the entire chunk body (checkpoint + deltas + tx_maps + footer). Any single
byte changed anywhere in the file changes this hash, so it's the cheapest possible tamper
check — verified first, before any expensive replay work.

**Checkpoint hash**
The `LedgerHash` of the checkpoint ledger (the first ledger in the chunk's range), stored
in the chunk header for quick reference. Same value as that ledger's `ledger_hash` field in
its TX Map entry.

**Replay**
The import-side process of starting from the checkpoint state and applying each delta in
order to reconstruct every intermediate ledger's state, re-deriving each one's state root
and comparing it to the ledger's stored `account_hash`. Implemented in
`replay_chunk` (`crates/xrla-import/src/main.rs`).

## XRPL / rippled concepts

**Ledger**
One "block" in XRPL's terminology — a fully-closed, validated snapshot of the network at a
point in time, identified by a sequence number (`LedgerSeq`) that increments by 1 each time.

**LedgerHash**
The canonical, top-level hash identifying a ledger. Computed as:
```
SHA512half(HashPrefix::LedgerMaster + seq + drops + parent_hash + tx_hash +
           account_hash + parent_close_time + close_time +
           close_time_resolution + close_flags)
```
A pure, deterministic function of that ledger's own header fields — no network or consensus
participation is needed to *recompute* it once you have the inputs (only to *originally
decide* what those inputs should be). This is why offline verification is possible at all.

**AccountSetHash (`account_hash`)**
The root hash of the ledger's *state* SHAMap — the Merkle tree of every account, trust
line, offer, and other ledger object that exists at that ledger. Two ledgers with identical
account_hash have byte-identical state.

**TransSetHash (`tx_hash`)**
The root hash of the ledger's *transaction* SHAMap — the Merkle tree of every transaction
(+ its metadata) that was applied in that ledger. Not stored directly in a chunk's TX Map
entry; rebuilt from the entry's transaction list via `build_tx_tree` and compared.

**parent_hash**
A ledger's `LedgerHash` field is not self-contained — it embeds the *previous* ledger's
`LedgerHash`, chaining every ledger to its predecessor all the way back to the genesis
ledger. This is what makes tampering with one ledger break every hash after it (the
"avalanche" property discussed in "Verification without full history").

**drops**
XRP's smallest unit; 1,000,000 drops = 1 XRP. A ledger's `drops` field (`TotalCoins` in
rippled) is the total XRP in existence at that ledger — it only ever decreases, by the
amount of transaction fees burned (destroyed, not paid to anyone) in that ledger.

**close_time / parent_close_time / close_time_resolution / close_flags**
Metadata about when a ledger closed (was finalized by consensus) and at what time
granularity, all folded into the `LedgerHash` computation.

**SHAMap**
XRPL's Merkle-Patricia-trie data structure — a 16-ary (one branch per hex nibble) hash
tree. Both the state tree (`AccountSetHash`) and the transaction tree (`TransSetHash`) are
SHAMaps; they use the same inner-node hash formula but store different kinds of leaves.

**SHAMap node — inner vs. leaf**
An *inner* node has up to 16 children (one per nibble value 0-F) and its hash is
`SHA512half(HashPrefix::innerNode "MIN\0" + 16×32-byte children)`. A *leaf* node holds
actual content (an account, a transaction+meta, etc.) and its hash formula depends on what
it's a leaf of (see `HashPrefix` below).

**Nibble / tree placement**
Each SHAMap item (account, transaction) is placed in the tree by walking its own hash one
4-bit nibble at a time, picking which of the 16 children to descend into at each level. A
subtree collapses to a direct leaf reference once only one item remains in it.

**HashPrefix**
A 4-byte tag prepended before hashing, so the same raw bytes hashed for different purposes
(e.g. a transaction ID vs. a tree leaf) never collide. Ones used in this project:
`"LWR\0"` (LedgerMaster/LedgerHash), `"MIN\0"` (inner node), `"SND\0"` (transaction tree
leaf), `"TXN\0"` (transaction ID).

**tx_hash (per-transaction)**
Not to be confused with the ledger-wide `TransSetHash` — this is one specific transaction's
own identifying hash: `SHA512half(HashPrefix::transactionID "TXN\0" + tx_blob)`. Used both
as the transaction's public ID and as the key that determines its placement in the
transaction SHAMap.

**tx_blob / meta_blob**
The raw, rippled binary-serialized bytes of a transaction and of its execution metadata
(what actually happened when it ran — balance changes, created/deleted objects, etc.),
respectively. Opaque hex outside of a full rippled binary-format decoder; `xrla-inspect
--tx` prints them as-is.

**Genesis ledger**
Ledger sequence 1 (or the network's very first ledger) — a free, permanent, publicly known
hash anchor that requires no query to obtain, useful as the ultimate root of trust for the
hash-chain verification described in "Verification without full history."

**Flag ledger**
Every 256th ledger, which additionally carries a `LedgerHashes` object listing the last 256
ledgers' hashes — lets any currently-synced node answer "what was ledger N's hash" without
having retained ledger N's own data.

**Validated ledger**
A ledger that has reached consensus and been confirmed final by a quorum of the network's
trusted validators (its UNL). rippled's `ledger` RPC reports `"validated": true` for such
ledgers — this is the network's own attestation that a hash is real, not merely one node's
local record.

## NuDB / storage concepts

**NuDB**
The on-disk key-value store rippled uses for ledger object storage (a `.dat` file with the
actual records + a `.key` file with a hash-bucket index for O(1) lookup). See
[crates/xrla-nudb/NUDB_FORMAT.md](crates/xrla-nudb/NUDB_FORMAT.md) for the full format
(headers, bucket layout, spill chains, hashing scheme) as reverse-engineered and verified
against real rippled databases.

**online_delete / shard rotation**
rippled's mechanism for bounding disk usage by periodically deleting old ledger data. It
rotates between two live NuDB databases at once, so a consistent read of "current state"
may need to check both — `xrla-export --dat` accepts multiple `.dat` paths for this reason.

**Spill chain**
NuDB's overflow mechanism: when a hash bucket has more entries than fit in one block, extra
entries are chained into overflow records stored in the `.dat` file itself, linked from the
bucket via a spill pointer.

**History Sharding (rippled, removed)**
A now-removed rippled feature (dropped in v2.3.0) that split full history into fixed
ledger-range "shards," each backed by its own NuDB database. Discussed at length in
[DESIGN_NOTES.md](DESIGN_NOTES.md) as prior art this project's content-addressed,
delta-encoded design was built to avoid repeating (the design flaw being range/time-based
bucketing misaligned with power-law object access, causing redundant re-serialization of
unchanged objects across shard boundaries).

## Tooling

**`xrla-export`**
Reads a rippled NuDB store + `ledger.db` directly (no running rippled process, no RPC) and
writes a `.xrla` chunk for a given ledger range.

**`xrla-import`**
Reads a `.xrla` chunk, verifies its `chunk_hash`, replays checkpoint+deltas while
independently recomputing every transaction hash / account-state root / chained
`LedgerHash`, then writes a real NuDB `.dat`/`.key` pair from the result.

**`xrla-inspect`**
Read-only viewer for a `.xrla` chunk's contents — whole-chunk summary table, one ledger's
detail, or one transaction's raw blob/meta hex — without importing or writing anything.
