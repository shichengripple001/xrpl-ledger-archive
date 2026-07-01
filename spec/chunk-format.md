# XRPL Ledger Archive — Chunk Format Specification

Version: 2  
Status: DRAFT

> Version 2 added `account_hash`, `drops`, and the close-time fields to each TX Map Entry
> (below) so a ledger's full `LedgerHash` can be independently reconstructed from chunk
> contents alone. Version 1 was never distributed outside this repo, so there is no
> compatibility shim — bump this number again, and add one, the next time a breaking field
> change happens after this format has real consumers.

---

## Overview

An `.xrla` file (XRP Ledger Archive chunk) contains a contiguous range of ledger history
encoded as:
- One **checkpoint**: full SHAMap state at the first ledger in the range
- N **deltas**: one per subsequent ledger, containing only the nodes that changed
- N+1 **transaction maps**: transactions and metadata for every ledger in the range

All multi-byte integers are big-endian.
All node lists within a section are sorted ascending by node hash.

---

## File Layout

```
[HEADER]       fixed size, 85 bytes
[CHECKPOINT]   variable
[DELTAS]       variable, one entry per ledger from start+1 to end
[TX_MAPS]      variable, one entry per ledger from start to end
[FOOTER]       fixed size, 4 bytes
```

---

## Header

| Field            | Type      | Size | Description                                      |
|------------------|-----------|------|--------------------------------------------------|
| magic            | bytes     | 4    | 0x58524C41 ("XRLA")                              |
| version          | uint8     | 1    | Format version = 1                               |
| network_id       | uint32    | 4    | 1 = mainnet, 2 = testnet, 3 = devnet             |
| start_ledger     | uint32    | 4    | First ledger sequence in this chunk              |
| end_ledger       | uint32    | 4    | Last ledger sequence in this chunk               |
| checkpoint_hash  | bytes     | 32   | Ledger hash at start_ledger (from ledger header) |
| chunk_hash       | bytes     | 32   | SHA-512/half of all bytes after this field       |

Total: 81 bytes

The `chunk_hash` covers: CHECKPOINT + DELTAS + TX_MAPS + FOOTER.
It does NOT cover the header itself (the header contains the hash).

---

## Checkpoint

Full SHAMap state at `start_ledger`. Contains every node in the state trie.

| Field       | Type   | Size     | Description                    |
|-------------|--------|----------|--------------------------------|
| node_count  | uint32 | 4        | Number of nodes                |
| nodes[]     | —      | variable | Node records, sorted by hash   |

### Node Record

| Field   | Type   | Size     | Description                              |
|---------|--------|----------|------------------------------------------|
| hash    | bytes  | 32       | SHA-512/half of node content             |
| type    | uint8  | 1        | 0 = inner node, 1 = leaf node            |
| length  | uint16 | 2        | Byte length of content field             |
| content | bytes  | `length` | Raw serialized node content              |

Nodes are sorted ascending by `hash` (lexicographic byte order).

---

## Deltas

One delta entry per ledger from `start_ledger + 1` through `end_ledger`.
Entries appear in ascending ledger sequence order.

### Delta Entry

| Field         | Type   | Size     | Description                                   |
|---------------|--------|----------|-----------------------------------------------|
| ledger_seq    | uint32 | 4        | Ledger sequence this delta applies to         |
| added_count   | uint32 | 4        | Number of added or modified nodes             |
| added[]       | —      | variable | Node records (same format as checkpoint)      |
| deleted_count | uint32 | 4        | Number of deleted nodes                       |
| deleted[]     | —      | variable | Deleted node hashes                           |

### Deleted Node Record

| Field | Type  | Size | Description          |
|-------|-------|------|----------------------|
| hash  | bytes | 32   | Hash of deleted node |

Added nodes are sorted ascending by hash.
Deleted nodes are sorted ascending by hash.

---

## Transaction Maps

> **Status: populated and verified.** The exporter fills TX_MAPS from the transaction SHAMap
> (`TransSetHash` tree) read directly from NuDB. Verified end-to-end: every `tx_hash` equals
> `SHA512half(HashPrefix::transactionID + tx_blob)`, and each ledger's reconstructed tx-tree
> root equals the on-chain `TransSetHash`. Each leaf's source content is
> `['SND\0'][VL(tx_blob)][VL(meta_blob)][32-byte tx_hash]` (rippled `HashPrefix::txNode`).

One entry per ledger from `start_ledger` through `end_ledger`.
Entries appear in ascending ledger sequence order.

### TX Map Entry

| Field                 | Type   | Size     | Description                                          |
|-----------------------|--------|----------|-------------------------------------------------------|
| ledger_seq            | uint32 | 4        | Ledger sequence                                       |
| ledger_hash           | bytes  | 32       | This ledger's full LedgerHash, recomputed + verified  |
| account_hash          | bytes  | 32       | AccountSetHash — this ledger's state SHAMap root      |
| drops                 | uint64 | 8        | Total XRP drops in circulation (TotalCoins)           |
| parent_close_time     | uint32 | 4        | Previous ledger's close time                          |
| close_time            | uint32 | 4        | This ledger's close time                              |
| close_time_resolution | uint8  | 1        | Close-time rounding resolution                        |
| close_flags           | uint8  | 1        | Close flags bitmask                                   |
| tx_count              | uint16 | 2        | Number of transactions                                |
| txs[]                 | —      | variable | Transaction records                                   |

`ledger_hash` = `SHA512half(HashPrefix::LedgerMaster + seq + drops + parent_hash + tx_hash
+ account_hash + parent_close_time + close_time + close_time_resolution + close_flags)`.
The exporter recomputes this from the source ledger DB and aborts if it doesn't match the
DB's stored value.

`tx_hash` and `parent_hash` are deliberately NOT stored as separate fields — both are
already reconstructable from the chunk itself, so storing them would be redundant:
- `tx_hash` = root of the transaction SHAMap rebuilt from this entry's `txs[]` (see
  "Transaction tree reconstruction" below).
- `parent_hash` = the previous TX Map Entry's `ledger_hash` (or, for the first ledger in a
  chunk, an externally obtained anchor — see "Verification without full history" below).

Together with the fields above, an importer can independently recompute and assert every
ledger's full `LedgerHash` — and the chain-of-custody link to the next ledger — using
nothing but this chunk. `xrla-import` does this (`replay_chunk`), starting at the second
ledger in the chunk (the first ledger's `parent_hash` is external by construction).

### Transaction tree reconstruction

Rebuilding a ledger's transaction SHAMap from its `txs[]` records (implemented in
`xrla_common::tx_tree::build_tx_tree`; the formula was verified against 51 real mainnet
ledgers / 4,500 transactions):

- **Leaf hash** (the node's own hash — used as the NuDB storage key and as the child
  reference in its parent inner node):
  `SHA512half(HashPrefix::txNode "SND\0" + VL(tx_blob) + VL(meta_blob) + tx_hash)`
- **Tree placement** uses the transaction's own `tx_hash` as the SHAMap item key (which
  nibble path the leaf sits at) — a *different* value from the leaf's own content hash
  above. Conflating the two produces a self-consistent but wrong tree — the same class of
  mistake as the sparse-inner-node bit-order bug.
- **Inner hash**: `SHA512half(HashPrefix::innerNode "MIN\0" + 16×32-byte children)`, same
  formula as the state tree. An inner node is created at a nibble depth only where two or
  more items still share a prefix; a subtree with exactly one item stores that item
  directly as the leaf child, with no further chain beneath it.
- An empty ledger's transaction tree root is `ZERO_HASH` directly (not a hashed empty
  inner node), matching rippled.


### Transaction Record

| Field     | Type   | Size      | Description                      |
|-----------|--------|-----------|----------------------------------|
| tx_hash   | bytes  | 32        | Transaction hash                 |
| tx_len    | uint32 | 4         | Byte length of tx_blob           |
| tx_blob   | bytes  | `tx_len`  | Raw serialized transaction       |
| meta_len  | uint32 | 4         | Byte length of meta_blob         |
| meta_blob | bytes  | `meta_len`| Raw serialized transaction meta  |

Transactions within a ledger are sorted ascending by tx_hash.

---

## Footer

| Field     | Type  | Size | Description              |
|-----------|-------|------|--------------------------|
| end_magic | bytes | 4    | 0x454E4458 ("ENDX")      |

---

## Verification Algorithm

Implemented in `xrla-import` (`replay_chunk` in `crates/xrla-import/src/main.rs`) as of
format version 2. To verify a chunk file:

1. Read header, check magic = "XRLA", version = 2
2. Compute SHA-512/half of bytes from after chunk_hash field to end of file
3. Assert computed hash == header.chunk_hash
4. Load checkpoint nodes into an in-memory SHAMap; assert the first TX_MAPS entry's
   `account_hash` is present among them
5. For each subsequent ledger (start+1 to end):
   a. Apply added/deleted nodes to the state SHAMap; assert the resulting root equals that
      ledger's TX_MAPS `account_hash`
   b. Rebuild the transaction SHAMap from that ledger's TX_MAPS entry (see "Transaction tree
      reconstruction"); this gives `tx_hash`
   c. Recompute `LedgerHash` from `(seq, drops, parent_hash, tx_hash, account_hash, close-time
      fields)` — `parent_hash` is the previous TX_MAPS entry's `ledger_hash`, everything else
      is derivable from or stored in this entry — and assert it equals the stored `ledger_hash`
6. If all assertions pass, the chunk is internally consistent **from the second ledger
   onward** — the very first ledger's `ledger_hash` is trusted as given, since its
   `parent_hash` is external to the chunk by construction (see below).
7. To confirm the chunk also matches the real network (not just itself), see "Verification
   without full history" below — in particular, verifying the *first* ledger's `ledger_hash`
   against an external anchor is what makes step 6 meaningful rather than self-referential.

---

## Verification without full history

A chunk can prove internal consistency entirely offline (step 6 above). To prove the *chunk
itself* is authentic — not just self-consistent — a verifier needs exactly **one** independently
obtained `LedgerHash` at or after the ledger they care about, then walks it backward:

- Every ledger's `LedgerHash` embeds `parent_hash`, the previous ledger's `LedgerHash`. So a chunk
  spanning many ledgers is itself a hash chain, and chaining consecutive chunks together
  (`checkpoint_hash` of chunk N+1 should equal the last `ledger_hash` of chunk N) extends that
  chain across the whole archive.
- Cryptographic hashes have the avalanche property: altering any transaction, account, or ledger
  anywhere breaks every hash downstream of that point. There is no way to tamper with the middle
  of an archive and still land on a correct anchor hash at the end.
- A single trusted anchor is enough to validate an arbitrarily large archive. Cheapest sources,
  in order:
  1. **The genesis ledger hash** — fixed forever, publicly known, free.
  2. **A skip-list-derived flag-ledger hash** — every current ledger's state tree contains a
     `LedgerHashes` object (`ltLEDGER_HASHES`) with the last 256 ledger hashes, and flag ledgers
     (multiples of 256) get their hash permanently chained forward. This lets *any* currently
     synced node — even one with zero retained history — answer "what was ledger N's hash" for
     any N, without ever having stored ledger N itself.
  3. **A live RPC query** to any node (full-history or not) for a ledger still inside its
     retention window — trivial if the chunk's range is recent.
  4. **Any independently published hash** — another provider's manifest, a historical record —
     the more independent sources agree, the stronger the trust.

This is the same trust model as a blockchain: verifying the tip (or any single validated point)
transitively verifies everything chained behind it. A buyer of a full-history "tape" does not need
a second full-history copy to catch tampering — they need the chunk's own hash chain plus one
independently obtained anchor.

## Partial Fetch & Stream Separation

A user typically wants one of two things, with very different costs:

| Need | Sections required | Approx size |
|------|-------------------|-------------|
| Inspect/query transactions in a range | TX_MAPS only | ~KB/ledger |
| Reconstruct full state / serve a node | CHECKPOINT + DELTAS | checkpoint is multi-GB |

A single bundled `.xrla` forces a transaction-querier to download the heavy checkpoint they don't
need. To avoid that, the three sections SHOULD be independently fetchable. Two mechanisms (decide
in Phase 1):

1. **Sidecar files** per range, each independently content-addressed and verifiable:
   ```
   xrla_<net>_<start>_<end>.ckpt    CHECKPOINT   (heavy; only for state reconstruction)
   xrla_<net>_<start>_<end>.delta   DELTAS       (per-ledger state changes)
   xrla_<net>_<start>_<end>.tx      TX_MAPS      (cheap; for transaction queries)
   ```
2. **Section byte-ranges in the manifest** — keep one `.xrla` but publish per-section offsets so
   clients can issue HTTP range requests for just the part they need.

Either way, checkpoints SHOULD be sparse (e.g. one per ~1M ledgers), with delta/tx streams
referencing the nearest preceding checkpoint, so checkpoint bytes are not repeated per chunk.

## Local Query Index (informative)

The query tool builds a local index over downloaded streams (not part of the wire format):
`tx_hash → (file, offset)`, `account_id → ledgers touched`, `ledger_seq → file`. Transaction
lookups resolve from `.tx` alone; full-state and balance-at-ledger queries additionally need
`.ckpt` + `.delta`.

## Chunk Naming Convention

```
xrla_<network_id>_<start_ledger>_<end_ledger>.xrla

Examples:
  xrla_1_80000000_80100000.xrla   (mainnet, 100k ledgers)
  xrla_1_00000001_00100000.xrla   (mainnet, genesis chunk)
```

---

## Manifest File

A `manifest.json` at the root of a distribution lists all available chunks:

```json
{
  "network_id": 1,
  "updated_at": "2026-01-25T00:00:00Z",
  "chunks": [
    {
      "start_ledger": 1,
      "end_ledger": 100000,
      "chunk_hash": "abc123...",
      "size_bytes": 18000000000,
      "url": "https://s3.amazonaws.com/xrpl-archive/xrla_1_00000001_00100000.xrla"
    }
  ]
}
```
