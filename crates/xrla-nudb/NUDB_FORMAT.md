# NuDB dat file format (rippled 3.2.0)

Empirically determined by scanning a live mainnet NuDB dat file (5.8 GB, 447 ledgers, ledgers 105271056–105271502).

## Dat file layout

From NuDB source (https://github.com/cppalliance/NuDB, dat_file.hpp):

```
offset 0:   [8 bytes]  magic = b"nudb.dat"
offset 8:   [2 bytes]  version  (u16 BE)
offset 10:  [8 bytes]  uid      (u64 BE)
offset 18:  [8 bytes]  appnum   (u64 BE)
offset 26:  [2 bytes]  key_size (u16 BE) = 32
offset 28:  [64 bytes] reserved (all zeros)
--- header ends at offset 92 ---
--- records begin at offset 92 ---
```

Records start immediately at byte 92. No padding scan needed.

**FIELD_SIZE=6 is a NuDB library constant** (`uint48_t`) — it is NOT stored in the header and is NOT a per-database setting. All NuDB databases always use 6-byte (48-bit) fields for val_size, dat_offset, and record_size. The header byte at offset 48 has no meaning; it is part of the 64-byte reserved block and is always zero.

## Record format

Each record is tightly packed, no inter-record padding:

```
[6 bytes]  val_size  (u48 big-endian) — size of VALUE only, NOT key+value
[32 bytes] key       — raw 32-byte SHA-512/half hash
[val_size] value     — compressed node data (see codec below)
```

`FIELD_SIZE = 6` is a NuDB library constant (`uint48_t`) — not stored anywhere in the file.
`KEY_SIZE = 32` is read from the header's key_size field at offset 26.

## Codec types (value[0])

### 0x00 — Uncompressed

```
[1 byte]   0x00
[8 bytes]  zeros
[1 byte]   NodeObjectType (see below)
[N bytes]  payload
```

Rare in modern rippled. payload = SHAMap node wire bytes.

### 0x01 — LZ4 compressed

```
[1 byte]   0x01
[varint]   original_size  (LEB128 unsigned varint)
[N bytes]  LZ4 block data (raw LZ4, NOT LZ4 frame format)
```

Decompresses to: `[8 zeros][NodeObjectType (1 byte)][payload]`
Used for ALL leaf nodes (AccountNode, TransactionNode, Ledger).

LZ4 decompression: `LZ4_decompress_safe(data, buf, data_size, original_size)` — no frame headers.

### 0x02 — Sparse inner node

```
[1 byte]    0x02
[2 bytes]   mask     (u16 big-endian)
[N×32 bytes] hashes  — one 32-byte hash per set bit, packed in ascending slot order
```

N = popcount(mask). **Branch bit order is big-endian: branch slot `s` (0..15) is present iff
`mask & (0x8000 >> s)`.** Slot 0 is the most-significant bit, slot 15 the least. To expand to a
full 512-byte inner: walk `bit = 0x8000` down to `0x0001`; for each set bit consume the next
32-byte hash into that slot, otherwise leave it zero.

This matches rippled `nodestore/detail/codec.h` `nodeobject_decompress` case 2
(`std::uint16_t bit = 0x8000; for (int i = 16; i--; bit >>= 1) if (mask & bit) ...`).

> ⚠️ It is NOT `mask & (1 << s)` (low bit = slot 0). That reversed mapping decodes ~93% of sparse
> inner nodes wrong while still being deterministic — it passed determinism checks and only failed
> when the reconstructed root was compared against the on-chain `AccountSetHash`.

### 0x03 — Full inner node

```
[1 byte]    0x03
[512 bytes] 16 × 32-byte child hashes, slots 0..15
```

Slot with all-zero hash = empty child slot.

## NodeObjectType (byte at decoded[8])

From `rippled include/xrpl/nodestore/NodeObject.h`:

| Value | Name            | XRLA wire type       |
|-------|-----------------|----------------------|
| 0     | Unknown         | Inner (inner nodes)  |
| 1     | Ledger          | (skip in account SHAMap) |
| 3     | AccountNode     | AccountState (1)     |
| 4     | TransactionNode | TransactionWithMeta (4) |

## XRLA wire byte mapping

The SHAMapNode wire format (used by XRLA) is: `[content bytes][trailing type byte]`

For **inner nodes** (codec 0x02 or 0x03):
- content = 512 bytes (16 × 32-byte child hashes, all slots present, empty = zero hash)
- trailing type byte = 0x02 (NodeType::Inner)

For **leaf nodes** (codec 0x01 or 0x00):
- Decompress/strip 9-byte EncodedBlob prefix (`[8 zeros][NodeObjectType]`)
- content = payload bytes (decoded[9..])
- trailing type byte = map NodeObjectType → XRLA wire type (see table above)

## EncodedBlob decompressed structure (525 bytes for inner nodes)

When nodeobjectDecompress reconstructs an inner node it produces:

```
[4 bytes] u32 = 0
[4 bytes] u32 = 0
[1 byte]  NodeObjectType = 0 (Unknown)
[4 bytes] HashPrefix::InnerNode = [0x4D, 0x49, 0x4E, 0x00]
[512 bytes] 16 × 32-byte child hashes
```

= 13-byte prefix + 512 bytes = 525 bytes total

For XRLA inner node content: take bytes [13..525] = 512 bytes. This is exactly what codec 0x03 value[1..513] contains.

## NuDB key file — the correct way to read all nodes

**Use key-file lookups, not the dat scan.** A live mainnet `.dat` file is multi-GB with valid
records scattered across its entire length (offsets reach 4+ GB), so a sequential front-to-back
scan stops at the first zero gap and recovers only a tiny fraction of the tree. The `.key` file
is a hash table giving O(1) random access to any node by hash. Implemented in `keyfile.rs`.

### Key file header (first block = block_size bytes)

```
offset 0:   [8]  magic = "nudb.key"
offset 8:   [2]  version
offset 10:  [8]  uid
offset 18:  [8]  appnum
offset 26:  [2]  key_size = 32
offset 28:  [8]  salt        (u64 BE)
offset 36:  [8]  pepper      (u64 BE)
offset 44:  [2]  block_size  (u16 BE) = 4096
offset 46:  [2]  load_factor (u16 BE)
... zero padding to block_size ...
```

`num_buckets = (key_file_size - block_size) / block_size`. Bucket N is at file offset
`block_size * (N + 1)` (the header occupies the first block).

### Bucket layout (from NuDB detail/bucket.hpp)

```
[2]  count  (u16 BE)  — number of entries
[6]  spill  (u48 BE)  — .dat offset of next spill bucket, or 0
count × entry, each 18 bytes, sorted ascending by hash:
  [6] offset (u48 BE) — .dat offset of the record (start of its val_size field)
  [6] size   (u48 BE) — value size of the record
  [6] hash   (u48 BE) — hash prefix (== nhash below)
```

Bucket capacity = `(block_size - 8) / 18` = 227 entries for block_size 4096.

### Hashing (empirically verified against rippled 3.2.0)

```
nhash  = xxh64(key, seed=salt) >> 16          (NuDB's effective 48-bit hash)
modulus = smallest power of two >= num_buckets (linear hashing)
bucket = nhash % modulus
if bucket >= num_buckets { bucket -= modulus / 2 }
```

Both the bucket index **and** the stored 6-byte entry hash use `nhash`. `pepper` is part of
the header but is not needed for read-side bucket placement. After matching a candidate by
the 48-bit `nhash` prefix, verify the full 32-byte key from the `.dat` record (prefix
collisions occur).

### Spill buckets

When a bucket overflows it spills to a record in the `.dat` file:
`[6 zero][2 size BE][bucket body of `size` bytes]`. The `spill` field stored in the parent
bucket points **directly at the bucket body** (it already skips the 8-byte `[zero][size]`
header — that header exists only so a dat-recovery scan can identify and skip spill records).
Read the body in place as a normal bucket (count + spill + entries) and follow the chain.

Note: spilled entries are typically *superseded* nodes (older versions GC'd by online_delete),
so a walk of the current state tree usually finds every node in primary buckets — but the
spill chain must still be followed for correctness.

### online_delete = two live shards

rippled's `online_delete` keeps **two** NuDB databases live at once during rotation
(e.g. `rippledb.f380/` and `rippledb.fccd/`). The complete state spans both, so a reader must
try each shard in turn. `NuDBReader::open` takes a list of `.dat` paths for this reason.

## Confirmed record positions (from 200KB scan at FIELD_SIZE=6)

```
offset 92:   val_size=67  codec=0x02  (sparse inner,  3 children)
offset 197:  val_size=78  codec=0x01  (LZ4 leaf)
...
offset 3934: val_size=513 codec=0x03  (full inner, 16 children)
offset 4485: val_size=513 codec=0x03
...
```

Consistent chain of 379 records spanning offsets 92–~200KB, confirming FIELD_SIZE=6 is correct.
