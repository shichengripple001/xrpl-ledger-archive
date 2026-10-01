# XRLA — Status: what is proven, what is not

Last updated 2026-09-29.

Companion to `PLAN.md` (which holds the full design rationale and the ordered TODO list). This
file answers one question: **which claims about this project are backed by observed behavior on
real data, and which are still reasoning?**

The distinction matters because the failure mode of this project is silent. A binary-format
decoder that is subtly wrong still produces confident, deterministic, plausible-looking output —
see the 2026-07-08 sparse-inner-node bit-order bug, which passed every determinism check and was
caught only by comparing a reconstructed root against the on-chain `AccountSetHash`. **Determinism
is not correctness.** Only comparison against an independently-produced ground truth counts as
proof here.

---

## Proven on real mainnet data

### Export → import round-trip is cryptographically correct

Validated 2026-07-08 against a real mainnet NuDB snapshot, ledgers 105277428–105277528 (100
ledgers), exported with `--chunk-size 30` into 4 chunks (30/30/30/11).

`xrla-import` independently recomputed and matched, for every ledger:

- each transaction's own `TransactionID` — `SHA512half(HashPrefix::transactionID ‖ tx_blob)`
- the replayed account-state root against the ledger's stored `account_hash`
- the full `LedgerHash`, chained via `parent_hash` to the previous ledger
- every state node's own hash against its claimed content (27M+ nodes per chunk)

Zero failures. Verified against both the first chunk (checkpoint from a real NuDB trie walk) and
the last, partial chunk (checkpoint from the in-memory snapshot — this also exercises the
shorter-final-chunk edge case).

This is the load-bearing result. The NuDB format reverse-engineering, the SHAMap wire format, the
delta encoding, and the chunk serialization are all downstream of it: if any were wrong, the
recomputed hashes would not match the chain.

### Maintain-state multi-chunk architecture

Same run. Exactly one `"Initial checkpoint"` line for the whole export (27,031,655 nodes, 77.5s) —
confirming one full trie walk per invocation regardless of chunk count, not one per chunk. All
later chunks' checkpoints came from the in-memory state map with no further NuDB access.

### Concurrent delta batching produces identical output

A real 500-ledger export (105277428–105277928) run twice — once batched, once with concurrency
forced to 1 — produced **byte-identical `chunk_hash`** (`de734d235a4acf…`) and identical totals
(1,043,162 added nodes, 46,064 real transactions).

### Space savings

Not an estimate — measured from real data:

- Real per-ledger delta rate: **~1,966 changed state nodes/ledger**
- Against a checkpoint carrying **27M+ live nodes**

That ratio *is* the space argument. A delta stores only what changed; a snapshot stores everything,
every time. This follows from the format definition and is confirmed by the measured rate — it does
not need a further run to demonstrate.

### Full-history scale, from real servers

Inspected two real xrpld full-history nodes (`devnet-fh-usw2-01`, `livenet-fh-usw2-01`)
on-disk, 2026-07-08:

- Mainnet `nudb.dat` ≈ 29.5 TB, `nudb.key` ≈ 4.0 TB (~33.5 TB combined)
- Derived total record count from the real key-file size: **~55–111 billion** unique
  (state + tx-tree) nodes across all history
- **No `shard_db`, no `online_delete`** on either box — a single permanently-growing `node_db`.
  This invalidates any plan assuming "just open the shard for the ledger range you want."

Re-confirmed 2026-09-29 against a third full-history box (`livenet-fh-use1-01`), which also
breaks out the non-NodeStore files:

| File | Size | Relevance to this project |
|------|------|---------------------------|
| `nudb/` | **32 TB** | The content our chunk format has to represent. Matches the 33.5 TB figure above. |
| `transaction.db` | **11 TB** | A derived tx-hash → ledger index xrpld builds for `account_tx`. **We deliberately don't build it** (`xrla-inspect --account` scans chunk data instead), so it's out of scope — 11 TB we don't have to archive. |
| `ledger.db` | **296 GB** | Ledger header metadata. Our from-scratch rebuild of the same rows would be ~44 GB (extrapolated from 8.5 MB for 20,775 real rows) — the 6.7x gap is 15 years of SQLite page/WAL bloat with no `VACUUM`, not inherent data. |

**Do not extrapolate archive size from a recent-ledger sample.** Our measured density on the
20k ledgers immediately before the current tip is ~1.4 MB/ledger; scaling that across 107.3M
ledgers predicts ~206 TB, roughly 6x the real ~32–35 TB. These three figures (33.5 TB on
2026-07-08, 32 TB and separately ~35 TB on 2026-09-29, from different hosts) are not in
tension — a full-history node grows continuously (~12 GB/day, established above), so
33.5 TB + ~83 days × 12 GB/day ≈ 34.5 TB lands right in that range. The lifetime average is
~310-330 KB/ledger (32-35 TB ÷ 107.3M) because early mainnet years were nearly empty — closer
to our PoC-network test than to today's traffic. Recent rate ≈ 4.3-4.5x the lifetime average.

---

## Proven: a real xrpld process opens, boots from, and correctly serves our output (2026-09-28)

This closes the single largest open claim in the project — see `CONTEXT.md`'s "The load-bearing
question" — that a real xrpld can bootstrap from our chunks, not just that our own reader can
read our own writer's output back.

**Setting**: a real internal PoC network (`network_id 3001`, 7 real EC2 nodes across two regions —
one designated full-history box, three validators, two hubs, one p2p node — running real
`xrpld 3.4.0-rc6`), not mainnet. Small network (~149,000 ledgers, only 2 real transactions in its
entire history), but every mechanism exercised is the same code path mainnet uses.

**Three real bugs found and fixed, only findable by testing against a real binary** (none of these
were visible from our own reader/writer round-trip tests):

1. **NuDB key-file `pepper` was wrong.** We computed `xxh64(&[], salt)`; NuDB's real
   `pepper<Hasher>(salt)` hashes the salt's little-endian bytes. A real xrpld rejected our store
   outright with `hash_mismatch` on open. Fixed in `writer.rs::write_key_header`. Invisible to our
   own tests because our own reader never checked pepper.
2. **`xrla-import` destroyed a target `ledger.db` instead of merging into it.** `write_ledger_db`
   deleted-and-recreated the file; pointed at an already-populated `ledger.db` (e.g. a running
   node's own data), it silently wiped the existing rows. Fixed to open-and-`INSERT OR IGNORE`
   instead — verified with a new regression test
   (`write_ledger_db_merges_into_an_existing_populated_file`).
3. **A naive `.clone()` doubled resident memory and caused a real OOM.** Building the
   checkpoint∪deltas union (needed so every ledger in a chunk's range stays servable, not just the
   last one — see Immediate TODOs item, now done) by cloning the entire live-state map duplicated a
   27M-node checkpoint's content, crashing a real machine during testing. Fixed by sharing node
   storage between the two maps via `Rc<SHAMapNode>` instead of cloning.

**The actual cold-start proof, staged to rule out "peer sync supplied it" as an alternative
explanation**:

- Exported a real node's full history (149,205 ledgers at the time, ~1.2 GB raw, 12s to export)
  into 15 range chunks.
- Deleted everything below ledger 140007 from **every one of the 7 real peers'** `ledger.db`
  (not just the target node) — confirmed each one independently now returns `lgrNotFound` below
  that point, so no peer in the network could have supplied that range even if asked.
- Wiped the full-history node's NodeStore + `ledger.db` entirely and reseeded it, offline, purely
  from `xrla-import`'s own output (no network involved during the write).
- Started it peered (`--net`, real consensus, real other nodes) and queried ledgers spanning its
  full history (4, 100, 70000, 140100, ...): every `account_hash` matched the untouched
  ground-truth snapshot exactly, served correctly within seconds of startup — far faster than the
  network's own measured backward-history-acquisition rate (~77 ledgers/min, and that rate itself
  stalls unpredictably) could possibly reconstruct 140,000+ ledgers.
- Independently cross-checked two real accounts (the network's genesis account and its faucet
  account) for both current state (`account_info`) and full transaction history
  (`xrla-inspect --account`, since xrpld's own `account_tx` needs a separate `transaction.db`
  index this project deliberately does not populate) — exact match against an untouched peer.

**Real operational findings, not just the proof itself**:

- **`earliest_seq` silently caps what a node will ever serve, independent of what data actually
  exists.** A stale `earliest_seq=140007` left over from an abandoned experiment made a node with
  fully correct, verified data below that point still return `lgrNotFound` for it. Cost real
  debugging time before the leftover config was found and reverted. Worth remembering: config, not
  just data, must be checked when a node under-serves what it should have.
- **Real xrpld `--import` (`Database::importInternal`, genuine incremental NuDB insert into an
  already-populated store) is dramatically slower than a fresh bulk rebuild for large merges.**
  Measured directly: merging ~281K new objects into a live store via `--import` progressed at
  roughly 1.4–2.9 KB/s and was still decelerating after 15+ minutes (would have taken hours to
  days); rebuilding the same total dataset from scratch via `xrla-import`'s bulk writer took
  ~93–98 seconds. The mechanism difference: bulk rebuild sizes and writes the bucket table once,
  almost entirely sequentially; real incremental insert does per-object random-access bucket
  lookups and triggers linear-hashing bucket splits as the table grows, which is exactly what NuDB
  incremental insert is for (a live, slowly-growing store) and exactly wrong for a one-shot bulk
  merge. Confirms `writer.rs`'s own design choice (bulk rebuild, not incremental insert) was
  correct — and means "merge into an already-populated store" is only ever practical via
  rebuild-and-replace (export the live tail, combine with the archive chunks, reseed from scratch),
  not via xrpld's own `--import` at any real scale.
- **A real, measured (if small-scale) backward-history-acquisition rate**: a wiped node's own
  `complete_ledgers` floor moved from a live-tip-only window back to ~77 ledgers/min at times, but
  the rate is not sustained — it plateaued for 15+ minutes with zero further movement at least
  twice during testing, on a healthy, well-peered node with no configuration difference from nodes
  that did progress. Cause unknown. This is a small private network, not mainnet, but it's the
  first real (not paper) evidence of this project's core "P2P backfill is slow and unreliable"
  premise (`PLAN.md`'s opening claim), rather than an assumption.
- **`ledger`-by-index RPC and `complete_ledgers` are not the same signal, and neither is fully
  reliable in isolation.** A direct `ledger {ledger_index: N}` call can succeed for data
  `complete_ledgers` doesn't yet (or ever) advertise, and — separately from the `earliest_seq`
  issue above — `complete_ledgers` connecting an imported range back to a node's live-tracked
  window is not immediate; it took anywhere from seconds to a few minutes across different runs.
  Always verify with the direct per-ledger RPC, not the summary field.

### `xrla-import` now accepts multiple chunks in one invocation

Added to combine a full-history export's many range files into a single NuDB store / `ledger.db`
write, rather than one fresh store per chunk. Every chunk's checkpoint + delta nodes are unioned
before one write, so nodes shared across chunk boundaries are deduped, not written twice. This is
what made the 149,205-ledger reseed (280K+ nodes) a single ~93-second operation instead of 15
separate ones.

---

## Real-mainnet export: measured cost, and the memory bug it exposed (2026-09-29)

Everything above was measured on a private PoC network with near-empty ledgers. This section is
the first measurement against **real mainnet traffic**, which turns out to dominate every cost in
the project.

### Cold-start peer sync from real mainnet

Repointed `xrpld-poc-fh-usw2-01` off the PoC network at real mainnet (removed `[network_id]` and
the private `[ips_fixed]`, real UNL via `vl.ripple.com` + `unl.xrplf.org`, `ledger_history=10000`),
wiped every db file, and started from nothing:

- **Empty db → `server_state: full`: 12m41s.** Most of that is spent in `connected`, repeatedly
  restarting `InboundLedger` acquisition of the live account-state tree — each attempt races a
  network that closes a new ledger every ~4s, so early attempts are abandoned mid-fetch. The
  node's own `closed_ledger.seq` counts from 1 during this phase and means nothing.
- **Backward history backfill: ~12 ledgers/min** (9,052 ledgers in ~12.5h), versus ~77/min
  measured on the PoC network. Real ledgers carry vastly more state to fetch per ledger.

### Export size and time, real mainnet density

All runs on 16 vCPU / 123 GB RAM, local NVMe, `xrpld` **stopped** (see race-condition warning
below):

| Range | Layout | Output | Wall clock | Peak RSS |
|-------|--------|--------|-----------|----------|
| 10,000 ledgers | one v2 chunk | 24.87 GB | 3m55s | 96 GB |
| 20,000 ledgers | two v2 10k chunks | 51.33 GB | 10m52s | 107 GB |
| 20,000 ledgers | one v2 chunk | — **OOM-killed** | — | 121.6 GB (killed) |
| 20,000 ledgers | one **v3** chunk | **41.35 GB** | 7m35s | **18.3 GB** |

Density at the current tip: **~141 txns/ledger**, ~2,620 changed state nodes/ledger, ~1.4 MB/ledger
incremental. Splitting a 20k range into two 10k chunks costs an extra ~10 GB of duplicated
checkpoint (measured directly: chunk1 + chunk2 − single = 9.98 GB — not the ~13 GB a naive
"total bytes / total nodes" average would suggest; delta content and checkpoint content don't
average the same bytes/node, so that heuristic overestimates by ~30%), which is why the two-chunk run is
*larger* than the single-chunk one covering identical ledgers.

### The OOM, and the format change that fixed it

`xrla-export` buffered the entire chunk — every `LedgerDelta` and every `TxMap` (with full
transaction blobs) for the whole range — in memory, then `serialize_chunk` copied all of it into
one `Vec<u8>`, and only then wrote to disk. With `--chunk-size 10000` a 20k range flushes at the
boundary and survives; asking for a genuine single 20k chunk never flushes and died at 121.6 GB.

The v2 layout can't be streamed as-is: you can't know all deltas are finished until the last
ledger's tx_map is also computed, but tx_maps can't be written until every delta is already on
disk. So **format v3** interleaves each ledger's delta with its tx_map (see
`spec/chunk-format.md`), and `ChunkWriter` streams straight to disk with an incremental SHA-512,
seeking back once at the end to fill in `chunk_hash`, writing to `.tmp` and renaming on success so
a crash can't leave a corrupt file at the final name. Only the live SHAMap `state` stays resident.

Result: **121.6 GB → 18.3 GB peak** for the same 20k-ledger single chunk. v2 files (including the
ones already produced above) stay readable — `deserialize_chunk` dispatches on the version byte,
and `xrla-import` / `xrla-inspect` needed no changes at all.

**Warning — do not export from a running node.** Two earlier attempts failed with
`Error: node not found: <hash>` partway through the checkpoint walk. The data was fine; the
cause was reading `nudb.dat`/`nudb.key` while `xrpld` was concurrently appending and splitting
hash buckets. Our `NuDBReader` is not xrpld's own reader and does not tolerate a live writer.
Stopping `xrpld` made the identical range export cleanly. A spurious "node not found" here means
a race, not missing history.

---

## Real-mainnet reseed: `xrla-import` had the same OOM bug, twice (2026-09-29/30)

Repeated the earlier PoC-network proof — erase a node's db, reseed purely from `xrla-import`
output, confirm it catches up and serves the reseeded range — this time on the mainnet-connected
node (`xrpld-poc-fh-usw2-01`), importing the real 20,000-ledger v3 chunk from the section above.

### Bug 1: the read side (`ChunkReader`, fixed 2026-09-29)

The first attempt **OOM-killed at 127.46 GB RSS** — the same class of bug as the export-side fix,
just on the read/replay side. `xrla-import` read the whole 41.35 GB chunk file into one `Vec<u8>`
(`fs::read`), then `deserialize_chunk` parsed it into a second, fully-owned `Chunk` struct
(another full copy of every node's content), all while `replay_chunk` built a *third* copy in
`state`/`all_state_nodes`. Fixed the same way as the exporter: a streaming `ChunkReader` (mirrors
`ChunkWriter`, reuses the same `read_node`/`read_delta`/`read_tx_map` parsing functions through a
hashing `Read` wrapper) that folds each node straight into `state`/`all_state_nodes` as it's
parsed, verifying each checkpoint node's own hash inline (`recompute_node_hash`) instead of
batching a second clone of the whole checkpoint just to call `verify_state_nodes` on it. v2 files
(not streamable — see `spec/chunk-format.md`) fall back to the original buffered path.

Result: **127.46 GB → 96.2 GB peak**, real chunk, identical `ledger.db` output
(19,999 rows, `107287901–107307899`). Added
`streaming_replay_matches_buffered_replay_on_the_same_chunk` (writes a chunk via `ChunkWriter`,
replays it via both paths, asserts identical results — including that a tampered `LedgerHash` is
still caught).

**This did not fully fix the OOM** — 96.2 GB is still high for a 20k-ledger chunk. The remaining
cost was `write_to_nudb` (see Bug 2).

### Cross-checked against real mainnet, not just self-consistency

Once reseeded and caught up (`server_state: full` in ~7 min this run, versus 12m41s cold-starting
from nothing), queried 1,650 ledgers total (400 + 250 + 1,000, sampled across the reseeded range)
against **real xrpld reference nodes** (`r.ripple.com`, `xrplcluster.com`) and Clio
(`s2.ripple.com`) — `ledger_hash`, `account_hash`, `parent_hash`, `transaction_hash`,
`total_coins`, every close-time field, and per-transaction `Fee`/`Account`/`Destination`/`Amount`/
`TransactionType`/`Sequence`/`Flags`/`SigningPubKey`/`TransactionResult`/`delivered_amount`/
affected-node count for ~220,000 transactions: **~2.4M field comparisons, zero canonical data
mismatches.** The only field that ever differed was `delivered_amount` — a serve-time-derived
field `xrplcluster.com` omits in this RPC shape that both we and `r.ripple.com`/Clio populate
identically; the underlying `transaction_hash` (tx-tree root) matched byte-for-byte across every
endpoint regardless.

Also surfaced two real quirks in the public reference endpoints worth remembering for any future
comparison: `tooBusy`/`slowDown` arrive as **HTTP 200 with a JSON-level error**, not 429/503 — a
naive HTTP-status-only backoff will silently misread these as data mismatches. And
`r.ripple.com` is a load-balanced pool of backends with differing rolling retention floors, so a
`lgrNotFound` from it can mean "this particular backend doesn't have it," not "it doesn't exist."

**Methodology, reusable:** `tools/compare_ledgers.py` (run on the xrpld host itself, so our side
has no rate limit). For a random sample of ledger sequences, calls the `ledger` RPC
(`transactions: true, expand: true`) against `127.0.0.1:51234` (ours) and against a rotating pair
of public endpoints, then diffs every header field and every transaction's canonical fields
listed above. Handles both quirks above directly: backoff keys off the JSON `error` field (not
HTTP status), and `lgrNotFound` triggers an endpoint rotation instead of being counted as a
mismatch. `delivered_amount` differences are bucketed separately as "derived-field diffs," not
real mismatches, since it's serve-time-derived rather than canonical ledger data. Prints a final
summary (ledgers matched/mismatched/skipped, field/tx counts, rate-limit stats) plus the first 20
real mismatches, if any. Usage: `compare_ledgers.py <start> <end> <sample_count> [delay_seconds]`.

### Second, larger cross-check: 10,000 ledgers after the 150k reseed (2026-10-01)

After the real 150,000-ledger export/import measurement (see Bug 3 below) and restarting
`xrpld`, ran 20 batches of 500 ledgers each (`tools/compare_ledgers.py`), spread across 20
roughly-equal sub-ranges spanning the full `107145192-107351007` history, against
`s2.ripple.com`/`r.ripple.com`:

**10,000/10,000 ledgers matched, 0 mismatches, 0 skipped.** 1,237,494 transactions compared,
**13,722,434 field comparisons, zero real mismatches**, 0 `delivered_amount` diffs, 0 batches
with any FAIL line. This independently confirms the reseeded node serves byte-for-byte correct
canonical data across its entire history, including the 150k-ledger range imported with the
fixed `NuDbSink`.

Also confirmed: after `xrpld` was stopped (for the export, to avoid racing NuDB's in-place
bucket-table mutation) and restarted, `server_info`'s `complete_ledgers` temporarily reported a
tiny recent-only range (e.g. `107350685-107350894`) even though the real data was intact the
whole time (`ledger.db`'s `Ledgers` table still had all 204,024 rows,
`107145192-107350899`) — it re-expanded back to the full range on its own within a few minutes.
Worth remembering so a post-restart "empty"/truncated-looking `complete_ledgers` isn't mistaken
for data loss.

### Bug 2: the write side (`write_nudb_store_streaming`, fixed 2026-09-30)

Chasing a chunk-size decision (see PLAN.md's "Chunk size decision" section) surfaced a second,
separate buffering bug. `write_to_nudb` built `all_state_nodes`/`tx_nodes` (already resident from
replay) into a *third* copy — a `HashMap<Hash256, Vec<u8>>` re-encoding every node's content into
NuDB wire format — before handing it to `write_nudb_store`, which then held a *fourth* copy
(`entries: Vec<(Hash256, Vec<u8>)>`, collected from that map) for the entire `.dat`-writing pass.

Fixed by adding `write_nudb_store_streaming`, which takes an iterator instead of a slice and
writes each entry straight to the `.dat` file as it's pulled — the caller now passes a lazy
iterator over `all_state_nodes.values().chain(tx_nodes.iter())` that encodes each node's wire
value one at a time, never collecting it. Dedup (a hash appearing in both `all_state_nodes` and
`tx_nodes` keeps whichever is seen first, matching the old `HashMap::entry().or_insert_with()`
behavior) now happens via a `HashSet<Hash256>` alongside the small `placed: Vec<(u64, Hash256,
u64, u64)>` metadata list the bucket-sizing pass needs — neither holds node content.
`write_nudb_store` (the original slice-based function) now just delegates to the streaming
version, so nothing calling it needed to change.

Result on the same real 20,000-ledger chunk: **96.2 GB → 64.3 GB peak**, identical output
(19,999 rows, same range, exit 0).

**Still not fully fixed at the time this was written.** `all_state_nodes` and `tx_nodes`
themselves were still fully resident for the entire run — replay happened completely, *then*
writing happened completely. Extrapolating 64.3 GB linearly to a 150,000-ledger chunk (~7.5x the
delta/tx content) gave **~482 GB** — down from a ~720 GB projection before this fix, but still
far past the 123 GB test box.

### Bug 3 (architectural): interleaving replay and writing (2026-09-30, measured 2026-10-01)

Closing the remaining gap needed the same shift `ChunkWriter`/`ChunkReader` made for
export/read — write each node the moment it's produced, never accumulate it. Implemented: a new
`NuDbSink` in `xrla-nudb` accepts nodes one at a time (`write_node(hash, value)`) and writes each
straight to the `.dat` file. `replay_chunk`/`replay_chunk_streaming` now call it directly for
every checkpoint node, every delta's added nodes, and every ledger's rebuilt tx-tree nodes —
`ReplayResult` no longer has `all_state_nodes`/`tx_nodes` fields at all. The only thing genuinely
resident for the whole run is `state` (the *live-only* map, bounded by current account-state
size — same property that keeps export cheap, since superseded nodes are removed from it as
replay progresses) plus the sink's own dedup `HashSet<Hash256>` and a small per-node placement
record.

**Two more real bugs found reviewing this change, both fixed, neither about memory:**

- **Destructive overwrite, again.** `NuDbSink::create` originally truncated the target `.dat`
  immediately — before a single chunk had even been read. A failed import (bad path, corrupt
  chunk, anything) destroyed an existing store. Confirmed with a live test: a 45-byte file became
  a 92-byte empty header after a deliberately-failed import. This is the exact same class of bug
  the `ledger.db` writer had (see the 2026-09-28 section above) — a second instance, not a repeat
  of the same one. Fixed: the sink writes to `<path>.tmp` throughout and only renames over the
  real path in `finish()`, after everything has succeeded. Dropping an unfinished sink deletes the
  temp file. Two renames (`.dat` then `.key`) can't be jointly atomic, so the old `.key` is moved
  aside to `.key.bak` first — a crash mid-swap leaves *no* `.key` (xrpld refuses to open that
  loudly) rather than a new `.dat` silently paired with a stale `.key`. Regression-tested
  (`aborted_sink_leaves_existing_store_untouched`, `finished_sink_replaces_existing_store_and_is_readable`).
- **Efficiency, found in the same review pass:** three unbuffered `write(2)` calls per node (now
  a 4 MiB `BufWriter`; on the 20k run, kernel time already exceeded user time — 502s vs 469s);
  a full 32-byte hash kept per placement record that the bucket-table format never reads (now
  dropped, 56 → 24 bytes/entry, ~13 GB saved at 150k scale); duplicate nodes were encoded and
  then discarded (now checked *before* encoding, which matters most on multi-chunk imports where
  every later chunk's checkpoint repeats ~28M already-written nodes).

**Measured 2026-10-01, real mainnet, full 150,000-ledger chunk — not an extrapolation.**
`xrpld` was stopped on `xrpld-poc-fh-usw2-01` to get a consistent read (NuDB's `.key` file isn't
append-only — linear-hashing splits rewrite bucket entries in place, including ones for
already-written keys, so concurrent writes from a live node can race a reader even on historical
ranges). Ran a real export of ledgers 107147192–107297191 (149,999 ledgers, one chunk,
`--chunk-size 150000`) against the live NuDB store, then imported that chunk into a fresh store
with this fix in place:

- **Export:** 47:17 wall clock, peak RSS 19.2 GB (the known, accepted `state` map cost — unrelated
  to this fix), output chunk 207.8 GB, `chunk_hash b18c25586bdea6b75f3794b15f1a416f6ccaff3defab2cf9740d14499d04f50c`.
- **Import:** 39:39 wall clock, **peak RSS 46.1 GB**, 345,242,107 unique nodes written, 149,999
  `ledger.db` rows, exit status 0. Every ledger's `account_hash`, txn authenticity, and
  `LedgerHash` (chained to parent) verified clean end to end.

46 GB confirms the expected effect: peak memory scales with total unique node count, not chunk
length. The spill-to-disk/external-sort fallback is **not needed** at 150k — 46 GB is well within
normal headroom, nowhere near the ~482 GB the pre-fix architecture would have extrapolated to.
Not built, no longer a near-term action item.

### Correction: the "~12 ledgers/min" backfill rate above was misleading

That figure (in "Cold-start peer sync from real mainnet") was total ledgers backfilled divided by
total wall-clock time, including long stalls — not a real rate. Investigated directly on
2026-09-30 by inspecting the `peers` RPC: of 16 connected peers, only **3 hold history deeper
than ~20,000 ledgers**; the other 13 hold exactly the newest ~20,000 and nothing older, and all 3
deep peers connected to us (inbound), so we don't control routing to them. Measured rate while
those 3 were actually being used: **~132 ledgers/min** over a 2-minute window — much faster than
"~12/min" suggested — but with only 3 shared peers to serve every node backfilling similarly, the
rate should be expected to degrade unpredictably past ~20k ledgers deep, not stay constant. This
is the real reason backfill is slow and unreliable, not a generically low rate: **it's a peer
topology problem** (real full-history sources are rare and shared), which is exactly the gap this
project's chunk-distribution approach is meant to route around.

---

## Built and working, but not yet proven at scale

### `meta_decode` / `account_tx` query

`xrla-common/src/meta_decode.rs` decodes xrpld's binary STObject format to extract every
`AccountID` at any nesting depth from a `meta_blob`; `xrla-inspect --account <r-address>` uses it
to list every transaction in a chunk touching that account.

Validated against real mainnet data, independently cross-checked, not just self-consistent:
- Ran clean across all 1,236 real transactions in a real 10-ledger export (105277428–105277438) —
  zero decode errors, after fixing one real gap it exposed (the `Issue`/type-24 STI, hit live and
  fixed in ~2 minutes; see "External changes assessed" below).
- For one real account (`rf7QoGcRk2aFMSQNY3zt6FsADavoVucLni`), the decoder found 3 matching
  transactions in that range; all 3 were independently confirmed against a live public API
  (xrpscan) — exact match on `Account`, `Destination`, and `ledger_index` for every one, zero false
  positives observed.

**Gap: no unit tests, and completeness (false negatives) unverified.** The above proves the
decoder isn't producing wrong or fabricated matches on real data — it does not prove it never
*misses* an account reference (e.g. one reachable only through a binary-format edge case this
10-ledger sample didn't happen to contain). This is exactly the shape of thing that fails
silently — and worse, `--account`'s error path warns-and-skips, so a systematically broken decoder
would present as "no matches found" rather than an error. Still needs: known-vector tests
cross-checked against an independent implementation, malformed-input cases, and a real `account_tx`
RPC comparison against a node whose retention covers the test range (attempted this session via
xrpscan's account-transactions endpoint; it ignored the ledger-range filter and only returned its
most recent page, so completeness is still unverified, not just untested).

### NuDB writer

`xrla-nudb/src/writer.rs::write_nudb_store` writes a fresh `.dat`/`.key` pair.

**No longer a gap** — see "Proven: a real xrpld process opens, boots from, and correctly serves our
output" above. A real xrpld 3.4.0-rc6 opened, booted from, and correctly served output from this
writer, including on a wiped-and-reseeded live node that then rejoined real peered consensus.

---

## Not started

### Other open items

See `PLAN.md` "Immediate TODOs" for full detail. Headlines:

- **Item 8 — real cold-read benchmark on dedicated full-history hardware.** Blocks any speed claim.
  The synthetic benchmark proved the lookup *algorithm* is O(1) to a 180 MB key file, cache-warm on
  local SSD; it says nothing about a 4 TB key file that cannot fit in RAM. **Must run on dedicated
  hardware** — see the 2026-07-08 I/O incident below.
- **Item 12 — export resumability.** Depends on the maintain-state work (done). Bounds crash loss
  to one chunk of delta replay instead of a repeated full trie walk.
- **Item 13 (remainder) — the persistent `account_tx` index sidecar.**
- **Item 14 — AccountRoot / lines / NFTs / order-book decoding.** Shares the STObject-parser
  foundation now begun in `meta_decode.rs`.

---

## Known-unmeasured, explicitly

- **Concurrent diff batching showed no local speedup.** Three timed runs on the same 500-ledger
  range (delta loop only): batched-cold ~56.6s, serial-warm ~26.3s, batched-warm ~48.2s. Batched
  was *slower* under the most cache-favorable conditions tested. Read as expected, not alarming:
  concurrency only pays when there is real disk latency to hide, and this dataset was
  page-cache-warm. **Do not claim a speed benefit until item 8 measures it on cold, dedicated
  storage.**
- **Full-history export timing.** Paper estimate 10–40 days with both architectural fixes; the
  pre-fix design was years. These are order-of-magnitude figures from hardware classes and
  small-scale benchmarks, not measurements. The 2026-09-29 run gives the first real anchor:
  **44 ledgers/sec** (19,999 ledgers in 455s) at current tip density, on 16 vCPU / local NVMe.
  Naively that's ~28 days for 107.3M ledgers — but that rate is measured on the *heaviest*
  ledgers in history, and the lifetime average is ~4.3x lighter, so a genuine genesis-to-tip
  run should land in the lower half of the 10–40 day estimate. Still not measured end-to-end:
  the rate almost certainly degrades as the checkpoint grows from empty (genesis) toward 28M+
  nodes, and this run never exercised that growth.

---

## Operational warning (2026-07-08 incident)

An early "adaptive" concurrency calibration climbed a hardcoded `[1, 4, 16, 64, 256]` ladder by
live-testing each level against the real store. Run against two ~6 GB files on a laptop's single
shared disk, 256 concurrent threads saturated I/O badly enough to require a hard restart.

Root cause: "adaptive" meant *tuned for maximum throughput*, not *safe to run here* — a dangerous
level got a real trial run before anything could rule it out, the stopping rule was relative
("did throughput plateau") with no absolute latency ceiling, and there was no notion of whether the
target disk was shared with the OS.

Fixed in `NuDBReader::calibrate_concurrency` / `shares_device_with_os_root`: device detection
(`st_dev` vs. OS root, fail-safe to "shared") selects the ladder — `[1, 2, 4, 8, 16, 32]` on
dedicated storage, `[1, 2, 4]` when sharing with the OS — plus a 50 ms absolute per-request latency
circuit breaker, and a regression test (`calibration_never_exceeds_the_given_ladder`).

**Never run concurrency or throughput experiments against a daily-driver machine's disk.**

---

## External changes assessed — no impact

- **Wrapped SLEs** (xrpld internal refactor, PRs #7791 / #7916): wraps `shared_ptr<SLE>` in
  type-safe classes. Pure C++ API refactoring, explicitly no behavior change. XRLA reads the NuDB
  binary layer and treats leaf payloads as opaque blobs. No impact.
- **XLS-100 Smart Escrows** (WASM): adds `Bytecode`/`Data` blob fields to the existing `Escrow`
  entry and a `Gas` field to `EscrowFinish`. No new ledger entry types, no new state trees, no
  change to root-hash computation. A fatter `Escrow` leaf is still just a bigger opaque blob. No
  impact.
- **XLS-101 (full Smart Contracts)** is a separate, more expansive spec — worth watching, but not
  live.
- **xrpld 3.3.0 / lending protocol** — not yet assessed in detail (specifics not reviewed as of
  this writing). By the general rule below, expected to be no-impact on the core archive (new
  object type = new opaque leaf content); the only real exposure is `meta_decode.rs`, and only if
  the lending protocol introduces a genuinely new binary wire type (a new `STI_*`), not just new
  fields on existing types. Revisit once the ledger-entry format is published.
- **Real evidence of the decoder-patch cost, not just theory**: this session hit exactly this
  scenario live — a real transaction in the test chunk used `STI_ISSUE` (type 24, added for AMM),
  which `meta_decode.rs` didn't yet handle. It failed soft (that one tx's decode errored, scan kept
  going) and the fix — adding the `Issue`-type case — took about 2 minutes. This is the concrete
  answer to "how often would a new xrpld release break this": rarely, and cheaply, when it does.

The general rule: XRLA is affected only by changes to the **SHAMap structure, node hashing, or the
NuDB on-disk format**. Changes to ledger-entry *contents* or xrpld's internal C++ APIs are
transparent to it.

---

## Repo state

- Branch `e2e-cold-start-test` (off `main` at `a777eb7`), pushed to `origin`: `994b05e` (pepper fix,
  `ledger.db` merge-safety, union-write, `Rc`-sharing memory fix) and `d8db485` (multi-chunk
  `xrla-import`). Not yet merged to `main`.
- **Uncommitted** on that branch as of 2026-09-29: the streaming `ChunkWriter` + format v3
  (`xrla-common/src/serialize.rs`, `chunk.rs`, `xrla-export/src/main.rs`) and the doc updates
  described in this file.
- Test suite: `xrla-common` has 10 tests (8 pre-existing + 2 new `ChunkWriter` tests: a v3
  write → `deserialize_chunk` round-trip, and a drop-without-`finish()` temp-file cleanup check);
  `xrla-import` has 2. All passing, no build warnings.
