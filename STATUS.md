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
ledgers predicts ~206 TB, roughly 6x the real 32–35 TB. The lifetime average is ~326 KB/ledger
(35 TB ÷ 107.3M) because early mainnet years were nearly empty — closer to our PoC-network
test than to today's traffic. Recent rate ≈ 4.3x the lifetime average.

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
| 20,000 ledgers | one **v3** chunk | **38.5 GB** | 7m35s | **18.3 GB** |

Density at the current tip: **~141 txns/ledger**, ~2,620 changed state nodes/ledger, ~1.4 MB/ledger
incremental. A mainnet checkpoint is ~13 GB (28.3M nodes, ~468 B/node) — so splitting a range into
two chunks costs an extra ~13 GB of duplicated checkpoint, which is why the two-chunk run is
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
