# XRLA — Status: what is proven, what is not

Last updated 2026-08-24.

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

Inspected two real rippled full-history nodes (`devnet-fh-usw2-01`, `livenet-fh-usw2-01`)
on-disk, 2026-07-08:

- Mainnet `nudb.dat` ≈ 29.5 TB, `nudb.key` ≈ 4.0 TB (~33.5 TB combined)
- Derived total record count from the real key-file size: **~55–111 billion** unique
  (state + tx-tree) nodes across all history
- **No `shard_db`, no `online_delete`** on either box — a single permanently-growing `node_db`.
  This invalidates any plan assuming "just open the shard for the ledger range you want."

---

## Built and working, but not yet proven at scale

### `meta_decode` / `account_tx` query

`xrla-common/src/meta_decode.rs` decodes rippled's binary STObject format to extract every
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

`xrla-nudb/src/writer.rs::write_nudb_store` writes a fresh `.dat`/`.key` pair. Round-trip tested
through our own `keyfile::Shard::fetch`, including a forced spill-chain case.

**Gap: never opened by a real rippled process.** Reading back through the same reader that informed
the writer's design proves internal consistency, not compatibility. A shared misunderstanding of
the format would be invisible to this test. The file says so itself.

---

## Not started

### `ledger.db` writer — the blocker for a real cold-start test

The only missing artifact between "we can write a NuDB store" and "rippled boots from it."

Researched 2026-08-24 from rippled source:

| File | Needed? | Why |
|---|---|---|
| NuDB `.dat`/`.key` | ✅ required | All ledger data — accounts, trust lines, offers, transactions |
| `ledger.db` | ✅ required | Ledger index; rippled reads it to find its tip and resume |
| `wallet.db` | ❌ not needed | Server's own P2P identity, peer reservations, manifest cache — **no XRPL account data** |
| `transaction.db` | ❌ not needed | Convenience tx-lookup index |

All three SQLite DBs are opened via `DatabaseCon` (`include/xrpl/rdb/DatabaseCon.h`), which lets
SQLite create a missing file and then runs `CREATE TABLE IF NOT EXISTS` from the `*DbInit` arrays
in `include/xrpl/rdb/DBInit.h`. Nothing checks for a pre-existing file. An empty `NodeIdentity`
table causes `getNodeIdentity()` (`src/libxrpl/server/Wallet.cpp`) to mint a fresh random keypair —
fine for a non-validating server.

Writing `ledger.db` is wiring, not research: every needed value (`LedgerSeq`, `LedgerHash`,
`PrevHash`, `AccountSetHash`, `TransSetHash`, close-time fields) is already present and verified in
the chunk.

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
  small-scale benchmarks, not measurements.

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

- **Wrapped SLEs** (rippled internal refactor, PRs #7791 / #7916): wraps `shared_ptr<SLE>` in
  type-safe classes. Pure C++ API refactoring, explicitly no behavior change. XRLA reads the NuDB
  binary layer and treats leaf payloads as opaque blobs. No impact.
- **XLS-100 Smart Escrows** (WASM): adds `Bytecode`/`Data` blob fields to the existing `Escrow`
  entry and a `Gas` field to `EscrowFinish`. No new ledger entry types, no new state trees, no
  change to root-hash computation. A fatter `Escrow` leaf is still just a bigger opaque blob. No
  impact.
- **XLS-101 (full Smart Contracts)** is a separate, more expansive spec — worth watching, but not
  live.
- **rippled 3.3.0 / lending protocol** — not yet assessed in detail (specifics not reviewed as of
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
NuDB on-disk format**. Changes to ledger-entry *contents* or rippled's internal C++ APIs are
transparent to it.

---

## Repo state

- Uncommitted on `main`: `meta_decode.rs` + `examples/decode_one_meta.rs` (new), and edits to
  `lib.rs`, `serialize.rs` (adds `sha256`), `xrla-inspect/src/main.rs` (adds `--account`).
- Branch `feat/concurrent-diff-batching` — **already fast-forward merged into `main`** (`main` HEAD
  is `a777eb7`, identical to the branch tip) and pushed to `origin/main`. The branch ref itself
  still exists but is fully merged, not pending.
- Test suite: **14 tests pass, 2 ignored** (real-snapshot-gated) — 8 in `xrla-common`, 1 in
  `xrla-import`, 5 passing + 2 ignored in `xrla-nudb`. Verified directly via `cargo test --release`,
  not carried over from an earlier count. `meta_decode.rs` contributes none yet (see gap above).
