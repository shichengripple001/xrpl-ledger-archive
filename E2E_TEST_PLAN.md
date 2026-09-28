# E2E test plan: export → import → a real xrpld serves it

> Written 2026-09-28. Companion to `TEST_PLAN.md` (unit/integration tests) and `STATUS.md`
> (what is proven vs. reasoned). This plan targets the single largest untested claim in the
> project: that a real xrpld process can open our output. See `CONTEXT.md` for why that claim
> matters to the intended users.

## Context

`xrpl-ledger-archive` has proven, on real mainnet data, that an export round-trips
*cryptographically*: `xrla-import` replays checkpoint + deltas and matches every ledger's
`account_hash`, chained `LedgerHash`, transaction IDs, and per-node hashes.

It has **never** proven the thing the project exists for: **that a real xrpld process can open our
output and serve those ledgers.** `STATUS.md` says so plainly — the NuDB writer "has NOT been
tested against a real rippled process." Reading our own writer back through our own reader proves
internal consistency, not compatibility; a shared misunderstanding of the format is invisible to
that test.

Three questions, in priority order:

1. **Does a real xrpld accept our output and serve the range over RPC?**
2. **What chunk size should we ship?**
3. **How do we justify that total archive size won't blow past a real full-history node?** (The
   exact failure that killed history sharding in 2018–2024.)

## Decisions taken

- **Target xrpld 3.4.1** (`rippleci/xrpld:3.4.1`). amd64-only, so it runs emulated on this arm64
  Mac. Source data will be produced by the 3.2.0 sensor, making this a cross-version test by
  construction. *Contingency*: if it fails, re-run against the local native `3.2.0-arm64` image to
  separate "our format bug" from "3.2.0→3.4.1 drift" before drawing conclusions.
- **Source = most recent ledgers.** Discard the stale July data.
- **Both nodes configured to never delete** — this is a config property, not a storage commitment.
  No 33.5 TB and no months of P2P backfill required.

## Verified findings that shape this plan

| Finding | Evidence | Consequence |
|---|---|---|
| **FATAL: wrong NuDB `pepper`** — we write `xxh64(&[], salt)`; NuDB computes it from the salt's little-endian bytes (`pepper()` in `detail/format.hpp`) and `verify()` rejects a mismatch with `error::hash_mismatch` | `writer.rs:123` vs upstream NuDB source | **rippled cannot open our store at all.** One-line fix. Invisible to our tests because our own reader ignores pepper. |
| Header otherwise correct: `version=2` = `currentVersion`; `appnum=1` = rippled's `kAppNum` (it throws `"nodestore: unknown appnum"` otherwise); uid consistent across `.dat`/`.key` | `writer.rs:43-46`; `NuDBFactory.cpp:56,164` | No further header changes needed. |
| **No `ledger.db` writer exists** | `xrla-import` takes only `--chunk`/`--dat`/`--skip-verify` | Blocks the test. Must be built. |
| **Import writes only the FINAL live state** | `ReplayResult.state` doc comment; `state.remove(hash)` on every `diff.deleted` (`xrla-import/src/main.rs:142-144`) | Imported DB can serve only the *last* ledger of a chunk. Must be fixed. |
| **`complete_ledgers` is runtime-only state** | `LedgerMaster::completeLedgers_` (`RangeSet`); nothing seeds it from `ledger.db` at startup | Importing alone will **not** make the node advertise the range. Needs `--force_ledger_present_range MIN,MAX`. |
| **But serving doesn't need it** | `ledger ledger_index:N` → `loadByIndex()` reads ledger.db, rebuilds SHAMap from NodeStore | `ledger` RPC is the *real* proof — it actually exercises our NuDB. `tx` does check `haveLedger`. |
| **We write codec 0x00 (raw); rippled writes LZ4** | `dat.rs:313 encode_wire_to_value` always emits uncompressed | Our imported `.dat` is materially larger than an equivalent real node's. Directly relevant to Q3. |
| **Chunks are not compressed** | no zstd/lz4 in `serialize.rs` | Every size projection in `PLAN.md` assumes ~2x compression that isn't implemented. |
| Checkpoint ≈ **9.65 GB raw** (27,031,655 nodes); delta ≈ **1.02 MB/ledger** | measured export | Checkpoint dominates any chunk under ~9,460 ledgers. |
| Free disk: **126 GB**; history grows ~12 GB/day | `df -h`; `PLAN.md` | Bounds both accumulation time and the chunk-size experiment. |

## Part 1 — Three code changes (prerequisites)

**1a. Fix the NuDB `pepper` (one line, blocks everything else).** In `writer.rs::write_key_header`,
replace `xxh64(&[], salt)` with `xxh64(&salt.to_le_bytes(), salt)` to match NuDB's
`pepper<Hasher>(salt)`. Until this lands, a real xrpld throws on open and *no* other result from
this test is meaningful.

**Smoke test immediately after 1a**, before building anything else: import any existing chunk, point
a throwaway 3.4.1 container at it, and confirm it opens the store without `hash_mismatch` or
`"unknown appnum"`. This is the cheapest possible early kill-signal — if the store still won't open,
stop and debug the format rather than proceeding to `ledger.db` work.

**1b. `ledger.db` writer** — new `--ledger-db <path>` on `xrla-import`. Schema must match rippled's
`kLgrDbInit` (`include/xrpl/rdb/DBInit.h`) exactly:

```sql
CREATE TABLE IF NOT EXISTS Ledgers (
  LedgerHash CHARACTER(64) PRIMARY KEY, LedgerSeq BIGINT UNSIGNED,
  PrevHash CHARACTER(64), TotalCoins BIGINT UNSIGNED,
  ClosingTime BIGINT UNSIGNED, PrevClosingTime BIGINT UNSIGNED,
  CloseTimeRes BIGINT UNSIGNED, CloseFlags BIGINT UNSIGNED,
  AccountSetHash CHARACTER(64), TransSetHash CHARACTER(64) );
CREATE INDEX IF NOT EXISTS SeqLedger ON Ledgers(LedgerSeq);
```

Every value is already available and *already verified* during replay: `TxMap` carries
`ledger_seq`/`ledger_hash`/`account_hash`/`drops`/close-time fields; `TransSetHash` comes from
`build_tx_tree`; `PrevHash` is the previous entry's `ledger_hash`. This is the same field set
`serialize::calculate_ledger_hash` consumes, and the same set `xrla-export` SELECTs — so it's a
direct mapping, not new research. Hashes are stored as uppercase hex strings (match
`xrla-export`'s `parse_hash` convention). Note: the **first** ledger in a chunk has no in-chunk
`PrevHash`; either carry it from the export or omit that one row.

**1c. Write the union, not the final live set** — in `write_to_nudb`, accumulate every node the
chunk contains (checkpoint ∪ all `diff.added`) instead of the post-replay `state` map. Keep
applying `deleted` for root-tracking/verification; just stop dropping those nodes from the
write-set. No new data is needed — it's already in the chunk. This is what a real full-history node
holds, and without it only the last ledger of each chunk is servable.

`wallet.db`/`transaction.db` remain deliberately unwritten (researched 2026-08-24: no XRPL account
data; an empty `NodeIdentity` makes rippled mint a fresh keypair, fine for a non-validator).

## Part 2 — The cold-start test

**2a. Reconfigure the sensor to stop deleting.** In `xrpl-sensor-node/config/xrpld.cfg`: remove
`online_delete=256`, set `[ledger_history]` to `full`. (`online_delete` must not be less than
`ledger_history` or startup throws.) Restart, confirm `server_state: full`/`proposing` via
`server_info` on `127.0.0.1:5005`. It now accumulates forward from the current tip instead of
holding a rolling 256-ledger window. **Watch disk: ~12 GB/day against 126 GB free.**

**2b. Snapshot.** Once enough ledgers have accumulated, `docker compose stop` — the DB **must** be
quiesced; copying a live NuDB yields a torn snapshot. Copy both shard dirs + `ledger.db` to the
scratchpad. Restart the sensor afterwards if you want it to keep collecting.

**2c. Export** a recent range from the snapshot, then **import** it (with 1a–1c) into a fresh
NuDB + `ledger.db`.

**2d. Serve it.** A second, isolated container (`rippleci/xrpld:3.4.1`, own volume — the sensor's
volume is the *source* and must stay pristine), its own RPC port, with a config that cannot prune
or overwrite: `[ledger_history] full`, **no** `online_delete`, `earliest_seq` ≤ lowest imported
seq, and no peers (`--standalone`) so it can't sync over the range.

**2e. Verify over RPC**, weakest-to-strongest:

| Check | Expectation | What it proves |
|---|---|---|
| `--ledger <seq>` at boot | node loads it | Forces `walkLedger` over our NuDB — fails loudly if malformed |
| `ledger {ledger_index: N}` | `ledger_hash` matches our export **and** a public explorer | Real read path: ledger.db → NodeStore SHAMap rebuild |
| Same, at range **start**, middle, end | all succeed | Validates the 1c union fix (pre-fix, only the last would work) |
| `ledger_data` | returns state nodes | NodeStore genuinely readable, not just the index |
| `server_info` → `complete_ledgers` | shows range **only** with `--force_ledger_present_range` | Confirms the advertise-vs-serve distinction |
| `tx <hash>` | returns matching blob/meta | `tx` gates on `haveLedger`, so needs the forced range |

**Pass = a real 3.4.1 xrpld serves our imported ledgers.** Anything less is a
format-compatibility finding — which is precisely what this test exists to surface.

## Part 3 — Chunk size

Chunk size currently *is* checkpoint interval — one knob, two opposing pressures:

```
chunk bytes      ≈ 9.65 GB (checkpoint) + C × 1.02 MB (deltas)
archive overhead ≈ (N / C) × checkpoint_size
worst-case replay ≈ C deltas
```

Crossover where deltas overtake the checkpoint: **C ≈ 9,460 ledgers.**

**Experiment** (bounded, cleaning between runs): export the *same* range at 3 chunk sizes (e.g.
300/100/50 → 1/3/6 chunks), measure total bytes, confirm overhead scales as
`n_chunks × checkpoint_size`. Also **zstd-3 a real chunk** to confirm the claimed ~1.95x, since
every projection depends on it and compression isn't implemented.

At full-history scale (today's checkpoint as an upper bound, raw):

| Chunk size | Checkpoints | Overhead | Verdict |
|---|---|---|---|
| 10,000 | ~10,500 | ~101 TB | fatal — 3x the entire floor |
| 100,000 | ~1,050 | ~10 TB | ~30% overhead |
| 1,000,000 | ~105 | ~1 TB | ~3%, but ~1 TB *per file* — unusable |

**Recommendation: decouple checkpoint interval from chunk size** (flagged "decide in Phase 1" in
`PLAN.md`, never done). Chunks stay small for download granularity (~1k–10k ledgers); a full
checkpoint is written only every ~500k–1M ledgers, with intervening chunks referencing the nearest
preceding one. This is a format change, not a flag — scope it after this test.

## Part 4 — The size justification

Measured anchors from a real full-history node: `nudb.dat` = **29.5 TB**, `nudb.key` = **4.0 TB**.

1. **The floor is fixed and equals a full node's `.dat`.** Sum of all deltas = every unique SHAMap
   node, stored once (content-addressed). Not an estimate — it follows from the format. This is
   also exactly why this isn't the 2018 sharding blowup, where every shard re-stored unchanged
   upper-trie inner nodes.
2. **Checkpoint overhead is the only thing that can exceed it**, and it's a controllable
   `(N / interval) × checkpoint_size`.
3. **Today's checkpoint is a strict upper bound on every historical one** — state has grown over
   the network's life, so using 9.65 GB for all of history deliberately over-estimates.
4. **We omit the 4.0 TB `.key` index entirely** (rebuilt at import).

→ At a 1M-ledger checkpoint interval, overhead ≤ ~1 TB **while saving 4.0 TB** on the key file.
The archive lands *below* a full node, with ~3 TB of margin — before compression.

**Caveats to state, not bury:** (a) the ~2x compression underpinning half these figures isn't
implemented; (b) we write codec 0x00 raw where rippled writes LZ4, so an operator's *imported* DB
is larger than a real node's — **measure this directly in the test** by comparing our `.dat`
against the sensor's for the same ledger count; (c) "state only grows" is broadly true but not
rigorous (`AccountDelete` exists); (d) early-history delta rates are unmeasured — only recent
samples exist, and `PLAN.md` already notes recent rates over-estimate early history.

## Risks

- **I/O safety.** The 2026-07-08 incident (hard restart required) came from concurrency
  experiments on this machine's shared disk. Nothing here runs above the hardened shared-disk
  ladder `[1,2,4]`. Observed: checkpoint walk ~126s at concurrency 2.
- **Disk.** 126 GB free; each checkpoint written is ~9.65 GB and the sensor now grows ~12 GB/day
  once pruning is off. Check free space before each step; clean up between runs.
- **The sensor's volume is the export source** — the test node gets its own.

## Verification / definition of done

1. A real 3.4.1 xrpld returns a correct `ledger_hash` for ledgers at the **start, middle, and end**
   of the imported range (start/middle passing is the proof that 1c works).
2. The chunk-size cost model is confirmed by measurement, not assumed.
3. Compression ratio measured on a real chunk; our `.dat` size compared against the sensor's.
4. `PLAN.md` / `STATUS.md` updated with results — **including negative ones**, per the project's
   established "determinism ≠ correctness" discipline.
