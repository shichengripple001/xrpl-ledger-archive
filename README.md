# xrpl-ledger-archive

Canonical, content-addressed chunked archive format for XRPL full ledger history.

Getting full history today means ~43 TB and months of P2P backfill, all-or-nothing (measured
2026-09-29 on a real full-history node: 32 TB NuDB + 11 TB `transaction.db` + 296 GB
`ledger.db`; a fresh node backfills real mainnet history at ~12 ledgers/min). This project
encodes history as deterministic, hash-verified chunks that anyone can download in parallel from
any source — and that double as the storage backend for a query layer.

See [CONTEXT.md](CONTEXT.md) for who this is for and what it competes with,
[PLAN.md](PLAN.md) for the design, [STATUS.md](STATUS.md) for what is actually proven vs. still
reasoning,
[E2E_TEST_PLAN.md](E2E_TEST_PLAN.md) for the end-to-end cold-start test and its results,
[spec/chunk-format.md](spec/chunk-format.md) for the binary format, and
[crates/xrla-nudb/NUDB_FORMAT.md](crates/xrla-nudb/NUDB_FORMAT.md) for how the NuDB store is read.

## What it does

- **Delta-encoded, deduped.** Each chunk stores a state checkpoint plus only the SHAMap nodes that
  changed per ledger. Each unique node is stored once across the whole archive (the fix for what
  killed 2018 history sharding). Aggregate stays *below* a full node, not above.
- **Reads NuDB directly.** No running xrpld, no RPC — O(1) `.key`-file lookups over the on-disk
  store, multi-shard (online_delete) and spill-chain aware.
- **Deterministic.** Two independent exports of the same range produce byte-identical chunks
  (nodes sorted by hash), so chunks are verifiable by `chunk_hash` — trustless distribution.
- **Range-addressed + stream-separable.** Download only the ledger range you need; fetch only the
  streams you need (transactions without the heavy state checkpoint).

> The chunk store can also back a query layer (local tool or a hosted, Cassandra-free Clio
> alternative). That's a design direction, not part of this repo's scope yet — see
> [CONTEXT.md](CONTEXT.md) ("Correcting the record on Clio" and "Cost: no ScyllaDB/Cassandra tier").

## Build & run

```bash
cargo build --release
export PATH="$(pwd)/target/release:$PATH"

# Export a ledger range from a (stopped) xrpld NuDB snapshot.
# Pass every online_delete shard's .dat — each needs a sibling nudb.key; state spans both.
# --chunk-size controls ledgers per chunk (default 10,000); only the first chunk in the
# whole run costs a full trie walk, every later chunk snapshots the running in-memory state.
xrla-export \
  --dat /snap/shard0/nudb.dat /snap/shard1/nudb.dat \
  --ledgers /snap/ledger.db \
  --start 105277428 --end 105277478 \
  --chunk-size 10000 \
  --out ./chunks/

# Inspect a chunk without importing it — summary, per-ledger detail, or a specific
# transaction by index or directly by hash (no need to know which ledger it's in).
xrla-inspect --chunk ./chunks/xrla_1_0105277428_0105277478.xrla
xrla-inspect --chunk ./chunks/xrla_1_0105277428_0105277478.xrla --ledger 105277430
xrla-inspect --chunk ./chunks/xrla_1_0105277428_0105277478.xrla --tx-hash <64-char hex tx hash>
```

## Status

**Proven on real mainnet data** — a real xrpld process opens, boots from, and correctly serves
output written by `xrla-import` (2026-09-28), verified on a 7-node network with the target range
deleted from *every* peer first, so no peer could have supplied it. The export path round-trips
cryptographically: every transaction ID, account-state root, and chained `LedgerHash` is
independently recomputed and checked against on-chain values.

**Measured at the live mainnet tip** (2026-09-29): 44 ledgers/sec; 20,000 ledgers export to a
single 41.35 GB chunk in 7m35s at 18.3 GB peak RSS.

**Not proven**: anything at full-history scale. No genesis-to-tip run has ever been done, and
compression is unmeasured. Note that extrapolating archive size from tip density overestimates
badly — 1.4 MB/ledger x 107.3M ledgers predicts ~206 TB against a real ~32 TB, because early
mainnet years were nearly empty.

> For the claim-by-claim breakdown of **what is proven on real data vs. what is still
> reasoning** — including the three bugs that only a real xrpld could surface — see
> **[STATUS.md](STATUS.md)**. [PLAN.md](PLAN.md) holds the design rationale, storage model,
> and ordered TODO list.
