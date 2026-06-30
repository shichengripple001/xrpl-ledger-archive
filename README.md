# xrpl-ledger-archive

Canonical, content-addressed chunked archive format for XRPL full ledger history.

Getting full history today means ~39 TB and months of P2P backfill, all-or-nothing. This project
encodes history as deterministic, hash-verified chunks that anyone can download in parallel from
any source — and that double as the storage backend for a query layer.

See [PLAN.md](PLAN.md) for the design, [DESIGN_NOTES.md](DESIGN_NOTES.md) for the rationale,
[spec/chunk-format.md](spec/chunk-format.md) for the binary format, and
[crates/xrla-nudb/NUDB_FORMAT.md](crates/xrla-nudb/NUDB_FORMAT.md) for how the NuDB store is read.

## What it does

- **Delta-encoded, deduped.** Each chunk stores a state checkpoint plus only the SHAMap nodes that
  changed per ledger. Each unique node is stored once across the whole archive (the fix for what
  killed 2018 history sharding). Aggregate stays *below* a full node, not above.
- **Reads NuDB directly.** No running rippled, no RPC — O(1) `.key`-file lookups over the on-disk
  store, multi-shard (online_delete) and spill-chain aware.
- **Deterministic.** Two independent exports of the same range produce byte-identical chunks
  (nodes sorted by hash), so chunks are verifiable by `chunk_hash` — trustless distribution.
- **Range-addressed + stream-separable.** Download only the ledger range you need; fetch only the
  streams you need (transactions without the heavy state checkpoint).

> The chunk store can also back a query layer (local tool or a hosted, Cassandra-free Clio
> alternative). That's a design direction, not part of this repo's scope yet — see
> [DESIGN_NOTES.md](DESIGN_NOTES.md) ("Why Not Clio" and "Cost: no ScyllaDB/Cassandra tier").

## Build & run

```bash
cargo build --release

# Export a ledger range from a (stopped) rippled NuDB snapshot.
# Pass every online_delete shard's .dat — each needs a sibling nudb.key; state spans both.
./target/release/xrla-export \
  --dat /snap/shard0/nudb.dat /snap/shard1/nudb.dat \
  --ledgers /snap/ledger.db \
  --start 105277428 --end 105277478 \
  --out ./chunks/
```

## Status

PoC export path proven end-to-end on mainnet and **verified against on-chain hashes**:
- Full 27M-node state checkpoint — root hashes to the ledger's `AccountSetHash`.
- 50-ledger state deltas, deterministic across runs.
- Transactions + metadata (4,500 over 51 ledgers) — every txid authentic and each ledger's
  tx-tree root matches the on-chain `TransSetHash`.

Open: round-trip verification (`xrla-import` replay), state-leaf hash coverage, and validating the
storage floor at scale. See PLAN.md. (A deterministic-but-wrong sparse-inner decode bug was caught
here only by the on-chain hash check — determinism alone is not correctness.)
