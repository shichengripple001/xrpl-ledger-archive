# xrpl-ledger-archive

Canonical, content-addressed chunked archive format for XRPL full ledger history.

Getting full history today means ~39 TB and months of P2P backfill, all-or-nothing. This project
encodes history as deterministic, hash-verified chunks that anyone can download in parallel from
any source — and that double as the storage backend for a query layer.

See [CONTEXT.md](CONTEXT.md) for who this is for and what it competes with,
[PLAN.md](PLAN.md) for the design, [STATUS.md](STATUS.md) for what is actually proven vs. still
reasoning, [DESIGN_NOTES.md](DESIGN_NOTES.md) for the rationale,
[E2E_TEST_PLAN.md](E2E_TEST_PLAN.md) for the outstanding end-to-end test,
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
export PATH="$(pwd)/target/release:$PATH"

# Export a ledger range from a (stopped) rippled NuDB snapshot.
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

> For a claim-by-claim breakdown of **what is proven on real data vs. what is still reasoning**,
> see [STATUS.md](STATUS.md). `PLAN.md` holds the design rationale and ordered TODO list.

PoC export path proven end-to-end on mainnet and **verified against on-chain hashes**:
- Full 27M-node state checkpoint — root hashes to the ledger's `AccountSetHash`.
- 50-ledger state deltas, deterministic across runs.
- Transactions + metadata (4,500 over 51 ledgers) — every txid authentic and each ledger's
  tx-tree root matches the on-chain `TransSetHash`.
- Full `LedgerHash` per ledger — independently recomputed and verified against `ledger.db` for
  all 51 ledgers, embedding `parent_hash` so a chunk is a self-contained hash chain (see
  DESIGN_NOTES.md "Tamper detection without a second full-history copy").

`xrla-import` now really does something (it used to be two stubs — a `verify_ledger_hashes`
that only printed, and a NuDB writer that only counted). It replays checkpoint+deltas,
rebuilds each ledger's transaction tree, and independently recomputes + asserts every
transaction's own hash, the account-state root, and the full chained `LedgerHash` — bailing
on any mismatch instead of trusting the file. It then writes a real NuDB `.dat`/`.key` pair.
Validated so far: a synthetic 2-ledger chunk exercising the whole chain (including a
deliberately-tampered `LedgerHash` being caught), plus ~200 real, rippled-produced node
values sampled from a live mainnet shard round-tripped byte-for-byte through the new
writer, and later a full real 51-ledger export → import round trip against a real mainnet
NuDB snapshot (4,500 real transactions, 27M+ state nodes, every ledger's account_hash and
chained LedgerHash independently verified). **Not yet done**: running the writer's output
through an actual rippled process (see TEST_PLAN.md).

On import, every state-tree node's own hash — inner *and* leaf (`AccountState`) — is now
independently recomputed from its raw content and checked against its claimed identity
(`xrla_common::state_tree`), not just the overall root. Both formulas are real-data-validated
exhaustively (not sampled): all 27,031,655 nodes in a real mainnet checkpoint (7,912,690
inner + 19,118,965 leaves), zero mismatches. This catches source-side corruption or decode
bugs at any single node, not only ones large enough to shift the root.

Open: automated checkpoint RPC anchoring (checking a chunk's first ledger against
independent nodes, not just self-consistency), running the writer's output through an actual
rippled process, checkpoint sparsity across chunks (every `.xrla` file still bundles its own
full checkpoint — see DESIGN_NOTES.md), and validating the storage floor at scale. See
PLAN.md. (A deterministic-but-wrong sparse-inner decode bug was caught here only by the
on-chain hash check — determinism alone is not correctness.)

**Full-history export at scale: the two blocking architectural changes are now implemented and
validated against real data at small scale; unmeasured at full-history scale.** Real full-history
rippled nodes inspected directly (`livenet-fh-usw2-01`): a single, permanently growing
`nudb.dat`/`nudb.key` pair (no shard store, no `online_delete`), 29.5 TB / 4.0 TB. Deriving total
record count from the real key-file size gives ~55–111 billion node touches across mainnet
history — the real total workload for a full export, not a guess. `NuDBReader`'s lookup mechanism
is confirmed genuinely O(1) with store size (benchmarked up to a 180 MB key file, see
`crates/xrla-nudb/examples/bench_lookup.rs`). `xrla-export` now (1) maintains one running state
map across the whole export instead of re-walking per chunk (cuts full trie walks from ~1/chunk
to 1 total) and (2) issues concurrent, self-calibrating NuDB reads instead of one blocking read
at a time. **2026-07-08 incident and fix**: the first version of the concurrency calibration
saturated a laptop's shared disk badly enough to require a hard restart — fixed with a
same-disk-detection safety ceiling and an absolute-latency circuit breaker (see PLAN.md Phase 2
item 2). With the fix in place, both changes were then validated end-to-end against 100 real
mainnet ledgers (`--chunk-size 30` → 4 chunks): exactly one trie walk for the whole run, every
chunk `xrla-import`-verified (account_hash, chained LedgerHash, full state-tree self-consistency
over 27M+ nodes each) with no failures. Paper estimate for full mainnet scale with both changes:
~10–40 days; still unmeasured at that scale, and any such testing must run on dedicated storage,
never a daily-driver machine. See PLAN.md Phase 2 and Immediate TODOs 8–11.
