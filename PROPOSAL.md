# XRPL Ledger Archive — Proposal

Draft, 2026-10-01. Informal: written to get agreement on direction, not to be the spec.
Design detail lives in [PLAN.md](PLAN.md), evidence in [STATUS.md](STATUS.md), positioning in
[CONTEXT.md](CONTEXT.md).

Numbers are tagged **measured** (we ran it), **observed** (read from production dashboards or a
real node), or **estimate** (arithmetic, not yet confirmed). Estimates are listed again at the end.

## The problem

Full XRPL history can't be obtained, can't be verified in bulk, and costs a lot to serve.

- **It is huge and slow to copy.** A full-history node is **32 TB NuDB + 11 TB `transaction.db` +
  296 GB `ledger.db`** (observed, 2026-09-29), growing ~12 GB/day. The only route is P2P backfill.
  On a real node only 3 of 16 peers held deep history, and the rate degrades past ~20k ledgers deep
  (both measured). A full backfill takes months (**estimate**, never measured end to end).
- **Most nodes don't have it to give.** Most run `online_delete` with a rolling window (our own
  sensor node keeps 256 ledgers, about 17 minutes). A node that never held the history can't supply
  it, and nothing downstream, Clio included, can recover it.
- **It can't be handed over in pieces.** Operators can't share history or take just a range; it is
  all or nothing. History sharding, the official attempt, was removed in xrpld 2.3.0 because every
  shard duplicated unchanged tree nodes.
- **There is no verified, bulk form.** Clio returns database rows, with no way to prove they belong
  to a ledger's state without trusting the database and the pipeline that wrote it. Its API is
  point queries, so a historical time series is N round trips.
- **Serving it is expensive.** Clio's ScyllaDB tier is **~$232k/year across devnet, testnet and
  mainnet**. The mainnet-only figure is not known. Observed on mainnet Clio over 7 days: ~1,760 req/s
  average, flat for 90 days; about 90% is current-state traffic, and `account_tx` and `tx` together
  are ~165 req/s (~9%). Of ledger-scoped requests, ~89% ask for the newest ledger, ~94% stay within
  the last ~5 days, and only ~5% reach back more than ~46 days. Current state is served from memory
  (97.7% cache hit rate, ~12-13 GB per node), not from the database.
- **The store is slow to change.** It is filled by sequential ingest from a live rippled. We found no
  offline bulk-load path in Clio's docs or source (read, not run). Fixing a bug or changing the
  schema means re-ingesting history ledger by ledger; how long that takes is not measured.
- **The demand is real.** The anchor case: a market-making firm couldn't get what it needed from
  Clio's full-history mode and asked for a full-history xrpld node directly (CONTEXT.md).

### What a solution has to do

- Let an operator fetch all of history or just a range, from any source, and verify every chunk
  against on-chain hashes without trusting where it came from.
- Seed a working node from it in hours, not months (import is measured at 39 minutes per 150k chunk;
  the whole archive is not measured).
- Serve `account_tx` and `tx` for all history at roughly Clio's latency (observed 7-day mean:
  `account_tx` 21 ms, `tx` 3 ms) at ~165 req/s with headroom.
- Serve current state from memory at Clio's volume, with xrpld only feeding ledgers in and
  forwarding writes.
- Include the newest ledgers through a live tail, not only sealed chunks.
- Rebuild from the archive in parallel, and lose a server without a long re-ingest.
- Never silently drop a transaction from an account's history.

## Stage 1 — Archive service

### What it is

History cut into **150,000-ledger chunks** (~7 days of mainnet each, ~715 chunks). Each chunk holds
one state checkpoint plus only the tree nodes that changed per ledger, and the full transactions
and metadata. Chunks are deterministic (nodes sorted by hash), so two independent exports of a
range produce the same bytes, and anyone can verify a chunk without trusting where it came from.

### What already works (measured)

- **Export** reads a stopped xrpld's NuDB store directly. One real 150k chunk
  (ledgers 107,147,192–107,297,191): **47 min, 19.2 GB peak RAM, 207.8 GB chunk**.
- **Import** into a fresh store: **39 min, 46.1 GB peak RAM**, 345M unique nodes. Every ledger's
  `account_hash`, transaction hashes and parent-chained `LedgerHash` are recomputed and checked.
- **Cold start:** a node wiped and reseeded from the archive caught up to the network and served
  data matching r.ripple.com. Sampled comparison: **10,000 ledgers, 1.24M transactions, 13.7M field
  comparisons, zero mismatches** (earlier run: 1,650 ledgers, ~220k transactions, zero).
- Both memory blowups found on the way (127 GB read side, 96 GB write side) are fixed, and import
  memory now scales with unique-node count, not chunk length.

### What is left

| Item | Notes |
|---|---|
| **Export the whole history** | The long pole. At the measured ~53 ledgers/s, 107M ledgers is ~23 days in one process (**estimate**, upper bound: early history is far sparser, and separate ranges can run in parallel, each paying one checkpoint walk). Needs a stopped full-history node as the source. |
| **Publisher** | A tool that writes `manifest.json` (chunk range, `chunk_hash`, size, URL, torrent info-hash) and one torrent per chunk. Not built. |
| **Hosting** | S3 (or equivalent) as the always-on source and web seed. See egress below. |
| **Freeze the format** | `spec/chunk-format.md` is still DRAFT. Publishing freezes it, and changing it later means re-exporting everything. See the first decision below. |
| **Measure compression** | Done for the recent era: the 207.8 GB chunk compresses to **93.6 GB (2.22×)** with zstd level 3. Older eras are not measured. |
| **Operator guide** | Download, verify, import, catch up, with the failure modes we hit. |

### Distribution: S3 plus BitTorrent

- **S3** holds every chunk and the manifest. It is the reliable seed.
- **BitTorrent** with S3 as a web seed lets operators and mirrors share the load. Chunks are
  content-addressed and deterministic, so any holder can seed them.
- **Why not S3 alone:** egress. At roughly $0.09/GB (**verify**) one full download is ~20 TB, about
  $1.8k, per downloader. BitTorrent moves most of that off us. A zero-egress host is another option
  to evaluate.
- **Trust:** a downloader verifies `chunk_hash` against the manifest, and import re-derives every
  ledger hash. A final check of the newest ledger hash against the live network anchors the chain.

### Size and time (all estimates until the full export exists)

| | |
|---|---|
| Archive, uncompressed | ~39–42 TB (floor of unique nodes + one ~10 GB checkpoint per chunk) |
| Archive, compressed | ~18–19 TB (**estimate**: the 39–42 TB total divided by the 2.22× measured on one recent chunk, assuming older eras compress alike) |
| One recent 150k chunk | 207.8 GB measured; **93.6 GB compressed, measured** (zstd level 3, 2.22×) |
| Download of the whole archive | ~4–5 h at 10 Gbps, ~2 days at 1 Gbps |
| S3 storage | ~19 TB (the 39–42 TB total divided by the measured 2.22×), ~$440/month. Upper bound if older eras do not compress: ~42 TB, ~$970/month. (**verify** pricing) |

### Limits to state up front

- A seeded node serves state and ledgers immediately. `xrla-import --txdb` also writes xrpld's
  `transaction.db` (Transactions + AccountTransactions) and each ledger's header object, so
  `account_tx` works on imported history as soon as the imported range joins the live tip. Until
  then xrpld returns `lgrIdxsInvalid`: the gap between the chunk's end and the tip is fetched from
  peers (13.5 h for a 71k-ledger gap, measured). Import cost for a 150k chunk: 47 min with
  `--txdb` against 39 min without, 38 GB `transaction.db`, 46 GB peak memory.
- Measured per 150k chunk: 17,512,432 transactions, 34,997,796 account rows (2.0 per transaction),
  index build 13:52 and 5.07 GB (SQLite, unpacked).
- Verified on the 150k chunk against s2.ripple.com (Clio, full history): `account_tx` lists for
  10,000 random accounts (926,903 rows, same hashes, same order) and the stored transaction and
  metadata bytes of 10,000 random transactions, 0 differences. On the 5k chunk the import's rows
  matched xrpld's own rows exactly (275,510 transactions, 527,613 account rows).
- Import memory grows with the number of unique nodes imported (46 GB for one 150k chunk). How it
  scales to a many-chunk, full-history import is not measured. The export source must also be
  stopped while it is read.

### Done when

The full range is published with a manifest and torrents; a fresh machine can pull it and reach
`full` on mainnet; and a sampled comparison against independent real xrpld nodes shows zero
mismatches.

## Stage 2 — Query layer

### Goal

Serve the same API Clio serves, from the archive, without the ScyllaDB tier.

### What traffic actually looks like (observed, Clio dashboard, 7 days)

- **~1,760 requests/s average, ~2,070 peak**, flat for 90 days, on 5 read nodes (~350 req/s each).
- Biggest methods: `account_info` 350, `ping` 296, `ledger` 250, **`account_tx` 138**,
  `server_info` 136, `nft_sell_offers` 130, `amm_info` 105, `book_offers` 101, **`tx` 25**.
- **89% of ledger-scoped requests ask for the newest ledger; ~94% stay within the last ~5 days.**
- Clio keeps the **whole current state in RAM** on each read node (cache hit rate **97.7%**, ~13 GB
  per node). That is why one Clio node handles ~350 req/s. xrpld cannot, so xrpld does not serve
  this traffic.

### Design

| Part | What it does | Sizing |
|---|---|---|
| **Current-state cache** | Latest state in memory, updated per validated ledger (~190 changed objects each). Serves `account_info`, `account_lines`, `book_offers`, `amm_info`, etc. | ~13 GB per node; ~6 nodes (**estimate**) |
| **History service** | `account_tx`, `tx`, `ledger` for all history, from compressed per-ledger transaction blocks plus an account index and a hash index, on local NVMe. | ~3–4 TB (**estimate**); 3 × i4i.8xlarge across zones |
| **Historical-state store** | Object versions per ledger, for state queries on old ledgers. | ~1.5–1.6 TB (**estimate**; the successor table part is unmeasured); fits on the history nodes |
| **xrpld feeders** | Small xrpld nodes that feed validated ledgers in and forward `submit`, `fee`, `ledger_current`. | 2–3 small, ~100 req/s |
| **Router** | Sends each request to the right part. Stateless. | 2 small |

Key choices:

- **Reuse Clio's server code** by implementing its storage interface against our store, instead of
  rewriting ~39 methods and their amendment handling. We would keep Clio's handlers, JSON and cache.
  The cost is a C++ layer over our Rust code, and tracking changes to Clio's internal interface.
- **Transaction bodies stored once, in ledger order**, so a transaction's Merkle path stays
  verifiable. Indexes point at them.
- **Built offline in parallel from verified chunks.** Chunks are independent, so a full rebuild is
  hours of parallel work (**estimate**), where Clio's only path is sequential ingest. A decoder bug
  or schema change means re-running the build, not re-ingesting for months.
- **Live tail:** the newest ledgers are indexed from the feeders, so `account_tx` includes today,
  and the tail hands over when the next chunk is built.
- **Recovery:** local NVMe is lost on maintenance, so replicas re-download built indexes from S3
  (~1 h, **estimate**). Three replicas allow rolling maintenance with redundancy.

### Cost (**estimate**; AWS us-west-2 on-demand prices from memory, verify)

New pieces for **mainnet**, replacing the ScyllaDB tier:

| | On-demand | With the 50% savings plan |
|---|---|---|
| History service, 3 × i4i.8xlarge | ~$6.0k/month | ~$3.0k |
| S3 (~20 TB archive + ~3.7 TB built indexes, ~$0.55k), monitoring and transfer (~$0.3k, **a guess**) | ~$0.85k | ~$0.85k (assumed not covered by the plan, **verify**) |
| **Total** | **~$6.9k/month** | **~$3.9k/month (~$46k/year)** |

If the archive does not compress at all (~42 TB), S3 adds about $0.5k/month to both columns.

ScyllaDB today is **$232k/year across devnet, testnet and mainnet (~$19.3k/month). The
mainnet-only figure is not known**, so the saving cannot be stated yet. The new pieces pay for
themselves if mainnet ScyllaDB costs more than ~$3.9k/month (~$46k/year) with the savings plan, or
~$6.9k/month without it. Illustrative only, since mainnet's share is a guess:

| Mainnet share of the $232k | Mainnet ScyllaDB | Saving with the savings plan |
|---|---|---|
| 50% | ~$9.7k/month | ~$5.8k/month (~$70k/year) |
| 75% | ~$14.5k/month | ~$10.7k/month (~$128k/year) |
| 90% | ~$17.4k/month | ~$13.6k/month (~$163k/year) |

Devnet and testnet stay on ScyllaDB unless the same stack also serves them. That is not costed.

The current-state cache nodes replace today's Clio read nodes and the feeders replace the ETL
nodes, so they are not new spend. **Not included:** engineering time (the largest cost), the
overlap period when both systems run, and public download traffic.

### Not covered at first

- **Clio-only NFT methods** (~50 req/s) need a token index added to the same build.
- **Verified state proofs** at old ledgers (the "hold a Merkle proof" product) need a full node
  store, ~32 TB, roughly 10 servers with redundancy. This is optional, not needed for Clio parity.
- `subscribe` comes from xrpld; path finding and `submit` are forwarded, as Clio does today.

### How we prove it is right

- **No silently missing transactions** is the central risk. The metadata decoder is proven not to
  report wrong accounts, but never proven not to miss any. Checks: a transaction touching zero
  accounts is a hard build error; every metadata blob is decoded with an independent
  implementation and diffed in both directions; results are compared against real xrpld nodes
  (r.ripple.com, xrplcluster.com), not Clio alone.
- **Shadow traffic:** replay real Clio requests against the new stack and compare answers and
  latency before any cutover. Target: no data mismatches, latency within Clio's today
  (7-day mean: `account_tx` 21 ms, `tx` 3 ms; the dashboard has no true per-request p95).

### Done when

Shadow traffic matches production Clio on the sampled methods with latency at or below today's,
the build is repeatable from S3 alone, and a replica can be rebuilt without anyone hand-holding it.

## Decisions needed

1. **Stream separation before the full export.** Writing transactions as their own file at export
   time makes every later rebuild read ~6 TB instead of ~40 TB, and shortens the freshness gap.
   Doing it after publishing means re-exporting. This is the one choice that cannot wait.
2. **Hosting and who pays for egress** (S3, a zero-egress host, or BitTorrent-first).
3. **Reuse Clio's server code, or write our own** handlers in Rust.
4. **Range nodes / verified proofs:** build or skip.
5. **Who runs the full export**, and on which stopped full-history node.

## Estimates still to be measured

Compression on older eras (measured on one recent chunk only); total archive size; full-history export and import time; xrpld and
cache node capacity (sets the node counts); transaction and index sizes (sampled on 13 points, so
roughly ±30%); the historical-state store; per-method share of the ~5% of requests that reach
deeper than 1M ledgers; engineering effort for Stage 2; **mainnet-only ScyllaDB cost** (the
$232k/year covers three networks), and whether the 50% savings plan covers S3 and the load balancer.
