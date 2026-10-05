# XRPL Ledger Archive — Proposal

Draft, 2026-10-01. Informal: written to get agreement on direction, not to be the spec.
Design detail lives in [PLAN.md](PLAN.md), evidence in [STATUS.md](STATUS.md), positioning in
[CONTEXT.md](CONTEXT.md).

Numbers are tagged **measured** (we ran it), **observed** (read from production dashboards or a
real node), or **estimate** (arithmetic, not yet confirmed). Estimates are listed again at the end.

## The problem

Full XRPL history is hard to distribute, hard to keep current, and expensive to run and serve.

### Getting full history

- **It is hard to distribute.** A full-history node holds **32 TB NuDB + 11 TB `transaction.db` +
  296 GB `ledger.db`** (observed, 2026-09-29). Someone who wants to run one has two options: get a
  copy from XRPL Commons and download the entire database, or backfill from peers with xrpld. Either
  takes months.
- **Every new snapshot starts from scratch.** There is no incremental way to publish history. To
  produce a new snapshot you stop the service, compress the whole database, split the compressed
  file, and upload all of it. The process is tedious and takes weeks.
- **Clio's data can't be cheaply checked.** Clio returns rows with no proof attached. You can check
  them against a ledger's hash, but only by refetching the whole ledger (every transaction, or the
  entire state) and rebuilding its tree, one full fetch per ledger. At full-history scale that does
  not work through a public API (not measured).

### Running a full-history node

- **It is expensive.** We run it on an i3en.24xlarge. Anyone running one pays $10.848/hour on
  demand (us-west-2, AWS price list): ~$7.9k/month, ~$95k/year per node. This is the instance only;
  data transfer and backups are not included. One node can serve only very limited traffic, so
  serving real load means paying that per node, many times over.
- **It can't grow forever.** The i3en.24xlarge has 60 TB of local NVMe. The node already uses
  ~43 TB and grows ~12 GB/day (observed). Disk is a hard ceiling on one machine.
- **The data is lost when the instance stops.** The disks are local NVMe (instance store), and
  their contents are lost when the instance is stopped or terminated, or when the underlying host
  fails. Keeping the data safe means keeping a backup, which is not in the cost above. With no
  backup, recovery is a full re-download or backfill of ~43 TB, which takes months.

### Serving it through Clio

- **The database tier is expensive.** Clio's ScyllaDB tier is **~$232k/year across devnet, testnet
  and mainnet**. The mainnet-only figure is not known.
- **The database is hard to operate.** Clio needs a Cassandra-compatible database (ScyllaDB), and
  managing one is difficult for node operators.
- **Its database is as hard to hand over as xrpld's, or harder.** To share it you export the
  database and audit its integrity, and the recipient imports it and audits again. Apart from
  hosting ScyllaDB yourself, there is no practical way to get the data.
- **Rebuilding it means re-ingesting everything.** Clio fills its database by ingesting ledgers one
  at a time from a live rippled. After a data loss, a bug fix or a schema change, the history is
  ingested again from the start.

### What we want the solution to do

We are building a hash-verified archive of XRPL history, cut into chunks. The data can be served
three ways:

1. **Query with the tool.** Look up account history and transactions straight from a downloaded
   chunk, with no node.
2. **Bootstrap an xrpld node.** Import a recent range or the full history (all ranges), and the
   node serves it to the network over P2P.
3. **A query layer built on top.** An API that serves the archive, like Clio but without ScyllaDB.

**1. Download chunks and query them directly** (no node, any subset of chunks)
- Fetch all of history or just a range, from S3 or BitTorrent, and verify every chunk against
  on-chain hashes without trusting where it came from.
- Look up an account's transactions (`account_tx`) and a transaction by hash straight from the
  downloaded chunk, using an index published with it, without running xrpld.
- Prove the index is right: it records which chunk and which version of the account rule built it,
  and can be rebuilt and checked.
- Never silently drop a transaction from an account's history.

**2. Download chunks and spin up an xrpld node**
- Not only for full history. A normal P2P xrpld node that needs a longer history than it holds can
  import just the range it needs from chunks, instead of waiting for peers to supply it. Full
  history is the case where the range is everything.
- Seed a working node in days, not months: download is hours on a fast link, and import is measured
  at 39 minutes per recent 150k chunk, at most ~19 days for the whole archive (estimate, see "Size
  and time").
- The seeded node answers `account_tx` and `tx` on the imported ledgers (`xrla-import --txdb`), once
  its imported range joins the live tip.
- Publishing a new snapshot adds a chunk instead of re-uploading the whole database, and the chunks
  in storage are the backup: a lost node is rebuilt from them, not re-downloaded for months.

**3. A query layer built on top** (planned, not built)
- Serve `account_tx` and `tx` for all history at roughly Clio's latency (observed 7-day mean:
  `account_tx` 21 ms, `tx` 3 ms) at ~165 req/s with headroom.
- Serve current state from memory at Clio's volume, with xrpld only feeding ledgers in and
  forwarding writes.
- Serve the newest ledgers too. A chunk is only built after its 150,000 ledgers (about a week) have
  closed, so the latest days are in no chunk yet; the service has to pick them up from a running
  xrpld until the next chunk is built.
- Route each request to a server that holds the ledger range it needs, so history can be split
  across servers by range and the service scales by adding servers.
- If a server is lost or the index format changes, rebuild from the chunks, many at the same time,
  instead of re-ingesting ledger by ledger.

## Stage 1 — Archive service

### What it is

History cut into **150,000-ledger chunks** (~7 days of mainnet each, ~715 chunks). Each chunk holds
one state checkpoint plus only the tree nodes that changed per ledger, and the full transactions
and metadata. Chunks are deterministic (nodes sorted by hash), so two independent exports of a
range produce the same bytes, and anyone can verify a chunk without trusting where it came from.

### What already works (measured)

**Scope.** Everything below was run on one chunk: 150,000 ledgers (107,147,192 to 107,297,191),
about a week of history and about 0.14% of mainnet's ~107 million ledgers. Nothing has been run on
the full history or on older eras. The whole-archive numbers are estimates, in "Size and time".

**Making and loading the chunk**
- **Export:** exporting those 150,000 ledgers from a full-history node took 47 minutes and up to
  19.2 GB of memory. The chunk file is 207.8 GB, or 93.6 GB compressed. xrpld has to be stopped
  while its database files are read.
- **Import:** loading the chunk into an empty xrpld node took 39 minutes and up to 46.1 GB of
  memory. Every imported ledger is checked: its state and transactions are recomputed and must
  match the ledger's hash, and it must chain to the ledger before it.

**A node seeded from the chunk**
- **It serves correct ledgers.** A node reseeded this way caught up to the network and served
  ledgers that matched r.ripple.com and s2.ripple.com. We sampled 10,000 ledgers (1.24 million
  transactions) from the range 107,145,192 to 107,351,007, which covers the imported chunk and the
  ledgers after it, with zero mismatches.
- **It serves account history.** Importing with `--txdb` took 47 minutes and added a 38 GB
  `transaction.db`. After xrpld fetched the gap between the chunk and the live tip (13.5 hours for
  about 71,000 ledgers), it answered `account_tx` on the imported ledgers.

**Querying the chunk with the tool (no node)**
- **Index:** reading a 207.8 GB chunk for every question would be far too slow, so the tool first
  builds a lookup file from the chunk, once. It reads every transaction and records which accounts
  it touched and where it sits in the chunk (its ledger and its position in that ledger). For this
  chunk that is 17.5 million transactions and 35 million account entries. It took 14 minutes and
  produced a 5.07 GB file. After that, finding an account's transactions, or one transaction by its
  hash, is a lookup in that file and does not touch the chunk.
- **Checked against xrpld:** our rule for which accounts a transaction touched matched xrpld's own
  records on about 3.9 million transactions from several ledger ranges. The tool's `account_tx`
  answers matched xrpld's `account_tx` for 201 accounts, on a 5,000-ledger chunk.
- **Checked against an independent server:** on the 150k chunk, the tool matched the imported
  `transaction.db` for 66 accounts (14.3 million rows). That table matched s2.ripple.com (Clio, full
  history) for 10,000 random accounts (926,903 rows, same transactions in the same order) and for
  the stored bytes of 10,000 random transactions. There were zero differences.

### What is left

| Item | Notes |
|---|---|
| **Export the whole history** | The long pole. At the measured ~53 ledgers/s, 107M ledgers is ~23 days in one process (**estimate**, upper bound: early history is far sparser, and separate ranges can run in parallel, each paying one checkpoint walk). Needs a stopped full-history node as the source. |
| **Publisher** | A tool that writes `manifest.json` (chunk range, `chunk_hash`, size, URL, torrent info-hash) and one torrent per chunk. Not built. |
| **Hosting** | S3 (or equivalent) as the always-on source, plus BitTorrent so downloaders and mirrors share the load, with S3 as a web seed. One torrent per chunk. See "Distribution" for egress. |
| **Decide the chunk layout before publishing** | The layout of a chunk file (`spec/chunk-format.md`) is still a draft. Once chunks are published, changing it means exporting all of history again. The likeliest change is storing transactions in their own file, which is decision 1 below. |
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
| Download of the whole archive (~19 TB) | **estimate**, assuming a full link: 100 Mbps ~17.6 days, 1 Gbps ~42 h, 10 Gbps ~4.2 h, 25 Gbps ~1.7 h. At the ~42 TB upper bound, 2.2× longer |
| Download of one recent chunk (93.6 GB) | 100 Mbps ~2.1 h, 1 Gbps ~12.5 min, 10 Gbps ~1.25 min. The average chunk is ~27 GB (19 TB ÷ 715), so ~3.5× faster |
| Import of one recent chunk | **39 min measured** (47 min with `--txdb`), 46 GB peak RAM |
| Import of the whole archive (715 chunks) | **upper bound ~19 days** (715 × 39 min, one after another; ~23 days with `--txdb`). Older chunks are smaller and should be faster, but no old chunk has been imported, so by how much is not known. Download can overlap with import, so total time is roughly the import time |
| S3 storage | ~19 TB (the 39–42 TB total divided by the measured 2.22×), ~$440/month. Upper bound if older eras do not compress: ~42 TB, ~$970/month. (**verify** pricing) |

Several chunks can be imported in one run (`--chunk a b c`): they share one NuDB store and shared
nodes are written once. They run one after another, not in parallel. Not yet verified on real data:
memory across hundreds of chunks (it grows with unique nodes; 46 GB is for one), how `ledger.db`
looks at chunk boundaries (the importer does not write a chunk's first ledger), and parallel
imports.

### Limits

- **Account history on a seeded node waits for the gap.** `account_tx` on imported ledgers works
  only once the imported range joins the live tip. Until then xrpld returns `lgrIdxsInvalid`, and it
  fetches the gap from peers (13.5 hours for about 71,000 ledgers, measured). Chunks that end near
  the tip shorten the wait.
- **Memory for a many-chunk import is not measured.** Import memory grows with the number of unique
  tree nodes (46 GB for one chunk); the figure for the whole history is unknown.
- **Export needs the source node stopped** while its database files are read.

## Stage 2 — Query layer PoC

### Goal

Serve the same API Clio serves, for all of history, from the archive, without the ScyllaDB tier.
History is sharded by ledger range: each server holds some ranges (built from the chunks), and each
request goes to the server that holds its range. (This is unrelated to xrpld's removed "history
sharding".)

### What traffic actually looks like (observed, Clio dashboard, 7 days)

- **~1,760 requests/s average, ~2,070 peak**, flat for 90 days, on 5 read nodes (~350 req/s each).
- Biggest methods: `account_info` 350, `ping` 296, `ledger` 250, **`account_tx` 138**,
  `server_info` 136, `nft_sell_offers` 130, `amm_info` 105, `book_offers` 101, **`tx` 25**.
- **89% of ledger-scoped requests ask for the newest ledger; ~94% stay within the last ~5 days.**
- Clio keeps the **whole current state in RAM** on each read node (cache hit rate **97.7%**, ~13 GB
  per node). That is why one Clio node handles ~350 req/s. xrpld cannot, so xrpld does not serve
  this traffic.

### Two ways to build it (decision 3)

Answering in Clio's JSON needs libxrpl, the library xrpld and Clio use to turn ledger data into
JSON. Either option uses it; they differ in how much existing code we take on. Neither has been
tried or estimated.

**What Clio looks like** (from the Clio 2.8.0 source, the version production runs):
- Each Clio instance keeps one record of which ledgers it holds: a single range, from its lowest to
  its highest ledger. A request for a ledger newer than its highest is refused ("ledger not
  found"). What happens below its lowest is not checked.
- Every request it answers is either about one ledger, or walks ledgers in order (`account_tx`,
  `nft_history`).
- It reads all data through one storage interface: 41 functions, about 22 of them reads.
- It answers 37 methods itself and forwards 13 to rippled (`submit`, `fee`, `ledger_current`, path
  finding and others).

| | Modify Clio | New service on libxrpl |
|---|---|---|
| What we write | A storage backend for Clio's interface, reading our stores | Request handling for the methods we serve, using libxrpl for JSON |
| What we get for free | Clio's request handling, JSON output, caching, forwarding | Nothing beyond libxrpl |
| Risks | Clio is built around a live database filled by its own ETL; serving a fixed old range with no live feed is not checked (see below). We must keep up with changes to Clio's internal interface. C++ code on top of our Rust stores | Re-implementing 37 methods and their behaviour across amendments, and matching Clio's responses exactly |
| Fit with sharding | Its one-range-per-instance design fits, but it was not built for it | Designed for it from the start |

### Design

| Part | What it does |
|---|---|
| **Shard** | A server (a modified Clio or the new service) serving one ledger range from stores built from that range's chunks. Run with replicas. |
| **Newest shard** | Serves the latest ledgers and current state, fed live from xrpld as Clio is today. When a chunk is sealed, its range moves to a sealed shard. How it stores the not-yet-sealed ledgers is not decided. |
| **Router** | Sends each request to the shard holding its ledger. A range that spans shards (`account_tx`, `nft_history`) is split at the shard boundary: it asks the newest shard first, then older ones until the limit is filled, and the paging marker's ledger number says which shard to continue on. Shards do not overlap, so the order matches a single server. Finds `tx` by hash through one global hash-to-ledger index (a `ctid` already contains the ledger, so needs none). |
| **xrpld** | Feeds new ledgers and answers the forwarded methods. |

### What each shard must hold

All built from that range's chunks. A chunk holds the full state at its first ledger plus every
change after it, so a shard needs no data from other shards.

| Store | Used by | Status |
|---|---|---|
| Ledger headers | every request | in the chunks; needs a lookup table |
| Every object's state at any ledger in the range | `account_info`, `account_lines`, `ledger_entry`, `amm_info` and most other methods | not built; the largest piece |
| Successor index (the next key after a key, at a ledger) | `book_offers`, `ledger_data` | not built |
| Each ledger's transactions and changes | `ledger`, `book_changes` | in the chunks; needs reading by position |
| Account history | `account_tx` | built (`xrla-index`) and verified |
| Transactions by hash | `tx` | built per chunk; the global index across shards is not built |
| NFT and MPT indexes | `nft_info`, `nft_history`, `nfts_by_issuer`, `mpt_holders` | not built |

### Open question that could change the approach

If we modify Clio: whether a Clio instance can serve a fixed old range with no live rippled feed.
Clio has a strict read-only mode, but it refuses to start on an empty database and is built to
follow a writer. Not checked yet. If it cannot, modifying Clio means changing more than its storage.

### Cost

Not yet estimated for the sharded design. The earlier estimate assumed three servers each holding
all history (3 × i4i.8xlarge, ~$6.9k/month on demand); sharding changes the server count and size,
which depend on the store sizes above, none of which are measured. ScyllaDB today is **$232k/year
across devnet, testnet and mainnet**; the mainnet-only share is not known, so the saving cannot be
stated yet. **Not included:** engineering time (the largest cost) and the period when both systems
run.

### Not covered at first

- **Verified state proofs** at old ledgers (a Merkle proof for any object) need a full node store,
  ~32 TB. Optional; not needed to match Clio.
- `subscribe` comes from xrpld; path finding and `submit` are forwarded, as Clio does today.

### How we check it gives the right answers

- **While building:** each store is checked by sending sample requests to it and to real xrpld nodes
  and Clio, and comparing the answers. Missing and extra results are counted separately. This is
  how account history was checked (see "What already works").
- **Before switching users over:** send copies of real Clio requests to the new service as well, and
  compare its answers and speed with Clio's. Users move only when the answers match and it is as fast
  as Clio is today (7-day average: `account_tx` 21 ms, `tx` 3 ms).

### Done when

Shadow traffic matches production Clio on the sampled methods with latency at or below today's,
the build is repeatable from S3 alone, and a shard can be rebuilt from its chunks without anyone
hand-holding it.

## Decisions needed

1. **Stream separation before the full export.** Writing transactions as their own file at export
   time makes every later rebuild read ~6 TB instead of ~40 TB, and shortens the freshness gap.
   Doing it after publishing means re-exporting. This is the one choice that cannot wait.
2. **Hosting and who pays for egress** (S3, a zero-egress host, or BitTorrent-first).
3. **Modify Clio, or implement a new service on libxrpl**, to serve history sharded by ledger
   range. See "Two ways to build it".
4. **Verified state proofs:** build or skip.
5. **Who runs the full export**, and on which stopped full-history node.

## Estimates still to be measured

Compression on older eras (measured on one recent chunk only); total archive size; full-history export and import time; xrpld and
cache node capacity (sets the node counts); transaction and index sizes (sampled on 13 points, so
roughly ±30%); size and build time of each shard's stores (state at any ledger, successor, NFT and MPT
indexes); whether Clio can serve a fixed old range with no live feed; per-method share of the ~5% of requests that reach
deeper than 1M ledgers; engineering effort for Stage 2; **mainnet-only ScyllaDB cost** (the
$232k/year covers three networks), and whether the 50% savings plan covers S3 and the load balancer.
