# CONTEXT — who this is for, and what it is actually competing with

Written 2026-09-28. Companion to `PLAN.md` (design and rationale),
`STATUS.md` (proven vs. reasoned), `E2E_TEST_PLAN.md` (the current test).

This file records reasoning that lives nowhere else in the repo: the driving use case, a
**correction** to claims made about Clio elsewhere in these docs, and the analysis of what this
project should and should not become. It is here so the next person doesn't re-derive it — or,
worse, repeat the incorrect version.

---

## The anchor use case

A market-maker/trading firm could not get what they needed from **Clio's full-history mode** and
instead asked for a full-history **xrpld** node directly.

That is the whole motivation in one sentence. Everything below is the analysis of why that
happened and what it implies for this project.

Note what the request was *not*: they did not ask for a faster API, or a cheaper Clio. They went
around Clio to the raw source. The thing they wanted was closer to the ledger than Clio sits.

---

## Correcting the record on Clio

An earlier draft of this argument (in the since-deleted `DESIGN_NOTES.md`) overstated the case.
Two of its bullets were **wrong**, and shipping them would have discredited the rest:

| Claim in that earlier draft | Reality |
|---|---|
| "Cannot reconstruct full ledger state at an arbitrary historical ledger" | **False.** Clio stores per-key value history plus a successor table that mimics SHAMap key ordering, specifically so it *can* answer "what did this object look like at ledger N" for any ledger in its ingested range. |
| "Clio is not real full history" (as a blanket statement) | **Imprecise.** Clio's coverage is whatever its ETL ingested. That can genuinely be full history. |
| "Cannot cryptographically prove the state of any object at any point in time" | **True, and this is the real differentiator.** |

Other corrections established by reading source/docs rather than assuming:

- **Clio does not run `online_delete`.** That is a xrpld concept. Clio is not xrpld and does
  not prune on that mechanism. Do not repeat this claim.
- **Clio is an index of what a xrpld node streamed into it** — not an independent source of
  history. It cannot self-heal a gap; it can only reflect what it was fed.
- **`account_tx`, `account_nfts`, `account_lines` are standard xrpld RPCs**, not Clio
  inventions. Clio re-implements them against its own store to offload xrpld.

### What Clio actually can't do

State these, not the overreaching versions:

1. **No cryptographic verifiability.** Clio returns rows from Cassandra. There is no way to prove
   a returned value belonged to a specific ledger's state without trusting the DB and the pipeline
   that wrote it. This archive verifies against on-chain `AccountHash`/`LedgerHash`.
2. **Coverage is inherited, not guaranteed.** If the upstream xrpld never had the history (most
   run `online_delete` with a rolling window), Clio never received it. Nothing in Clio fixes that
   — only a full-history source can, which is exactly why the market maker asked for one.
3. **No bulk export / "tape" mode.** `book_offers`, `account_tx` etc. are point-query RPCs. Getting
   a full historical order-book time series means N sequential RPC round-trips. There is no
   mechanism to hand someone the underlying data in bulk. This archive is bulk-first by nature.

### Cost: no ScyllaDB/Cassandra tier

Clio's dominant cost is its **ScyllaDB/Cassandra cluster** — a distributed database provisioned for
write throughput and replication: multiple always-on, RAM-heavy, SSD-provisioned nodes plus the
operational burden of running, repairing, and rebalancing a cluster. The Clio process itself is
cheap; the database tier is not.

This archive has **no database tier**. The data is immutable, content-addressed chunks, so it lives
on cheap object storage (S3/R2) or plain disk, and a query server just reads chunk slices through a
lightweight local index (SQLite/RocksDB). Consequences:

- **Storage** — object storage per-TB is far cheaper than a Scylla cluster's provisioned SSD+RAM,
  and the data is written roughly once (immutable, deduped).
- **No cluster, no replication ops** — static files are trivially replicated and CDN-cacheable;
  read capacity scales horizontally for free.
- **Elastic** — spin query nodes up/down against the same shared chunk store; no rebalancing,
  no repair.

Honest tradeoff: Scylla buys very low-latency random point lookups. We trade some of that for an
index + chunk-range fetch (warm cache / CDN narrows the gap; a latency-critical hot API would add
its own caching layer). For full historical-state queries Clio cannot serve at all, there is no
comparison.

---

## The retention gap, made concrete

The abstract claim "thin nodes can't answer old queries" has a local, concrete instance: the
`xrpl-sensor-node` on this machine runs `online_delete=256` / `[ledger_history] 256`. It retains
**256 ledgers — about 17 minutes of history.** Ask it about anything older and it has no data, not
slow data.

That is not a misconfiguration; it's normal. Full history is ~33.5 TB (measured: `nudb.dat`
29.5 TB + `nudb.key` 4.0 TB on `livenet-fh-usw2-01`), so nearly every operator prunes. The
population of nodes that can answer historical queries at all is tiny, and that scarcity is the
market this project addresses.

**"Full history" is two separable things**: (a) *retaining* what you have — a pure config choice
(`online_delete` off, `[ledger_history] full`), costing nothing until data accumulates; and
(b) *possessing* all ~105M ledgers — the 33.5 TB / months-of-backfill problem. Don't conflate
them. This project attacks (b); (a) is just configuration.

---

## Two query shapes (do not conflate)

Derived datasets split cleanly by mechanism, not by which RPC name they share:

**1. State-snapshot queries** — `AccountRoot` balance, `account_lines`, `account_nfts`, DEX order
books. "What did X hold *at ledger N*." Answered by: reconstruct state at N (checkpoint + replay
deltas), then walk/decode. No index required — this is how xrpld itself answers them. **No new
export or storage needed**; everything is already in the chunks.

**2. History-index queries** — `account_tx`. "What did this account *do*, and when." No state
reconstruction at all. Needs a decoder for `meta.AffectedNodes` plus a **persistent index**
(`account → [(ledger_seq, tx_hash)]`), because a live scan of the whole archive per query is far
slower than an indexed lookup. This is exactly how xrpld (local SQLite) and Clio (Cassandra)
answer it — neither re-scans.

Cost of that index at full-history scale: ~100 GB–1.5 TB (corrected estimate — an earlier "tens of
GB" figure used a guessed ~4 tx/ledger against the real measured ~90 tx/ledger, and was wrong by
~22x). Small relative to the archive; storage was never the constraint.

**The index is optional.** For the anchor use case — one firm reconciling *their own* account — a
plain scan over the existing `tx_maps` answers it at zero extra storage. A persistent all-accounts
index only earns its cost when serving many unknown accounts, repeatedly, at low latency; i.e.
when running a live multi-tenant service.

---

## The design principle: preserve raw, derive later

The archive stores raw, content-addressed SHAMap nodes and verbatim `tx_blob`/`meta_blob`. It
deliberately does **not** decode into an opinionated schema.

That is the structural difference from Clio, which decides at ingestion time which shapes to store
— so anything outside those shapes is a dead end. Deferring the decision means the menu of derived
datasets is open-ended: order books, balance histories, trustline snapshots, NFT ownership
timelines, whatever someone needs later, without us having anticipated it.

Corollary: **derived artifacts stay out of the `.xrla` format.** An `account_tx` index is a
separate, rebuildable sidecar — a deterministic function of immutable chunk data. Losing it costs
nothing; the chunks are the source of truth. Keep the chunk format neutral.

---

## Version coupling — what actually breaks, and how often

Three different exposure levels. Do not treat them as one.

| Layer | Exposure | Frequency |
|---|---|---|
| **Core archive** (export/import/verify) | **None.** Treats ledger content as opaque, content-addressed bytes. New object types (AMM, MPT, lending protocol, …) are just new leaf blobs. | Never |
| **Decoders** (`meta_decode.rs` etc.) | Only a genuinely new *binary wire type* (`STI_*`), not new fields on existing types. Fails **soft** — skips one transaction, logs, continues. | ~Once a year or two |
| **A query API** promising xrpld-compatible responses | **Full Clio treadmill.** Must track every amendment's field/object shape to keep responses correct. | Every release |

Real data point, not theory: a live transaction in a test chunk used `STI_ISSUE` (type 24, added
for AMM) that the decoder didn't handle. It failed soft; the fix took about two minutes.

**The one genuine coupling risk is different from all of the above**: we read NuDB `.dat`/`.key`
and xrpld's node wire encoding **by reverse engineering** (`crates/xrla-nudb/NUDB_FORMAT.md`),
with no stability guarantee and no deprecation warning. That format has been stable since NuDB
became xrpld's standard backend (~2017–2018), through many releases — so it's a low-probability
tail risk, not a maintenance burden. **Watch xrpld release notes for a NodeStore/NuDB version
bump specifically**; ignore amendment noise.

---

## Recommendations (what to build, what to refuse)

1. **Don't build a Clio-style always-current API service** unless there's concrete demand. It is
   the single thing that buys the per-release maintenance burden, and the core value doesn't need
   it.
2. **Keep decoder work reactive.** Don't pre-support wire types that don't exist. Soft-fail and
   patch when hit.
3. **Prefer these query-layer shapes**, in rough order of maintenance cost:
   - a **local tool** over locally-held chunks (no uptime, no API contract);
   - **ship raw verified bytes** and let consumers decode with `ripple-binary-codec` / `xrpl-py` /
     `xrpl4j` — libraries the ecosystem *already* keeps current, so the treadmill isn't ours;
   - a **local index** (batch-built sidecar, still no service);
   - a **narrow purpose-built API** that does *not* promise xrpld JSON parity.
4. **Forwarding is the right answer for recent ledgers.** If a ledger is inside a live node's
   retention window, forward to it — it's correct, current, and free of maintenance. This archive's
   value starts precisely where retention ends. The two are complementary, not competing.
5. **Don't compete on decoding new object types as fast as Clio.** That's their game. The
   differentiator is durable, verifiable preservation.

---

## The load-bearing question — answered 2026-09-28

Everything above is about *what to build on top*. The load-bearing claim underneath it —
**that a real xrpld can open and serve our output** — was untested for most of this project's
life. It is now proven: a real xrpld process opens, boots from, and correctly serves a NuDB
store plus `ledger.db` written by `xrla-import`, verified on a 7-node private PoC network where
the target ledger range had first been deleted from *every* peer, so no peer could have supplied
it. See `STATUS.md` for the full method and `E2E_TEST_PLAN.md` for what was actually run.

Three real bugs had to be fixed to get there, none of which our own reader could have caught —
reading our writer's output back through our own reader proves internal consistency, not
compatibility:

1. **NuDB `pepper`** was computed as `xxh64(&[], salt)`; the correct value is
   `xxh64(&salt.to_le_bytes(), salt)`. Real xrpld rejects a mismatch with `hash_mismatch` and
   will not open the store at all.
2. **`ledger.db` writer** — it did not exist (xrpld needs it to learn which ledgers it holds),
   and its first version *deleted* an existing file; it now merges via `INSERT OR IGNORE`.
3. An **OOM** from cloning a 27M-node state map, fixed by sharing via `Rc`.

Still genuinely unproven: anything at full-history scale. See `STATUS.md`'s
"Known-unmeasured, explicitly" section — a real end-to-end full-history export has never run.
