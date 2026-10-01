#!/usr/bin/env python3
"""Compare our reseeded node against public full-history endpoints, field by field.

Runs ON the xrpld host. Queries 127.0.0.1 (our reseeded node, no rate limit) and a public
ground-truth endpoint (rotated across s2.ripple.com / xrplcluster.com), then diffs
everything that should be identical.

Rate limiting is the binding constraint on the public side, not our node, so:
  - requests to public endpoints are paced by --delay (default 0.6s)
  - 503/429 triggers exponential backoff AND a switch to the other endpoint
  - a ledger is only counted as a real mismatch after every endpoint has been tried

Usage: compare_ledgers2.py <start> <end> <count> [delay_seconds]
"""
import json
import random
import ssl
import sys
import time
import urllib.error
import urllib.request

START, END, COUNT = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
DELAY = float(sys.argv[4]) if len(sys.argv) > 4 else 0.6
SEED = int(sys.argv[5]) if len(sys.argv) > 5 else 1337

OURS = "http://127.0.0.1:51234"
# r.ripple.com = real xrpld 3.4.1 (covers 107276316+); s2 = Clio 2.8.0 (full history).
# xrplcluster.com is real xrpld too but does NOT populate the derived delivered_amount
# field in this RPC shape, so it is excluded from strict comparison (verified 2026-09-29:
# its canonical metadata and tx tree roots match ours exactly).
PUBLIC = ["https://r.ripple.com:51234", "https://s2.ripple.com:51234"]

CTX = ssl.create_default_context()
stats = {"requests": 0, "rate_limited": 0, "backoff_seconds": 0.0,
         "lgr_not_found": 0, "by_endpoint": {u: 0 for u in PUBLIC}}


def rpc(url, ledger_index, timeout=60):
    body = json.dumps({
        "method": "ledger",
        "params": [{"ledger_index": ledger_index, "transactions": True, "expand": True}],
    }).encode()
    req = urllib.request.Request(url, data=body, headers={
        "Content-Type": "application/json",
        "User-Agent": "xrla-verify/1.0",
    })
    stats["requests"] += 1
    with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
        return json.load(r)


def rpc_public(ledger_index, endpoint_idx):
    """Try each public endpoint, backing off and rotating on rate limits."""
    order = [PUBLIC[(endpoint_idx + i) % len(PUBLIC)] for i in range(len(PUBLIC))]
    backoff = 2.0
    last_err = None
    for attempt in range(10):
        url = order[attempt % len(order)]
        try:
            time.sleep(DELAY)
            resp = rpc(url, ledger_index)
            # Rate limiting arrives as HTTP 200 with a JSON-level error, not a 429/503.
            jerr = resp.get("result", {}).get("error")
            # r.ripple.com is a load-balanced pool of xrpld nodes with differing rolling
            # retention floors, so lgrNotFound can mean "this backend lacks it", not "it
            # does not exist". Rotate to another endpoint rather than calling it an error.
            if jerr == "lgrNotFound":
                stats["lgr_not_found"] += 1
                time.sleep(0.3)
                continue
            if jerr in ("tooBusy", "slowDown", "noNetwork", "noCurrent"):
                stats["rate_limited"] += 1
                stats["backoff_seconds"] += backoff
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            stats["by_endpoint"][url] += 1
            return resp, None
        except urllib.error.HTTPError as e:
            last_err = f"HTTP {e.code} from {url}"
            if e.code in (429, 503, 502, 504):
                stats["rate_limited"] += 1
                stats["backoff_seconds"] += backoff
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            return None, last_err
        except Exception as e:  # noqa: BLE001 - network flake, keep trying
            last_err = f"{type(e).__name__}: {e} from {url}"
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
    return None, last_err


def summarize(resp):
    res = resp.get("result", {})
    lg = res.get("ledger")
    if lg is None:
        return {"error": res.get("error", "no ledger in response")}
    txs = lg.get("transactions", []) or []
    tx_detail = {}
    for t in txs:
        if isinstance(t, str):
            tx_detail[t] = None
            continue
        h = t.get("hash") or t.get("tx_json", {}).get("hash")
        tj = t.get("tx_json", t)
        meta = t.get("meta") or t.get("metaData") or {}
        tx_detail[h] = {
            "Fee": tj.get("Fee"),
            "Account": tj.get("Account"),
            "TransactionType": tj.get("TransactionType"),
            "Sequence": tj.get("Sequence"),
            "Destination": tj.get("Destination"),
            "Amount": json.dumps(tj.get("Amount"), sort_keys=True),
            "Flags": tj.get("Flags"),
            "SigningPubKey": tj.get("SigningPubKey"),
            "TransactionResult": (meta or {}).get("TransactionResult"),
            "delivered": json.dumps((meta or {}).get("delivered_amount"), sort_keys=True),
            "affected": len((meta or {}).get("AffectedNodes", []) or []),
        }
    return {
        "ledger_hash": lg.get("ledger_hash"),
        "account_hash": lg.get("account_hash"),
        "parent_hash": lg.get("parent_hash"),
        "transaction_hash": lg.get("transaction_hash"),
        "total_coins": lg.get("total_coins"),
        "close_time": lg.get("close_time"),
        "parent_close_time": lg.get("parent_close_time"),
        "close_time_resolution": lg.get("close_time_resolution"),
        "close_flags": lg.get("close_flags"),
        "closed": lg.get("closed"),
        "tx_count": len(txs),
        "tx_detail": tx_detail,
    }


HEADER_FIELDS = [
    "ledger_hash", "account_hash", "parent_hash", "transaction_hash", "total_coins",
    "close_time", "parent_close_time", "close_time_resolution", "close_flags", "closed",
    "tx_count",
]

random.seed(SEED)
ledgers = sorted(random.sample(range(START, END + 1), COUNT))

ok = 0
unreachable = 0
total_tx = 0
total_fields = 0
mismatches = []
derived_diffs = []
t0 = time.time()

for i, seq in enumerate(ledgers):
    try:
        a = summarize(rpc(OURS, seq))
    except Exception as e:  # noqa: BLE001
        print(f"{seq} OURS_ERROR {e}", flush=True)
        mismatches.append(f"{seq}: our node failed: {e}")
        continue

    resp_b, err = rpc_public(seq, i)
    if resp_b is None:
        unreachable += 1
        print(f"{seq} SKIP (public endpoints unavailable: {err})", flush=True)
        continue
    b = summarize(resp_b)

    if "error" in a or "error" in b:
        mismatches.append(f"{seq}: ours={a.get('error')} public={b.get('error')}")
        print(f"{seq} ERROR ours={a.get('error')} public={b.get('error')}", flush=True)
        continue

    diffs = []
    for f in HEADER_FIELDS:
        total_fields += 1
        if a[f] != b[f]:
            diffs.append(f"{f}: ours={a[f]} public={b[f]}")

    ours_h, pub_h = set(a["tx_detail"]), set(b["tx_detail"])
    if ours_h != pub_h:
        diffs.append(f"tx set differs: only-ours={len(ours_h - pub_h)} only-public={len(pub_h - ours_h)}")
    for h in sorted(ours_h & pub_h):
        da, db = a["tx_detail"][h], b["tx_detail"][h]
        if da is None or db is None:
            continue
        total_tx += 1
        for k in da:
            total_fields += 1
            if da[k] != db[k]:
                if k == "delivered":  # serve-time derived, not canonical ledger data
                    derived_diffs.append(f"{seq} tx {h[:10]}: ours={da[k]} public={db[k]}")
                else:
                    diffs.append(f"tx {h[:10]} {k}: ours={da[k]} public={db[k]}")

    if diffs:
        mismatches.append(f"{seq}: " + "; ".join(diffs[:4]))
        print(f"{seq} MISMATCH({len(diffs)}) {diffs[0][:120]}", flush=True)
    else:
        ok += 1
        if ok % 10 == 0:
            print(f"  ...{ok} ledgers matched so far (last {seq}, {a['tx_count']} txs)", flush=True)

elapsed = time.time() - t0
print()
print("================ RESULT ================")
print(f"ledgers requested      : {len(ledgers)}")
print(f"ledgers fully matched  : {ok}")
print(f"ledgers w/ mismatch    : {len(mismatches)}")
print(f"ledgers skipped (rate) : {unreachable}")
print(f"transactions compared  : {total_tx}")
print(f"field comparisons      : {total_fields}")
print(f"derived-field diffs    : {len(derived_diffs)}  (delivered_amount; not canonical data)")
print()
print(f"http requests          : {stats['requests']}")
print(f"rate-limit responses   : {stats['rate_limited']}")
print(f"lgrNotFound (rotated)  : {stats['lgr_not_found']}")
print(f"time in backoff        : {stats['backoff_seconds']:.0f}s")
print(f"served per endpoint    : {stats['by_endpoint']}")
print(f"wall clock             : {elapsed:.0f}s  ({elapsed / max(len(ledgers),1):.2f}s/ledger)")
for m in mismatches[:20]:
    print("  FAIL", m)

