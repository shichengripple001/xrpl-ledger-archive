#!/usr/bin/env python3
"""
XRPL Ledger Archive - PoC Exporter

Connects to a rippled node via WebSocket, exports a small ledger range
as a chunk file, and verifies determinism.

Usage:
    python3 exporter.py --url ws://localhost:6006 --start 1000000 --end 1001000
    python3 exporter.py --url ws://localhost:6006 --start 1000000 --end 1001000 --verify

This PoC works at the ledger-object level (not raw SHAMap nodes) to prove
the design. The real C++ exporter uses visitDifferences() for SHAMap node access.
"""

import argparse
import asyncio
import hashlib
import json
import struct
import sys
from pathlib import Path

import websockets

MAGIC_HEADER = b"XRLA"
MAGIC_FOOTER = b"ENDX"
FORMAT_VERSION = 1
MAINNET_ID = 1


# ---------------------------------------------------------------------------
# WebSocket RPC helpers
# ---------------------------------------------------------------------------

async def rpc(ws, method, params=None):
    req = {"command": method, "id": 1}
    if params:
        req.update(params)
    await ws.send(json.dumps(req))
    resp = json.loads(await ws.recv())
    if resp.get("status") != "success":
        raise RuntimeError(f"RPC {method} failed: {resp}")
    return resp["result"]


async def get_ledger_state(ws, ledger_seq):
    """Fetch full ledger state as {object_id: hex_blob} dict via ledger_data pagination."""
    state = {}
    marker = None
    page = 0
    while True:
        params = {
            "ledger_index": ledger_seq,
            "binary": True,
            "limit": 2048,
        }
        if marker:
            params["marker"] = marker
        result = await rpc(ws, "ledger_data", params)
        for obj in result.get("state", []):
            state[obj["index"]] = obj["data"]
        marker = result.get("marker")
        page += 1
        if page % 10 == 0:
            print(f"  fetched {len(state)} objects (page {page})...", end="\r")
        if not marker:
            break
    print(f"  fetched {len(state)} objects total          ")
    return state


async def get_ledger_header(ws, ledger_seq):
    """Fetch ledger header, return ledger_hash and close_time."""
    result = await rpc(ws, "ledger", {
        "ledger_index": ledger_seq,
        "transactions": True,
        "binary": True,
    })
    ledger = result["ledger"]
    return {
        "hash": ledger["ledger_hash"],
        "close_time": ledger.get("close_time", 0),
        "txns": ledger.get("transactions", []),
    }


async def get_transactions(ws, ledger_seq):
    """Fetch all transactions for a ledger as list of {hash, tx_blob, meta_blob}."""
    result = await rpc(ws, "ledger", {
        "ledger_index": ledger_seq,
        "transactions": True,
        "expand": True,
        "binary": True,
    })
    txns = []
    for tx in result["ledger"].get("transactions", []):
        txns.append({
            "hash": tx["hash"],
            "tx_blob": tx["tx_blob"],
            "meta_blob": tx.get("metaData", tx.get("meta", "")),
        })
    return sorted(txns, key=lambda t: t["hash"])


# ---------------------------------------------------------------------------
# Delta computation
# ---------------------------------------------------------------------------

def compute_delta(state_a, state_b):
    """Compute delta between two state dicts. Returns (added, deleted)."""
    keys_a = set(state_a.keys())
    keys_b = set(state_b.keys())

    added = {}
    deleted = []

    for k in keys_b - keys_a:
        added[k] = state_b[k]

    for k in keys_a & keys_b:
        if state_a[k] != state_b[k]:
            added[k] = state_b[k]

    for k in keys_a - keys_b:
        deleted.append(k)

    return added, sorted(deleted)


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------

def encode_node(obj_id_hex, blob_hex):
    obj_id = bytes.fromhex(obj_id_hex)
    content = bytes.fromhex(blob_hex)
    node_hash = hashlib.sha512(content).digest()[:32]
    return node_hash, struct.pack(">32sH", node_hash, len(content)) + content


def encode_checkpoint(state):
    """Encode full state as checkpoint section. Returns bytes."""
    nodes = []
    for obj_id, blob in state.items():
        node_hash, encoded = encode_node(obj_id, blob)
        nodes.append((node_hash, encoded))
    nodes.sort(key=lambda x: x[0])

    out = struct.pack(">I", len(nodes))
    for _, enc in nodes:
        out += enc
    return out


def encode_delta(ledger_seq, added, deleted):
    """Encode one delta entry. Returns bytes."""
    added_nodes = []
    for obj_id, blob in added.items():
        node_hash, encoded = encode_node(obj_id, blob)
        added_nodes.append((node_hash, encoded))
    added_nodes.sort(key=lambda x: x[0])

    deleted_hashes = sorted(
        hashlib.sha512(bytes.fromhex(b)).digest()[:32] for b in
        [added.get(d, "") for d in deleted]
        if b
    )
    # Simpler: use object ID as hash for deleted in PoC
    deleted_hashes = sorted(bytes.fromhex(d) for d in deleted)

    out = struct.pack(">III", ledger_seq, len(added_nodes), 0)
    for _, enc in added_nodes:
        out += enc
    out = out[:8] + struct.pack(">I", len(deleted_hashes)) + out[8:]

    # Rebuild properly
    out = struct.pack(">I", ledger_seq)
    out += struct.pack(">I", len(added_nodes))
    for _, enc in added_nodes:
        out += enc
    out += struct.pack(">I", len(deleted_hashes))
    for h in deleted_hashes:
        out += h
    return out


def encode_tx_map(ledger_seq, txns):
    """Encode transaction map for one ledger. Returns bytes."""
    out = struct.pack(">IH", ledger_seq, len(txns))
    for tx in txns:
        tx_hash = bytes.fromhex(tx["hash"])
        tx_blob = bytes.fromhex(tx["tx_blob"])
        meta_blob = bytes.fromhex(tx["meta_blob"]) if tx["meta_blob"] else b""
        out += tx_hash
        out += struct.pack(">I", len(tx_blob)) + tx_blob
        out += struct.pack(">I", len(meta_blob)) + meta_blob
    return out


def sha512half(data):
    return hashlib.sha512(data).digest()[:32]


def write_chunk(path, network_id, start_ledger, end_ledger,
                checkpoint_hash_hex, checkpoint_bytes,
                delta_bytes_list, tx_map_bytes_list):
    body = checkpoint_bytes
    for d in delta_bytes_list:
        body += d
    for t in tx_map_bytes_list:
        body += t
    body += MAGIC_FOOTER

    chunk_hash = sha512half(body)
    checkpoint_hash = bytes.fromhex(checkpoint_hash_hex)

    header = MAGIC_HEADER
    header += struct.pack(">B", FORMAT_VERSION)
    header += struct.pack(">I", network_id)
    header += struct.pack(">I", start_ledger)
    header += struct.pack(">I", end_ledger)
    header += checkpoint_hash
    header += chunk_hash

    with open(path, "wb") as f:
        f.write(header + body)

    return chunk_hash.hex()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def export(url, start_ledger, end_ledger, output_path, network_id=MAINNET_ID):
    print(f"Connecting to {url}")
    async with websockets.connect(url) as ws:
        print(f"Fetching checkpoint state at ledger {start_ledger}...")
        checkpoint_state = await get_ledger_state(ws, start_ledger)
        checkpoint_header = await get_ledger_header(ws, start_ledger)
        checkpoint_bytes = encode_checkpoint(checkpoint_state)
        print(f"Checkpoint: {len(checkpoint_state)} objects, {len(checkpoint_bytes):,} bytes")

        delta_bytes_list = []
        tx_map_bytes_list = []

        tx_map_bytes_list.append(
            encode_tx_map(start_ledger, await get_transactions(ws, start_ledger))
        )

        prev_state = checkpoint_state
        total_delta_bytes = 0

        for seq in range(start_ledger + 1, end_ledger + 1):
            print(f"Processing ledger {seq} ({seq - start_ledger}/{end_ledger - start_ledger})...")
            curr_state = await get_ledger_state(ws, seq)
            added, deleted = compute_delta(prev_state, curr_state)
            delta_bytes = encode_delta(seq, added, deleted)
            delta_bytes_list.append(delta_bytes)
            total_delta_bytes += len(delta_bytes)

            txns = await get_transactions(ws, seq)
            tx_map_bytes_list.append(encode_tx_map(seq, txns))

            print(f"  delta: +{len(added)} modified, -{len(deleted)} deleted, {len(delta_bytes):,} bytes")
            prev_state = curr_state

        avg_delta = total_delta_bytes // (end_ledger - start_ledger) if end_ledger > start_ledger else 0
        print(f"\nDelta stats: total={total_delta_bytes:,} bytes, avg={avg_delta:,} bytes/ledger")

        chunk_hash = write_chunk(
            output_path, network_id,
            start_ledger, end_ledger,
            checkpoint_header["hash"],
            checkpoint_bytes,
            delta_bytes_list,
            tx_map_bytes_list,
        )

        size = Path(output_path).stat().st_size
        print(f"\nWrote {output_path} ({size:,} bytes)")
        print(f"chunk_hash: {chunk_hash}")
        return chunk_hash


def verify_determinism(url, start_ledger, end_ledger):
    """Run export twice, compare output hashes."""
    import asyncio

    out1 = f"/tmp/chunk_{start_ledger}_{end_ledger}_run1.xrla"
    out2 = f"/tmp/chunk_{start_ledger}_{end_ledger}_run2.xrla"

    print("=== Run 1 ===")
    h1 = asyncio.run(export(url, start_ledger, end_ledger, out1))
    print("\n=== Run 2 ===")
    h2 = asyncio.run(export(url, start_ledger, end_ledger, out2))

    print(f"\nRun 1 hash: {h1}")
    print(f"Run 2 hash: {h2}")
    if h1 == h2:
        print("DETERMINISM: PASS - chunk files are identical")
    else:
        print("DETERMINISM: FAIL - chunk files differ")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="XRPL Ledger Archive PoC Exporter")
    parser.add_argument("--url", default="ws://localhost:6006", help="rippled WebSocket URL")
    parser.add_argument("--start", type=int, required=True, help="Start ledger sequence")
    parser.add_argument("--end", type=int, required=True, help="End ledger sequence")
    parser.add_argument("--out", default=None, help="Output file path")
    parser.add_argument("--verify", action="store_true", help="Run twice and verify determinism")
    parser.add_argument("--network-id", type=int, default=1, help="Network ID (1=mainnet)")
    args = parser.parse_args()

    if args.end <= args.start:
        print("--end must be greater than --start")
        sys.exit(1)

    if args.verify:
        verify_determinism(args.url, args.start, args.end)
        return

    out = args.out or f"chunk_{args.start}_{args.end}.xrla"
    asyncio.run(export(args.url, args.start, args.end, out, args.network_id))


if __name__ == "__main__":
    main()
