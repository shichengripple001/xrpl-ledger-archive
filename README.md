# xrpl-ledger-archive

Canonical chunked archive format for XRPL full ledger history.

See [PLAN.md](PLAN.md) for the full design.
See [spec/chunk-format.md](spec/chunk-format.md) for the binary format.

## PoC

```bash
cd poc
pip install websockets
python3 exporter.py --url ws://localhost:6006 --start 1000000 --end 1001000
python3 exporter.py --url ws://localhost:6006 --start 1000000 --end 1001000 --verify
```
