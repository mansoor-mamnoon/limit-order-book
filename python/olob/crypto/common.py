# python/olob/crypto/common.py
from __future__ import annotations
import os, json, gzip
from pathlib import Path
from typing import Iterable, Dict, Any, List
from datetime import datetime, timezone
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ---------- path helpers ----------
def dpath(root: str, date_str: str, *parts: str) -> Path:
    p = Path(root) / date_str
    for pt in parts:
        p = p / pt
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def new_jsonl_path(base_dir: Path, prefix: str) -> Path:
    ts = datetime.now(timezone.utc).strftime("%H%M%S")
    return base_dir / f"{prefix}-{ts}.jsonl.gz"

# ---------- gzip jsonl writer ----------
class JsonlGz:
    def __init__(self, path: Path):
        self.path = path
        self.f = gzip.open(self.path, "wt", encoding="utf-8", compresslevel=6)
    def write(self, obj: Dict[str, Any]) -> None:
        self.f.write(json.dumps(obj, separators=(",", ":")))
        self.f.write("\n")
    def close(self) -> None:
        try:
            self.f.flush()
        finally:
            self.f.close()

# ---------- normalization ----------
# unified schema:
#   ts   : int64 (ns UTC)
#   side : string
#   price: float64
#   qty  : float64
#   type : 'book' or 'trade'
_ARROW_SCHEMA = pa.schema([
    pa.field("ts", pa.int64(), nullable=False),
    pa.field("side", pa.string(), nullable=False),
    pa.field("price", pa.float64(), nullable=False),
    pa.field("qty", pa.float64(), nullable=False),
    pa.field("type", pa.string(), nullable=False),
])

def _iter_jsonl_gz(paths: Iterable[Path]) -> Iterable[Dict[str, Any]]:
    for p in paths:
        with gzip.open(p, "rt", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)

def _book_rows_from_binance_event(ev: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    data = ev.get("data", ev)
    if data.get("e") != "depthUpdate":
        return
    ts_ns = int(data["E"]) * 1_000_000  # ms -> ns
    for px, qty, *_ in data.get("b", []):
        yield {"ts": ts_ns, "side": "B", "price": float(px), "qty": float(qty), "type": "book"}
    for px, qty, *_ in data.get("a", []):
        yield {"ts": ts_ns, "side": "A", "price": float(px), "qty": float(qty), "type": "book"}

def _trade_rows_from_binance_event(ev: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    data = ev.get("data", ev)
    if data.get("e") != "trade":
        return
    ts_ns = int(data["T"]) * 1_000_000  # ms -> ns
    price = float(data["p"]); qty = float(data["q"])
    side = "S" if bool(data.get("m")) else "B"  # buyer is maker -> taker sold
    yield {"ts": ts_ns, "side": side, "price": price, "qty": qty, "type": "trade"}

def normalize_day(
    date_str: str,
    exchange: str,
    symbol: str,
    raw_root: str = "raw",
    out_root: str = "parquet",
) -> None:
    date_dir = Path(raw_root) / date_str / exchange / symbol.upper()
    if not date_dir.exists():
        raise FileNotFoundError(f"No raw dir: {date_dir}")

    depth_dir = date_dir / "depth"
    trades_dir = date_dir / "trades"
    depth_files = sorted(depth_dir.glob("diffs-*.jsonl.gz"))
    trade_files = sorted(trades_dir.glob("trades-*.jsonl.gz"))

    rows: List[Dict[str, Any]] = []
    for ev in _iter_jsonl_gz(depth_files):
        rows.extend(_book_rows_from_binance_event(ev) or [])
    for ev in _iter_jsonl_gz(trade_files):
        rows.extend(_trade_rows_from_binance_event(ev) or [])

    if not rows:
        raise RuntimeError("No rows parsed for normalization")

    df = pd.DataFrame(rows, columns=["ts", "side", "price", "qty", "type"])
    df.sort_values(by=["ts"], kind="stable", inplace=True)

    out_dir = Path(out_root) / date_str / exchange / symbol.upper()
    ensure_dir(out_dir)
    out_path = out_dir / "events.parquet"

    table = pa.Table.from_pandas(df, schema=_ARROW_SCHEMA, preserve_index=False)
    pq.write_table(table, out_path)
    print(f"Wrote Parquet: {out_path}")
