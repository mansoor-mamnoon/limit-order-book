#!/usr/bin/env python3
"""
Day 20 charts: compare A (single pass) vs B (snapshot+resume).

Generates in <root_out>/:
  - quotes_compare.png           (top-of-book A vs B)
  - fills_compare.png            (cumulative filled qty A vs B)  [if fills present]
  - pnl_timeseries_compare.png   (PnL A vs B)                    [if PnL present]

Usage:
  python make_day20_charts.py --root out/snapshot_proof

Optional flags:
  --tick-size <float>     multiply tick px to price (default=1.0)
  --tz UTC|local          time axis zone (default=UTC)
  --fills-a FPATH         override fills A path
  --fills-b FPATH         override fills B path
  --fills-ts COL          override fills timestamp column name
  --fills-qty COL         override fills filled-qty column name
  --pnl-a FPATH           override pnl A path
  --pnl-b FPATH           override pnl B path
  --pnl-ts COL            override pnl timestamp column name
  --pnl-val COL           override pnl value column name
"""

from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

# ---------- utilities ----------

def to_dt_ns(series, tz: str) -> pd.Series:
    dt = pd.to_datetime(series.astype("int64"), utc=True, errors="coerce")
    if tz.lower() == "local":
        return dt.dt.tz_convert(None)
    return dt

def load_csv(path: Path) -> pd.DataFrame | None:
    if not path.exists() or path.stat().st_size == 0:
        print(f"[skip] {path} (missing or empty)")
        return None
    try:
        return pd.read_csv(path)
    except Exception as e:
        print(f"[warn] failed to read {path}: {e}")
        return None

def save_fig(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    print(f"[ok] wrote {path}")
    plt.close()

def pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    # fuzzy: look for contains substrings in order of preference
    lowers = {c.lower(): c for c in df.columns}
    def first_contains(keys):
        for key in keys:
            for low, real in lowers.items():
                if key in low:
                    return real
        return None
    return None

def auto_ts_col(df: pd.DataFrame, explicit: str | None = None) -> str | None:
    if explicit and explicit in df.columns: return explicit
    for c in ["ts_ns","time_ns","timestamp_ns","ts","timestamp","time"]:
        if c in df.columns: return c
    # last resort: any column containing 'ts' and 'ns'
    lowers = {c.lower(): c for c in df.columns}
    for low, real in lowers.items():
        if "ts" in low and "ns" in low:
            return real
    return None

def auto_qty_col(df: pd.DataFrame, explicit: str | None = None) -> str | None:
    if explicit and explicit in df.columns: return explicit
    for c in ["filled_qty","fill_qty","exec_qty","executed_qty","qty_filled","qty"]:
        if c in df.columns: return c
    # fuzzy: any col with both "fill"/"exec" and "qty"
    lowers = {c.lower(): c for c in df.columns}
    for low, real in lowers.items():
        if ("fill" in low or "exec" in low) and "qty" in low:
            return real
    return None

def auto_pnl_col(df: pd.DataFrame, explicit: str | None = None) -> str | None:
    if explicit and explicit in df.columns: return explicit
    for c in ["pnl","pnl_total","cum_pnl","pnl_net","pnl_usd"]:
        if c in df.columns: return c
    lowers = {c.lower(): c for c in df.columns}
    for low, real in lowers.items():
        if "pnl" in low:
            return real
    return None

# ---------- plots ----------

def plot_quotes(root: Path, tick_size: float, tz: str):
    qA = load_csv(root / "quotes_A.csv")
    qB = load_csv(root / "quotes_B.csv")
    if qA is None or qB is None: return

    for col in ("ts_ns","bid_px","ask_px","bid_qty","ask_qty"):
        if col not in qA.columns or col not in qB.columns:
            print("[skip] quotes comparison (missing required columns in A or B)")
            return

    if tick_size and tick_size != 1.0:
        for c in ("bid_px","ask_px"):
            qA[c] = qA[c] * tick_size
            qB[c] = qB[c] * tick_size
        ylab = "price"
    else:
        ylab = "ticks"

    tA = to_dt_ns(qA["ts_ns"], tz)
    tB = to_dt_ns(qB["ts_ns"], tz)

    plt.figure(figsize=(11, 4.5))
    plt.plot(tA, qA["bid_px"], label="A bid")
    plt.plot(tB, qB["bid_px"], linestyle="--", label="B bid")
    plt.plot(tA, qA["ask_px"], label="A ask")
    plt.plot(tB, qB["ask_px"], linestyle="--", label="B ask")
    plt.title("Top-of-book: A (single pass) vs B (snapshot+resume)")
    plt.xlabel("time"); plt.ylabel(ylab); plt.legend()
    save_fig(root / "quotes_compare.png")

def plot_fills(root: Path, tz: str, pathA: Path | None, pathB: Path | None,
               ts_override: str | None, qty_override: str | None):
    fA = load_csv(pathA or (root / "bt" / "A" / "twap_fills.csv"))
    fB = load_csv(pathB or (root / "bt" / "B" / "twap_fills.csv"))
    if fA is None or fB is None:
        print("[skip] fills comparison (missing A/B fills)")
        return

    tsA = auto_ts_col(fA, ts_override)
    tsB = auto_ts_col(fB, ts_override)
    qA = auto_qty_col(fA, qty_override)
    qB = auto_qty_col(fB, qty_override)

    if not tsA or not tsB or not qA or not qB:
        print(f"[skip] fills comparison (columns) -> tsA:{tsA} tsB:{tsB} qA:{qA} qB:{qB}")
        print(f"[hint] override with --fills-ts <col> and --fills-qty <col>")
        return

    print(f"[fills] using A: ts={tsA}, qty={qA} | B: ts={tsB}, qty={qB}")

    A = fA[[tsA, qA]].copy().rename(columns={tsA:"ts_ns", qA:"filled_qty"}).sort_values("ts_ns")
    B = fB[[tsB, qB]].copy().rename(columns={tsB:"ts_ns", qB:"filled_qty"}).sort_values("ts_ns")

    A["cum_fill"] = A["filled_qty"].cumsum()
    B["cum_fill"] = B["filled_qty"].cumsum()

    tA = to_dt_ns(A["ts_ns"], tz)
    tB = to_dt_ns(B["ts_ns"], tz)

    plt.figure(figsize=(11, 4.5))
    plt.plot(tA, A["cum_fill"], label="A cumulative fill")
    plt.plot(tB, B["cum_fill"], linestyle="--", label="B cumulative fill")
    plt.title("Cumulative filled quantity: A vs B")
    plt.xlabel("time"); plt.ylabel("qty"); plt.legend()
    save_fig(root / "fills_compare.png")

def plot_pnl(root: Path, tz: str, pathA: Path | None, pathB: Path | None,
             ts_override: str | None, pnl_override: str | None):
    pA = load_csv(pathA or (root / "bt" / "A" / "pnl_timeseries.csv"))
    pB = load_csv(pathB or (root / "bt" / "B" / "pnl_timeseries.csv"))
    if pA is None or pB is None:
        print("[skip] pnl comparison (missing A/B pnl_timeseries.csv)")
        return

    tsA = auto_ts_col(pA, ts_override)
    tsB = auto_ts_col(pB, ts_override)
    vA  = auto_pnl_col(pA, pnl_override)
    vB  = auto_pnl_col(pB, pnl_override)

    if not tsA or not tsB or not vA or not vB:
        print(f"[skip] pnl comparison (columns) -> tsA:{tsA} tsB:{tsB} pnlA:{vA} pnlB:{vB}")
        print(f"[hint] override with --pnl-ts <col> and --pnl-val <col>")
        return

    print(f"[pnl] using A: ts={tsA}, val={vA} | B: ts={tsB}, val={vB}")

    A = pA[[tsA, vA]].copy().rename(columns={tsA:"ts_ns", vA:"pnl"}).sort_values("ts_ns")
    B = pB[[tsB, vB]].copy().rename(columns={tsB:"ts_ns", vB:"pnl"}).sort_values("ts_ns")

    tA = to_dt_ns(A["ts_ns"], tz)
    tB = to_dt_ns(B["ts_ns"], tz)

    plt.figure(figsize=(11, 4.5))
    plt.plot(tA, A["pnl"], label="A PnL")
    plt.plot(tB, B["pnl"], linestyle="--", label="B PnL")
    plt.title("PnL timeseries: A vs B")
    plt.xlabel("time"); plt.ylabel("pnl"); plt.legend()
    save_fig(root / "pnl_timeseries_compare.png")

# ---------- main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True, help="snapshot-proof output dir (e.g., out/snapshot_proof)")
    ap.add_argument("--tick-size", type=float, default=1.0, help="multiply tick px to price (default=1.0)")
    ap.add_argument("--tz", default="UTC", choices=["UTC","local"], help="time axis zone")

    ap.add_argument("--fills-a", type=Path, default=None)
    ap.add_argument("--fills-b", type=Path, default=None)
    ap.add_argument("--fills-ts", type=str, default=None)
    ap.add_argument("--fills-qty", type=str, default=None)

    ap.add_argument("--pnl-a", type=Path, default=None)
    ap.add_argument("--pnl-b", type=Path, default=None)
    ap.add_argument("--pnl-ts", type=str, default=None)
    ap.add_argument("--pnl-val", type=str, default=None)

    args = ap.parse_args()
    root = Path(args.root).resolve()

    # Quotes always first (most reliable)
    plot_quotes(root, args.tick_size, args.tz)
    # Fills + PnL with auto-detection and optional overrides
    plot_fills(root, args.tz, args.fills_a, args.fills_b, args.fills_ts, args.fills_qty)
    plot_pnl(root, args.tz, args.pnl_a, args.pnl_b, args.pnl_ts, args.pnl_val)

if __name__ == "__main__":
    main()
