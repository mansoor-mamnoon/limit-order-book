# python/olob/make_readme_figs.py
from __future__ import annotations
import argparse, json
from pathlib import Path

import pandas as pd
import numpy as np

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _read_agg(agg_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(agg_csv)
    # params is JSON in a CSV field — keep string but don’t explode
    # coerce numeric fields if present
    for col in ["score", "pnl_total", "max_drawdown", "sharpe_like", "filled_qty", "fees"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _risk_scatter(agg_csv: Path, out_png: Path) -> None:
    df = _read_agg(agg_csv)
    # filter sensible points
    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["sharpe_like", "max_drawdown"])
    if df.empty:
        print(f"[risk_scatter] no usable rows in {agg_csv}")
        return
    x = df["max_drawdown"].astype(float)
    y = df["sharpe_like"].astype(float)

    fig = plt.figure(figsize=(7, 5))
    plt.scatter(x, y, s=18, alpha=0.8)
    plt.xlabel("Max drawdown")
    plt.ylabel("Sharpe-like")
    plt.title("Risk vs Return (all configs)")
    plt.grid(True, alpha=0.25)
    plt.tight_layout()
    fig.savefig(out_png, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[ok] risk_scatter -> {out_png}")


def _equity_from_pnl_csv(pnl_csv: Path) -> pd.DataFrame:
    """
    Expect columns like: ts_ns, cash, inventory, equity  (names may vary slightly).
    We’ll try a few common patterns and fall back gracefully.
    """
    df = pd.read_csv(pnl_csv)
    # find time column
    ts_col = None
    for c in ["ts_ns", "ts", "timestamp", "time_ns", "time"]:
        if c in df.columns:
            ts_col = c
            break
    if ts_col is None:
        raise ValueError(f"No timestamp column found in {pnl_csv} (looked for ts_ns/ts/timestamp/time_ns/time)")

    # to datetime
    if ts_col.endswith("_ns") or ts_col.endswith("ns"):
        df["ts_dt"] = pd.to_datetime(df[ts_col], unit="ns", utc=True)
    elif ts_col.endswith("_us") or ts_col.endswith("us"):
        df["ts_dt"] = pd.to_datetime(df[ts_col], unit="us", utc=True)
    elif ts_col.endswith("_ms") or ts_col.endswith("ms"):
        df["ts_dt"] = pd.to_datetime(df[ts_col], unit="ms", utc=True)
    else:
        # hope it parses
        df["ts_dt"] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")

    # value columns
    for need in ["equity"]:
        if need not in df.columns:
            # try to reconstruct equity if cash & inventory + mark-to-mid exist; otherwise skip
            pass
    return df


def _equity_curve(best_run_dir: Path, out_png: Path) -> None:
    # locate PnL time series
    pnl_csv = best_run_dir / "pnl_timeseries.csv"
    if not pnl_csv.exists():
        # try a fallback naming: any matching file
        cand = list(best_run_dir.glob("*pnl*csv"))
        if not cand:
            print(f"[equity_curve] no pnl_timeseries.csv in {best_run_dir}")
            return
        pnl_csv = cand[0]

    df = _equity_from_pnl_csv(pnl_csv)

    # choose fields
    y_cols = [c for c in ["equity", "cash"] if c in df.columns]
    if not y_cols and "equity" not in df.columns:
        print(f"[equity_curve] no equity/cash columns in {pnl_csv}")
        return

    fig = plt.figure(figsize=(8, 5))
    if "equity" in df.columns:
        plt.plot(df["ts_dt"], df["equity"], label="Equity")
    if "cash" in df.columns:
        plt.plot(df["ts_dt"], df["cash"], label="Cash", alpha=0.7)
    plt.xlabel("Time (UTC)")
    plt.ylabel("Notional")
    plt.title("Equity curve (best run)")
    plt.grid(True, alpha=0.25)
    plt.legend()
    plt.tight_layout()
    fig.savefig(out_png, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[ok] equity_curve -> {out_png}")


def _pnl_timeseries(best_run_dir: Path, out_png: Path) -> None:
    pnl_csv = best_run_dir / "pnl_timeseries.csv"
    if not pnl_csv.exists():
        cand = list(best_run_dir.glob("*pnl*csv"))
        if not cand:
            print(f"[pnl_timeseries] no pnl_timeseries.csv in {best_run_dir}")
            return
        pnl_csv = cand[0]

    df = _equity_from_pnl_csv(pnl_csv)

    cols = [c for c in ["equity", "cash", "inventory"] if c in df.columns]
    if not cols:
        print(f"[pnl_timeseries] no equity/cash/inventory columns in {pnl_csv}")
        return

    fig = plt.figure(figsize=(8, 5))
    for c in cols:
        plt.plot(df["ts_dt"], df[c], label=c)
    plt.xlabel("Time (UTC)")
    plt.ylabel("Value")
    plt.title("PnL timeseries (best run)")
    plt.grid(True, alpha=0.25)
    plt.legend()
    plt.tight_layout()
    fig.savefig(out_png, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[ok] pnl_timeseries -> {out_png}")


def main():
    ap = argparse.ArgumentParser(description="Generate README figures from sweep artifacts.")
    ap.add_argument("--sweep-dir", required=True, help="e.g. out/sweeps/acceptance")
    ap.add_argument("--best-json", default=None, help="Path to best.json (defaults to <sweep-dir>/best.json)")
    args = ap.parse_args()

    sweep_dir = Path(args.sweep_dir).resolve()
    best_json = Path(args.best_json) if args.best_json else (sweep_dir / "best.json")

    agg_csv = sweep_dir / "aggregate.csv"
    if not agg_csv.exists():
        raise SystemExit(f"aggregate.csv not found under {sweep_dir}")

    plots_dir = sweep_dir / "plots"
    _ensure_dir(plots_dir)

    # 1) risk scatter for all configs
    _risk_scatter(agg_csv, plots_dir / "risk_scatter.png")

    # 2) figures for the best run
    if not best_json.exists():
        raise SystemExit(f"best.json not found under {sweep_dir}")
    best = json.loads(best_json.read_text())
    best_run_dir = Path(best["run_dir"]).resolve()

    _equity_curve(best_run_dir, plots_dir / "equity_curve.png")
    _pnl_timeseries(best_run_dir, plots_dir / "pnl_timeseries.png")


if __name__ == "__main__":
    main()
