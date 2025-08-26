#!/usr/bin/env python3
"""
Run Day 17 end-to-end for a full hour:
- Capture 60 minutes of live data (Binance US by default)
- Normalize raw -> Parquet
- Convert Parquet -> CSV for replay
- Replay -> TAQ quotes/trades
- Generate strategy YAMLs for the captured hour
- Backtest TWAP/VWAP/POV/Iceberg on the same hour
- Produce comparison.csv and print the table

Requires your package entrypoint `lob` and the C++ replay tool to be available.
"""
from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple

import pandas as pd


# -----------------------------
# helpers
# -----------------------------

def _run(cmd: str, cwd: Path | None = None) -> None:
    print("[run]", cmd)
    proc = subprocess.run(shlex.split(cmd), cwd=str(cwd) if cwd else None)
    if proc.returncode != 0:
        raise SystemExit(f"command failed ({proc.returncode}): {cmd}")


def _find_replay_tool() -> str:
    # 1) PATH
    exe = shutil.which("replay_tool")
    if exe:
        return exe

    # 2) common build locations relative to this script / repo
    here = Path(__file__).resolve()
    repo = here.parents[1]  # scripts/ -> repo root assumed one level up
    candidates = [
        repo / "build" / "cpp" / "replay_tool",
        repo / "cpp" / "build" / "replay_tool",
        repo / "python" / "olob" / "replay_tool",  # rare
    ]
    for c in candidates:
        if c.is_file() and os.access(c, os.X_OK):
            return str(c)

    # 3) env override
    env = os.getenv("LOB_REPLAY")
    if env and Path(env).is_file() and os.access(env, os.X_OK):
        return env

    raise FileNotFoundError(
        "Could not find replay_tool. Build it first:\n"
        "  cmake -S cpp -B build/cpp -DCMAKE_BUILD_TYPE=Release\n"
        "  cmake --build build/cpp -j\n"
        "or set LOB_REPLAY=/full/path/to/replay_tool"
    )


def _parquet_to_csv_for_replay(parquet_path: Path, csv_out: Path) -> None:
    df = pd.read_parquet(parquet_path)
    # Normalize schema expected by replay_tool helper in docs:
    if "ts_ns" not in df.columns:
        df["ts_ns"] = pd.to_datetime(df["ts"], utc=True).astype("int64")
    df["type"] = df["type"].astype(str).str.lower()
    # Map sides into B/A (conservative default ask for unrecognized)
    side_map = {"b": "B", "bid": "B", "buy": "B", "a": "A", "ask": "A", "sell": "A", "s": "A"}
    df["side"] = df["side"].astype(str).str.lower().map(side_map).fillna("A")
    # Required columns for replay_tool:
    df[["ts_ns", "type", "side", "price", "qty"]].to_csv(csv_out, index=False)


def _quotes_range(quotes_csv: Path) -> Tuple[pd.Timestamp, pd.Timestamp]:
    # infer range in UTC
    q = pd.read_csv(quotes_csv)
    if "ts_ns" in q.columns:
        ts = pd.to_datetime(q["ts_ns"], utc=True, unit="ns")
    elif "ts" in q.columns:
        ts = pd.to_datetime(q["ts"], utc=True)
    else:
        raise RuntimeError("quotes CSV missing ts/ts_ns")
    return ts.min(), ts.max()


def _write_yaml(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    print(f"[yaml] wrote {path}")


def _make_yaml_bundle(start_ts: pd.Timestamp, end_ts: pd.Timestamp, out_dir: Path) -> dict[str, Path]:
    # Use a consistent hour window
    start_iso = start_ts.isoformat().replace("+00:00", "Z")
    end_iso = end_ts.isoformat().replace("+00:00", "Z")

    # Reasonable sizes so you get fills; taker by default for short windows.
    twap_yaml = f"""\
type: twap
name: twap_hour
side: buy
qty: 5.0
start: {start_iso}
end:   {end_iso}
bar_sec: 60
min_clip: 0.1
cooldown_ms: 0
force_taker: true
cost:
  tick_size: 0.01
  lot_size: 0.001
  fixed_latency_ms: 50
  taker_bps: 2.0
"""

    vwap_yaml = f"""\
type: vwap
name: vwap_hour
side: buy
qty: 5.0
start: {start_iso}
end:   {end_iso}
bar_sec: 60
min_clip: 0.1
cooldown_ms: 0
force_taker: true
cost:
  tick_size: 0.01
  lot_size: 0.001
  fixed_latency_ms: 50
  taker_bps: 2.0
"""

    pov_yaml = f"""\
strategy: pov
side: buy
parent_qty: 5.0
base_participation: 0.20
min_participation: 0.05
max_participation: 0.40
limit_offset_ticks: 0
bar_sec: 60
cooldown_ms: 200
min_clip: 0.1
mode: taker_on_lag
lag_tolerance: 0.05
# runner-side emergency switch only if your backtester supports it; harmless if ignored:
force_taker_on_timeout: true
start: {start_iso}
end:   {end_iso}
cost:
  tick_size: 0.01
  lot_size: 0.001
  fixed_latency_ms: 50
  taker_bps: 2.0
"""

    iceberg_yaml = f"""\
strategy: iceberg
side: buy
parent_qty: 5.0
peak_qty: 0.5
replenish_threshold: 0.6
limit_offset_ticks: 0
bar_sec: 60
jitter_ms: 150
cooldown_ms: 0
# runner-side emergency switch only if your backtester supports it; harmless if ignored:
force_taker_on_timeout: true
start: {start_iso}
end:   {end_iso}
cost:
  tick_size: 0.01
  lot_size: 0.001
  fixed_latency_ms: 50
  taker_bps: 2.0
"""

    dest = out_dir / "strategy_hour"
    twap = dest / "twap.yaml"
    vwap = dest / "vwap.yaml"
    pov = dest / "pov.yaml"
    ice = dest / "iceberg.yaml"
    _write_yaml(twap, twap_yaml)
    _write_yaml(vwap, vwap_yaml)
    _write_yaml(pov, pov_yaml)
    _write_yaml(ice, iceberg_yaml)
    return {"twap": twap, "vwap": vwap, "pov": pov, "iceberg": ice}


# -----------------------------
# main
# -----------------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--exchange", default="binanceus", choices=["binance", "binanceus"])
    p.add_argument("--symbol", default="BTCUSDT")
    p.add_argument("--minutes", type=int, default=60, help="Capture minutes (default 60)")
    p.add_argument("--raw-dir", default="raw")
    p.add_argument("--parquet-dir", default="parquet")
    p.add_argument("--outdir", default="out/backtests/day17_hour")
    p.add_argument("--cadence-ms", type=int, default=1000, help="Replay sampling cadence (ms)")
    p.add_argument("--speed", default="50x", help="Replay speed (cosmetic)")
    p.add_argument("--build-replay", action="store_true", help="Try to build replay_tool if missing")
    args = p.parse_args()

    repo = Path(__file__).resolve().parents[1]
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # 1) Capture a full hour (blocking)
    date_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    _run(
        f"lob crypto-capture --exchange {args.exchange} --symbol {args.symbol} "
        f"--minutes {args.minutes} --raw-dir {args.raw_dir} --snapshot-every-sec 60"
    )

    # 2) Normalize to parquet
    _run(
        f"lob normalize --exchange {args.exchange} --date {date_utc} "
        f"--symbol {args.symbol} --raw-dir {args.raw_dir} --out-dir {args.parquet_dir}"
    )

    # 3) Parquet -> CSV for replay
    parquet_path = Path(args.parquet_dir) / date_utc / args.exchange / args.symbol / "events.parquet"
    if not parquet_path.exists():
        raise SystemExit(f"missing parquet: {parquet_path}")
    csv_events = outdir / "parquet_export.csv"
    _parquet_to_csv_for_replay(parquet_path, csv_events)
    print(f"[ok] wrote {csv_events}")

    # 4) Replay -> TAQ quotes/trades
    try:
        replay = _find_replay_tool()
    except FileNotFoundError as e:
        if args.build_replay:
            print("[info] trying to build replay_tool…")
            _run("cmake -S cpp -B build/cpp -DCMAKE_BUILD_TYPE=Release", cwd=repo)
            _run("cmake --build build/cpp -j", cwd=repo)
            replay = _find_replay_tool()
        else:
            raise

    taq_quotes = outdir / "taq_quotes.csv"
    taq_trades = outdir / "taq_trades.csv"
    _run(
        f"{replay} --file {csv_events} --speed {args.speed} "
        f"--cadence-ms {args.cadence_ms} "
        f"--quotes-out {taq_quotes} --trades-out {taq_trades}"
    )

    # 5) Determine hour window from quotes
    q_min, q_max = _quotes_range(taq_quotes)
    hour_start = q_min.floor('H')
    hour_end = hour_start + pd.Timedelta(hours=1)
    print(f"[info] quotes UTC range: {q_min} → {q_max} ; using hour {hour_start} → {hour_end}")

    # 6) Write YAMLs for that hour
    yamls = _make_yaml_bundle(hour_start, hour_end, outdir)

    # 7) Backtest 4 strategies and compare
    cmp_dir = outdir / "cmp"
    cmp_dir.mkdir(parents=True, exist_ok=True)

    compare_py = Path("scripts") / "compare_execution.py"
    if not compare_py.exists():
        raise SystemExit("scripts/compare_execution.py not found. Save the file included previously.")

    _run(
        f"python {compare_py} "
        f"--quotes {taq_quotes} --trades {taq_trades} --outdir {cmp_dir} "
        f"--twap {yamls['twap']} --vwap {yamls['vwap']} --pov {yamls['pov']} --iceberg {yamls['iceberg']}"
    )

    # 8) Show table
    table = cmp_dir / "comparison.csv"
    if table.exists():
        df = pd.read_csv(table)
        print("\n=== comparison.csv ===")
        print(df.to_string(index=False))
        print(f"\n[ok] wrote {table}")
    else:
        print("[warn] comparison.csv not found")


if __name__ == "__main__":
    main()
