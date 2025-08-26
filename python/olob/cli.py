# python/olob/cli.py
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

import click

# Day 8: crypto connectors (make sure these modules exist under python/olob/crypto/)
from olob.crypto.binance import run_capture as _binance_capture
from olob.crypto.common import normalize_day as _normalize_day

# Analysis/report pipeline
from olob import analyze as _analyze

# Backtester (VWAP/TWAP)
from olob.backtest import run_backtest as _run_backtest


@click.group(help="LOB utilities")
def cli() -> None:
    """Root command group."""
    pass


# ---------------------------
# Existing benchmark command
# ---------------------------

def _existing(p: Path) -> Optional[str]:
    return str(p) if p.is_file() and os.access(p, os.X_OK) else None


def _find_bench_tool() -> Optional[str]:
    # 1) PATH
    exe = shutil.which("bench_tool")
    if exe:
        return exe

    # 2) venv bin/Scripts
    venv_bin = Path(sys.prefix) / ("Scripts" if os.name == "nt" else "bin")
    exe = _existing(venv_bin / ("bench_tool.exe" if os.name == "nt" else "bench_tool"))
    if exe:
        return exe

    # 3) scikit-build editable locations + common local builds
    here = Path(__file__).resolve()
    repo = here.parents[3] if (len(here.parents) >= 4 and (here.parents[3] / "cpp").exists()) else here.parents[2]
    candidates: list[Path] = []

    # a) _skbuild/**/bench_tool
    for p in repo.glob("_skbuild/*/*"):
        if p.is_dir():
            candidates.extend(p.rglob("bench_tool"))

    # b) common build dirs
    candidates.append(repo / "build" / "cpp" / "bench_tool")
    candidates.append(repo / "cpp" / "build" / "bench_tool")

    # c) rare: bundled next to package
    candidates.append(here.parent / "bench_tool")

    for c in candidates:
        exe = _existing(c)
        if exe:
            return exe

    # 4) user override
    env = os.getenv("LOB_BENCH")
    if env and _existing(Path(env)):
        return env

    return None


@cli.command("bench", help="Run the native C++ bench tool with --msgs.")
@click.option("--msgs", type=float, default=1e6, show_default=True, help="Number of messages")
def bench(msgs: float) -> None:
    exe = _find_bench_tool()
    if not exe:
        click.secho(
            "bench_tool not found.\n"
            "Fix options:\n"
            "  1) Build it:  cmake -S cpp -B build/cpp -DCMAKE_BUILD_TYPE=Release && cmake --build build/cpp -j\n"
            "  2) Or set LOB_BENCH=/full/path/to/bench_tool and rerun.\n",
            fg="red",
        )
        raise click.Abort()

    n = str(int(msgs))
    trials = [
        [exe, "--msgs", n],
        [exe, "--num", n],
        [exe, "-n", n],
        [exe, n],  # positional
    ]

    last_err: Optional[subprocess.CalledProcessError] = None
    for args in trials:
        try:
            subprocess.check_call(args)
            return
        except subprocess.CalledProcessError as e:
            last_err = e
            continue

    click.secho("bench_tool failed with all known argument forms:", fg="red")
    for a in trials:
        click.echo("  $ " + " ".join(a))
    if last_err is not None:
        click.echo(f"\nLast error code: {last_err.returncode}")
    click.echo("\nTry running the tool manually to see its usage/help:")
    click.echo(f"  $ {exe} --help  (or)  $ {exe}")
    raise click.Abort()


# ---------------------------
# Day 8 — crypto commands
# ---------------------------

@cli.command("crypto-capture", help="Capture depth diffs @100ms + trades (raw JSONL.GZ).")
@click.option("--exchange", default="binance", show_default=True,
              type=click.Choice(["binance", "binanceus"], case_sensitive=False),
              help="Exchange to use")
@click.option("--symbol", default="BTCUSDT", show_default=True, help="Trading pair symbol")
@click.option("--minutes", default=60, show_default=True, type=int, help="How long to capture")
@click.option("--raw-dir", default="raw", show_default=True, help="Root folder for raw output")
@click.option("--snapshot-every-sec", default=600, show_default=True, type=int,
              help="REST snapshot cadence (seconds)")
def crypto_capture(exchange: str, symbol: str, minutes: int, raw_dir: str, snapshot_every_sec: int) -> None:
    """
    Starts a capture session:
      - WebSocket: diff-depth(100ms) + trades
      - REST: periodic depth snapshots
      - Writes raw gzipped JSONL to raw/YYYY-MM-DD/<exchange>/<symbol>/{depth,trades}
    """
    _binance_capture(
        symbol=symbol.upper(),
        minutes=minutes,
        raw_root=raw_dir,
        snapshot_every_sec=snapshot_every_sec,
        exchange=exchange.lower(),
    )


@cli.command("normalize", help="Normalize raw depth diffs + trades to Parquet {ts,side,price,qty,type}.")
@click.option("--exchange", default="binance", show_default=True,
              type=click.Choice(["binance", "binanceus"], case_sensitive=False),
              help="Exchange that produced the raw files")
@click.option("--date", default=None, help="UTC date YYYY-MM-DD (defaults to today UTC)")
@click.option("--symbol", default="BTCUSDT", show_default=True, help="Symbol to normalize")
@click.option("--raw-dir", default="raw", show_default=True, help="Raw input root")
@click.option("--out-dir", default="parquet", show_default=True, help="Parquet output root")
def normalize(exchange: str, date: Optional[str], symbol: str, raw_dir: str, out_dir: str) -> None:
    """
    Reads raw gzipped JSONL from raw/<date>/<exchange>/<symbol> and writes a single Parquet
    at parquet/<date>/<exchange>/<symbol>/events.parquet with the schema:
      ts:int64(ns UTC), side:str, price:float64, qty:float64, type:str
    """
    day = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    _normalize_day(
        date_str=day,
        exchange=exchange.lower(),
        symbol=symbol.upper(),
        raw_root=raw_dir,
        out_root=out_dir,
    )


# ---------------------------
# Analyze (HTML report)
# ---------------------------

@cli.command("analyze", help="Generate a self-contained HTML report with plots + stats.")
@click.option("--exchange", default="binanceus", show_default=True)
@click.option("--symbol", default="BTCUSDT", show_default=True)
@click.option("--date", help="UTC date folder YYYY-MM-DD (default: yesterday UTC)")
@click.option("--hour-start", default="10:00", show_default=True, help="UTC hour start (HH:MM)")
@click.option("--parquet-dir", default="parquet", show_default=True)
@click.option("--build-dir", default="build", show_default=True)
@click.option("--out-reports", default="out/reports", show_default=True)
@click.option("--tmp", default="out/tmp_report", show_default=True)
@click.option("--cadence-ms", default=50, show_default=True, type=int)
@click.option("--speed", default="50x", show_default=True)
@click.option("--depth-top10", default=None, help="Optional path to L2 top-10 depth parquet")
def analyze(exchange, symbol, date, hour_start, parquet_dir, build_dir,
            out_reports, tmp, cadence_ms, speed, depth_top10):
    out = _analyze.run_pipeline(
        exchange=exchange,
        symbol=symbol,
        date=date or _analyze._yesterday_utc_date(),
        hour_start=hour_start,
        parquet_dir=Path(parquet_dir),
        build_dir=Path(build_dir),
        out_reports_dir=Path(out_reports),
        tmp_dir=Path(tmp),
        cadence_ms=cadence_ms,
        speed=speed,
        depth_top10=Path(depth_top10) if depth_top10 else None,
    )
    click.secho(f"[report] wrote {out}", fg="green")


# ---------------------------
# Backtest (VWAP/TWAP)
# ---------------------------

@cli.command("backtest", help="Run VWAP/TWAP strategy backtest and output fills + cost.")
@click.option("--strategy", required=True, help="YAML config (e.g., docs/strategy/vwap.yaml)")
@click.option("--quotes", required=False, help="TAQ quotes CSV (from replay_tool)")
@click.option("--file", required=False, help="Alias for --quotes")
@click.option("--trades", required=False, help="TAQ trades CSV (for VWAP volume weights)")
@click.option("--out", "out_dir", required=True, help="Output directory (e.g., out/backtests/run1)")
def backtest(strategy: str, quotes: Optional[str], file: Optional[str],
             trades: Optional[str], out_dir: str) -> None:
    """
    Schedules parent orders into child clips (TWAP/VWAP), applies fixed latency,
    quantizes by tick/lot, executes against top-of-book, and writes:
      - *_fills.csv
      - *_summary.json
    """
    qpath = quotes or file
    if not qpath:
        click.secho("Provide --quotes or --file (quotes CSV).", fg="red")
        raise click.Abort()

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    summary = _run_backtest(strategy_yaml=str(strategy),
                            quotes_csv=str(qpath),
                            trades_csv=str(trades) if trades else None,
                            out_dir=str(out))

    click.secho(f"[fills]   {summary.get('fills_csv')}", fg="green")
    click.secho(f"[summary] {out / (Path(strategy).stem + '_summary.json')}", fg="green")


if __name__ == "__main__":
    cli()
