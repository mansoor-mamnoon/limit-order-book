# python/olob/cli.py
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

import click

# Day 8: crypto connectors
from olob.crypto.binance import run_capture as _binance_capture
from olob.crypto.common import normalize_day as _normalize_day

# Analysis/report pipeline
from olob import analyze as _analyze

# Backtester (TWAP/VWAP/POV/Iceberg)
from olob.backtest import run_backtest as _run_backtest

# Sweep (parameter grid + parallel backtests)
try:
    from olob.sweep import run_sweep as _run_sweep  # provided by python/olob/sweep.py
except Exception:
    _run_sweep = None  # type: ignore

from olob.crashsnapshot import run_replay_with_snapshot as _run_snapshot
from olob.crashsnapshot import prove_equivalence as _prove_snapshot_equivalence

@click.group(help="LOB utilities")
def cli() -> None:
    pass


def _existing(p: Path) -> Optional[str]:
    return str(p) if p.is_file() and os.access(p, os.X_OK) else None


def _find_bench_tool() -> Optional[str]:
    exe = shutil.which("bench_tool")
    if exe:
        return exe
    venv_bin = Path(sys.prefix) / ("Scripts" if os.name == "nt" else "bin")
    exe = _existing(venv_bin / ("bench_tool.exe" if os.name == "nt" else "bench_tool"))
    if exe:
        return exe

    here = Path(__file__).resolve()
    repo = here.parents[3] if (len(here.parents) >= 4 and (here.parents[3] / "cpp").exists()) else here.parents[2]
    candidates: list[Path] = []
    for p in repo.glob("_skbuild/*/*"):
        if p.is_dir():
            candidates.extend(p.rglob("bench_tool"))
    candidates.append(repo / "build" / "cpp" / "bench_tool")
    candidates.append(repo / "cpp" / "build" / "bench_tool")
    candidates.append(here.parent / "bench_tool")
    for c in candidates:
        exe = _existing(c)
        if exe:
            return exe

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
        [exe, n],
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
# Crypto commands
# ---------------------------

@cli.command("crypto-capture", help="Capture depth diffs @100ms + trades (raw JSONL.GZ).")
@click.option("--exchange", default="binance", show_default=True,
              type=click.Choice(["binance", "binanceus"], case_sensitive=False))
@click.option("--symbol", default="BTCUSDT", show_default=True)
@click.option("--minutes", default=60, show_default=True, type=int)
@click.option("--raw-dir", default="raw", show_default=True)
@click.option("--snapshot-every-sec", default=600, show_default=True, type=int)
def crypto_capture(exchange: str, symbol: str, minutes: int, raw_dir: str, snapshot_every_sec: int) -> None:
    _binance_capture(
        symbol=symbol.upper(),
        minutes=minutes,
        raw_root=raw_dir,
        snapshot_every_sec=snapshot_every_sec,
        exchange=exchange.lower(),
    )


@cli.command("normalize", help="Normalize raw depth diffs + trades to Parquet {ts,side,price,qty,type}.")
@click.option("--exchange", default="binance", show_default=True,
              type=click.Choice(["binance", "binanceus"], case_sensitive=False))
@click.option("--date", default=None, help="UTC date YYYY-MM-DD (defaults to today UTC)")
@click.option("--symbol", default="BTCUSDT", show_default=True)
@click.option("--raw-dir", default="raw", show_default=True)
@click.option("--out-dir", default="parquet", show_default=True)
def normalize(exchange: str, date: Optional[str], symbol: str, raw_dir: str, out_dir: str) -> None:
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
# Backtest (TWAP/VWAP/POV/Iceberg)
# ---------------------------

@cli.command("backtest", help="Run strategy backtest and output fills + cost.")
@click.option("--strategy", required=True, help="YAML config (e.g., docs/strategy/twap.yaml)")
@click.option("--quotes", required=False, help="TAQ quotes CSV (from replay_tool)")
@click.option("--file", required=False, help="Alias for --quotes")
@click.option("--trades", required=False, help="TAQ trades CSV (for VWAP/POV weights)")
@click.option("--out", "out_dir", required=True, help="Output directory (e.g., out/backtests/run1)")
@click.option("--seed", default=42, show_default=True, type=int, help="Deterministic RNG seed")
def backtest(strategy: str, quotes: Optional[str], file: Optional[str],
             trades: Optional[str], out_dir: str, seed: int) -> None:
    qpath = quotes or file
    if not qpath:
        click.secho("Provide --quotes or --file (quotes CSV).", fg="red")
        raise click.Abort()

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    summary = _run_backtest(strategy_yaml=str(strategy),
                            quotes_csv=str(qpath),
                            trades_csv=str(trades) if trades else None,
                            out_dir=str(out),
                            seed=int(seed))

    fills_path = summary.get("fills_csv")
    summary_path = Path(fills_path).with_name(
        Path(fills_path).stem.replace("_fills", "_summary") + ".json"
    ) if fills_path else (out / (Path(strategy).stem + "_summary.json"))

    click.secho(f"[fills]   {fills_path}", fg="green")
    click.secho(f"[summary] {summary_path}", fg="green")


# ---------------------------
# Sweep (parameter grid + parallel backtests)
# ---------------------------

@cli.command("sweep", help="Run parameter sweep in parallel from a grid YAML.")
@click.option("--grid", "grid_path", required=True, type=click.Path(dir_okay=False, path_type=Path),
              help="Grid YAML describing base_strategy, inputs, and grid dot-keys")
@click.option("--out", "out_override", required=False, type=click.Path(file_okay=False, path_type=Path),
              help="Override out_root in the YAML (optional)")
@click.option("--max-procs", type=int, default=None, help="Override concurrency (defaults to YAML or CPU count)")
@click.option("--metric", type=click.Choice(["sharpe_like", "pnl_over_dd", "pnl_total"]), default=None,
              help="Ranking metric (default from YAML or sharpe_like)")
@click.option("--topk", type=int, default=None, help="How many configs to plot/rank (default from YAML or 20)")
@click.option("--resume", is_flag=True, default=False, help="Skip runs with _SUCCESS sentinel")
def sweep(grid_path: Path,
          out_override: Optional[Path],
          max_procs: Optional[int],
          metric: Optional[str],
          topk: Optional[int],
          resume: bool) -> None:
    if _run_sweep is None:
        click.secho("Sweep module not available. Ensure python/olob/sweep.py exists and imports correctly.", fg="red")
        raise click.Abort()

    try:
        import yaml  # PyYAML required to parse the grid
    except ImportError:
        click.secho("PyYAML is required for sweeps. Install with: pip install pyyaml", fg="red")
        raise click.Abort()

    try:
        cfg: Dict[str, Any] = yaml.safe_load(grid_path.read_text())
    except Exception as e:
        click.secho(f"Failed to read grid YAML: {grid_path}\n{e}", fg="red")
        raise click.Abort()

    grid_dir = grid_path.parent.resolve()
    def _resolve(p: str | os.PathLike[str]) -> Path:
        p = Path(p)
        return p if p.is_absolute() else (grid_dir / p)

    # Required fields from YAML (resolved relative to YAML file)
    base_strategy = _resolve(cfg["base_strategy"])
    quotes        = _resolve(cfg["quotes"])
    trades        = _resolve(cfg["trades"])

    out_root = Path(cfg.get("out_root", "out/sweeps/default"))
    if out_override:
        out_root = out_override

    # Optional fields
    seeds: List[int] = list(cfg.get("seeds", [1]))
    grid: Dict[str, List[Any]] = dict(cfg["grid"])  # dot-keys -> values
    metric_val: str = metric or cfg.get("metric", "sharpe_like")
    topk_val: int = int(topk or cfg.get("topk", 20))
    resume_val: bool = bool(resume or cfg.get("resume", True))
    max_procs_val: int = int(max_procs or cfg.get("max_procs", os.cpu_count() or 4))

    # Optional ranking filters (defaults are off)
    require_full_fill: bool = bool(cfg.get("require_full_fill", False))
    min_fill_ratio: float = float(cfg.get("min_fill_ratio", 1.0))

    extra_args = cfg.get("extra_args", [])
    if isinstance(extra_args, str):
        import shlex
        extra_args = shlex.split(extra_args)
    elif not isinstance(extra_args, list):
        extra_args = []

    # Pre-flight: inputs must exist
    missing = [p for p in [base_strategy, quotes, trades] if not Path(p).exists()]
    if missing:
        click.secho("[sweep] Missing required files:", fg="red")
        for m in missing:
            click.echo(f"  - {m}")
        raise click.Abort()

    click.secho("== Sweep plan ==", fg="cyan")
    click.echo(f"base_strategy: {base_strategy}")
    click.echo(f"quotes:        {quotes}")
    click.echo(f"trades:        {trades}")
    click.echo(f"out_root:      {out_root}")
    click.echo(f"seeds:         {seeds}")
    click.echo(f"grid keys:     {list(grid.keys())}")
    click.echo(f"metric:        {metric_val}")
    click.echo(f"topk:          {topk_val}")
    click.echo(f"max_procs:     {max_procs_val}")
    click.echo(f"resume:        {resume_val}")
    if require_full_fill:
        click.echo(f"require_full_fill: True (min_fill_ratio={min_fill_ratio})")
    if extra_args:
        click.echo(f"extra_args:    {extra_args}")

    out_root.mkdir(parents=True, exist_ok=True)
    try:
        agg_csv, plot_path = _run_sweep(
            base_strategy=base_strategy,
            quotes=quotes,
            trades=trades,
            out_root=out_root,
            grid=grid,
            seeds=seeds,
            max_procs=max_procs_val,
            metric=metric_val,
            topk=topk_val,
            resume=resume_val,
            extra_args=extra_args,
            require_full_fill=require_full_fill,
            min_fill_ratio=min_fill_ratio,
        )
    except KeyError as e:
        click.secho(f"[sweep] Missing required YAML key: {e!s}", fg="red")
        raise click.Abort()
    except Exception as e:
        click.secho(f"[sweep] Failed: {e}", fg="red")
        raise click.Abort()

    click.secho(f"[ok] aggregate -> {agg_csv}", fg="green")
    if plot_path and Path(plot_path).exists():
        click.secho(f"[ok] ranking plot -> {plot_path}", fg="green")
    best = out_root / "best.json"
    if best.exists():
        click.secho(f"[ok] best -> {best}", fg="green")

# ---------------------------
# Crash recovery check
# ---------------------------

from olob.crashcheck import run_crash_check as _run_crash_check

@cli.command("crash-check", help="Prove two-phase (crash->resume) equals single-pass fills (TWAP recommended).")
@click.option("--strategy", required=True, help="YAML config (e.g., docs/strategy/twap.yaml)")
@click.option("--quotes", required=True, help="TAQ quotes CSV")
@click.option("--trades", required=False, help="TAQ trades CSV")
@click.option("--out", "out_dir", required=True, help="Output dir for ref/partA/partB")
@click.option("--bar-sec", default=60, show_default=True, type=int, help="Bar size in seconds (align cut)")
@click.option("--cut-pct", default=0.6, show_default=True, type=float, help="Cut as fraction of total duration (0..1)")
@click.option("--seed", default=123, show_default=True, type=int)
def crash_check_cmd(strategy: str, quotes: str, trades: Optional[str],
                    out_dir: str, bar_sec: int, cut_pct: float, seed: int) -> None:
    res = _run_crash_check(strategy_yaml=strategy,
                           quotes_csv=quotes,
                           trades_csv=trades,
                           out_dir=out_dir,
                           bar_sec=bar_sec,
                           cut_pct=cut_pct,
                           seed=seed)
    if res.ok:
        click.secho(f"[ok] crash-check passed: {res.message}", fg="green")
    else:
        click.secho(f"[FAIL] crash-check failed: {res.message}", fg="red")
        click.secho(f"  ref:   {res.ref_dir}", fg="yellow")
        click.secho(f"  partA: {res.partA_dir}", fg="yellow")
        click.secho(f"  partB: {res.partB_dir}", fg="yellow")
        raise click.Abort()

@cli.command("snapshot-proof", help="Dev proof: snapshot+mid-file == single-pass (TWAP-friendly).")
@click.option("--events", "events_csv", required=True, help="Events CSV with ts_ns")
@click.option("--cut-ns", required=True, type=int, help="Cut timestamp in ns")
@click.option("--out", "out_dir", required=True, help="Output dir for artifacts")
@click.option("--strategy", required=False, help="YAML strategy (TWAP recommended)")
def snapshot_proof_cmd(events_csv: str, cut_ns: int, out_dir: str, strategy: str | None) -> None:
    artifacts = _run_snapshot(events_csv=events_csv, cut_ns=cut_ns, out_dir=out_dir)
    click.secho(f"[ok] snapshot -> {artifacts['snap']}", fg="green")
    if strategy:
        eq = _prove_snapshot_equivalence(
            strategy_yaml=strategy,
            quotes_full=artifacts["quotes_A"],
            quotes_resume=artifacts["quotes_B"],
            out_dir=str(Path(out_dir) / "bt"),
        )
        click.secho(f"[ok] fills equivalent: {eq['message']}", fg="green")
    else:
        click.secho("[note] Pass --strategy docs/strategy/twap.yaml to run the fills equivalence check.", fg="yellow")


if __name__ == "__main__":
    cli()
