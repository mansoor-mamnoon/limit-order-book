# python/olob/analyze.py
from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

import pandas as pd

# ---------------------------
# Helpers
# ---------------------------

# Fixed-offset for simplicity; adjust if you want true IANA tz handling
TZ_LOCAL = dt.timezone(dt.timedelta(hours=-7))  # America/Los_Angeles (PDT)

def _yesterday_utc_date() -> str:
    """Compute 'yesterday' based on local time, then return the UTC date string (YYYY-MM-DD)."""
    now_local = dt.datetime.now(TZ_LOCAL)
    y_local = (now_local - dt.timedelta(days=1)).date()
    y_start_local = dt.datetime.combine(y_local, dt.time(0, 0), tzinfo=TZ_LOCAL)
    y_start_utc = y_start_local.astimezone(dt.timezone.utc)
    return y_start_utc.date().isoformat()

def _find_events_parquet(parquet_dir: Path, date: str, exchange: str, symbol: str) -> Path:
    p = parquet_dir / date / exchange / symbol / "events.parquet"
    if not p.exists():
        raise FileNotFoundError(f"Parquet not found: {p}")
    return p

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _embed_png_b64(png_path: Path) -> Optional[str]:
    if not png_path.exists():
        return None
    with open(png_path, "rb") as f:
        b = f.read()
    return "data:image/png;base64," + base64.b64encode(b).decode("ascii")

def _bin_ok(bin_path: Path) -> bool:
    return bin_path.exists() and bin_path.is_file() and os.access(bin_path, os.X_OK)

def _run(cmd: List[str], cwd: Optional[Path] = None) -> None:
    try:
        subprocess.run(cmd, check=True, cwd=cwd)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Command failed ({e.returncode}): {' '.join(cmd)}") from e

# ---------------------------
# Pipeline
# ---------------------------

def run_pipeline(
    exchange: str,
    symbol: str,
    date: str,
    hour_start: str,
    parquet_dir: Path,
    build_dir: Path,
    out_reports_dir: Path,
    tmp_dir: Path,
    cadence_ms: int = 50,
    speed: str = "50x",
    depth_top10: Optional[Path] = None,
) -> Path:
    """
    Steps:
      1) slice 1h window from events.parquet -> csv
      2) replay_tool -> taq_quotes.csv + taq_trades.csv
      3) metrics -> analytics/plots/*.png + analytics/summary.json
         microstructure -> extra plots + microstructure_summary.json
      4) emit self-contained HTML under out/reports/DATE_SYMBOL.html
    """
    events_parquet = _find_events_parquet(parquet_dir, date, exchange, symbol)
    _ensure_dir(tmp_dir)
    _ensure_dir(out_reports_dir)

    # 1) Slice 1h window to CSV expected by replay_tool
    hour_dt = dt.datetime.strptime(hour_start, "%H:%M").time()
    start_utc = dt.datetime.fromisoformat(f"{date}T{hour_dt.strftime('%H:%M')}:00+00:00")
    end_utc = start_utc + dt.timedelta(hours=1)

    df = pd.read_parquet(events_parquet)
    if "ts" not in df.columns:
        raise ValueError("events.parquet must contain a 'ts' timestamp column")
    ts_all = pd.to_datetime(df["ts"], utc=True)
    mask = (ts_all >= start_utc) & (ts_all < end_utc)
    df = df.loc[mask].copy()
    if df.empty:
        raise ValueError(f"No events in the selected window: {start_utc} .. {end_utc}")

    # Normalize columns for replay
    df["ts_ns"] = ts_all.loc[mask].astype("int64")  # ns since epoch
    df["type"] = df["type"].astype(str).str.lower()
    side_map = {"b": "B", "bid": "B", "buy": "B", "a": "A", "ask": "A", "sell": "A", "s": "A"}
    df["side"] = df["side"].astype(str).str.lower().map(side_map).fillna("A")
    csv_events = tmp_dir / "events_window.csv"
    df[["ts_ns", "type", "side", "price", "qty"]].to_csv(csv_events, index=False)

    # 2) replay_tool
    from pathlib import Path
    import os, shutil

    candidates = [
        Path("build/cpp/replay_tool"),
        Path("build/cpp/tools/replay_tool"),
        Path(shutil.which("replay_tool") or "/nonexistent"),
    ]
    replay = next((p for p in candidates if p.exists() and os.access(p, os.X_OK)), None)
    if not replay:
        raise FileNotFoundError(
        "replay_tool not found or not executable. Tried:\n  " +
        "\n  ".join(map(str, candidates))
    )
    if not _bin_ok(replay):
        raise FileNotFoundError(f"replay_tool not found or not executable: {replay}")

    quotes_csv = tmp_dir / "taq_quotes.csv"
    trades_csv = tmp_dir / "taq_trades.csv"
    _run([
        str(replay),
        "--file", str(csv_events),
        "--speed", speed,
        "--cadence-ms", str(cadence_ms),
        "--quotes-out", str(quotes_csv),
        "--trades-out", str(trades_csv),
    ])

    # 3) metrics
    analytics_dir = tmp_dir / "analytics"
    plots_dir = analytics_dir / "plots"
    _ensure_dir(plots_dir)

    # depth parquet path (optional)
    depth_path = depth_top10 or (Path("recon") / f"{date}" / exchange / symbol / "top10_depth.parquet")

    metrics_cmd = [
        sys.executable, "-m", "olob.metrics",
        "--quotes", str(quotes_csv),
        "--out-json", str(analytics_dir / "summary.json"),
        "--plots-out", str(plots_dir),
    ]
    if depth_path.exists():
        metrics_cmd += ["--depth-top10", str(depth_path)]
    else:
        print(f"[info] depth file not found: {depth_path} — running metrics without L2 depth")
    _run(metrics_cmd)

    # 3b) microstructure (robust: do not fail the whole report if this errors)
    micro_json = analytics_dir / "microstructure_summary.json"
    micro_cmd = [
        sys.executable, "-m", "olob.microstructure",
        "--quotes", str(quotes_csv),
        "--trades", str(trades_csv),
        "--plots-out", str(plots_dir),
        "--out-json", str(micro_json),
        "--bar-sec", "60",
        "--rv-window", "30",
        "--impact-horizons-ms", "500,1000",
        "--autocorr-max-lag", "50",
        "--drift-grid-ms", "1000",
    ]
    if depth_path.exists():
        micro_cmd += ["--depth-top10", str(depth_path)]
    else:
        print(f"[info] microstructure: no L2 depth at {depth_path}; continuing")

    try:
        _run(micro_cmd)
    except Exception as e:
        print(f"[warn] microstructure step skipped: {e}")

    # 4) build self-contained HTML
    summary_json = analytics_dir / "summary.json"
    summary: dict = {}
    if summary_json.exists():
        with open(summary_json, "r") as f:
            summary = json.load(f)
    if micro_json.exists():
        try:
            with open(micro_json, "r") as f:
                micro = json.load(f)
            summary["microstructure"] = micro
        except Exception:
            pass

    figs = {
        "spread": _embed_png_b64(plots_dir / "spread.png"),
        "mid_microprice": _embed_png_b64(plots_dir / "mid_microprice.png"),
        "imbalance_L1": _embed_png_b64(plots_dir / "imbalance_L1.png"),
        "depth_bid": _embed_png_b64(plots_dir / "depth_bid.png"),
        "depth_ask": _embed_png_b64(plots_dir / "depth_ask.png"),
        # microstructure extras (if present)
        "vol": _embed_png_b64(plots_dir / "vol.png"),
        "impact": _embed_png_b64(plots_dir / "impact.png"),
        "oflow_autocorr": _embed_png_b64(plots_dir / "oflow_autocorr.png"),
        "drift_vs_imbalance": _embed_png_b64(plots_dir / "drift_vs_imbalance.png"),
        "impact_clusters": _embed_png_b64(plots_dir / "impact_clusters.png"),
    }

    title = f"{date} {symbol} — {hour_start}–{(dt.datetime.strptime(hour_start, '%H:%M') + dt.timedelta(hours=1)).strftime('%H:%M')} UTC"
    html = _render_html(
        title=title,
        exchange=exchange,
        symbol=symbol,
        date=date,
        hour_start=hour_start,
        summary=summary,
        figs=figs,
    )
    out_file = out_reports_dir / f"{date}_{symbol}.html"
    out_file.write_text(html, encoding="utf-8")
    return out_file

def _render_html(*, title: str, exchange: str, symbol: str, date: str, hour_start: str, summary: dict, figs: dict) -> str:
    def img(tag: str, label: str) -> str:
        uri = figs.get(tag)
        if not uri:
            return f'<div class="card"><h3>{label}</h3><p class="muted">Not available</p></div>'
        return f'<div class="card"><h3>{label}</h3><img src="{uri}" alt="{label}" /></div>'

    pretty_json = json.dumps(summary, indent=2) if summary else "{}"

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>LOB Report — {title}</title>
<style>
:root {{
  --bg: #0b0d10; --fg: #e8eef2; --muted:#9aa4ad; --card:#12161a; --accent:#6bc2ff;
}}
*{{box-sizing:border-box}}
body{{margin:0;padding:24px;background:var(--bg);color:var(--fg);font:14px/1.5 system-ui,Segoe UI,Roboto}}
h1{{margin:0 0 8px 0;font-size:20px}}
h2{{margin:16px 0 8px 0;font-size:16px;color:var(--accent)}}
h3{{margin:0 0 8px 0;font-size:14px}}
.header{{display:flex;justify-content:space-between;align-items:last baseline;gap:16px;flex-wrap:wrap}}
.meta{{color:var(--muted)}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:16px;margin-top:16px}}
.card{{background:var(--card);border:1px solid #1c2228;border-radius:12px;padding:12px}}
img{{width:100%;height:auto;border-radius:8px;border:1px solid #1c2228;background:#000}}
pre{{background:#0e1116;border:1px solid #1c2228;border-radius:12px;padding:12px;overflow:auto}}
.kv{{display:grid;grid-template-columns:max-content 1fr;gap:6px 12px}}
.kv div.k{{color:var(--muted)}}
hr{{border:0;border-top:1px solid #1c2228;margin:16px 0}}
.muted{{color:var(--muted)}}
.badge{{display:inline-block;padding:2px 8px;border:1px solid #1c2228;border-radius:999px;background:#0f1419;color:var(--fg);font-size:12px}}
</style>
</head>
<body>
  <div class="header">
    <div>
      <h1>LOB Report — {symbol}</h1>
      <div class="meta">{exchange} · {date} · Window: {hour_start}–{(dt.datetime.strptime(hour_start,'%H:%M') + dt.timedelta(hours=1)).strftime('%H:%M')} UTC</div>
    </div>
    <div><span class="badge">Self-contained HTML</span></div>
  </div>

  <h2>Key Charts</h2>
  <div class="grid">
    {img('spread', 'Spread over time')}
    {img('mid_microprice', 'Mid vs Microprice')}
    {img('imbalance_L1', 'Best-level imbalance (L1)')}
    {img('depth_bid', 'Top-10 Bid Depth')}
    {img('depth_ask', 'Top-10 Ask Depth')}
  </div>

  <h2>Microstructure (if computed)</h2>
  <div class="grid">
    {img('vol', 'Realized Volatility')}
    {img('impact', 'Impact Curves')}
    {img('oflow_autocorr', 'Order-Flow Autocorr')}
    {img('drift_vs_imbalance', 'Drift vs Imbalance')}
    {img('impact_clusters', 'Impact Clusters')}
  </div>

  <h2>Stats JSON</h2>
  <div class="card"><pre>{pretty_json}</pre></div>

  <hr/>
  <div class="muted">Generated by <strong>lob analyze</strong></div>
</body>
</html>
"""

# ---------------------------
# CLI
# ---------------------------

def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="lob analyze", description="Generate a self-contained HTML report with plots + stats.")
    ap.add_argument("--exchange", default="binanceus")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--date", help="UTC date folder (YYYY-MM-DD). Default = yesterday (local).")
    ap.add_argument("--hour-start", default="10:00", help="UTC hour start (HH:MM). Default 10:00.")
    ap.add_argument("--parquet-dir", default="parquet", help="Root of normalized parquet captures.")
    ap.add_argument("--build-dir", default="build", help="CMake build directory (for replay_tool).")
    ap.add_argument("--out-reports", default="out/reports", help="Where to write the HTML report.")
    ap.add_argument("--tmp", default="out/tmp_report", help="Scratch workspace (safe to delete).")
    ap.add_argument("--cadence-ms", type=int, default=50, help="Replay sampling cadence (ms).")
    ap.add_argument("--speed", default="50x", help="Replay speed (ignored for offline, but required by tool).")
    ap.add_argument("--depth-top10", help="Optional path to L2 top-10 depth parquet (if available).")
    args = ap.parse_args(argv)

    date = args.date or _yesterday_utc_date()

    try:
        out = run_pipeline(
            exchange=args.exchange,
            symbol=args.symbol,
            date=date,
            hour_start=args.hour_start,
            parquet_dir=Path(args.parquet_dir),
            build_dir=Path(args.build_dir),
            out_reports_dir=Path(args.out_reports),
            tmp_dir=Path(args.tmp),
            cadence_ms=args.cadence_ms,
            speed=args.speed,
            depth_top10=Path(args.depth_top10) if args.depth_top10 else None,
        )
        print(f"[report] wrote {out}")
        # Tidy tmp (best effort)
        try:
            shutil.rmtree(args.tmp, ignore_errors=True)
        except Exception:
            pass
        return 0
    except Exception as e:
        print(f"[error] {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
