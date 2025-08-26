#!/usr/bin/env python3
from __future__ import annotations
import argparse, csv, json, os, shlex, subprocess, sys, tempfile
from pathlib import Path
import pandas as pd
import yaml


def _read_quotes_window(quotes_csv: str) -> tuple[pd.Timestamp, pd.Timestamp, int]:
    q = pd.read_csv(quotes_csv)
    if "ts_ns" in q.columns:
        ts = pd.to_datetime(q["ts_ns"], unit="ns", utc=True)
    elif "ts" in q.columns:
        ts = pd.to_datetime(q["ts"], utc=True)
    else:
        for c in ["timestamp", "time", "time_ns", "timestamp_ns"]:
            if c in q.columns:
                ts = pd.to_datetime(q[c], utc=True)
                break
        else:
            raise ValueError("quotes CSV needs ts_ns/ts/timestamp/time column")
    t0, t1 = ts.min(), ts.max()
    return t0, t1, len(q)


def _make_tmp_yaml(base_yaml: Path,
                   name: str,
                   start_iso: str,
                   end_iso: str,
                   overrides: dict) -> str:
    base = {}
    if base_yaml and base_yaml.exists():
        base = yaml.safe_load(base_yaml.read_text())
        if base is None:
            base = {}
    # core
    base["name"] = name
    base["start"] = start_iso
    base["end"] = end_iso

    # ensure cost defaults
    cost = dict(base.get("cost", {}))
    # sensible default lot/tick to avoid quantizing to 0
    cost.setdefault("lot_size", 0.01)
    cost.setdefault("tick_size", 0.01)
    cost.setdefault("taker_bps", 2.0)
    cost.setdefault("maker_bps", 0.0)
    cost.setdefault("fixed_latency_ms", 50)
    base["cost"] = cost

    # apply specific overrides for each strategy
    for k, v in overrides.items():
        base[k] = v

    # write tmp
    fd, path = tempfile.mkstemp(prefix=f"cmp_tmp_{name}_", suffix=".yaml")
    os.close(fd)
    Path(path).write_text(yaml.safe_dump(base, sort_keys=False))
    return path


def _run_lob(strategy_yaml: Path, quotes: Path, trades: Path, outdir: Path, label: str, seed: int = 17) -> tuple[str, Path]:
    outdir.mkdir(parents=True, exist_ok=True)
    cmd = (
        f"lob backtest "
        f"--strategy {strategy_yaml} "
        f"--quotes {quotes} "
        f"--trades {trades} "
        f"--out {outdir} "
        f"--seed {seed}"
    )
    print("[run]", cmd)
    subprocess.run(shlex.split(cmd), check=True)

    # summary file name is derived from strategy 'name' inside the YAML;
    # we can find it by scanning for *_summary.json in outdir
    summaries = sorted(outdir.glob("*_summary.json"))
    if not summaries:
        # try fallback fixed names
        fallback = outdir / f"{label}_summary.json"
        if not fallback.exists():
            raise FileNotFoundError(f"No summary JSON found in {outdir}")
        summary_file = fallback
    else:
        summary_file = summaries[0]

    return str(summary_file), outdir


def main():
    ap = argparse.ArgumentParser(description="Compare TWAP / VWAP / POV / Iceberg over the same hour.")
    ap.add_argument("--quotes", required=True, help="TAQ quotes CSV")
    ap.add_argument("--trades", required=True, help="TAQ trades CSV (for VWAP/POV volume)")
    ap.add_argument("--outdir", required=True, help="Output root for runs + comparison.csv")
    ap.add_argument("--twap", required=True, help="TWAP YAML (base)")
    ap.add_argument("--vwap", required=True, help="VWAP YAML (base)")
    ap.add_argument("--pov", required=True, help="POV YAML (base)")
    ap.add_argument("--iceberg", required=True, help="Iceberg YAML (base)")
    args = ap.parse_args()

    quotes = Path(args.quotes)
    trades = Path(args.trades)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    t0, t1, nrows = _read_quotes_window(str(quotes))
    print(f"quotes UTC range: {t0} → {t1} rows: {nrows}")

    # use full quotes window but clamp to 1 hour if longer
    # (if you captured exactly an hour, this is effectively [t0, t1])
    # pane: leave as-is; your capture was one hour already
    start_iso = t0.isoformat()
    end_iso   = t1.isoformat()

    # Build temp yaml per strategy with **working defaults**.
    twap_tmp = _make_tmp_yaml(
        Path(args.twap),
        name="twap_btcusdt_hour",
        start_iso=start_iso,
        end_iso=end_iso,
        overrides={
            "type": "twap",
            "side": "buy",
            "qty": 5.0,
            "bar_sec": 60,
            "min_clip": 0.05,        # > lot_size, avoids zeroing
            "cooldown_ms": 0,
            "force_taker": True,
        },
    )
    vwap_tmp = _make_tmp_yaml(
        Path(args.vwap),
        name="vwap_btcusdt_hour",
        start_iso=start_iso,
        end_iso=end_iso,
        overrides={
            "type": "vwap",
            "side": "buy",
            "qty": 5.0,
            "bar_sec": 60,
            "min_clip": 0.05,
            "cooldown_ms": 0,
            "force_taker": True,
        },
    )
    pov_tmp = _make_tmp_yaml(
        Path(args.pov),
        name="pov_btcusdt_hour",
        start_iso=start_iso,
        end_iso=end_iso,
        overrides={
            "type": "pov",
            "side": "buy",
            "qty": 5.0,
            "bar_sec": 60,
            # IMPORTANT: larger target POV; small bar volumes + rounding otherwise → 0
            "target_pov": 0.20,      # 20% of bar volume
            "min_clip": 0.05,
            "cooldown_ms": 0,
            "force_taker": True,
        },
    )
    iceberg_tmp = _make_tmp_yaml(
        Path(args.iceberg),
        name="iceberg_btcusdt_hour",
        start_iso=start_iso,
        end_iso=end_iso,
        overrides={
            "type": "iceberg",
            "side": "buy",
            "qty": 5.0,
            "bar_sec": 60,
            "display": 0.20,         # 0.2 visible
            "replenish": 1.0,        # full replenish each bar
            "min_clip": 0.05,
            "cooldown_ms": 0,
            "force_taker": True,
        },
    )

    # Run backtests
    twap_summary, twap_dir = _run_lob(Path(twap_tmp), quotes, trades, outdir / "twap", label="twap")
    vwap_summary, vwap_dir = _run_lob(Path(vwap_tmp), quotes, trades, outdir / "vwap", label="vwap")
    pov_summary, pov_dir   = _run_lob(Path(pov_tmp), quotes, trades, outdir / "pov", label="pov")
    iceberg_summary, iceberg_dir = _run_lob(Path(iceberg_tmp), quotes, trades, outdir / "iceberg", label="iceberg")

    # Load summaries (be tolerant about fields)
    def _load(js):
        d = json.loads(Path(js).read_text())
        # normalize missing to zero for the comparison row
        return {
            "filled_qty": float(d.get("filled_qty") or 0.0),
            "avg_px": d.get("avg_px"),
            "notional": float(d.get("notional") or 0.0),
            "fees": float(d.get("fees") or 0.0),
            "signed_cost": float(d.get("signed_cost") or 0.0),
        }

    rows = []
    for name, sf, run_dir in [
        ("twap", twap_summary, twap_dir),
        ("vwap", vwap_summary, vwap_dir),
        ("pov", pov_summary, pov_dir),
        ("iceberg", iceberg_summary, iceberg_dir),
    ]:
        d = _load(sf)
        # try to enrich from risk_summary.json if exists
        risk_json = next(run_dir.glob("risk_summary.json"), None)
        risk = {}
        if risk_json and risk_json.exists():
            risk = json.loads(risk_json.read_text())
        rows.append({
            "strategy": name,
            **d,
            "pnl_total": float(risk.get("pnl_total") or 0.0),
            "max_drawdown": float(risk.get("max_drawdown") or 0.0),
            "turnover": float(risk.get("turnover") or 0.0),
            "sharpe_like": float(risk.get("sharpe_like") or 0.0),
        })

    cmp_df = pd.DataFrame(rows, columns=[
        "strategy","filled_qty","avg_px","notional","fees","signed_cost",
        "pnl_total","max_drawdown","turnover","sharpe_like"
    ])
    out_csv = outdir / "comparison.csv"
    cmp_df.to_csv(out_csv, index=False)
    print(f"[ok] wrote {out_csv}\n")
    with pd.option_context("display.max_columns", None, "display.width", 160):
        print(cmp_df.to_string(index=False))


if __name__ == "__main__":
    sys.exit(main())
