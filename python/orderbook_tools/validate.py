from __future__ import annotations
import os, gzip, json, argparse, glob
from typing import List, Dict, Any, Tuple
from decimal import Decimal

try:
    import pandas as pd
except Exception:
    pd = None

def _open_text(path: str):
    return gzip.open(path, "rt") if path.endswith(".gz") else open(path, "rt")

def _load_json_file(path: str) -> Any:
    with _open_text(path) as f:
        return json.load(f)

def _recursive_globs(root: str, patterns: List[str]) -> List[str]:
    out: List[str] = []
    for pat in patterns:
        out += glob.glob(os.path.join(root, "**", pat), recursive=True)
    return sorted(set(out))

def _scan_snapshots(raw_dir: str, date: str, exchange: str, symbol: str,
                    snap_glob: str | None, debug: bool) -> List[str]:
    root = os.path.join(raw_dir, date, exchange, symbol)
    if not os.path.isdir(root):
        raise RuntimeError(f"Raw root not found: {root}")
    pats = [snap_glob] if snap_glob else ["*snapshot*.json*", "*book_snapshot*.json*", "*rest*book*.json*", "*snap*.json*"]
    snaps = _recursive_globs(root, pats)
    if debug:
        print(f"[validate] search root: {root}")
        print(f"[validate] snapshot patterns: {pats}")
        print(f"[validate] found snapshots: {len(snaps)}")
        for p in snaps[:10]:
            print("  snap:", os.path.relpath(p, root))
        if len(snaps) > 10: print("  ...")
    if not snaps:
        raise RuntimeError("No REST snapshots found for validation.")
    return snaps

def _unwrap(obj: Any) -> Any:
    if isinstance(obj, dict):
        if "data" in obj and isinstance(obj["data"], dict):
            return _unwrap(obj["data"])
        if "result" in obj and isinstance(obj["result"], dict):
            return _unwrap(obj["result"])
        return obj
    return obj

def _parse_snapshot_best(path: str) -> Tuple[int, Decimal, Decimal]:
    obj = _unwrap(_load_json_file(path))
    last_id = int(obj["lastUpdateId"])
    bids = obj.get("bids", []); asks = obj.get("asks", [])
    if not bids or not asks:
        raise RuntimeError(f"Empty snapshot bids/asks: {path}")
    best_bid = Decimal(bids[0][0]); best_ask = Decimal(asks[0][0])
    return last_id, best_bid, best_ask

def _load_states(recon_root: str, date: str, exchange: str, symbol: str):
    root = os.path.join(recon_root, date, exchange, symbol)
    pq = os.path.join(root, "reconstructed_states.parquet")
    jl = os.path.join(root, "reconstructed_states.jsonl")
    if pd is not None and os.path.exists(pq):
        return pd.read_parquet(pq)
    if os.path.exists(jl):
        if pd is None: raise RuntimeError("pandas required to load jsonl.")
        rows = [json.loads(x) for x in open(jl, "r")]
        return pd.DataFrame(rows)
    raise FileNotFoundError(f"Reconstruction output not found in {root}")

def validate(raw_dir: str, recon_dir: str, date: str, exchange: str, symbol: str,
             tick_size: float, window_min: int = 30, report_out: str = "docs/validator_report.json",
             levels: int = 10, snap_glob: str | None = None, debug: bool = False):
    if pd is None:
        raise RuntimeError("pandas is required for validation.")
    tick = Decimal(str(tick_size))
    df = _load_states(recon_dir, date, exchange, symbol)

    if df.empty or not {"event_time_ms","seq_u"}.issubset(df.columns):
        raise RuntimeError(
            "Reconstruction produced no events or missing columns. "
            "Run reconstruction again with --debug --peek to verify the diff format is recognized."
        )

    df = df.sort_values(by=["event_time_ms","seq_u"])
    t0 = df["event_time_ms"].min()
    t_end = t0 + window_min * 60_000

    snap_paths = _scan_snapshots(raw_dir, date, exchange, symbol, snap_glob, debug)
    checks = []
    for p in snap_paths:
        try:
            last_id, bb, aa = _parse_snapshot_best(p)
            state = df[(df["seq_u"] >= last_id) & (df["event_time_ms"] <= t_end)]
            if state.empty: continue
            st = state.iloc[-1]
            if st.isnull().any(): continue

            bid_ticks_recon = int(st["best_bid_ticks"]) if st["best_bid_ticks"] == st["best_bid_ticks"] else None
            ask_ticks_recon = int(st["best_ask_ticks"]) if st["best_ask_ticks"] == st["best_ask_ticks"] else None
            if bid_ticks_recon is None or ask_ticks_recon is None: continue

            bid_ticks_snap = int((bb / tick).quantize(Decimal("1.")))
            ask_ticks_snap = int((aa / tick).quantize(Decimal("1.")))
            drift = max(abs(bid_ticks_recon - bid_ticks_snap), abs(ask_ticks_recon - ask_ticks_snap))

            checks.append({
                "snapshot_file": os.path.basename(p),
                "lastUpdateId": last_id,
                "recon_bid_ticks": bid_ticks_recon,
                "recon_ask_ticks": ask_ticks_recon,
                "snap_bid_ticks": bid_ticks_snap,
                "snap_ask_ticks": ask_ticks_snap,
                "drift_ticks": int(drift),
                "applied": bool(st["applied"]),
                "drops": int(st["drops"]),
                "gaps": int(st["gaps"]),
                "resyncs": int(st["resyncs"]),
                "event_time_ms": int(st["event_time_ms"]),
            })
        except Exception:
            continue

    if not checks:
        raise RuntimeError("No comparable checkpoints found for validator window.")

    vd = pd.DataFrame(checks).sort_values(by="event_time_ms")
    max_drift = int(vd["drift_ticks"].max())
    mean_drift = float(vd["drift_ticks"].mean())
    acceptance_passed = (max_drift < 1)

    os.makedirs(os.path.dirname(report_out), exist_ok=True)
    with open(report_out, "w") as f:
        json.dump({
            "date": date,
            "exchange": exchange,
            "symbol": symbol,
            "tick_size": float(tick_size),
            "window_minutes": window_min,
            "levels_for_checksum": levels,
            "max_drift_ticks": max_drift,
            "mean_drift_ticks": mean_drift,
            "acceptance_passed": bool(acceptance_passed),
            "checkpoints": int(len(vd)),
        }, f, indent=2)

    csv_out = report_out.replace(".json", ".csv")
    vd.to_csv(csv_out, index=False)
    return report_out, csv_out

def main():
    ap = argparse.ArgumentParser(description="Validator: compare reconstructed L2 vs REST snapshots.")
    ap.add_argument("--raw-dir", required=True)
    ap.add_argument("--recon-dir", default="recon")
    ap.add_argument("--date", required=True)
    ap.add_argument("--exchange", required=True)
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--tick-size", type=float, default=0.01)
    ap.add_argument("--window-min", type=int, default=30)
    ap.add_argument("--levels", type=int, default=10)
    ap.add_argument("--snap-glob", default=None, help="Override snapshot glob, e.g. '*depth/snapshot-*.json.gz'")
    ap.add_argument("--report-out", default="docs/validator_report.json")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    rep, csvp = validate(args.raw_dir, args.recon_dir, args.date, args.exchange, args.symbol,
                         args.tick_size, args.window_min, args.report_out, args.levels,
                         snap_glob=args.snap_glob, debug=args.debug)
    print(f"Wrote validator report: {rep}\nDetails CSV: {csvp}")

if __name__ == "__main__":
    main()
