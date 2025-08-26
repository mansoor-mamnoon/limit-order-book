#!/usr/bin/env python3
"""
Microstructure analytics:
- Realized volatility on mid (Parkinson & Garman–Klass, on rolling OHLC of mid)
- Impact curves: future mid move vs. trade size buckets (notional + percentiles)
- Order-flow autocorrelation (signed trades)
- Short-horizon drift vs. (L1) imbalance deciles

Inputs:
  Quotes (CSV/Parquet): ts or ts_ns (ns), bid, ask, [mid], [spread], [bid_sz], [ask_sz]
  Trades (CSV/Parquet): ts or ts_ns (ns), price, qty, [side in {B,A,buy,sell,1,-1}]
  Depth (Parquet, optional): top-of-book for L1 imbalance
    - wide: bid_px1..bid_px10, bid_qty1..bid_qty10, ask_px1..ask_px10, ask_qty1..ask_qty10
    - tidy: ts/ts_ns, side∈{B,A}, level, price, qty  (uses level==1)

Outputs:
  PNGs: vol.png, impact.png, oflow_autocorr.png, drift_vs_imbalance.png
  JSON: microstructure_summary.json
"""
from __future__ import annotations
import argparse, json, math
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# -----------------------------
# Helpers
# -----------------------------
def _load_any(path: str | Path) -> pd.DataFrame:
    p = str(path)
    if p.endswith(".parquet"):
        return pd.read_parquet(p)
    return pd.read_csv(p)

def _as_ns(col: pd.Series) -> pd.Series:
    if np.issubdtype(col.dtype, np.integer):
        return col.astype("int64")
    ts = pd.to_datetime(col, utc=True, errors="coerce")
    if ts.isna().all():
        return pd.to_numeric(col, errors="coerce").astype("Int64").astype("int64")
    return ts.view("int64")

def _ensure_mid(dfq: pd.DataFrame) -> pd.DataFrame:
    if "mid" in dfq.columns:
        return dfq
    if {"bid","ask"}.issubset(dfq.columns):
        dfq["mid"] = 0.5*(dfq["bid"].astype(float)+dfq["ask"].astype(float))
        return dfq
    raise ValueError("Quotes need 'mid' or both 'bid' and 'ask'.")

def _maybe_bidask_sizes(dfq: pd.DataFrame) -> tuple[pd.Series|None, pd.Series|None]:
    bs = None
    if "bid_sz" in dfq.columns: bs = dfq["bid_sz"].astype(float)
    if "bid_size" in dfq.columns: bs = dfq["bid_size"].astype(float) if bs is None else bs
    if "bid_qty" in dfq.columns: bs = dfq["bid_qty"].astype(float) if bs is None else bs
    asz = None
    if "ask_sz" in dfq.columns: asz = dfq["ask_sz"].astype(float)
    if "ask_size" in dfq.columns: asz = dfq["ask_size"].astype(float) if asz is None else asz
    if "ask_qty" in dfq.columns: asz = dfq["ask_qty"].astype(float) if asz is None else asz
    return bs, asz

def _infer_trade_sign(dft: pd.DataFrame, mid_at_trade: pd.Series) -> pd.Series:
    for c in ["side","Side","is_buyer_maker","buyer_maker","direction","dir"]:
        if c in dft.columns:
            col = dft[c]
            if c in ("is_buyer_maker","buyer_maker"):
                return np.where(col.astype(bool), -1, 1).astype(int)
            s = col.astype(str).str.lower()
            if set(s.unique()) & {"b","buy","a","ask","sell","s","1","+1","-1"}:
                return np.where(s.isin(["b","buy","1","+1"]), 1, -1).astype(int)
    sign = np.sign(dft["price"].astype(float) - mid_at_trade.astype(float))
    s = pd.Series(sign, index=dft.index).replace(0, np.nan).ffill().bfill().fillna(0).astype(int)
    s[s==0] = 1
    return s

def _join_mid_at(dft: pd.DataFrame, dfq: pd.DataFrame) -> pd.Series:
    q = dfq[["ts_ns", "mid"]].dropna().copy()
    q["ts_ns"] = q["ts_ns"].astype("int64")
    q = q.sort_values("ts_ns")
    q["mid"] = q["mid"].astype(float).ffill()

    tmp = dft[["ts_ns"]].copy()
    tmp["ts_ns"] = tmp["ts_ns"].astype("int64")
    tmp["_orig_idx"] = tmp.index

    merged = pd.merge_asof(
        tmp.sort_values("ts_ns"), q,
        on="ts_ns", direction="backward", allow_exact_matches=True
    )
    mid_series = pd.Series(merged["mid"].values, index=merged["_orig_idx"])
    return mid_series.reindex(dft.index)

def _compute_imbalance_from_depth(df_depth: pd.DataFrame) -> pd.DataFrame:
    dd = df_depth.copy()
    ts_col = "ts_ns" if "ts_ns" in dd.columns else "ts"
    dd["ts_ns"] = _as_ns(dd[ts_col])

    cols = {c.lower(): c for c in dd.columns}
    candidate_pairs = [
        ("bid_qty1","ask_qty1"), ("bid_sz1","ask_sz1"),
        ("bid_size1","ask_size1"), ("b1_qty","a1_qty"),
        ("b1_size","a1_size"), ("bidqty1","askqty1"),
        ("bid_quantity1","ask_quantity1"),
    ]
    for lb, la in candidate_pairs:
        if lb in cols and la in cols:
            b = dd[cols[lb]].astype(float)
            a = dd[cols[la]].astype(float)
            out = pd.DataFrame({"ts_ns": dd["ts_ns"], "imbalance_l1": b / np.maximum(b + a, 1e-12)})
            return out.dropna().sort_values("ts_ns")

    if {"side","level","qty"}.issubset(dd.columns):
        lvl1 = dd[dd["level"]==1].copy()
        lvl1["side"] = lvl1["side"].astype(str).str.upper().map({"B":"B","BID":"B","A":"A","ASK":"A"})
        pivot = lvl1.pivot_table(index="ts_ns", columns="side", values="qty", aggfunc="last")
        if "B" in pivot.columns and "A" in pivot.columns:
            pivot = pivot.sort_index().ffill()
            pivot["imbalance_l1"] = pivot["B"] / np.maximum(pivot["B"] + pivot["A"], 1e-12)
            return pivot.reset_index()[["ts_ns","imbalance_l1"]].dropna()

    raise ValueError("Could not find L1 bid/ask quantities in depth file.")

def _parkinson_sigma(h: pd.Series, l: pd.Series, window: int) -> pd.Series:
    x = np.log(h/l).pow(2)
    coef = 1.0/(4.0*np.log(2.0))
    return (coef * x.rolling(window, min_periods=max(2,window//5)).mean()).pow(0.5)

def _garman_klass_sigma(o: pd.Series, h: pd.Series, l: pd.Series, c: pd.Series, window: int) -> pd.Series:
    hl = np.log(h/l).pow(2)
    co = np.log(c/o).pow(2)
    var_bar = 0.5*hl - (2*np.log(2)-1.0)*co
    return var_bar.rolling(window, min_periods=max(2,window//5)).mean().clip(lower=0).pow(0.5)

def _bp(x: pd.Series) -> pd.Series:
    return 1e4 * x

def _percentile_bins(x: pd.Series, pct=[0,20,40,60,80,90,95,99,100]) -> pd.Categorical:
    qs = np.nanpercentile(x, pct)
    qs = np.unique(qs)
    for i in range(1, len(qs)):
        if qs[i] <= qs[i-1]:
            qs[i] = qs[i-1] + 1e-12
    return pd.cut(x, bins=qs, include_lowest=True, duplicates="drop")

def _future_mid_series(ts_ns_array: np.ndarray, quotes_idx: np.ndarray, mid_values: np.ndarray, horizon_ms: int) -> np.ndarray:
    targets = ts_ns_array.astype("int64") + horizon_ms * 1_000_000
    q = pd.DataFrame({"ts_ns": quotes_idx.astype("int64"), "mid": mid_values})
    t = pd.DataFrame({"ts_ns": targets})
    merged = pd.merge_asof(t.sort_values("ts_ns"), q.sort_values("ts_ns"),
                           on="ts_ns", direction="backward", allow_exact_matches=True)
    return merged["mid"].to_numpy()

# -----------------------------
# Main analytics
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Microstructure analytics (volatility, impact, order-flow, drift).")
    ap.add_argument("--quotes", required=True, help="Quotes CSV/Parquet (best bid/ask samples).")
    ap.add_argument("--trades", required=True, help="Trades CSV/Parquet (prints).")
    ap.add_argument("--depth-top10", default=None, help="(Optional) reconstructed depth Parquet for imbalance (top10 or tidy).")
    ap.add_argument("--plots-out", required=True, help="Directory to save PNG charts.")
    ap.add_argument("--out-json", required=True, help="Path to write JSON summary.")
    ap.add_argument("--bar-sec", type=float, default=60.0, help="Bar size in seconds for OHLC of mid (default: 60s).")
    ap.add_argument("--rv-window", type=int, default=30, help="Rolling window (bars) for Parkinson/GK.")
    ap.add_argument("--impact-horizons-ms", type=str, default="1000,5000", help="Comma-separated horizons in ms for impact and drift.")
    ap.add_argument("--autocorr-max-lag", type=int, default=50, help="Max lag (trades) for order-flow autocorrelation.")
    ap.add_argument("--drift-grid-ms", type=int, default=100, help="Grid resolution (ms) for drift vs. imbalance computation.")
    ap.add_argument("--debug-out", type=str, default="", help="Optional directory to write drift diagnostics (parquet/csv).")
    args = ap.parse_args()

    plots_dir = Path(args.plots_out); plots_dir.mkdir(parents=True, exist_ok=True)
    out_json = Path(args.out_json); out_json.parent.mkdir(parents=True, exist_ok=True)

    # ---------- Quotes ----------
    dfq = _load_any(args.quotes).copy()
    tsq = "ts_ns" if "ts_ns" in dfq.columns else "ts"
    dfq["ts_ns"] = _as_ns(dfq[tsq])
    dfq = _ensure_mid(dfq)
    dfq = dfq.sort_values("ts_ns").drop_duplicates("ts_ns")

    # Cadence stats (quotes)
    q_diffs = np.diff(np.sort(dfq["ts_ns"].values.astype("int64")))
    if len(q_diffs):
        med_ms = float(np.median(q_diffs) / 1e6)
        p90_ms = float(np.percentile(q_diffs, 90) / 1e6)
        print(f"[cadence] quotes median Δ={med_ms:.1f} ms, p90 Δ={p90_ms:.1f} ms, rows={len(dfq)}")
    else:
        print("[cadence] quotes have <2 rows")

    # OHLC bars from mid
    q_ts = pd.to_datetime(dfq["ts_ns"], utc=True)
    s = pd.Series(dfq["mid"].astype(float).values, index=q_ts, name="mid")
    o = s.resample(f"{int(args.bar_sec)}s").first()
    h = s.resample(f"{int(args.bar_sec)}s").max()
    l = s.resample(f"{int(args.bar_sec)}s").min()
    c = s.resample(f"{int(args.bar_sec)}s").last()
    ohlc = pd.DataFrame({"open":o,"high":h,"low":l,"close":c}).dropna()

    # Realized vol (annualized)
    parkinson = _parkinson_sigma(ohlc["high"], ohlc["low"], window=args.rv_window)
    gk = _garman_klass_sigma(ohlc["open"], ohlc["high"], ohlc["low"], ohlc["close"], window=args.rv_window)
    seconds_per_year = 252*6.5*3600
    scale = math.sqrt(seconds_per_year / args.bar_sec)
    parkinson_ann = parkinson * scale
    gk_ann = gk * scale

    # ---------- Trades ----------
    dft = _load_any(args.trades).copy()
    tst = "ts_ns" if "ts_ns" in dft.columns else "ts"
    dft["ts_ns"] = _as_ns(dft[tst])
    dft = dft.sort_values("ts_ns").dropna(subset=["price","qty"])
    dft["price"] = dft["price"].astype(float)
    dft["qty"]   = dft["qty"].astype(float)
    mid_at = _join_mid_at(dft, dfq[["ts_ns","mid"]])
    dft["mid"] = mid_at
    dft["sign"] = _infer_trade_sign(dft, mid_at)
    dft["notional"] = dft["price"] * dft["qty"]

    # ---------- Impact curves ----------
    horizons_ms = [int(x) for x in str(args.impact_horizons_ms).split(",") if x.strip()]
    impact_results: dict[int, dict[str, pd.Series]] = {}
    q_idx = dfq.set_index("ts_ns")[["mid"]].sort_index()
    for H in horizons_ms:
        fut = _future_mid_series(dft["ts_ns"].values, q_idx.index.values, q_idx["mid"].values, H)
        dft[f"ret_{H}ms"] = (fut - dft["mid"].values) / dft["mid"].values
        dft[f"bp_{H}ms"]  = _bp(dft[f"ret_{H}ms"])
        dft[f"notional_bucket_{H}ms"]   = pd.qcut(dft["notional"], q=10, duplicates="drop")
        dft[f"percentile_bucket_{H}ms"] = _percentile_bins(dft["qty"])
        g1 = dft.groupby(f"notional_bucket_{H}ms", observed=False)[f"bp_{H}ms"].mean()
        g2 = dft.groupby(f"percentile_bucket_{H}ms", observed=False)[f"bp_{H}ms"].mean()
        impact_results[H] = {"by_notional": g1, "by_qty_percentile": g2}

    # ---------- Order-flow autocorrelation ----------
    sgn = dft["sign"].astype(float).values
    maxlag = min(args.autocorr_max_lag, len(sgn)-2) if len(sgn) > 2 else 0
    lags = np.arange(1, maxlag+1, dtype=int)
    acorr = []
    if maxlag > 0:
        x = (sgn - sgn.mean())
        var = (x@x)
        for k in lags:
            num = (x[:-k] * x[k:]).sum()
            acorr.append(num/var if var > 0 else np.nan)
    acorr = np.array(acorr, dtype=float) if maxlag>0 else np.array([])

    # ---------- Drift vs. imbalance (uniform grid with diagnostics) ----------
    # Prefer depth -> L1 imbalance; else quote sizes; else neutral 0.5
    imb = None
    if args.depth_top10:
        try:
            df_depth = _load_any(args.depth_top10)
            print(f"[imb] depth rows: {len(df_depth)}")
            imb = _compute_imbalance_from_depth(df_depth)
            print(f"[imb] computed from depth, rows={len(imb)}")
        except Exception as e:
            print(f"[warn] depth provided but could not compute imbalance: {e}")
    if imb is None:
        bid_sz, ask_sz = _maybe_bidask_sizes(dfq)
        if bid_sz is not None and ask_sz is not None:
            qq = dfq[["ts_ns"]].copy()
            qq["imbalance_l1"] = (bid_sz / np.maximum(bid_sz + ask_sz, 1e-12)).values
            imb = qq[["ts_ns","imbalance_l1"]]
            print(f"[imb] computed from quote sizes, rows={len(imb)}")
        else:
            qq = dfq[["ts_ns"]].copy()
            qq["imbalance_l1"] = 0.5
            imb = qq
            print(f"[imb] using neutral 0.5 imbalance, rows={len(imb)}")

        # --- Normalize imbalance timestamps to ns and forward-fill ---
    def _normalize_to_ns(ts: pd.Series) -> pd.Series:
        ts = ts.astype("int64")
        # Heuristic by magnitude: s~1e9, ms~1e12, µs~1e15, ns~1e18 (2020s epoch)
        mx = int(np.log10(max(1, int(ts.max()))))
        if mx < 11:      # seconds
            scale = 1_000_000_000
        elif mx < 14:    # milliseconds
            scale = 1_000_000
        elif mx < 17:    # microseconds
            scale = 1_000
        else:            # already ns
            scale = 1
        if scale != 1:
            print(f"[imb] ts units appear < ns; upscaling by ×{scale}")
            ts = ts * scale
        return ts

    # If depth-derived imbalance is (nearly) constant, try quotes sizes as fallback
    try:
        span = float(imb["imbalance_l1"].max() - imb["imbalance_l1"].min())
    except Exception:
        span = 0.0
    if not np.isfinite(span) or span < 1e-6:
        bid_sz, ask_sz = _maybe_bidask_sizes(dfq)
        if bid_sz is not None and ask_sz is not None:
            print("[imb] depth imbalance is flat; falling back to quote sizes")
            qq = dfq[["ts_ns"]].copy()
            qq["imbalance_l1"] = (bid_sz / np.maximum(bid_sz + ask_sz, 1e-12)).values
            imb = qq
            # normalize & ffill again for the new source
            imb["ts_ns"] = _normalize_to_ns(imb["ts_ns"].astype("int64"))
            imb = imb.sort_values("ts_ns")
            imb["imbalance_l1"] = pd.to_numeric(imb["imbalance_l1"], errors="coerce").bfill().ffill()
            print(f"[imb] fallback (quotes) rows={len(imb)}")
        else:
            print("[imb] depth imbalance flat and no quote sizes available; plot will be a single bar.")


    # Print ranges before normalization (helps debug)
    try:
        print(f"[imb] ts_ns min/max (raw): {int(imb['ts_ns'].min())} .. {int(imb['ts_ns'].max())}")
        print(f"[qts] ts_ns min/max:      {int(dfq['ts_ns'].min())} .. {int(dfq['ts_ns'].max())}")
    except Exception:
        pass

    imb = imb.copy()
    imb["ts_ns"] = _normalize_to_ns(imb["ts_ns"])
    # Sort + ffill so sparse depth still covers the grid
    imb = imb.sort_values("ts_ns")
    imb["imbalance_l1"] = pd.to_numeric(imb["imbalance_l1"], errors="coerce")
    # If the first rows are NaN, ffill won't fill them; backfill once then ffill
    imb["imbalance_l1"] = imb["imbalance_l1"].bfill().ffill()

    # Show ranges after normalization
    try:
        print(f"[imb] ts_ns min/max (ns):  {int(imb['ts_ns'].min())} .. {int(imb['ts_ns'].max())}")
    except Exception:
        pass

    # --- Ensure imbalance covers the grid range (extend if non-overlapping) ---
    # We'll extend with the nearest available value so asof/backward can match.
    # This handles three cases: [imb << grid], [grid << imb], and partial overlap.
    def _extend_imbalance_to_cover(imb_df: pd.DataFrame, grid_start_ns: int, grid_end_ns: int) -> pd.DataFrame:
        if len(imb_df) == 0:
            return imb_df
        imb_df = imb_df.sort_values("ts_ns")
        imb_min = int(imb_df["ts_ns"].iloc[0]); imb_max = int(imb_df["ts_ns"].iloc[-1])
        rows = [imb_df]

        if imb_min > grid_start_ns:
            # grid starts earlier than first imbalance point -> prepend first value at grid_start_ns
            first_val = float(imb_df["imbalance_l1"].iloc[0])
            rows.append(pd.DataFrame({"ts_ns": [grid_start_ns], "imbalance_l1": [first_val]}))
            print(f"[imb] extended head to grid_start with first value ({first_val:.4f})")

        if imb_max < grid_end_ns:
            # grid ends after last imbalance point -> append last value at grid_end_ns
            last_val = float(imb_df["imbalance_l1"].iloc[-1])
            rows.append(pd.DataFrame({"ts_ns": [grid_end_ns], "imbalance_l1": [last_val]}))
            print(f"[imb] extended tail to grid_end with last value ({last_val:.4f})")

        out = pd.concat(rows, ignore_index=True).sort_values("ts_ns")
        # consolidate any duplicates
        out = out.drop_duplicates(subset=["ts_ns"], keep="last")
        return out

    
    
    
    
    # Params
    H_drift_ms = horizons_ms[0]
    drift_grid_ms = int(args.drift_grid_ms)

    # Build uniform time grid over the quotes range
    q_sorted = dfq[["ts_ns","mid"]].sort_values("ts_ns").dropna()
    print(f"[drift] quotes rows={len(q_sorted)}; grid={drift_grid_ms} ms; horizon={H_drift_ms} ms")

    if len(q_sorted) == 0:
        drift_by_decile = pd.Series(dtype=float)
        drift_df = pd.DataFrame(columns=["ts_ns","retH","imbalance_l1","imb_decile"])
    else:
        start_ns = int(q_sorted["ts_ns"].iloc[0])
        end_ns   = int(q_sorted["ts_ns"].iloc[-1])
        step_ns  = int(drift_grid_ms * 1_000_000)

        if (end_ns - start_ns) < step_ns:
            print("[drift] quotes span < one grid step; increase --drift-grid-ms or use smaller horizon.")
        grid_ns = np.arange(start_ns, end_ns + 1, step_ns, dtype=np.int64)

        imb = _extend_imbalance_to_cover(imb, grid_ns[0], grid_ns[-1])
        
        q_asof = pd.merge_asof(
            pd.DataFrame({"ts_ns": grid_ns}),
            q_sorted[["ts_ns","mid"]].rename(columns={"mid":"mid_q"}),
            on="ts_ns", direction="backward", allow_exact_matches=True
        )
        q_asof["mid_q"] = q_asof["mid_q"].ffill()
        before = len(q_asof)
        q_asof = q_asof.dropna(subset=["mid_q"])
        print(f"[drift] grid points={before}, usable mid points after ffill={len(q_asof)}")

        # future mid via shift(-k)print(f"[imb]
        k = max(1, int(round(H_drift_ms / drift_grid_ms)))
        q_asof["mid_fut"] = q_asof["mid_q"].shift(-k)
        q_asof["retH"] = (q_asof["mid_fut"] - q_asof["mid_q"]) / q_asof["mid_q"]
        valid_ret = q_asof["retH"].notna().sum()
        print(f"[drift] valid future returns={valid_ret} (drop tail ~k={k})")

        # asof-join imbalance to grid
        imb_idx = imb.dropna().sort_values("ts_ns")
        print(f"[drift] imbalance rows available={len(imb_idx)}")
        grid_imb = pd.merge_asof(
            q_asof[["ts_ns","retH"]].dropna(subset=["retH"]).sort_values("ts_ns"),
            imb_idx.sort_values("ts_ns"),
            on="ts_ns", direction="backward", allow_exact_matches=True
        )
        print(f"[drift] after join rows={len(grid_imb)} (dropna retH applied)")

        drift_df = grid_imb.dropna(subset=["imbalance_l1","retH"]).copy()
        print(f"[drift] after dropna(imbalance_l1,retH) rows={len(drift_df)}")

        if len(drift_df) == 0:
            drift_by_decile = pd.Series(dtype=float)
        else:
            imb_span = drift_df["imbalance_l1"].max() - drift_df["imbalance_l1"].min()
            print(f"[drift] imbalance span={imb_span:.6f} (0→flat)")
            if not np.isfinite(imb_span) or imb_span < 1e-9:
                drift_df["imb_decile"] = 0
            else:
                try:
                    drift_df["imb_decile"] = pd.qcut(
                        drift_df["imbalance_l1"], q=10, labels=False, duplicates="drop"
                    )
                except Exception:
                    drift_df["imb_decile"] = pd.cut(
                        drift_df["imbalance_l1"], bins=10, labels=False, include_lowest=True
                    )
            drift_by_decile = drift_df.groupby("imb_decile", observed=False)["retH"].mean().pipe(_bp)

    # Optional debug dumps
    if args.debug_out:
        dbg = Path(args.debug_out); dbg.mkdir(parents=True, exist_ok=True)
        try:
            q_asof.to_parquet(dbg / "drift_grid.parquet", index=False)
        except Exception:
            q_asof.to_csv(dbg / "drift_grid.csv", index=False)
        try:
            imb_idx.to_parquet(dbg / "imbalance.parquet", index=False)
        except Exception:
            imb_idx.to_csv(dbg / "imbalance.csv", index=False)
        try:
            drift_df.to_parquet(dbg / "drift_df.parquet", index=False)
        except Exception:
            drift_df.to_csv(dbg / "drift_df.csv", index=False)
        print(f"[debug] wrote diagnostics to {dbg}")

    # -----------------------------
    # Plotting
    # -----------------------------
    # 1) RV
    fig1 = plt.figure(figsize=(10,5))
    plt.plot(parkinson_ann.index, parkinson_ann.values, label="Parkinson (ann.)")
    plt.plot(gk_ann.index, gk_ann.values, label="Garman–Klass (ann.)")
    plt.title(f"Realized Volatility on Mid — bars={int(args.bar_sec)}s, window={args.rv_window}")
    plt.xlabel("Time"); plt.ylabel("Sigma (annualized)"); plt.legend()
    fig1.tight_layout(); fig1.savefig(plots_dir / "vol.png", dpi=150); plt.close(fig1)

    # 2) Impact
    fig2 = plt.figure(figsize=(10,5))
    for H in horizons_ms:
        g = impact_results[H]["by_notional"]
        x = np.arange(len(g))
        plt.plot(x, g.values, marker="o", label=f"{H} ms")
    plt.title("Impact Curve — Future Mid Move vs Trade Notional Buckets")
    plt.xlabel("Notional buckets (deciles)"); plt.ylabel("Avg Δ mid (bp)"); plt.legend()
    if len(horizons_ms)>0:
        n = len(impact_results[horizons_ms[0]]["by_notional"])
        plt.xticks(np.arange(n), [str(i) for i in range(n)])
    fig2.tight_layout(); fig2.savefig(plots_dir / "impact.png", dpi=150); plt.close(fig2)

    # 3) Order-flow autocorr
    fig3 = plt.figure(figsize=(10,5))
    if len(acorr) > 0:
        plt.stem(lags, acorr)
    else:
        plt.text(0.5, 0.5, "Insufficient trades to compute autocorrelation.",
                 ha="center", va="center", transform=plt.gca().transAxes)
    plt.title("Order-Flow Autocorrelation (signed trades)")
    plt.xlabel("Lag (trades)"); plt.ylabel("Autocorr")
    fig3.tight_layout(); fig3.savefig(plots_dir / "oflow_autocorr.png", dpi=150); plt.close(fig3)

    # 4) Drift vs imbalance
    fig4 = plt.figure(figsize=(10,5))
    if len(drift_by_decile) == 0:
        plt.text(0.5, 0.5, "No data for drift vs imbalance (check L1 sizes & horizon).",
                 ha="center", va="center", transform=plt.gca().transAxes)
    else:
        x = np.arange(len(drift_by_decile))
        plt.bar(x, drift_by_decile.values)
        plt.xticks(x, [str(i) for i in x])
    plt.title(f"Future Mid Drift vs L1 Imbalance (grid={drift_grid_ms} ms, H={H_drift_ms} ms)")
    plt.xlabel("Imbalance decile (0=ask-heavy … 9=most bid-heavy)"); plt.ylabel("Avg future return (bp)")
    fig4.tight_layout(); fig4.savefig(plots_dir / "drift_vs_imbalance.png", dpi=150); plt.close(fig4)

        # ---------- Clustering impact curves ----------
    try:
        from sklearn.cluster import KMeans
        # Build trade-level feature matrix: notional bucket → future mid bp change
        H = horizons_ms[0]  # use the shortest horizon
        buckets = dft[f"notional_bucket_{H}ms"].cat.codes.values  # bucket index 0–9
        bp = dft[f"bp_{H}ms"].values

        # Wide pivot: rows=trades, cols=buckets, value=bp
        df_feat = pd.DataFrame({"bucket": buckets, "bp": bp, "trade_id": np.arange(len(buckets))})
        feat = df_feat.pivot_table(index="trade_id", columns="bucket", values="bp", aggfunc="mean").fillna(0)

        # Run KMeans
        k = 3
        km = KMeans(n_clusters=k, n_init=10, random_state=42)
        labels = km.fit_predict(feat.values)
        feat["cluster"] = labels

        # Plot cluster centroids
        fig5 = plt.figure(figsize=(10,5))
        for ci in range(k):
            centroid = km.cluster_centers_[ci]
            plt.plot(np.arange(len(centroid)), centroid, marker="o", label=f"Cluster {ci}")
        plt.title(f"Impact Curve Clusters (horizon={H} ms, k={k})")
        plt.xlabel("Notional bucket"); plt.ylabel("Δ mid (bp)")
        plt.legend()
        fig5.tight_layout()
        fig5.savefig(plots_dir / "impact_clusters.png", dpi=150)
        plt.close(fig5)
        print(f"[ok] wrote clustering figure impact_clusters.png with k={k}")

    except Exception as e:
        print(f"[warn] clustering step skipped: {e}")




    # -----------------------------
    # JSON summary
    # -----------------------------
    summary = {
        "rv": {
            "bar_sec": args.bar_sec, "window": args.rv_window,
            "parkinson": {
                "mean": float(np.nanmean(parkinson_ann.values)),
                "median": float(np.nanmedian(parkinson_ann.values)),
                "p90": float(np.nanpercentile(parkinson_ann.values, 90)) if len(parkinson_ann)>0 else float("nan")
            },
            "garman_klass": {
                "mean": float(np.nanmean(gk_ann.values)),
                "median": float(np.nanmedian(gk_ann.values)),
                "p90": float(np.nanpercentile(gk_ann.values, 90)) if len(gk_ann)>0 else float("nan")
            }
        },
        "impact": {
            str(H): {
                "by_notional_bp": {str(i): float(v) for i, v in enumerate(impact_results[H]["by_notional"].values)},
                "by_qty_percentile_bp": {str(i): float(v) for i, v in enumerate(impact_results[H]["by_qty_percentile"].values)}
            } for H in horizons_ms
        },
        "order_flow_autocorr": {
            "max_lag": int(maxlag),
            "lags": lags.tolist() if len(lags)>0 else [],
            "values": [float(x) for x in acorr] if len(acorr)>0 else []
        },
        "drift_vs_imbalance": {
            "horizon_ms": H_drift_ms,
            "grid_ms": drift_grid_ms,
            "deciles_bp": {str(int(k)): float(v) for k, v in drift_by_decile.items()}
        }
    }
    out_json.write_text(json.dumps(summary, indent=2))
    print(f"[ok] Wrote: {plots_dir/'vol.png'}, {plots_dir/'impact.png'}, {plots_dir/'oflow_autocorr.png'}, {plots_dir/'drift_vs_imbalance.png'}")
    print(f"[ok] Summary JSON: {out_json}")



if __name__ == "__main__":
    main()
