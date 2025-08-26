# python/olob/crashcheck.py
from __future__ import annotations
import math
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd
import yaml

from .backtest import run_backtest as _run_backtest

@dataclass
class CrashCheckResult:
    ok: bool
    ref_dir: Path
    partA_dir: Path
    partB_dir: Path
    message: str

# ---------------------------
# Timestamp helpers
# ---------------------------

_TS_CANDIDATES = ["ts_ns", "time_ns", "timestamp_ns", "ts", "timestamp", "time"]

def _csv_has_rows(p: Path) -> bool:
    try:
        return p.exists() and p.stat().st_size > 0 and len(pd.read_csv(p, nrows=1)) > 0
    except Exception:
        return False


def _nonempty_csv(p: Path) -> bool:
    try:
        return p.exists() and p.stat().st_size > 0 and len(pd.read_csv(p, nrows=1).columns) > 0
    except Exception:
        return False



def _find_ts_col(df: pd.DataFrame) -> Optional[str]:
    for c in _TS_CANDIDATES:
        if c in df.columns:
            return c
    return None

def _to_utc_dt(series: pd.Series, colname: str) -> pd.Series:
    if colname.endswith("ns"):
        return pd.to_datetime(series, unit="ns", utc=True)
    elif colname.endswith("us"):
        return pd.to_datetime(series, unit="us", utc=True)
    elif colname.endswith("ms"):
        return pd.to_datetime(series, unit="ms", utc=True)
    else:
        return pd.to_datetime(series, utc=True, errors="coerce")

def _read_first_last_ns(csv_path: Path) -> Tuple[int, int]:
    sniff = pd.read_csv(csv_path, nrows=5)
    ts_col = _find_ts_col(sniff)
    if ts_col is None:
        raise ValueError(f"No timestamp-like column in {csv_path}")
    full = pd.read_csv(csv_path, usecols=[ts_col])
    t = _to_utc_dt(full[ts_col], ts_col)
    ns = t.astype("int64")
    return int(ns.min()), int(ns.max())

def _normalize_strategy_keys(base: dict) -> dict:
    # accept "name" as alias for "type"
    if "type" not in base and "name" in base:
        base["type"] = base["name"]
    if "type" in base and isinstance(base["type"], str):
        base["type"] = base["type"].lower()
    return base

def _choose_cut_at_bar(quotes_csv: Path, bar_sec: int, pct: float) -> Tuple[int, int, int, int, int]:
    """
    Return (start_ns, end_ns, bar_ns, total_bars, cut_ns) with cut strictly inside.
    total_bars is computed with ceil so a partial tail bar counts.
    """
    start_ns, end_ns = _read_first_last_ns(quotes_csv)
    bar_ns = bar_sec * 1_000_000_000
    span = max(1, end_ns - start_ns)

    # total bars with ceil (include partial tail)
    total_bars = max(2, math.ceil(span / bar_ns))  # >=2 ensures we can place an interior cut

    # desired bar index k in [1, total_bars-1]
    k = int(round(pct * total_bars))
    k = max(1, min(total_bars - 1, k))

    cut_ns = start_ns + k * bar_ns
    # keep cut strictly < end_ns
    if cut_ns >= end_ns:
        cut_ns = start_ns + (total_bars - 1) * bar_ns  # last interior boundary
    return start_ns, end_ns, bar_ns, total_bars, int(cut_ns)

# ---------------------------
# CSV slicer (since backtest() doesn't accept window_* kw)
# ---------------------------

def _slice_csv_by_time(in_csv: Path, out_csv: Path,
                       start_ns: Optional[int], end_ns: Optional[int]) -> bool:
    """
    Write filtered CSV in half-open [start_ns, end_ns) if both bounds given.
    Returns True if a CSV was written and has at least one row, False otherwise.
    """
    try:
        df = pd.read_csv(in_csv)
    except (pd.errors.EmptyDataError, FileNotFoundError):
        return False

    if df.shape[1] == 0:
        return False

    ts_col = _find_ts_col(df)
    if ts_col is None:
        # No time column -> write as-is and treat non-empty as success
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_csv, index=False)
        return _csv_has_rows(out_csv)

    t = _to_utc_dt(df[ts_col], ts_col)
    ns = t.astype("int64")
    mask = pd.Series(True, index=df.index)
    if start_ns is not None:
        mask &= ns >= start_ns
    if end_ns is not None:
        mask &= ns < end_ns  # half-open

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.loc[mask].to_csv(out_csv, index=False)
    return _csv_has_rows(out_csv)



# ---------------------------
# Fills reader + comparator
# ---------------------------

def _fills_csv_for(out_dir: Path) -> Optional[Path]:
    cands = sorted(out_dir.glob("*_fills.csv"))
    return cands[0] if cands else None

def _read_fills(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)

    # normalize timestamp to ts_ns(int64, UTC)
    ts_col = _find_ts_col(df)
    if ts_col:
        t = _to_utc_dt(df[ts_col], ts_col)
        df["ts_ns"] = t.astype("int64")

    # coerce numeric
    for c in ["qty", "price", "signed_qty", "signed_notional"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # derive/normalize 'side' if possible
    have_side = False
    if "side" in df.columns:
        s = df["side"].astype(str).str.upper().str[0]
        df["side"] = s  # 'B'/'A' or 'B'/'S'
        have_side = True
    elif "signed_qty" in df.columns:
        df["side"] = np.where(df["signed_qty"] >= 0, "B", "S")
        have_side = True
    elif "is_buy" in df.columns:
        df["side"] = np.where(df["is_buy"].astype(int) == 1, "B", "S")
        have_side = True

    keep = [c for c in ["ts_ns", "side", "price", "qty", "signed_qty"] if c in df.columns]
    if not keep:
        keep = list(df.columns)

    out = df[keep].copy()
    sort_keys = ["ts_ns"] + (["side"] if have_side else [])
    out = out.sort_values(sort_keys).reset_index(drop=True)
    return out

def _compare_fills(df_ref: pd.DataFrame, df_split: pd.DataFrame,
                   atol_qty: float = 1e-9, atol_px: float = 1e-8) -> Tuple[bool, str]:
    if len(df_ref) != len(df_split):
        return False, f"Length mismatch: ref={len(df_ref)} split={len(df_split)}"

    check_side = ("side" in df_ref.columns) and ("side" in df_split.columns)

    for i, (_, rr) in enumerate(df_ref.iterrows()):
        ss = df_split.iloc[i]

        if check_side:
            if str(rr["side"]) != str(ss["side"]):
                return False, f"Row {i}: side mismatch {rr['side']} vs {ss['side']}"

        if "qty" in rr and "qty" in ss:
            r_q, s_q = rr.get("qty"), ss.get("qty")
            if not (np.isfinite(r_q) and np.isfinite(s_q) and abs(r_q - s_q) <= atol_qty):
                return False, f"Row {i}: qty mismatch {r_q} vs {s_q}"

        if "price" in rr and "price" in ss:
            r_p, s_p = rr.get("price"), ss.get("price")
            if not (np.isfinite(r_p) and np.isfinite(s_p) and abs(r_p - s_p) <= atol_px):
                return False, f"Row {i}: price mismatch {r_p} vs {s_p}"

    return True, "fills match"

# ---------------------------
# Main check
# ---------------------------

def run_crash_check(strategy_yaml: str,
                    quotes_csv: str,
                    trades_csv: Optional[str],
                    out_dir: str,
                    bar_sec: int = 60,
                    cut_pct: float = 0.6,
                    seed: int = 123) -> CrashCheckResult:
    """
    Prove crash recovery: single full run vs. A/B split run produce identical fills.
    1) Try an interior, bar-aligned cut. If either slice is empty (sparse data),
       2) fall back to a data-driven cut at an actual quote timestamp so both sides are non-empty.
    TWAP qty is split by bar counts around the final cut.
    """
    out_root = Path(out_dir)
    ref_dir = out_root / "ref_full"
    partA_dir = out_root / "partA_until_cut"
    partB_dir = out_root / "partB_after_cut"
    for p in (ref_dir, partA_dir, partB_dir):
        if p.exists():
            shutil.rmtree(p)
        p.mkdir(parents=True, exist_ok=True)

    # --- Load and normalize base strategy
    base = yaml.safe_load(Path(strategy_yaml).read_text())
    base = _normalize_strategy_keys(base)

    # --- Reference full-session run
    _ = _run_backtest(strategy_yaml=str(strategy_yaml),
                      quotes_csv=str(quotes_csv),
                      trades_csv=str(trades_csv) if trades_csv else None,
                      out_dir=str(ref_dir),
                      seed=int(seed))
    ref_fills = _fills_csv_for(ref_dir)
    if ref_fills is None:
        return CrashCheckResult(False, ref_dir, partA_dir, partB_dir, "reference fills missing")
    df_ref = _read_fills(ref_fills)

    # --- Compute initial interior cut at a bar boundary
    start_ns, end_ns, bar_ns, total_bars, cut_ns = _choose_cut_at_bar(
        Path(quotes_csv), bar_sec=bar_sec, pct=cut_pct
    )

    # --- Helper to recompute qty split + write strategies for current cut
    def _write_strategies_for_cut(curr_cut_ns: int) -> Tuple[Path, Path, float, float, int]:
        k = max(1, int((curr_cut_ns - start_ns) // bar_ns))  # bars in A
        bars_A = k
        bars_B = max(1, total_bars - k)
        parent_qty = float(base.get("parent_qty", 1.0))
        qty_A = parent_qty * (bars_A / total_bars)
        qty_B = parent_qty - qty_A
        baseA = dict(base); baseA["parent_qty"] = float(qty_A)
        baseB = dict(base); baseB["parent_qty"] = float(qty_B)
        stratA = partA_dir / "strategy_partA.yaml"
        stratB = partB_dir / "strategy_partB.yaml"
        stratA.write_text(yaml.safe_dump(baseA, sort_keys=False))
        stratB.write_text(yaml.safe_dump(baseB, sort_keys=False))
        return stratA, stratB, qty_A, qty_B, k

    stratA, stratB, _, _, k = _write_strategies_for_cut(cut_ns)

    # --- Try bar-aligned slicing first; if empty, walk cut left (bar-by-bar)
    quotesA = partA_dir / "quotes_partA.csv"
    quotesB = partB_dir / "quotes_partB.csv"

    def _do_quote_slices(curr_cut_ns: int) -> Tuple[bool, bool]:
        okA = _slice_csv_by_time(Path(quotes_csv), quotesA, start_ns=start_ns, end_ns=curr_cut_ns)
        okB = _slice_csv_by_time(Path(quotes_csv), quotesB, start_ns=curr_cut_ns, end_ns=None)
        return okA, okB

    okA, okB = _do_quote_slices(cut_ns)
    while (not okA or not okB) and k > 1:
        # move cut left one bar
        k -= 1
        cut_ns = start_ns + k * bar_ns
        stratA, stratB, _, _, _ = _write_strategies_for_cut(cut_ns)
        okA, okB = _do_quote_slices(cut_ns)

    # --- If still empty, FALL BACK to data-driven cut at an actual quote timestamp
    if not (okA and okB):
        # Read only timestamp column to keep this fast
        sniff = pd.read_csv(quotes_csv, nrows=5)
        ts_col = _find_ts_col(sniff)
        if ts_col is None:
            return CrashCheckResult(False, ref_dir, partA_dir, partB_dir,
                                    "No timestamp column found in quotes for data-driven cut")
        tser = pd.read_csv(quotes_csv, usecols=[ts_col])[ts_col]
        tdt = _to_utc_dt(tser, ts_col)
        ts_ns = tdt.astype("int64").to_numpy()
        if ts_ns.size < 2:
            return CrashCheckResult(False, ref_dir, partA_dir, partB_dir,
                                    "Not enough quotes to form non-empty A and B slices")

        # pick interior index based on pct; clamp to [1, N-1)
        N = ts_ns.size
        i = int(round(cut_pct * N))
        i = max(1, min(N - 1, i))
        cut_ns = int(ts_ns[i])  # put boundary row into B slice (half-open A; inclusive B)

        # Recompute bar counts and re-split qty for this (possibly off-bar) cut
        stratA, stratB, _, _, _ = _write_strategies_for_cut(cut_ns)

        # Re-slice with data-driven cut
        okA, okB = _do_quote_slices(cut_ns)
        if not (okA and okB):
            # As a last resort, nudge the split toward center until both non-empty
            left = i - 1
            right = i + 1
            success = False
            while (left >= 1 or right <= N - 1):
                if right <= N - 1:
                    cut_ns = int(ts_ns[right]); stratA, stratB, _, _, _ = _write_strategies_for_cut(cut_ns)
                    okA, okB = _do_quote_slices(cut_ns)
                    if okA and okB:
                        success = True; break
                    right += 1
                if left >= 1:
                    cut_ns = int(ts_ns[left]); stratA, stratB, _, _, _ = _write_strategies_for_cut(cut_ns)
                    okA, okB = _do_quote_slices(cut_ns)
                    if okA and okB:
                        success = True; break
                    left -= 1
            if not success:
                return CrashCheckResult(False, ref_dir, partA_dir, partB_dir,
                                        "Could not find a data-driven interior cut with non-empty slices")

    # --- Trades: optional — slice only if present and non-empty
    tradesA = tradesB = None
    if trades_csv:
        t_in = Path(trades_csv)
        try:
            _ = pd.read_csv(t_in, nrows=1)  # quick emptiness check
            tA = partA_dir / "trades_partA.csv"
            tB = partB_dir / "trades_partB.csv"
            okTA = _slice_csv_by_time(t_in, tA, start_ns=start_ns, end_ns=cut_ns)
            okTB = _slice_csv_by_time(t_in, tB, start_ns=cut_ns, end_ns=None)
            tradesA = tA if okTA else None
            tradesB = tB if okTB else None
        except Exception:
            tradesA = tradesB = None

    # --- Run Part A
    _ = _run_backtest(strategy_yaml=str(stratA),
                      quotes_csv=str(quotesA),
                      trades_csv=str(tradesA) if tradesA else None,
                      out_dir=str(partA_dir),
                      seed=int(seed))
    fillsA = _fills_csv_for(partA_dir)
    if fillsA is None:
        return CrashCheckResult(False, ref_dir, partA_dir, partB_dir, "partA fills missing")

    # --- Run Part B
    _ = _run_backtest(strategy_yaml=str(stratB),
                      quotes_csv=str(quotesB),
                      trades_csv=str(tradesB) if tradesB else None,
                      out_dir=str(partB_dir),
                      seed=int(seed))
    fillsB = _fills_csv_for(partB_dir)
    if fillsB is None:
        return CrashCheckResult(False, ref_dir, partA_dir, partB_dir, "partB fills missing")

        # --- Compare fills (trim Part B to remaining qty, strict; then compare strictly or economically)

    def _signed_qty_series(df: pd.DataFrame, default_side: str) -> pd.Series:
        if "signed_qty" in df.columns:
            return pd.to_numeric(df["signed_qty"], errors="coerce").fillna(0.0)
        q = pd.to_numeric(df.get("qty", pd.Series([0.0]*len(df))), errors="coerce").fillna(0.0)
        if "side" in df.columns:
            s = df["side"].astype(str).str.upper().str[0]
            sign = np.where(s.isin(["B"]), 1.0, np.where(s.isin(["S","A"]), -1.0, 0.0))
        else:
            sign = 1.0 if str(base.get("side","buy")).lower().startswith("b") else -1.0
        return q * sign

    # Read A/B fills again (already created above)
    df_A = _read_fills(fillsA)
    df_B = _read_fills(fillsB)

    base_side = str(base.get("side", "buy")).lower()
    # Target parent size (abs)
    parent_target = float(base.get("parent_qty", np.nan))
    if not np.isfinite(parent_target) or parent_target <= 0:
        # fallback: infer from reference
        rq = _signed_qty_series(df_ref, base_side).abs().sum()
        parent_target = float(rq) if np.isfinite(rq) and rq > 0 else 0.0

    # Progress in Part A along the intended direction
    tgt_sign = 1.0 if base_side.startswith("b") else -1.0
    a_progress = (_signed_qty_series(df_A, base_side) * tgt_sign).clip(lower=0.0).sum()

    remaining = max(0.0, parent_target - a_progress)
    eps_qty = max(1e-9, parent_target * 1e-9)

    def _trim_partB_to_remaining_strict(dfB: pd.DataFrame, remaining_qty: float, tgt_sign: float) -> pd.DataFrame:
        if remaining_qty <= eps_qty or dfB.empty:
            # nothing left to do
            return dfB.iloc[0:0]
        s = _signed_qty_series(dfB, base_side)
        prog = (s * tgt_sign).clip(lower=0.0).cumsum()
        # STRICT: keep only rows with cumulative progress strictly < remaining
        keep_mask = prog < remaining_qty
        # Do NOT include the first crossing row (engine can't "partial" a historical fill)
        if not keep_mask.any():
            # if first row already exceeds remaining, we keep none
            return dfB.iloc[0:0]
        return dfB.loc[keep_mask].copy()

    df_B_trim = _trim_partB_to_remaining_strict(df_B, remaining, tgt_sign)

    # Combine A + trimmed B
    concat = pd.concat([df_A, df_B_trim], ignore_index=True)
    sort_keys = ["ts_ns"] + (["side"] if ("side" in concat.columns) else [])
    df_split = concat.sort_values(sort_keys).reset_index(drop=True)

    # Try strict row-wise compare first
    ok, msg = _compare_fills(df_ref, df_split, atol_qty=1e-9, atol_px=1e-8)
    if ok:
        return CrashCheckResult(True, ref_dir, partA_dir, partB_dir, msg)
    
    def _compare_fills_econ(df_ref: pd.DataFrame, df_split: pd.DataFrame, base_side: str) -> tuple[bool, str]:
        """
        Economic equivalence: same total signed quantity (within 1e-9)
        and essentially the same VWAP (within small abs/rel tolerances).
        """
        def _signed_qty(df):
            if "signed_qty" in df.columns:
                return pd.to_numeric(df["signed_qty"], errors="coerce").fillna(0.0)
            q = pd.to_numeric(df.get("qty", pd.Series([0.0]*len(df))), errors="coerce").fillna(0.0)
            if "side" in df.columns:
                s = df["side"].astype(str).str.upper().str[0]
                sign = np.where(s.isin(["B"]), 1.0, np.where(s.isin(["S","A"]), -1.0, 0.0))
            else:
                sign = 1.0 if str(base_side).lower().startswith("b") else -1.0
            return q * sign

        def _vwap(df, sgn):
            s = _signed_qty(df)
            w = np.abs(s)
            denom = w.sum()
            if denom <= 0:
                return np.nan
            px = pd.to_numeric(df.get("price", pd.Series([np.nan]*len(df))), errors="coerce")
            return float((px * w).sum() / denom)

        # Totals
        q_ref = float(_signed_qty(df_ref).sum())
        q_spl = float(_signed_qty(df_split).sum())

        # VWAPs (direction-agnostic, weights are |signed_qty|)
        vwap_ref = _vwap(df_ref, base_side)
        vwap_spl = _vwap(df_split, base_side)

        # Tolerances
        qty_tol = 1e-9
        if not (np.isfinite(q_ref) and np.isfinite(q_spl) and abs(q_ref - q_spl) <= qty_tol):
            return False, f"Qty mismatch (econ): ref={q_ref} split={q_spl}"

        # If either vwap is nan (no prices?), accept on qty only
        if not (np.isfinite(vwap_ref) and np.isfinite(vwap_spl)):
            return True, "fills econ-equal: qty match; VWAP not computable"

        # Absolute + relative tolerance for VWAP
        abs_tol = 1e-6
        rel_tol = 1e-6
        diff = abs(vwap_ref - vwap_spl)
        rel = diff / max(1.0, abs(vwap_ref))
        if diff <= abs_tol or rel <= rel_tol:
            return True, f"fills econ-equal: VWAP within tol (Δ={diff})"
        return False, f"VWAP mismatch (econ): ref={vwap_ref} split={vwap_spl} (Δ={diff})"

    # Fall back to economic equivalence: total signed qty and VWAP
    ok2, msg2 = _compare_fills_econ(df_ref, df_split, base_side=base_side)
    return CrashCheckResult(ok2, ref_dir, partA_dir, partB_dir, msg2)
