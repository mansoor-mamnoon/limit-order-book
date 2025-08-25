from __future__ import annotations
import argparse, json
from pathlib import Path
import pandas as pd
import numpy as np

"""
Converts reconstructed_states.parquet -> top10_depth.parquet with schema:
  ts_ns:int64, side:('B'|'A'), level:int (1..10), price:float64, qty:float64

Precedence (most specific → fallback):
A) bids/asks as list-of-[price,qty] (absolute prices)  <-- preferred
B) Wide columns: b1_px,b1_qty,...,a10_px,a10_qty
C) Separate arrays: bids_px,bids_qty,asks_px,asks_qty
D) Ticks-only L1 fields: event_time_ms + best_*_ticks/_qty  (requires --tick-size)
"""

def _coerce_float(x):
    try:
        return float(x)
    except Exception:
        return np.nan

def _ensure_ts_ns(df: pd.DataFrame, debug: bool) -> pd.DataFrame:
    cols = {c.lower(): c for c in df.columns}
    if "ts_ns" in cols:
        c = cols["ts_ns"]
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64").astype("int64")
        if debug: print("[export_top10] using ts_ns column")
        return df.rename(columns={c: "ts_ns"})
    if "event_time_ms" in cols:
        c = cols["event_time_ms"]
        df["ts_ns"] = (pd.to_numeric(df[c], errors="coerce") * 1_000_000).astype("int64")
        if debug: print("[export_top10] derived ts_ns from event_time_ms")
        return df
    if "timestamp" in cols:
        c = cols["timestamp"]
        df["ts_ns"] = pd.to_numeric(df[c], errors="coerce").astype("int64")
        if debug: print("[export_top10] derived ts_ns from timestamp")
        return df
    raise KeyError("Could not find a timestamp column (expected 'ts_ns' or 'event_time_ms').")

def _normalize_pairs(pairs, debug: bool):
    """
    Yield (price, qty) tuples from a variety of possible encodings:
    - list/tuple of [price, qty]
    - numpy arrays
    - strings like '[[p,q],[p,q],...]'
    - dict-like {'price':..., 'qty':...} or {'0':..., '1':...}
    """
    if pairs is None:
        return
    # If it came in as a string (sometimes parquet round-trips do this), parse JSON
    if isinstance(pairs, str):
        try:
            pairs = json.loads(pairs)
        except Exception:
            return

    if hasattr(pairs, "__iter__") and not isinstance(pairs, (str, bytes)):
        for item in pairs:
            if item is None:
                continue
            # tuple/list
            if isinstance(item, (list, tuple, np.ndarray)):
                if len(item) >= 2:
                    yield _coerce_float(item[0]), _coerce_float(item[1])
                continue
            # dict-like
            if hasattr(item, "get"):
                if "price" in item and "qty" in item:
                    yield _coerce_float(item.get("price")), _coerce_float(item.get("qty"))
                    continue
                # numeric keys
                if (0 in item or "0" in item) and (1 in item or "1" in item):
                    k0 = 0 if 0 in item else "0"
                    k1 = 1 if 1 in item else "1"
                    yield _coerce_float(item.get(k0)), _coerce_float(item.get(k1))
                    continue
            # last resort: try to treat as sequence
            try:
                a = list(item)
                if len(a) >= 2:
                    yield _coerce_float(a[0]), _coerce_float(a[1])
            except Exception:
                continue

def _rows_from_list_of_pairs(ts, side, pairs, levels, debug: bool):
    out = []
    n = 0
    for px, qty in _normalize_pairs(pairs, debug):
        if np.isnan(px) or np.isnan(qty):
            continue
        out.append({"ts_ns": int(ts), "side": side, "level": n + 1, "price": px, "qty": qty})
        n += 1
        if n >= levels:
            break
    return out

def _rows_from_two_lists(ts, side, pxs, qtys, levels):
    out = []
    pxs = list(pxs or [])
    qtys = list(qtys or [])
    for i in range(1, levels + 1):
        px = _coerce_float(pxs[i - 1]) if i - 1 < len(pxs) else np.nan
        qty = _coerce_float(qtys[i - 1]) if i - 1 < len(qtys) else np.nan
        if np.isnan(px) or np.isnan(qty):
            continue
        out.append({"ts_ns": int(ts), "side": side, "level": i, "price": px, "qty": qty})
    return out

def _rows_from_wide_cols(row, prefix, side, levels):
    out = []
    for i in range(1, levels + 1):
        px = _coerce_float(row.get(f"{prefix}{i}_px"))
        qty = _coerce_float(row.get(f"{prefix}{i}_qty"))
        if np.isnan(px) or np.isnan(qty):
            continue
        out.append({"ts_ns": int(row["ts_ns"]), "side": side, "level": i, "price": px, "qty": qty})
    return out

def infer_and_convert(df: pd.DataFrame, levels: int, tick_size: float | None, debug: bool):
    df = _ensure_ts_ns(df, debug)

    lower = {c.lower(): c for c in df.columns}
    cols_lower = set(lower.keys())

    # ---------- A) bids/asks list-of-[price,qty] (absolute prices) ----------
    if "bids" in cols_lower and "asks" in cols_lower:
        bids_c = lower["bids"]; asks_c = lower["asks"]
        rows = []
        # choose only needed columns to speed up
        for _, r in df[["ts_ns", bids_c, asks_c]].iterrows():
            rows += _rows_from_list_of_pairs(r["ts_ns"], "B", r[bids_c], levels, debug)
            rows += _rows_from_list_of_pairs(r["ts_ns"], "A", r[asks_c], levels, debug)
        if debug:
            print(f"[export_top10] Schema A: produced {len(rows)} rows")
        if rows:
            if debug: print("[export_top10] Using schema: A (bids/asks arrays)")
            return pd.DataFrame(rows)

    # ---------- B) wide columns b1_px/b1_qty & a1_px/a1_qty ----------
    have_b_wide = all((f"b{i}_px" in cols_lower and f"b{i}_qty" in cols_lower) for i in range(1, min(levels, 10) + 1))
    have_a_wide = all((f"a{i}_px" in cols_lower and f"a{i}_qty" in cols_lower) for i in range(1, min(levels, 10) + 1))
    if have_b_wide and have_a_wide:
        rows = []
        df_l = df.rename(columns={v: k for k, v in lower.items()})
        for _, r in df_l.iterrows():
            rows += _rows_from_wide_cols(r, "b", "B", levels)
            rows += _rows_from_wide_cols(r, "a", "A", levels)
        if debug:
            print(f"[export_top10] Schema B: produced {len(rows)} rows")
        if rows:
            if debug: print("[export_top10] Using schema: B (wide columns)")
            return pd.DataFrame(rows)

    # ---------- C) separate arrays ----------
    need_c = {"bids_px", "bids_qty", "asks_px", "asks_qty"}
    if need_c.issubset(cols_lower):
        rows = []
        bp, bq, ap, aq = [lower[x] for x in ("bids_px", "bids_qty", "asks_px", "asks_qty")]
        for _, r in df[["ts_ns", bp, bq, ap, aq]].iterrows():
            rows += _rows_from_two_lists(r["ts_ns"], "B", r[bp], r[bq], levels)
            rows += _rows_from_two_lists(r["ts_ns"], "A", r[ap], r[aq], levels)
        if debug:
            print(f"[export_top10] Schema C: produced {len(rows)} rows")
        if rows:
            if debug: print("[export_top10] Using schema: C (separate arrays)")
            return pd.DataFrame(rows)

    # ---------- D) ticks-only L1 (requires tick_size) ----------
    need_d = {"event_time_ms", "best_bid_ticks", "best_bid_qty", "best_ask_ticks", "best_ask_qty"}
    if need_d.intersection(cols_lower) and (("best_bid_ticks" in cols_lower) or ("best_ask_ticks" in cols_lower)):
        if not tick_size or tick_size <= 0:
            raise ValueError("This states file uses *_ticks columns; please pass --tick-size (e.g., 0.01).")
        bb_t = lower.get("best_bid_ticks"); bb_q = lower.get("best_bid_qty")
        ba_t = lower.get("best_ask_ticks"); ba_q = lower.get("best_ask_qty")
        use_bb = bb_t in df.columns and bb_q in df.columns
        use_ba = ba_t in df.columns and ba_q in df.columns

        rows = []
        cols = ["ts_ns"] + [c for c in (bb_t, bb_q, ba_t, ba_q) if c is not None and c in df.columns]
        for _, r in df[cols].iterrows():
            ts_ns = int(r["ts_ns"])
            if use_bb:
                bid_px  = _coerce_float(r[bb_t]) * tick_size
                bid_qty = _coerce_float(r[bb_q])
                if np.isfinite(bid_px) and np.isfinite(bid_qty) and bid_qty > 0:
                    rows.append({"ts_ns": ts_ns, "side": "B", "level": 1, "price": bid_px, "qty": bid_qty})
            if use_ba:
                ask_px  = _coerce_float(r[ba_t]) * tick_size
                ask_qty = _coerce_float(r[ba_q])
                if np.isfinite(ask_px) and np.isfinite(ask_qty) and ask_qty > 0:
                    rows.append({"ts_ns": ts_ns, "side": "A", "level": 1, "price": ask_px, "qty": ask_qty})
        if debug:
            print(f"[export_top10] Schema D: produced {len(rows)} rows")
        if rows:
            if debug: print("[export_top10] Using schema: D (ticks-only L1)")
            return pd.DataFrame(rows)

    raise ValueError(
        "Unrecognized reconstructed_states schema.\n"
        f"Columns seen: {list(df.columns)}\n"
        "Supported precedence:\n"
        "  A) bids/asks as list-of-[price,qty]\n"
        "  B) wide b1_px,b1_qty,... / a1_px,a1_qty,...\n"
        "  C) separate arrays bids_px,bids_qty,asks_px,asks_qty\n"
        "  D) ticks-only L1 best_*_ticks/_qty (requires --tick-size)\n"
    )

def main():
    ap = argparse.ArgumentParser(description="Export Top-10 depth parquet from reconstructed_states.parquet")
    ap.add_argument("--states", required=True, help="Path to reconstructed_states.parquet")
    ap.add_argument("--out", required=True, help="Output parquet path (e.g. recon/.../top10_depth.parquet)")
    ap.add_argument("--levels", type=int, default=10, help="Levels per side (default 10)")
    ap.add_argument("--tick-size", type=float, default=None, help="For ticks-only schema (D): tick size to convert *_ticks -> price")
    ap.add_argument("--print-columns", action="store_true", help="Print columns and first row, then exit")
    ap.add_argument("--debug", action="store_true", help="Verbose detection output")
    args = ap.parse_args()

    df = pd.read_parquet(args.states)

    if args.print_columns:
        print("Columns:", list(df.columns))
        with pd.option_context("display.max_colwidth", 160, "display.width", 200):
            print(df.head(1))
        return

    out_df = infer_and_convert(df, args.levels, args.tick_size, args.debug).sort_values(["ts_ns", "side", "level"])
    out_df["ts_ns"] = out_df["ts_ns"].astype("int64")
    out_df["side"] = out_df["side"].astype(str).str.upper().str[0]
    out_df["level"] = out_df["level"].astype("int32")
    out_df["price"] = out_df["price"].astype("float64")
    out_df["qty"] = out_df["qty"].astype("float64")

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out_df.to_parquet(args.out, index=False)
    print(f"[export_top10] wrote {args.out} rows={len(out_df)}")

if __name__ == "__main__":
    main()
