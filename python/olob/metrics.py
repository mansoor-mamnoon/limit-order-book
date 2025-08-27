# python/olob/metrics.py
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# --------------------- helpers ---------------------

def _time_weights(ts_ns: np.ndarray) -> np.ndarray:
    ts_ns = np.asarray(ts_ns, dtype=np.int64)
    if ts_ns.size == 0:
        return np.array([1.0], dtype=float)
    if ts_ns.size == 1:
        return np.array([1.0], dtype=float)
    dt = np.diff(ts_ns, append=ts_ns[-1] + (ts_ns[-1] - ts_ns[-2]))
    w = dt.astype(float)
    s = w.sum()
    return w / s if s > 0 else np.full_like(w, 1.0 / w.size, dtype=float)

def _twa(x: np.ndarray, w: np.ndarray) -> float:
    x = np.asarray(x, dtype=float)
    w = np.asarray(w, dtype=float)
    m = np.isfinite(x)
    if not m.any():
        return float("nan")
    x = x[m]; w = w[m]
    s = w.sum()
    if s <= 0:
        return float("nan")
    return float(np.sum(x * w) / s)

def _pct(x: np.ndarray, q: float) -> float:
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    if x.size == 0:
        return float("nan")
    return float(np.nanpercentile(x, q))

def _safe_imb(b: pd.Series, a: pd.Series) -> pd.Series:
    denom = (b + a).replace(0, np.nan)
    out = (b / denom).clip(lower=0, upper=1)
    return out.fillna(0.5)

def _normalize_quote_columns(q: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure quotes frame has the canonical columns:
      ts_ns:int64, bid_px:float, ask_px:float, bid_qty:float, ask_qty:float,
      mid:float, spread:float, microprice:float
    Accepts legacy names {bid,ask,bid_sz,ask_sz} and derives missing fields.
    """
    cols = set(q.columns)

    # Map legacy names -> canonical
    rename_map = {}
    if "bid" in cols and "bid_px" not in cols: rename_map["bid"] = "bid_px"
    if "ask" in cols and "ask_px" not in cols: rename_map["ask"] = "ask_px"
    if "bid_sz" in cols and "bid_qty" not in cols: rename_map["bid_sz"] = "bid_qty"
    if "ask_sz" in cols and "ask_qty" not in cols: rename_map["ask_sz"] = "ask_qty"

    if rename_map:
        q = q.rename(columns=rename_map)
        cols = set(q.columns)

    # Basic presence check
    need = {"ts_ns", "bid_px", "ask_px", "bid_qty", "ask_qty"}
    missing = need - cols
    if missing:
        raise ValueError(f"quotes CSV missing required columns: {sorted(missing)}")

    # Types
    q["ts_ns"] = pd.to_numeric(q["ts_ns"], errors="coerce").astype("Int64").astype(np.int64)
    for c in ["bid_px", "ask_px", "bid_qty", "ask_qty"]:
        q[c] = pd.to_numeric(q[c], errors="coerce")

    # Derive spread/mid if absent
    if "spread" not in q.columns:
        q["spread"] = q["ask_px"] - q["bid_px"]
    if "mid" not in q.columns:
        q["mid"] = 0.5 * (q["ask_px"] + q["bid_px"])

    # Derive microprice if absent (fallback to mid when sizes sum to 0)
    if "microprice" not in q.columns:
        denom = (q["bid_qty"] + q["ask_qty"]).replace(0, np.nan)
        q["microprice"] = (
            (q["ask_px"] * q["bid_qty"] + q["bid_px"] * q["ask_qty"]) / denom
        ).fillna(q["mid"])

    return q


# --------------------- core calcs ---------------------

def calc_from_quotes(quotes_csv: str) -> tuple[dict, pd.DataFrame]:
    q = pd.read_csv(quotes_csv)
    q = _normalize_quote_columns(q)
    q = q.sort_values("ts_ns").reset_index(drop=True)

    w = _time_weights(q["ts_ns"].to_numpy())

    spread = q["spread"].to_numpy()
    mid = q["mid"].to_numpy()
    micro = q["microprice"].to_numpy()
    imb = _safe_imb(q["bid_qty"], q["ask_qty"]).to_numpy()

    out = {
        "twa": {
            "spread": _twa(spread, w),
            "mid": _twa(mid, w),
            "microprice": _twa(micro, w),
            "imbalance_L1": _twa(imb, w),
        },
        "percentiles": {
            "spread": {"p50": _pct(spread, 50), "p90": _pct(spread, 90), "p99": _pct(spread, 99)},
            "imbalance_L1": {"p5": _pct(imb, 5), "p50": _pct(imb, 50), "p95": _pct(imb, 95)},
        },
        "counts": {"samples": int(len(q))},
    }
    return out, q


def calc_depth_top10(depth_parquet: str) -> tuple[dict, pd.DataFrame]:
    """
    Parquet schema expected:
      ts_ns:int64, side:('B'|'A'), level:int (1..10), price:float, qty:float
    Returns (summary dict, wide dataframe with columns 'B_L1'..'B_L10','A_L1'..)
    """
    d = pd.read_parquet(depth_parquet)
    d = d.sort_values(["ts_ns", "side", "level"])
    d["col"] = d["side"].astype(str) + "_L" + d["level"].astype(int).astype(str)
    wide = d.pivot_table(
        index="ts_ns", columns="col", values="qty", aggfunc="last"
    ).sort_index().fillna(0.0).reset_index()

    ts = wide["ts_ns"].to_numpy()
    w = _time_weights(ts)

    b_cols = [c for c in wide.columns if c.startswith("B_L")]
    a_cols = [c for c in wide.columns if c.startswith("A_L")]
    bid_arr = wide[b_cols].to_numpy() if b_cols else np.zeros((len(wide), 0))
    ask_arr = wide[a_cols].to_numpy() if a_cols else np.zeros((len(wide), 0))

    twa_b = {c: _twa(bid_arr[:, i], w) for i, c in enumerate(b_cols)}
    twa_a = {c: _twa(ask_arr[:, i], w) for i, c in enumerate(a_cols)}
    total_b = _twa(bid_arr.sum(axis=1) if bid_arr.size else np.zeros(len(wide)), w)
    total_a = _twa(ask_arr.sum(axis=1) if ask_arr.size else np.zeros(len(wide)), w)

    return {
        "twa_depth": {
            "bid": twa_b,
            "ask": twa_a,
            "total_bid": total_b,
            "total_ask": total_a,
        }
    }, wide


# --------------------- plotting ---------------------

def make_plots(q: pd.DataFrame, depth_wide: pd.DataFrame | None, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1) Spread over time
    plt.figure()
    plt.plot(q["ts_ns"], q["spread"])
    plt.xlabel("ts_ns"); plt.ylabel("spread"); plt.title("Spread over time")
    plt.savefig(out_dir / "spread.png", dpi=200); plt.close()

    # 2) Microprice vs Mid
    plt.figure()
    plt.plot(q["ts_ns"], q["mid"], label="mid")
    plt.plot(q["ts_ns"], q["microprice"], label="microprice")
    plt.legend(); plt.xlabel("ts_ns"); plt.ylabel("price"); plt.title("Mid vs Microprice")
    plt.savefig(out_dir / "mid_microprice.png", dpi=200); plt.close()

    # 3) L1 imbalance
    imb = _safe_imb(q["bid_qty"], q["ask_qty"])
    plt.figure()
    plt.plot(q["ts_ns"], imb)
    plt.xlabel("ts_ns"); plt.ylabel("imbalance (L1)"); plt.title("Best-level imbalance")
    plt.savefig(out_dir / "imbalance_L1.png", dpi=200); plt.close()

    # 4) Depth stacks (optional)
    if depth_wide is not None and not depth_wide.empty:
        b_cols = [c for c in depth_wide.columns if c.startswith("B_L")]
        a_cols = [c for c in depth_wide.columns if c.startswith("A_L")]
        if b_cols:
            plt.figure()
            plt.stackplot(depth_wide["ts_ns"], depth_wide[b_cols].to_numpy().T)
            plt.xlabel("ts_ns"); plt.ylabel("bid depth"); plt.title("Bid depth L1–L10")
            plt.savefig(out_dir / "depth_bid.png", dpi=200); plt.close()
        if a_cols:
            plt.figure()
            plt.stackplot(depth_wide["ts_ns"], depth_wide[a_cols].to_numpy().T)
            plt.xlabel("ts_ns"); plt.ylabel("ask depth"); plt.title("Ask depth L1–L10")
            plt.savefig(out_dir / "depth_ask.png", dpi=200); plt.close()


# --------------------- CLI ---------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="LOB Analytics v1 (spread, depth, imbalance, microprice)")
    ap.add_argument("--quotes", required=True, help="TAQ quotes CSV from replay_tool")
    ap.add_argument("--depth-top10", help="Top-10 depth Parquet (optional)")
    ap.add_argument("--out-json", required=True, help="Path to write summary JSON")
    ap.add_argument("--plots-out", required=True, help="Directory to save PNG plots")
    args = ap.parse_args()

    summary_q, q = calc_from_quotes(args.quotes)
    depth_summary, depth_wide = ({}, None)
    if args.depth_top10:
        depth_summary, depth_wide = calc_depth_top10(args.depth_top10)

    make_plots(q, depth_wide, Path(args.plots_out))

    out = {"quotes": summary_q}
    if depth_summary:
        out["depth"] = depth_summary
    Path(args.out_json).write_text(json.dumps(out, indent=2))
    print(f"[analytics] wrote {args.out_json} and plots -> {args.plots_out}")


if __name__ == "__main__":
    main()
