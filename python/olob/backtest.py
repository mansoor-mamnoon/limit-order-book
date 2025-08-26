# python/olob/backtest.py
from __future__ import annotations
import argparse, json, yaml, os
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Optional, Dict, Any, Tuple

from .strategies import StrategyConfig, TWAPStrategy, VWAPStrategy, CostModel

def _read_quotes(path: str) -> pd.DataFrame:
    """
    Normalize quotes to columns:
      ts_ns:int64, bid_px:float64, bid_sz:float64, ask_px:float64, ask_sz:float64
    Accept many aliases and reconstruct missing side from mid+other side if needed.
    """
    df = pd.read_csv(path)

    def _pick(aliases: list[str]) -> Optional[str]:
        for a in aliases:
            if a in df.columns:
                return a
        return None

    # Aliases
    ts_col    = _pick(["ts_ns","time_ns","timestamp_ns","t_ns","ts","timestamp","time"])
    bidpx_col = _pick(["bid_px","bid","best_bid","bpx","bidPrice","best_bid_price"])
    askpx_col = _pick(["ask_px","ask","best_ask","apx","askPrice","best_ask_price"])
    bidsz_col = _pick(["bid_sz","bid_size","best_bid_size","bqty","bidQty","bid_size_l1","bid_sz_l1"])
    asksz_col = _pick(["ask_sz","ask_size","best_ask_size","aqty","askQty","ask_size_l1","ask_sz_l1"])
    mid_col   = _pick(["mid_px","mid","midprice","mid_price"])
    spr_col   = _pick(["spread","spr","spread_px"])

    if ts_col is None:
        raise ValueError("Quotes CSV missing any timestamp column (ts_ns/ts/timestamp).")

    # Normalize timestamp to ns
    if ts_col == "ts_ns":
        ts_ns = df[ts_col].astype("int64")
    else:
        ts_ns = pd.to_datetime(df[ts_col], utc=True).view("int64")

    # Prices
    bid_px = pd.to_numeric(df[bidpx_col], errors="coerce") if bidpx_col else None
    ask_px = pd.to_numeric(df[askpx_col], errors="coerce") if askpx_col else None

    # Try reconstructing from mid/spread
    if (bid_px is None or bid_px.isna().all()) or (ask_px is None or ask_px.isna().all()):
        mid = pd.to_numeric(df[mid_col], errors="coerce") if mid_col else None
        spr = pd.to_numeric(df[spr_col], errors="coerce") if spr_col else None

        if bid_px is None or bid_px.isna().all():
            if mid is not None and ask_px is not None and not ask_px.isna().all():
                bid_px = 2.0 * mid - ask_px
            elif mid is not None and spr is not None and not spr.isna().all():
                bid_px = mid - spr / 2.0

        if ask_px is None or ask_px.isna().all():
            if mid is not None and bid_px is not None and not bid_px.isna().all():
                ask_px = 2.0 * mid - bid_px
            elif mid is not None and spr is not None and not spr.isna().all():
                ask_px = mid + spr / 2.0

    # Final guard: still missing? try more aliases or fail
    if bid_px is None or ask_px is None or bid_px.isna().all() or ask_px.isna().all():
        # Last-ditch camelCase
        if bid_px is None or bid_px.isna().all():
            alt = _pick(["best_bid_price","BidPrice","BestBid","L1_Bid"])
            if alt:
                bid_px = pd.to_numeric(df[alt], errors="coerce")
        if ask_px is None or ask_px.isna().all():
            alt = _pick(["best_ask_price","AskPrice","BestAsk","L1_Ask"])
            if alt:
                ask_px = pd.to_numeric(df[alt], errors="coerce")

    if bid_px is None or ask_px is None or bid_px.isna().all() or ask_px.isna().all():
        raise ValueError("Could not determine bid/ask prices from CSV (need bid/ask or mid+other side).")

    # Sizes (optional)
    bid_sz = pd.to_numeric(df[bidsz_col], errors="coerce") if bidsz_col else pd.Series(np.nan, index=df.index)
    ask_sz = pd.to_numeric(df[asksz_col], errors="coerce") if asksz_col else pd.Series(np.nan, index=df.index)

    out = pd.DataFrame({
        "ts_ns": ts_ns.astype("int64"),
        "bid_px": bid_px.astype("float64"),
        "bid_sz": bid_sz.astype("float64"),
        "ask_px": ask_px.astype("float64"),
        "ask_sz": ask_sz.astype("float64"),
    })
    out = out.dropna(subset=["bid_px","ask_px"]).sort_values("ts_ns").reset_index(drop=True)
    if out.empty:
        raise ValueError("After normalization, quotes are empty. Check column mappings or file contents.")
    return out


def _read_trades(path: Optional[str]) -> Optional[pd.DataFrame]:
    """Load trades as [ts_ns, qty]. Return None if file is missing or empty."""
    if not path:
        return None
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        # No trades available -> VWAP will degrade to uniform schedule
        return None
    try:
        df = pd.read_csv(p)
    except pd.errors.EmptyDataError:
        return None

    # Timestamp
    if "ts_ns" in df.columns:
        ts_ns = df["ts_ns"].astype("int64")
    elif "ts" in df.columns:
        ts_ns = pd.to_datetime(df["ts"], utc=True).view("int64")
    else:
        # Try a few common aliases; if none, treat as no-trades
        for alias in ["timestamp_ns", "time_ns", "time", "timestamp"]:
            if alias in df.columns:
                ts_ns = pd.to_datetime(df[alias], utc=True).view("int64")
                break
        else:
            return None

    qty = df["qty"] if "qty" in df.columns else pd.Series(1.0, index=df.index)
    out = pd.DataFrame({"ts_ns": ts_ns, "qty": pd.to_numeric(qty, errors="coerce").fillna(0.0)})
    out = out[out["qty"] > 0].sort_values("ts_ns").reset_index(drop=True)
    if out.empty:
        return None
    return out


def _side_mult(side: str) -> int:
    return +1 if side.lower() == "buy" else -1

def _apply_latency(quotes: pd.DataFrame, ts_ns: int, latency_ms: int) -> Optional[int]:
    if latency_ms <= 0: 
        return ts_ns
    tgt = ts_ns + latency_ms * 1_000_000
    # next quote >= tgt
    idx = quotes["ts_ns"].searchsorted(tgt, side="left")
    if idx >= len(quotes):
        return None
    return int(quotes.iloc[idx]["ts_ns"])

def _fill_at_quote(qrow: pd.Series, side: str, qty: float, cost: CostModel, force_taker=True) -> Tuple[float,float,bool]:
    """Return (filled_qty, exec_px, taker?) using L1 size if available, otherwise assume full market fill."""
    if qty <= 0: 
        return 0.0, float("nan"), True
    if side.lower() == "buy":
        px = float(qrow["ask_px"])
        avail = float(qrow["ask_sz"]) if not np.isnan(qrow["ask_sz"]) else qty
    else:
        px = float(qrow["bid_px"])
        avail = float(qrow["bid_sz"]) if not np.isnan(qrow["bid_sz"]) else qty
    take_qty = min(qty, max(0.0, avail))
    take_qty = cost.quant_qty(take_qty)
    px = cost.quant_price(px)
    return take_qty, px, True  # taker fill

def _fee_amount(notional: float, bps: float) -> float:
    return notional * (bps / 10_000.0)

def _summarize_fills(fills: pd.DataFrame, side: str, cost: CostModel) -> Dict[str,Any]:
    if fills.empty:
        return {"filled_qty": 0.0, "avg_px": None, "notional": 0.0, "fees": 0.0, "signed_cost": 0.0}
    notional = (fills["px"] * fills["qty"]).sum()
    avg_px = notional / max(1e-12, fills["qty"].sum())
    # Fee model (all taker for now)
    fees = _fee_amount(notional, cost.taker_bps)
    sign = _side_mult(side)
    signed_cost = sign * notional + fees  # +fee increases cost
    return {
        "filled_qty": float(fills["qty"].sum()),
        "avg_px": float(avg_px),
        "notional": float(notional),
        "fees": float(fees),
        "signed_cost": float(signed_cost),
    }

def load_strategy_yaml(path: str) -> StrategyConfig:
    cfg = yaml.safe_load(Path(path).read_text())
    return StrategyConfig(
        name=cfg.get("name", cfg.get("type","strategy")).strip(),
        type=cfg["type"].strip().lower(),
        side=cfg["side"].strip().lower(),
        qty=float(cfg["qty"]),
        start=str(cfg["start"]),
        end=str(cfg["end"]),
        bar_sec=int(cfg.get("bar_sec", 60)),
        min_clip=float(cfg.get("min_clip", 0.0)),
        cooldown_ms=int(cfg.get("cooldown_ms", 0)),
        force_taker=bool(cfg.get("force_taker", True)),
        cost=cfg.get("cost", {}),
    )

def run_backtest(strategy_yaml: str, quotes_csv: str, trades_csv: Optional[str], out_dir: str) -> Dict[str,Any]:
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    cfg = load_strategy_yaml(strategy_yaml)
    quotes = _read_quotes(quotes_csv)
    trades = _read_trades(trades_csv)

    # Window the quotes to [start, end)
    start_ns = pd.Timestamp(cfg.start, tz="UTC").value if pd.Timestamp(cfg.start).tz is not None else pd.Timestamp(cfg.start).tz_localize("UTC").value
    end_ns   = pd.Timestamp(cfg.end,   tz="UTC").value if pd.Timestamp(cfg.end).tz is not None else pd.Timestamp(cfg.end).tz_localize("UTC").value
    q = quotes[(quotes["ts_ns"] >= start_ns) & (quotes["ts_ns"] < end_ns)].reset_index(drop=True)
    if q.empty:
        raise ValueError("No quotes in the configured backtest window.")

    # Build strategy
    if cfg.type == "twap":
        strat = TWAPStrategy(cfg)
    elif cfg.type == "vwap":
        strat = VWAPStrategy(cfg, trades)
    else:
        raise ValueError(f"Unknown strategy type: {cfg.type}")

    fills = []
    cost = strat.cost
    bar_edge_ns = start_ns + cfg.bar_sec * 1_000_000_000
    last_bar_idx = -1

    for i, row in q.iterrows():
        now_ns = int(row["ts_ns"])
        # Bar hook
        cur_bar_idx = int((now_ns - start_ns) // (cfg.bar_sec * 1_000_000_000))
        if cur_bar_idx != last_bar_idx:
            strat.on_bar(now_ns)
            last_bar_idx = cur_bar_idx

        if strat.remaining() <= 0:
            break
        # Decide child order
        desired = strat.on_tick(now_ns)
        desired = cost.quant_qty(desired)
        if desired <= 0:
            continue
        # Apply latency -> choose quote row at arrival
        arrive_ns = _apply_latency(q, now_ns, cost.fixed_latency_ms)
        if arrive_ns is None:
            break
        j = q["ts_ns"].searchsorted(arrive_ns, side="left")
        qrow = q.iloc[j]
        # Execute (taker L1)
        child_qty, exec_px, is_taker = _fill_at_quote(qrow, cfg.side, desired, cost, cfg.force_taker)
        if child_qty > 0:
            fills.append({"ts_ns": int(qrow["ts_ns"]), "px": exec_px, "qty": child_qty})
            strat.on_fill(child_qty, exec_px)
            strat.last_send_ns = now_ns

    fills_df = pd.DataFrame(fills, columns=["ts_ns","px","qty"])
    fills_path = Path(out_dir) / f"{cfg.name}_fills.csv"
    fills_df.to_csv(fills_path, index=False)

    summary = _summarize_fills(fills_df, cfg.side, cost)
    summary.update({
        "strategy": cfg.name,
        "type": cfg.type,
        "side": cfg.side,
        "parent_qty": cfg.qty,
        "start": cfg.start,
        "end": cfg.end,
        "bar_sec": cfg.bar_sec,
        "latency_ms": cost.fixed_latency_ms,
        "min_clip": cfg.min_clip,
        "cooldown_ms": cfg.cooldown_ms,
        "fees_bps": cost.taker_bps if True else cost.maker_bps,
        "fills_csv": str(fills_path),
    })
    (Path(out_dir) / f"{cfg.name}_summary.json").write_text(json.dumps(summary, indent=2))
    print(f"[fills] {fills_path}")
    print(f"[summary] {Path(out_dir) / f'{cfg.name}_summary.json'}")
    print(json.dumps(summary, indent=2))
    return summary

def main():
    ap = argparse.ArgumentParser(prog="lob backtest", description="Strategy backtester (VWAP/TWAP).")
    ap.add_argument("--strategy", required=True, help="YAML file (see docs/strategy/*.yaml)")
    ap.add_argument("--quotes",   required=False, help="TAQ quotes CSV (from replay_tool)")
    ap.add_argument("--file",     required=False, help="Alias for --quotes")
    ap.add_argument("--trades",   required=False, help="TAQ trades CSV (for VWAP volume weights)")
    ap.add_argument("--out",      required=True,  help="Output directory (e.g., out/backtests/run1)")
    args = ap.parse_args()

    quotes = args.quotes or args.file
    if not quotes:
        raise SystemExit("Provide --quotes or --file (quotes CSV)")

    run_backtest(args.strategy, quotes, args.trades, args.out)

if __name__ == "__main__":
    main()
