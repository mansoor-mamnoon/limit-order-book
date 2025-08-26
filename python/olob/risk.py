# python/olob/risk.py
from __future__ import annotations
import json, math
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

SECONDS_PER_YEAR_CRYPTO = 365 * 24 * 60 * 60

@dataclass
class RiskInputs:
    quotes_csv: Path              # TAQ quotes with best bid/ask (from replay_tool)
    fills_csv: Path               # fills produced by lob backtest
    out_table_csv: Path           # time series: inventory, mid, equity, etc.
    out_summary_json: Path        # summary: pnl, drawdown, sharpe-like, turnover, etc.
    parent_side: Optional[str] = None   # "buy" | "sell" if fills don't include side column
    fee_bps: float = 0.0                 # if not already baked into fills (safety)

def _as_ns(ts):
    ts = pd.to_datetime(ts, utc=True, errors="coerce")
    return ts.view("int64")

def _mid_from_quotes(dfq: pd.DataFrame) -> pd.DataFrame:
    # required columns: ts_ns, bid_px, ask_px (or mid); we compute mid if needed
    if "ts_ns" not in dfq.columns:
        if "ts" in dfq.columns:
            dfq["ts_ns"] = _as_ns(dfq["ts"])
        else:
            raise ValueError("quotes CSV must have ts_ns or ts")
    if "mid" not in dfq.columns:
        need = {"bid_px", "ask_px"}
        if not need.issubset(dfq.columns):
            raise ValueError("quotes CSV needs either mid or (bid_px, ask_px)")
        dfq["mid"] = 0.5 * (dfq["bid_px"].astype(float) + dfq["ask_px"].astype(float))
    out = dfq[["ts_ns", "mid"]].dropna().sort_values("ts_ns").reset_index(drop=True)
    return out

def _load_fills(path: Path, parent_side: Optional[str]) -> pd.DataFrame:
    dff = pd.read_csv(path)
    cols = {c.lower(): c for c in dff.columns}
    # normalize columns
    if "ts_ns" not in dff.columns:
        if "ts" in cols:
            dff["ts_ns"] = _as_ns(dff[cols["ts"]])
        else:
            raise ValueError("fills CSV must have ts_ns or ts")
    price_col = cols.get("px") or cols.get("price") or "price"
    qty_col   = cols.get("qty") or "qty"
    dff.rename(columns={price_col: "price", qty_col: "qty"}, inplace=True)

    # side/sign
    if "side" in cols:
        sc = dff[cols["side"]].astype(str).str.lower()
        sign = sc.map({"b": +1, "buy": +1, "bid": +1, "a": -1, "ask": -1, "sell": -1, "s": -1})
        if sign.isna().any():
            raise ValueError("unknown side values in fills")
        dff["sign"] = sign
    elif parent_side:
        ps = parent_side.strip().lower()
        if ps not in ("buy", "sell"):
            raise ValueError("parent_side must be 'buy' or 'sell'")
        dff["sign"] = +1 if ps == "buy" else -1
    else:
        raise ValueError("fills missing 'side' column; provide parent_side")
    dff = dff[["ts_ns", "price", "qty", "sign"]].astype({"ts_ns":"int64","price":"float64","qty":"float64","sign":"int64"})
    dff.sort_values("ts_ns", inplace=True)
    return dff.reset_index(drop=True)

def _max_drawdown(equity: pd.Series) -> float:
    rollmax = equity.cummax()
    dd = (equity - rollmax)
    return -dd.min() if len(dd) else 0.0

def _turnover(dff: pd.DataFrame) -> float:
    # simple, execution-friendly definition: total notional traded / (|final inventory| * VWAP)
    if dff.empty:
        return 0.0
    total_notional = (dff["price"] * dff["qty"]).sum()
    final_inv = (dff["sign"] * dff["qty"]).sum()
    vwap = (dff["price"] * dff["qty"]).sum() / max(dff["qty"].sum(), 1e-12)
    denom = abs(final_inv) * vwap
    if denom < 1e-12:
        # if you end flat, define turnover as total notional / (mean notional at trade times)
        denom = max((dff["price"] * dff["qty"]).mean(), 1e-12)
    return float(total_notional / denom)

def compute_pnl_and_risk(inp: RiskInputs) -> dict:
    dfq = _mid_from_quotes(pd.read_csv(inp.quotes_csv))
    dff = _load_fills(inp.fills_csv, inp.parent_side)

    # mid at each fill time (nearest quote at or before fill)
    dfq["ts_ns"] = dfq["ts_ns"].astype("int64")
    dff["ts_ns"] = dff["ts_ns"].astype("int64")
    dff = pd.merge_asof(dff, dfq, on="ts_ns", direction="backward")
    if dff["mid"].isna().any():
        dff["mid"] = dff["price"]  # fallback if very early fills precede first quote

    # account process
    cash = 0.0
    inv  = 0.0
    avg_cost = 0.0
    realized = 0.0
    fee_mult = abs(inp.fee_bps) / 1e4

    cash_path, inv_path, mid_path, realized_path, unrealized_path, equity_path, ts_path = [], [], [], [], [], [], []

    # step over fills, but also create an equity curve over all quotes (for drawdown/sharpe)
    # First: compute state at fill times
    for row in dff.itertuples(index=False):
        ts, price, qty, sign, mid = int(row.ts_ns), float(row.price), float(row.qty), int(row.sign), float(row.mid)
        fee = fee_mult * price * qty

        if sign > 0:  # buy
            cash -= price * qty + fee
            new_inv = inv + qty
            avg_cost = (avg_cost * inv + price * qty) / new_inv if new_inv > 1e-12 else price
            inv = new_inv
        else:         # sell
            cash += price * qty - fee
            realized += (price - avg_cost) * min(qty, abs(inv))  # clamp if over-flat
            inv -= qty
            if abs(inv) < 1e-12:
                inv = 0.0  # avoid drift

        unreal = (mid - avg_cost) * inv
        equity = cash + unreal

        ts_path.append(ts)
        cash_path.append(cash)
        inv_path.append(inv)
        mid_path.append(mid)
        realized_path.append(realized)
        unrealized_path.append(unreal)
        equity_path.append(equity)

    # Build a quotes-aligned equity curve (uses last-known account state)
    eq_df = pd.DataFrame({"ts_ns": dfq["ts_ns"]})
    if ts_path:
        state = pd.DataFrame({
            "ts_ns": ts_path,
            "cash": cash_path,
            "inventory": inv_path,
            "mid_at_fill": mid_path,
            "realized": realized_path,
            "unrealized": unrealized_path,
            "equity": equity_path,
        })
        # carry forward last-known state onto quote grid
        eq_df = pd.merge_asof(eq_df, state.sort_values("ts_ns"), on="ts_ns", direction="backward")
        eq_df.fillna(method="ffill", inplace=True)
    else:
        # no fills => flat line at 0
        eq_df[["cash","inventory","realized","unrealized","equity"]] = 0.0

    # attach mid for MTM at all timestamps and recompute equity with current mid
    eq_df = pd.merge(eq_df, dfq, on="ts_ns", how="left")
    eq_df["mid"].fillna(method="ffill", inplace=True)
    eq_df["equity"] = eq_df["cash"] + eq_df["inventory"] * (eq_df["mid"] - (0.0 if abs(avg_cost) < 1e-12 else avg_cost)) + eq_df["realized"]

    # drawdown (absolute PnL units)
    max_dd = _max_drawdown(eq_df["equity"])

    # returns for sharpe-like (use 1s bars from equity)
    eq_df["ts"] = pd.to_datetime(eq_df["ts_ns"], utc=True)
    sec = eq_df.set_index("ts")["equity"].resample("1s").last().dropna()
    rets = sec.pct_change().dropna()
    mean_r, std_r = rets.mean(), rets.std()
    sharpe_like = float(mean_r / std_r * math.sqrt(SECONDS_PER_YEAR_CRYPTO)) if std_r > 0 else float("nan")

    # turnover
    turnover = _turnover(dff)

    # realized/unrealized at end on latest mid
    if not dfq.empty:
        last_mid = float(dfq["mid"].iloc[-1])
    else:
        last_mid = mid_path[-1] if mid_path else 0.0
    unreal_end = (last_mid - avg_cost) * inv
    equity_end = cash + realized + unreal_end

    # write artifacts
    out = pd.DataFrame({
        "ts": pd.to_datetime(eq_df["ts_ns"], utc=True),
        "ts_ns": eq_df["ts_ns"],
        "mid": eq_df["mid"],
        "inventory": eq_df["inventory"],
        "cash": eq_df["cash"],
        "realized": eq_df["realized"],
        "unrealized": eq_df["unrealized"],
        "equity": eq_df["equity"],
    })
    out.sort_values("ts_ns", inplace=True)
    inp.out_table_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(inp.out_table_csv, index=False)

    summary = {
        "final_inventory": float(inv),
        "avg_cost": float(avg_cost),
        "last_mid": float(last_mid),
        "pnl_realized": float(realized),
        "pnl_unrealized": float(unreal_end),
        "pnl_total": float(equity_end),
        "max_drawdown": float(max_dd),
        "turnover": float(turnover),
        "sharpe_like": float(sharpe_like),
        "fee_bps": float(inp.fee_bps),
        "rows_equity": int(len(out)),
        "rows_fills": int(len(dff)),
    }
    with open(inp.out_summary_json, "w") as f:
        json.dump(summary, f, indent=2)
    return summary
