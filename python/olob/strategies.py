# python/olob/strategies.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, Optional
import numpy as np
import pandas as pd
import math
import datetime as dt

Side = str  # "buy" or "sell"

@dataclass
class CostModel:
    tick: float = 0.01         # price quant
    lot: float = 0.0001        # qty quant
    taker_bps: float = 1.0     # + means fee, - means rebate
    maker_bps: float = -0.0
    fixed_latency_ms: int = 0  # applied to order arrival

    def quant_price(self, px: float) -> float:
        return round(px / self.tick) * self.tick

    def quant_qty(self, qty: float) -> float:
        # floor to lot to avoid over-buy/sell
        lots = math.floor(qty / self.lot)
        return max(0.0, lots * self.lot)

@dataclass
class StrategyConfig:
    name: str
    type: str                  # "twap" or "vwap"
    side: Side                 # "buy" or "sell"
    qty: float                 # total parent quantity
    start: str                 # ISO time (UTC recommended)
    end: str
    bar_sec: int = 60
    min_clip: float = 0.0
    cooldown_ms: int = 0
    force_taker: bool = True   # marketable by default
    cost: Dict[str, Any] = None

    def to_cost(self) -> CostModel:
        cost = self.cost or {}
        return CostModel(
            tick=cost.get("tick", 0.01),
            lot=cost.get("lot", 0.0001),
            taker_bps=cost.get("taker_bps", 1.0),
            maker_bps=cost.get("maker_bps", -0.0),
            fixed_latency_ms=cost.get("latency_ms", cost.get("fixed_latency_ms", 0)),
        )

def parse_time_ns(s: str) -> int:
    # Accepts "YYYY-MM-DDTHH:MM:SS[.fff]Z" or with timezone-naive (assume UTC)
    ts = pd.Timestamp(s)
    if ts.tz is None:
        ts = ts.tz_localize("UTC")
    return int(ts.value)  # ns

class Strategy:
    """Base interface."""
    def __init__(self, cfg: StrategyConfig):
        self.cfg = cfg
        self.cost = cfg.to_cost()
        self.start_ns = parse_time_ns(cfg.start)
        self.end_ns   = parse_time_ns(cfg.end)
        self.bar_ns   = cfg.bar_sec * 1_000_000_000
        self.side_sign = +1 if cfg.side.lower() == "buy" else -1
        self.parent_qty = cfg.qty
        self.filled_qty = 0.0
        self.last_send_ns: Optional[int] = None
        self.next_bar_edge = self.start_ns + self.bar_ns
        self.target_cum_by_bar: Optional[pd.Series] = None  # set by subclass

    # Hooks
    def on_bar(self, now_ns: int) -> None:
        pass

    def on_tick(self, now_ns: int) -> float:
        """Return child qty desired (raw, unquantized)."""
        return 0.0

    def on_fill(self, qty: float, px: float) -> None:
        self.filled_qty += qty

    # Helpers
    def remaining(self) -> float:
        return max(0.0, self.parent_qty - self.filled_qty)

    def cooled_down(self, now_ns: int) -> bool:
        if self.last_send_ns is None:
            return True
        return (now_ns - self.last_send_ns) >= (self.cfg.cooldown_ms * 1_000_000)

    def clip_qty(self, desired: float) -> float:
        if self.cfg.min_clip <= 0:
            return min(desired, self.remaining())
        return min(max(desired, self.cfg.min_clip), self.remaining())

class TWAPStrategy(Strategy):
    def on_tick(self, now_ns: int) -> float:
        if not self.cooled_down(now_ns) or self.remaining() <= 0:
            return 0.0
        # Remaining time fraction
        rem_ns = max(1, self.end_ns - now_ns)
        total_rem_bars = rem_ns / self.bar_ns
        target_this_bar = self.remaining() / max(1.0, total_rem_bars)
        return self.clip_qty(target_this_bar)

class VWAPStrategy(Strategy):
    def __init__(self, cfg: StrategyConfig, trades: Optional[pd.DataFrame]):
        super().__init__(cfg)
        self._build_vwap_schedule(trades)

    def _build_vwap_schedule(self, trades: Optional[pd.DataFrame]):
        # Build volume weights per bar using trades ts_ns within [start, end)
        if trades is None or trades.empty:
            # fallback to uniform (TWAP-like)
            n_bars = max(1, math.ceil((self.end_ns - self.start_ns) / self.bar_ns))
            w = pd.Series(1.0, index=pd.RangeIndex(n_bars))
        else:
            t = trades[(trades["ts_ns"] >= self.start_ns) & (trades["ts_ns"] < self.end_ns)].copy()
            if t.empty:
                n_bars = max(1, math.ceil((self.end_ns - self.start_ns) / self.bar_ns))
                w = pd.Series(1.0, index=pd.RangeIndex(n_bars))
            else:
                t["bar"] = ((t["ts_ns"] - self.start_ns) // self.bar_ns).astype(int)
                # use absolute qty for volume; if no qty column, assume 1
                vol = t["qty"] if "qty" in t.columns else pd.Series(1.0, index=t.index)
                w = vol.groupby(t["bar"]).sum()
        w = w.reindex(range(int(((self.end_ns - self.start_ns)+self.bar_ns-1)//self.bar_ns)), fill_value=0.0)
        if w.sum() <= 0:
            w[:] = 1.0
        w = w / w.sum()
        self.target_cum_by_bar = w.cumsum() * self.parent_qty

    def on_bar(self, now_ns: int) -> None:
        self.next_bar_edge = ((now_ns - self.start_ns) // self.bar_ns + 1) * self.bar_ns + self.start_ns

    def on_tick(self, now_ns: int) -> float:
        if not self.cooled_down(now_ns) or self.remaining() <= 0:
            return 0.0
        bar_idx = int((now_ns - self.start_ns) // self.bar_ns)
        target_cum = float(self.target_cum_by_bar.iloc[min(bar_idx, len(self.target_cum_by_bar)-1)])
        need = max(0.0, target_cum - self.filled_qty)
        return self.clip_qty(need)
