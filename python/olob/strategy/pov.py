# python/olob/strategy/pov.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Optional
import pandas as pd


def _cfg_get(cfg: Any, key: str, default: Any = None) -> Any:
    if isinstance(cfg, dict):
        return cfg.get(key, default)
    return getattr(cfg, key, default)


def _extract_bar_args(*args, **kwargs):
    """
    Backtest calls on_bar(now_ns, t0_ns=..., t1_ns=..., bar_sec=..., bar_trades=..., queue_model=..., quotes=...)
    """
    now_ns = args[0] if len(args) >= 1 else kwargs.get("now_ns")
    t0_ns = kwargs.get("t0_ns", None)
    t1_ns = kwargs.get("t1_ns", None)
    bar_sec = kwargs.get("bar_sec", None)
    bar_trades = kwargs.get("bar_trades", None)

    bar_quotes = kwargs.get("bar_quotes", None)
    if bar_quotes is None:
        bar_quotes = kwargs.get("quotes", None)

    queue_model = kwargs.get("queue_model", None)
    return now_ns, t0_ns, t1_ns, bar_sec, bar_trades, bar_quotes, queue_model


@dataclass
class POVParams:
    parent_qty: float
    target_pov: float     # e.g., 0.1 for 10%
    min_clip: float
    cooldown_ms: int
    side: str


class StrategyPOV:
    """
    Simple POV: at each bar, target 'target_pov' × traded volume during the bar.
    If bar_trades is unavailable, fall back to a small minimum clip.
    """
    def __init__(self, cfg: Any, quotes: Optional[pd.DataFrame] = None, trades: Optional[pd.DataFrame] = None):
        self.cfg = cfg
        self.quotes = quotes
        self.trades = trades

        parent_qty = float(_cfg_get(cfg, "qty", 1.0))
        target_pov = float(_cfg_get(cfg, "target_pov", 0.1))
        min_clip   = float(_cfg_get(cfg, "min_clip", 0.01))
        cooldown   = int(_cfg_get(cfg, "cooldown_ms", 0))
        side       = str(_cfg_get(cfg, "side", "buy")).lower()

        self.params = POVParams(
            parent_qty=parent_qty,
            target_pov=target_pov,
            min_clip=min_clip,
            cooldown_ms=cooldown,
            side=side,
        )

        self.done_qty = 0.0
        self._planned = 0.0  # next clip to emit via on_tick

    def on_fill(self, qty: float, px: float) -> None:
        self.done_qty += float(qty)
        if self.done_qty > self.params.parent_qty:
            self.done_qty = self.params.parent_qty

    def remaining(self) -> float:
        return max(0.0, self.params.parent_qty - self.done_qty)

    def on_bar(self, *args, **kwargs) -> None:
        now_ns, t0_ns, t1_ns, bar_sec, bar_trades, bar_quotes, queue_model = _extract_bar_args(*args, **kwargs)
        if self.remaining() <= 0:
            self._planned = 0.0
            return

        # Volume seen in this bar (if provided)
        bar_vol = 0.0
        if isinstance(bar_trades, pd.DataFrame) and not bar_trades.empty:
            if "qty" in bar_trades.columns:
                bar_vol = float(bar_trades["qty"].sum())
            else:
                # Fallback: count trades as 1 unit each if no qty column
                bar_vol = float(len(bar_trades))

        # POV target
        desired = max(self.params.min_clip, self.params.target_pov * bar_vol)

        # Do not exceed remaining
        desired = min(desired, self.remaining())

        self._planned = desired

    def on_tick(self, now_ns: int) -> float:
        # Emit planned once per bar
        planned = self._planned
        if planned <= 0:
            return 0.0
        self._planned = 0.0
        return float(min(planned, self.remaining()))
