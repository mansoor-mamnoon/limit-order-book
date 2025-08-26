# python/olob/strategy/iceberg.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Optional, Dict
import pandas as pd


def _cfg_get(cfg: Any, key: str, default: Any = None) -> Any:
    if isinstance(cfg, dict):
        return cfg.get(key, default)
    return getattr(cfg, key, default)


def _extract_bar_args(*args, **kwargs):
    """
    Backtest calls on_bar(now_ns, t0_ns=..., t1_ns=..., bar_sec=..., bar_trades=..., queue_model=..., quotes=...)
    Avoid boolean 'or' with DataFrames; use explicit None checks.
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
class IcebergParams:
    parent_qty: float
    display: float
    replenish: float  # fraction of display to post on replenish [0..1]
    min_clip: float
    side: str


class StrategyIceberg:
    """
    Minimal iceberg:
      - Aims to place 'display' sized clips.
      - When a planned child is consumed (by our taker model), we 'replenish' next bar.
      - We return 'planned' qty via on_tick() once per bar; backtest may enforce safety fill guard anyway.
    """
    def __init__(self, cfg: Any, quotes: Optional[pd.DataFrame] = None):
        self.cfg = cfg
        self.quotes = quotes

        parent_qty = float(_cfg_get(cfg, "qty", 1.0))
        display    = float(_cfg_get(cfg, "display", _cfg_get(cfg, "display_size", 0.1)))
        replenish  = float(_cfg_get(cfg, "replenish", 1.0))   # 1.0 = full display each time
        min_clip   = float(_cfg_get(cfg, "min_clip", 0.01))
        side       = str(_cfg_get(cfg, "side", "buy")).lower()

        self.params = IcebergParams(
            parent_qty=parent_qty,
            display=display,
            replenish=replenish,
            min_clip=min_clip,
            side=side,
        )

        self.done_qty = 0.0
        self._planned = 0.0  # next clip to emit via on_tick

    # Backtest may call this after fills
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

        # Plan a new replenishment clip: min(display*replenish, remaining), >= min_clip
        target = max(self.params.min_clip, min(self.remaining(), self.params.display * max(0.0, min(1.0, self.params.replenish))))
        self._planned = target

    def on_tick(self, now_ns: int) -> float:
        # Consume planned size once per bar; then 0 until next on_bar
        planned = self._planned
        if planned <= 0:
            return 0.0
        self._planned = 0.0
        return float(min(planned, self.remaining()))
