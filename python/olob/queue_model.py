# python/olob/queue_model.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
import pandas as pd
import numpy as np


@dataclass
class QueueApprox:
    """
    Simple queue-position approximation based on:
      - Observed bar traded volume (proxy for churn)
      - L1 displayed size (proxy for queue depth)
    """
    alpha: float = 0.5  # smoothing factor
    last_position: Optional[float] = None

    def update(self, bar_quotes: Optional[pd.DataFrame], bar_trades: Optional[pd.DataFrame]) -> float:
        # Estimate L1 size mean
        l1 = 0.0
        if (
            bar_quotes is not None
            and not bar_quotes.empty
            and {"bid_sz", "ask_sz"}.issubset(bar_quotes.columns)
        ):
            bs = pd.to_numeric(bar_quotes["bid_sz"], errors="coerce").fillna(0)
            asz = pd.to_numeric(bar_quotes["ask_sz"], errors="coerce").fillna(0)
            if len(bs) > 0:
                l1 = float(pd.concat([bs, asz], axis=1).mean(axis=1).mean())

        # Traded volume in bar
        vol = 0.0
        if bar_trades is not None and not bar_trades.empty:
            if "qty" in bar_trades.columns:
                vol = float(pd.to_numeric(bar_trades["qty"], errors="coerce").fillna(0).sum())
            else:
                vol = float(len(bar_trades))

        # position ~ depth - churn
        pos_est = max(0.0, l1 - vol)
        if self.last_position is None:
            self.last_position = pos_est
        else:
            self.last_position = self.alpha * pos_est + (1.0 - self.alpha) * self.last_position
        return float(self.last_position)
