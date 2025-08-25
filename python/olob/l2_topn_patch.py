# python/olob/l2_topn_patch.py
from __future__ import annotations
from typing import Iterable, Tuple, Any, Dict, List

def _materialize_levels(container: Any) -> List[Tuple[int, float]]:
    """
    Try to turn a levels container into a list of (price_ticks:int, qty:float).
    Supports:
      - dict-like: {price_ticks -> level_obj or qty}
      - iterable of (price_ticks, qty)
      - iterable of level_obj with .price_ticks/.qty
    """
    out: List[Tuple[int, float]] = []
    if container is None:
        return out

    # dict-like
    if hasattr(container, "items"):
        for k, v in container.items():
            try:
                px = int(getattr(v, "price_ticks", k))
                qty = float(getattr(v, "total_qty", getattr(v, "qty", v)))
                out.append((px, qty))
            except Exception:
                continue
        return out

    # iterable
    try:
        for it in container:
            try:
                # tuple (ticks, qty)
                px = int(it[0]); qty = float(it[1])
                out.append((px, qty)); continue
            except Exception:
                pass
            # object with fields
            try:
                px = int(getattr(it, "price_ticks"))
                qty = float(getattr(it, "total_qty", getattr(it, "qty")))
                out.append((px, qty))
            except Exception:
                continue
    except Exception:
        pass
    return out

def add_topn_methods(L2Book):
    """Monkey-patch bids_topN/asks_topN onto the given L2Book class."""
    def _topN(self, side: str, n: int):
        # Try common containers on the Python L2Book
        containers = []
        if side.upper().startswith("B"):
            for name in ("bids", "bid_levels", "levels_bid", "levels_bids"):
                if hasattr(self, name):
                    containers.append(getattr(self, name))
        else:
            for name in ("asks", "ask_levels", "levels_ask", "levels_asks"):
                if hasattr(self, name):
                    containers.append(getattr(self, name))

        levels: List[Tuple[int, float]] = []
        for c in containers:
            levels = _materialize_levels(c)
            if levels:
                break

        # If the class exposes an iterator, try that last.
        if not levels:
            it_name = "iter_bid_levels" if side.upper().startswith("B") else "iter_ask_levels"
            it = getattr(self, it_name, None)
            if callable(it):
                levels = _materialize_levels(it())

        # Sort best→worse and take top-n
        if side.upper().startswith("B"):
            levels.sort(key=lambda x: x[0], reverse=True)   # bids: high→low
        else:
            levels.sort(key=lambda x: x[0])                 # asks: low→high

        out = []
        for px, qty in levels:
            if qty <= 0:
                continue
            out.append((px, qty))
            if len(out) >= n:
                break
        return out

    def bids_topN(self, n: int):
        return _topN(self, "B", n)

    def asks_topN(self, n: int):
        return _topN(self, "A", n)

    # Attach
    setattr(L2Book, "bids_topN", bids_topN)
    setattr(L2Book, "asks_topN", asks_topN)
