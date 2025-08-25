# python/orderbook_tools/l2_topn_patch.py
from __future__ import annotations
from typing import Iterable, Tuple, Any, List

def _materialize_levels(container: Any) -> List[Tuple[int, float]]:
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
                px = int(it[0]); qty = float(it[1])   # tuple
                out.append((px, qty)); continue
            except Exception:
                pass
            try:
                px = int(getattr(it, "price_ticks")) # object
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

        if not levels:
            it_name = "iter_bid_levels" if side.upper().startswith("B") else "iter_ask_levels"
            it = getattr(self, it_name, None)
            if callable(it):
                levels = _materialize_levels(it())

        if side.upper().startswith("B"):
            levels.sort(key=lambda x: x[0], reverse=True)   # high→low
        else:
            levels.sort(key=lambda x: x[0])                 # low→high

        out = []
        for px, qty in levels:
            if qty <= 0:
                continue
            out.append((px, qty))
            if len(out) >= n:
                break
        return out

    def bids_topN(self, n: int): return _topN(self, "B", n)
    def asks_topN(self, n: int): return _topN(self, "A", n)

    setattr(L2Book, "bids_topN", bids_topN)
    setattr(L2Book, "asks_topN", asks_topN)
