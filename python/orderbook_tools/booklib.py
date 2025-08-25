from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional
from bisect import bisect_left
from decimal import Decimal, getcontext
import zlib

getcontext().prec = 28  # ample precision for crypto prices/qty

Side = str  # "B" or "A"

def to_ticks(price: Decimal, tick: Decimal) -> int:
    """Convert price to integer ticks on the given grid."""
    q = (price / tick).quantize(Decimal("1."))
    return int(q)

def from_ticks(ticks: int, tick: Decimal) -> Decimal:
    return (Decimal(ticks) * tick).normalize()

@dataclass
class TopLevel:
    price_ticks: int
    qty: Decimal

class L2Book:
    """
    Deterministic L2 order book, stores prices as integer ticks.
    Bids kept in descending order, asks in ascending order.
    """
    def __init__(self, tick_size: Decimal, max_depth: int = 5000):
        self.tick = Decimal(tick_size)
        self.max_depth = max_depth
        self._bids: Dict[int, Decimal] = {}
        self._asks: Dict[int, Decimal] = {}
        self._bids_px: List[int] = []  # descending
        self._asks_px: List[int] = []  # ascending

    # ---------- internals ----------
    def _set_level(self, side: Side, ticks: int, qty: Decimal):
        book = self._bids if side == "B" else self._asks
        arr  = self._bids_px if side == "B" else self._asks_px

        if qty <= 0:
            if ticks in book:
                del book[ticks]
                i = self._index_of(arr, ticks, side)
                if i is not None:
                    arr.pop(i)
            return

        existed = ticks in book
        book[ticks] = qty
        if not existed:
            if side == "B":
                # keep descending by inserting using negated key
                ins = bisect_left([-p for p in arr], -ticks)
                arr.insert(ins, ticks)
            else:
                ins = bisect_left(arr, ticks)
                arr.insert(ins, ticks)

        # trim if needed
        if len(arr) > self.max_depth:
            if side == "B":
                arr[:] = arr[:self.max_depth]
                self._bids = {p: self._bids[p] for p in arr}
            else:
                arr[:] = arr[:self.max_depth]
                self._asks = {p: self._asks[p] for p in arr}

    @staticmethod
    def _index_of(arr: List[int], key: int, side: Side) -> Optional[int]:
        if side == "B":
            neg = [-p for p in arr]
            j = bisect_left(neg, -key)
            if 0 <= j < len(arr) and arr[j] == key:
                return j
            return None
        j = bisect_left(arr, key)
        if 0 <= j < len(arr) and arr[j] == key:
            return j
        return None

    # ---------- public ----------
    def apply_snapshot(self, bids: List[Tuple[Decimal, Decimal]],
                       asks: List[Tuple[Decimal, Decimal]]):
        self._bids.clear(); self._asks.clear()
        self._bids_px.clear(); self._asks_px.clear()
        for px, qty in bids:
            self._set_level("B", to_ticks(px, self.tick), qty)
        for px, qty in asks:
            self._set_level("A", to_ticks(px, self.tick), qty)

    def apply_updates(self,
                      bids: List[Tuple[Decimal, Decimal]],
                      asks: List[Tuple[Decimal, Decimal]]):
        for px, qty in bids:
            self._set_level("B", to_ticks(px, self.tick), qty)
        for px, qty in asks:
            self._set_level("A", to_ticks(px, self.tick), qty)

    def best_bid(self) -> Optional[TopLevel]:
        if not self._bids_px: return None
        p = self._bids_px[0]
        return TopLevel(p, self._bids[p])

    def best_ask(self) -> Optional[TopLevel]:
        if not self._asks_px: return None
        p = self._asks_px[0]
        return TopLevel(p, self._asks[p])

    def top_n(self, side: Side, n: int) -> List[TopLevel]:
        arr  = self._bids_px if side == "B" else self._asks_px
        book = self._bids if side == "B" else self._asks
        out: List[TopLevel] = []
        for p in arr[:n]:
            out.append(TopLevel(p, book[p]))
        return out

    def checksum(self, levels: int = 10) -> int:
        """
        Deterministic CRC32 over top-N levels: "<ticks>:<qty>:" … bids first, then asks.
        """
        parts: List[str] = []
        for t in self.top_n("B", levels):
            parts.append(f"{t.price_ticks}:{str(t.qty)}:")
        for t in self.top_n("A", levels):
            parts.append(f"{t.price_ticks}:{str(t.qty)}:")
        return zlib.crc32("".join(parts).encode("utf-8"))

    def as_price_qty(self, t: TopLevel) -> Tuple[Decimal, Decimal]:
        return from_ticks(t.price_ticks, self.tick), t.qty

    def snapshot_tops(self, n: int = 10):
        bb = [(from_ticks(x.price_ticks, self.tick), x.qty) for x in self.top_n("B", n)]
        aa = [(from_ticks(x.price_ticks, self.tick), x.qty) for x in self.top_n("A", n)]
        return {"bids": bb, "asks": aa}
