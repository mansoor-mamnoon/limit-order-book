from hypothesis import given, strategies as st
from olob import Book, NewOrder, Side

def mk(seq, ts, oid, uid, side, px, qty, flags=0):
    o = NewOrder()
    o.seq = seq; o.ts = ts; o.id = oid; o.user = uid
    o.side = side; o.price = px; o.qty = qty; o.flags = flags
    return o

@given(st.lists(
    st.tuples(
        st.integers(min_value=0, max_value=10**6),   # price
        st.integers(min_value=1, max_value=1000),    # qty
        st.sampled_from([Side.Bid, Side.Ask])        # side
    ),
    min_size=1, max_size=50
))
def test_total_qty_conservation_for_noncrossing_inserts(entries):
    b = Book()
    seq = 1
    total_in = 0
    for px, qty, side in entries:
        # Force non-crossing: bids really low, asks really high
        if side == Side.Bid:
            px = -abs(px) - 1000
        else:
            px = abs(px) + 1000
        total_in += qty
        b.submit_limit(mk(seq, seq, seq, seq, side, px, qty))
        seq += 1
    # No trades should have occurred; sum of all visible level qty equals total inserted
    l2 = b.l2(depth=10_000)
    total_levels = sum(q for _, q in l2["bids"]) + sum(q for _, q in l2["asks"])
    assert total_levels == total_in
