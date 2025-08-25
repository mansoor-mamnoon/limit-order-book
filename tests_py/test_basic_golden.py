from olob import Book, NewOrder, ModifyOrder, Side, STP, POST_ONLY

def mk(ts, oid, uid, side, px, qty, flags=0, seq=0):
    o = NewOrder()
    o.seq = seq; o.ts = ts; o.id = oid; o.user = uid
    o.side = side; o.price = px; o.qty = qty; o.flags = flags
    return o

def test_limit_crosses_and_rests_fifo():
    b = Book()
    # Add resting ask @101 x 5
    r = b.submit_limit(mk(1, 1, 42, Side.Ask, 101, 5))
    assert r.filled == 0 and r.remaining == 5
    # Aggressive bid @103 x 8 should fill 5 and rest 3 @103
    r2 = b.submit_limit(mk(2, 2, 7, Side.Bid, 103, 8))
    assert r2.filled == 5 and r2.remaining == 3
    l1 = b.l1()
    assert l1["best_ask_px"] is None
    assert l1["best_bid_px"] == 103 and l1["best_bid_qty"] == 3

def test_stp_cancels_self_trade():
    b = Book()
    # User 9 posts ask 100 x 4
    b.submit_limit(mk(1, 11, 9, Side.Ask, 100, 4))
    # Same user 9 tries to buy @101 x 10 with STP -> cancels resting instead of trading
    r = b.submit_limit(mk(2, 12, 9, Side.Bid, 101, 10, flags=STP))
    # No fills because resting same-owner gets canceled
    assert r.filled == 0
    l1 = b.l1()
    assert l1["best_ask_px"] is None  # the resting ask was canceled

def test_post_only_rejects_if_cross():
    b = Book()
    b.submit_limit(mk(1, 1, 1, Side.Ask, 100, 1))
    # Bid @100 with POST_ONLY would cross -> should not rest nor trade
    r = b.submit_limit(mk(2, 2, 2, Side.Bid, 100, 1, flags=POST_ONLY))
    assert r.filled == 0 and r.remaining == 1
    l1 = b.l1()
    assert l1["best_bid_px"] is None  # did not rest
