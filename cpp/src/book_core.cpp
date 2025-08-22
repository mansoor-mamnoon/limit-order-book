#include "lob/book_core.hpp"

namespace lob {

// Consume from the opposite book while we can cross price (or unbounded for market).
// px_limit:
//   - LIMIT: worst acceptable price (buyer accepts asks <= px_limit; seller accepts bids >= px_limit)
//   - MARKET: use +/- infinity so only book emptiness/size stops us.
Quantity BookCore::match_against(Side taker_side,
                                 UserId taker_user,
                                 uint32_t taker_flags,
                                 Quantity want,
                                 Tick px_limit) {
  Quantity filled = 0;
  auto& opp = (taker_side == Side::Bid) ? asks_ : bids_;

  const Tick MINP = std::numeric_limits<Tick>::min();
  const Tick MAXP = std::numeric_limits<Tick>::max();

  auto crosses = [&](Tick best_px) -> bool {
    if (taker_side == Side::Bid) return best_px <= px_limit; // buy vs asks
    else                         return best_px >= px_limit; // sell vs bids
  };

  while (want > 0) {
    Tick best_px = (taker_side == Side::Bid) ? opp.best_ask() : opp.best_bid();
    if ((taker_side == Side::Bid && best_px == MAXP) ||
        (taker_side == Side::Ask && best_px == MINP)) {
      break; // empty
    }
    if (!crosses(best_px)) break;

    LevelFIFO& L = opp.get_level(best_px);
    OrderNode* h = L.head;
    if (!h) {
      // Level is empty: advance best to next non-empty level
      Tick nxt = (taker_side == Side::Bid)
                 ? opp.next_ask_after(best_px)
                 : opp.next_bid_before(best_px);
      if (taker_side == Side::Bid) opp.set_best_ask(nxt); else opp.set_best_bid(nxt);
      continue;
    }

    // Self-trade prevention: if taker flags request STP and owner matches
    if ((taker_flags & STP) && h->user == taker_user) {
      // Cancel resting order (no trade), advance within the level
      erase(L, h);
      id_index_.erase(h->id);
      delete h;

      if (!L.head) {
        Tick nxt = (taker_side == Side::Bid)
                   ? opp.next_ask_after(best_px)
                   : opp.next_bid_before(best_px);
        if (taker_side == Side::Bid) opp.set_best_ask(nxt); else opp.set_best_bid(nxt);
      }
      continue; // still want the same quantity
    }

    // Trade with head (FIFO)
    const Quantity tr = std::min(want, h->qty);
    h->qty      -= tr;
    L.total_qty -= tr;
    filled      += tr;
    want        -= tr;

    if (h->qty == 0) {
      // Fully consumed
      erase(L, h);
      id_index_.erase(h->id);
      delete h;

      if (!L.head) {
        Tick nxt = (taker_side == Side::Bid)
                   ? opp.next_ask_after(best_px)
                   : opp.next_bid_before(best_px);
        if (taker_side == Side::Bid) opp.set_best_ask(nxt); else opp.set_best_bid(nxt);
      }
    }
  }

  return filled;
}

ExecResult BookCore::submit_market(const NewOrder& o) {
  ExecResult r{};
  if (o.qty <= 0) return r;

  const Tick bound = (o.side == Side::Bid)
      ? std::numeric_limits<Tick>::max()
      : std::numeric_limits<Tick>::min();

  r.filled    = match_against(o.side, o.user, o.flags, o.qty, bound);
  r.remaining = o.qty - r.filled; // never rests
  return r;
}

ExecResult BookCore::submit_limit(const NewOrder& o) {
  ExecResult r{};
  if (o.qty <= 0) return r;

  // First, try to execute within price constraint
  r.filled = match_against(o.side, o.user, o.flags, o.qty, o.price);
  Quantity leftover = o.qty - r.filled;
  if (leftover <= 0) { r.remaining = 0; return r; }

  // Rest the remainder at its price
  auto& same = (o.side == Side::Bid) ? bids_ : asks_;
  LevelFIFO& L = same.get_level(o.price);

  OrderNode* n = new OrderNode{
      /*id*/    o.id,
      /*user*/  o.user,
      /*qty*/   leftover,
      /*ts*/    o.ts,
      /*flags*/ o.flags,
      /*prev*/  nullptr,
      /*next*/  nullptr
  };
  // If level was empty before, enqueue will mark it non-empty via L.head check
  enqueue(L, n);

  // Improve top-of-book if better
  if (o.side == Side::Bid) {
    Tick best = same.best_bid();
    if (o.price > best) same.set_best_bid(o.price);
  } else {
    Tick best = same.best_ask();
    if (o.price < best) same.set_best_ask(o.price);
  }

  id_index_[o.id] = IdEntry{ o.side, o.price, n };
  r.remaining = leftover;
  return r;
}

bool BookCore::cancel(OrderId id) {
  auto it = id_index_.find(id);
  if (it == id_index_.end()) return false;
  const IdEntry e = it->second;

  auto& book = (e.side == Side::Bid) ? bids_ : asks_;
  LevelFIFO& L = book.get_level(e.px);
  OrderNode* n = e.node;
  if (!n) return false;

  const bool was_best = (e.side == Side::Bid) ? (book.best_bid() == e.px)
                                              : (book.best_ask() == e.px);

  erase(L, n);
  delete n;
  id_index_.erase(it);

  if (!L.head && was_best) {
    Tick nxt = (e.side == Side::Bid)
               ? book.next_bid_before(e.px)   // for bids, next lower
               : book.next_ask_after(e.px);   // for asks, next higher
    if (e.side == Side::Bid) book.set_best_bid(nxt);
    else                     book.set_best_ask(nxt);
  }
  return true;
}

ExecResult BookCore::modify(const NewOrder& replacement) {
  // replacement.id must exist; behavior:
  // - if price unchanged: adjust size in-place (<=0 -> cancel)
  // - if price changed: cancel old, resubmit as LIMIT with new price/qty (can cross)
  ExecResult r{};
  auto it = id_index_.find(replacement.id);
  if (it == id_index_.end()) return r; // treat as no-op

  IdEntry e = it->second;
  auto& side_book = (e.side == Side::Bid) ? bids_ : asks_;
  LevelFIFO& L = side_book.get_level(e.px);
  OrderNode* n = e.node;
  if (!n) return r;

  if (replacement.price == e.px) {
    // same price: adjust size in place
    Quantity new_qty = replacement.qty;
    if (new_qty <= 0) { // becomes cancellation
      const bool was_best = (e.side == Side::Bid) ? (side_book.best_bid() == e.px)
                                                  : (side_book.best_ask() == e.px);
      erase(L, n);
      delete n;
      id_index_.erase(it);
      if (!L.head && was_best) {
        Tick nxt = (e.side == Side::Bid)
                   ? side_book.next_bid_before(e.px)
                   : side_book.next_ask_after(e.px);
        if (e.side == Side::Bid) side_book.set_best_bid(nxt);
        else                     side_book.set_best_ask(nxt);
      }
      return r; // no fills; effectively cancel
    } else {
      // Adjust total and node qty; keep time priority
      L.total_qty += (new_qty - n->qty);
      n->qty = new_qty;
      n->ts  = replacement.ts;
      n->flags = replacement.flags;
      return r; // no fills on in-place modify
    }
  }

  // Price changed: cancel old, then resubmit as new LIMIT (can cross)
  // Save side from original entry; replacement.side should match.
  const Side s = e.side;

  // Remove old
  const bool was_best = (s == Side::Bid) ? (side_book.best_bid() == e.px)
                                         : (side_book.best_ask() == e.px);
  erase(L, n);
  delete n;
  id_index_.erase(it);
  if (!L.head && was_best) {
    Tick nxt = (s == Side::Bid)
               ? side_book.next_bid_before(e.px)
               : side_book.next_ask_after(e.px);
    if (s == Side::Bid) side_book.set_best_bid(nxt);
    else                side_book.set_best_ask(nxt);
  }

  // Resubmit as a fresh LIMIT with same id/user (loses time priority)
  NewOrder neo = replacement;
  neo.side = s; // ensure same side as original
  r = submit_limit(neo);
  return r;
}

} // namespace lob
