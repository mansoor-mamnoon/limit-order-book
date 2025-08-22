#include "lob/book_core.hpp"
#include <algorithm>
#include <limits>

namespace lob {

// Day-2: no-op placeholder (future: scan to next non-empty price in O(1))
void BookCore::refresh_best_after_depletion(Side /*s*/, Tick /*emptied_px*/) {
  // intentionally empty
}

// Consume from opposite book while we can cross price (or unbounded for market).
// px_limit:
//   - LIMIT order: worst acceptable price (bid taker accepts asks <= px_limit;
//                  ask taker accepts bids >= px_limit)
//   - MARKET order: +/- infinity sentinels so only book emptiness stops us.
Quantity BookCore::match_against(Side taker_side, Quantity qty, Tick px_limit) {
  Quantity filled = 0;
  auto& opp = (taker_side == Side::Bid) ? asks_ : bids_;

  const Tick MINP = std::numeric_limits<Tick>::min();
  const Tick MAXP = std::numeric_limits<Tick>::max();

  auto crosses = [&](Tick best_px) -> bool {
    if (taker_side == Side::Bid) return best_px <= px_limit; // buyer
    else                         return best_px >= px_limit; // seller
  };

  while (qty > 0) {
    // Check if opposite side is empty via sentinels
    Tick best_px = (taker_side == Side::Bid) ? opp.best_ask() : opp.best_bid();
    if ((taker_side == Side::Bid && best_px == MAXP) ||
        (taker_side == Side::Ask && best_px == MINP)) {
      break; // nothing to match against
    }

    // Respect price constraint (for MARKET we passed ±inf so this is always true)
    if (!crosses(best_px)) break;

    LevelFIFO& L = opp.get_level(best_px);
    OrderNode* h = L.head;
    if (!h) {
      // Level empty—mark best empty and try again
      if (taker_side == Side::Bid) opp.set_best_ask(MAXP);
      else                         opp.set_best_bid(MINP);
      continue;
    }

    // Trade with the head (FIFO)
    const Quantity tr = std::min(qty, h->qty);
    h->qty      -= tr;
    L.total_qty -= tr;
    filled      += tr;
    qty         -= tr;

    if (h->qty == 0) {
      // Fully consumed: unlink, update id index, free
      erase(L, h);
      id_index_.erase(h->id);
      delete h;

      // If the whole price level emptied, mark best as empty (sentinel).
      if (!L.head) {
        if (taker_side == Side::Bid) opp.set_best_ask(MAXP);
        else                         opp.set_best_bid(MINP);
        // Optional future improvement:
        // refresh_best_after_depletion((taker_side == Side::Bid) ? Side::Ask : Side::Bid, best_px);
      }
    }
    // else: partial on head; keep same best_px and continue
  }

  return filled;
}

ExecResult BookCore::submit_market(const NewOrder& o) {
  ExecResult r{};
  if (o.qty <= 0) return r;

  const Tick bound = (o.side == Side::Bid)
                   ? std::numeric_limits<Tick>::max()
                   : std::numeric_limits<Tick>::min();

  r.filled    = match_against(o.side, o.qty, bound);
  r.remaining = o.qty - r.filled; // leftover is unfilled (never rests)
  return r;
}

ExecResult BookCore::submit_limit(const NewOrder& o) {
  ExecResult r{};
  if (o.qty <= 0) return r;

  // Trade first within price constraint
  r.filled = match_against(o.side, o.qty, o.price);
  Quantity leftover = o.qty - r.filled;

  if (leftover <= 0) {
    r.remaining = 0;
    return r; // fully filled immediately
  }

  // Rest the remainder at its price (tail of FIFO on same side)
  auto& same = (o.side == Side::Bid) ? bids_ : asks_;
  LevelFIFO& L = same.get_level(o.price);

  // Allocate intrusive node
  OrderNode* n = new OrderNode{
      /*id*/    o.id,
      /*user*/  o.user,
      /*qty*/   leftover,
      /*ts*/    o.ts,
      /*flags*/ o.flags,
      /*prev*/  nullptr,
      /*next*/  nullptr
  };

  // enqueue at tail (FIFO)
  // (embedded helper keeps O(1))
  {
    n->next = nullptr;
    n->prev = L.tail;
    if (L.tail) L.tail->next = n; else L.head = n;
    L.tail = n;
    L.total_qty += n->qty;
  }

  // Improve top-of-book if this price is better than current best (or side was empty)
  if (o.side == Side::Bid) {
    Tick best = same.best_bid();
    if (o.price > best) same.set_best_bid(o.price);
  } else {
    Tick best = same.best_ask();
    if (o.price < best) same.set_best_ask(o.price);
  }

  // Index for O(1) cancels/modifies later
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

  // unlink and free
  if (n->prev) n->prev->next = n->next; else L.head = n->next;
  if (n->next) n->next->prev = n->prev; else L.tail = n->prev;
  L.total_qty -= n->qty;
  delete n;
  id_index_.erase(it);

  // If cancelled at best and the level emptied, mark best as empty (sentinel)
  if (!L.head) {
    if (e.side == Side::Bid) book.set_best_bid(std::numeric_limits<Tick>::min());
    else                     book.set_best_ask(std::numeric_limits<Tick>::max());
    // refresh_best_after_depletion(e.side, e.px); // optional future improvement
  }
  return true;
}

} // namespace lob
