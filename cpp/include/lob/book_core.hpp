#pragma once
#include <unordered_map>
#include <limits>
#include "types.hpp"
#include "price_levels.hpp"

namespace lob {

// Order message (minimal for Day 2)
struct NewOrder {
  SeqNo     seq;       // event ordering
  Timestamp ts;        // timestamp
  OrderId   id;        // unique
  UserId    user;      // owner (for STP later)
  Side      side;      // Bid/Ask
  Tick      price;     // ignored for pure MARKET
  Quantity  qty;       // desired size
  uint32_t  flags;     // IOC/FOK/POST_ONLY/STP (not fully used today)
};

// Execution summary of a submit
struct ExecResult {
  Quantity filled{0};
  Quantity remaining{0}; // LIMIT: what rested; MARKET: unfilled qty
};

class BookCore {
public:
  BookCore(IPriceLevels& bids, IPriceLevels& asks)
    : bids_(bids), asks_(asks) {}

  // Submit a limit order (can trade first, then rest remainder)
  ExecResult submit_limit(const NewOrder& o);
  // Submit a market order (can trade, never rests)
  ExecResult submit_market(const NewOrder& o);

  // (Day 3+) cancel/modify will use this index
  bool cancel(OrderId id);

  // Is a side empty (by sentinel best)?
  bool empty(Side s) const {
    return (s == Side::Bid)
      ? (bids_.best_bid() == std::numeric_limits<Tick>::min())
      : (asks_.best_ask() == std::numeric_limits<Tick>::max());
  }

private:
  // Where each order lives
  struct IdEntry {
    Side side;
    Tick px;
    OrderNode* node;
  };

  IPriceLevels& bids_;
  IPriceLevels& asks_;
  std::unordered_map<OrderId, IdEntry> id_index_; // O(1) find node for cancel/modify

  // ----- intrusive FIFO helpers (operate on LevelFIFO in-place) -----
  static inline void enqueue(LevelFIFO& L, OrderNode* n) {
    n->next = nullptr;
    n->prev = L.tail;
    if (L.tail) L.tail->next = n; else L.head = n;
    L.tail = n;
    L.total_qty += n->qty;
  }

  static inline void erase(LevelFIFO& L, OrderNode* n) {
    if (n->prev) n->prev->next = n->next; else L.head = n->next;
    if (n->next) n->next->prev = n->prev; else L.tail = n->prev;
    L.total_qty -= n->qty;
    n->prev = n->next = nullptr;
  }

  // After a price level empties, we could search next best; Day 2: no-op
  void refresh_best_after_depletion(Side s, Tick emptied_px);

  // Core matching against opposite book (used by both limit and market)
  Quantity match_against(Side taker_side, Quantity qty, Tick px_limit);
};

} // namespace lob
