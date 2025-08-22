#pragma once
#include <unordered_map>
#include <limits>
#include <algorithm>
#include "types.hpp"
#include "price_levels.hpp"

namespace lob {

// Order message (minimal)
struct NewOrder {
  SeqNo     seq;
  Timestamp ts;
  OrderId   id;
  UserId    user;
  Side      side;     // Bid/Ask
  Tick      price;    // ignored for pure MARKET
  Quantity  qty;      // desired size
  uint32_t  flags;    // IOC/FOK/POST_ONLY/STP (we only use STP today)
};

struct ExecResult {
  Quantity filled{0};
  Quantity remaining{0}; // For LIMIT: what rested; for MARKET: unfilled qty
};

class BookCore {
public:
  BookCore(IPriceLevels& bids, IPriceLevels& asks)
    : bids_(bids), asks_(asks) {}

  ExecResult submit_limit (const NewOrder& o);  // trade then rest
  ExecResult submit_market(const NewOrder& o);  // trade, never rest

  // O(1) cancel via id index
  bool cancel(OrderId id);

  // MODIFY: if price changes, requeue (lose time priority).
  // If price unchanged, adjust size in-place (reduce/increase; <=0 cancels).
  // If price improves enough to cross, this will trade (by cancel+resubmit).
  ExecResult modify(const NewOrder& replacement);

  bool empty(Side s) const {
    return (s == Side::Bid)
      ? (bids_.best_bid() == std::numeric_limits<Tick>::min())
      : (asks_.best_ask() == std::numeric_limits<Tick>::max());
  }

private:
  struct IdEntry {
    Side side;
    Tick px;
    OrderNode* node;
  };

  IPriceLevels& bids_;
  IPriceLevels& asks_;
  std::unordered_map<OrderId, IdEntry> id_index_; // O(1) node lookup

  // ----- intrusive FIFO helpers -----
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

  // Core matcher used by limit/market/modify resubmits
  // STP mode: if taker has STP and resting user == taker_user => cancel resting (no trade).
  Quantity match_against(Side taker_side,
                         UserId taker_user,
                         uint32_t taker_flags,
                         Quantity want,
                         Tick px_limit);
};

} // namespace lob
