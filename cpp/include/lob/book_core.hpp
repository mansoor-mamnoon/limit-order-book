#pragma once
#include <unordered_map>
#include <limits>
#include "types.hpp"
#include "price_levels.hpp"
#include "logging.hpp"

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
  uint32_t  flags;    // IOC/FOK/POST_ONLY/STP...
};

struct ModifyOrder {
  SeqNo     seq;
  Timestamp ts;
  OrderId   id;
  Tick      new_price;
  Quantity  new_qty;
  uint32_t  flags;
};

// Return summary of a submit/modify
struct ExecResult {
  Quantity filled{0};
  Quantity remaining{0}; // for LIMIT, remaining rests; for MARKET it's unfilled
};

class BookCore {
public:
  BookCore(IPriceLevels& bids, IPriceLevels& asks, IEventLogger* logger=nullptr)
    : bids_(bids), asks_(asks), logger_(logger) {
    if (logger_) logger_->set_snapshot_sources(&bids_, &asks_);
  }

  ExecResult submit_limit(const NewOrder& o);
  ExecResult submit_market(const NewOrder& o);

  bool       cancel(OrderId id);
  ExecResult modify(const ModifyOrder& m);

  // Convenience overload because some tests call modify(NewOrder{...})
  ExecResult modify(const NewOrder& asModify) {
    ModifyOrder m{
      asModify.seq,
      asModify.ts,
      asModify.id,
      asModify.price,   // new price
      asModify.qty,     // new qty
      asModify.flags
    };
    return modify(m);
  }

  bool empty(Side s) const {
    return (s == Side::Bid)
      ? (bids_.best_bid() == std::numeric_limits<Tick>::min())
      : (asks_.best_ask() == std::numeric_limits<Tick>::max());
  }

  // (optional helper for replay-from-snapshot tests)
  void rebuild_index_from_books();

private:
  struct IdEntry {
    Side side;
    Tick px;
    OrderNode* node;
  };

  IPriceLevels& bids_;
  IPriceLevels& asks_;
  IEventLogger* logger_{nullptr};
  std::unordered_map<OrderId, IdEntry> id_index_;

  // intrusive FIFO helpers
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

  Quantity match_against(Side taker_side, Quantity qty, Tick px_limit,
                         OrderId taker_order_id, UserId taker_user,
                         Timestamp ts, bool enable_stp);

  void refresh_best_after_depletion(Side s);
};

} // namespace lob
