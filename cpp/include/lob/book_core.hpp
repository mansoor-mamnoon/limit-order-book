#pragma once
#include <unordered_map>
#include <limits>
#include <vector>   // <-- added
#include <utility>  // <-- added
#include "types.hpp"
#include "price_levels.hpp"
#include "logging.hpp"
#include "mempool.hpp"

#if defined(__GNUC__) || defined(__clang__)
  #define LOB_LIKELY(x)   __builtin_expect(!!(x), 1)
  #define LOB_UNLIKELY(x) __builtin_expect(!!(x), 0)
  #define LOB_ALWAYS_INLINE inline __attribute__((always_inline))
#else
  #define LOB_LIKELY(x)   (x)
  #define LOB_UNLIKELY(x) (x)
  #define LOB_ALWAYS_INLINE inline
#endif

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

  ExecResult submit_limit (const NewOrder& o);
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

  // -------- NEW: Top-N levels helper --------
  // Returns up to `levels` pairs of (price_ticks, total_qty) ordered best→worse.
  // For bids: descending prices; for asks: ascending prices.
  // NOTE: returns ticks (not absolute price). Convert with your tick size as needed.
  std::vector<std::pair<Tick, Quantity>> topN(Side s, int levels);

private:
  struct IdEntry {
    Side side;
    Tick px;
    OrderNode* node;
  };

  // -------- Memory pool helpers (no malloc/free in hot path) --------
  SlabPool<OrderNode> order_pool_{2}; // 2 slabs to start; grows on demand

  LOB_ALWAYS_INLINE OrderNode* alloc_node() {
    OrderNode* n = order_pool_.alloc();
    n->alloc_kind = 1; // pooled
    return n;
  }
  LOB_ALWAYS_INLINE void free_node(OrderNode* n) {
    if (LOB_LIKELY(n->alloc_kind == 1)) {
      order_pool_.free(n);
    } else {
      // nodes built by loaders/tests not using the pool
      delete n;
    }
  }

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

  // -------- Branch-minimized matching (compile-time side) --------
  template<bool IsBid>
  ExecResult submit_limit_side(const NewOrder& o);

  template<bool IsBid>
  Quantity match_against_side(Quantity qty, Tick px_limit,
                              OrderId taker_order_id, UserId taker_user,
                              Timestamp ts, bool enable_stp);

  // Fallback helpers used by generic code
  Quantity match_against(Side taker_side, Quantity qty, Tick px_limit,
                         OrderId taker_order_id, UserId taker_user,
                         Timestamp ts, bool enable_stp);

  void refresh_best_after_depletion(Side s);

private:
  IPriceLevels& bids_;
  IPriceLevels& asks_;
  IEventLogger* logger_{nullptr};
  std::unordered_map<OrderId, IdEntry> id_index_;
};

} // namespace lob
