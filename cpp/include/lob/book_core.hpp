#pragma once
#include <unordered_map>
#include "types.hpp"
#include "price_levels.hpp"

namespace lob {

struct NewOrder {
  SeqNo     seq;
  Timestamp ts;
  OrderId   id;
  UserId    user;
  Side      side;
  Tick      price;   // ignored for pure market
  Quantity  qty;
  uint32_t  flags;   // IOC/FOK/POST_ONLY/STP
};

class BookCore {
public:
  explicit BookCore(IPriceLevels& bids, IPriceLevels& asks)
    : bids_(bids), asks_(asks) {}

  // (implement Day 2)
  void submit(const NewOrder& o);
  bool cancel(OrderId id);
  bool modify_price(OrderId id, Tick new_px);
  bool modify_qty(OrderId id, Quantity new_qty);

private:
  IPriceLevels& bids_;
  IPriceLevels& asks_;
  std::unordered_map<OrderId, OrderNode*> id_index_; // O(1) cancel/modify
};

} // namespace lob
