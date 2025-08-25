#include "lob/book_core.hpp"
#include <algorithm>
#include <limits>

namespace lob {

// Recompute best-of-book for side s by scanning nonempty levels (simple & correct)
void BookCore::refresh_best_after_depletion(Side s) {
  if (s == Side::Bid) {
    Tick best = std::numeric_limits<Tick>::min();
    bool found = false;
    bids_.for_each_nonempty([&](Tick px, const LevelFIFO& L){
      if (L.head && (!found || px > best)) { best = px; found = true; }
    });
    bids_.set_best_bid(found ? best : std::numeric_limits<Tick>::min());
  } else {
    Tick best = std::numeric_limits<Tick>::max();
    bool found = false;
    asks_.for_each_nonempty([&](Tick px, const LevelFIFO& L){
      if (L.head && (!found || px < best)) { best = px; found = true; }
    });
    asks_.set_best_ask(found ? best : std::numeric_limits<Tick>::max());
  }
}

// -------- Branch-minimized, side-specialized taker matching --------
template<bool IsBid>
Quantity BookCore::match_against_side(Quantity qty, Tick px_limit,
                                      OrderId taker_order_id, UserId taker_user,
                                      Timestamp ts, bool enable_stp) {
  Quantity filled = 0;
  auto& opp = IsBid ? asks_ : bids_;

  const Tick MINP = std::numeric_limits<Tick>::min();
  const Tick MAXP = std::numeric_limits<Tick>::max();

  auto crosses = [&](Tick best_px) -> bool {
    if constexpr (IsBid) return best_px <= px_limit;
    else                  return best_px >= px_limit;
  };

  while (qty > 0) {
    // cache-hot: pointer to best level (no map lookup in the hot loop)
    LevelFIFO* Lp = opp.best_level_ptr(IsBid ? Side::Ask : Side::Bid);
    Tick best_px = IsBid ? opp.best_ask() : opp.best_bid();
    if ((IsBid && best_px == MAXP) || (!IsBid && best_px == MINP)) break;
    if (!Lp) { refresh_best_after_depletion(IsBid ? Side::Ask : Side::Bid); continue; }
    if (!crosses(best_px)) break;

    LevelFIFO& L = *Lp;
    OrderNode* h = L.head;
    if (!h) { refresh_best_after_depletion(IsBid ? Side::Ask : Side::Bid); continue; }

    // Self-Trade Prevention: cancel resting same-owner if enabled
    if (enable_stp && h->user == taker_user) {
      erase(L, h);
      if (logger_) logger_->log_cancel(h->id, ts);
      id_index_.erase(h->id);
      free_node(h);
      if (!L.head) refresh_best_after_depletion(IsBid ? Side::Ask : Side::Bid);
      continue;
    }

    const Quantity tr = (h->qty <= qty) ? h->qty : qty;
    h->qty      -= tr;
    L.total_qty -= tr;
    filled      += tr;
    qty         -= tr;

    if (logger_) {
      Side passive_side = IsBid ? Side::Ask : Side::Bid;
      logger_->log_fill(best_px, tr, passive_side, h->id, taker_order_id, ts);
    }

    if (h->qty == 0) {
      erase(L, h);
      id_index_.erase(h->id);
      free_node(h);
      if (!L.head) {
        refresh_best_after_depletion(IsBid ? Side::Ask : Side::Bid);
      }
    }
  }

  return filled;
}

// Generic entry (kept for callers needing runtime side)
Quantity BookCore::match_against(Side taker_side, Quantity qty, Tick px_limit,
                                 OrderId taker_order_id, UserId taker_user,
                                 Timestamp ts, bool enable_stp) {
  return (taker_side == Side::Bid)
    ? match_against_side<true>(qty, px_limit, taker_order_id, taker_user, ts, enable_stp)
    : match_against_side<false>(qty, px_limit, taker_order_id, taker_user, ts, enable_stp);
}

ExecResult BookCore::submit_market(const NewOrder& o) {
  ExecResult r{};
  if (o.qty <= 0) return r;

  const Tick bound = (o.side == Side::Bid) ? std::numeric_limits<Tick>::max()
                                           : std::numeric_limits<Tick>::min();

  if (logger_) logger_->log_new(o, /*is_limit=*/false, bound, o.ts);

  r.filled    = (o.side == Side::Bid)
    ? match_against_side<true> (o.qty, bound, o.id, o.user, o.ts, (o.flags & STP) != 0u)
    : match_against_side<false>(o.qty, bound, o.id, o.user, o.ts, (o.flags & STP) != 0u);
  r.remaining = o.qty - r.filled;

  if (logger_) logger_->on_book_after_event(o.ts);
  return r;
}

// -------- Side-specialized limit submit --------
template<bool IsBid>
ExecResult BookCore::submit_limit_side(const NewOrder& o) {
  ExecResult r{};
  if (o.qty <= 0) return r;

  // FOK: compute available up to price bound
  if ((o.flags & FOK) != 0u) {
    Quantity need = o.qty;
    auto& opp = IsBid ? asks_ : bids_;
    Quantity avail = 0;
    opp.for_each_nonempty([&](Tick px, const LevelFIFO& L){
      if constexpr (IsBid) { if (px <= o.price) avail += L.total_qty; }
      else                 { if (px >= o.price) avail += L.total_qty; }
    });
    if (avail < need) {
      if (logger_) logger_->log_new(o, /*is_limit=*/true, o.price, o.ts);
      r.filled = 0; r.remaining = o.qty;
      if (logger_) logger_->on_book_after_event(o.ts);
      return r;
    }
  }

  // Minimal POST_ONLY: reject if it would cross (no trade, no rest)
  if ((o.flags & POST_ONLY) != 0u) {
    Tick opp_best = IsBid ? asks_.best_ask() : bids_.best_bid();
    bool would_cross = IsBid ? (opp_best <= o.price) : (opp_best >= o.price);
    if (would_cross) {
      if (logger_) logger_->log_new(o, /*is_limit=*/true, o.price, o.ts);
      r.remaining = o.qty;
      if (logger_) logger_->on_book_after_event(o.ts);
      return r;
    }
  }

  if (logger_) logger_->log_new(o, /*is_limit=*/true, o.price, o.ts);

  Quantity filled = match_against_side<IsBid>(
      o.qty, o.price, o.id, o.user, o.ts, (o.flags & STP) != 0u);

  // IOC: don't rest leftovers
  if ((o.flags & IOC) != 0u) {
    r.filled = filled;
    r.remaining = o.qty - filled;
    if (logger_) logger_->on_book_after_event(o.ts);
    return r;
  }

  Quantity leftover = o.qty - filled;
  if (leftover <= 0) {
    r.filled = filled; r.remaining = 0;
    if (logger_) logger_->on_book_after_event(o.ts);
    return r;
  }

  auto& same = IsBid ? bids_ : asks_;
  LevelFIFO& L = same.get_level(o.price);

  OrderNode* n = alloc_node();
  n->id = o.id; n->user = o.user; n->qty = leftover; n->ts = o.ts; n->flags = o.flags;
  n->prev = n->next = nullptr;
  enqueue(L, n);

  if constexpr (IsBid) {
    if (o.price > same.best_bid()) same.set_best_bid(o.price);
  } else {
    if (o.price < same.best_ask()) same.set_best_ask(o.price);
  }

  id_index_[o.id] = IdEntry{ IsBid ? Side::Bid : Side::Ask, o.price, n };
  r.filled = filled;
  r.remaining = leftover;

  if (logger_) logger_->on_book_after_event(o.ts);
  return r;
}

ExecResult BookCore::submit_limit(const NewOrder& o) {
  return (o.side == Side::Bid) ? submit_limit_side<true>(o)
                               : submit_limit_side<false>(o);
}

bool BookCore::cancel(OrderId id) {
  auto it = id_index_.find(id);
  if (it == id_index_.end()) return false;
  const IdEntry e = it->second;

  auto& book = (e.side == Side::Bid) ? bids_ : asks_;
  LevelFIFO& L = book.get_level(e.px);
  OrderNode* n = e.node;
  if (!n) return false;

  erase(L, n);
  free_node(n);
  id_index_.erase(it);

  if (logger_) logger_->log_cancel(id, /*ts*/0);

  if (!L.head) {
    // level emptied -> recompute best for this side
    refresh_best_after_depletion(e.side);
  }

  if (logger_) logger_->on_book_after_event(/*ts*/0);
  return true;
}

ExecResult BookCore::modify(const ModifyOrder& m) {
  ExecResult r{};
  auto it = id_index_.find(m.id);
  if (it == id_index_.end()) return r; // not found

  IdEntry e = it->second;
  auto& side = (e.side == Side::Bid) ? bids_ : asks_;
  LevelFIFO& L = side.get_level(e.px);
  OrderNode* n = e.node;
  if (!n) return r;

  // Remove from old level position
  erase(L, n);

  // Apply changes
  n->qty   = m.new_qty;
  n->flags = m.flags;

  Tick new_px = m.new_price;

  // If price improves and crosses, trade first (unless POST_ONLY forbids)
  auto opp_best = (e.side == Side::Bid) ? asks_.best_ask() : bids_.best_bid();
  bool crosses_now = (e.side == Side::Bid) ? (opp_best <= new_px) : (opp_best >= new_px);

  if (crosses_now && n->qty > 0 && ((n->flags & POST_ONLY) == 0u)) {
    Quantity took = (e.side == Side::Bid)
      ? match_against_side<true>(n->qty, new_px, n->id, n->user, n->ts, (n->flags & STP) != 0u)
      : match_against_side<false>(n->qty, new_px, n->id, n->user, n->ts, (n->flags & STP) != 0u);
    n->qty -= took;
    r.filled += took;
  }

  // Rest leftover (respect IOC on modify)
  if (n->qty > 0 && (n->flags & IOC) == 0u) {
    LevelFIFO& L2 = side.get_level(new_px);
    enqueue(L2, n);
    it->second.px   = new_px;
    it->second.node = n;

    if (e.side == Side::Bid) {
      if (new_px > side.best_bid()) side.set_best_bid(new_px);
    } else {
      if (new_px < side.best_ask()) side.set_best_ask(new_px);
    }
    r.remaining = n->qty;
  } else {
    // Fully consumed or IOC: destroy node, drop from index
    free_node(n);
    id_index_.erase(it);
    r.remaining = 0;
  }

  // If original level emptied, recompute best
  if (!L.head) refresh_best_after_depletion(e.side);

  if (logger_) logger_->on_book_after_event(m.ts);
  return r;
}

void BookCore::rebuild_index_from_books() {
  id_index_.clear();
  bids_.for_each_nonempty([&](Tick px, const LevelFIFO& L){
    for (auto* p = L.head; p; p = p->next) {
      id_index_[p->id] = IdEntry{Side::Bid, px, const_cast<OrderNode*>(p)};
    }
  });
  asks_.for_each_nonempty([&](Tick px, const LevelFIFO& L){
    for (auto* p = L.head; p; p = p->next) {
      id_index_[p->id] = IdEntry{Side::Ask, px, const_cast<OrderNode*>(p)};
    }
  });
}

// -------- NEW: Top-N levels helper --------
std::vector<std::pair<Tick, Quantity>> BookCore::topN(Side s, int levels) {
  std::vector<std::pair<Tick, Quantity>> out;
  if (levels <= 0) return out;
  out.reserve(static_cast<size_t>(levels));

  const bool isBid = (s == Side::Bid);
  auto& book = isBid ? bids_ : asks_;

  // Collect all non-empty levels
  std::vector<std::pair<Tick, Quantity>> tmp;
  tmp.reserve(128); // heuristic
  book.for_each_nonempty([&](Tick px, const LevelFIFO& L){
    if (L.head && L.total_qty > 0) {
      tmp.emplace_back(px, L.total_qty);
    }
  });

  if (tmp.empty()) return out;

  // Sort best→worse
  if (isBid) {
    std::sort(tmp.begin(), tmp.end(),
              [](const auto& a, const auto& b){ return a.first > b.first; });
  } else {
    std::sort(tmp.begin(), tmp.end(),
              [](const auto& a, const auto& b){ return a.first < b.first; });
  }

  const int n = std::min<int>(levels, static_cast<int>(tmp.size()));
  out.insert(out.end(), tmp.begin(), tmp.begin() + n);
  return out;
}

// Explicit instantiations for the template members we defined in this TU
template Quantity BookCore::match_against_side<true >(Quantity, Tick, OrderId, UserId, Timestamp, bool);
template Quantity BookCore::match_against_side<false>(Quantity, Tick, OrderId, UserId, Timestamp, bool);
template ExecResult BookCore::submit_limit_side<true >(const NewOrder&);
template ExecResult BookCore::submit_limit_side<false>(const NewOrder&);

} // namespace lob
