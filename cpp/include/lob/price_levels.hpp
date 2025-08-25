#pragma once
#include <unordered_map>
#include <vector>
#include <limits>
#include <functional>
#include "types.hpp"

namespace lob {

// Intrusive FIFO node stored inside the order itself
struct OrderNode {
  OrderId    id;
  UserId     user;
  Quantity   qty;      // remaining
  Timestamp  ts;
  uint32_t   flags;
  OrderNode* prev{nullptr};
  OrderNode* next{nullptr};
  // Day 7: allocation tag (0 = new/delete; 1 = pooled)
  uint8_t    alloc_kind{0};
};

struct LevelFIFO {
  OrderNode* head{nullptr};
  OrderNode* tail{nullptr};
  Quantity   total_qty{0};
};

// Abstract interface for "a side's price ladder" (bids OR asks)
class IPriceLevels {
public:
  virtual ~IPriceLevels() = default;

  // Access the FIFO at a price (create level if missing for this impl)
  virtual LevelFIFO& get_level(Tick px) = 0;
  virtual bool       has_level(Tick px) const = 0;

  // Top-of-book getters; the inactive one should return a sentinel:
  //  - best_bid(): std::numeric_limits<Tick>::min() means "empty"
  //  - best_ask(): std::numeric_limits<Tick>::max() means "empty"
  virtual Tick best_bid() const = 0;
  virtual Tick best_ask() const = 0;

  // Cache-hot pointer to current best level (nullptr if empty) for a side.
  virtual LevelFIFO* best_level_ptr(Side s) = 0;

  // Top-of-book setters used by BookCore to mark emptiness/improvements
  virtual void set_best_bid(Tick px) = 0;
  virtual void set_best_ask(Tick px) = 0;

  // Enumerate all orders currently present (for snapshot/rebuild).
  // The callback is called for each (price level, node) pair.
  virtual void for_each_order(const std::function<void(Tick, OrderNode*)>& fn) const = 0;

  // Enumerate only non-empty levels (used by logging/snapshots).
  // Calls fn(price, const LevelFIFO&) for each level that has at least one order.
  virtual void for_each_nonempty(const std::function<void(Tick, const LevelFIFO&)>& fn) const = 0;
};

// -------- Contiguous array for bounded [min,max] ticks (replay) --------
class PriceLevelsContig final : public IPriceLevels {
public:
  explicit PriceLevelsContig(PriceBand band)
    : band_(band),
      // allocate one LevelFIFO per tick in [min,max]
      levels_(static_cast<size_t>(band.max_tick - band.min_tick + 1)),
      best_bid_(std::numeric_limits<Tick>::min()),
      best_ask_(std::numeric_limits<Tick>::max()),
      best_bid_ptr_(nullptr),
      best_ask_ptr_(nullptr) {}

  LevelFIFO& get_level(Tick px) override { return levels_[idx(px)]; }

  bool has_level(Tick px) const override {
    const auto& L = levels_[idx(px)];
    return L.head != nullptr;
  }

  Tick best_bid() const override { return best_bid_; }
  Tick best_ask() const override { return best_ask_; }

  LevelFIFO* best_level_ptr(Side s) override {
    return (s == Side::Bid) ? best_bid_ptr_ : best_ask_ptr_;
  }

  void set_best_bid(Tick px) override {
    best_bid_ = px;
    best_bid_ptr_ = (px == std::numeric_limits<Tick>::min()) ? nullptr : &levels_[idx(px)];
  }
  void set_best_ask(Tick px) override {
    best_ask_ = px;
    best_ask_ptr_ = (px == std::numeric_limits<Tick>::max()) ? nullptr : &levels_[idx(px)];
  }

  void for_each_order(const std::function<void(Tick, OrderNode*)>& fn) const override {
    for (Tick px = band_.min_tick; px <= band_.max_tick; ++px) {
      const LevelFIFO& L = levels_[idx(px)];
      for (OrderNode* n = L.head; n; n = n->next) {
        fn(px, n);
      }
    }
  }

  void for_each_nonempty(const std::function<void(Tick, const LevelFIFO&)>& fn) const override {
    for (Tick px = band_.min_tick; px <= band_.max_tick; ++px) {
      const LevelFIFO& L = levels_[idx(px)];
      if (L.head) fn(px, L);
    }
  }

private:
  size_t idx(Tick px) const { return static_cast<size_t>(px - band_.min_tick); }

  PriceBand              band_;
  std::vector<LevelFIFO> levels_;
  Tick                   best_bid_;
  Tick                   best_ask_;
  LevelFIFO*             best_bid_ptr_;
  LevelFIFO*             best_ask_ptr_;
};

// -------- Sparse map for wide/unknown bands --------
class PriceLevelsSparse final : public IPriceLevels {
public:
  LevelFIFO& get_level(Tick px) override { return map_[px]; }

  bool has_level(Tick px) const override {
    auto it = map_.find(px);
    return it != map_.end() && it->second.head != nullptr;
  }

  Tick best_bid() const override { return best_bid_; }
  Tick best_ask() const override { return best_ask_; }

  LevelFIFO* best_level_ptr(Side s) override {
    return (s == Side::Bid) ? best_bid_ptr_ : best_ask_ptr_;
  }

  void set_best_bid(Tick px) override {
    best_bid_ = px;
    if (px == std::numeric_limits<Tick>::min()) { best_bid_ptr_ = nullptr; return; }
    best_bid_ptr_ = &map_[px]; // existing top must be present
  }
  void set_best_ask(Tick px) override {
    best_ask_ = px;
    if (px == std::numeric_limits<Tick>::max()) { best_ask_ptr_ = nullptr; return; }
    best_ask_ptr_ = &map_[px];
  }

  void for_each_order(const std::function<void(Tick, OrderNode*)>& fn) const override {
    for (const auto& kv : map_) {
      Tick px = kv.first;
      const LevelFIFO& L = kv.second;
      for (OrderNode* n = L.head; n; n = n->next) {
        fn(px, n);
      }
    }
  }

  void for_each_nonempty(const std::function<void(Tick, const LevelFIFO&)>& fn) const override {
    for (const auto& kv : map_) {
      Tick px = kv.first;
      const LevelFIFO& L = kv.second;
      if (L.head) fn(px, L);
    }
  }

private:
  std::unordered_map<Tick, LevelFIFO> map_; // (later: absl::flat_hash_map)
  Tick        best_bid_{std::numeric_limits<Tick>::min()};
  Tick        best_ask_{std::numeric_limits<Tick>::max()};
  LevelFIFO*  best_bid_ptr_{nullptr};
  LevelFIFO*  best_ask_ptr_{nullptr};
};

} // namespace lob
