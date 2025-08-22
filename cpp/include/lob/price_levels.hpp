#pragma once
#include <unordered_map>
#include <vector>
#include <limits>
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

  // Top-of-book setters
  virtual void set_best_bid(Tick px) = 0;
  virtual void set_best_ask(Tick px) = 0;
};

// -------- Contiguous array for bounded [min,max] ticks (replay) --------
class PriceLevelsContig final : public IPriceLevels {
public:
  explicit PriceLevelsContig(PriceBand band)
    : band_(band),
      levels_(static_cast<size_t>(band.max_tick - band.min_tick + 1)),
      best_bid_(std::numeric_limits<Tick>::min()),
      best_ask_(std::numeric_limits<Tick>::max()) {}

  LevelFIFO& get_level(Tick px) override { return levels_[idx(px)]; }

  bool has_level(Tick px) const override {
    const auto& L = levels_[idx(px)];
    return L.head != nullptr;
  }

  Tick best_bid() const override { return best_bid_; }
  Tick best_ask() const override { return best_ask_; }

  void set_best_bid(Tick px) override { best_bid_ = px; }
  void set_best_ask(Tick px) override { best_ask_ = px; }

private:
  size_t idx(Tick px) const {
    return static_cast<size_t>(px - band_.min_tick);
  }

  PriceBand              band_;
  std::vector<LevelFIFO> levels_;
  Tick                   best_bid_;
  Tick                   best_ask_;
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

  void set_best_bid(Tick px) override { best_bid_ = px; }
  void set_best_ask(Tick px) override { best_ask_ = px; }

private:
  std::unordered_map<Tick, LevelFIFO> map_; // later: flat_hash_map
  Tick best_bid_{std::numeric_limits<Tick>::min()};
  Tick best_ask_{std::numeric_limits<Tick>::max()};
};

} // namespace lob
