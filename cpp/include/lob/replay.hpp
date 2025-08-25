#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
#include <map>
#include <optional>

#include "lob/types.hpp"
#include "lob/book_core.hpp"
#include "lob/price_levels.hpp"
#include "lob/taq_writer.hpp"

namespace lob {

// Normalized event types we accept from a CSV exporter.
// We keep the schema minimal and deterministic.
enum class NormType : uint8_t { Book = 0, Trade = 1 };

struct NormEvent {
  int64_t ts_ns;   // nanoseconds since epoch
  NormType type;   // Book / Trade
  Side side;       // Bid/Ask for Book, aggressing side for Trade if known
  double price;    // price
  double qty;      // qty (Book = total size at that level AFTER update; Trade = executed size)
};

// Simple, dependency-free CSV reader for normalized events:
// Expected header and columns (in this order):
// ts_ns,type,side,price,qty
// - ts_ns: int64
// - type: "book" | "trade" (case-insensitive)
// - side: "B" | "A" (empty allowed for trade if unknown -> defaults to 'A')
// - price: double
// - qty: double
bool load_normalized_csv(const std::string& path, std::vector<NormEvent>& out);

// In-memory level book (per side) to compute quotes efficiently and deterministically.
class LevelBook {
public:
  // Book is represented as an ordered map from price -> size.
  // For bids we want descending; for asks ascending.
  // We'll keep both maps sorted ascending and query appropriate ends.
  void set_level(Side s, double px, double total_sz);
  double best_px(Side s) const;  // NaN if none
  double best_sz(Side s) const;  // 0 if none
  void clear();

private:
  std::map<double, double> bids_; // key: price ascending; last is best
  std::map<double, double> asks_; // key: price ascending; first is best
};

// Replayer: feeds normalized events into BookCore with inter-arrival gaps preserved
// under a speed multiplier, and emits TAQ-like outputs at a fixed cadence.
class Replayer {
public:
  struct Options {
    double   speed = 1.0;           // 1x, 10x, 50x...
    int64_t  cadence_ns = 50'000'000; // e.g., 50ms
    bool     realtime_sleep = true;  // if true, we sleep wall-clock time / speed
    std::string quotes_out_csv = "taq_quotes.csv";
    std::string trades_out_csv = "taq_trades.csv";
  };

  Replayer(BookCore& book, TaqWriter& writer);

  // Run replay over sorted events.
  // Returns false on fatal IO/invalid-data errors.
  bool run(const std::vector<NormEvent>& events, const Options& opt);

private:
  BookCore& book_;
  TaqWriter& writer_;

  // Synthetic aggregated orders per level: we create one resting order per (side, price).
  // We store a stable OrderId per level so we can modify or cancel.
  std::unordered_map<uint64_t, OrderId> level_order_id_;
  std::unordered_map<uint64_t, double>  level_size_;  // current total size we believe at that level

  LevelBook level_book_; // used to compute quotes for TAQ output quickly

  // Utility: build key (side + price) for maps, deterministic across runs.
  static uint64_t level_key(Side s, double px);

  // Feed a "book" event by adjusting the synthetic level-order.
  void apply_book_event(const NormEvent& e);

  // Forward trade to TAQ (we derive trades straight from the normalized input).
  void emit_trade_taq(const NormEvent& e);

  // Time helpers
  static int64_t align_up(int64_t ts_ns, int64_t step_ns);
};

} // namespace lob
