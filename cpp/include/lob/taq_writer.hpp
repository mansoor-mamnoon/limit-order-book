#pragma once
#include <cstdio>
#include <string>
#include <cstdint>
#include <optional>

namespace lob {

// Dependency-free TAQ-like CSV writer.
// Quotes are sampled on a fixed cadence by the replay tool; trades are event-driven.
class TaqWriter {
public:
  TaqWriter() = default;
  ~TaqWriter() { close(); }

  // Open CSVs; writes headers.
  // quotes_csv columns (Analytics v1 contract):
  //   ts_ns,bid,ask,bid_sz,ask_sz,mid,spread,microprice
  // trades_csv columns:
  //   ts_ns,price,qty,side
  bool open(const std::string& quotes_csv, const std::string& trades_csv);

  // Close files if open (flushes).
  void close();

  // Quotes sampled on a time grid.
  // All timestamps are nanoseconds since UNIX epoch.
  void write_quote_row(
      int64_t ts_ns,
      double bid_px, double bid_sz,
      double ask_px, double ask_sz);

  // Trades (usually from input feed). Side is 'B' (aggressing buy) or 'A' (aggressing sell), if known.
  void write_trade_row(
      int64_t ts_ns,
      double price, double qty,
      char side);

private:
  std::FILE* qf_ = nullptr;
  std::FILE* tf_ = nullptr;

  // Monotonicity best-effort warnings (we don't throw).
  std::optional<int64_t> last_quote_ts_ns_;
  std::optional<int64_t> last_trade_ts_ns_;

  static void fprint_double(std::FILE* f, double v);
};

} // namespace lob
