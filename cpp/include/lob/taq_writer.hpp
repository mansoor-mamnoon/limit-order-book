#pragma once
#include <cstdio>
#include <string>
#include <cstdint>
#include <optional>

namespace lob {

// Simple CSV writer for TAQ-like outputs.
// We keep it dependency-free (no Arrow); Parquet is handled by a tiny Python helper (provided).
class TaqWriter {
public:
  // Two files: one for quotes sampled at fixed cadence; one for raw trades.
  TaqWriter() = default;
  ~TaqWriter() { close(); }

  // Open CSVs; write headers.
  bool open(const std::string& quotes_csv, const std::string& trades_csv);

  // Close files if open.
  void close();

  // Quotes sampled on a time grid.
  // All timestamps are nanoseconds since UNIX epoch for determinism + monotonicity checks.
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

  // For monotonicity asserts (best-effort; we don't throw)
  std::optional<int64_t> last_quote_ts_ns_;
  std::optional<int64_t> last_trade_ts_ns_;

  // Naive CSV escape for safety (not strictly needed for numeric data).
  static std::string esc(const std::string& s);

  static void fprint_double(std::FILE* f, double v);
};

} // namespace lob
