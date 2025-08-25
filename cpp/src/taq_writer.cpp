#include "lob/taq_writer.hpp"
#include <cerrno>
#include <cstring>
#include <limits>
#include <cmath>

namespace lob {

bool TaqWriter::open(const std::string& quotes_csv, const std::string& trades_csv) {
  close();
  qf_ = std::fopen(quotes_csv.c_str(), "wb");
  if (!qf_) {
    std::fprintf(stderr, "TaqWriter: failed to open quotes CSV '%s': %s\n",
                 quotes_csv.c_str(), std::strerror(errno));
    return false;
  }
  tf_ = std::fopen(trades_csv.c_str(), "wb");
  if (!tf_) {
    std::fprintf(stderr, "TaqWriter: failed to open trades CSV '%s': %s\n",
                 trades_csv.c_str(), std::strerror(errno));
    std::fclose(qf_); qf_ = nullptr;
    return false;
  }

  // ---- Headers (Analytics v1 contract) ----
  // NOTE: column names are intentionally "bid" and "ask" (not bid_px/ask_px)
  // to match the metrics module.
  std::fprintf(qf_, "ts_ns,bid,ask,bid_sz,ask_sz,mid,spread,microprice\n");
  std::fprintf(tf_, "ts_ns,price,qty,side\n");
  return true;
}

void TaqWriter::close() {
  if (qf_) {
    std::fflush(qf_);
    std::fclose(qf_);
    qf_ = nullptr;
  }
  if (tf_) {
    std::fflush(tf_);
    std::fclose(tf_);
    tf_ = nullptr;
  }
  last_quote_ts_ns_.reset();
  last_trade_ts_ns_.reset();
}

void TaqWriter::fprint_double(std::FILE* f, double v) {
  if (std::isnan(v)) {
    // empty cell for NaN (CSV-friendly)
    return;
  }
  // Avoid scientific notation; sufficient precision for prices/sizes here.
  std::fprintf(f, "%.12g", v);
}

void TaqWriter::write_quote_row(
    int64_t ts_ns,
    double bid_px, double bid_sz,
    double ask_px, double ask_sz)
{
  if (!qf_) return;

  // Monotonicity warning (best-effort)
  if (last_quote_ts_ns_ && ts_ns < *last_quote_ts_ns_) {
    std::fprintf(stderr, "WARN: Non-monotonic quote ts: %lld < %lld\n",
                 (long long)ts_ns, (long long)*last_quote_ts_ns_);
  }
  last_quote_ts_ns_ = ts_ns;

  const bool have_bid = bid_sz > 0.0 && std::isfinite(bid_px);
  const bool have_ask = ask_sz > 0.0 && std::isfinite(ask_px);

  double mid    = std::numeric_limits<double>::quiet_NaN();
  double spread = std::numeric_limits<double>::quiet_NaN();
  double micro  = std::numeric_limits<double>::quiet_NaN();

  if (have_bid && have_ask) {
    mid = 0.5 * (bid_px + ask_px);
    spread = ask_px - bid_px;
    const double denom = (bid_sz + ask_sz);
    micro = (denom > 0.0) ? ((ask_px * bid_sz + bid_px * ask_sz) / denom) : mid;
  } else if (have_bid) {
    mid = bid_px;
  } else if (have_ask) {
    mid = ask_px;
  }

  // ts_ns
  std::fprintf(qf_, "%lld,", (long long)ts_ns);

  // bid, ask, bid_sz, ask_sz
  if (have_bid) fprint_double(qf_, bid_px);
  std::fputc(',', qf_);
  if (have_ask) fprint_double(qf_, ask_px);
  std::fputc(',', qf_);
  if (have_bid) fprint_double(qf_, bid_sz);
  std::fputc(',', qf_);
  if (have_ask) fprint_double(qf_, ask_sz);
  std::fputc(',', qf_);

  // mid,spread,microprice
  if (std::isfinite(mid))    fprint_double(qf_, mid);
  std::fputc(',', qf_);
  if (std::isfinite(spread)) fprint_double(qf_, spread);
  std::fputc(',', qf_);
  if (std::isfinite(micro))  fprint_double(qf_, micro);
  std::fputc('\n', qf_);
}

void TaqWriter::write_trade_row(
    int64_t ts_ns,
    double price, double qty,
    char side)
{
  if (!tf_) return;

  if (last_trade_ts_ns_ && ts_ns < *last_trade_ts_ns_) {
    std::fprintf(stderr, "WARN: Non-monotonic trade ts: %lld < %lld\n",
                 (long long)ts_ns, (long long)*last_trade_ts_ns_);
  }
  last_trade_ts_ns_ = ts_ns;

  std::fprintf(tf_, "%lld,", (long long)ts_ns);
  fprint_double(tf_, price); std::fputc(',', tf_);
  fprint_double(tf_, qty);   std::fputc(',', tf_);
  std::fputc(side ? side : ' ', tf_);
  std::fputc('\n', tf_);
}

} // namespace lob
