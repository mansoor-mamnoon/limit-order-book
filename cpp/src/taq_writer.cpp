#include "lob/taq_writer.hpp"
#include <cstdio>
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
  // headers
  std::fprintf(qf_, "ts_ns,bid_px,bid_sz,ask_px,ask_sz,mid,spread,microprice\n");
  std::fprintf(tf_, "ts_ns,price,qty,side\n");
  return true;
}

void TaqWriter::close() {
  if (qf_) { std::fclose(qf_); qf_ = nullptr; }
  if (tf_) { std::fclose(tf_); tf_ = nullptr; }
  last_quote_ts_ns_.reset();
  last_trade_ts_ns_.reset();
}

std::string TaqWriter::esc(const std::string& s) {
  // Simple: wrap if contains comma/quote/newline.
  bool needs = false;
  for (char c: s) if (c == ',' || c == '"' || c == '\n' || c == '\r') { needs = true; break; }
  if (!needs) return s;
  std::string out = "\"";
  for (char c: s) {
    if (c == '"') out += "\"\"";
    else out += c;
  }
  out += '"';
  return out;
}

void TaqWriter::fprint_double(std::FILE* f, double v) {
  if (std::isnan(v)) {
    std::fputs("", f);
    return;
  }
  // Avoid scientific by formatting with sufficient precision.
  // 12 digits is safe for prices/sizes here.
  std::fprintf(f, "%.12g", v);
}

void TaqWriter::write_quote_row(
    int64_t ts_ns,
    double bid_px, double bid_sz,
    double ask_px, double ask_sz)
{
  if (!qf_) return;

  // Monotonic (best-effort warning)
  if (last_quote_ts_ns_ && ts_ns < *last_quote_ts_ns_) {
    std::fprintf(stderr, "WARN: Non-monotonic quote ts: %lld < %lld\n",
                 (long long)ts_ns, (long long)*last_quote_ts_ns_);
  }
  last_quote_ts_ns_ = ts_ns;

  const bool have_bid = bid_sz > 0.0 && std::isfinite(bid_px);
  const bool have_ask = ask_sz > 0.0 && std::isfinite(ask_px);

  double mid = std::numeric_limits<double>::quiet_NaN();
  double spread = std::numeric_limits<double>::quiet_NaN();
  double micro = std::numeric_limits<double>::quiet_NaN();

  if (have_bid && have_ask) {
    mid = 0.5 * (bid_px + ask_px);
    spread = ask_px - bid_px;
    double denom = (bid_sz + ask_sz);
    micro = (denom > 0.0) ? ((bid_px * ask_sz + ask_px * bid_sz) / denom) : mid;
  } else if (have_bid) {
    mid = bid_px;
  } else if (have_ask) {
    mid = ask_px;
  }

  std::fprintf(qf_, "%lld,", (long long)ts_ns);

  if (have_bid) {
    fprint_double(qf_, bid_px); std::fputc(',', qf_);
    fprint_double(qf_, bid_sz); std::fputc(',', qf_);
  } else {
    std::fputs(",,", qf_);
  }

  if (have_ask) {
    fprint_double(qf_, ask_px); std::fputc(',', qf_);
    fprint_double(qf_, ask_sz); std::fputc(',', qf_);
  } else {
    std::fputs(",,", qf_);
  }

  if (std::isfinite(mid)) fprint_double(qf_, mid);
  std::fputc(',', qf_);
  if (std::isfinite(spread)) fprint_double(qf_, spread);
  std::fputc(',', qf_);
  if (std::isfinite(micro)) fprint_double(qf_, micro);
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
