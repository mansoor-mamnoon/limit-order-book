#include <chrono>
#include <cinttypes>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <string>
#include <string_view>

#include "lob/book_core.hpp"
#include "lob/price_levels.hpp"
#include "lob/types.hpp"

using namespace lob;

namespace {

// Very small/fast arg parser supporting: --msgs N, --num N, -n N, or positional N
bool parse_msgs(int argc, char** argv, std::uint64_t& out_msgs) {
  out_msgs = 1'000'000; // default
  bool saw_positional = false;

  for (int i = 1; i < argc; ++i) {
    std::string_view a{argv[i]};
    if (a == "--help" || a == "-h") {
      std::cout <<
        "Usage: bench_tool [--msgs N | --num N | -n N | N]\n"
        "  Run a synthetic benchmark against the LOB core.\n"
        "  Examples:\n"
        "    bench_tool          # default 1,000,000 messages\n"
        "    bench_tool --msgs 2000000\n"
        "    bench_tool -n 5e6   # not supported; use integer only\n";
      std::exit(0);
    }

    auto take_uint = [&](int& idx) -> bool {
      if (idx + 1 >= argc) return false;
      std::string s{argv[idx + 1]};
      char* end = nullptr;
      errno = 0;
      unsigned long long v = std::strtoull(s.c_str(), &end, 10);
      if (errno != 0 || end == s.c_str() || *end != '\0') return false;
      out_msgs = static_cast<std::uint64_t>(v);
      idx++; // consume value
      return true;
    };

    if (a == "--msgs" || a == "--num") {
      if (!take_uint(i)) { std::cerr << "Invalid value for " << a << "\n"; return false; }
    } else if (a == "-n") {
      if (!take_uint(i)) { std::cerr << "Invalid value for -n\n"; return false; }
    } else if (!saw_positional) {
      // positional integer
      std::string s{argv[i]};
      char* end = nullptr;
      errno = 0;
      unsigned long long v = std::strtoull(s.c_str(), &end, 10);
      if (errno == 0 && end != s.c_str() && *end == '\0') {
        out_msgs = static_cast<std::uint64_t>(v);
        saw_positional = true;
      } else {
        std::cerr << "Unknown arg: " << s << "\n";
        return false;
      }
    } else {
      std::cerr << "Unknown extra arg: " << std::string(a) << "\n";
      return false;
    }
  }
  return true;
}

} // namespace

int main(int argc, char** argv) {
  std::uint64_t N = 0;
  if (!parse_msgs(argc, argv, N)) {
    std::cerr << "Usage: bench_tool [--msgs N | --num N | -n N | N]\n";
    return 1;
  }

  // Wire up a simple book (sparse ladders)
  PriceLevelsSparse bids, asks;
  BookCore book(bids, asks, /*logger*/ nullptr);

  // Warm up: plant one resting ask so early bids can cross
  {
    NewOrder o{};
    o.seq = 1; o.ts = 1; o.id = 1; o.user = 42;
    o.side = Side::Ask; o.price = 100; o.qty = 1000; o.flags = 0;
    book.submit_limit(o);
  }

  auto t0 = std::chrono::steady_clock::now();

  // Simple synthetic stream:
  //  - even i: submit aggressive bid that may trade/rest
  //  - odd  i: submit passive ask and occasionally cancel a prior id
  // Keeps data/locality decent and exercises submit/modify/cancel paths.
  OrderId next_id = 2;
  for (std::uint64_t i = 0; i < N; ++i) {
    if ((i & 1ull) == 0) {
      NewOrder b{};
      b.seq = next_id; b.ts = static_cast<Timestamp>(next_id);
      b.id = next_id;  b.user = 7 + (next_id & 3);
      b.side = Side::Bid;
      b.price = 100 + static_cast<Tick>(i % 8); // sometimes crosses the ask@100
      b.qty   = 1 + static_cast<Quantity>(i % 4);
      b.flags = 0;
      (void)book.submit_limit(b);
      ++next_id;
    } else {
      if ((i % 16ull) == 1ull && next_id > 5) {
        // cancel a recent id to hit cancel path
        (void)book.cancel(next_id - 5);
      }
      NewOrder a{};
      a.seq = next_id; a.ts = static_cast<Timestamp>(next_id);
      a.id = next_id;  a.user = 9 + (next_id & 3);
      a.side = Side::Ask;
      a.price = 102 + static_cast<Tick>(i % 8);
      a.qty   = 1 + static_cast<Quantity>(i % 4);
      a.flags = 0;
      (void)book.submit_limit(a);
      ++next_id;
    }
  }

  auto t1 = std::chrono::steady_clock::now();
  const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
  const double secs = ns / 1e9;
  const double mps  = (secs > 0.0) ? (static_cast<double>(N) / secs) : 0.0;

  auto l1 = book.empty(Side::Bid) && book.empty(Side::Ask) ? std::string("EMPTY") : std::string("NONEMPTY");

  std::cout << "bench_tool processed " << N << " messages in " << secs << " s ("
            << static_cast<long long>(mps) << " msg/s), book=" << l1 << "\n";

  return 0;
}
