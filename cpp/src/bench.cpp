#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <algorithm>
#include <chrono>
#include <optional>

// Linux-only pinning headers (no-op elsewhere)
#if defined(__linux__)
  #include <sched.h>
  #include <pthread.h>
#endif

// Project headers
#include "lob/book_core.hpp"
#include "lob/price_levels.hpp"
#include "lob/types.hpp"

using namespace lob;
using clk = std::chrono::steady_clock;

// -----------------------------
// Args / parsing
// -----------------------------
struct Args {
  uint64_t msgs   = 2'000'000;
  uint64_t warmup = 50'000;
  const char* out_csv = nullptr;
  const char* hist    = nullptr;
  std::optional<int> pin_core;     // --pin-core N
  // --band lo:hi:tick  → switches to PriceLevelsContig for compact ladders
  bool use_band = false;
  long long band_lo = 0, band_hi = 0, band_tick = 1;
} A;

static bool arg_eq(const char* a, const char* b){ return std::strcmp(a,b)==0; }
static uint64_t to_u64(const char* s){
  char* e; auto v = std::strtoull(s,&e,10);
  if(*e) { std::fprintf(stderr,"bad int: %s\n",s); std::exit(2);}
  return v;
}

static bool parse_band(const char* s, long long& lo, long long& hi, long long& tick){
  long long l=0,h=0,t=1;
  if (std::sscanf(s,"%lld:%lld:%lld",&l,&h,&t)==3 && h>l && t>0) { lo=l; hi=h; tick=t; return true; }
  return false;
}

static void parse(int argc, char** argv){
  for (int i=1;i<argc;i++){
    if (arg_eq(argv[i],"--msgs") && i+1<argc)           { A.msgs = to_u64(argv[++i]); }
    else if (arg_eq(argv[i],"--warmup") && i+1<argc)    { A.warmup = to_u64(argv[++i]); }
    else if (arg_eq(argv[i],"--out-csv") && i+1<argc)   { A.out_csv = argv[++i]; }
    else if (arg_eq(argv[i],"--hist") && i+1<argc)      { A.hist = argv[++i]; }
    else if (arg_eq(argv[i],"--pin-core") && i+1<argc)  { A.pin_core = std::atoi(argv[++i]); }
    else if (arg_eq(argv[i],"--band") && i+1<argc) {
      if (!parse_band(argv[++i], A.band_lo, A.band_hi, A.band_tick)) {
        std::fprintf(stderr, "Bad --band format. Use: lo:hi:tick (e.g. 999000:1001000:1)\n");
        std::exit(2);
      }
      A.use_band = true;
    }
    else {
      std::fprintf(stderr,"Unknown arg: %s\n", argv[i]);
      std::fprintf(stderr,"Usage: bench_tool --msgs N --warmup N [--out-csv path] [--hist path] [--pin-core N] [--band lo:hi:tick]\n");
      std::exit(2);
    }
  }
}

// -----------------------------
// Pinning (Linux only; no-op elsewhere)
// -----------------------------
static void maybe_pin_core(std::optional<int> core){
#if defined(__linux__)
  if (!core) return;
  cpu_set_t set; CPU_ZERO(&set); CPU_SET(*core, &set);
  (void)pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
#else
  (void)core; // no-op
#endif
}

// -----------------------------
// Histogram 0–100us
// -----------------------------
struct Histo {
  static constexpr int MAX_US=100;  // 0–100us buckets, last is overflow
  uint64_t buckets[MAX_US+1]{};
  void add(double us) {
    int i = (us<MAX_US)? (int)us : MAX_US;
    buckets[i]++;
  }
  void write(const char* path){
    if(!path) return;
    FILE* f = std::fopen(path,"w");
    if(!f) return;
    for(int i=0;i<=MAX_US;i++) std::fprintf(f,"%d,%llu\n", i, (unsigned long long)buckets[i]);
    std::fclose(f);
  }
};

// -----------------------------
// Bench core (templated on ladder type)
// -----------------------------
template <typename Bids, typename Asks>
static int run_bench(Bids& bids, Asks& asks){
  BookCore book(bids, asks, /*logger*/nullptr);

  std::vector<double> us; us.reserve(A.msgs);
  Histo H{};

  // Warmup
  for(uint64_t i=0;i<A.warmup;i++){
    NewOrder o{/*seq*/(uint64_t)i, /*ts*/0, /*id*/i, /*user*/1,
               /*side*/ (i&1)?Side::Bid:Side::Ask, /*px*/1000 + int(i%25),
               /*qty*/1, /*flags*/0};
    (void)book.submit_limit(o);
  }

  auto t0 = clk::now();
  for(uint64_t i=0;i<A.msgs;i++){
    NewOrder o{/*seq*/(uint64_t)(i + A.warmup), /*ts*/0, /*id*/i+1'000'000, /*user*/2,
               /*side*/ (i&1)?Side::Ask:Side::Bid, /*px*/1000 + int(i%25),
               /*qty*/1, /*flags*/0};
    auto s = clk::now();
    (void)book.submit_limit(o);
    auto e = clk::now();
    double d_us = std::chrono::duration<double,std::micro>(e-s).count();
    us.push_back(d_us);
    H.add(d_us);
  }
  auto t1 = clk::now();

  const double wall_s = std::chrono::duration<double>(t1-t0).count();
  const double mps = A.msgs / wall_s;

  std::sort(us.begin(), us.end());
  auto pct = [&](double q){
    if (us.empty()) return 0.0;
    const double idx = q * (us.size()-1);
    const size_t i = (size_t)idx;
    const double frac = idx - i;
    if (i+1 < us.size()) return us[i]*(1.0-frac) + us[i+1]*frac;
    return us[i];
  };
  const double p50   = pct(0.50);
  const double p90   = pct(0.90);
  const double p99   = pct(0.99);
  const double p999  = pct(0.999);
  const double p9999 = pct(0.9999);

  std::printf("msgs=%llu, time=%.3fs, rate=%.1f msgs/s\n",
              (unsigned long long)A.msgs, wall_s, mps);
  std::printf("latency_us: p50=%.3f p90=%.3f p99=%.3f p99.9=%.3f p99.99=%.3f\n",
              p50, p90, p99, p999, p9999);

  // CSV (simple single-row summary)
  if (A.out_csv){
    FILE* f = std::fopen(A.out_csv,"w");
    if (f){
      std::fprintf(f,
        "msgs,wall_s,rate_msgs_s,p50_us,p90_us,p99_us,p99_9_us,p99_99_us,band,band_lo,band_hi,band_tick,pin_core\n");
      std::fprintf(f,
        "%llu,%.6f,%.1f,%.6f,%.6f,%.6f,%.6f,%.6f,%s,%lld,%lld,%lld,%d\n",
        (unsigned long long)A.msgs, wall_s, mps, p50, p90, p99, p999, p9999,
        (A.use_band ? "yes" : "no"),
        (long long)A.band_lo, (long long)A.band_hi, (long long)A.band_tick,
        (A.pin_core ? *A.pin_core : -1)
      );
      std::fclose(f);
    }
  }

  H.write(A.hist);
  return 0;
}

// -----------------------------
// main
// -----------------------------
int main(int argc, char** argv){
  parse(argc, argv);
  maybe_pin_core(A.pin_core);

  if (A.use_band){
    // contiguous ladders in a compact price band → better locality, no hash lookups
    PriceBand band{(Tick)A.band_lo, (Tick)A.band_hi, (Tick)A.band_tick};
    PriceLevelsContig bids(band), asks(band);
    return run_bench(bids, asks);
  } else {
    PriceLevelsSparse bids, asks;
    return run_bench(bids, asks);
  }
}
