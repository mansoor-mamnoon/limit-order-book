#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <algorithm>
#include <chrono>

// Include your library headers
#include "lob/book_core.hpp"
#include "lob/price_levels.hpp"
#include "lob/types.hpp"

using namespace lob;
using clk = std::chrono::steady_clock;

struct Args {
  uint64_t msgs = 2'000'000;
  uint64_t warmup = 50'000;
  const char* out_csv = nullptr;
  const char* hist = nullptr;
} A;

static bool arg_eq(const char* a, const char* b){ return std::strcmp(a,b)==0; }
static uint64_t to_u64(const char* s){ char* e; auto v = std::strtoull(s,&e,10); if(*e) { std::fprintf(stderr,"bad int: %s\n",s); std::exit(2);} return v; }

static void parse(int argc, char** argv){
  for (int i=1;i<argc;i++){
    if (arg_eq(argv[i],"--msgs") && i+1<argc)    { A.msgs = to_u64(argv[++i]); }
    else if (arg_eq(argv[i],"--warmup") && i+1<argc){ A.warmup = to_u64(argv[++i]); }
    else if (arg_eq(argv[i],"--out-csv") && i+1<argc){ A.out_csv = argv[++i]; }
    else if (arg_eq(argv[i],"--hist") && i+1<argc){ A.hist = argv[++i]; }
    else { std::fprintf(stderr,"Unknown arg: %s\n", argv[i]); std::exit(2); }
  }
}

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

int main(int argc, char** argv){
  parse(argc, argv);

  // Simple synthetic book with sparse ladders
  PriceLevelsSparse bids;
  PriceLevelsSparse asks;
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
  auto p = [&](double q){
    size_t idx = (size_t)(q * (us.size()-1) + 0.5);
    return us[idx];
  };
  double p50=p(0.50), p90=p(0.90), p99=p(0.99), p999=p(0.999);

  std::printf("msgs=%llu, time=%.3fs, rate=%.1f msgs/s\n",
              (unsigned long long)A.msgs, wall_s, mps);
  std::printf("latency_us: p50=%.2f p90=%.2f p99=%.2f p99.9=%.2f\n",
              p50, p90, p99, p999);

  if (A.out_csv)_
