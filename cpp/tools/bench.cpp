#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "lob/types.hpp"
#include "lob/price_levels.hpp"
#include "lob/book_core.hpp"
#include "lob/logging.hpp"

#if defined(__APPLE__)
  #include <sys/types.h>
  #include <sys/sysctl.h>
#endif

// -----------------------------
// Small platform helpers
// -----------------------------
#if defined(__linux__)
  #define LOB_LINUX 1
  #include <sched.h>
  #include <unistd.h>
#else
  #define LOB_LINUX 0
#endif

#if defined(__x86_64__) || defined(_M_X64)
  #define LOB_X86_64 1
#else
  #define LOB_X86_64 0
#endif

#if LOB_X86_64
static inline uint64_t rdtsc_now() {
  unsigned int hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((uint64_t)hi << 32) | lo;
}
#endif

// -----------------------------
// CSV writer (tiny helper)
// -----------------------------
struct Csv {
  std::ofstream f;
  explicit Csv(const std::string& path) : f(path, std::ios::out | std::ios::trunc) {}
  template <typename... Ts>
  void row(const Ts&... xs) {
    bool first = true;
    (([&]{ if (!first) f << ','; first = false; f << xs; }()), ...);
    f << '\n';
  }
  explicit operator bool() const { return static_cast<bool>(f); }
};

// -----------------------------
// CPU/Env info (best-effort)
// -----------------------------
static std::string get_cpu_model() {
#if defined(__APPLE__)
  char buf[256]; size_t sz = sizeof(buf);
  if (sysctlbyname("machdep.cpu.brand_string", buf, &sz, nullptr, 0) == 0)
    return std::string(buf, sz ? sz-1 : 0);
  return "unknown (sysctl failed)";
#elif LOB_LINUX
  std::ifstream in("/proc/cpuinfo");
  std::string line;
  while (std::getline(in, line)) {
    auto pos = line.find("model name");
    if (pos != std::string::npos) {
      auto colon = line.find(':');
      if (colon != std::string::npos)
        return line.substr(colon + 2);
    }
  }
  return "unknown (/proc/cpuinfo missing)";
#else
  return "unknown (platform)";
#endif
}

static std::string get_compiler() {
#if defined(__clang__)
  return std::string("Clang ") + __clang_version__;
#elif defined(__GNUC__)
  return std::string("GCC ") + std::to_string(__GNUC__) + "." + std::to_string(__GNUC_MINOR__);
#elif defined(_MSC_VER)
  return std::string("MSVC ") + std::to_string(_MSC_VER);
#else
  return "unknown";
#endif
}

static std::string get_os() {
#if defined(__APPLE__)
  return "macOS";
#elif defined(_WIN32)
  return "Windows";
#elif defined(__linux__)
  return "Linux";
#else
  return "UnknownOS";
#endif
}

// -----------------------------
// Distributions: Zipf & Pareto
// -----------------------------
struct Zipf {
  // Zipf(s, N): Pr[k] ~ 1 / k^s, with support k=1..N
  // We sample by rejection using a simple approximate method (good enough for benchmarking).
  double s;
  int64_t N;
  std::mt19937_64& rng;
  std::uniform_real_distribution<double> U{0.0, 1.0};

  int64_t sample() {
    // For s≈1..2 typical, we can approximate via inverse transform of a continuous analog:
    // F(x) ~ (H_N - H_x)/H_N; we use a quick-and-dirty power-law inversion.
    double u = U(rng);
    double a = 1.0 / (1.0 - s + 1e-12); // avoid div by zero at s=1.0
    // Continuous power-law on [1,N]: x = [1 + u*(N^{1-s}-1)]^{1/(1-s)}
    double one_minus_s = 1.0 - s;
    double x;
    if (std::abs(one_minus_s) < 1e-6) {
      // s ~ 1: use exponential-ish fallback
      double C = std::log((double)N + 1.0);
      x = std::exp(u * C);
    } else {
      double A = std::pow((double)N, one_minus_s) - 1.0;
      x = std::pow(1.0 + u * A, 1.0 / one_minus_s);
    }
    int64_t k = (int64_t)std::llround(x);
    if (k < 1) k = 1;
    if (k > N) k = N;
    return k;
  }
};

struct Pareto {
  // scale xm, shape alpha => P(X >= x) = (xm/x)^alpha, x >= xm
  double xm;
  double alpha;
  std::mt19937_64& rng;
  std::uniform_real_distribution<double> U{0.0, 1.0};

  int64_t sample() {
    double u = U(rng);
    double x = xm / std::pow(1.0 - u, 1.0 / alpha);
    return (int64_t)std::llround(x);
  }
};

// -----------------------------
// Synthetic event generator
// -----------------------------
using namespace lob;

struct GenCfg {
  uint64_t total_events = 1'000'000;
  uint64_t warmup_events = 50'000;
  double   zipf_s = 1.2;       // for price level popularity
  int64_t  zipf_levels = 2000; // number of distinct price offsets used by generator
  double   pareto_alpha = 1.3; // for order sizes
  double   walk_sigma = 1.0;   // random-walk stddev (ticks)
  uint64_t seed = 0;
  bool     use_rdtsc = true;
  int      pin_core = -1;      // Linux-only; -1 means don't pin
  std::string outdir = "bench_out";
  double   market_ratio = 0.10; // 10% market orders
  double   cancel_ratio = 0.05; // 5% cancels (if any ids available)
  double   modify_ratio = 0.05; // 5% modifies
  bool     enable_stp = true;   // enable STP on takers
};

struct Percentiles {
  double p50, p90, p99, p999;
};

static Percentiles compute_percentiles(std::vector<uint64_t>& ns) {
  if (ns.empty()) return {0,0,0,0};
  std::sort(ns.begin(), ns.end());
  auto at = [&](double q)->double {
    size_t idx = (size_t)std::min<double>(ns.size()-1, std::floor(q * (ns.size()-1)));
    return (double)ns[idx];
  };
  return { at(0.50), at(0.90), at(0.99), at(0.999) };
}

static void make_histogram(const std::vector<uint64_t>& ns, Csv& csv) {
  // Log2 buckets: [0..1),[1..2),..,[2^k..2^{k+1})
  const int MAX_BUCKET = 40; // up to ~1s in ns (2^40 ~ 1.1e12)
  std::vector<uint64_t> buckets(MAX_BUCKET+1, 0);
  for (auto v : ns) {
    int b = (v == 0) ? 0 : (int)std::min<int>(MAX_BUCKET, (int)std::log2((double)v));
    buckets[b]++;
  }
  csv.row("bucket_log2_ns","count");
  uint64_t cum = 0;
  for (int b = 0; b <= MAX_BUCKET; ++b) {
    cum += buckets[b];
    csv.row(b, buckets[b]);
  }
}

// -----------------------------
// CPU pinning (Linux)
// -----------------------------
static void maybe_pin_core(int core) {
#if LOB_LINUX
  if (core < 0) return;
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(core, &set);
  if (sched_setaffinity(0, sizeof(set), &set) != 0) {
    std::perror("sched_setaffinity");
  } else {
    std::cerr << "[bench] pinned to CPU " << core << "\n";
  }
#else
  (void)core;
  std::cerr << "[bench] CPU pinning not supported on this OS; consider 'taskset' if on Linux.\n";
#endif
}

// -----------------------------
// Main benchmark
// -----------------------------
int main(int argc, char** argv) {
  // ---- Parse simple flags
  GenCfg cfg;
  for (int i = 1; i < argc; ++i) {
    auto arg = std::string(argv[i]);
    auto next = [&](int& i){ return (i+1 < argc) ? std::string(argv[++i]) : std::string(); };
    if (arg == "--events")          cfg.total_events = std::stoull(next(i));
    else if (arg == "--warmup")     cfg.warmup_events = std::stoull(next(i));
    else if (arg == "--zipf-s")     cfg.zipf_s = std::stod(next(i));
    else if (arg == "--zipf-levels")cfg.zipf_levels = std::stoll(next(i));
    else if (arg == "--pareto-alpha") cfg.pareto_alpha = std::stod(next(i));
    else if (arg == "--walk-sigma") cfg.walk_sigma = std::stod(next(i));
    else if (arg == "--seed")       cfg.seed = std::stoull(next(i));
    else if (arg == "--outdir")     cfg.outdir = next(i);
    else if (arg == "--pin-core")   cfg.pin_core = std::stoi(next(i));
    else if (arg == "--rdtsc")      cfg.use_rdtsc = (next(i) != "0");
    else if (arg == "--market-ratio") cfg.market_ratio = std::stod(next(i));
    else if (arg == "--cancel-ratio") cfg.cancel_ratio = std::stod(next(i));
    else if (arg == "--modify-ratio") cfg.modify_ratio = std::stod(next(i));
    else if (arg == "--stp")        cfg.enable_stp = (next(i) != "0");
    else {
      std::cerr << "Unknown arg: " << arg << "\n";
      return 1;
    }
  }

  std::filesystem::create_directories(cfg.outdir);

  // ---- Pin if requested
  maybe_pin_core(cfg.pin_core);

  // ---- RNG
  uint64_t seed = cfg.seed ? cfg.seed : (uint64_t)std::chrono::high_resolution_clock::now().time_since_epoch().count();
  std::mt19937_64 rng(seed);

  Zipf zipf{cfg.zipf_s, cfg.zipf_levels, rng};
  Pareto pareto{/*xm*/1.0, cfg.pareto_alpha, rng};
  std::normal_distribution<double> walk(0.0, cfg.walk_sigma);
  std::uniform_real_distribution<double> U(0.0, 1.0);
  std::uniform_int_distribution<uint64_t> user_pick(1, 2000);

  // ---- Book setup
  using lob::PriceLevelsSparse;
  PriceLevelsSparse bids, asks;
  lob::BookCore book(bids, asks, /*logger*/nullptr);

  // ---- State for cancels/modifies
  std::vector<lob::OrderId> live_ids;
  live_ids.reserve(1'000'000);
  lob::OrderId next_id = 1;

  // ---- Helpers to build synthetic prices (random walk around mid)
  lob::Tick mid = 100000; // arbitrary
  lob::Tick last_offset = 0;

  auto new_price = [&](lob::Side s)->lob::Tick {
    // Choose a popular offset from Zipf, then apply a small random walk
    int64_t base_off = zipf.sample();           // 1..N
    int64_t sign = (s == lob::Side::Bid ? -1 : +1);
    double step = walk(rng);
    last_offset += (lob::Tick)std::llround(step);
    lob::Tick px = mid + sign * (base_off + std::abs(last_offset));
    if (px < 1) px = 1;
    return px;
  };

  auto new_size = [&]()->lob::Quantity {
    int64_t q = pareto.sample(); // heavy‑tailed sizes
    if (q < 1) q = 1;
    if (q > 1'000'000) q = 1'000'000; // clamp
    return q;
  };

  // ---- Timing storage
  std::vector<uint64_t> lat_ns; lat_ns.reserve(cfg.total_events);
  std::vector<uint64_t> lat_ns_post_warm; lat_ns_post_warm.reserve(cfg.total_events);

  auto now_steady_ns = []()->uint64_t {
    return (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
  };

  auto measure_event = [&](auto&& fn)->uint64_t {
#if LOB_X86_64
    if (cfg.use_rdtsc) {
      uint64_t t0 = rdtsc_now();
      fn();
      uint64_t t1 = rdtsc_now();
      // We don't know TSC->ns ratio portably; still useful as cycles.
      return (t1 - t0);
    }
#endif
    uint64_t t0 = now_steady_ns();
    fn();
    uint64_t t1 = now_steady_ns();
    return (t1 - t0);
  };

  // ---- Run
  uint64_t started = now_steady_ns();
  uint64_t started_post_warm = 0;
  uint64_t events_post_warm = 0;

  for (uint64_t i = 0; i < cfg.total_events; ++i) {
    double r = U(rng);

    if (r < cfg.cancel_ratio && !live_ids.empty()) {
      // cancel
      size_t idx = (size_t)(rng() % live_ids.size());
      lob::OrderId id = live_ids[idx];
      auto dur = measure_event([&]{ book.cancel(id); });
      lat_ns.push_back(dur);
      if (i >= cfg.warmup_events) { lat_ns_post_warm.push_back(dur); events_post_warm++; }
      // swap-remove
      live_ids[idx] = live_ids.back(); live_ids.pop_back();
      continue;
    }

    if (r < cfg.cancel_ratio + cfg.modify_ratio && !live_ids.empty()) {
      // modify: change price & qty moderately
      size_t idx = (size_t)(rng() % live_ids.size());
      lob::OrderId id = live_ids[idx];
      lob::ModifyOrder m{
        .seq = (lob::SeqNo)i,
        .ts  = (lob::Timestamp)i, // synthetic
        .id  = id,
        .new_price = new_price((rng()&1) ? lob::Side::Bid : lob::Side::Ask),
        .new_qty   = (lob::Quantity)std::max<int64_t>(1, (int64_t)(new_size()/2)),
        .flags     = 0u
      };
      auto dur = measure_event([&]{ (void)book.modify(m); });
      lat_ns.push_back(dur);
      if (i >= cfg.warmup_events) { lat_ns_post_warm.push_back(dur); events_post_warm++; }
      continue;
    }

    // new order (limit or market)
    bool market = (U(rng) < cfg.market_ratio);
    lob::Side side = (U(rng) < 0.5) ? lob::Side::Bid : lob::Side::Ask;
    lob::NewOrder o{
      .seq   = (lob::SeqNo)i,
      .ts    = (lob::Timestamp)i,
      .id    = next_id++,
      .user  = user_pick(rng),
      .side  = side,
      .price = market ? 0 : new_price(side),
      .qty   = new_size(),
      .flags = (cfg.enable_stp ? lob::OrderFlags::STP : 0u)
    };

    uint64_t dur = 0;
    if (market) {
      dur = measure_event([&]{ (void)book.submit_market(o); });
    } else {
      dur = measure_event([&]{ auto r = book.submit_limit(o); });
    }
    lat_ns.push_back(dur);
    if (i >= cfg.warmup_events) {
      if (events_post_warm == 0) started_post_warm = now_steady_ns();
      lat_ns_post_warm.push_back(dur);
      events_post_warm++;
    } else {
      // if it rested, keep id for potential cancel/modify
      live_ids.push_back(o.id);
    }
  }

  uint64_t ended = now_steady_ns();

  // ---- If we used rdtsc, we can't convert to ns portably;
  // still write cycles and label accordingly.
  const bool units_are_cycles = (cfg.use_rdtsc && LOB_X86_64);

  // ---- Percentiles (post-warm)
  auto pct = compute_percentiles(lat_ns_post_warm);

  // ---- Throughput (post-warm)
  double dur_ns = (double)(ended - started_post_warm ? ended - started_post_warm : 1);
  double ev_ps  = (dur_ns > 0) ? (1e9 * (double)events_post_warm / dur_ns) : 0.0;

  // ---- Write CSVs
  // per-event latencies
  {
    Csv csv(cfg.outdir + "/latencies.csv");
    csv.row("i","latency", units_are_cycles ? "cycles" : "ns");
    for (size_t i = 0; i < lat_ns.size(); ++i) csv.row(i, lat_ns[i], "");
  }
  // histogram (post-warm)
  {
    Csv csv(cfg.outdir + "/latency_histogram.csv");
    make_histogram(lat_ns_post_warm, csv);
  }
  // summary
  {
    Csv csv(cfg.outdir + "/summary.csv");
    csv.row("metric","value","units");
    csv.row("events_total", cfg.total_events, "");
    csv.row("warmup_events", cfg.warmup_events, "");
    csv.row("events_measured", events_post_warm, "");
    csv.row("p50", pct.p50, units_are_cycles ? "cycles" : "ns");
    csv.row("p90", pct.p90, units_are_cycles ? "cycles" : "ns");
    csv.row("p99", pct.p99, units_are_cycles ? "cycles" : "ns");
    csv.row("p999", pct.p999, units_are_cycles ? "cycles" : "ns");
    csv.row("throughput", ev_ps, "events_per_second");
  }
  // environment
  {
    Csv csv(cfg.outdir + "/environment.csv");
    csv.row("field","value");
    csv.row("cpu_model", get_cpu_model());
    csv.row("os", get_os());
    csv.row("compiler", get_compiler());
    csv.row("march_native", "yes"); // set in CMake for bench_tool
    csv.row("seed", seed);
    csv.row("rdtsc_mode", (units_are_cycles ? "cycles" : "ns"));
    csv.row("pin_core", cfg.pin_core);
    csv.row("zipf_s", cfg.zipf_s);
    csv.row("zipf_levels", cfg.zipf_levels);
    csv.row("pareto_alpha", cfg.pareto_alpha);
    csv.row("walk_sigma", cfg.walk_sigma);
    csv.row("market_ratio", cfg.market_ratio);
    csv.row("cancel_ratio", cfg.cancel_ratio);
    csv.row("modify_ratio", cfg.modify_ratio);
    csv.row("stp_enabled", cfg.enable_stp ? "1" : "0");
  }

  std::cout << "[bench] wrote CSVs to: " << cfg.outdir << "\n";
  std::cout << "[bench] p50=" << pct.p50 << (units_are_cycles ? " cycles" : " ns")
            << " p90=" << pct.p90 << (units_are_cycles ? " cycles" : " ns")
            << " p99=" << pct.p99 << (units_are_cycles ? " cycles" : " ns")
            << " p999=" << pct.p999 << (units_are_cycles ? " cycles" : " ns")
            << " | throughput=" << ev_ps << " ev/s"
            << " | measured=" << events_post_warm << " events\n";
  return 0;
}
