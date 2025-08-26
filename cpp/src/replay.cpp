#include <chrono>
#include <thread>
#include <string>
#include <vector>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <limits>
#include <optional>

// Linux-only pinning headers (no-op elsewhere)
#if defined(__linux__)
  #include <sched.h>
  #include <pthread.h>
#endif

#include "lob/replay.hpp"
#include "lob/taq_writer.hpp"
#include "lob/book_core.hpp"
#include "lob/price_levels.hpp"

using namespace lob;

// -----------------------------
// Pinning
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
// CSV loader (tiny & strict)
// -----------------------------
static inline std::string trim(const std::string& s) {
  size_t i = 0, j = s.size();
  while (i < j && std::isspace((unsigned char)s[i])) ++i;
  while (j > i && std::isspace((unsigned char)s[j-1])) --j;
  return s.substr(i, j - i);
}

static bool parse_type(const std::string& t, NormType& out) {
  std::string x = t;
  for (auto& c : x) c = (char)std::tolower((unsigned char)c);
  if (x == "book")  { out = NormType::Book;  return true; }
  if (x == "trade") { out = NormType::Trade; return true; }
  return false;
}

static bool parse_side(const std::string& s, Side& out) {
  if (s.empty()) { out = Side::Ask; return true; }  // default for trades if unknown
  std::string x; x.reserve(s.size());
  for (char c : s) x.push_back((char)std::tolower((unsigned char)c));
  if (x.size() == 1) {
    if (x[0] == 'b') { out = Side::Bid; return true; }
    if (x[0] == 'a') { out = Side::Ask; return true; }
    if (x[0] == 's') { out = Side::Ask; return true; } // 's' == sell
  }
  if (x == "b" || x == "bid"  || x == "buy")         { out = Side::Bid; return true; }
  if (x == "a" || x == "ask"  || x == "sell" || x=="s"){ out = Side::Ask; return true; }
  return false;
}

bool lob::load_normalized_csv(const std::string& path, std::vector<NormEvent>& out) {
  out.clear();
  std::FILE* f = std::fopen(path.c_str(), "rb");
  if (!f) {
    std::fprintf(stderr, "Failed to open normalized CSV '%s': %s\n",
                 path.c_str(), std::strerror(errno));
    return false;
  }

  char* line = nullptr;
  size_t cap = 0;
  ssize_t n = 0;

  // header
  n = getline(&line, &cap, f);
  if (n <= 0) { std::fprintf(stderr, "Empty CSV: %s\n", path.c_str()); std::fclose(f); return false; }
  std::string header(line, (size_t)n);
  if (header.find("ts_ns") == std::string::npos ||
      header.find("type")  == std::string::npos ||
      header.find("side")  == std::string::npos ||
      header.find("price") == std::string::npos ||
      header.find("qty")   == std::string::npos) {
    std::fprintf(stderr, "Unexpected CSV header for '%s'. Expected: ts_ns,type,side,price,qty\n", path.c_str());
    std::fclose(f); return false;
  }

  auto next_field = [](const char* s, size_t& i, size_t N)->std::string {
    size_t start = i;
    while (i < N && s[i] != ',' && s[i] != '\n' && s[i] != '\r') ++i;
    std::string v(s + start, i - start);
    if (i < N && s[i] == ',') ++i;
    return trim(v);
  };

  while ((n = getline(&line, &cap, f)) > 0) {
    const size_t N = (size_t)n;
    size_t i = 0;
    std::string f_ts   = next_field(line, i, N);
    std::string f_type = next_field(line, i, N);
    std::string f_side = next_field(line, i, N);
    std::string f_px   = next_field(line, i, N);
    std::string f_qty  = next_field(line, i, N);

    if (f_ts.empty()) continue;

    NormEvent ev{};
    ev.ts_ns = std::strtoll(f_ts.c_str(), nullptr, 10);

    if (!parse_type(f_type, ev.type)) {
      std::fprintf(stderr, "Bad type '%s'\n", f_type.c_str());
      continue;
    }
    if (!parse_side(f_side, ev.side)) {
      std::fprintf(stderr, "Bad side '%s'\n", f_side.c_str());
      continue;
    }
    ev.price = std::strtod(f_px.c_str(), nullptr);
    ev.qty   = std::strtod(f_qty.c_str(), nullptr);

    out.push_back(ev);
  }

  if (line) free(line);
  std::fclose(f);
  return true;
}

// -----------------------------
// LevelBook impl
// -----------------------------
void LevelBook::set_level(Side s, double px, double total_sz) {
  auto& M = (s == Side::Bid) ? bids_ : asks_;
  if (total_sz <= 0.0) {
    auto it = M.find(px);
    if (it != M.end()) M.erase(it);
    return;
  }
  M[px] = total_sz;
}

double LevelBook::best_px(Side s) const {
  if (s == Side::Bid) {
    if (bids_.empty()) return std::numeric_limits<double>::quiet_NaN();
    return bids_.rbegin()->first;
  } else {
    if (asks_.empty()) return std::numeric_limits<double>::quiet_NaN();
    return asks_.begin()->first;
  }
}

double LevelBook::best_sz(Side s) const {
  if (s == Side::Bid) {
    if (bids_.empty()) return 0.0;
    return bids_.rbegin()->second;
  } else {
    if (asks_.empty()) return 0.0;
    return asks_.begin()->second;
  }
}

void LevelBook::clear() {
  bids_.clear(); asks_.clear();
}

// -----------------------------
// Replayer impl
// -----------------------------
static inline uint64_t pack64(uint64_t a, uint64_t b) { return (a << 32) ^ b; }

uint64_t Replayer::level_key(Side s, double px) {
  const double q = std::round(px * 1e8) / 1e8;
  uint64_t hi = (s == Side::Bid) ? 1u : 0u;
  uint64_t h = std::hash<long long>{}((long long)std::llround(q * 1e8));
  return pack64(hi, h);
}

Replayer::Replayer(BookCore& book, TaqWriter& writer)
  : book_(book), writer_(writer) {}

int64_t Replayer::align_up(int64_t ts_ns, int64_t step_ns) {
  if (step_ns <= 0) return ts_ns;
  const int64_t r = ts_ns % step_ns;
  return r ? (ts_ns + (step_ns - r)) : ts_ns;
}

void Replayer::apply_book_event(const NormEvent& e) {
  const uint64_t key = level_key(e.side, e.price);
  const double new_total = (e.qty < 0.0 ? 0.0 : e.qty);
  const double prev_total = [this, key]{
    auto it = level_size_.find(key);
    return (it == level_size_.end()) ? 0.0 : it->second;
  }();

  level_book_.set_level(e.side, e.price, new_total);
  if (new_total == prev_total) return;

  auto it_id = level_order_id_.find(key);
  if (it_id == level_order_id_.end()) {
    if (new_total <= 0.0) return;
    NewOrder o{};
    o.seq   = 0; o.ts = 0;
    o.id    = (OrderId) (0x9000000000000000ull ^ key);
    o.user  = (UserId) 0x42;
    o.side  = e.side;
    o.price = (Tick) e.price;
    o.qty   = (Quantity) new_total;
    o.flags = 0;
    (void) book_.submit_limit(o);
    level_order_id_[key] = o.id;
    level_size_[key] = new_total;
    return;
  }

  OrderId id = it_id->second;
  if (new_total <= 0.0) {
    (void) book_.cancel(id);
    level_order_id_.erase(it_id);
    level_size_.erase(key);
    return;
  }

  const double delta = new_total - prev_total;
  if (delta > 0.0) {
    NewOrder o{};
    o.seq   = 0; o.ts = 0;
    o.id    = (OrderId) (0xA000000000000000ull ^ (key + (OrderId)std::llround(delta * 1e8)));
    o.user  = (UserId) 0x42;
    o.side  = e.side;
    o.price = (Tick) e.price;
    o.qty   = (Quantity) delta;
    o.flags = 0;
    (void) book_.submit_limit(o);
    level_size_[key] = new_total;
  } else {
    ModifyOrder m{};
    m.seq       = 0; m.ts = 0;
    m.id        = id;
    m.new_price = (Tick) e.price;
    m.new_qty   = (Quantity) new_total;
    m.flags     = 0;
    (void) book_.modify(m);
    level_size_[key] = new_total;
  }
}

void Replayer::emit_trade_taq(const NormEvent& e) {
  const char side_char = (e.side == Side::Bid) ? 'B' : 'A';
  writer_.write_trade_row(e.ts_ns, e.price, e.qty, side_char);
}

bool Replayer::run(const std::vector<NormEvent>& events, const Options& opt) {
  if (events.empty()) {
    std::fprintf(stderr, "Replayer: no events provided.\n");
    return false;
  }

  const int64_t start_ns = events.front().ts_ns;
  int64_t next_sample_ns = align_up(start_ns, opt.cadence_ns);

  auto wall_start = std::chrono::steady_clock::now();
  const double speed = (opt.speed <= 0.0) ? 1.0 : opt.speed;

  int64_t last_ts_ns = start_ns;

  for (const auto& e : events) {
    while (e.ts_ns >= next_sample_ns) {
      const double bid_px = level_book_.best_px(Side::Bid);
      const double bid_sz = level_book_.best_sz(Side::Bid);
      const double ask_px = level_book_.best_px(Side::Ask);
      const double ask_sz = level_book_.best_sz(Side::Ask);
      writer_.write_quote_row(next_sample_ns, bid_px, bid_sz, ask_px, ask_sz);
      next_sample_ns += opt.cadence_ns;
    }

    if (opt.realtime_sleep) {
      const int64_t gap_ns = e.ts_ns - last_ts_ns;
      if (gap_ns > 0) {
        const double scaled_ns = (double)gap_ns / speed;
        auto sleep_dur = std::chrono::nanoseconds((int64_t)scaled_ns);
        if (sleep_dur.count() > 0) std::this_thread::sleep_for(sleep_dur);
      }
    }
    last_ts_ns = e.ts_ns;

    if (e.type == NormType::Book) {
      apply_book_event(e);
    } else {
      emit_trade_taq(e);
    }
  }
  return true;
}

// -----------------------------
// CLI
// -----------------------------
static void usage() {
  std::fprintf(stderr,
R"(lob replay --file <normalized.csv> [--speed <Nx>] [--cadence-ms <ms>]
          [--quotes-out <quotes.csv>] [--trades-out <trades.csv>] [--no-sleep]
          [--pin-core N] [--band lo:hi:tick]

Required:
  --file         Normalized CSV with columns: ts_ns,type,side,price,qty

Options:
  --speed        "1x", "10x", "50x" or just "50" (default 1x)
  --cadence-ms   TAQ quote sampling cadence in ms (default 50)
  --quotes-out   Quotes CSV path (default taq_quotes.csv)
  --trades-out   Trades CSV path (default taq_trades.csv)
  --no-sleep     Do not sleep between events
  --pin-core     Pin worker thread to core N (Linux only; no-op elsewhere)
  --band         Use compact contiguous ladders: lo:hi:tick (e.g. 999000:1001000:1)

Example:
  lob replay --file parquet_export.csv --speed 50x --cadence-ms 50 \
             --pin-core 2 --band 999000:1001000:1
)");
}

int main(int argc, char** argv) {
  std::string file;
  double speed = 1.0;
  int cadence_ms = 50;
  std::string quotes_csv = "taq_quotes.csv";
  std::string trades_csv = "taq_trades.csv";
  bool realtime_sleep = true;
  std::optional<int> pin_core;
  bool use_band=false; long long band_lo=0, band_hi=0, band_tick=1;

  auto need = [&](const char* name)->bool {
    (void)name;
    return true;
  };

  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--file") { if (i+1>=argc){usage();return 2;} file = argv[++i]; }
    else if (a == "--speed") {
      if (i+1>=argc){usage();return 2;}
      std::string s = argv[++i];
      if (!s.empty() && (s.back()=='x'||s.back()=='X')) s.pop_back();
      speed = std::strtod(s.c_str(), nullptr);
      if (speed <= 0.0) speed = 1.0;
    }
    else if (a == "--cadence-ms") {
      if (i+1>=argc){usage();return 2;}
      cadence_ms = std::atoi(argv[++i]);
      if (cadence_ms <= 0) cadence_ms = 50;
    }
    else if (a == "--quotes-out") {
      if (i+1>=argc){usage();return 2;}
      quotes_csv = argv[++i];
    }
    else if (a == "--trades-out") {
      if (i+1>=argc){usage();return 2;}
      trades_csv = argv[++i];
    }
    else if (a == "--no-sleep") {
      realtime_sleep = false;
    }
    else if (a == "--pin-core") {
      if (i+1>=argc){usage();return 2;}
      pin_core = std::atoi(argv[++i]);
    }
    else if (a == "--band") {
      if (i+1>=argc){usage();return 2;}
      long long lo=0,hi=0,t=1;
      if (std::sscanf(argv[++i], "%lld:%lld:%lld", &lo,&hi,&t)==3 && hi>lo && t>0){
        use_band=true; band_lo=lo; band_hi=hi; band_tick=t;
      } else {
        std::fprintf(stderr,"Bad --band format. Use: lo:hi:tick\n");
        return 2;
      }
    }
    else if (a == "-h" || a == "--help") {
      usage(); return 0;
    }
    else {
      std::fprintf(stderr, "Unknown arg: %s\n", a.c_str());
      usage();
      return 2;
    }
  }

  if (file.empty()) {
    usage();
    return 2;
  }

  maybe_pin_core(pin_core);

  // Build a BookCore with chosen ladders
  std::unique_ptr<BookCore> book_ptr;
  std::unique_ptr<PriceLevelsContig> contig_bids;
  std::unique_ptr<PriceLevelsContig> contig_asks;
  std::unique_ptr<PriceLevelsSparse> sparse_bids;
  std::unique_ptr<PriceLevelsSparse> sparse_asks;

  if (use_band){
    PriceBand band{(Tick)band_lo, (Tick)band_hi, (Tick)band_tick};
    contig_bids = std::make_unique<PriceLevelsContig>(band);
    contig_asks = std::make_unique<PriceLevelsContig>(band);
    book_ptr = std::make_unique<BookCore>(*contig_bids, *contig_asks, /*logger*/nullptr);
  } else {
    sparse_bids = std::make_unique<PriceLevelsSparse>();
    sparse_asks = std::make_unique<PriceLevelsSparse>();
    book_ptr = std::make_unique<BookCore>(*sparse_bids, *sparse_asks, /*logger*/nullptr);
  }

  // Load normalized CSV
  std::vector<NormEvent> events;
  if (!load_normalized_csv(file, events)) {
    return 2;
  }
  if (events.empty()) {
    std::fprintf(stderr, "No rows in input.\n");
    return 2;
  }

  std::stable_sort(events.begin(), events.end(),
                   [](const NormEvent& a, const NormEvent& b){ return a.ts_ns < b.ts_ns; });

  // Open TAQ outputs
  TaqWriter writer;
  if (!writer.open(quotes_csv, trades_csv)) {
    return 2;
  }

  // Run replay
  Replayer::Options opt;
  opt.speed = speed;
  opt.cadence_ns = (int64_t)cadence_ms * 1'000'000;
  opt.realtime_sleep = realtime_sleep;
  opt.quotes_out_csv = quotes_csv;
  opt.trades_out_csv = trades_csv;

  Replayer rp(*book_ptr, writer);
  const bool ok = rp.run(events, opt);
  writer.close();
  return ok ? 0 : 3;
}
