// cpp/tools/replay.cpp
#include "lob/book_core.hpp"
#include "lob/logging.hpp"
#include "lob/price_levels.hpp"

#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <limits>

using namespace lob;

struct Args {
  std::string file;            // events.{bin|csv}
  std::string quotes_out;      // L1 CSV: ts_ns,bid_px,bid_qty,ask_px,ask_qty
  std::string trades_out;      // trades.bin copied from logger
  std::string snapshot_in;     // resume seed
  std::string snapshot_out;    // exact snapshot file path we will produce
  int         cadence_ms = 50;
  std::string speed = "50x";
  long long   snapshot_at_ns = -1;
};

static bool ends_with(std::string_view s, std::string_view suf) {
  if (s.size() < suf.size()) return false;
  return std::equal(s.end()-suf.size(), s.end(), suf.begin());
}

static std::optional<Args> parse_args(int argc, char** argv) {
  Args a;
  for (int i = 1; i < argc; ++i) {
    std::string k = argv[i];
    auto need = [&](const char* opt) -> std::string {
      if (i + 1 >= argc) { std::cerr << "Missing value after " << opt << "\n"; std::exit(2); }
      return std::string(argv[++i]);
    };
    if (k == "--file") a.file = need("--file");
    else if (k == "--quotes-out") a.quotes_out = need("--quotes-out");
    else if (k == "--trades-out") a.trades_out = need("--trades-out");
    else if (k == "--cadence-ms") a.cadence_ms = std::stoi(need("--cadence-ms"));
    else if (k == "--speed") a.speed = need("--speed");
    else if (k == "--snapshot-at-ns") a.snapshot_at_ns = std::stoll(need("--snapshot-at-ns"));
    else if (k == "--snapshot-out") a.snapshot_out = need("--snapshot-out");
    else if (k == "--snapshot-in") a.snapshot_in = need("--snapshot-in");
    else if (k == "--help" || k == "-h") {
      std::cout <<
R"(Usage: replay_tool --file EVENTS.{bin|csv}
                    [--quotes-out QUOTES.csv]
                    [--trades-out TRADES.bin]
                    [--snapshot-in SNAP.bin]
                    [--snapshot-at-ns CUT_NS --snapshot-out SNAP.bin]
)";
      std::exit(0);
    } else {
      std::cerr << "Unknown option: " << k << "\n"; std::exit(2);
    }
  }
  if (a.file.empty()) { std::cerr << "Required: --file <events.bin|events.csv>\n"; return std::nullopt; }
  return a;
}

static std::optional<std::filesystem::path> newest_bin_in(const std::filesystem::path& dir) {
  if (!std::filesystem::exists(dir)) return std::nullopt;
  std::optional<std::filesystem::path> best;
  std::filesystem::file_time_type best_t;
  for (auto& e : std::filesystem::directory_iterator(dir)) {
    if (!e.is_regular_file()) continue;
    if (e.path().extension() != ".bin") continue;
    auto t = std::filesystem::last_write_time(e.path());
    if (!best || t > best_t) { best = e.path(); best_t = t; }
  }
  return best;
}

// ----- L1 writer --------------------------------------------------------------
struct L1CSV {
  std::ofstream out;
  explicit L1CSV(const std::string& path) {
    if (!path.empty()) {
      std::filesystem::create_directories(std::filesystem::path(path).parent_path());
      out.open(path);
      if (!out) { std::perror(("open " + path).c_str()); std::exit(1); }
      out << "ts_ns,bid_px,bid_qty,ask_px,ask_qty\n";
    }
  }
  inline void write(long long ts_ns, long long bid_px, long long bid_qty,
                    long long ask_px, long long ask_qty) {
    if (out) out << ts_ns << "," << bid_px << "," << bid_qty << ","
                 << ask_px << "," << ask_qty << "\n";
  }
};

static void write_top_of_book(L1CSV& l1, long long ts_ns,
                              const IPriceLevels& bids, const IPriceLevels& asks) {
  const Tick bb = bids.best_bid();
  const Tick aa = asks.best_ask();
  long long bb_qty = 0;
  long long aa_qty = 0;
  if (bb != std::numeric_limits<Tick>::min()) {
    if (auto* L = const_cast<IPriceLevels&>(bids).best_level_ptr(Side::Bid)) bb_qty = static_cast<long long>(L->total_qty);
  }
  if (aa != std::numeric_limits<Tick>::max()) {
    if (auto* L = const_cast<IPriceLevels&>(asks).best_level_ptr(Side::Ask)) aa_qty = static_cast<long long>(L->total_qty);
  }
  l1.write(ts_ns, static_cast<long long>(bb), bb_qty, static_cast<long long>(aa), aa_qty);
}

int main(int argc, char** argv) {
  auto maybe = parse_args(argc, argv);
  if (!maybe) return 2;
  Args a = *maybe;

  // Seed ladders (empty or from snapshot_in)
  PriceLevelsSparse bids, asks;
  SeqNo   snap_seq{0};
  Timestamp snap_ts{0};
  if (!a.snapshot_in.empty()) {
    if (!load_snapshot_file(a.snapshot_in, bids, asks, snap_seq, snap_ts)) {
      std::cerr << "Failed to load snapshot: " << a.snapshot_in << "\n";
      return 1;
    }
    std::cerr << "[replay] loaded snapshot seq=" << snap_seq << " ts=" << snap_ts << "\n";
  }

  // Logger (no snapshots produced by logger during replay)
  SnapshotWriter dummy_snap{"."};
  JsonlBinLogger logger("replay_tmp", /*snapshot_every*/0, &dummy_snap);

  // Book holds references to bids/asks above
  BookCore book{bids, asks, &logger};
  book.rebuild_index_from_books();

  // Prepare writer into a temp dir; then copy to exact --snapshot-out path
  std::filesystem::path desired_snap = a.snapshot_out.empty()
    ? std::filesystem::path{}
    : std::filesystem::weakly_canonical(std::filesystem::path(a.snapshot_out));
  std::filesystem::path tmp_dir = desired_snap.empty()
    ? std::filesystem::path(".")
    : desired_snap.parent_path() / ".snap_tmp";
  if (!desired_snap.empty()) {
    std::error_code ec;
    std::filesystem::create_directories(tmp_dir, ec);
  }
  SnapshotWriter snapshot_writer{ tmp_dir.string().empty() ? std::string(".") : tmp_dir.string() };

  bool did_snapshot = false;
  auto maybe_dump = [&](long long current_ts_ns, SeqNo current_seq) {
    if (a.snapshot_at_ns >= 0 && !did_snapshot) {
      if (current_ts_ns >= a.snapshot_at_ns) {
        if (!snapshot_writer.write_snapshot(bids, asks, current_seq, static_cast<Timestamp>(current_ts_ns))) {
          std::cerr << "[replay] ERROR writing snapshot\n";
          std::exit(1);
        }
        if (!desired_snap.empty()) {
          auto latest = newest_bin_in(tmp_dir);
          if (!latest) {
            std::cerr << "[replay] ERROR: no snapshot file found in " << tmp_dir << "\n";
            std::exit(1);
          }
          std::error_code ec;
          std::filesystem::create_directories(desired_snap.parent_path(), ec);
          std::filesystem::copy_file(*latest, desired_snap,
                                     std::filesystem::copy_options::overwrite_existing, ec);
          if (ec) {
            std::cerr << "[replay] ERROR copying snapshot to " << desired_snap << ": " << ec.message() << "\n";
            std::exit(1);
          }
          std::cerr << "[replay] wrote snapshot at ts=" << current_ts_ns
                    << " seq=" << current_seq << " -> " << desired_snap << "\n";
        } else {
          std::cerr << "[replay] wrote snapshot at ts=" << current_ts_ns
                    << " seq=" << current_seq << "\n";
        }
        did_snapshot = true;
      }
    }
  };

  // L1 quotes CSV
  L1CSV l1(a.quotes_out);

  // State for snapshot timing
  SeqNo last_seq{snap_seq};
  long long last_ts{static_cast<long long>(snap_ts)};

  if (ends_with(a.file, ".bin")) {
    std::ifstream in(a.file, std::ios::binary);
    if (!in) { std::perror(("open " + a.file).c_str()); return 1; }
    EventBin e{};
    while (true) {
      in.read(reinterpret_cast<char*>(&e), sizeof(e));
      if (!in) break;

      if (snap_seq > 0 && e.seq <= snap_seq) continue;

      if (e.type == EventType::NewLimit || e.type == EventType::NewMarket) {
        NewOrder o{e.seq, e.ts, e.id, e.user,
                   (e.side==0?Side::Bid:Side::Ask),
                   e.price, e.qty, (e.is_limit?0:IOC)};
        if (e.type == EventType::NewLimit) book.submit_limit(o);
        else                               book.submit_market(o);
      } else if (e.type == EventType::Cancel) {
        book.cancel(e.id);
      }
      last_seq = e.seq;
      last_ts  = static_cast<long long>(e.ts);

      if (l1.out) write_top_of_book(l1, last_ts, bids, asks);
      maybe_dump(last_ts, last_seq);
    }
  } else if (ends_with(a.file, ".csv")) {
    // CSV: use first column as ts_ns (or adapt to your schema).
    std::ifstream in(a.file);
    if (!in) { std::perror(("open " + a.file).c_str()); return 1; }
    std::string header, line;
    if (std::getline(in, header)) {
      while (std::getline(in, line)) {
        if (line.empty()) continue;
        auto pos = line.find(',');
        long long ts_ns = last_ts;
        try { ts_ns = std::stoll(line.substr(0, pos)); } catch (...) {}
        last_ts = ts_ns;

        if (l1.out) write_top_of_book(l1, last_ts, bids, asks);
        maybe_dump(last_ts, last_seq);
      }
    }
  } else {
    std::cerr << "Unsupported --file extension (need .bin or .csv): " << a.file << "\n";
    return 2;
  }

  logger.flush();

  if (!a.trades_out.empty()) {
    std::ifstream t_in(logger.trades_bin_path(), std::ios::binary);
    if (!t_in) {
      std::cerr << "[warn] no trades_bin found at " << logger.trades_bin_path() << "\n";
    } else {
      std::error_code ec;
      auto p = std::filesystem::path(a.trades_out);
      std::filesystem::create_directories(p.parent_path(), ec);
      std::ofstream t_out(a.trades_out, std::ios::binary | std::ios::trunc);
      t_out << t_in.rdbuf();
      std::cerr << "[replay] trades -> " << a.trades_out << "\n";
    }
  }

  std::cerr << "[replay] done. last_seq=" << last_seq << " last_ts=" << last_ts << "\n";
  return 0;
}
