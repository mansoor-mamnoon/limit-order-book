#include "lob/book_core.hpp"
#include "lob/logging.hpp"
#include "lob/price_levels.hpp"

#include <fstream>
#include <iostream>
#include <string>
#include <filesystem>

using namespace lob;

static bool file_exists(const std::string& path, uintmax_t* sz = nullptr) {
  std::error_code ec;
  auto st = std::filesystem::status(path, ec);
  if (ec || !std::filesystem::exists(st)) return false;
  if (sz) {
    auto s = std::filesystem::file_size(path, ec);
    *sz = ec ? 0 : s;
  }
  return true;
}

int main(int argc, char** argv) {
  if (argc < 4) {
    std::cerr << "Usage: " << argv[0]
              << " SNAPSHOT_FILE EVENTS_BIN OUT_TRADES_BIN\n"
              << "Tip: If you ran tests with CTest, artifacts are under build/test_out/.\n";
    return 1;
  }
  std::string snapshot_file = argv[1];
  std::string events_file   = argv[2];
  std::string out_trades    = argv[3];

  // Quick existence checks
  uintmax_t sz_snap = 0, sz_evt = 0;
  if (!file_exists(snapshot_file, &sz_snap)) {
    std::cerr << "Snapshot not found: " << snapshot_file << "\n"
              << "Hint: Run tests first (ctest --test-dir build) or point to build/test_out/...\n";
    return 1;
  }
  if (sz_snap < sizeof(SnapshotHeader)) {
    std::cerr << "Snapshot file too small (" << sz_snap << " bytes): " << snapshot_file << "\n";
    return 1;
  }
  if (!file_exists(events_file, &sz_evt)) {
    std::cerr << "Events file not found: " << events_file << "\n";
    return 1;
  }

  // Load snapshot into fresh ladders
  PriceLevelsSparse bids, asks; // unbounded; fine for replay
  SeqNo seq{};
  Timestamp ts{};
  if (!load_snapshot_file(snapshot_file, bids, asks, seq, ts)) {
    // Show header to help debugging
    std::ifstream in(snapshot_file, std::ios::binary);
    if (in) {
      SnapshotHeader hdr{};
      in.read(reinterpret_cast<char*>(&hdr), sizeof(hdr));
      std::cerr << "Failed to load snapshot: " << snapshot_file << "\n"
                << "Header => magic=0x" << std::hex << hdr.magic
                << " version=" << std::dec << hdr.version
                << " seq=" << hdr.seq << " ts=" << hdr.ts
                << " levels=" << hdr.n_levels << " orders=" << hdr.n_orders << "\n"
                << "Expected magic=0x4C4F4253\n";
    } else {
      std::cerr << "Failed to load snapshot: " << snapshot_file << " (cannot open)\n";
    }
    return 1;
  }

  // Build a book with a trades-only logger (no snapshots during replay)
  SnapshotWriter dummy_snap{"."};
  JsonlBinLogger logger("replay_tmp", /*snapshot_every*/0, &dummy_snap);
  BookCore book{bids, asks, &logger};
  book.rebuild_index_from_books();

  // Replay events with seq > snapshot seq
  std::ifstream in(events_file, std::ios::binary);
  if (!in) {
    std::cerr << "Cannot open events.bin: " << events_file << "\n";
    return 1;
  }

  while (true) {
    EventBin e{};
    in.read(reinterpret_cast<char*>(&e), sizeof(e));
    if (!in) break;

    if (e.seq <= seq) continue; // skip events before/at snapshot

    if (e.type == EventType::NewLimit || e.type == EventType::NewMarket) {
      NewOrder o{e.seq, e.ts, e.id, e.user,
                 (e.side==0?Side::Bid:Side::Ask),
                 e.price, e.qty, (e.is_limit?0:IOC)};
      if (e.type == EventType::NewLimit)
        book.submit_limit(o);
      else
        book.submit_market(o);
    } else if (e.type == EventType::Cancel) {
      book.cancel(e.id);
    }
  }

  logger.flush();

  // Copy logger’s trades.bin into out_trades
  {
    std::ifstream replayed(logger.trades_bin_path(), std::ios::binary);
    if (!replayed) {
      std::cerr << "Internal error: missing replay trades at " << logger.trades_bin_path() << "\n";
      return 1;
    }
    std::ofstream trades_out(out_trades, std::ios::binary | std::ios::trunc);
    trades_out << replayed.rdbuf();
  }

  std::cout << "Replay complete.\n"
            << "Snapshot seq: " << seq << "\n"
            << "Events read from: " << events_file << " (size=" << sz_evt << " bytes)\n"
            << "Trades written to: " << out_trades << "\n";
  return 0;
}
