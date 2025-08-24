#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include "lob/book_core.hpp"
#include "lob/price_levels.hpp"
#include "lob/logging.hpp"
#include <limits>

using namespace lob;

static PriceBand kBand{90,110,1};

TEST_CASE("Snapshot v2 roundtrip rebuilds FIFO and best-of-book") {
  std::filesystem::create_directories("test_out");

  PriceLevelsContig bids{kBand}, asks{kBand};
  SnapshotWriter snap{"test_out"};
  JsonlBinLogger logger{"test_out/runSnap", /*snapshot_every*/ 2, &snap};
  BookCore book{bids, asks, &logger};

  // Create some state: two bids at 100, one ask at 103
  book.submit_limit(NewOrder{1,1000,10001,1,Side::Bid,100,3,NONE});
  book.submit_limit(NewOrder{2,1001,10002,2,Side::Bid,100,4,NONE});
  // triggers snapshot at event_count=2
  book.submit_limit(NewOrder{3,1002,10003,3,Side::Ask,103,5,NONE});

  logger.flush();

  // Load snapshot written at seq=2
  SeqNo s{}; Timestamp ts{};
  PriceLevelsContig bids2{kBand}, asks2{kBand};
  REQUIRE(load_snapshot_file(snap.path_for(2), bids2, asks2, s, ts) == true);

  // Check bests & FIFO restored
  REQUIRE(bids2.best_bid() == 100);
  REQUIRE(asks2.best_ask() == std::numeric_limits<Tick>::max());
  auto& Lb100 = bids2.get_level(100);
  REQUIRE(Lb100.total_qty == 7);
  REQUIRE(Lb100.head != nullptr);
  REQUIRE(Lb100.head->id == 10001);
  REQUIRE(Lb100.tail != nullptr);
  REQUIRE(Lb100.tail->id == 10002);

  // Create a new BookCore from restored ladders and verify a market ask consumes 7
  BookCore book2{bids2, asks2, nullptr};
  book2.rebuild_index_from_books();

  ExecResult r = book2.submit_market(NewOrder{4,2000,20000,9,Side::Ask,0,7,NONE});
  REQUIRE(r.filled == 7);
  REQUIRE(bids2.best_bid() == std::numeric_limits<Tick>::min());
}
