#include <catch2/catch_test_macros.hpp>
#include "lob/book_core.hpp"
#include "lob/price_levels.hpp"
#include "lob/logging.hpp"
#include <limits>

using namespace lob;

static PriceBand kBand{90,110,1};

TEST_CASE("Modify to better price that crosses -> trades first") {
  PriceLevelsContig bids{kBand}, asks{kBand};
  SnapshotWriter snap{"test_out"};
  JsonlBinLogger logger{"test_out/run_mod1", 10, &snap};
  BookCore book{bids, asks, &logger};

  // Resting ask @105(5)
  book.submit_limit(NewOrder{1,1000,3001,30,Side::Ask,105,5,NONE});
  // Resting bid @100(5), then modify to 106
  book.submit_limit(NewOrder{2,1001,4001,40,Side::Bid,100,5,NONE});

  ExecResult r = book.modify(ModifyOrder{3,1002,4001,106,5,NONE});
  REQUIRE(r.filled == 5);
  REQUIRE(asks.best_ask() == std::numeric_limits<Tick>::max());
}

TEST_CASE("Modify to worse price -> requeues at tail of new price") {
  PriceLevelsContig bids{kBand}, asks{kBand};
  SnapshotWriter snap{"test_out"};
  JsonlBinLogger logger{"test_out/run_mod2", 10, &snap};
  BookCore book{bids, asks, &logger};

  // Two bids at 100: b1 then b2
  book.submit_limit(NewOrder{1,1000,5001,50,Side::Bid,100,3,NONE});
  book.submit_limit(NewOrder{2,1001,5002,51,Side::Bid,100,3,NONE});

  // Modify b1 to worse 99; it should move to 99 (new queue)
  ExecResult r = book.modify(ModifyOrder{3,1002,5001,99,3,NONE});
  REQUIRE(r.filled == 0);

  auto& L100 = bids.get_level(100);
  REQUIRE(L100.head != nullptr);
  REQUIRE(L100.head->id == 5002);

  auto& L99 = bids.get_level(99);
  REQUIRE(L99.head != nullptr);
  REQUIRE(L99.head->id == 5001);
  REQUIRE(L99.total_qty == 3);
}
