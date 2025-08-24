#include <catch2/catch_test_macros.hpp>
#include "lob/book_core.hpp"
#include "lob/price_levels.hpp"
#include "lob/logging.hpp"
#include <limits>

using namespace lob;

static PriceBand kBand{90,110,1};

TEST_CASE("Partial fill on one-sided books and empties correctly") {
  PriceLevelsContig bids{kBand}, asks{kBand};
  SnapshotWriter snap{"test_out"};
  JsonlBinLogger logger{"test_out/run_one", 10, &snap};
  BookCore book{bids, asks, &logger};

  // Only asks exist
  book.submit_limit(NewOrder{1,1000,1,1,Side::Ask,100,3,NONE});
  ExecResult r = book.submit_limit(NewOrder{2,1001,2,2,Side::Bid,100,5,NONE});
  REQUIRE(r.filled == 3);
  REQUIRE(asks.best_ask() == std::numeric_limits<Tick>::max());
  REQUIRE(bids.best_bid() == 100);
}
