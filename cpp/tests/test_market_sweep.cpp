#include <catch2/catch_test_macros.hpp>
#include "lob/book_core.hpp"
#include "lob/price_levels.hpp"
#include "lob/logging.hpp"

using namespace lob;

static PriceBand kBand{90,110,1};

TEST_CASE("Multi-level sweep across asks") {
  PriceLevelsContig bids{kBand}, asks{kBand};
  SnapshotWriter snap{"test_out"};
  JsonlBinLogger logger{"test_out/run_sweep", 10, &snap};
  BookCore book{bids, asks, &logger};

  // Asks: 101(2), 102(3), 103(4)
  book.submit_limit(NewOrder{1,1000,10101,10,Side::Ask,101,2,NONE});
  book.submit_limit(NewOrder{2,1001,10102,11,Side::Ask,102,3,NONE});
  book.submit_limit(NewOrder{3,1002,10103,12,Side::Ask,103,4,NONE});

  // Limit buy @103 for 7 should consume 2+3+2 = 7
  ExecResult r = book.submit_limit(NewOrder{4,1003,2001,20,Side::Bid,103,7,NONE});
  REQUIRE(r.filled == 7);

  // Remaining: ask 103 should have 2 left
  auto& L103 = asks.get_level(103);
  REQUIRE(L103.total_qty == 2);
  REQUIRE(asks.best_ask() == 103);
}
