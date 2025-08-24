#include <catch2/catch_test_macros.hpp>
#include "lob/book_core.hpp"
#include "lob/price_levels.hpp"
#include "lob/logging.hpp"
#include <limits>

using namespace lob;

static PriceBand kBand{90,110,1};

TEST_CASE("STP cancels resting same-owner orders instead of trading") {
  PriceLevelsContig bids{kBand}, asks{kBand};
  SnapshotWriter snap{"test_out"};
  JsonlBinLogger logger{"test_out/run_stp", 10, &snap};
  BookCore book{bids, asks, &logger};

  // User 77 posts ask 100(3)
  book.submit_limit(NewOrder{1,1000,7001,77,Side::Ask,100,3,NONE});
  // Same user 77 sends bid with STP at 100(3) that would cross -> resting ask should be canceled
  ExecResult r = book.submit_limit(NewOrder{2,1001,7002,77,Side::Bid,100,3,STP});
  REQUIRE(r.filled == 0);
  REQUIRE(asks.best_ask() == std::numeric_limits<Tick>::max());

  auto& Lb = bids.get_level(100);
  REQUIRE(Lb.total_qty == 3);
  REQUIRE(Lb.head != nullptr);
  REQUIRE(Lb.head->id == 7002);
}
