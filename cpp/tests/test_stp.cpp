#include <catch2/catch_test_macros.hpp>
#include <limits>
#include "lob/book_core.hpp"

using namespace lob;

TEST_CASE("STP cancels resting same-owner orders instead of trading") {
  PriceBand band{100, 110, 1};
  PriceLevelsContig bids(band), asks(band);
  BookCore book(bids, asks);
  bids.set_best_bid(std::numeric_limits<Tick>::min());
  asks.set_best_ask(std::numeric_limits<Tick>::max());

  // Rest an ask from user 9001
  NewOrder A{1,1000,201,9001,Side::Ask,105,5,0};
  book.submit_limit(A);
  REQUIRE(asks.best_ask() == 105);

  // Incoming buy from the SAME user with STP
  NewOrder M{2,1001,301,9001,Side::Bid,0,10,STP};
  ExecResult r = book.submit_market(M);

  // No trade executed; resting same-owner ask got cancelled
  REQUIRE(r.filled == 0);
  REQUIRE(r.remaining == 10);
  REQUIRE(asks.best_ask() == std::numeric_limits<Tick>::max());
}
