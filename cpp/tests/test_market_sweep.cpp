#include <catch2/catch_test_macros.hpp>
#include <limits>
#include "lob/book_core.hpp"

using namespace lob;

TEST_CASE("Market order sweeps multiple levels correctly") {
  PriceBand band{100, 110, 1};
  PriceLevelsContig bids(band), asks(band);
  BookCore book(bids, asks);
  bids.set_best_bid(std::numeric_limits<Tick>::min());
  asks.set_best_ask(std::numeric_limits<Tick>::max());

  // Asks at 101(3), 102(4), 103(2) => total 9
  book.submit_limit({1,1000,101,1,Side::Ask,101,3,0});
  book.submit_limit({2,1001,102,2,Side::Ask,102,4,0});
  book.submit_limit({3,1002,103,3,Side::Ask,103,2,0});
  REQUIRE(asks.best_ask() == 101);

  // Market buy 10 should fill 9 and stop with 1 remaining
  ExecResult r = book.submit_market({4,1003,201,9,Side::Bid,0,10,0});
  REQUIRE(r.filled == 9);
  REQUIRE(r.remaining == 1);

  // All three levels emptied at 101..103
  REQUIRE(asks.best_ask() == std::numeric_limits<Tick>::max());
}
