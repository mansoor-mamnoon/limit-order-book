#include <catch2/catch_test_macros.hpp>
#include <limits>
#include "lob/book_core.hpp"

using namespace lob;

TEST_CASE("Market order on empty book fills zero and does not rest") {
  PriceBand band{100, 110, 1};
  PriceLevelsContig bids(band), asks(band);
  bids.set_best_bid(std::numeric_limits<Tick>::min());
  asks.set_best_ask(std::numeric_limits<Tick>::max());
  BookCore book(bids, asks);

  NewOrder M{10, 2000, 301, 7001, Side::Bid, 0, 10, 0};
  ExecResult r = book.submit_market(M);
  REQUIRE(r.filled == 0);
  REQUIRE(r.remaining == 10);
  REQUIRE(book.empty(Side::Bid)); // nothing rested
}

TEST_CASE("One-sided book: partial fills up to available depth") {
  PriceBand band{100, 110, 1};
  PriceLevelsContig bids(band), asks(band);
  bids.set_best_bid(std::numeric_limits<Tick>::min());
  asks.set_best_ask(std::numeric_limits<Tick>::max());
  BookCore book(bids, asks);

  // Rest 4 lots on ask @106
  NewOrder A{11, 2100, 401, 6001, Side::Ask, 106, 4, 0};
  book.submit_limit(A);
  REQUIRE(asks.best_ask() == 106);

  // Market buy 10 should fill only 4 and leave no resting qty
  NewOrder M{12, 2101, 402, 6002, Side::Bid, 0, 10, 0};
  ExecResult r = book.submit_market(M);
  REQUIRE(r.filled == 4);
  REQUIRE(r.remaining == 6);
  // Ask side becomes empty at top
  LevelFIFO& L = asks.get_level(106);
  REQUIRE(L.head == nullptr);
}
