#include <catch2/catch_test_macros.hpp>
#include "lob/book_core.hpp"

using namespace lob;

TEST_CASE("FIFO at same price is respected") {
  // Bid book with one price level; we'll hit it with a market sell
  PriceBand band{100, 110, 1};
  PriceLevelsContig bids(band), asks(band);
  bids.set_best_bid(std::numeric_limits<Tick>::min()); // empty
  asks.set_best_ask(std::numeric_limits<Tick>::max()); // empty

  BookCore book(bids, asks);

  // Rest three buy limits at px=105 in arrival order: A(5), B(7), C(3)
  NewOrder A{1, 1000, 101, 9001, Side::Bid, 105, 5, 0};
  NewOrder B{2, 1001, 102, 9002, Side::Bid, 105, 7, 0};
  NewOrder C{3, 1002, 103, 9003, Side::Bid, 105, 3, 0};
  book.submit_limit(A);
  book.submit_limit(B);
  book.submit_limit(C);

  // We should have best bid = 105
  REQUIRE(bids.best_bid() == 105);

  // Now a market sell of size 10 should consume A fully(5) then B partially(5 of 7)
  NewOrder S{4, 1003, 201, 8001, Side::Ask, 0, 10, 0};
  ExecResult r = book.submit_market(S);
  REQUIRE(r.filled == 10);
  REQUIRE(r.remaining == 0);

  // Inspect the level queue at 105: B should remain with qty=2, then C(3)
  LevelFIFO& L = bids.get_level(105);
  REQUIRE(L.head != nullptr);
  REQUIRE(L.head->id == 102);
  REQUIRE(L.head->qty == 2);
  REQUIRE(L.head->next != nullptr);
  REQUIRE(L.head->next->id == 103);
  REQUIRE(L.head->next->qty == 3);
}
