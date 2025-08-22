#include <catch2/catch_test_macros.hpp>
#include "lob/book_core.hpp"

using namespace lob;

TEST_CASE("Modify to worse price requeues to tail at new price") {
  PriceBand band{100, 110, 1};
  PriceLevelsContig bids(band), asks(band);
  BookCore book(bids, asks);
  bids.set_best_bid(std::numeric_limits<Tick>::min());
  asks.set_best_ask(std::numeric_limits<Tick>::max());

  // Two resting bids at 105: A then B
  NewOrder A{1,1000,101,9001,Side::Bid,105,5,0};
  NewOrder B{2,1001,102,9002,Side::Bid,105,5,0};
  book.submit_limit(A);
  book.submit_limit(B);
  REQUIRE(bids.best_bid() == 105);

  // Move A to worse price 104 (should lose time priority at 104)
  NewOrder A2{3,1010,101,9001,Side::Bid,104,5,0};
  book.modify(A2);

  // At 105, B should now be head; at 104, A should be head
  LevelFIFO& L105 = bids.get_level(105);
  REQUIRE(L105.head != nullptr);
  REQUIRE(L105.head->id == 102);

  LevelFIFO& L104 = bids.get_level(104);
  REQUIRE(L104.head != nullptr);
  REQUIRE(L104.head->id == 101);
}

TEST_CASE("Modify to better price can cross and trade immediately") {
  PriceBand band{100, 110, 1};
  PriceLevelsContig bids(band), asks(band);
  BookCore book(bids, asks);
  bids.set_best_bid(std::numeric_limits<Tick>::min());
  asks.set_best_ask(std::numeric_limits<Tick>::max());

  // Rest ask @106 qty=3
  NewOrder S{10,2000,201,8001,Side::Ask,106,3,0};
  book.submit_limit(S);
  REQUIRE(asks.best_ask() == 106);

  // Rest bid B @105 qty=5
  NewOrder B{11,2001,301,7001,Side::Bid,105,5,0};
  book.submit_limit(B);

  // Modify B -> price 106 (better), expect immediate trade of 3
  NewOrder B2{12,2002,301,7001,Side::Bid,106,5,0};
  ExecResult r = book.modify(B2);
  REQUIRE(r.filled == 3);
  REQUIRE(r.remaining == 2); // 2 rested at 106
  REQUIRE(asks.best_ask() == std::numeric_limits<Tick>::max()); // ask top emptied
}
