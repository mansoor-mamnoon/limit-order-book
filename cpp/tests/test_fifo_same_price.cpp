#include <catch2/catch_test_macros.hpp>
#include "lob/book_core.hpp"
#include "lob/price_levels.hpp"
#include "lob/logging.hpp"

using namespace lob;

static PriceBand kBand{90,110,1};

struct TestEnvA {
  PriceLevelsContig bids{kBand};
  PriceLevelsContig asks{kBand};
  SnapshotWriter snap{ "test_out" };
  JsonlBinLogger logger{ "test_out/run_fifo", /*snapshot_every*/ 10, &snap };
  BookCore book;
  TestEnvA() : book(bids, asks, &logger) {}
};

TEST_CASE("FIFO within a single price level") {
  TestEnvA T;

  // Two sells at same price, different users/ids
  NewOrder s1{1, 1000, 1001, 42, Side::Ask, 100, 5, NONE};
  NewOrder s2{2, 1001, 1002, 43, Side::Ask, 100, 7, NONE};
  T.book.submit_limit(s1);
  T.book.submit_limit(s2);

  // Market buy for 6 should fully fill s1(5) and then 1 from s2
  NewOrder mb{3, 1002, 2001, 99, Side::Bid, 0, 6, NONE};
  ExecResult r = T.book.submit_market(mb);

  REQUIRE(r.filled == 6);
  // Ask level 100 should still have 6 left (7-1) from s2
  auto& L = T.asks.get_level(100);
  REQUIRE(L.total_qty == 6);
  REQUIRE(L.head != nullptr);
  REQUIRE(L.head->id == 1002); // FIFO honored
}
