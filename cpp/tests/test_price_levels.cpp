#include <catch2/catch_test_macros.hpp>
#include "lob/price_levels.hpp"

using namespace lob;

TEST_CASE("PriceLevelsContig basic set/get") {
  PriceBand band{90,110,1};
  PriceLevelsContig contig{band};
  auto& L = contig.get_level(100);
  REQUIRE(L.total_qty == 0);
  contig.set_best_bid(100);
  contig.set_best_ask(105);
  REQUIRE(contig.best_bid() == 100);
  REQUIRE(contig.best_ask() == 105);
}

TEST_CASE("PriceLevelsSparse existence") {
  PriceLevelsSparse sparse;
  REQUIRE_FALSE(sparse.has_level(100));
  auto& L = sparse.get_level(100);
  REQUIRE(L.head == nullptr);
}
