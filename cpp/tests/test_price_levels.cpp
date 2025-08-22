#include <catch2/catch_test_macros.hpp>
#include "lob/price_levels.hpp"

using namespace lob;

TEST_CASE("contiguous levels: index within band and empty by default") {
  PriceBand band{1000, 1010, 1};
  PriceLevelsContig levels(band);

  auto &lvl = levels.get_level(1005);
  REQUIRE(lvl.head == nullptr);
  REQUIRE(lvl.tail == nullptr);
  REQUIRE(lvl.total_qty == 0);

  REQUIRE_FALSE(levels.has_level(1005));
}

TEST_CASE("sparse levels: creates on access and empty by default") {
  PriceLevelsSparse levels;
  auto &lvl = levels.get_level(4242);
  REQUIRE(lvl.head == nullptr);
  REQUIRE(lvl.tail == nullptr);
  REQUIRE(lvl.total_qty == 0);
  REQUIRE_FALSE(levels.has_level(4242));
}
