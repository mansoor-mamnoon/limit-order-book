#include <catch2/catch_test_macros.hpp>
#include "lob/types.hpp"
#include "lob/price_levels.hpp"
#include "lob/book_core.hpp"

TEST_CASE("public headers compile and basic types are usable") {
  lob::Tick p = 12345;
  lob::Quantity q = 100;
  REQUIRE(p > 0);
  REQUIRE(q > 0);
}
