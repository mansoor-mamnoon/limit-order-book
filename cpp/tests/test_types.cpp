#include <catch2/catch_test_macros.hpp>
#include "lob/types.hpp"

using namespace lob;

TEST_CASE("Basic type properties") {
  STATIC_REQUIRE(std::is_signed_v<Tick>);
  STATIC_REQUIRE(std::is_signed_v<Quantity>);
  STATIC_REQUIRE(sizeof(Side) == 1);
}
