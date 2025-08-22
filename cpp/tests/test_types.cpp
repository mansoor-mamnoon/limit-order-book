#include <catch2/catch_test_macros.hpp>
#include <type_traits>
#include "lob/types.hpp"

using namespace lob;

TEST_CASE("type sizes and signedness") {
  REQUIRE(sizeof(Tick)     == 8);
  REQUIRE(sizeof(Quantity) == 8);
  REQUIRE(sizeof(OrderId)  == 8);
  REQUIRE(sizeof(UserId)   == 8);
  REQUIRE(sizeof(Timestamp)== 8);
  REQUIRE(sizeof(SeqNo)    == 8);
  STATIC_REQUIRE(std::is_signed_v<Tick>);
  STATIC_REQUIRE(std::is_signed_v<Quantity>);
}

TEST_CASE("side encoding is compact and correct") {
  REQUIRE(sizeof(Side) == 1);
  REQUIRE(static_cast<int>(Side::Bid) == 0);
  REQUIRE(static_cast<int>(Side::Ask) == 1);
}

TEST_CASE("order flags bitmask distinct bits") {
  const uint32_t ioc = IOC, fok = FOK, post = POST_ONLY, stp = STP;
  REQUIRE((ioc & fok) == 0);
  REQUIRE((ioc & post) == 0);
  REQUIRE((post & stp) == 0);
  REQUIRE((ioc | fok | post | stp) != 0);
}
