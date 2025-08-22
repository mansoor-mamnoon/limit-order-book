#pragma once
#include <cstdint>
#include <type_traits>

namespace lob {

// fixed-precision: prices in integer ticks (no doubles anywhere)
using Tick      = int64_t;   // price in ticks
using Quantity  = int64_t;   // size in lots/contracts
using OrderId   = uint64_t;  // unique per session
using UserId    = uint64_t;  // owner (for STP)
using Timestamp = int64_t;   // nanoseconds since epoch
using SeqNo     = uint64_t;  // deterministic event ordering

enum class Side : uint8_t { Bid=0, Ask=1 };

enum OrderFlags : uint32_t {
  NONE       = 0,
  IOC        = 1u << 0,
  FOK        = 1u << 1,
  POST_ONLY  = 1u << 2,
  STP        = 1u << 3
};

struct PriceBand {
  Tick min_tick;   // inclusive
  Tick max_tick;   // inclusive
  Tick tick_size;  // granularity in ticks (often 1)
};

// sanity checks that catch mistakes at compile time
static_assert(std::is_signed_v<Tick>);
static_assert(std::is_signed_v<Quantity>);
static_assert(sizeof(Side) == 1);

} // namespace lob
