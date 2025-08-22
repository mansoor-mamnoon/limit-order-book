cmake_minimum_required(VERSION 3.21)
project(lob LANGUAGES CXX)

# ---------------- Options ----------------
option(LOB_ENABLE_ASAN "Enable AddressSanitizer in Debug" ON)

# ---------------- Global defaults ----------------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Default to Release if not provided
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# ---------------- Core (hot path) ----------------
add_library(lob_core STATIC
  src/book_core.cpp
  src/price_levels.cpp
)
target_include_directories(lob_core PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Aggressive hot-path flags (Clang/GCC)
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(lob_core PRIVATE
    -O3 -march=native
    -fno-exceptions
    -fno-rtti
    -fvisibility=hidden
  )
endif()

# ---------------- Util (non-hot) ----------------
add_library(lob_util STATIC
  src/util.cpp
)
target_include_directories(lob_util PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(lob_util PRIVATE -O2)
endif()

# ---------------- Tests ----------------
# Catch2 is provided by top-level via FetchContent and exposes Catch2::Catch2WithMain
add_executable(lob_tests
  tests/test_types.cpp
  tests/test_price_levels.cpp
  tests/test_headers_compile.cpp
  tests/test_fifo_same_price.cpp
  tests/test_partial_empty_onesided.cpp
)


# Tests need public headers
target_include_directories(lob_tests PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(lob_tests PRIVATE
  lob_core
  lob_util
  Catch2::Catch2WithMain
)

add_test(NAME lob_tests COMMAND lob_tests)

# ---------------- Sanitizers in Debug only ----------------
if(LOB_ENABLE_ASAN AND CMAKE_BUILD_TYPE STREQUAL "Debug")
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(lob_core  PRIVATE -fsanitize=address -fno-omit-frame-pointer)
    target_link_options   (lob_core  PRIVATE -fsanitize=address)
    target_compile_options(lob_tests PRIVATE -fsanitize=address -fno-omit-frame-pointer)
    target_link_options   (lob_tests PRIVATE -fsanitize=address)
  endif()
endif()
