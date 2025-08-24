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
  src/logging.cpp
)
target_include_directories(lob_util PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(lob_util PRIVATE -O2)
endif()

# ---------------- Replay tool ----------------
add_executable(replay_tool tools/replay.cpp)
target_link_libraries(replay_tool PRIVATE lob_core lob_util)
target_include_directories(replay_tool PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

# ---------------- Benchmark tool ----------------
add_executable(bench_tool tools/bench.cpp)
target_link_libraries(bench_tool PRIVATE lob_core lob_util)
target_include_directories(bench_tool PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(bench_tool PRIVATE -O3 -march=native)
endif()

# ---------------- Tests ----------------
# Catch2 is provided by top-level via FetchContent and exposes Catch2::Catch2WithMain
add_executable(lob_tests
  tests/test_types.cpp
  tests/test_price_levels.cpp
  tests/test_headers_compile.cpp
  tests/test_fifo_same_price.cpp
  tests/test_partial_empty_onesided.cpp
  tests/test_modify_price.cpp
  tests/test_stp.cpp
  tests/test_market_sweep.cpp
  tests/test_replay_golden.cpp
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

# ---------------- pybind11 bindings ----------------
include(FetchContent)
# Avoid double-declare if outer project already brought it in
if(NOT TARGET pybind11::module)
  FetchContent_Declare(
    pybind11
    GIT_REPOSITORY https://github.com/pybind/pybind11.git
    GIT_TAG v2.12.0
  )
  FetchContent_MakeAvailable(pybind11)
endif()

pybind11_add_module(lob_cpp MODULE
  ../python/lob/_bindings.cpp
)
target_link_libraries(lob_cpp PRIVATE lob_core lob_util)
target_include_directories(lob_cpp PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(lob_cpp PRIVATE -O3 -march=native -fvisibility=hidden)
endif()

# The Python module filename will be "_lob.*" and must live inside package "lob".
set_target_properties(lob_cpp PROPERTIES OUTPUT_NAME "_lob")

# scikit-build-core will place this into site-packages/lob thanks to the DESTINATION below.
# On *nix, shared library extension is LIBRARY; on Windows it's RUNTIME.
if(WIN32)
  install(TARGETS lob_cpp RUNTIME DESTINATION lob)
  install(TARGETS bench_tool RUNTIME DESTINATION Scripts)
  install(TARGETS replay_tool RUNTIME DESTINATION Scripts)
else()
  install(TARGETS lob_cpp LIBRARY DESTINATION lob)
  install(TARGETS bench_tool RUNTIME DESTINATION bin)
  install(TARGETS replay_tool RUNTIME DESTINATION bin)
endif()
