cmake_minimum_required(VERSION 3.21)
project(lob LANGUAGES CXX)

# ---------------- Options ----------------
option(LOB_ENABLE_ASAN   "Enable AddressSanitizer in Debug" ON)
option(LOB_BUILD_TESTS   "Build C++ tests (requires Catch2)" OFF)  # default OFF standalone
option(LOB_PROFILING     "Build with frame pointers for perf/Instruments" OFF)
option(LOB_LTO           "Enable Link Time Optimization in Release" OFF)

# ---------------- Global defaults ----------------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Default to Release if not provided
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# Global compile/link opts influenced by switches
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  if(LOB_PROFILING)
    add_compile_options(-fno-omit-frame-pointer -g)
  endif()
  if(CMAKE_BUILD_TYPE STREQUAL "Release" AND LOB_LTO)
    add_link_options(-flto)
    add_compile_options(-flto)
  endif()
  add_compile_definitions(NDEBUG)
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

# ---------------- Tools ----------------
add_executable(replay_tool tools/replay.cpp)
target_link_libraries(replay_tool PRIVATE lob_core lob_util)
target_include_directories(replay_tool PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_executable(bench_tool tools/bench.cpp)
target_link_libraries(bench_tool PRIVATE lob_core lob_util)
target_include_directories(bench_tool PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(bench_tool PRIVATE -O3 -march=native)
endif()

# ---------------- Tests (optional) ----------------
if(LOB_BUILD_TESTS)
  if (TARGET Catch2::Catch2WithMain)
    set(_LOB_HAVE_CATCH2 TRUE)
  else()
    find_package(Catch2 3 QUIET CONFIG)
    if (Catch2_FOUND)
      set(_LOB_HAVE_CATCH2 TRUE)
    else()
      message(STATUS "LOB_BUILD_TESTS=ON but Catch2 not found; skipping C++ tests.")
    endif()
  endif()

  if (_LOB_HAVE_CATCH2)
    enable_testing()
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
    target_include_directories(lob_tests PRIVATE
      ${CMAKE_CURRENT_SOURCE_DIR}/include
    )
    target_link_libraries(lob_tests PRIVATE
      lob_core
      lob_util
      Catch2::Catch2WithMain
    )
    add_test(NAME lob_tests COMMAND lob_tests)

    if(LOB_ENABLE_ASAN AND CMAKE_BUILD_TYPE STREQUAL "Debug")
      if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(lob_tests PRIVATE -fsanitize=address -fno-omit-frame-pointer)
        target_link_options   (lob_tests PRIVATE -fsanitize=address)
      endif()
    endif()
  endif()
endif()

# ---------------- Sanitizers in Debug only ----------------
if(LOB_ENABLE_ASAN AND CMAKE_BUILD_TYPE STREQUAL "Debug")
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(lob_core  PRIVATE -fsanitize=address -fno-omit-frame-pointer)
    target_link_options   (lob_core  PRIVATE -fsanitize=address)
  endif()
endif()

# ---------------- pybind11 module (for direct builds) ----------------
include(FetchContent)
if(NOT TARGET pybind11::module)
  FetchContent_Declare(
    pybind11
    GIT_REPOSITORY https://github.com/pybind/pybind11.git
    GIT_TAG v2.12.0
  )
  FetchContent_MakeAvailable(pybind11)
endif()

pybind11_add_module(lob_cpp MODULE
  ../python/olob/_bindings.cpp
)
target_link_libraries(lob_cpp PRIVATE lob_core lob_util)
target_include_directories(lob_cpp PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(lob_cpp PRIVATE -O3 -march=native -fvisibility=hidden)
endif()
set_target_properties(lob_cpp PROPERTIES OUTPUT_NAME "_lob")

# Install into the Python package 'olob' (library on Unix, runtime on Windows)
if(WIN32)
  install(TARGETS lob_cpp RUNTIME DESTINATION olob)
  install(TARGETS bench_tool RUNTIME DESTINATION Scripts)
  install(TARGETS replay_tool RUNTIME DESTINATION Scripts)
else()
  install(TARGETS lob_cpp LIBRARY DESTINATION olob)
  install(TARGETS bench_tool RUNTIME DESTINATION bin)
  install(TARGETS replay_tool RUNTIME DESTINATION bin)
endif()
