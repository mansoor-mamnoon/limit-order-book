cmake_minimum_required(VERSION 3.21)
project(lob LANGUAGES CXX)

option(LOB_ENABLE_ASAN "Enable AddressSanitizer in Debug" ON)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

add_library(lob_core STATIC
  src/book_core.cpp
  src/price_levels.cpp
)
target_include_directories(lob_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(lob_core PRIVATE
    -O3 -march=native
    -fno-exceptions
    -fno-rtti
    -fvisibility=hidden
  )
endif()

add_library(lob_util STATIC src/util.cpp)
target_include_directories(lob_util PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(lob_util PRIVATE -O2)
endif()

enable_testing()
add_executable(lob_tests tests/test_smoke.cpp)
target_link_libraries(lob_tests PRIVATE lob_core lob_util)
add_test(NAME lob_tests COMMAND lob_tests)

if(LOB_ENABLE_ASAN AND CMAKE_BUILD_TYPE STREQUAL "Debug")
  target_compile_options(lob_core PRIVATE -fsanitize=address -fno-omit-frame-pointer)
  target_link_options(lob_core PRIVATE -fsanitize=address)
  target_compile_options(lob_tests PRIVATE -fsanitize=address -fno-omit-frame-pointer)
  target_link_options(lob_tests PRIVATE -fsanitize=address)
endif()
