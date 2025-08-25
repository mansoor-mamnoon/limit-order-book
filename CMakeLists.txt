cmake_minimum_required(VERSION 3.21)
project(lob_root)
enable_testing()

set(LOB_BUILD_TESTS ON  CACHE BOOL "Build C++ tests (requires Catch2)" FORCE)
set(LOB_PROFILING  ON  CACHE BOOL "Enable frame pointers for profiling" FORCE)
set(LOB_LTO        OFF CACHE BOOL "Enable LTO in Release" FORCE)

# ---- Catch2 for tests ----
include(FetchContent)
FetchContent_Declare(
  catch2
  GIT_REPOSITORY https://github.com/catchorg/Catch2.git
  GIT_TAG v3.6.0
)
FetchContent_MakeAvailable(catch2)

# Build all native code inside cpp/
add_subdirectory(cpp)

# Note: Python packaging is driven by scikit-build-core (pyproject.toml).
