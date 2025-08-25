cmake_minimum_required(VERSION 3.21)
project(lob_root)
enable_testing()

# Turn these ON from the top so ctest actually has tests and profiling is available.
set(LOB_BUILD_TESTS ON  CACHE BOOL "Build C++ tests (requires Catch2)" FORCE)
set(LOB_PROFILING  ON  CACHE BOOL "Enable frame pointers for profiling" FORCE)
# Optional: keep OFF by default, flip to ON locally if you want LTO
set(LOB_LTO        OFF CACHE BOOL "Enable LTO in Release" FORCE)

# ---- Catch2 for tests (available to subdirs) ----
include(FetchContent)
FetchContent_Declare(
  catch2
  GIT_REPOSITORY https://github.com/catchorg/Catch2.git
  GIT_TAG v3.6.0
)
FetchContent_MakeAvailable(catch2)

# Build all native code inside cpp/
add_subdirectory(cpp)

# Note:
# Pure-Python packaging is driven by scikit-build-core via pyproject.toml.
# We do NOT install Python files from CMake. scikit-build-core picks up python/
# via [tool.scikit-build].wheel.packages in pyproject.toml.
