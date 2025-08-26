cmake_minimum_required(VERSION 3.21)
project(lob_root)
enable_testing()

# Global cache options (control from top-level)
set(LOB_BUILD_TESTS ON  CACHE BOOL "Build C++ tests (requires Catch2)" FORCE)
set(LOB_PROFILING  ON  CACHE BOOL "Enable frame pointers for profiling" FORCE)
set(LOB_LTO        OFF CACHE BOOL "Enable LTO in Release" FORCE)
set(LOB_USE_MIMALLOC OFF CACHE BOOL "Link mimalloc (tools + libs)" FORCE)
set(LOB_ENABLE_ASAN ON CACHE BOOL "Enable AddressSanitizer in Debug" FORCE)

# ---- Catch2 for tests (made available to cpp/ if tests are enabled) ----
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
