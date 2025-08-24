cmake_minimum_required(VERSION 3.21)
project(lob_root)
enable_testing()

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
