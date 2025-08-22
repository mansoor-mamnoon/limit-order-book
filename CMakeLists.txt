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

add_subdirectory(cpp)
