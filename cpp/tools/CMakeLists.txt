# cpp/tools/CMakeLists.txt

# bench_tool
add_executable(bench_tool bench.cpp)
target_link_libraries(bench_tool PRIVATE lob_core lob_util)
target_include_directories(bench_tool PRIVATE ${CMAKE_CURRENT_LIST_DIR}/../include)
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(bench_tool PRIVATE -O3 -march=native)
endif()

# replay_tool
add_executable(replay_tool replay.cpp)
target_link_libraries(replay_tool PRIVATE lob_core lob_util)
target_include_directories(replay_tool PRIVATE ${CMAKE_CURRENT_LIST_DIR}/../include)
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(replay_tool PRIVATE -O3 -march=native)
endif()

# Optional mimalloc for tools
if(LOB_USE_MIMALLOC)
  target_link_libraries(bench_tool  PRIVATE mimalloc-static)
  target_link_libraries(replay_tool PRIVATE mimalloc-static)
endif()

# Install tools
if(WIN32)
  install(TARGETS bench_tool  RUNTIME DESTINATION Scripts)
  install(TARGETS replay_tool RUNTIME DESTINATION Scripts)
else()
  install(TARGETS bench_tool  RUNTIME DESTINATION bin)
  install(TARGETS replay_tool RUNTIME DESTINATION bin)
endif()
