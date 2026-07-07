cmake_minimum_required(VERSION 3.16)
project(marpul_execution_engine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

find_package(Threads REQUIRED)

# Buildable demo: lock-free ring buffer + CPU pinning, header-only.
add_executable(ring_buffer_bench bench/ring_buffer_bench.cpp)
target_include_directories(ring_buffer_bench PRIVATE include)
target_link_libraries(ring_buffer_bench PRIVATE Threads::Threads)

# execution_engine.cpp is included for reference — it's one module of a
# larger private trading system and depends on sibling headers (types,
# constants, stock_state, stage_gates, paper_trading, kelly_sizer,
# risk_engine) that aren't part of this extract, plus libcurl and hiredis.
# It is not wired into this build. See README.md.
