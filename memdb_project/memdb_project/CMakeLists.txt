cmake_minimum_required(VERSION 3.14)
project(memdb_project2)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

include_directories(include)

file(GLOB CORE_SRC "src/core/*.cpp")
file(GLOB API_SRC "src/api/*.cpp")
file(GLOB EXAMPLES_SRC "examples/*.cpp")
file(GLOB TESTS_SRC "tests/test_core/*.cpp")

add_library(memdb_static STATIC ${CORE_SRC} ${API_SRC})

target_include_directories(memdb_static PUBLIC include/memdb/dependencies)
target_include_directories(memdb_static PUBLIC include/memdb/core)

add_executable(example_basic_usage examples/example_basic_usage.cpp)

target_include_directories(example_basic_usage PRIVATE include/memdb/dependencies)
target_include_directories(example_basic_usage PRIVATE include/memdb/core)

target_link_libraries(example_basic_usage memdb_static)

add_subdirectory(external/googletest-1.15.2)

enable_testing()

add_executable(run_tests ${TESTS_SRC})

target_link_libraries(run_tests memdb_static GTest::gtest_main GTest::gmock_main pthread)

include(GoogleTest)
gtest_discover_tests(run_tests)