cmake_minimum_required(VERSION 3.10)
project(mini-lsm)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/52eb8108c5bdec04579160ae17225d66034bd723.zip #v1.17
)
FetchContent_MakeAvailable(googletest)

# Source files
set(SOURCES
    src/memtable.cc
)

set(HEADERS
    include/memtable.hpp
)

# Main library
add_library(mini_lsm ${SOURCES} ${HEADERS})
target_include_directories(mini_lsm PUBLIC include)

# Executable
#add_executable(mini_lsm_app src/main.cpp)
#target_link_libraries(mini_lsm_app mini_lsm)

# Tests (optional)
enable_testing()
add_subdirectory(tests)
