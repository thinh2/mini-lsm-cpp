# Tests configuration
add_executable(memtable_test
    memtable/memtable_test.cc
)

target_link_libraries(memtable_test
    mini_lsm
    gtest_main
)

target_include_directories(memtable_test PRIVATE ${CMAKE_SOURCE_DIR}/include)

add_test(NAME memtable_test COMMAND memtable_test)
