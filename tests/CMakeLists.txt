# Tests configuration
add_executable(memtable_test
    memtable/memtable_test.cc
)

target_link_libraries(memtable_test
    mini_lsm
    gtest_main
)

target_include_directories(memtable_test PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(memtable_test PRIVATE ${CMAKE_SOURCE_DIR}/tests/include)

add_test(NAME memtable_test COMMAND memtable_test)

# Storage tests
add_executable(storage_test
    storage/storage_test.cc
)

target_link_libraries(storage_test
    mini_lsm
    gtest_main
)

target_include_directories(storage_test PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(storage_test PRIVATE ${CMAKE_SOURCE_DIR}/tests/include)
add_test(NAME storage_test COMMAND storage_test)
