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

# SST tests
add_executable(sst_test
    sst/block_test.cc
    sst/block_iterator_test.cc
    sst/sst_test.cc
)

target_link_libraries(sst_test
    mini_lsm
    gtest_main
)

target_include_directories(sst_test PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(sst_test PRIVATE ${CMAKE_SOURCE_DIR}/tests/include)
add_test(NAME sst_test COMMAND sst_test)

# IO tests
add_executable(io_test
    io/file_reader_test.cc
)

target_link_libraries(io_test
    mini_lsm
    gtest_main
)

target_include_directories(io_test PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(io_test PRIVATE ${CMAKE_SOURCE_DIR}/tests/include)
add_test(NAME io_test COMMAND io_test)

# encode test
add_executable(encode_test
    encode/test_encode.cc
)

target_link_libraries(encode_test
    mini_lsm
    gtest_main
)

target_include_directories(encode_test PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(encode_test PRIVATE ${CMAKE_SOURCE_DIR}/tests/include)
add_test(NAME encode_test COMMAND io_test)

# manifest test
add_executable(manifest_test
    manifest/manifest_test.cc
)

target_link_libraries(manifest_test
    mini_lsm
    gtest_main
)

target_include_directories(manifest_test PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(manifest_test PRIVATE ${CMAKE_SOURCE_DIR}/tests/include)
add_test(NAME manifest_test COMMAND manifest_test)

# wal test
add_executable(wal_test
    wal/wal_test.cc
)

target_link_libraries(wal_test
    mini_lsm
    gtest_main
)

target_include_directories(wal_test PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(wal_test PRIVATE ${CMAKE_SOURCE_DIR}/tests/include)
add_test(NAME wal_test COMMAND wal_test)


