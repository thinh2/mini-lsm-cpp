#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <thread>
#include <vector>

#include "storage.hpp"
#include "test_utilities.hpp"

using test_utils::BytesToString;
using test_utils::MakeBytesVector;

// ============================================================================
// BASIC SINGLE-THREADED OPERATIONS
// ============================================================================

class StorageBasicTest : public ::testing::Test {
protected:
  StorageOption opt{1024}; // 1KB memtable size
  Storage storage{opt};
};

TEST_F(StorageBasicTest, SimplePutAndGet) {
  auto key = MakeBytesVector("hello");
  auto value = MakeBytesVector("world");

  storage.put(key, value);
  auto result = storage.get(key);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(BytesToString(result.value()), "world");
}

TEST_F(StorageBasicTest, OverwriteAndRemove) {
  auto key = MakeBytesVector("key");
  auto value1 = MakeBytesVector("value1");
  auto value2 = MakeBytesVector("value2");

  // Put initial value
  storage.put(key, value1);
  EXPECT_EQ(BytesToString(storage.get(key).value()), "value1");

  // Overwrite with new value
  storage.put(key, value2);
  EXPECT_EQ(BytesToString(storage.get(key).value()), "value2");

  // Remove (TOMBSTONE)
  storage.remove(key);
  EXPECT_FALSE(storage.get(key).has_value());
}

TEST_F(StorageBasicTest, GetNonExistentKey) {
  auto key = MakeBytesVector("nonexistent");
  auto result = storage.get(key);

  EXPECT_FALSE(result.has_value());
}

// ============================================================================
// MEMTABLE FLUSH LOGIC
// ============================================================================

class StorageFlushTest : public ::testing::Test {
protected:
  // Use small memtable size (500 bytes) to trigger flushes easily
  StorageOption opt{500};
  Storage storage{opt};
};

TEST_F(StorageFlushTest, FlushTriggeredWhenThresholdExceeded) {
  // Put small entries that don't trigger flush
  auto key1 = MakeBytesVector("key1");
  auto value1 = MakeBytesVector("val1");
  storage.put(key1, value1);

  auto key2 = MakeBytesVector("key2");
  auto value2 = MakeBytesVector("val2");
  storage.put(key2, value2);

  // Put large entry that triggers flush
  auto key3 = MakeBytesVector("key3");
  auto value3 =
      MakeBytesVector(std::string(600, 'x')); // Large value exceeds threshold
  storage.put(key3, value3);

  // Verify all data is still accessible (from active and immutable memtables)
  EXPECT_EQ(BytesToString(storage.get(key1).value()), "val1");
  EXPECT_EQ(BytesToString(storage.get(key2).value()), "val2");
  EXPECT_EQ(storage.get(key3).value().size(), 600);
}

TEST_F(StorageFlushTest, MultipleFlushesCumulativeData) {
  // Trigger multiple flushes
  for (int i = 0; i < 5; ++i) {
    auto key = MakeBytesVector("key" + std::to_string(i));
    auto value = MakeBytesVector(
        std::string(400, 'a' + i)); // ~400 bytes each, triggers flush
    storage.put(key, value);
  }

  // Verify all data from all memtables (active and immutables) is accessible
  for (int i = 0; i < 5; ++i) {
    auto key = MakeBytesVector("key" + std::to_string(i));
    auto result = storage.get(key);

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value().size(), 400);
    // Verify correct character
    EXPECT_EQ(static_cast<char>(result.value()[0]), 'a' + i);
  }
}

// ============================================================================
// TOMBSTONE SEMANTICS
// ============================================================================

class StorageTombstoneTest : public ::testing::Test {
protected:
  StorageOption opt{1024};
  Storage storage{opt};
};

TEST_F(StorageTombstoneTest, RemoveThenReAdd) {
  auto key = MakeBytesVector("key");
  auto value1 = MakeBytesVector("value1");
  auto value2 = MakeBytesVector("value2_restored");

  // Put initial value
  storage.put(key, value1);
  EXPECT_EQ(BytesToString(storage.get(key).value()), "value1");

  // Remove (TOMBSTONE)
  storage.remove(key);
  EXPECT_FALSE(storage.get(key).has_value());

  // Re-add with new value
  storage.put(key, value2);
  auto result = storage.get(key);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(BytesToString(result.value()), "value2_restored");
}

TEST_F(StorageTombstoneTest, TombstoneInActiveHidesImmutable) {
  // Use small memtable to trigger flush
  StorageOption small_opt{300};
  Storage small_storage{small_opt};

  auto key = MakeBytesVector("key");
  auto value1 = MakeBytesVector("original");
  auto value2 = MakeBytesVector(std::string(400, 'x')); // Triggers flush

  // Put initial value (will be in immutable after flush)
  small_storage.put(key, value1);

  // Put large value to trigger flush (moves key/value1 to immutable)
  small_storage.put(key, value2);

  // Verify we get the new value from active
  EXPECT_EQ(small_storage.get(key).value().size(), 400);

  // Remove (TOMBSTONE in active hides value in immutable)
  small_storage.remove(key);
  EXPECT_FALSE(small_storage.get(key).has_value());

  // Re-add should work
  auto restored_value = MakeBytesVector("restored");
  small_storage.put(key, restored_value);
  EXPECT_EQ(BytesToString(small_storage.get(key).value()), "restored");
}

// ============================================================================
// CONCURRENT READER TESTS
// ============================================================================

class StorageConcurrentReaderTest : public ::testing::Test {
protected:
  StorageOption opt{1024};
  Storage storage{opt};

  void SetUp() override {
    // Pre-populate storage with test data
    for (int i = 0; i < 20; ++i) {
      auto key = MakeBytesVector("key" + std::to_string(i));
      auto value = MakeBytesVector("value" + std::to_string(i));
      storage.put(key, value);
    }
  }
};

TEST_F(StorageConcurrentReaderTest, FourThreadsConcurrentReads) {
  const int num_threads = 4;
  const int reads_per_thread = 50;
  std::vector<std::thread> threads;

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([this, reads_per_thread]() {
      for (int i = 0; i < reads_per_thread; ++i) {
        auto key = MakeBytesVector("key" + std::to_string(i % 20));
        auto result = storage.get(key);

        ASSERT_TRUE(result.has_value());
        std::string expected = "value" + std::to_string(i % 20);
        EXPECT_EQ(BytesToString(result.value()), expected);
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }
}

// ============================================================================
// THREAD-SAFETY: SEQUENTIAL WRITES THEN CONCURRENT READS
// ============================================================================

class StorageSequentialWriteTest : public ::testing::Test {
protected:
  StorageOption opt{1024};
  Storage storage{opt};
};

TEST_F(StorageSequentialWriteTest, SequentialWritesThenConcurrentReads) {
  // Phase 1: Sequential writes
  for (int i = 0; i < 30; ++i) {
    auto key = MakeBytesVector("key" + std::to_string(i));
    auto value = MakeBytesVector("value" + std::to_string(i));
    storage.put(key, value);
  }

  // Phase 2: 4 concurrent readers
  const int num_threads = 4;
  const int reads_per_thread = 75;
  std::vector<std::thread> threads;

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([this, reads_per_thread]() {
      for (int i = 0; i < reads_per_thread; ++i) {
        auto key = MakeBytesVector("key" + std::to_string(i % 30));
        auto result = storage.get(key);

        ASSERT_TRUE(result.has_value());
        std::string expected = "value" + std::to_string(i % 30);
        EXPECT_EQ(BytesToString(result.value()), expected);
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }
}

// ============================================================================
// THREAD-SAFETY: READ-HEAVY MIXED OPERATIONS
// ============================================================================

class StorageReadHeavyTest : public ::testing::Test {
protected:
  StorageOption opt{1024};
  Storage storage{opt};

  void SetUp() override {
    // Initial data population
    for (int i = 0; i < 25; ++i) {
      auto key = MakeBytesVector("key" + std::to_string(i));
      auto value = MakeBytesVector("value" + std::to_string(i));
      storage.put(key, value);
    }
  }
};

TEST_F(StorageReadHeavyTest, ReadHeavyMixedOperations) {
  const int num_threads = 4;
  const int operations_per_thread = 200;
  std::vector<std::thread> threads;

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([this, operations_per_thread, t]() {
      for (int op = 0; op < operations_per_thread; ++op) {
        // 80% reads, 20% writes
        if (op % 5 == 0) {
          // Write operation - update or add new keys
          int key_idx = 25 + t * 5 + (op / 5);
          auto key = MakeBytesVector("key" + std::to_string(key_idx));
          auto value = MakeBytesVector("updated_" + std::to_string(key_idx));
          storage.put(key, value);
        } else {
          // Read operation
          int key_idx = op % 25;
          auto key = MakeBytesVector("key" + std::to_string(key_idx));
          auto result = storage.get(key);

          // Key should exist (either original or updated)
          ASSERT_TRUE(result.has_value());
        }
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }
}

class StorageFlushRunTest : public ::testing::Test {
protected:
  void SetUp() override {
    sst_directory_ = std::filesystem::current_path() / "storage_flush_test";
    std::filesystem::remove_all(sst_directory_);
    std::filesystem::create_directories(sst_directory_);

    StorageOption opt{};
    opt.mem_table_size_ = 4096;
    opt.max_number_of_memtable_ = 3;
    opt.max_sst_block_size_ = 1024;
    opt.sst_directory_ = sst_directory_;

    storage_ = std::make_unique<Storage>(opt);
  }

  void TearDown() override {
    storage_.reset();
    std::filesystem::remove_all(sst_directory_);
  }

  std::filesystem::path sst_directory_;
  std::unique_ptr<Storage> storage_;
};

TEST_F(StorageFlushRunTest, FlushRunPersistsAllEntries) {
  constexpr int total_entries = 5000;

  for (int i = 0; i < total_entries; ++i) {
    auto key = MakeBytesVector("key" + std::to_string(i));
    auto value = MakeBytesVector("value" + std::to_string(i));
    storage_->put(key, value);
  }

  storage_->flush_run();

  for (int i = 0; i < total_entries; ++i) {
    auto key = MakeBytesVector("key" + std::to_string(i));
    auto expected_value = "value" + std::to_string(i);
    auto result = storage_->get(key);

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(BytesToString(result.value()), expected_value);
  }

  // Non-existed key
  {
    auto key = MakeBytesVector("key-hello");
    auto result = storage_->get(key);
    ASSERT_FALSE(result.has_value());
  }
}
