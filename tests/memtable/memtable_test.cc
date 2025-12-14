#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "memtable.hpp"
#include "test_utilities.hpp"

using test_utils::BytesToString;
using test_utils::MakeBytesVector;
using test_utils::MakeBytesVectorRepeated;
using test_utils::MakeBytesVectorWithNulls;

// ============================================================================
// BASIC SINGLE-THREADED TESTS
// ============================================================================

class MemTableBasicTest : public ::testing::Test {
protected:
  MemTable memtable{1024};
};

TEST_F(MemTableBasicTest, PutAndGetSimpleValue) {
  auto key = MakeBytesVector("hello");
  auto value = MakeBytesVector("world");

  memtable.put(key, value);
  auto result = memtable.get(key);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(BytesToString(result.value()), "world");
}

TEST_F(MemTableBasicTest, GetNonExistentKey) {
  auto key = MakeBytesVector("nonexistent");
  auto result = memtable.get(key);

  EXPECT_FALSE(result.has_value());
}

TEST_F(MemTableBasicTest, OverwriteExistingValue) {
  auto key = MakeBytesVector("key");
  auto value1 = MakeBytesVector("value1");
  auto value2 = MakeBytesVector("value2");

  memtable.put(key, value1);
  EXPECT_EQ(BytesToString(memtable.get(key).value()), "value1");

  memtable.put(key, value2);
  EXPECT_EQ(BytesToString(memtable.get(key).value()), "value2");
}

TEST_F(MemTableBasicTest, MultipleKeyValuePairs) {
  auto key1 = MakeBytesVector("key1");
  auto value1 = MakeBytesVector("value1");
  auto key2 = MakeBytesVector("key2");
  auto value2 = MakeBytesVector("value2");
  auto key3 = MakeBytesVector("key3");
  auto value3 = MakeBytesVector("value3");

  memtable.put(key1, value1);
  memtable.put(key2, value2);
  memtable.put(key3, value3);

  EXPECT_EQ(BytesToString(memtable.get(key1).value()), "value1");
  EXPECT_EQ(BytesToString(memtable.get(key2).value()), "value2");
  EXPECT_EQ(BytesToString(memtable.get(key3).value()), "value3");
}

TEST_F(MemTableBasicTest, EmptyKeyValue) {
  auto empty_key = MakeBytesVector("");
  auto empty_value = MakeBytesVector("");

  memtable.put(empty_key, empty_value);
  auto result = memtable.get(empty_key);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().size(), 0);
}

TEST_F(MemTableBasicTest, EmptyValueWithNonEmptyKey) {
  auto key = MakeBytesVector("key");
  auto empty_value = MakeBytesVector("");

  memtable.put(key, empty_value);
  auto result = memtable.get(key);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().size(), 0);
}

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

class MemTableEdgeCaseTest : public ::testing::Test {
protected:
  MemTable memtable{1024};
};

TEST_F(MemTableEdgeCaseTest, LargeKeyAndValue) {
  auto large_key = MakeBytesVectorRepeated('k', 10000);
  auto large_value = MakeBytesVectorRepeated('v', 50000);

  memtable.put(large_key, large_value);
  auto result = memtable.get(large_key);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().size(), 50000);
}

TEST_F(MemTableEdgeCaseTest, KeyWithNullBytes) {
  auto key = MakeBytesVectorWithNulls("k1Nk2"); // k1 null k2
  auto value = MakeBytesVector("value");

  memtable.put(key, value);
  auto result = memtable.get(key);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(BytesToString(result.value()), "value");
}

TEST_F(MemTableEdgeCaseTest, ValueWithNullBytes) {
  auto key = MakeBytesVector("key");
  auto value = MakeBytesVectorWithNulls("v1Nv2Nv3"); // v1 null v2 null v3

  memtable.put(key, value);
  auto result = memtable.get(key);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().size(), 8);
}

TEST_F(MemTableEdgeCaseTest, UpdatingLargeValueWithSmall) {
  auto key = MakeBytesVector("key");
  auto large_value = MakeBytesVectorRepeated('L', 10000);
  auto small_value = MakeBytesVector("s");

  memtable.put(key, large_value);
  memtable.put(key, small_value);

  auto result = memtable.get(key);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(BytesToString(result.value()), "s");
}

TEST_F(MemTableEdgeCaseTest, UpdatingSmallValueWithLarge) {
  auto key = MakeBytesVector("key");
  auto small_value = MakeBytesVector("s");
  auto large_value = MakeBytesVectorRepeated('L', 10000);

  memtable.put(key, small_value);
  memtable.put(key, large_value);

  auto result = memtable.get(key);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().size(), 10000);
}

// ============================================================================
// CONCURRENT READER TESTS
// ============================================================================

class MemTableConcurrentReaderTest : public ::testing::Test {
protected:
  MemTable memtable{1024};

  void SetUp() override {
    // Pre-populate with test data
    for (int i = 0; i < 100; ++i) {
      auto key = MakeBytesVector("key" + std::to_string(i));
      auto value = MakeBytesVector("value" + std::to_string(i));
      memtable.put(key, value);
    }
  }
};

TEST_F(MemTableConcurrentReaderTest, FourThreadsConcurrentRead) {
  const int num_threads = 4;
  const int reads_per_thread = 100;
  std::vector<std::thread> threads;

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([this, reads_per_thread]() {
      for (int i = 0; i < reads_per_thread; ++i) {
        auto key = MakeBytesVector("key" + std::to_string(i % 100));
        auto result = memtable.get(key);

        ASSERT_TRUE(result.has_value());
        std::string expected = "value" + std::to_string(i % 100);
        EXPECT_EQ(BytesToString(result.value()), expected);
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }
}

// ============================================================================
// READ-HEAVY MIXED OPERATION TESTS
// ============================================================================

class MemTableReadHeavyTest : public ::testing::Test {
protected:
  MemTable memtable{1024};

  void SetUp() override {
    // Pre-populate with initial data
    for (int i = 0; i < 50; ++i) {
      auto key = MakeBytesVector("key" + std::to_string(i));
      auto value = MakeBytesVector("value" + std::to_string(i));
      memtable.put(key, value);
    }
  }
};

TEST_F(MemTableReadHeavyTest, FourThreadsReadHeavyWithOccasionalWrites) {
  const int num_threads = 4;
  const int operations_per_thread = 200;
  std::vector<std::thread> threads;

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([this, operations_per_thread, t]() {
      for (int op = 0; op < operations_per_thread; ++op) {
        // 80% reads, 20% writes
        if (op % 5 == 0) {
          // Write operation
          int key_idx = 50 + t * 10 + (op / 5);
          auto key = MakeBytesVector("key" + std::to_string(key_idx));
          auto value =
              MakeBytesVector("value" + std::to_string(key_idx) + "_v2");
          memtable.put(key, value);
        } else {
          // Read operation
          int key_idx = op % 50;
          auto key = MakeBytesVector("key" + std::to_string(key_idx));
          auto result = memtable.get(key);
          ASSERT_TRUE(result.has_value());
        }
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }
}

// ============================================================================
// THREAD-SAFETY VALIDATION TESTS
// ============================================================================

class MemTableThreadSafetyTest : public ::testing::Test {
protected:
  MemTable memtable{1024};
};

TEST_F(MemTableThreadSafetyTest, SequentialWritesThenConcurrentReads) {
  // Phase 1: Sequential writes
  for (int i = 0; i < 50; ++i) {
    auto key = MakeBytesVector("key" + std::to_string(i));
    auto value = MakeBytesVector("value" + std::to_string(i));
    memtable.put(key, value);
  }

  // Phase 2: Concurrent reads from 4 threads
  const int num_threads = 4;
  const int reads_per_thread = 100;
  std::vector<std::thread> threads;

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([this, reads_per_thread]() {
      for (int i = 0; i < reads_per_thread; ++i) {
        auto key = MakeBytesVector("key" + std::to_string(i % 50));
        auto result = memtable.get(key);

        ASSERT_TRUE(result.has_value());
        std::string expected = "value" + std::to_string(i % 50);
        EXPECT_EQ(BytesToString(result.value()), expected);
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }
}

TEST_F(MemTableThreadSafetyTest, SequentialWritesThenConcurrentMixedOps) {
  // Phase 1: Sequential writes of initial data
  for (int i = 0; i < 30; ++i) {
    auto key = MakeBytesVector("key" + std::to_string(i));
    auto value = MakeBytesVector("value" + std::to_string(i));
    memtable.put(key, value);
  }

  // Phase 2: 4 threads with mixed operations
  const int num_threads = 4;
  const int operations_per_thread = 150;
  std::vector<std::thread> threads;

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([this, operations_per_thread, t]() {
      for (int op = 0; op < operations_per_thread; ++op) {
        // 70% reads, 30% writes to test thread-safety
        if (op % 10 < 3) {
          // Write operation
          int key_idx = 30 + t * 5 + (op / 10);
          auto key = MakeBytesVector("key" + std::to_string(key_idx));
          auto value = MakeBytesVector("updated" + std::to_string(key_idx));
          memtable.put(key, value);
        } else {
          // Read operation
          int key_idx = op % 30;
          auto key = MakeBytesVector("key" + std::to_string(key_idx));
          auto result = memtable.get(key);
          ASSERT_TRUE(result.has_value());
        }
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }
}
