#include "merge_iterator.hpp"

#include "memtable.hpp"
#include "test_utilities.hpp"
#include "gtest/gtest.h"
#include <algorithm>
#include <iostream>

using test_utils::MakeRandomKeyValue;
class MergeIteratorTest : public ::testing::Test {};

TEST_F(MergeIteratorTest, MergeMemTableIterator) {
  std::vector<std::unique_ptr<MemTable>> mem_table;
  std::vector<std::pair<std::vector<std::byte>, std::vector<std::byte>>> kvs;
  for (int i = 0; i < 2; i++) {
    mem_table.emplace_back(
        std::make_unique<MemTable>((uint64_t)10000000000, (uint64_t)i));
  }

  for (int i = 0; i < mem_table.size(); i++) {
    size_t mem_table_sz = 1;
    for (size_t idx = 0; idx < mem_table_sz; idx++) {
      auto random_kv = MakeRandomKeyValue(1, 1);
      std::cout << (char)random_kv.first[0] << " " << (char)random_kv.second[0]
                << std::endl;
      kvs.push_back(random_kv);
      mem_table[i]->put(random_kv.first, random_kv.second);
    }
  }

  std::vector<std::unique_ptr<Iterator>> iters;
  for (auto &table : mem_table) {
    table->freeze();
    iters.emplace_back(
        std::make_unique<ImmutableMemTableIterator>(table->get_iteartor()));
  }

  std::sort(
      kvs.begin(), kvs.end(),
      [](const std::pair<std::vector<std::byte>, std::vector<std::byte>> &a,
         const std::pair<std::vector<std::byte>, std::vector<std::byte>> &b) {
        return a.first < b.first;
      });
  auto expected_it = kvs.begin();

  MergeIterator merge_iterator{std::move(iters)};
  while (merge_iterator.is_valid()) {
    EXPECT_EQ(expected_it->first, merge_iterator.key());
    EXPECT_EQ(expected_it->second, merge_iterator.value());
    expected_it = std::next(expected_it);
    merge_iterator.next();
  }
}
