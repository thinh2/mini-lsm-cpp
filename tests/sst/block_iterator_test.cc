#include "sst/block.hpp"
#include "sst/block_builder.hpp"
#include "sst/block_iterator.hpp"
#include "test_utilities.hpp"
#include <gtest/gtest.h>

using test_utils::MakeKeyValueEntryFromString;
class BlockIteratorTest : public ::testing::Test {};

TEST_F(BlockIteratorTest, EmptyBlock) {
  BlockBuilder builder;
  auto block_ptr = std::make_shared<Block>(builder.build());
  auto block_iter = BlockIterator(block_ptr);
  EXPECT_EQ(block_iter.is_valid(), false);
}

TEST_F(BlockIteratorTest, SingleEntryBlock) {
  std::vector<std::pair<std::string, std::string>> entries_str{
      {"hello", "world"}};

  auto entries = MakeKeyValueEntryFromString(entries_str);
  BlockBuilder builder;
  builder.add_entry(entries[0].first, entries[0].second);
  auto block_ptr = std::make_shared<Block>(builder.build());
  auto block_iter = BlockIterator(block_ptr);

  int iter_size = 0;
  while (block_iter.is_valid()) {
    EXPECT_EQ(entries[iter_size].first, block_iter.key());
    EXPECT_EQ(entries[iter_size].second, block_iter.value());
    iter_size++;
    block_iter.next();
  }
  EXPECT_EQ(iter_size, 1);
}

TEST_F(BlockIteratorTest, NextAfterIteratorEnd) {
  std::vector<std::pair<std::string, std::string>> entries_str{
      {"hello", "world"}};

  auto entries = MakeKeyValueEntryFromString(entries_str);
  BlockBuilder builder;
  builder.add_entry(entries[0].first, entries[0].second);
  auto block_ptr = std::make_shared<Block>(builder.build());
  auto block_iter = BlockIterator(block_ptr);

  int iter_size = 0;
  while (block_iter.is_valid()) {
    iter_size++;
    block_iter.next();
  }

  block_iter.next();
  EXPECT_EQ(iter_size, 1);
  EXPECT_EQ(block_iter.is_valid(), false);
  EXPECT_EQ(entries[0].first, block_iter.key());
  EXPECT_EQ(entries[0].second, block_iter.value());
}

TEST_F(BlockIteratorTest, MulitpleEntryBlock) {
  std::vector<std::pair<std::string, std::string>> entries_str{
      {"hello", "world"}, {"banana", "pudding"}, {"mash", "potato"}};

  auto entries = MakeKeyValueEntryFromString(entries_str);
  BlockBuilder builder;

  for (auto &entry : entries) {
    builder.add_entry(entry.first, entry.second);
  }
  auto block_ptr = std::make_shared<Block>(builder.build());
  auto block_iter = BlockIterator(block_ptr);

  int iter_size = 0;
  while (block_iter.is_valid()) {
    EXPECT_EQ(entries[iter_size].first, block_iter.key());
    EXPECT_EQ(entries[iter_size].second, block_iter.value());
    iter_size++;
    block_iter.next();
  }
  EXPECT_EQ(iter_size, 3);
}
