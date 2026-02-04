#include "two_merge_iterator.hpp"

#include "test_utilities.hpp"
#include "gtest/gtest.h"

#include <memory>
#include <utility>
#include <vector>

using test_utils::KeyValue;
using test_utils::MakeKeyValueEntryFromString;
using test_utils::VectorIteratorStub;
// namespace
std::vector<KeyValue> DrainIterator(Iterator *iterator) {
  std::vector<KeyValue> results;
  while (iterator->is_valid()) {
    results.emplace_back(iterator->key(), iterator->value());
    iterator->next();
  }
  return results;
}

TEST(TwoMergeIteratorTest, MergesSortedEntries) {
  auto left_entries = MakeKeyValueEntryFromString(
      {{"a", "left-a"}, {"c", "left-c"}, {"f", "left-f"}});
  auto right_entries = MakeKeyValueEntryFromString(
      {{"b", "right-b"}, {"d", "right-d"}, {"e", "right-e"}});
  auto expected = MakeKeyValueEntryFromString({{"a", "left-a"},
                                               {"b", "right-b"},
                                               {"c", "left-c"},
                                               {"d", "right-d"},
                                               {"e", "right-e"},
                                               {"f", "left-f"}});

  TwoMergeIterator iterator(
      std::make_unique<VectorIteratorStub>(left_entries),
      std::make_unique<VectorIteratorStub>(right_entries));

  EXPECT_EQ(DrainIterator(&iterator), expected);
  EXPECT_FALSE(iterator.is_valid());
}

TEST(TwoMergeIteratorTest, PrefersLeftOnDuplicateKeys) {
  auto left_entries = MakeKeyValueEntryFromString(
      {{"a", "left-a"}, {"b", "left-b"}, {"d", "left-d"}});
  auto right_entries = MakeKeyValueEntryFromString({{"a", "right-a"},
                                                    {"b", "right-b"},
                                                    {"b", "right-b2"},
                                                    {"c", "right-c"}});
  auto expected = MakeKeyValueEntryFromString(
      {{"a", "left-a"}, {"b", "left-b"}, {"c", "right-c"}, {"d", "left-d"}});

  TwoMergeIterator iterator(
      std::make_unique<VectorIteratorStub>(left_entries),
      std::make_unique<VectorIteratorStub>(right_entries));

  EXPECT_EQ(DrainIterator(&iterator), expected);
  EXPECT_FALSE(iterator.is_valid());
}

TEST(TwoMergeIteratorTest, HandlesExhaustedIterators) {
  auto empty = MakeKeyValueEntryFromString({});
  auto left_only_entries = MakeKeyValueEntryFromString({{"a", "left-a"}});
  auto right_only_entries =
      MakeKeyValueEntryFromString({{"a", "right-a"}, {"b", "right-b"}});

  {
    TwoMergeIterator iterator(
        std::make_unique<VectorIteratorStub>(left_only_entries),
        std::make_unique<VectorIteratorStub>(empty));
    EXPECT_EQ(DrainIterator(&iterator), left_only_entries);
    EXPECT_FALSE(iterator.is_valid());
  }

  {
    TwoMergeIterator iterator(
        std::make_unique<VectorIteratorStub>(empty),
        std::make_unique<VectorIteratorStub>(right_only_entries));
    EXPECT_EQ(DrainIterator(&iterator), right_only_entries);
    EXPECT_FALSE(iterator.is_valid());
  }

  {
    auto interleaved_left =
        MakeKeyValueEntryFromString({{"a", "left-a"}, {"c", "left-c"}});
    auto interleaved_right =
        MakeKeyValueEntryFromString({{"b", "right-b"}, {"d", "right-d"}});
    auto expected = MakeKeyValueEntryFromString(
        {{"a", "left-a"}, {"b", "right-b"}, {"c", "left-c"}, {"d", "right-d"}});

    TwoMergeIterator iterator(
        std::make_unique<VectorIteratorStub>(interleaved_left),
        std::make_unique<VectorIteratorStub>(interleaved_right));
    EXPECT_EQ(DrainIterator(&iterator), expected);
    EXPECT_FALSE(iterator.is_valid());
  }
}
