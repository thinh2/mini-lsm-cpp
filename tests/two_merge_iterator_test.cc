#include "two_merge_iterator.hpp"

#include "test_utilities.hpp"
#include "gtest/gtest.h"

#include <memory>
#include <utility>
#include <vector>

namespace {

using KeyValue = std::pair<std::vector<std::byte>, std::vector<std::byte>>;
using test_utils::MakeKeyValueEntryFromString;

class VectorIteratorStub : public Iterator {
public:
  explicit VectorIteratorStub(std::vector<KeyValue> entries)
      : entries_(std::move(entries)) {}

  void next() override {
    if (is_valid()) {
      ++current_index_;
    }
  }

  std::vector<std::byte> key() override {
    if (!is_valid()) {
      return {};
    }
    return entries_[current_index_].first;
  }

  std::vector<std::byte> value() override {
    if (!is_valid()) {
      return {};
    }
    return entries_[current_index_].second;
  }

  bool is_valid() override { return current_index_ < entries_.size(); }

private:
  std::vector<KeyValue> entries_;
  std::size_t current_index_{0};
};

std::vector<KeyValue> DrainIterator(TwoMergeIterator &iterator) {
  std::vector<KeyValue> results;
  while (iterator.is_valid()) {
    results.emplace_back(iterator.key(), iterator.value());
    iterator.next();
  }
  return results;
}

} // namespace

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

  EXPECT_EQ(DrainIterator(iterator), expected);
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

  EXPECT_EQ(DrainIterator(iterator), expected);
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
    EXPECT_EQ(DrainIterator(iterator), left_only_entries);
    EXPECT_FALSE(iterator.is_valid());
  }

  {
    TwoMergeIterator iterator(
        std::make_unique<VectorIteratorStub>(empty),
        std::make_unique<VectorIteratorStub>(right_only_entries));
    EXPECT_EQ(DrainIterator(iterator), right_only_entries);
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
    EXPECT_EQ(DrainIterator(iterator), expected);
    EXPECT_FALSE(iterator.is_valid());
  }
}
