#include "concat_iterator.hpp"

#include "test_utilities.hpp"
#include "gtest/gtest.h"

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>
using test_utils::KeyValue;
using test_utils::MakeKeyValueEntryFromString;
using test_utils::VectorIteratorStub;

std::vector<KeyValue> DrainIterator(Iterator *iterator) {
  std::vector<KeyValue> results;
  while (iterator->is_valid()) {
    results.emplace_back(iterator->key(), iterator->value());
    iterator->next();
  }
  return results;
}

std::vector<std::unique_ptr<Iterator>>
build_iterator_vector(std::vector<std::vector<KeyValue>> values) {
  std::vector<std::unique_ptr<Iterator>> iters;
  for (auto value : values) {
    iters.push_back(std::make_unique<VectorIteratorStub>(value));
  }
  return iters;
}

TEST(ConcatIteratorTest, ConcatTwoIterator) {
  auto first_iter = MakeKeyValueEntryFromString(
      {{"a", "left-a"}, {"c", "left-c"}, {"f", "left-f"}});
  auto second_iter = MakeKeyValueEntryFromString(
      {{"b", "right-b"}, {"d", "right-d"}, {"e", "right-e"}});
  auto expected = MakeKeyValueEntryFromString({{"a", "left-a"},
                                               {"c", "left-c"},
                                               {"f", "left-f"},
                                               {"b", "right-b"},
                                               {"d", "right-d"},
                                               {"e", "right-e"}});

  auto iters = build_iterator_vector({first_iter, second_iter});
  ConcatIterator iterator(std::move(iters));

  EXPECT_EQ(DrainIterator(&iterator), expected);
  EXPECT_FALSE(iterator.is_valid());
}

TEST(ConcatIteratorTest, ConcatThreeIterator) {
  auto first_iter = MakeKeyValueEntryFromString({{"a", "value-a"}});
  auto second_iter = MakeKeyValueEntryFromString(
      {{"b", "value-b"}, {"d", "value-d"}, {"e", "value-e"}});
  auto third_iter =
      MakeKeyValueEntryFromString({{"c", "value-c"}, {"f", "value-f"}});
  auto expected = MakeKeyValueEntryFromString({{"a", "value-a"},
                                               {"b", "value-b"},
                                               {"d", "value-d"},
                                               {"e", "value-e"},
                                               {"c", "value-c"},
                                               {"f", "value-f"}});

  auto iters = build_iterator_vector({first_iter, second_iter, third_iter});
  ConcatIterator iterator(std::move(iters));

  EXPECT_EQ(DrainIterator(&iterator), expected);
  EXPECT_FALSE(iterator.is_valid());
}

TEST(ConcatIteratorTest, ConcatWithEmptyIterator) {
  auto first_iter = MakeKeyValueEntryFromString({{"a", "value-a"}});
  auto second_iter = MakeKeyValueEntryFromString({});
  auto third_iter =
      MakeKeyValueEntryFromString({{"c", "value-c"}, {"f", "value-f"}});
  auto expected = MakeKeyValueEntryFromString(
      {{"a", "value-a"}, {"c", "value-c"}, {"f", "value-f"}});

  auto iters = build_iterator_vector({first_iter, second_iter, third_iter});
  ConcatIterator iterator(std::move(iters));

  EXPECT_EQ(DrainIterator(&iterator), expected);
  EXPECT_FALSE(iterator.is_valid());
}

TEST(ConcatIteratorTest, ConcatWithMultipleEmptyIterator) {
  std::vector<std::vector<KeyValue>> values{
      MakeKeyValueEntryFromString({{"a", "value-a"}}),
      MakeKeyValueEntryFromString({}),
      MakeKeyValueEntryFromString({}),
      MakeKeyValueEntryFromString({{"c", "value-c"}, {"f", "value-f"}}),
  };

  auto expected = MakeKeyValueEntryFromString(
      {{"a", "value-a"}, {"c", "value-c"}, {"f", "value-f"}});

  auto iters = build_iterator_vector(values);
  ConcatIterator iterator(std::move(iters));
  EXPECT_EQ(DrainIterator(&iterator), expected);
  EXPECT_FALSE(iterator.is_valid());
}
