
#include "sst/block.hpp"
#include "sst/block_builder.hpp"
#include "test_utilities.hpp"
#include <gtest/gtest.h>
#include <vector>

using test_utils::MakeBytesVector;

class BlockTest : public ::testing ::Test {};

TEST_F(BlockTest, BlockGetEntrySingle) {
  BlockBuilder builder;

  auto key = MakeBytesVector("hello");
  auto val = MakeBytesVector("world");
  builder.add_entry(key, val);

  auto block = builder.build();
  EXPECT_EQ(block.size(), 1);

  auto entry = block.get_entry(0);
  EXPECT_EQ(entry.key_, key);
  EXPECT_EQ(entry.value_, val);
}

TEST_F(BlockTest, BlockGetMultipleEntry) {
  BlockBuilder builder;

  std::vector<std::pair<std::vector<std::byte>, std::vector<std::byte>>>
      records{{MakeBytesVector("hello"), MakeBytesVector("world")},
              {MakeBytesVector("test"), MakeBytesVector("kv")},
              {MakeBytesVector("heal"), MakeBytesVector("the world")},
              {MakeBytesVector("banana192"), MakeBytesVector("pudding + cup")}};

  for (auto &record : records) {
    builder.add_entry(record.first, record.second);
  }

  auto block = builder.build();

  for (size_t idx = 0; idx < records.size(); idx++) {
    auto entry = block.get_entry(idx);
    EXPECT_EQ(entry.key_, records[idx].first);
    EXPECT_EQ(entry.value_, records[idx].second);
  }
}

TEST_F(BlockTest, BlockEncodeSingleEntry) {
  BlockBuilder builder;

  std::vector<std::pair<std::vector<std::byte>, std::vector<std::byte>>>
      records = {{MakeBytesVector("hello"), MakeBytesVector("world")}};
  for (auto &record : records) {
    builder.add_entry(record.first, record.second);
  }

  auto block = builder.build();

  auto encoded_output = block.encode();
  std::vector<std::byte> expected_encoded{
      std::byte(0),   std::byte(5),   std::byte('h'), std::byte('e'),
      std::byte('l'), std::byte('l'), std::byte('o'), std::byte(0),
      std::byte(5),   std::byte('w'), std::byte('o'), std::byte('r'),
      std::byte('l'), std::byte('d'), std::byte(0),   std::byte(0), // offset
      std::byte(0),
      std::byte(1) // footer
  };
  EXPECT_EQ(encoded_output.size(), expected_encoded.size());
  EXPECT_EQ(encoded_output, expected_encoded);
}

TEST_F(BlockTest, BlockEncodeMultipleEntry) {
  BlockBuilder builder;

  std::vector<std::pair<std::vector<std::byte>, std::vector<std::byte>>>
      records = {{MakeBytesVector("a"), MakeBytesVector("b")},
                 {MakeBytesVector("x"), MakeBytesVector("y")},
                 {MakeBytesVector("xx"), MakeBytesVector("yy")}};
  for (auto &record : records) {
    builder.add_entry(record.first, record.second);
  }

  auto block = builder.build();

  auto encoded_output = block.encode();
  std::vector<std::byte> expected_encoded = {
      std::byte(0),   std::byte(1),   std::byte('a'), std::byte(0),
      std::byte(1),   std::byte('b'), std::byte(0),   std::byte(1),
      std::byte('x'), std::byte(0),   std::byte(1),   std::byte('y'),
      std::byte(0),   std::byte(2),   std::byte('x'), std::byte('x'),
      std::byte(0),   std::byte(2),   std::byte('y'), std::byte('y'),
      std::byte(0),   std::byte(0),   std::byte(0),   std::byte(6),
      std::byte(0),   std::byte(12),  std::byte(0),   std::byte(3) // footer
  };
  EXPECT_EQ(encoded_output.size(), expected_encoded.size());
  EXPECT_EQ(encoded_output, expected_encoded);
}

TEST_F(BlockTest, BlockDecodeSingleEntry) {
  std::vector<std::byte> binary_data{
      std::byte(0),   std::byte(5),   std::byte('h'), std::byte('e'),
      std::byte('l'), std::byte('l'), std::byte('o'), std::byte(0),
      std::byte(5),   std::byte('w'), std::byte('o'), std::byte('r'),
      std::byte('l'), std::byte('d'), std::byte(0),   std::byte(0), // offset
      std::byte(0),
      std::byte(1) // footer
  };

  auto block = Block::decode(binary_data);
  EXPECT_EQ(block.size(), 1);
}

TEST_F(BlockTest, BlockEncodeDecodeSingleEntry) {
  BlockBuilder builder;

  std::vector<std::pair<std::vector<std::byte>, std::vector<std::byte>>>
      records{{MakeBytesVector("hello"), MakeBytesVector("world")}};

  for (auto &record : records) {
    builder.add_entry(record.first, record.second);
  }

  auto block = builder.build();

  auto binary_data = block.encode();
  auto decoded_block = Block::decode(binary_data);
  for (size_t idx = 0; idx < records.size(); idx++) {
    auto entry = decoded_block.get_entry(idx);
    EXPECT_EQ(entry.key_, records[idx].first);
    EXPECT_EQ(entry.value_, records[idx].second);
  }
}

TEST_F(BlockTest, BlockEncodeDecodeMultipleEntry) {
  BlockBuilder builder;

  std::vector<std::pair<std::vector<std::byte>, std::vector<std::byte>>>
      records{{MakeBytesVector("hello"), MakeBytesVector("world")},
              {MakeBytesVector("test"), MakeBytesVector("kv")},
              {MakeBytesVector("heal"), MakeBytesVector("the world")},
              {MakeBytesVector("banana192"), MakeBytesVector("pudding + cup")}};

  for (auto &record : records) {
    builder.add_entry(record.first, record.second);
  }

  auto block = builder.build();

  auto binary_data = block.encode();
  auto decoded_block = Block::decode(binary_data);

  EXPECT_EQ(decoded_block.size(), records.size());

  for (size_t idx = 0; idx < records.size(); idx++) {
    auto entry = decoded_block.get_entry(idx);
    EXPECT_EQ(entry.key_, records[idx].first);
    EXPECT_EQ(entry.value_, records[idx].second);
  }
}
