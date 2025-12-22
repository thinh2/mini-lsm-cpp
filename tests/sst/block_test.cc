
#include "sst/block.hpp"
#include "sst/block_builder.hpp"
#include "test_utilities.hpp"
#include <gtest/gtest.h>

using test_utils::MakeBytesVector;

class BlockTest : public ::testing ::Test {};

TEST_F(BlockTest, BlockGetEntrySimple) {
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
