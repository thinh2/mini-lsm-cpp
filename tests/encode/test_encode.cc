#include "utils.hpp"
#include <array>
#include <gtest/gtest.h>

class EncodingTest : public ::testing::Test {};

TEST_F(EncodingTest, U64EncodeTest) {
  auto encode_result = encode_uint64_t(83129192);
  std::array<std::byte, 4> expected_result{
      {std::byte(0x04), std::byte(0xf4), std::byte(0x73), std::byte(0x68)}};
  EXPECT_EQ(encode_result, expected_result);
}

TEST_F(EncodingTest, U64DecodeTest) {
  uint64_t expected_result = 83129192;
  std::array<std::byte, 4> encoded_val{
      {std::byte(0x04), std::byte(0xf4), std::byte(0x73), std::byte(0x68)}};
  auto decode_val = decode_uint64_t(encoded_val);
  EXPECT_EQ(expected_result, decode_val);
}
