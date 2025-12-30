#include "utils.hpp"
#include <array>
#include <gtest/gtest.h>

class EncodingTest : public ::testing::Test {};

TEST_F(EncodingTest, U64EncodeTest) {
  auto encode_result = encode_uint64_t(8312912411919373312LL);
  std::array<std::byte, 8> expected_result{
      {std::byte(0x73), std::byte(0x5d), std::byte(0x65), std::byte(0xcb),
       std::byte(0x81), std::byte(0x63), std::byte(0xd4), std::byte(0x00)}};
  EXPECT_EQ(encode_result, expected_result);
}

TEST_F(EncodingTest, U64DecodeTest) {
  uint64_t expected_result = 8312912411919373312;
  std::array<std::byte, 8> encoded_val{
      {std::byte(0x73), std::byte(0x5d), std::byte(0x65), std::byte(0xcb),
       std::byte(0x81), std::byte(0x63), std::byte(0xd4), std::byte(0x00)}};
  auto decode_val = decode_uint64_t(encoded_val);
  EXPECT_EQ(expected_result, decode_val);
}
