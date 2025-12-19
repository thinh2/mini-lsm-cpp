#pragma once
#include <array>

std::array<std::byte, 2> encode_uint16_t(uint16_t val) {
  std::array<std::byte, 2> res;
  res[0] = std::byte(val >> 8);
  res[1] = std::byte(val & 0xFF);
  return res;
}

uint16_t decode_uin16_t(const std::array<std::byte, 2> &tmp) {
  return std::bit_cast<uint16_t>(tmp);
}
