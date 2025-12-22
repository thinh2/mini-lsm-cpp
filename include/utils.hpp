#pragma once
#include <array>
#include <cstddef>

inline std::array<std::byte, 2> encode_uint16_t(uint16_t val) {
  std::array<std::byte, 2> res;
  res[0] = std::byte(val >> 8);
  res[1] = std::byte(val & 0xFF);
  return res;
}

inline uint16_t decode_uin16_t(std::array<std::byte, 2> &tmp) {
  uint16_t high = static_cast<uint16_t>(std::to_integer<uint8_t>(tmp[0]));
  uint16_t low = static_cast<uint16_t>(std::to_integer<uint8_t>(tmp[1]));
  return static_cast<uint16_t>(high << 8 | low);
}
