#pragma once
#include <array>
#include <cstddef>
#include <filesystem>
#include <glob.h>
#include <span>
#include <vector>
inline std::array<std::byte, 2> encode_uint16_t(uint16_t val) {
  std::array<std::byte, 2> res;
  res[0] = std::byte(val >> 8);
  res[1] = std::byte(val & 0xFF);
  return res;
}

inline uint16_t decode_uint16_t(std::span<const std::byte, 2> &tmp) {
  uint16_t high = static_cast<uint16_t>(std::to_integer<uint8_t>(tmp[0]));
  uint16_t low = static_cast<uint16_t>(std::to_integer<uint8_t>(tmp[1]));
  return static_cast<uint16_t>(high << 8 | low);
}

inline std::array<std::byte, 8> encode_uint64_t(uint64_t val) {
  std::array<std::byte, 8> encoded_bytes;
  for (int i = 7; i >= 0; i--) {
    encoded_bytes[i] = std::byte(val & 0xFF);
    val = val >> 8;
  }
  return encoded_bytes;
}

inline uint64_t decode_uint64_t(std::span<const std::byte, 8> byte_views) {
  uint64_t decoded_val = 0;
  for (auto &val : byte_views) {
    decoded_val = (decoded_val << 8) |
                  static_cast<uint64_t>(std::to_integer<uint8_t>(val));
  }
  return decoded_val;
}

inline std::vector<std::filesystem::path>
glob_paths(const std::string &pattern) {
  glob_t g{};
  std::vector<std::filesystem::path> matches;
  if (glob(pattern.c_str(), GLOB_TILDE, nullptr, &g) == 0) {
    for (size_t i = 0; i < g.gl_pathc; ++i)
      matches.emplace_back(g.gl_pathv[i]);
  }
  globfree(&g);
  return matches;
}
