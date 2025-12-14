#pragma once

#include <cstddef>
#include <string>
#include <vector>

namespace test_utils {

// Convert string to std::vector<std::byte>
inline std::vector<std::byte> MakeBytesVector(const std::string &str) {
  std::vector<std::byte> bytes;
  for (char c : str) {
    bytes.push_back(static_cast<std::byte>(c));
  }
  return bytes;
}

// Convert std::vector<std::byte> to string
inline std::string BytesToString(const std::vector<std::byte> &bytes) {
  std::string str;
  for (std::byte b : bytes) {
    str.push_back(static_cast<char>(b));
  }
  return str;
}

// Create a byte vector with repeated character
inline std::vector<std::byte> MakeBytesVectorRepeated(char c, size_t count) {
  std::vector<std::byte> bytes;
  for (size_t i = 0; i < count; ++i) {
    bytes.push_back(static_cast<std::byte>(c));
  }
  return bytes;
}

// Create a byte vector with null bytes
inline std::vector<std::byte>
MakeBytesVectorWithNulls(const std::string &pattern) {
  std::vector<std::byte> bytes;
  for (char c : pattern) {
    if (c == 'N') {
      bytes.push_back(static_cast<std::byte>(0));
    } else {
      bytes.push_back(static_cast<std::byte>(c));
    }
  }
  return bytes;
}

} // namespace test_utils
