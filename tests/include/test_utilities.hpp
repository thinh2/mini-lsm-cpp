#pragma once

#include <cstddef>
#include <iostream>
#include <random>
#include <string>
#include <vector>
namespace test_utils {

// Convert string to std::vector<std::byte>
inline std::vector<std::byte> MakeBytesVector(const std::string &&str) {
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

inline std::vector<std::pair<std::vector<std::byte>, std::vector<std::byte>>>
MakeKeyValueEntryFromString(
    const std::vector<std::pair<std::string, std::string>> &kv_strs) {
  std::vector<std::pair<std::vector<std::byte>, std::vector<std::byte>>> res;
  res.reserve(kv_strs.size());
  for (auto &entry : kv_strs) {
    res.emplace_back(MakeBytesVector(std::string(entry.first)),
                     MakeBytesVector(std::string(entry.second)));
  }
  return res;
}

// Generate a random string of given length
inline std::string MakeRandomString(size_t length) {
  static const char charset[] = "0123456789"
                                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                "abcdefghijklmnopqrstuvwxyz";
  static thread_local std::mt19937 rng{std::random_device{}()};
  static thread_local std::uniform_int_distribution<size_t> dist(
      0, sizeof(charset) - 2);

  std::string result;
  result.reserve(length);
  for (size_t i = 0; i < length; ++i) {
    result += charset[dist(rng)];
  }
  return result;
}

// Generate random key and value as byte vectors
inline std::pair<std::vector<std::byte>, std::vector<std::byte>>
MakeRandomKeyValue(size_t key_len, size_t value_len) {
  std::string key = MakeRandomString(key_len);
  std::string value = MakeRandomString(value_len);
  // std::cout << key << " " << value << std::endl;
  return {MakeBytesVector(std::move(key)), MakeBytesVector(std::move(value))};
}

using KeyValue = std::pair<std::vector<std::byte>, std::vector<std::byte>>;

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
} // namespace test_utils
