#pragma once

#include <optional>
#include <span>
#include <vector>

class Block {
public:
  struct Entry {
    std::vector<std::byte> key_;
    std::vector<std::byte> value_;
  };
  static const size_t EntryKeyLenSize = 2;
  static const size_t EntryValueLenSize = 2;
  static const size_t FooterLenSize = 2;
  static const size_t OffsetSize = 2;

  Block(std::vector<std::byte> data, std::vector<uint16_t> offset)
      : data_(std::move(data)), offsets_(std::move(offset)) {}

  /**
   * @brief The encoded format is
   * data_ | offsets_ | number_of_entries (2byte)
   * Decode read the number_of_entries at the end of the data, and subsequent
   * read the offsets_ value. Each element in offsets_ value is 2 bytes.
   * @return std::vector<std::byte>
   */
  std::vector<std::byte> encode();
  static Block decode(const std::vector<std::byte> &bytes);
  Entry get_entry(size_t entry_idx);
  size_t size();
  std::optional<std::vector<std::byte>> get(const std::vector<std::byte> &key);

  std::vector<std::byte> get_first_key();
  std::vector<std::byte> get_last_key();

private:
  std::vector<std::byte> data_;
  std::vector<uint16_t> offsets_;
};
