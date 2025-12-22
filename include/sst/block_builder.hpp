#pragma once
#include <vector>

class Block;
class BlockBuilder {
public:
  BlockBuilder() : size_(0) {}
  void add_entry(std::vector<std::byte> &key, std::vector<std::byte> &value);
  Block build();
  size_t get_size();

private:
  std::vector<std::byte> data_;
  std::vector<std::uint16_t> offsets_;
  size_t size_;
};
