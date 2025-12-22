#pragma once

#include "iterator.hpp"
#include <memory>

class Block;
class BlockIterator : public Iterator {
public:
  BlockIterator(std::shared_ptr<Block> block);
  void next() override;
  std::vector<std::byte> key() override;
  std::vector<std::byte> value() override;
  bool is_valid() override;

private:
  struct Record {
    std::vector<std::byte> key_;
    std::vector<std::byte> value_;
  };

private:
  std::shared_ptr<Block> block_ptr_;
  Record curr_record_;
  uint64_t curr_offsets_idx_;
};
