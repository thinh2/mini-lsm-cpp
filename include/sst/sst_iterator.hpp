#pragma once
#include "block_iterator.hpp"
#include "iterator.hpp"
#include <memory>

class SST;
class BlockIterator;

class SSTIterator : public Iterator {
public:
  SSTIterator(std::shared_ptr<SST> sst_ptr);
  void next();
  std::vector<std::byte> key();
  std::vector<std::byte> value();
  bool is_valid();

private:
  struct Entry {
    std::vector<std::byte> key_;
    std::vector<std::byte> val_;
  };

private:
  std::shared_ptr<SST> sst_ptr_;
  BlockIterator curr_block_iterator_;
  size_t block_idx_;
  Entry curr_entry;
};
