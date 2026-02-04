#pragma once

#include "iterator.hpp"

class ConcatIterator : public Iterator {
public:
  ConcatIterator(std::vector<std::unique_ptr<Iterator>> &&iters);
  void next();
  std::vector<std::byte> key();
  std::vector<std::byte> value();
  bool is_valid();

private:
  std::vector<std::unique_ptr<Iterator>> iters_;
  size_t curr_iter_idx_;
};
