#pragma once
#include "iterator.hpp"
#include <functional>
#include <memory>
#include <vector>

class MergeIterator : public Iterator {
public:
  MergeIterator(std::vector<std::unique_ptr<Iterator>> &&iterators);
  void next();
  std::vector<std::byte> key();
  std::vector<std::byte> value();
  bool is_valid();
  ~MergeIterator() = default;

private:
  static const std::function<bool(const std::unique_ptr<Iterator> &a,
                                  const std::unique_ptr<Iterator> &b)>
      comp;
  std::vector<std::unique_ptr<Iterator>> heap_;
};
