#include "merge_iterator.hpp"
#include <algorithm>
#include <iostream>
MergeIterator::MergeIterator(std::vector<std::unique_ptr<Iterator>> &&iterators)
    : heap_(std::move(iterators)) {
  std::make_heap(heap_.begin(), heap_.end(), MergeIterator::comp);
}

void MergeIterator::next() {
  std::pop_heap(heap_.begin(), heap_.end(), MergeIterator::comp);
  heap_.back()->next();
  if (heap_.back()->is_valid()) {
    std::push_heap(heap_.begin(), heap_.end(), MergeIterator::comp);
  } else {
    heap_.pop_back();
  }

  if (is_valid()) {
    std::pop_heap(heap_.begin(), heap_.end(), MergeIterator::comp);
  }
}

std::vector<std::byte> MergeIterator::key() {
  // std::pop_heap(heap_.begin(), heap_.end(), MergeIterator::comp);
  return heap_.front()->key();
}

std::vector<std::byte> MergeIterator::value() {
  // std::pop_heap(heap_.begin(), heap_.end(), MergeIterator::comp);
  return heap_.front()->value();
}

bool MergeIterator::is_valid() { return !heap_.empty(); }
const std::function<bool(const std::unique_ptr<Iterator> &a,
                         const std::unique_ptr<Iterator> &b)>
    MergeIterator::comp =
        [](const std::unique_ptr<Iterator> &a,
           const std::unique_ptr<Iterator> &b) { return a->key() > b->key(); };
