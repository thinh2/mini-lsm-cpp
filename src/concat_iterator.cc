#include "concat_iterator.hpp"

ConcatIterator::ConcatIterator(std::vector<std::unique_ptr<Iterator>> &&iters)
    : iters_(std::move(iters)), curr_iter_idx_(0) {}

void ConcatIterator::next() {
  iters_[curr_iter_idx_]->next();
  if (!iters_[curr_iter_idx_]->is_valid()) {
    while (is_valid() && !iters_[curr_iter_idx_]->is_valid()) {
      curr_iter_idx_++;
    }
  }
}

std::vector<std::byte> ConcatIterator::key() {
  return iters_[curr_iter_idx_]->key();
}

std::vector<std::byte> ConcatIterator::value() {
  return iters_[curr_iter_idx_]->value();
}

bool ConcatIterator::is_valid() { return curr_iter_idx_ < iters_.size(); }
