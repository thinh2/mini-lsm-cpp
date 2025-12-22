#include "sst/block_iterator.hpp"
#include "sst/block.hpp"
#include <algorithm>

BlockIterator::BlockIterator(std::shared_ptr<Block> block_ptr)
    : block_ptr_(block_ptr), curr_offsets_idx_(0) {
  if (block_ptr_->size() > 0) {
    auto entry = block_ptr_->get_entry(curr_offsets_idx_);
    curr_record_ = Record{.key_ = std::move(entry.key_),
                          .value_ = std::move(entry.value_)};
  }
}

void BlockIterator::next() {
  if (curr_offsets_idx_ < block_ptr_->size()) {
    curr_offsets_idx_++;
    auto entry = block_ptr_->get_entry(curr_offsets_idx_);
    curr_record_ = Record{.key_ = std::move(entry.key_),
                          .value_ = std::move(entry.value_)};
  }
}

std::vector<std::byte> BlockIterator::key() { return curr_record_.key_; }

std::vector<std::byte> BlockIterator::value() { return curr_record_.value_; }

bool BlockIterator::is_valid() {
  return curr_offsets_idx_ < block_ptr_->size();
}
