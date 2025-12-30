#include "sst/sst_iterator.hpp"
#include "sst/block.hpp"
#include "sst/block_iterator.hpp"
#include "sst/sst.hpp"
#include <memory>

SSTIterator::SSTIterator(std::shared_ptr<SST> sst_ptr)
    : sst_ptr_(sst_ptr), block_idx_(0),
      curr_block_iterator_(std::make_shared<Block>(sst_ptr->get_block(0))) {
  curr_entry.key_ = curr_block_iterator_.key();
  curr_entry.val_ = curr_block_iterator_.value();
}

void SSTIterator::next() {
  if (!is_valid()) {
    return;
  }

  curr_block_iterator_.next();

  if (!curr_block_iterator_.is_valid()) {
    block_idx_++;
    if (!is_valid())
      return;
    curr_block_iterator_ =
        BlockIterator(std::make_shared<Block>(sst_ptr_->get_block(block_idx_)));
  }

  curr_entry.key_ = curr_block_iterator_.key();
  curr_entry.val_ = curr_block_iterator_.value();
}

std::vector<std::byte> SSTIterator::key() { return curr_entry.key_; }

std::vector<std::byte> SSTIterator::value() { return curr_entry.val_; }

bool SSTIterator::is_valid() {
  return (block_idx_ < sst_ptr_->number_of_block());
}
