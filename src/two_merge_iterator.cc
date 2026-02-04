#include "two_merge_iterator.hpp"
#include <algorithm>

TwoMergeIterator::TwoMergeIterator(std::unique_ptr<Iterator> left,
                                   std::unique_ptr<Iterator> right)
    : left_(std::move(left)), right_(std::move(right)) {
  refresh_active();
}

void TwoMergeIterator::next() {
  if (current_side_ == ActiveSide::None) {
    return;
  }

  if (current_side_ == ActiveSide::Left && left_) {
    const auto current_key = left_->key();
    left_->next();
    skip_right_duplicates(current_key);
  } else if (current_side_ == ActiveSide::Right && right_) {
    right_->next();
  }

  refresh_active();
}

std::vector<std::byte> TwoMergeIterator::key() {
  if (current_side_ == ActiveSide::Left && left_) {
    return left_->key();
  }
  if (current_side_ == ActiveSide::Right && right_) {
    return right_->key();
  }
  return {};
}

std::vector<std::byte> TwoMergeIterator::value() {
  if (current_side_ == ActiveSide::Left && left_) {
    return left_->value();
  }
  if (current_side_ == ActiveSide::Right && right_) {
    return right_->value();
  }
  return {};
}

bool TwoMergeIterator::is_valid() { return current_side_ != ActiveSide::None; }

void TwoMergeIterator::refresh_active() {
  current_side_ = ActiveSide::None;

  const bool left_valid = left_ && left_->is_valid();
  const bool right_valid = right_ && right_->is_valid();

  if (left_valid && right_valid) {
    const auto left_key = left_->key();
    const auto right_key = right_->key();

    if (left_key == right_key) {
      current_side_ = ActiveSide::Left;
      skip_right_duplicates(left_key);
      return;
    }

    if (std::lexicographical_compare(left_key.begin(), left_key.end(),
                                     right_key.begin(), right_key.end())) {
      current_side_ = ActiveSide::Left;
    } else {
      current_side_ = ActiveSide::Right;
    }
    return;
  }

  if (left_valid) {
    current_side_ = ActiveSide::Left;
    return;
  }

  if (right_valid) {
    current_side_ = ActiveSide::Right;
  }
}

void TwoMergeIterator::skip_right_duplicates(
    const std::vector<std::byte> &key) {
  if (!right_) {
    return;
  }

  while (right_->is_valid()) {
    const auto candidate = right_->key();
    if (candidate == key) {
      right_->next();
      continue;
    }
    break;
  }
}
