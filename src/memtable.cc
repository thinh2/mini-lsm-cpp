#include "memtable.hpp"

#include <mutex>
MemTable::MemTable(uint64_t size)
    : approximate_size_(0), cap_size_(size), status_(Status::Mutable) {
  storage_ = std::make_shared<MemTableStorage>();
}

std::optional<std::vector<std::byte>>
MemTable::get(const std::vector<std::byte> &key) {
  std::shared_lock lk{shared_mu_};
  if (storage_->contains(key)) {
    return storage_->at(key);
  }
  return std::nullopt;
}

void MemTable::put(const std::vector<std::byte> &key,
                   const std::vector<std::byte> &value) {
  std::lock_guard lk{shared_mu_};
  if (status_ == Status::Immutable) {
    throw std::runtime_error("write to immutable");
  }

  if (storage_->contains(key)) {
    approximate_size_ -= storage_->at(key).size();
    approximate_size_ += value.size();
  } else {
    approximate_size_ += key.size() + value.size();
  }
  storage_->insert_or_assign(key, value);
}

ImmutableMemTableIterator MemTable::get_iteartor() {
  std::lock_guard lk{shared_mu_};
  if (status_ != Status::Immutable) {
    throw std::runtime_error("get_iterator for mutable mem_table");
  }
  return ImmutableMemTableIterator{storage_};
}

ImmutableMemTableIterator::ImmutableMemTableIterator(
    std::shared_ptr<MemTableStorage> storage)
    : storage_(storage) {
  curr_it_ = storage_->begin();
}

bool ImmutableMemTableIterator::is_valid() {
  return curr_it_ != storage_->end();
}

void ImmutableMemTableIterator::next() {
  if (curr_it_ != storage_->end()) {
    curr_it_ = std::next(curr_it_);
    curr_val_.key_ = curr_it_->first;
    curr_val_.value_ = curr_it_->second;
  }
}
