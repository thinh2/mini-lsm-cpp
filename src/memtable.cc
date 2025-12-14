#include "memtable.hpp"

#include <mutex>
std::optional<std::vector<std::byte>>
MemTable::get(const std::vector<std::byte> &key) {
  std::shared_lock lk{shared_mu_};
  if (storage_.contains(key)) {
    return storage_[key];
  }
  return std::nullopt;
}

void MemTable::put(const std::vector<std::byte> &key,
                   const std::vector<std::byte> &value) {
  std::lock_guard lk{shared_mu_};
  if (storage_.contains(key)) {
    approximate_size_ -= storage_[key].size();
    approximate_size_ += value.size();
  } else {
    approximate_size_ += key.size() + value.size();
  }
  storage_[key] = value;
}
