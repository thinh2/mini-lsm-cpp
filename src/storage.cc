#include "storage.hpp"
#include <mutex>

void Storage::put(std::vector<std::byte> &key, std::vector<std::byte> &value) {
  uint64_t record_size = key.size() + value.size();
  std::lock_guard lk{mu_};
  if (record_size + active_memtable_->size() > opt_.mem_table_size) {
    immutable_memtable_.push_back(std::move(active_memtable_));
    active_memtable_ = std::make_unique<MemTable>(opt_.mem_table_size);
  }

  active_memtable_->put(key, value);
}

std::optional<std::vector<std::byte>>
Storage::get(std::vector<std::byte> &key) {
  std::shared_lock lk{mu_};
  std::optional<std::vector<std::byte>> value_slice;
  value_slice = active_memtable_->get(key);
  if (value_slice.has_value()) {
    if (value_slice.value().size() > 0) {
      return value_slice;
    }
    return std::nullopt;
  }

  for (auto it = immutable_memtable_.rbegin(); it != immutable_memtable_.rend();
       it = next(it)) {
    value_slice = (*it)->get(key);
    if (value_slice == std::nullopt)
      continue;
    if (value_slice.value().size() > 0) {
      return value_slice;
    }
    return std::nullopt;
  }

  return std::nullopt;
}

void Storage::remove(std::vector<std::byte> &key) { put(key, TOMBSTONE); }
