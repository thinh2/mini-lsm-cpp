#include "memtable.hpp"
#include "sst/sst.hpp"
#include "sst/sst_builder.hpp"
#include <filesystem>
#include <format>
#include <mutex>

MemTable::MemTable(uint64_t size, uint64_t id)
    : approximate_size_(0), cap_size_(size), status_(Status::Mutable), id_(id) {
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

SST MemTable::flush(SSTConfig &sst_config) {
  auto filename = std::format("sst_{}", id_);
  const std::filesystem::path sst_path =
      sst_config.sst_directory_.empty() ? std::filesystem::path(filename)
                                        : sst_config.sst_directory_ / filename;
  SSTBuilder sst_builder(sst_path, sst_config);
  auto mem_table_iter = get_iteartor();
  while (mem_table_iter.is_valid()) {
    auto key = mem_table_iter.key();
    auto value = mem_table_iter.value();
    sst_builder.add_entry(key, value);
    mem_table_iter.next();
  }

  return sst_builder.build();
}

ImmutableMemTableIterator::ImmutableMemTableIterator(
    std::shared_ptr<MemTableStorage> storage)
    : storage_(storage) {
  curr_it_ = storage_->begin();
}

bool ImmutableMemTableIterator::is_valid() {
  return curr_it_ != storage_->end();
}

std::vector<std::byte> ImmutableMemTableIterator::key() {
  if (!is_valid()) {
    return {};
  }
  return curr_it_->first;
}

std::vector<std::byte> ImmutableMemTableIterator::value() {
  if (!is_valid()) {
    return {};
  }
  return curr_it_->second;
}

void ImmutableMemTableIterator::next() {
  if (curr_it_ != storage_->end()) {
    curr_it_ = std::next(curr_it_);
    if (curr_it_ == storage_->end())
      return;
  }
}
