#pragma once

#include "iterator.hpp"
#include <cstddef>
#include <map>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>

using MemTableStorage =
    std::map<std::vector<std::byte>, std::vector<std::byte>>;

class ImmutableMemTableIterator;

class MemTable {
public:
  enum class Status { Mutable, Immutable };

public:
  MemTable(uint64_t size);
  std::optional<std::vector<std::byte>> get(const std::vector<std::byte> &key);
  void put(const std::vector<std::byte> &key,
           const std::vector<std::byte> &value);
  uint64_t size() {
    std::shared_lock lk{shared_mu_};
    return approximate_size_;
  }

  void freeze() {
    std::lock_guard lk{shared_mu_};
    status_ = Status::Immutable;
  }

  ImmutableMemTableIterator get_iteartor();

private:
  std::shared_ptr<MemTableStorage> storage_;
  std::shared_mutex shared_mu_;
  std::uint64_t approximate_size_;
  std::uint64_t cap_size_;
  Status status_;
};

// support immutable/freezed memtable iterator only.
class ImmutableMemTableIterator : public Iterator {
public:
  ImmutableMemTableIterator(std::shared_ptr<MemTableStorage> storage);
  bool is_valid();

  // return the latest valid key
  std::vector<std::byte> key();

  // return the latest valid value
  std::vector<std::byte> value();

  void next();

  ~ImmutableMemTableIterator() = default;

private:
  std::shared_ptr<MemTableStorage> storage_;
  MemTableStorage::iterator curr_it_;
};
