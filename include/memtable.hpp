#pragma once

#include <cstddef>
#include <map>
#include <shared_mutex>
#include <string>
#include <vector>

class MemTable {
public:
  MemTable(uint64_t size) : approximate_size_(0), cap_size_(size) {}
  std::optional<std::vector<std::byte>> get(const std::vector<std::byte> &key);
  void put(const std::vector<std::byte> &key,
           const std::vector<std::byte> &value);
  uint64_t size() {
    std::shared_lock lk{shared_mu_};
    return approximate_size_;
  }

private:
  std::map<std::vector<std::byte>, std::vector<std::byte>> storage_;
  std::shared_mutex shared_mu_;
  std::uint64_t approximate_size_;
  std::uint64_t cap_size_;
};
