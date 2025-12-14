#pragma once
#include "memtable.hpp"
#include <optional>
#include <vector>

class Storage {
public:
  Storage(StorageOption opt)
      : opt_(std::move(opt)), active_memtable_(opt.mem_table_size) {};
  void put(std::vector<std::byte> &key, std::vector<std::byte> &value);
  std::optional<std::vector<std::byte>> get(std::vector<std::byte> &key);

private:
  StorageOption opt_;
  std::vector<MemTable> immutable_memtable_;
  MemTable active_memtable_;
  std::mutex mu_;
};

struct StorageOption {
  std::uint64_t mem_table_size;
};
