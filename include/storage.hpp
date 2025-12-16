#pragma once
#include "memtable.hpp"
#include <optional>
#include <vector>

struct StorageOption {
  std::uint64_t mem_table_size;
};

class Storage {
public:
  Storage(StorageOption opt) : opt_(std::move(opt)) {
    active_memtable_ = std::make_unique<MemTable>(opt_.mem_table_size);
  };

  void put(std::vector<std::byte> &key, std::vector<std::byte> &value);
  std::optional<std::vector<std::byte>> get(std::vector<std::byte> &key);
  void remove(std::vector<std::byte> &key);

private:
  std::vector<std::byte> TOMBSTONE = std::vector<std::byte>();

private:
  StorageOption opt_;
  std::vector<std::unique_ptr<MemTable>> immutable_memtable_;
  std::unique_ptr<MemTable> active_memtable_;
  std::shared_mutex mu_;
};
