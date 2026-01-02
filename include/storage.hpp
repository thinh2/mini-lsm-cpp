#pragma once
#include "memtable.hpp"
#include "sst/sst.hpp"
#include <filesystem>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

struct StorageOption {
  std::uint64_t mem_table_size_{4096};
  std::uint64_t max_number_of_memtable_{2};
  std::uint64_t max_sst_block_size_{1024};
  std::filesystem::path sst_directory_{"./sst"};
};

class SST;

class Storage {
public:
  Storage(StorageOption opt) : opt_(std::move(opt)), latest_table_id_(0) {
    if (!opt_.sst_directory_.empty()) {
      std::filesystem::create_directories(opt_.sst_directory_);
    }

    recover();

    active_memtable_ =
        std::make_unique<MemTable>(opt_.mem_table_size_, latest_table_id_++);
    stopped_.store(false, std::memory_order_relaxed);
    flush_thread_ = std::thread([this]() { this->flush_thread(); });
  };

  void close();
  void put(std::vector<std::byte> &key, std::vector<std::byte> &value);
  std::optional<std::vector<std::byte>> get(std::vector<std::byte> &key);
  void remove(std::vector<std::byte> &key);

  void flush_run();
  uint64_t get_current_table_id();
  ~Storage();

private:
  std::vector<std::byte> TOMBSTONE = std::vector<std::byte>();
  std::vector<std::unique_ptr<SST>>
  flush_to_SST(std::vector<std::shared_ptr<MemTable>> &mem_table_ptr);
  void flush_thread();
  void recover();

private:
  StorageOption opt_;
  std::vector<std::shared_ptr<MemTable>> immutable_memtable_;
  std::vector<std::unique_ptr<SST>> sst_;
  std::unique_ptr<MemTable> active_memtable_;
  std::shared_mutex mu_;

  uint64_t latest_table_id_;

  std::atomic<bool> stopped_;
  std::thread flush_thread_;
};
