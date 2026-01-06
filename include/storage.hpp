#pragma once
#include "manifest/manifest.hpp"
#include "memtable.hpp"
#include "sst/sst.hpp"
#include "wal/wal.hpp"
#include <filesystem>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

enum class WALSyncOption {
  SYNC_ON_CLOSE,
  SYNC_ON_WRITE,
};

struct StorageOption {
  std::uint64_t mem_table_size_{4096};
  std::uint64_t max_number_of_memtable_{2};
  std::uint64_t max_sst_block_size_{1024};
  std::filesystem::path sst_directory_{"./sst"};
  std::filesystem::path manifest_path_{"./manifest.json"};
  std::filesystem::path wal_directory_{"./wal"};
  WALSyncOption wal_sync_option{WALSyncOption::SYNC_ON_CLOSE};
};

class SST;

class Storage {
public:
  Storage(StorageOption opt);

  void close();
  void put(std::vector<std::byte> &key, std::vector<std::byte> &value);
  std::optional<std::vector<std::byte>> get(std::vector<std::byte> &key);
  void remove(std::vector<std::byte> &key);

  void flush_run(bool flush_all = false);
  uint64_t get_current_table_id();
  ~Storage();

private:
  std::vector<std::byte> TOMBSTONE = std::vector<std::byte>();
  std::vector<std::unique_ptr<SST>>
  flush_to_SST(std::vector<std::shared_ptr<MemTable>> &mem_table_ptr);
  void flush_thread();
  void recover(const std::vector<ManifestRecord> &);
  void new_active_memtable();

private:
  StorageOption opt_;
  std::vector<std::shared_ptr<MemTable>> immutable_memtable_;
  std::vector<std::unique_ptr<SST>> sst_;
  std::unique_ptr<MemTable> active_memtable_;
  std::unique_ptr<WAL> active_wal_;
  std::shared_mutex mu_;

  uint64_t latest_table_id_;
  Manifest manifest_;
  std::atomic<bool> stopped_;
  std::thread flush_thread_;
};
